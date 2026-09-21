"""
Telegram user-account content access (Telethon), bolted onto the bridge service.

Optional: active only when TG_API_ID / TG_API_HASH are set. With a StringSession
in TG_SESSION it is fully authorized; without one, the /tg/setup/* flow creates
a session (the owner approves a login code in his own Telegram app once).

Security:
  - data endpoints  -> require WEBHOOK_SECRET as ?secret=
  - setup endpoints -> require TG_SETUP_TOKEN as ?token=
"""

import asyncio
import logging
import os
import queue
import re
import threading
import urllib.parse

from flask import Blueprint, Response, jsonify, request, stream_with_context

log = logging.getLogger("tgcontent")

API_ID = os.environ.get("TG_API_ID", "").strip()
API_HASH = os.environ.get("TG_API_HASH", "").strip()
SESSION = os.environ.get("TG_SESSION", "").strip()
SETUP_TOKEN = os.environ.get("TG_SETUP_TOKEN", "").strip()
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "")

CONFIGURED = bool(API_ID and API_HASH)

_loop = None
_client = None
_thread = None
_ready = threading.Event()
_init_err = None
_pending = {}  # setup flow: phone + phone_code_hash

bp = Blueprint("tgcontent", __name__)


def _thread_main():
    global _loop, _client, _init_err
    try:
        from telethon import TelegramClient
        from telethon.sessions import StringSession
        _loop = asyncio.new_event_loop()
        asyncio.set_event_loop(_loop)
        _client = TelegramClient(StringSession(SESSION or None), int(API_ID), API_HASH, loop=_loop)
        _loop.run_until_complete(_client.connect())
        log.info("tgcontent telethon client connected (session %s)", "present" if SESSION else "empty")
    except Exception as e:  # keep the bridge alive even if telethon fails
        _init_err = e
        log.error("tgcontent init failed: %r", e)
    finally:
        _ready.set()
    if _loop is not None:
        _loop.run_forever()


_start_lock = threading.Lock()


def ensure_started():
    """Start the Telethon loop thread in THIS process, once (fork-safe).

    If gunicorn preloads the app, the module import runs in the master and the
    thread objects it created do not survive into the forked worker. Detect a
    dead thread and restart it inside the serving worker.
    """
    global _thread, _client, _init_err
    if not CONFIGURED:
        return False
    with _start_lock:
        if _thread is not None and not _thread.is_alive():
            log.warning("tgcontent thread not alive in this process; restarting")
            _thread = None
            _client = None
            _init_err = None
            _ready.clear()
        if _thread is None:
            _thread = threading.Thread(target=_thread_main, daemon=True, name="tgcontent")
            _thread.start()
    _ready.wait(timeout=60)
    return _init_err is None and _client is not None


def run(coro, timeout=120):
    """Run a coroutine on the Telethon loop from a Flask worker thread."""
    if not ensure_started():
        raise RuntimeError("tgcontent unavailable: %r" % (_init_err,))
    fut = asyncio.run_coroutine_threadsafe(coro, _loop)
    return fut.result(timeout=timeout)


def _secret_ok():
    s = request.args.get("secret", "")
    return (WEBHOOK_SECRET and s == WEBHOOK_SECRET) or (SETUP_TOKEN and s == SETUP_TOKEN)


def _token_ok():
    return bool(SETUP_TOKEN) and request.args.get("token") == SETUP_TOKEN


async def _resolve(chat):
    """Resolve a channel/group/user by id, rebuilding the entity cache if needed."""
    from telethon.utils import get_peer_id
    cid = int(chat)
    try:
        return await _client.get_entity(cid)
    except Exception:
        pass
    async for d in _client.iter_dialogs(limit=300):
        ent = d.entity
        if getattr(ent, "id", None) == cid:
            return ent
        try:
            if get_peer_id(ent) == cid:
                return ent
        except Exception:
            continue
    raise ValueError("chat %s not found among dialogs" % chat)


# ---------------- async workers ----------------

async def _status():
    out = {"configured": True, "authorized": False}
    try:
        if await _client.is_user_authorized():
            me = await _client.get_me()
            out["authorized"] = True
            out["me"] = {
                "id": me.id,
                "phone": getattr(me, "phone", None),
                "name": " ".join(x for x in [me.first_name, me.last_name] if x),
                "username": getattr(me, "username", None),
            }
    except Exception as e:
        out["error"] = repr(e)
    return out


async def _dialogs(limit):
    from telethon.utils import get_peer_id
    out = []
    async for d in _client.iter_dialogs(limit=limit):
        ent = d.entity
        kind = "channel" if d.is_channel else ("group" if d.is_group else "user")
        if d.is_channel and getattr(ent, "broadcast", False):
            kind = "channel"
        elif d.is_channel:
            kind = "supergroup"
        out.append({
            "id": getattr(ent, "id", d.id),
            "peer_id": get_peer_id(ent),
            "title": d.title or d.name or "",
            "type": kind,
            "unread": d.unread_count,
        })
    return out


async def _history(chat, limit, offset_id=0):
    entity = await _resolve(chat)
    out = []
    async for m in _client.iter_messages(entity, limit=limit, offset_id=offset_id):
        item = {
            "id": m.id,
            "date": m.date.isoformat() if m.date else None,
            "text": (m.message or "")[:800],
        }
        if m.media is not None:
            info = {"kind": type(m.media).__name__}
            f = m.file
            if f is not None:
                info["size"] = f.size
                info["name"] = f.name
                info["mime"] = f.mime_type
                info["duration"] = getattr(f, "duration", None)
            item["media"] = info
        out.append(item)
    return out


async def _search(chat, q, limit):
    entity = await _resolve(chat)
    out = []
    async for m in _client.iter_messages(entity, limit=limit, search=q):
        item = {"id": m.id, "date": m.date.isoformat() if m.date else None,
                "text": (m.message or "")[:800]}
        if m.media is not None and m.file is not None:
            item["media"] = {"kind": type(m.media).__name__, "size": m.file.size,
                             "name": m.file.name, "mime": m.file.mime_type}
        out.append(item)
    return out


async def _get_message(chat, msg_id):
    entity = await _resolve(chat)
    m = await _client.get_messages(entity, ids=int(msg_id))
    if m is None:
        raise ValueError("message not found")
    return m


async def _setup_start(phone):
    r = await _client.send_code_request(phone)
    _pending["phone"] = phone
    _pending["hash"] = r.phone_code_hash
    return {"sent": True, "code_delivery": type(r.type).__name__}


async def _setup_complete(code):
    code = re.sub(r"[\s\-]", "", code or "")
    try:
        await _client.sign_in(_pending["phone"], code, phone_code_hash=_pending["hash"])
    except Exception as e:
        from telethon.errors import SessionPasswordNeededError
        if isinstance(e, SessionPasswordNeededError):
            return {"authorized": False, "password_required": True}
        raise
    session_string = _client.session.save()
    me = await _client.get_me()
    return {"authorized": True, "session": session_string, "user_id": me.id}


# ---------------- routes ----------------

def _sess_diag():
    """Non-secret diagnostics about the TG_SESSION env value."""
    import base64, struct
    if not SESSION:
        return {"sess_len": 0}
    try:
        raw = base64.urlsafe_b64decode(SESSION)
        dc = raw[1] if len(raw) > 1 else None
        return {"sess_len": len(SESSION), "sess_bytes": len(raw), "sess_dc": dc}
    except Exception as e:
        return {"sess_len": len(SESSION), "sess_parse_error": repr(e)}


@bp.get("/tg/status")
def tg_status():
    if not _secret_ok():
        return "forbidden", 403
    if not CONFIGURED:
        return jsonify({"configured": False}), 503
    try:
        out = run(_status(), timeout=60)
        out.update(_sess_diag())
        return jsonify(out)
    except Exception as e:
        out = {"configured": True, "error": repr(e)}
        out.update(_sess_diag())
        return jsonify(out), 500


@bp.get("/tg/dialogs")
def tg_dialogs():
    if not _secret_ok():
        return "forbidden", 403
    if not CONFIGURED:
        return jsonify({"configured": False}), 503
    limit = min(int(request.args.get("limit", "100")), 300)
    try:
        return jsonify({"dialogs": run(_dialogs(limit))})
    except Exception as e:
        return jsonify({"error": repr(e)}), 500


@bp.get("/tg/history")
def tg_history():
    if not _secret_ok():
        return "forbidden", 403
    if not CONFIGURED:
        return jsonify({"configured": False}), 503
    chat = request.args.get("chat", "")
    limit = min(int(request.args.get("limit", "20")), 100)
    offset_id = int(request.args.get("offset_id", "0"))
    try:
        return jsonify({"messages": run(_history(chat, limit, offset_id))})
    except Exception as e:
        return jsonify({"error": repr(e)}), 500


@bp.get("/tg/search")
def tg_search():
    if not _secret_ok():
        return "forbidden", 403
    if not CONFIGURED:
        return jsonify({"configured": False}), 503
    chat = request.args.get("chat", "")
    q = request.args.get("q", "")
    limit = min(int(request.args.get("limit", "20")), 50)
    try:
        return jsonify({"messages": run(_search(chat, q, limit))})
    except Exception as e:
        return jsonify({"error": repr(e)}), 500


@bp.get("/tg/download")
def tg_download():
    if not _secret_ok():
        return "forbidden", 403
    if not CONFIGURED:
        return jsonify({"configured": False}), 503
    chat = request.args.get("chat", "")
    msg_id = request.args.get("msg", "")
    try:
        m = run(_get_message(chat, msg_id))
    except Exception as e:
        return jsonify({"error": repr(e)}), 500
    if m.media is None or m.file is None:
        return jsonify({"error": "message has no downloadable media"}), 404

    filename = m.file.name or ("file-%s" % msg_id)
    mime = m.file.mime_type or "application/octet-stream"
    size = m.file.size
    chunks = queue.Queue(maxsize=8)

    async def pump():
        try:
            async for chunk in _client.iter_download(m.media, chunk_size=256 * 1024):
                # bounded put: if the client went away, abort instead of
                # freezing the whole event loop on a full queue
                chunks.put(chunk, timeout=30)
        except BaseException as e:
            try:
                chunks.put(e, timeout=5)
            except Exception:
                pass
        try:
            chunks.put(None, timeout=5)
        except Exception:
            pass

    def gen():
        fut = asyncio.run_coroutine_threadsafe(pump(), _loop)
        try:
            while True:
                try:
                    item = chunks.get(timeout=600)
                except queue.Empty:
                    log.error("tg download stalled; aborting stream")
                    break
                if item is None:
                    break
                if isinstance(item, BaseException):
                    log.error("tg download pump failed: %r", item)
                    break
                yield bytes(item)  # telethon yields memoryview; WSGI needs bytes
        finally:
            fut.cancel()

    headers = {
        "Content-Disposition": "attachment; filename*=UTF-8''%s" % urllib.parse.quote(filename),
    }
    if size:
        headers["Content-Length"] = str(size)
    return Response(stream_with_context(gen()), headers=headers, content_type=mime)


@bp.post("/tg/setup/start")
def tg_setup_start():
    if not _token_ok():
        return "forbidden", 403
    if not CONFIGURED:
        return jsonify({"configured": False}), 503
    phone = (request.get_json(silent=True) or {}).get("phone") or request.args.get("phone", "")
    if not phone:
        return jsonify({"error": "phone required"}), 400
    try:
        return jsonify(run(_setup_start(phone)))
    except Exception as e:
        return jsonify({"error": repr(e)}), 500


@bp.post("/tg/setup/complete")
def tg_setup_complete():
    if not _token_ok():
        return "forbidden", 403
    if not CONFIGURED:
        return jsonify({"configured": False}), 503
    if "phone" not in _pending:
        return jsonify({"error": "no setup in progress"}), 400
    code = (request.get_json(silent=True) or {}).get("code") or request.args.get("code", "")
    try:
        return jsonify(run(_setup_complete(code)))
    except Exception as e:
        return jsonify({"error": repr(e)}), 500


def register(app):
    app.register_blueprint(bp)
    # Lazy: the Telethon thread starts on the first /tg/* request (see ensure_started),
    # so it always lives in the process that actually serves requests.
    if not CONFIGURED:
        log.info("tgcontent not configured (TG_API_ID/TG_API_HASH missing); endpoints return 503")
