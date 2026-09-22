"""
Telegram user-account content access (Telethon), bolted onto the bridge service.

Optional: active only when TG_API_ID / TG_API_HASH are set. With a StringSession
in TG_SESSION it is fully authorized; without one, the /tg/setup/* flow creates
a session (the owner approves a login code in his own Telegram app once).

Also hosts the Saved Messages watcher: messages the owner writes in his own
Saved Messages (chat "me") are forwarded to the agent by email, and the agent's
email replies are posted back into Saved Messages as his account. The watcher
never writes to any other chat.

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

# ---------------- saved-messages watcher state ----------------

SM_REPLY_PREFIX = "🤖 "          # prefix on answers we post; loop backstop across restarts
SM_MEDIA_MAX = 20 * 1024 * 1024  # attachments above this are noted, not emailed

_me_id = None
_deliver_fn = None   # set by app.py: fn(body_text, attachments) -> email to agent
_sent_ids = set()    # ids of messages we posted into Saved Messages this run

bp = Blueprint("tgcontent", __name__)


def set_saved_deliver(fn):
    """app.py hands over its send_email_to_agent wrapper (avoids a circular import)."""
    global _deliver_fn
    _deliver_fn = fn


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
        try:
            _loop.run_until_complete(_watch_setup())
        except Exception as e:
            log.error("saved watcher setup failed: %r", e)
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
    if str(chat).lower() in ("me", "self", "saved"):
        return "me"  # Saved Messages; telethon accepts the literal
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


# ---------------- saved-messages watcher ----------------

def _register_watcher():
    """Attach the Saved Messages NewMessage handler to the live client (once per client)."""
    if _client is None or getattr(_client, "_saved_watcher_on", False):
        return
    from telethon import events

    @_client.on(events.NewMessage())
    async def _on_new_message(event):
        try:
            if _me_id is None or event.chat_id != _me_id:
                return  # not Saved Messages
            m = event.message
            if m is None or not m.out:
                return
            if m.id in _sent_ids:
                return  # posted by us this run
            if (m.message or "").startswith(SM_REPLY_PREFIX):
                return  # posted by us before a restart
            if getattr(m, "action", None) is not None:
                return  # service message
            await _handle_saved_message(m)
        except Exception as e:
            log.error("saved watcher handler error: %r", e)

    _client._saved_watcher_on = True
    log.info("saved-messages watcher registered (me=%s)", _me_id)


async def _watch_setup():
    """After connect: if authorized, resolve self and arm the watcher + catch up missed updates."""
    global _me_id
    if not await _client.is_user_authorized():
        log.info("saved watcher: client not authorized yet; will register after setup completes")
        return
    me = await _client.get_me()
    _me_id = me.id
    _register_watcher()
    try:
        await _client.catch_up()  # replay questions written while the service was down
    except Exception as e:
        log.warning("saved watcher catch_up failed: %r", e)


async def _handle_saved_message(m):
    text = (m.message or "").strip()
    notes, atts = [], []
    if m.media is not None:
        f = m.file
        if f is None:
            notes.append("[מדיה מסוג שאינו נתמך]")
        elif f.size and f.size > SM_MEDIA_MAX:
            notes.append("[קובץ מצורף גדול מדי להעברה: %s (%.1fMB)]" % (f.name or "media", f.size / 1048576))
        else:
            try:
                data = await _client.download_media(m, file=bytes)
                if data:
                    atts.append((f.name or "file", f.mime_type or "application/octet-stream", data))
            except Exception as e:
                log.error("saved media download failed (msg %s): %r", m.id, e)
                notes.append("[הורדת הקובץ המצורף נכשלה]")
    body = "\n".join(x for x in [text] + notes if x).strip()
    if not body and not atts:
        return
    deliver = _deliver_fn
    if deliver is None:
        log.error("saved watcher: no deliver function set; dropping message %s", m.id)
        return
    try:
        await asyncio.get_running_loop().run_in_executor(None, deliver, body, atts)
        log.info("saved question msg=%s forwarded to agent (%d chars, %d atts)", m.id, len(body), len(atts))
    except Exception as e:
        log.error("saved question msg=%s email failed: %r", m.id, e)


async def _send_saved_async(body, atts, mark=True):
    """Send text chunks and attachments into Saved Messages ('me')."""
    import io
    ids = []
    MAX = 3800
    chunks = [body[i:i + MAX] for i in range(0, len(body), MAX)] if body else []
    for chunk in chunks:
        msg = await _client.send_message("me", (SM_REPLY_PREFIX + chunk) if mark else chunk)
        ids.append(msg.id)
    for i, (fname, ctype, data) in enumerate(atts or []):
        bio = io.BytesIO(data)
        bio.name = fname or "file"
        caption = None
        if mark and not chunks and i == 0:
            caption = SM_REPLY_PREFIX.strip()  # file-only reply still carries the loop marker
        try:
            msg = await _client.send_file("me", bio, caption=caption)
            ids.append(msg.id)
        except Exception as e:
            log.error("saved attachment send failed (%s): %r", fname, e)
    if mark:
        _sent_ids.update(ids)
        while len(_sent_ids) > 500:
            _sent_ids.pop()
    return ids


def send_to_saved(body, atts=None):
    """Post the agent's reply into the owner's Saved Messages, as his account."""
    return run(_send_saved_async(body or "", atts, mark=True))


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
            out["saved_watcher"] = bool(getattr(_client, "_saved_watcher_on", False))
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
            "id": getattr(ent, "id", None) or d.id,
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


async def _video_items(chat, limit, only_id=None):
    entity = await _resolve(chat)
    messages = [await _client.get_messages(entity, ids=int(only_id))] if only_id else []
    if not only_id:
        async for m in _client.iter_messages(entity, limit=limit):
            messages.append(m)
    out = []
    for m in messages:
        if m is None or m.media is None or m.file is None:
            continue
        mime = (m.file.mime_type or "").lower()
        if not (mime.startswith("video/") or getattr(m.file, "duration", None)):
            continue
        text = (m.message or "").strip()
        title = re.split(r"[\n.!?]", text, 1)[0][:140] or (m.file.name or "Telegram video")
        out.append({
            "id": "tg-%s-%s" % (str(chat).replace("@", ""), m.id),
            "type": "telegram", "title": title, "description": text,
            "telegram_channel": str(chat).replace("@", ""), "telegram_message_id": m.id,
            "source_url": "https://t.me/%s/%s" % (str(chat).replace("@", ""), m.id),
            "published_at": m.date.isoformat() if m.date else None,
            "duration_seconds": getattr(m.file, "duration", None),
            "file_size": m.file.size, "mime_type": mime,
        })
    return out

async def _send_botfather(text):
    """Send one command/message to Telegram's verified BotFather only."""
    entity = await _client.get_entity("BotFather")
    if getattr(entity, "username", "").lower() != "botfather":
        raise ValueError("resolved entity is not BotFather")
    m = await _client.send_message(entity, text)
    return {"sent": True, "id": m.id, "date": m.date.isoformat() if m.date else None}


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
    global _me_id
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
    _me_id = me.id
    _register_watcher()
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



@bp.get("/tg/videos")
def tg_videos():
    if not _secret_ok(): return "forbidden", 403
    chat = request.args.get("chat", "")
    limit = min(int(request.args.get("limit", "50")), 100)
    try: return jsonify({"items": run(_video_items(chat, limit))})
    except Exception as e: return jsonify({"error": repr(e)}), 500

@bp.get("/tg/post")
def tg_post():
    if not _secret_ok(): return "forbidden", 403
    chat, msg = request.args.get("chat", ""), request.args.get("msg", "")
    try:
        items = run(_video_items(chat, 1, msg))
        if not items: return jsonify({"error": "message has no video"}), 404
        return jsonify({"item": items[0]})
    except Exception as e: return jsonify({"error": repr(e)}), 500

@bp.get("/tg/thumb")
def tg_thumb():
    if not _secret_ok(): return "forbidden", 403
    chat, msg = request.args.get("chat", ""), request.args.get("msg", "")
    try:
        m = run(_get_message(chat, msg))
        data = run(_client.download_media(m, file=bytes, thumb=-1))
        if not data: return "not found", 404
        return Response(data, content_type="image/jpeg", headers={"Cache-Control":"public,max-age=86400"})
    except Exception as e: return jsonify({"error": repr(e)}), 500

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


@bp.post("/tg/botfather/send")
def tg_botfather_send():
    """Narrow write endpoint: only the verified @BotFather account."""
    if not _token_ok():
        return "forbidden", 403
    if not CONFIGURED:
        return jsonify({"configured": False}), 503
    data = request.get_json(silent=True) or {}
    text = (data.get("text") or "").strip()
    if not text or len(text) > 128:
        return jsonify({"error": "text required (max 128 chars)"}), 400
    try:
        return jsonify(run(_send_botfather(text)))
    except Exception as e:
        return jsonify({"error": repr(e)}), 500


@bp.post("/tg/saved/send")
def tg_saved_send():
    """Post a raw text message into the owner's Saved Messages (verification/tests).

    Hardcoded to chat 'me' - this endpoint cannot write to any other chat.
    """
    if not _token_ok():
        return "forbidden", 403
    if not CONFIGURED:
        return jsonify({"configured": False}), 503
    data = request.get_json(silent=True) or {}
    text = (data.get("text") or request.args.get("text", "")).strip()
    if not text:
        return jsonify({"error": "text required"}), 400
    try:
        ids = run(_send_saved_async(text, None, mark=False))
        return jsonify({"sent": True, "ids": ids})
    except Exception as e:
        return jsonify({"error": repr(e)}), 500


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
    # so it always lives in the process that actually serves requests. app.py also
    # calls ensure_started() on boot so the Saved Messages watcher stays live.
    if not CONFIGURED:
        log.info("tgcontent not configured (TG_API_ID/TG_API_HASH missing); endpoints return 503")
