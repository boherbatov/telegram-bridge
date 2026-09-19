"""
Telegram <-> Email bridge.

Inbound : Telegram message to your bot -> email from your Gmail to the agent.
Outbound: agent's email replies (polled over IMAP) -> Telegram message.

Open bot: anyone who messages the bot is bridged. Only the owner's chat
(TELEGRAM_CHAT_ID) gets the trust marker prefix; other chats are forwarded
without it, on their own email thread, so the agent can tell them apart.

All configuration comes from environment variables (see README.md).
"""

import imaplib
import json
import logging
import socket as _socket

# Render free tier has no outbound IPv6; Gmail sometimes resolves to IPv6
# (Errno 101 Network is unreachable). Force IPv4 for all outbound sockets.
_orig_getaddrinfo = _socket.getaddrinfo
def _ipv4_getaddrinfo(*a, **kw):
    return [r for r in _orig_getaddrinfo(*a, **kw) if r[0] == _socket.AF_INET]
_socket.getaddrinfo = _ipv4_getaddrinfo

import os
import re
import smtplib
import threading
import base64
import urllib.request
import urllib.parse
import time
from email import policy
from email.header import decode_header, make_header
from email.message import EmailMessage
from email.parser import BytesParser
from email.utils import make_msgid
from html import unescape

import requests
from flask import Flask, jsonify, request

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("bridge")

# ---------------- configuration ----------------

TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")  # owner's chat id; optional but recommended
GMAIL_ADDRESS = os.environ["GMAIL_ADDRESS"]
GMAIL_APP_PASSWORD = os.environ["GMAIL_APP_PASSWORD"].replace(" ", "")
AGENT_EMAIL = os.environ.get("AGENT_EMAIL", "arielgolan@mail.instinct.com")
TRUST_MARKER = os.environ.get("TRUST_MARKER", "")  # required for owner trust; unset = owner forwards untrusted (fail-safe)
WEBHOOK_SECRET = os.environ.get("WEBHOOK_SECRET", "")
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL", "60"))  # seconds
STATE_FILE = os.environ.get("STATE_FILE", "bridge_state.json")
SUBJECT = os.environ.get("SUBJECT", "Telegram Bridge")

TG_API = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"

# ---------------- state ----------------

_state_lock = threading.Lock()

def _load_state():
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}

def _save_state(state):
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f)
    os.replace(tmp, STATE_FILE)

def get_state():
    with _state_lock:
        return _load_state()

def update_state(**kwargs):
    with _state_lock:
        state = _load_state()
        state.update(kwargs)
        _save_state(state)

def thread_refs(subject):
    return get_state().get("threads", {}).get(subject, [])

def add_thread_ref(subject, msg_id):
    state = get_state()
    threads = state.get("threads", {})
    threads[subject] = (threads.get(subject, []) + [msg_id])[-20:]
    update_state(threads=threads)

# ---------------- threading helpers ----------------

def is_owner_chat(chat_id):
    return bool(TELEGRAM_CHAT_ID) and str(chat_id) == str(TELEGRAM_CHAT_ID)

def subject_for_chat(chat_id):
    """The owner shares one clean thread; every other chat gets its own subject."""
    if is_owner_chat(chat_id):
        return SUBJECT
    return f"{SUBJECT} (chat {chat_id})"

def chat_for_subject(subject):
    """Route an agent reply back to the chat whose thread it answers."""
    m = re.search(r"\(chat (-?\d+)\)", subject or "")
    if m:
        return m.group(1)
    return TELEGRAM_CHAT_ID or None

# ---------------- telegram ----------------

def tg_send(text, chat_id=None):
    """Send a (possibly long) text to a Telegram chat, split at Telegram's limit."""
    chat_id = str(chat_id or TELEGRAM_CHAT_ID)
    MAX = 4000
    chunks = [text[i:i + MAX] for i in range(0, len(text), MAX)] or [""]
    for chunk in chunks:
        r = requests.post(
            f"{TG_API}/sendMessage",
            json={"chat_id": chat_id, "text": chunk},
            timeout=20,
        )
        if not r.ok:
            log.error("telegram sendMessage failed: %s %s", r.status_code, r.text)
            return False
    return True

TG_PHOTO_MAX = 10 * 1024 * 1024   # sendPhoto limit
TG_DOC_MAX = 50 * 1024 * 1024     # sendDocument limit (bots)


def tg_send_media(kind, chat_id, filename, ctype, data, caption=None):
    """Send one photo/document to a Telegram chat via multipart upload."""
    chat_id = str(chat_id or TELEGRAM_CHAT_ID)
    field = "photo" if kind == "photo" else "document"
    method = "sendPhoto" if kind == "photo" else "sendDocument"
    payload = {"chat_id": chat_id}
    if caption:
        payload["caption"] = caption[:1024]
    try:
        r = requests.post(f"{TG_API}/{method}", data=payload,
                          files={field: (filename, data, ctype)}, timeout=60)
    except Exception as e:
        log.error("telegram %s failed: %s", method, e)
        return False
    if not r.ok:
        log.error("telegram %s failed: %s %s", method, r.status_code, r.text[:200])
        return False
    return True


def forward_attachments(atts, chat_id, caption=None):
    """Send extracted attachments to Telegram; returns note lines for failures."""
    caption_left = caption[:1024] if caption else None
    notes = []
    for filename, ctype, data in atts:
        is_image = ctype.startswith("image/")
        if is_image and len(data) <= TG_PHOTO_MAX:
            kind = "photo"
        elif len(data) <= TG_DOC_MAX:
            kind = "document"  # oversized images ride as documents
        else:
            mb = round(len(data) / (1024 * 1024), 1)
            notes.append(f"\U0001F4CE {filename} - הקובץ גדול מדי להעברה לטלגרם ({mb}MB)")
            continue
        if tg_send_media(kind, chat_id, filename, ctype, data, caption=caption_left):
            caption_left = None
        else:
            notes.append(f"\U0001F4CE {filename} ({ctype}) - ההעברה לטלגרם נכשלה")
    return notes


# ---------------- email: send ----------------

GOOGLE_CLIENT_ID = os.environ.get("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.environ.get("GOOGLE_CLIENT_SECRET", "")
GOOGLE_REFRESH_TOKEN = os.environ.get("GOOGLE_REFRESH_TOKEN", "")


def gmail_api_send(msg):
    """Send via Gmail API over HTTPS (Render free blocks outbound SMTP ports)."""
    data = urllib.parse.urlencode({
        "client_id": GOOGLE_CLIENT_ID,
        "client_secret": GOOGLE_CLIENT_SECRET,
        "refresh_token": GOOGLE_REFRESH_TOKEN,
        "grant_type": "refresh_token",
    }).encode()
    with urllib.request.urlopen(urllib.request.Request(
            "https://oauth2.googleapis.com/token", data=data), timeout=20) as r:
        access_token = json.loads(r.read())["access_token"]
    raw = base64.urlsafe_b64encode(msg.as_bytes()).decode()
    req = urllib.request.Request(
        "https://gmail.googleapis.com/gmail/v1/users/me/messages/send",
        data=json.dumps({"raw": raw}).encode(),
        headers={"Authorization": "Bearer " + access_token,
                 "Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=20) as r:
        return json.loads(r.read())


def send_email_to_agent(body_text, chat_id, sender_label):
    subject = subject_for_chat(chat_id)
    references = thread_refs(subject)

    if is_owner_chat(chat_id):
        body = f"{TRUST_MARKER}\n{body_text}" if TRUST_MARKER else body_text
    else:
        # Strangers are bridged WITHOUT the trust marker, labelled by sender.
        body = f"[telegram: {sender_label}]\n{body_text}"

    msg = EmailMessage()
    msg["From"] = GMAIL_ADDRESS
    msg["To"] = AGENT_EMAIL
    msg["Subject"] = ("Re: " + subject) if references else subject
    msg_id = make_msgid("telegram-bridge")
    msg["Message-ID"] = msg_id
    if references:
        msg["In-Reply-To"] = references[-1]
        msg["References"] = " ".join(references[-10:])
    msg.set_content(body)

    if GOOGLE_REFRESH_TOKEN:
        gmail_api_send(msg)
        return True
    last_err = None
    for attempt in ("starttls", "ssl"):
        try:
            if attempt == "starttls":
                with smtplib.SMTP("smtp.gmail.com", 587, timeout=20) as smtp:
                    smtp.ehlo()
                    smtp.starttls()
                    smtp.login(GMAIL_ADDRESS, GMAIL_APP_PASSWORD)
                    smtp.send_message(msg)
            else:
                with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=20) as smtp:
                    smtp.login(GMAIL_ADDRESS, GMAIL_APP_PASSWORD)
                    smtp.send_message(msg)
            last_err = None
            break
        except Exception as e:
            last_err = e
            log.warning("smtp %s attempt failed: %r", attempt, e)
    if last_err is not None:
        raise last_err

    add_thread_ref(subject, msg_id)
    log.info("sent email to agent (chat %s, %d chars)", chat_id, len(body_text))

# ---------------- email: poll ----------------

def _decode(value):
    if value is None:
        return ""
    return str(make_header(decode_header(value)))

def _extract_text(msg):
    """Prefer text/plain; fall back to stripped text/html."""
    plain, html_part = None, None
    if msg.is_multipart():
        for part in msg.walk():
            if part.get_content_disposition() == "attachment":
                continue
            ctype = part.get_content_type()
            if ctype == "text/plain" and plain is None:
                plain = part
            elif ctype == "text/html" and html_part is None:
                html_part = part
    else:
        if msg.get_content_type() == "text/plain":
            plain = msg
        else:
            html_part = msg

    def payload(part):
        try:
            return part.get_content()
        except Exception:
            raw = part.get_payload(decode=True) or b""
            return raw.decode(part.get_content_charset() or "utf-8", errors="replace")

    if plain is not None:
        return payload(plain).strip()
    if html_part is not None:
        text = unescape(re.sub(r"<[^>]+>", " ", payload(html_part)))
        return re.sub(r"\n{3,}", "\n\n", re.sub(r"[ \t]+", " ", text)).strip()
    return ""

def _extract_attachments(msg):
    """Return [(filename, content_type, bytes)] for real attachments (inline skipped)."""
    out = []
    if not msg.is_multipart():
        return out
    for part in msg.walk():
        if part.get_content_disposition() != "attachment":
            continue
        filename = _decode(part.get_filename()) or "attachment"
        try:
            data = part.get_content()
            if isinstance(data, str):
                data = data.encode("utf-8", errors="replace")
        except Exception:
            data = part.get_payload(decode=True) or b""
        out.append((filename, part.get_content_type() or "application/octet-stream", data))
    return out


def _imap_connect():
    conn = imaplib.IMAP4_SSL("imap.gmail.com", 993)
    conn.login(GMAIL_ADDRESS, GMAIL_APP_PASSWORD)
    conn.select("INBOX")
    return conn

def poll_agent_replies():
    """Poll Gmail for agent replies and forward new ones to the right Telegram chat."""
    log.info("gmail poller started (every %ss, from %s)", POLL_INTERVAL, AGENT_EMAIL)
    while True:
        try:
            conn = _imap_connect()
            try:
                typ, data = conn.uid("search", None, f'(FROM "{AGENT_EMAIL}")')
                uids = data[0].split() if typ == "OK" and data and data[0] else []
                if uids:
                    state = get_state()
                    last_uid = state.get("last_uid")
                    if last_uid is None:
                        # First ever run (or state wiped): skip history, start from now.
                        update_state(last_uid=int(uids[-1]))
                        log.info("initialised last_uid=%s, history skipped", int(uids[-1]))
                    else:
                        new_uids = [u for u in uids if int(u) > int(last_uid)]
                        for uid in new_uids:
                            typ, fetched = conn.uid("fetch", uid, "(BODY.PEEK[])")
                            if typ != "OK" or not fetched or not fetched[0]:
                                continue
                            msg = BytesParser(policy=policy.default).parsebytes(fetched[0][1])
                            body = _extract_text(msg)
                            subject = _decode(msg.get("Subject"))
                            base_subject = re.sub(r"^(Re:\s*)+", "", subject).strip()
                            target_chat = chat_for_subject(base_subject)
                            atts = _extract_attachments(msg)
                            if target_chat and (body or atts):
                                notes = []
                                if atts:
                                    if body and len(body) <= 1024:
                                        notes = forward_attachments(atts, target_chat, caption=body)
                                    else:
                                        if body:
                                            tg_send(body, chat_id=target_chat)
                                        notes = forward_attachments(atts, target_chat)
                                elif body:
                                    tg_send(body, chat_id=target_chat)
                                if notes:
                                    tg_send("\n".join(notes), chat_id=target_chat)
                            elif body or atts:
                                log.info("no target chat for subject %r, dropped", subject)
                            mid = msg.get("Message-ID")
                            if mid:
                                add_thread_ref(base_subject, mid.strip())
                            update_state(last_uid=int(uid))
                            log.info("forwarded agent email uid=%s to chat %s", int(uid), target_chat)
            finally:
                try:
                    conn.logout()
                except Exception:
                    pass
        except Exception as e:
            log.error("gmail poll error: %s", e)
        time.sleep(POLL_INTERVAL)

# ---------------- flask app ----------------

app = Flask(__name__)

@app.get("/health")
def health():
    return "ok", 200

@app.post("/webhook")
def telegram_webhook():
    update = request.get_json(silent=True) or {}
    message = update.get("message") or update.get("edited_message") or {}
    chat = message.get("chat") or {}
    sender = message.get("from") or {}
    chat_id = chat.get("id")
    if not chat_id:
        return jsonify(ok=True), 200

    sender_label = sender.get("username") or sender.get("first_name") or str(chat_id)

    text = message.get("text")
    if not text:
        tg_send("כרגע אפשר לשלוח רק הודעות טקסט דרך הגשר.", chat_id=chat_id)
        return jsonify(ok=True), 200

    try:
        send_email_to_agent(text, chat_id, sender_label)
        tg_send("✅ נשלח", chat_id=chat_id)
    except Exception as e:
        log.error("failed to forward telegram message: %s", e)
        tg_send("❌ השליחה נכשלה. נסה שוב בעוד רגע.", chat_id=chat_id)
    return jsonify(ok=True), 200

@app.get("/selftest")
def selftest():
    """Check that Gmail IMAP + SMTP logins and the Telegram token all work."""
    if not WEBHOOK_SECRET or request.args.get("secret") != WEBHOOK_SECRET:
        return "forbidden", 403
    result = {}

    try:
        conn = _imap_connect()
        conn.logout()
        result["imap"] = "ok"
    except Exception as e:
        result["imap"] = f"fail: {type(e).__name__}: {e}"

    try:
        if GOOGLE_REFRESH_TOKEN:
            data = urllib.parse.urlencode({
                "client_id": GOOGLE_CLIENT_ID,
                "client_secret": GOOGLE_CLIENT_SECRET,
                "refresh_token": GOOGLE_REFRESH_TOKEN,
                "grant_type": "refresh_token",
            }).encode()
            with urllib.request.urlopen(urllib.request.Request(
                    "https://oauth2.googleapis.com/token", data=data), timeout=15) as r:
                json.loads(r.read())["access_token"]
            result["smtp"] = "ok (gmail-api)"
        else:
            errs = []
            for attempt in ("starttls", "ssl"):
                try:
                    if attempt == "starttls":
                        with smtplib.SMTP("smtp.gmail.com", 587, timeout=15) as smtp:
                            smtp.ehlo()
                            smtp.starttls()
                            smtp.login(GMAIL_ADDRESS, GMAIL_APP_PASSWORD)
                    else:
                        with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=15) as smtp:
                            smtp.login(GMAIL_ADDRESS, GMAIL_APP_PASSWORD)
                    result["smtp"] = "ok"
                    break
                except Exception as e:
                    errs.append(f"{attempt}: {e!r}")
            else:
                probes = {}
                for host, port in (("smtp.gmail.com",587),("smtp.gmail.com",465),("gmail.googleapis.com",443),("google.com",443),("imap.gmail.com",993)):
                    try:
                        with _socket.create_connection((host,port),timeout=8):
                            probes[f"{host}:{port}"]="tcp-ok"
                    except Exception as e:
                        probes[f"{host}:{port}"]=f"{type(e).__name__}"
                result["tcp"]=probes
                raise RuntimeError("; ".join(errs))
    except Exception as e:
        result["smtp"] = f"fail: {type(e).__name__}: {e}"

    try:
        r = requests.get(f"{TG_API}/getMe", timeout=20)
        j = r.json()
        result["telegram"] = "ok" if j.get("ok") else f"fail: {j}"
    except Exception as e:
        result["telegram"] = f"fail: {type(e).__name__}: {e}"

    healthy = all(str(v).startswith("ok") for v in result.values())
    return jsonify(result), (200 if healthy else 500)

@app.get("/set-webhook")
def set_webhook():
    if not WEBHOOK_SECRET or request.args.get("secret") != WEBHOOK_SECRET:
        return "forbidden", 403
    url = request.url_root.replace("http://", "https://") + "webhook"
    r = requests.post(f"{TG_API}/setWebhook", json={"url": url}, timeout=20)
    return jsonify(r.json()), (200 if r.ok else 500)

# Start the Gmail poller in the background (gunicorn runs this module once, one worker).
_poller = threading.Thread(target=poll_agent_replies, daemon=True)
_poller.start()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "8000")))
