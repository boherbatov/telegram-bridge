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
import io
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
POLL_INTERVAL = int(os.environ.get("POLL_INTERVAL", "15"))  # seconds
STATE_FILE = os.environ.get("STATE_FILE", "bridge_state.json")
SUBJECT = os.environ.get("SUBJECT", "Telegram Bridge")

TG_API = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"
AI_BOT_TOKEN = os.environ.get("AI_BOT_TOKEN", "").strip()
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "").strip()
AI_TG_API = f"https://api.telegram.org/bot{AI_BOT_TOKEN}" if AI_BOT_TOKEN else ""
GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-3.5-flash-lite")
GEMINI_IMAGE_MODEL = os.environ.get("GEMINI_IMAGE_MODEL", "gemini-2.5-flash-image")


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
    if str(chat_id) == "saved":
        return True  # Saved Messages: the owner talking to himself
    return bool(TELEGRAM_CHAT_ID) and str(chat_id) == str(TELEGRAM_CHAT_ID)

def subject_for_chat(chat_id):
    """The owner shares one clean thread; every other chat gets its own subject."""
    if str(chat_id) == "saved":
        return f"{SUBJECT} (saved)"
    if is_owner_chat(chat_id):
        return SUBJECT
    return f"{SUBJECT} (chat {chat_id})"

def chat_for_subject(subject):
    """Route an agent reply back to the chat whose thread it answers."""
    m = re.search(r"\(chat (-?\d+)\)", subject or "")
    if m:
        return m.group(1)
    if "(saved)" in (subject or ""):
        return "saved"
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


TG_FILE_MAX = 20 * 1024 * 1024  # Bot API getFile download limit


def tg_file_info(file_id):
    r = requests.get(f"{TG_API}/getFile", params={"file_id": file_id}, timeout=20)
    j = r.json()
    if not j.get("ok"):
        raise RuntimeError(f"getFile failed: {j}")
    return j["result"]


def tg_download_file(file_path):
    url = f"https://api.telegram.org/file/bot{TELEGRAM_BOT_TOKEN}/{file_path}"
    with urllib.request.urlopen(url, timeout=120) as r:
        return r.read()


def extract_media(message):
    """Return (filename, content_type, bytes) for one inbound media message, or None."""
    kind, file_id, file_name, ctype, fsize = None, None, None, None, None
    if message.get("photo"):
        p = message["photo"][-1]  # largest size
        kind, file_id, file_name, ctype = "photo", p["file_id"], "photo.jpg", "image/jpeg"
        fsize = p.get("file_size")
    elif message.get("document"):
        d = message["document"]
        kind, file_id = "document", d["file_id"]
        file_name = d.get("file_name") or "document"
        ctype = d.get("mime_type") or "application/octet-stream"
        fsize = d.get("file_size")
    elif message.get("video"):
        v = message["video"]
        kind, file_id, file_name, ctype = "video", v["file_id"], v.get("file_name") or "video.mp4", v.get("mime_type") or "video/mp4"
        fsize = v.get("file_size")
    elif message.get("audio"):
        a = message["audio"]
        kind, file_id, file_name, ctype = "audio", a["file_id"], a.get("file_name") or "audio.mp3", a.get("mime_type") or "audio/mpeg"
        fsize = a.get("file_size")
    elif message.get("voice"):
        v = message["voice"]
        kind, file_id, file_name, ctype = "voice", v["file_id"], "voice.ogg", v.get("mime_type") or "audio/ogg"
        fsize = v.get("file_size")
    elif message.get("animation"):
        a = message["animation"]
        kind, file_id, file_name, ctype = "animation", a["file_id"], a.get("file_name") or "animation.mp4", a.get("mime_type") or "video/mp4"
        fsize = a.get("file_size")
    if not file_id:
        return None
    if fsize and fsize > TG_FILE_MAX:
        raise OverflowError(fsize)
    info = tg_file_info(file_id)
    if info.get("file_size") and info["file_size"] > TG_FILE_MAX:
        raise OverflowError(info["file_size"])
    data = tg_download_file(info["file_path"])
    return (file_name or "file", ctype or "application/octet-stream", data)


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


def send_email_to_agent(body_text, chat_id, sender_label, attachments=None):
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
    for att_name, att_ctype, att_data in (attachments or []):
        maintype, _, subtype = (att_ctype or "application/octet-stream").partition("/")
        msg.add_attachment(att_data, maintype=maintype or "application",
                           subtype=subtype or "octet-stream", filename=att_name)

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
                            if str(target_chat) == "saved" and (body or atts):
                                tgc = globals().get("tgcontent")
                                if tgc is None:
                                    log.error("saved reply dropped: tgcontent module unavailable")
                                else:
                                    try:
                                        tgc.send_to_saved(body, atts)
                                        log.info("posted agent reply into Saved Messages (%d chars, %d atts)", len(body), len(atts or []))
                                    except Exception as e:
                                        log.error("saved-messages reply failed: %s", e)
                            elif target_chat and (body or atts):
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

# ---------------- standalone fast AI bot ----------------

_ai_rate = {}
_ai_rate_lock = threading.Lock()


def _ai_allowed(chat_id):
    """Per-chat anti-abuse limit: 12 requests/minute and 120/day."""
    now = time.time()
    day = time.strftime("%Y-%m-%d", time.gmtime(now))
    with _ai_rate_lock:
        row = _ai_rate.setdefault(str(chat_id), {"times": [], "day": day, "daily": 0})
        if row["day"] != day:
            row.update(day=day, daily=0, times=[])
        row["times"] = [x for x in row["times"] if now - x < 60]
        if len(row["times"]) >= 12 or row["daily"] >= 120:
            return False
        row["times"].append(now)
        row["daily"] += 1
        return True


def _ai_send(method, payload=None, files=None):
    r = requests.post(f"{AI_TG_API}/{method}", data=payload if files else None,
                      json=None if files else payload, files=files, timeout=90)
    if not r.ok:
        log.error("AI telegram %s failed: %s %s", method, r.status_code, r.text[:300])
    return r


def _ai_send_text(chat_id, text):
    text = (text or "לא הצלחתי ליצור תשובה.").strip()
    for i in range(0, len(text), 4000):
        _ai_send("sendMessage", {"chat_id": str(chat_id), "text": text[i:i+4000]})


def _gemini(parts, model=None, response_modalities=None):
    model = model or GEMINI_MODEL
    body = {"contents": [{"role": "user", "parts": parts}],
            "generationConfig": {"temperature": 0.35, "maxOutputTokens": 2048}}
    if response_modalities:
        body["generationConfig"]["responseModalities"] = response_modalities
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
    r = requests.post(url, params={"key": GEMINI_API_KEY}, json=body, timeout=120)
    if not r.ok:
        raise RuntimeError(f"Gemini {r.status_code}: {r.text[:500]}")
    return r.json()


def _gemini_text(prompt, image=None):
    parts = [{"text": ("ענה בעברית, מהר ובקצרה אלא אם ביקשו פירוט. " + prompt)}]
    if image:
        mime, data = image
        parts.insert(0, {"inline_data": {"mime_type": mime,
                                        "data": base64.b64encode(data).decode()}})
    j = _gemini(parts)
    return "\n".join(p.get("text", "") for c in j.get("candidates", [])
                      for p in c.get("content", {}).get("parts", []) if p.get("text")).strip()


def _gemini_image(prompt):
    j = _gemini([{"text": prompt}], model=GEMINI_IMAGE_MODEL,
                response_modalities=["TEXT", "IMAGE"])
    texts, image = [], None
    for c in j.get("candidates", []):
        for p in c.get("content", {}).get("parts", []):
            if p.get("text"):
                texts.append(p["text"])
            blob = p.get("inlineData") or p.get("inline_data")
            if blob and blob.get("data"):
                image = (blob.get("mimeType") or blob.get("mime_type") or "image/png",
                         base64.b64decode(blob["data"]))
    return "\n".join(texts).strip(), image


def _ai_download_photo(message):
    photo = (message.get("photo") or [])[-1]
    info = requests.get(f"{AI_TG_API}/getFile", params={"file_id": photo["file_id"]}, timeout=20).json()
    path = info["result"]["file_path"]
    data = requests.get(f"https://api.telegram.org/file/bot{AI_BOT_TOKEN}/{path}", timeout=60).content
    return "image/jpeg", data


def _process_ai_update(update):
    message = update.get("message") or update.get("edited_message") or {}
    chat_id = (message.get("chat") or {}).get("id")
    if not chat_id:
        return
    if not _ai_allowed(chat_id):
        _ai_send_text(chat_id, "יותר מדי בקשות כרגע. נסה שוב בעוד דקה.")
        return
    text = (message.get("text") or message.get("caption") or "").strip()
    try:
        if text in ("/start", "/help"):
            _ai_send_text(chat_id, "שלום! אני בוט AI מהיר. שלח שאלה או תמונה. ליצירת תמונה כתוב: צייר: ואז תיאור.")
            return
        if text.startswith(("צייר:", "/image ", "תיצור תמונה:")):
            prompt = text.split(":", 1)[-1].strip() if ":" in text else text[7:].strip()
            _ai_send("sendChatAction", {"chat_id": str(chat_id), "action": "upload_photo"})
            caption, image = _gemini_image(prompt)
            if not image:
                raise RuntimeError("Gemini image model returned no image")
            _ai_send("sendPhoto", {"chat_id": str(chat_id), "caption": caption[:900]},
                     files={"photo": ("image.png", image[1], image[0])})
            return
        _ai_send("sendChatAction", {"chat_id": str(chat_id), "action": "typing"})
        image = _ai_download_photo(message) if message.get("photo") else None
        prompt = text or "תאר את התמונה והסבר מה רואים בה."
        _ai_send_text(chat_id, _gemini_text(prompt, image=image))
    except Exception as e:
        log.error("AI bot error: %s", e)
        if "429" in str(e) and "image" in str(e).lower():
            _ai_send_text(chat_id, "יצירת תמונות לא זמינה כרגע במכסה החינמית. שאלות והבנת תמונות ממשיכות לעבוד.")
        else:
            _ai_send_text(chat_id, "לא הצלחתי לענות כרגע. נסה שוב בעוד רגע.")

# ---------------- flask app ----------------

app = Flask(__name__)

@app.get("/health")
def health():
    return "ok", 200

@app.post("/ai-webhook")
def ai_telegram_webhook():
    update = request.get_json(silent=True) or {}
    threading.Thread(target=_process_ai_update, args=(update,), daemon=True).start()
    return jsonify(ok=True), 200


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
    caption = message.get("caption") or ""
    has_media = any(k in message for k in ("photo", "document", "video", "audio", "voice", "animation"))
    if not text and not has_media:
        tg_send("כרגע אפשר לשלוח טקסט, תמונות וקבצים דרך הגשר.", chat_id=chat_id)
        return jsonify(ok=True), 200

    try:
        if has_media:
            try:
                att = extract_media(message)
            except OverflowError as oe:
                mb = round(int(oe.args[0]) / (1024 * 1024), 1) if oe.args else "?"
                tg_send(f"📎 הקובץ גדול מדי להעברה דרך הגשר ({mb}MB, המגבלה 20MB).", chat_id=chat_id)
                return jsonify(ok=True), 200
            note = caption or ""
            send_email_to_agent(note, chat_id, sender_label, attachments=[att])
        else:
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

@app.get("/set-ai-webhook")
def set_ai_webhook():
    if not WEBHOOK_SECRET or request.args.get("secret") != WEBHOOK_SECRET:
        return "forbidden", 403
    if not AI_BOT_TOKEN or not GEMINI_API_KEY:
        return jsonify({"ok": False, "error": "AI bot not configured"}), 503
    url = request.url_root.replace("http://", "https://") + "ai-webhook"
    r = requests.post(f"{AI_TG_API}/setWebhook", json={"url": url}, timeout=20)
    return jsonify(r.json()), (200 if r.ok else 500)


@app.get("/set-webhook")
def set_webhook():
    if not WEBHOOK_SECRET or request.args.get("secret") != WEBHOOK_SECRET:
        return "forbidden", 403
    url = request.url_root.replace("http://", "https://") + "webhook"
    r = requests.post(f"{TG_API}/setWebhook", json={"url": url}, timeout=20)
    return jsonify(r.json()), (200 if r.ok else 500)

def keep_alive():
    """Render free sleeps after 15 min without inbound traffic; ping ourselves every 10 min.

    RENDER_EXTERNAL_URL is set automatically by Render. The GitHub Actions cron
    remains as the boot kicker for the first wake after a real sleep.
    """
    url = os.environ.get("RENDER_EXTERNAL_URL", "").rstrip("/")
    if not url:
        log.info("keep-alive disabled (RENDER_EXTERNAL_URL not set)")
        return
    time.sleep(60)  # let the service finish booting first
    while True:
        try:
            requests.get(url + "/health", timeout=15)
        except Exception as e:
            log.warning("keep-alive ping failed: %s", e)
        time.sleep(600)


# Telegram user-account content access (optional; active when TG_API_ID/TG_API_HASH are set)
try:
    import tgcontent
    tgcontent.register(app)
    tgcontent.set_saved_deliver(
        lambda text, atts=None: send_email_to_agent(text, "saved", "saved", attachments=atts))
except Exception as e:
    log.error("tgcontent module not loaded: %r", e)

# Background threads start lazily on the first request (fork-safe: if gunicorn
# preloads the app, module-level threads would die in the master on fork).
_bg_lock = threading.Lock()
_bg = {}


@app.before_request
def _ensure_bg_threads():
    with _bg_lock:
        if _bg.get("poller") is None or not _bg["poller"].is_alive():
            t = threading.Thread(target=poll_agent_replies, daemon=True)
            t.start()
            _bg["poller"] = t
        if _bg.get("keepalive") is None or not _bg["keepalive"].is_alive():
            t = threading.Thread(target=keep_alive, daemon=True)
            t.start()
            _bg["keepalive"] = t
        tgc = globals().get("tgcontent")
        if tgc is not None and getattr(tgc, "CONFIGURED", False) and not _bg.get("tgcontent_ok"):
            try:
                _bg["tgcontent_ok"] = bool(tgc.ensure_started())
            except Exception as e:
                log.error("tgcontent start failed: %r", e)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "8000")))
