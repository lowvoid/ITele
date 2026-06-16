import os
import io
import time
import asyncio
import traceback
import mimetypes
import re
import base64
import json
from collections import OrderedDict
from urllib.parse import quote
from typing import Optional, List, Dict, Any
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query, UploadFile, File, Form, Request, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import StreamingResponse, FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from telethon import TelegramClient, events
from telethon.sessions import StringSession
from telethon.tl.types import (
    User, Chat, Channel,
    InputMessagesFilterDocument,
    InputMessagesFilterPhotos,
    InputMessagesFilterVideo,
    InputMessagesFilterVoice,
    InputFolderPeer,
    MessageMediaPhoto,
    MessageMediaDocument,
    MessageMediaWebPage,
    DocumentAttributeAudio,
    DocumentAttributeVideo,
    DocumentAttributeFilename,
)
from telethon.tl.functions.folders import EditPeerFoldersRequest
from telethon.tl.functions.messages import DeleteHistoryRequest
from telethon.errors import SessionPasswordNeededError

# ─── dotenv optional ───
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ─── Forum support detection ───
HAS_FORUM_SUPPORT = False
try:
    from telethon.tl.functions.messages import GetForumTopicsRequest
    HAS_FORUM_SUPPORT = True
    print("✅ Forum support enabled")
except ImportError as e:
    print("⚠️ Forum support not available:", e)

# ─── Environment variables ───
API_ID_STR     = os.getenv("API_ID", "")
API_HASH       = os.getenv("API_HASH", "")
SESSION_STRING = os.getenv("SESSION_STRING", "")
PORT           = int(os.getenv("PORT", "8000"))

if not API_ID_STR or not API_HASH or not SESSION_STRING:
    print("⚠️  WARNING: API_ID, API_HASH, or SESSION_STRING missing!")

API_ID  = int(API_ID_STR) if API_ID_STR.strip().isdigit() else 0
session = StringSession(SESSION_STRING) if SESSION_STRING else StringSession("")

current_dir  = os.path.dirname(os.path.abspath(__file__))
SESSION_FILE = os.path.join(current_dir, "study_session")

# ─────────────────────────────────────────────────
# LRU CACHE
# ─────────────────────────────────────────────────
class LRUCache:
    def __init__(self, maxsize=500):
        self.cache   = OrderedDict()
        self.maxsize = maxsize

    def get(self, key, default=None):
        if key in self.cache:
            self.cache.move_to_end(key)
            return self.cache[key]
        return default

    def set(self, key, value):
        if key in self.cache:
            self.cache.move_to_end(key)
        self.cache[key] = value
        if len(self.cache) > self.maxsize:
            self.cache.popitem(last=False)

    def pop(self, key, default=None):
        return self.cache.pop(key, default)

    def __contains__(self, key):  return key in self.cache
    def __getitem__(self, key):   return self.get(key)
    def __setitem__(self, key, v):self.set(key, v)

ENTITY_CACHE        = LRUCache(2000)
GLOBAL_SENDER_CACHE = LRUCache(5000)
CHAT_INFO_CACHE     = LRUCache(300)
PINNED_CACHE        = LRUCache(200)

CACHE_DIR = os.path.join(current_dir, "photo_cache")
os.makedirs(CACHE_DIR, exist_ok=True)

CHATS_CACHE      = None
CHATS_CACHE_TIME = 0.0

DOWNLOAD_SEMAPHORE  = asyncio.Semaphore(3)
currently_downloading: set = set()

# ─────────────────────────────────────────────────
# WEBSOCKET MANAGER
# ─────────────────────────────────────────────────
class ConnectionManager:
    def __init__(self):
        self.active: List[WebSocket] = []

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.active.append(ws)

    def disconnect(self, ws: WebSocket):
        if ws in self.active:
            self.active.remove(ws)

    async def broadcast(self, data: dict):
        dead = []
        for ws in self.active:
            try:
                await ws.send_json(data)
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.disconnect(ws)

manager = ConnectionManager()

# ─────────────────────────────────────────────────
# CLIENT MANAGEMENT
# ─────────────────────────────────────────────────
client: Optional[TelegramClient] = None
listen_task: Optional[asyncio.Task] = None
_tg_lock = asyncio.Lock()

async def get_client() -> TelegramClient:
    global client
    if client is None:
        client = TelegramClient(session, API_ID, API_HASH)
    if not client.is_connected():
        await client.connect()
    return client

async def is_authorized() -> bool:
    try:
        c = await get_client()
        if not c.is_connected():
            await c.connect()
        await asyncio.sleep(0.3)
        return await c.is_user_authorized()
    except Exception:
        return False

# ─────────────────────────────────────────────────
# REALTIME LISTENER
# ─────────────────────────────────────────────────
async def start_listening():
    global listen_task
    try:
        c = await get_client()
        # Remove old handlers safely
        try:
            c.remove_event_handler(None, events.NewMessage())
        except Exception:
            pass

        @c.on(events.NewMessage())
        async def handler(event):
            msg = event.message
            try:
                entity    = await get_cached_entity(event.chat_id)
                formatted = await build_message_dict(msg, entity)
                formatted["chat_id"] = event.chat_id
                await manager.broadcast({"type": "new_message", "data": formatted})
            except Exception as e:
                print(f"⚠️ Handler error: {e}")
                await manager.broadcast({
                    "type": "new_message",
                    "data": {
                        "id":      msg.id,
                        "text":    msg.message or "",
                        "chat_id": event.chat_id,
                        "date":    msg.date.isoformat() if msg.date else None,
                    }
                })

        print("✅ Real-time listener started")
    except Exception as e:
        print(f"⚠️ Listener init failed: {e}")
        traceback.print_exc()

# ─────────────────────────────────────────────────
# LIFESPAN (replaces deprecated on_event)
# ─────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    # startup
    try:
        authorized = await is_authorized()
        if authorized:
            print("✅ Session authorized!")
            asyncio.create_task(start_listening())
        else:
            print("⚠️  Not authorized – waiting for login.")
    except Exception as e:
        print(f"⚠️  Startup: {e}")
    yield
    # shutdown
    global client, listen_task
    if listen_task:
        listen_task.cancel()
    if client:
        try:
            await client.disconnect()
        except Exception:
            pass

# ─────────────────────────────────────────────────
# APP
# ─────────────────────────────────────────────────
app = FastAPI(title="StudyGram", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.add_middleware(GZipMiddleware, minimum_size=1000)

# ─────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────
HASHTAG_RE = re.compile(r'(?<!\w)#([\w\u0600-\u06FF]+)', re.UNICODE)

def extract_hashtags(text: str):
    if not text:
        return []
    seen, tags = set(), []
    for tag in HASHTAG_RE.findall(text):
        norm = tag.lower()
        if norm not in seen:
            seen.add(norm)
            tags.append(norm)
    return tags

async def get_cached_entity(chat_id):
    cached = ENTITY_CACHE.get(chat_id)
    if cached:
        return cached
    c      = await get_client()
    entity = await c.get_entity(chat_id)
    ENTITY_CACHE[chat_id] = entity
    return entity

def extract_media_info(message):
    result = {
        "has_media": False, "file_name": "", "file_size": 0,
        "mime_type": "", "media_type": None,
        "duration": None, "width": None, "height": None,
    }
    media = message.media
    if isinstance(media, MessageMediaWebPage) or not media:
        return result
    if not isinstance(media, (MessageMediaPhoto, MessageMediaDocument)):
        return result

    result["has_media"] = True
    file_obj = getattr(message, "file", None)
    if file_obj is not None:
        result["file_size"] = getattr(file_obj, "size", 0) or 0
        result["mime_type"] = getattr(file_obj, "mime_type", "") or ""

    if isinstance(media, MessageMediaPhoto):
        result["mime_type"]  = result["mime_type"] or "image/jpeg"
        result["media_type"] = "photo"
        result["file_name"]  = f"photo_{message.id}.jpg"
        photo = getattr(media, "photo", None)
        sizes = getattr(photo, "sizes", None) or []
        if sizes:
            largest = max(sizes, key=lambda s: getattr(s, "w", 0) * getattr(s, "h", 0))
            result["width"]  = getattr(largest, "w", None)
            result["height"] = getattr(largest, "h", None)
        return result

    if isinstance(media, MessageMediaDocument):
        doc = media.document
        if not doc:
            return result
        result["mime_type"] = getattr(doc, "mime_type", "") or result["mime_type"] or ""
        result["file_size"] = getattr(doc, "size", 0) or result["file_size"] or 0

        is_voice = is_audio = is_video = is_round = False
        custom_name = ""

        for attr in (doc.attributes or []):
            if isinstance(attr, DocumentAttributeFilename):
                custom_name = attr.file_name or ""
            elif isinstance(attr, DocumentAttributeAudio):
                is_audio = True
                is_voice = getattr(attr, "voice", False)
                result["duration"] = getattr(attr, "duration", None)
            elif isinstance(attr, DocumentAttributeVideo):
                is_video = True
                is_round = getattr(attr, "round_message", False)
                result["duration"] = getattr(attr, "duration", None)
                result["width"]    = getattr(attr, "w", None)
                result["height"]   = getattr(attr, "h", None)

        if is_voice:   result["media_type"] = "voice"
        elif is_audio: result["media_type"] = "audio"
        elif is_round: result["media_type"] = "round"
        elif is_video: result["media_type"] = "video"
        else:          result["media_type"] = "document"

        if custom_name:
            result["file_name"] = custom_name
        else:
            ext_map = {
                "image/jpeg":"jpg","image/png":"png","image/webp":"webp",
                "image/gif":"gif","video/mp4":"mp4","video/webm":"webm",
                "audio/mpeg":"mp3","audio/ogg":"ogg","audio/mp4":"m4a",
                "application/pdf":"pdf",
            }
            ext = ext_map.get(result["mime_type"], "")
            if not ext:
                ext = (mimetypes.guess_extension(result["mime_type"]) or ".bin").lstrip(".")
            result["file_name"] = f"file_{message.id}.{ext}"

    return result

async def get_sender_name(message, chat_entity=None):
    if message.out:
        return "Me"
    sid = message.sender_id
    if not sid:
        return ""
    cached = GLOBAL_SENDER_CACHE.get(sid)
    if cached is not None:
        return cached
    if chat_entity and sid == getattr(chat_entity, 'id', None):
        name = getattr(chat_entity, 'title', '') or ''
        GLOBAL_SENDER_CACHE[sid] = name
        return name
    sender = message.sender
    c = await get_client()
    if not sender:
        try:
            sender = await c.get_entity(sid)
        except Exception:
            try:
                sender = await message.get_sender()
            except Exception:
                pass
    name = ""
    if sender:
        fn = getattr(sender, 'first_name', '') or ''
        ln = getattr(sender, 'last_name',  '') or ''
        ti = getattr(sender, 'title',      '') or ''
        name = f"{fn} {ln}".strip() or ti
    GLOBAL_SENDER_CACHE[sid] = name
    return name

def extract_reply_info(message):
    reply = getattr(message, 'reply_to', None)
    if not reply:
        return None
    reply_id = getattr(reply, 'reply_to_msg_id', None)
    if not reply_id:
        return None
    return {"reply_to_msg_id": reply_id}

def extract_forward_info(message):
    fwd = getattr(message, 'fwd_from', None)
    if not fwd:
        return None
    from_name = ""
    from_id   = None
    if getattr(fwd, 'from_name', None):
        from_name = fwd.from_name
    elif getattr(fwd, 'from_id', None):
        from_id_obj = fwd.from_id
        cid = (
            getattr(from_id_obj, 'channel_id', None) or
            getattr(from_id_obj, 'user_id',    None) or
            getattr(from_id_obj, 'chat_id',    None)
        )
        from_id = cid
    date = getattr(fwd, 'date', None)
    return {
        "from_name":     from_name or "Unknown",
        "from_id":       from_id,
        "original_date": date.isoformat() if date else None,
    }

def extract_reactions(message):
    reactions_obj = getattr(message, 'reactions', None)
    if not reactions_obj:
        return []
    results_list = getattr(reactions_obj, 'results', None) or []
    out = []
    for r in results_list:
        reaction = getattr(r, 'reaction', None)
        count    = getattr(r, 'count', 0)
        if not reaction:
            continue
        emoticon = getattr(reaction, 'emoticon', None)
        if emoticon:
            out.append({"emoji": emoticon, "count": count})
    return out

async def build_message_dict(message, entity):
    media_info   = extract_media_info(message)
    sender_name  = await get_sender_name(message, entity)
    reply_info   = extract_reply_info(message)
    forward_info = extract_forward_info(message)
    reactions    = extract_reactions(message)
    text         = message.text or ""

    topic_id = None
    reply_to = getattr(message, 'reply_to', None)
    if reply_to:
        forum_topic = getattr(reply_to, 'forum_topic', False)
        if forum_topic:
            topic_id = getattr(reply_to, 'reply_to_msg_id', None)
        else:
            topic_id = getattr(reply_to, 'reply_to_top_id', None) or \
                       getattr(reply_to, 'reply_to_msg_id', None)

    return {
        "id":           message.id,
        "text":         text,
        "date":         message.date.isoformat() if message.date else None,
        "sender_name":  sender_name,
        "sender_id":    message.sender_id,
        "is_outgoing":  bool(message.out),
        "has_media":    media_info["has_media"],
        "file_name":    media_info["file_name"],
        "file_size":    media_info["file_size"],
        "mime_type":    media_info["mime_type"],
        "media_type":   media_info["media_type"],
        "duration":     media_info["duration"],
        "width":        media_info["width"],
        "height":       media_info["height"],
        "hashtags":     extract_hashtags(text),
        "reply_to":     reply_info,
        "forward_from": forward_info,
        "reactions":    reactions,
        "is_pinned":    getattr(message, 'pinned', False),
        "views":        getattr(message, 'views', None),
        "topic_id":     topic_id,
    }

async def resolve_single_sender(c, sid):
    try:
        ent  = await c.get_entity(sid)
        fn   = getattr(ent, 'first_name', '') or ''
        ln   = getattr(ent, 'last_name',  '') or ''
        ti   = getattr(ent, 'title',      '') or ''
        GLOBAL_SENDER_CACHE[sid] = f"{fn} {ln}".strip() or ti
    except Exception:
        GLOBAL_SENDER_CACHE[sid] = ""

async def batch_resolve_senders(raw_msgs):
    missing_sids = set()
    for msg in raw_msgs:
        if not msg.out and msg.sender_id:
            if GLOBAL_SENDER_CACHE.get(msg.sender_id) is None and not msg.sender:
                missing_sids.add(msg.sender_id)
    if not missing_sids:
        return
    c     = await get_client()
    tasks = [resolve_single_sender(c, sid) for sid in missing_sids]
    await asyncio.gather(*tasks)

# ─────────────────────────────────────────────────
# PYDANTIC MODELS
# ─────────────────────────────────────────────────
class SendCodeRequest(BaseModel):
    phone: Optional[str] = None

class SignInRequest(BaseModel):
    phone:           str
    code:            str
    phone_code_hash: str
    password:        Optional[str] = None

class SendMessageRequest(BaseModel):
    text: str

class SendMessageRequestFriend(BaseModel):
    chat_id:  int
    text:     str
    reply_to: Optional[int] = None

class DeleteMessageRequest(BaseModel):
    chat_id: int

# ─────────────────────────────────────────────────
# AUTH ENDPOINTS
# ─────────────────────────────────────────────────
@app.get("/api/auth/status")
async def auth_status():
    authorized = await is_authorized()
    return {"authorized": authorized}

@app.post("/api/auth/send_code")
async def send_code(req: SendCodeRequest):
    try:
        c      = await get_client()
        result = await c.send_code_request(req.phone)
        return {"phone_code_hash": result.phone_code_hash, "phone": req.phone}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/api/auth/sign_in")
async def sign_in(req: SignInRequest):
    try:
        c = await get_client()
        try:
            user = await c.sign_in(req.phone, req.code, phone_code_hash=req.phone_code_hash)
            asyncio.create_task(start_listening())
            return {"success": True, "user": {"id": user.id, "first_name": getattr(user, "first_name", "")}}
        except SessionPasswordNeededError:
            if req.password:
                user = await c.sign_in(password=req.password)
                asyncio.create_task(start_listening())
                return {"success": True, "user": {"id": user.id, "first_name": getattr(user, "first_name", "")}}
            return {"success": False, "requires_password": True}
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.post("/api/auth/logout")
async def logout():
    global client, listen_task
    try:
        c = await get_client()
        try:
            await c.log_out()
        except Exception:
            pass
        try:
            await c.disconnect()
        except Exception:
            pass
        if listen_task:
            listen_task.cancel()
            listen_task = None
        for ext in ("", ".session", ".session-journal"):
            path = SESSION_FILE + ext
            if os.path.exists(path):
                try:
                    os.remove(path)
                except Exception:
                    pass
        client = None
        return {"success": True}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ─────────────────────────────────────────────────
# PROFILE
# ─────────────────────────────────────────────────
@app.get("/api/profile")
async def get_profile():
    try:
        c    = await get_client()
        me   = await c.get_me()
        data = {
            "id":         me.id,
            "first_name": getattr(me, "first_name", "") or "",
            "last_name":  getattr(me, "last_name",  "") or "",
            "username":   getattr(me, "username",   "") or "",
            "phone":      getattr(me, "phone",      "") or "",
            "avatar":     None,
        }
        me_photo_path = os.path.join(CACHE_DIR, f"profile_{me.id}.jpg")
        if os.path.exists(me_photo_path):
            with open(me_photo_path, "rb") as f:
                photo = f.read()
        else:
            photo = await c.download_profile_photo(me, file=bytes)
            if photo:
                try:
                    with open(me_photo_path, "wb") as f:
                        f.write(photo)
                except Exception:
                    pass
        if photo:
            data["avatar"] = "data:image/jpeg;base64," + base64.b64encode(photo).decode()
        return data
    except Exception as e:
        raise HTTPException(500, str(e))

# ─────────────────────────────────────────────────
# CHATS LIST
# ─────────────────────────────────────────────────
@app.get("/api/chats")
async def get_chats(limit: int = 40):
    global CHATS_CACHE, CHATS_CACHE_TIME
    if CHATS_CACHE and (time.time() - CHATS_CACHE_TIME < 60.0):
        return CHATS_CACHE
    try:
        c      = await get_client()
        result = []
        async for dialog in c.iter_dialogs(limit=limit):
            entity = dialog.entity
            if getattr(entity, 'bot', False):
                continue

            ENTITY_CACHE[dialog.id] = entity
            last_msg_text  = ""
            last_msg_date  = None
            last_msg_media = None
            msg = dialog.message
            if msg:
                last_msg_text = getattr(msg, 'text', '') or ''
                last_msg_date = msg.date.isoformat() if msg.date else None
                media = getattr(msg, 'media', None)
                if media and not isinstance(media, MessageMediaWebPage):
                    if isinstance(media, MessageMediaPhoto):
                        last_msg_media = "photo"
                    elif isinstance(media, MessageMediaDocument):
                        last_msg_media = "document"

            is_forum = getattr(entity, 'forum', False)
            result.append({
                "id":             dialog.id,
                "title":          dialog.name or "Chat",
                "name":           dialog.name or "Chat",
                "unread_count":   dialog.unread_count,
                "is_archived":    dialog.folder_id == 1,
                "is_channel":     dialog.is_channel,
                "is_group":       dialog.is_group,
                "is_forum":       bool(is_forum),
                "last_msg_text":  last_msg_text,
                "last_msg_date":  last_msg_date,
                "last_msg_media": last_msg_media,
                "is_pinned":      getattr(dialog, 'pinned', False),
                "last_message": {
                    "text": last_msg_text or ("[Media]" if last_msg_media else ""),
                    "date": last_msg_date,
                } if last_msg_date else None,
                "avatar": None,
            })
        CHATS_CACHE      = result
        CHATS_CACHE_TIME = time.time()
        return result
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(500, str(e))

# ─────────────────────────────────────────────────
# CHAT AVATAR
# ─────────────────────────────────────────────────
@app.get("/api/chats/{chat_id}/photo")
async def get_chat_photo(chat_id: int):
    cache_path    = os.path.join(CACHE_DIR, f"{chat_id}.jpg")
    no_photo_path = os.path.join(CACHE_DIR, f"{chat_id}.nophoto")

    if os.path.exists(no_photo_path):
        raise HTTPException(404, "No photo (cached)")
    if os.path.exists(cache_path):
        return FileResponse(cache_path, media_type="image/jpeg",
                            headers={"Cache-Control": "public, max-age=86400"})

    if chat_id in currently_downloading:
        for _ in range(10):
            await asyncio.sleep(0.5)
            if os.path.exists(cache_path):
                return FileResponse(cache_path, media_type="image/jpeg",
                                    headers={"Cache-Control": "public, max-age=86400"})
        raise HTTPException(404, "Still downloading")

    currently_downloading.add(chat_id)
    try:
        async with DOWNLOAD_SEMAPHORE:
            c      = await get_client()
            entity = await get_cached_entity(chat_id)
            photo_path = await c.download_profile_photo(entity, file=cache_path, download_big=False)
            if photo_path and os.path.exists(photo_path):
                return FileResponse(photo_path, media_type="image/jpeg",
                                    headers={"Cache-Control": "public, max-age=86400"})
            else:
                open(no_photo_path, "w").close()
                raise HTTPException(404, "No photo")
    except HTTPException:
        raise
    except Exception as e:
        try:
            open(no_photo_path, "w").close()
        except Exception:
            pass
        raise HTTPException(404, f"No photo: {e}")
    finally:
        currently_downloading.discard(chat_id)

# ─────────────────────────────────────────────────
# CHAT INFO
# ─────────────────────────────────────────────────
@app.get("/api/chats/{chat_id}/info")
@app.get("/api/chats/{chat_id}")
async def get_chat_info(chat_id: int):
    cached = CHAT_INFO_CACHE.get(chat_id)
    if cached is not None:
        return cached
    try:
        c      = await get_client()
        entity = await get_cached_entity(chat_id)
        members_count = getattr(entity, 'participants_count', None)
        name = (
            getattr(entity, 'title', '') or
            f"{getattr(entity, 'first_name', '') or ''} {getattr(entity, 'last_name', '') or ''}".strip()
        )
        result = {
            "id":                 chat_id,
            "title":              name,
            "name":               name,
            "members_count":      members_count,
            "participants_count": members_count,
            "is_channel":         getattr(entity, 'broadcast', False),
            "is_forum":           getattr(entity, 'forum', False),
            "is_group":           isinstance(entity, (Chat, Channel)),
            "username":           getattr(entity, 'username', None),
            "phone":              getattr(entity, 'phone', None) if isinstance(entity, User) else None,
            "status":             str(getattr(entity, 'status', '')) if isinstance(entity, User) else None,
            "avatar":             None,
        }
        CHAT_INFO_CACHE[chat_id] = result
        return result
    except Exception as e:
        raise HTTPException(500, str(e))

# ─────────────────────────────────────────────────
# PINNED MESSAGES
# ─────────────────────────────────────────────────
@app.get("/api/chats/{chat_id}/pinned")
async def get_pinned_message(chat_id: int):
    cached = PINNED_CACHE.get(chat_id)
    if cached is not None:
        return cached
    try:
        c      = await get_client()
        entity = await get_cached_entity(chat_id)
        async for msg in c.iter_messages(entity, limit=50):
            if getattr(msg, 'pinned', False):
                result = {"pinned": {"id": msg.id, "text": msg.text or "", "date": msg.date.isoformat() if msg.date else None}}
                PINNED_CACHE[chat_id] = result
                return result
        result = {"pinned": None}
        PINNED_CACHE[chat_id] = result
        return result
    except Exception:
        return {"pinned": None}

# ─────────────────────────────────────────────────
# ARCHIVE / UNARCHIVE / READ
# ─────────────────────────────────────────────────
@app.post("/api/chats/{chat_id}/read")
async def mark_read(chat_id: int):
    try:
        c = await get_client()
        await c.send_read_acknowledge(chat_id)
        return {"status": "ok"}
    except Exception as e:
        raise HTTPException(500, str(e))

@app.post("/api/chats/{chat_id}/archive")
async def archive_chat(chat_id: int):
    try:
        c    = await get_client()
        peer = await c.get_input_entity(chat_id)
        await c(EditPeerFoldersRequest([InputFolderPeer(peer=peer, folder_id=1)]))
        return {"status": "ok"}
    except Exception as e:
        raise HTTPException(500, str(e))

@app.post("/api/chats/{chat_id}/unarchive")
async def unarchive_chat(chat_id: int):
    try:
        c    = await get_client()
        peer = await c.get_input_entity(chat_id)
        await c(EditPeerFoldersRequest([InputFolderPeer(peer=peer, folder_id=0)]))
        return {"status": "ok"}
    except Exception as e:
        raise HTTPException(500, str(e))

# ─────────────────────────────────────────────────
# SEND MESSAGES & FILES
# ─────────────────────────────────────────────────
@app.post("/api/chats/{chat_id}/messages")
async def send_message_to_chat(chat_id: int, req: SendMessageRequest):
    try:
        c      = await get_client()
        msg    = await c.send_message(chat_id, req.text)
        entity = await get_cached_entity(chat_id)
        return await build_message_dict(msg, entity)
    except Exception as e:
        raise HTTPException(500, str(e))

@app.post("/api/messages/send")
async def send_message_friend(req: SendMessageRequestFriend):
    try:
        c      = await get_client()
        entity = await get_cached_entity(req.chat_id)
        msg    = await c.send_message(entity, req.text, reply_to=req.reply_to)
        return await build_message_dict(msg, entity)
    except Exception as e:
        raise HTTPException(500, str(e))

@app.post("/api/chats/{chat_id}/upload")
async def upload_file_to_chat(chat_id: int, file: UploadFile = File(...)):
    try:
        c       = await get_client()
        content = await file.read()
        buffer  = io.BytesIO(content)
        buffer.name = file.filename or "upload.bin"
        buffer.seek(0)
        msg    = await c.send_file(chat_id, buffer, caption="", force_document=True)
        entity = await get_cached_entity(chat_id)
        return await build_message_dict(msg, entity)
    except Exception as e:
        raise HTTPException(500, str(e))

@app.post("/api/messages/send_file")
async def send_file_friend(
    chat_id: int      = Form(...),
    caption: str      = Form(""),
    file:    UploadFile = File(...),
):
    try:
        c       = await get_client()
        content = await file.read()
        buffer  = io.BytesIO(content)
        buffer.name = file.filename or "upload.bin"
        msg    = await c.send_file(chat_id, buffer, caption=caption, force_document=True)
        entity = await get_cached_entity(chat_id)
        return await build_message_dict(msg, entity)
    except Exception as e:
        raise HTTPException(500, str(e))

# ─────────────────────────────────────────────────
# DELETE HISTORY / MESSAGE
# ─────────────────────────────────────────────────
@app.post("/api/chats/{chat_id}/delete")
async def delete_chat_history(chat_id: int):
    try:
        c    = await get_client()
        peer = await c.get_input_entity(chat_id)
        deleted = False
        for revoke, just_clear in [(True, False), (False, True)]:
            try:
                await c(DeleteHistoryRequest(peer=peer, max_id=0, just_clear=just_clear, revoke=revoke))
                deleted = True
                break
            except Exception:
                continue
        if not deleted:
            raise HTTPException(500, "Could not delete history")
        ENTITY_CACHE.pop(chat_id, None)
        return {"status": "ok"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, str(e))

@app.delete("/api/messages/{message_id}")
async def delete_single_message(message_id: int, req: DeleteMessageRequest):
    try:
        c = await get_client()
        await c.delete_messages(req.chat_id, [message_id])
        return {"success": True}
    except Exception as e:
        raise HTTPException(500, str(e))

# ─────────────────────────────────────────────────
# TOPICS / FORUMS
# ─────────────────────────────────────────────────
@app.get("/api/chats/{chat_id}/topics")
async def get_topics(chat_id: int):
    try:
        c        = await get_client()
        entity   = await get_cached_entity(chat_id)
        is_forum = getattr(entity, 'forum', False)
        if not is_forum:
            return {"is_forum": False, "topics": []}
        if not HAS_FORUM_SUPPORT:
            return {"is_forum": True, "topics": [], "error": "Telethon version lacks Forum support"}
        result = await c(GetForumTopicsRequest(
            peer=entity, offset_date=None, offset_id=0, offset_topic=0, limit=100, q=None
        ))
        topics = []
        for t in result.topics:
            topics.append({
                "id":           t.id,
                "title":        t.title,
                "unread_count": getattr(t, 'unread_count', 0),
                "top_message":  getattr(t, 'top_message', 0),
                "is_pinned":    getattr(t, 'pinned', False),
                "is_closed":    getattr(t, 'closed', False),
                "is_general":   t.id == 1,
            })
        topics.sort(key=lambda t: (not t['is_general'], not t['is_pinned'], t['id']))
        return {"is_forum": True, "topics": topics}
    except Exception as e:
        traceback.print_exc()
        return {"is_forum": False, "topics": [], "error": str(e)}

@app.get("/api/chats/{chat_id}/topics/{topic_id}/messages")
async def get_topic_messages(
    chat_id:     int,
    topic_id:    int,
    limit:       int = Query(40),
    offset_id:   int = Query(0),
    search:      Optional[str] = Query(None),
    filter_type: str = Query("all"),
):
    try:
        c      = await get_client()
        entity = await get_cached_entity(chat_id)
        msg_filter = None
        if filter_type == "files":   msg_filter = InputMessagesFilterDocument()
        elif filter_type == "media": msg_filter = InputMessagesFilterPhotos()
        elif filter_type == "video": msg_filter = InputMessagesFilterVideo()
        elif filter_type == "voice": msg_filter = InputMessagesFilterVoice()

        kwargs: dict = {"limit": limit, "offset_id": offset_id, "reply_to": topic_id}
        if search:     kwargs["search"] = search
        if msg_filter: kwargs["filter"] = msg_filter

        raw_msgs = []
        async with _tg_lock:
            async for msg in c.iter_messages(entity, **kwargs):
                raw_msgs.append(msg)
        await batch_resolve_senders(raw_msgs)
        out = []
        for message in raw_msgs:
            if not hasattr(message, 'date') or not message.date:
                continue
            d = await build_message_dict(message, entity)
            d["topic_id"] = topic_id
            out.append(d)
        out.sort(key=lambda m: m["id"])
        return out
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(500, str(e))

@app.get("/api/chats/{chat_id}/topics/{topic_id}/pinned")
async def get_topic_pinned(chat_id: int, topic_id: int):
    try:
        c      = await get_client()
        entity = await get_cached_entity(chat_id)
        async for msg in c.iter_messages(entity, limit=50, reply_to=topic_id):
            if getattr(msg, 'pinned', False):
                return {"pinned": {"id": msg.id, "text": msg.text or "", "date": msg.date.isoformat() if msg.date else None}}
        return {"pinned": None}
    except Exception:
        return {"pinned": None}

# ─────────────────────────────────────────────────
# MESSAGES
# ─────────────────────────────────────────────────
@app.get("/api/messages/{chat_id}")
@app.get("/api/chats/{chat_id}/messages")
async def get_messages(
    chat_id:     int,
    limit:       int = Query(40),
    offset_id:   int = Query(0),
    topic_id:    Optional[int] = Query(None),
    search:      Optional[str] = Query(None),
    filter_type: str = Query("all"),
):
    try:
        msg_filter = None
        if filter_type == "files":   msg_filter = InputMessagesFilterDocument()
        elif filter_type == "media": msg_filter = InputMessagesFilterPhotos()
        elif filter_type == "video": msg_filter = InputMessagesFilterVideo()
        elif filter_type == "voice": msg_filter = InputMessagesFilterVoice()

        c      = await get_client()
        entity = await get_cached_entity(chat_id)
        kwargs: dict = {"limit": limit, "offset_id": offset_id}
        if msg_filter: kwargs["filter"] = msg_filter
        if search:     kwargs["search"] = search
        if topic_id:   kwargs["reply_to"] = topic_id

        raw_msgs = []
        async with _tg_lock:
            async for msg in c.iter_messages(entity, **kwargs):
                raw_msgs.append(msg)
        await batch_resolve_senders(raw_msgs)
        tasks = [build_message_dict(msg, entity) for msg in raw_msgs if hasattr(msg, 'date') and msg.date]
        out   = await asyncio.gather(*tasks) if tasks else []
        out   = list(out)
        out.sort(key=lambda m: m["id"])
        return out
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(500, str(e))

@app.get("/api/chats/{chat_id}/messages/{message_id}")
async def get_single_message(chat_id: int, message_id: int):
    try:
        c       = await get_client()
        entity  = await get_cached_entity(chat_id)
        message = await c.get_messages(entity, ids=message_id)
        if not message:
            raise HTTPException(404, "Message not found")
        sender_name = await get_sender_name(message, entity)
        media_info  = extract_media_info(message)
        return {
            "id":          message.id,
            "text":        message.text or "",
            "sender_name": sender_name,
            "date":        message.date.isoformat() if message.date else None,
            "has_media":   media_info["has_media"],
            "media_type":  media_info["media_type"],
            "file_name":   media_info["file_name"],
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, str(e))

# ─────────────────────────────────────────────────
# RESOLVE PEER
# ─────────────────────────────────────────────────
@app.get("/api/resolve/{peer}")
async def resolve_peer(peer: str):
    try:
        if peer.startswith("c_"):
            raw_id  = int(peer[2:])
            chat_id = int(f"-100{raw_id}")
            return {"chat_id": chat_id}
        c      = await get_client()
        entity = await get_cached_entity(peer)
        return {"chat_id": entity.id}
    except Exception as e:
        try:
            return {"chat_id": int(peer)}
        except ValueError:
            raise HTTPException(404, f"Could not resolve: {e}")

# ─────────────────────────────────────────────────
# MEDIA GALLERY
# ─────────────────────────────────────────────────
@app.get("/api/chats/{chat_id}/gallery")
async def get_media_gallery(
    chat_id:    int,
    limit:      int = Query(12),
    offset_id:  int = Query(0),
    media_type: str = Query("photo"),
    topic_id:   Optional[int] = Query(None),
):
    try:
        c      = await get_client()
        entity = await get_cached_entity(chat_id)
        f      = InputMessagesFilterVideo() if media_type == "video" else InputMessagesFilterPhotos()
        kwargs: dict = {"limit": limit, "offset_id": offset_id, "filter": f}
        if topic_id:
            kwargs["reply_to"] = topic_id
        items = []
        async with _tg_lock:
            async for msg in c.iter_messages(entity, **kwargs):
                info = extract_media_info(msg)
                if info["has_media"]:
                    items.append({
                        "id":         msg.id,
                        "date":       msg.date.isoformat() if msg.date else None,
                        "media_type": info["media_type"],
                        "mime_type":  info["mime_type"],
                        "file_size":  info["file_size"],
                        "width":      info["width"],
                        "height":     info["height"],
                        "duration":   info["duration"],
                    })
        return items
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(500, str(e))

# ─────────────────────────────────────────────────
# MESSAGES BY DATE
# ─────────────────────────────────────────────────
@app.get("/api/chats/{chat_id}/messages_by_date")
async def get_messages_by_date(
    chat_id:  int,
    date:     str = Query(...),
    limit:    int = Query(40),
    topic_id: Optional[int] = Query(None),
):
    try:
        from datetime import datetime, timezone
        c      = await get_client()
        entity = await get_cached_entity(chat_id)
        target = datetime.strptime(date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        kwargs: dict = {"limit": limit, "offset_date": target, "reverse": False}
        if topic_id:
            kwargs["reply_to"] = topic_id
        raw_msgs = []
        async with _tg_lock:
            async for msg in c.iter_messages(entity, **kwargs):
                raw_msgs.append(msg)
        await batch_resolve_senders(raw_msgs)
        out = []
        for message in raw_msgs:
            if not hasattr(message, 'date') or not message.date:
                continue
            out.append(await build_message_dict(message, entity))
        out.sort(key=lambda m: m["id"])
        return out
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(500, str(e))

# ─────────────────────────────────────────────────
# RANGE MEDIA STREAM
# ─────────────────────────────────────────────────
STREAM_CHUNK_SIZE = 524288

async def direct_stream_generator(c, file_to_download, start: int, end: int):
    if end < start:
        return
    length      = end - start + 1
    bytes_sent  = 0
    aligned_off = (start // STREAM_CHUNK_SIZE) * STREAM_CHUNK_SIZE
    skip_bytes  = start - aligned_off
    try:
        async for chunk in c.iter_download(file_to_download, offset=aligned_off, request_size=STREAM_CHUNK_SIZE):
            if skip_bytes > 0:
                if skip_bytes >= len(chunk):
                    skip_bytes -= len(chunk)
                    continue
                chunk      = chunk[skip_bytes:]
                skip_bytes = 0
            remaining = length - bytes_sent
            if len(chunk) > remaining:
                chunk = chunk[:remaining]
            yield chunk
            bytes_sent += len(chunk)
            if bytes_sent >= length:
                break
    except Exception as e:
        print(f"❌ [Stream Error] {e}")
        return

@app.get("/api/media/{chat_id}/{message_id}")
async def get_media(chat_id: int, message_id: int, request: Request):
    try:
        c       = await get_client()
        entity  = await get_cached_entity(chat_id)
        message = await c.get_messages(entity, ids=message_id)
        if not message or not message.media:
            raise HTTPException(404, "Media not found")
        if isinstance(message.media, MessageMediaWebPage):
            raise HTTPException(404, "No downloadable media")

        info      = extract_media_info(message)
        mime_type = info["mime_type"] or "application/octet-stream"
        file_name = info["file_name"] or f"file_{message_id}"
        file_size = info["file_size"] or 0
        encoded   = quote(file_name)

        file_to_download = message.media
        if isinstance(message.media, MessageMediaPhoto):
            file_to_download = message.media.photo
        elif isinstance(message.media, MessageMediaDocument):
            file_to_download = message.media.document

        range_hdr  = request.headers.get("range")
        start_byte = 0
        end_byte   = file_size - 1 if file_size > 0 else 0
        use_range  = bool(range_hdr and file_size > 0)

        if use_range:
            try:
                parts = range_hdr.replace("bytes=", "").split("-")
                if parts[0]:
                    start_byte = int(parts[0])
                if len(parts) > 1 and parts[1]:
                    end_byte = int(parts[1])
                else:
                    end_byte = file_size - 1
            except ValueError:
                start_byte = 0
                end_byte   = file_size - 1

        limit_bytes = end_byte - start_byte + 1 if (use_range and end_byte >= start_byte) else 0

        headers = {
            "Accept-Ranges":       "bytes",
            "Content-Disposition": f"inline; filename*=UTF-8''{encoded}",
            "Cache-Control":       "public, max-age=86400",
        }

        # Photos and small files: buffer fully
        if isinstance(message.media, MessageMediaPhoto) or not use_range:
            photo_bytes = io.BytesIO()
            await c.download_media(message, file=photo_bytes)
            photo_bytes.seek(0)
            data = photo_bytes.read()
            hdrs = {
                "Content-Disposition": f"inline; filename*=UTF-8''{encoded}",
                "Cache-Control":       "public, max-age=86400",
                "Content-Length":      str(len(data)),
                "Accept-Ranges":       "bytes",
            }
            return StreamingResponse(io.BytesIO(data), status_code=200, headers=hdrs, media_type=mime_type)

        if use_range and limit_bytes > 0:
            headers["Content-Length"] = str(limit_bytes)
            headers["Content-Range"]  = f"bytes {start_byte}-{end_byte}/{file_size}"

        return StreamingResponse(
            direct_stream_generator(c, file_to_download, start_byte, end_byte if end_byte >= start_byte else start_byte),
            status_code=206 if use_range else 200,
            headers=headers,
            media_type=mime_type,
        )
    except HTTPException:
        raise
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(500, str(e))

# ─────────────────────────────────────────────────
# WEBSOCKET
# ─────────────────────────────────────────────────
@app.websocket("/ws/messages")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            data = await websocket.receive_text()
            try:
                msg = json.loads(data)
                if msg.get("type") == "ping":
                    await websocket.send_json({"type": "pong"})
            except Exception:
                pass
    except WebSocketDisconnect:
        manager.disconnect(websocket)

# ─────────────────────────────────────────────────
# DEBUG
# ─────────────────────────────────────────────────
@app.get("/api/debug/cache")
async def debug_cache():
    return {
        "entity_cache_size":    len(ENTITY_CACHE.cache),
        "sender_cache_size":    len(GLOBAL_SENDER_CACHE.cache),
        "chat_info_cache_size": len(CHAT_INFO_CACHE.cache),
    }

# ─────────────────────────────────────────────────
# SERVE FRONTEND
# ─────────────────────────────────────────────────
@app.get("/")
async def serve_index():
    index_path = os.path.join(current_dir, "index.html")
    if os.path.exists(index_path):
        return FileResponse(index_path)
    raise HTTPException(404, "index.html not found")

# ─────────────────────────────────────────────────
# ENTRYPOINT
# ─────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=11619, reload=False)