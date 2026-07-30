import asyncio
import base64
import logging
import os
import random
import re
import signal
import struct
import time
from datetime import datetime, timezone
from typing import Any

import aiohttp
import aiohttp.web
from dotenv import load_dotenv
from pyrogram import Client

try:
    from motor.motor_asyncio import AsyncIOMotorClient
except Exception:
    AsyncIOMotorClient = None


load_dotenv()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("viralbot")


# ===== CONFIG =====
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
OWNER_CHAT_ID = int(os.getenv("OWNER_CHAT_ID", "0") or 0)


def parse_owner_ids() -> set[int]:
    ids: set[int] = set()
    if OWNER_CHAT_ID:
        ids.add(OWNER_CHAT_ID)
    raw = os.getenv("OWNER_CHAT_IDS", "").strip()
    for part in re.split(r"[,;\s]+", raw):
        part = part.strip()
        if part.lstrip("-").isdigit():
            ids.add(int(part))
    return ids


OWNER_CHAT_IDS = parse_owner_ids()
API_ID = int(os.getenv("API_ID", "0") or 0)
API_HASH = os.getenv("API_HASH", "").strip()

# Kept in .env for older Pyrogram bots, but this bot intentionally does not use
# API_ID/API_HASH or create a Telegram session file.
LIBRARY_TELEGRAM_CHANNEL = os.getenv("LIBRARY_TELEGRAM_CHANNEL", "").strip()
FORWARD_TEXT_CHANNEL = os.getenv("FORWARD_TEXT_CHANNEL", "").strip()

MONGO_URI = os.getenv("MONGO_URI", "").strip()
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "viralbots").strip()
MONGO_USERS_COLLECTION = os.getenv("MONGO_USERS_COLLECTION", "multi_bot_users").strip()
MONGO_CHANNELS_COLLECTION = os.getenv("MONGO_CHANNELS_COLLECTION", "channel_state").strip()

BOT_KEY = os.getenv("BOT_KEY", "").strip().lstrip("@")
BOT_KEY_SAFE = ""
BOT_USERNAME = ""
BOT_ID = 0

# Optional message id for the /start copied post. If empty, the bot uses the
# latest post id it has seen from FORWARD_TEXT_CHANNEL.
START_POST_MESSAGE_ID = int(os.getenv("START_POST_MESSAGE_ID", "0") or 0)

# Optional high-water mark for old libraries. Use this if the bot was added
# after your channel already had many posts, for example 20000.
LIBRARY_MAX_MESSAGE_ID = int(os.getenv("LIBRARY_MAX_MESSAGE_ID", "0") or 0)
RANDOM_VIDEO_RETRIES = int(os.getenv("RANDOM_VIDEO_RETRIES", "35") or 35)

POLL_TIMEOUT_SECONDS = int(os.getenv("POLL_TIMEOUT_SECONDS", "25") or 25)
DROP_PENDING_UPDATES = os.getenv("DROP_PENDING_UPDATES", "false").lower() in {"1", "true", "yes", "on"}

WATCH_VIDEO_TEXT = os.getenv("WATCH_VIDEO_TEXT", "🔞 Watch Now").strip() or "🎬 Watch Now"
WATCH_NEXT_TEXT = os.getenv("WATCH_NEXT_TEXT", "👙 Watch Next").strip() or "⏭ Watch Next"
JOIN_CHANNELS_TEXT = os.getenv("JOIN_CHANNELS_TEXT", "Join VIRALS").strip() or "📢 Join Viral Channel"
TUTORIAL_URL = os.getenv("TUTORIAL_URL", "https://t.me/howdisk/2").strip()
TRENDING_URL = os.getenv("TRENDING_URL", "bitly.cx/diskwala").strip()

EARN_CHANNEL_TEXT = os.getenv("EARN_CHANNEL_TEXT", "💰 Earning Channel").strip() or "💰 Earning Channel"
EARN_CHANNEL_URL = os.getenv("EARN_CHANNEL_URL", "https://t.me/+-WC_czgycbM5M2Rl").strip()


mongo_client = None
mongo_db = None
users_col = None
channels_col = None
mtproto_app: Client | None = None
memory_users: set[int] = set()
memory_channel_state: dict[str, int] = {}
broadcast_jobs: dict[str, dict[str, Any]] = {}


class TelegramAPIError(Exception):
    def __init__(self, method: str, status: int, description: str, parameters: dict | None = None):
        super().__init__(description)
        self.method = method
        self.status = status
        self.description = description
        self.parameters = parameters or {}

    @property
    def retry_after(self) -> int:
        return int(self.parameters.get("retry_after") or 0)


# Methods that don't send anything to a chat (or, for getUpdates, block on Telegram's
# own long-poll) and so should bypass send throttling entirely.
UNTHROTTLED_METHODS = {"getUpdates", "getMe", "deleteWebhook", "getChatMember"}

# Telegram's real caps are ~30 msg/sec bot-wide and ~1 msg/sec per chat. Stay a bit
# under both so we pace requests up front instead of tripping 429s and paying
# whatever retry_after Telegram hands back.
GLOBAL_RATE_PER_SEC = 25
GLOBAL_BUCKET_CAPACITY = 20
PER_CHAT_MIN_INTERVAL = 1.05


class _TokenBucket:
    def __init__(self, rate: float, capacity: float):
        self.rate = rate
        self.capacity = capacity
        self.tokens = capacity
        self.updated = time.monotonic()
        self.lock = asyncio.Lock()

    async def acquire(self) -> None:
        async with self.lock:
            while True:
                now = time.monotonic()
                self.tokens = min(self.capacity, self.tokens + (now - self.updated) * self.rate)
                self.updated = now
                if self.tokens >= 1:
                    self.tokens -= 1
                    return
                await asyncio.sleep((1 - self.tokens) / self.rate)


class TelegramBotAPI:
    def __init__(self, token: str):
        self.base_url = f"https://api.telegram.org/bot{token}"
        self.session: aiohttp.ClientSession | None = None
        self._global_bucket = _TokenBucket(GLOBAL_RATE_PER_SEC, GLOBAL_BUCKET_CAPACITY)
        self._chat_last_sent: dict[str, float] = {}
        self._chat_locks: dict[str, asyncio.Lock] = {}

    async def open(self) -> None:
        self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=120))

    async def close(self) -> None:
        if self.session:
            await self.session.close()

    async def _throttle(self, method: str, payload: dict | None) -> None:
        if method in UNTHROTTLED_METHODS:
            return
        await self._global_bucket.acquire()

        chat_id = (payload or {}).get("chat_id")
        if chat_id is None:
            return
        key = str(chat_id)
        lock = self._chat_locks.setdefault(key, asyncio.Lock())
        async with lock:
            now = time.monotonic()
            wait = PER_CHAT_MIN_INTERVAL - (now - self._chat_last_sent.get(key, 0.0))
            if wait > 0:
                await asyncio.sleep(wait)
            self._chat_last_sent[key] = time.monotonic()

    async def request(self, method: str, payload: dict | None = None, _retries: int = 3) -> Any:
        if self.session is None:
            raise RuntimeError("Telegram session is not open")

        for attempt in range(_retries + 1):
            await self._throttle(method, payload)
            async with self.session.post(f"{self.base_url}/{method}", json=payload or {}) as resp:
                data = await resp.json(content_type=None)
                if data.get("ok"):
                    return data.get("result")
                exc = TelegramAPIError(
                    method=method,
                    status=int(data.get("error_code") or resp.status),
                    description=str(data.get("description") or "Telegram API error"),
                    parameters=data.get("parameters") or {},
                )
            if exc.status == 429 and exc.retry_after and attempt < _retries:
                logger.info("Rate limited on %s, retrying after %ss", method, exc.retry_after)
                await asyncio.sleep(exc.retry_after)
                continue
            raise exc


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_chat_id(raw: str) -> int | str:
    value = (raw or "").strip()
    if value.startswith("-") and value[1:].isdigit():
        return int(value)
    if value.isdigit():
        return int(value)
    return value


def chat_key(chat_id: int | str) -> str:
    return str(chat_id).strip()


def safe_field(value: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9_]", "_", value.strip().lstrip("@"))
    return cleaned or "unknown_bot"


def command_name(text: str) -> str:
    first = (text or "").split(maxsplit=1)[0].lower()
    if not first.startswith("/"):
        return ""
    return first[1:].split("@", 1)[0]


def command_payload(text: str) -> str:
    parts = (text or "").split(maxsplit=1)
    return parts[1].strip() if len(parts) > 1 else ""


def user_from_message(message: dict) -> dict:
    return message.get("from") or {}


def extract_diskwala_link(text: str) -> str:
    match = re.search(r"(?:https?://)?(?:www\.)?diskwala\.com/[^\s<>)\]]+", text or "", flags=re.IGNORECASE)
    if not match:
        return ""
    link = match.group(0).rstrip(".,;:!?)\"]'")
    if not link.lower().startswith(("http://", "https://")):
        link = f"https://{link}"
    return link


def message_has_media(message: dict) -> bool:
    return any(
        key in message
        for key in (
            "photo",
            "video",
            "animation",
            "document",
            "audio",
            "voice",
            "video_note",
            "sticker",
        )
    )


def pyrogram_message_has_media(message) -> bool:
    return bool(getattr(message, "media", None))


def video_caption(diskwala_link: str) -> str:
    tutorial = TUTORIAL_URL or "https://t.me/howdisk/2"
    trending = TRENDING_URL or "bitly.cx/diskwala"
    return (
        "🎬 <b>Vdo 😍</b>\n\n"
        "<b>🔗🔗यह रहा वीडियो लिंक 👇</b>\n\n"
        f"{html_escape(diskwala_link)}\n\n"
        "<b>🤔 How to Open Links? | लिंक कैसे खोलें 👇</b>\n"
        f'<a href="{html_escape(tutorial)}">📖 View Tutorial</a>\n\n'
        "😉<b>Daily Trending. Open 👇</b>\n"
        f"{html_escape(trending)}"
    )


def html_escape(value: str) -> str:
    return (
        (value or "")
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def chat_matches_config(chat: dict, configured: str) -> bool:
    target = parse_chat_id(configured)
    chat_id = chat.get("id")
    username = (chat.get("username") or "").lstrip("@").lower()
    if isinstance(target, int):
        return int(chat_id or 0) == target
    target_text = str(target).strip().lstrip("@").lower()
    return bool(username and username == target_text)


def parse_join_channels() -> list[dict[str, str]]:
    """
    JOIN_CHANNELS examples:
      @mychannel
      Movies|https://t.me/mychannel|@mychannel
      Movies|https://t.me/+inviteLink|-1001234567890

    The 3rd value is optional but needed for join checking when the URL is an
    invite link. You can also use JOIN_CHANNEL_1, JOIN_CHANNEL_2, ...
    """
    raw_items: list[str] = []
    if os.getenv("JOIN_CHANNELS", "").strip():
        raw_items.extend(x.strip() for x in os.getenv("JOIN_CHANNELS", "").split(";") if x.strip())

    for idx in range(1, 21):
        item = os.getenv(f"JOIN_CHANNEL_{idx}", "").strip()
        if item:
            raw_items.append(item)

    channels: list[dict[str, str]] = []
    for item in raw_items:
        parts = [p.strip() for p in item.split("|") if p.strip()]
        if not parts:
            continue

        if len(parts) == 1 and parts[0].startswith("@"):
            username = parts[0].lstrip("@")
            channels.append({
                "title": JOIN_CHANNELS_TEXT,
                "url": f"https://t.me/{username}",
                "check_chat": f"@{username}",
            })
        elif len(parts) == 1 and parts[0].startswith("http"):
            channels.append({"title": JOIN_CHANNELS_TEXT, "url": parts[0], "check_chat": ""})
        else:
            title = parts[0]
            url = parts[1] if len(parts) > 1 else ""
            check_chat = parts[2] if len(parts) > 2 else ""
            channels.append({"title": title, "url": url, "check_chat": check_chat})

    return channels


JOIN_CHANNELS = parse_join_channels()


def video_action_keyboard(action_text: str) -> dict:
    rows = [[{"text": action_text, "callback_data": "video"}]]
    if EARN_CHANNEL_URL:
        rows.append([{"text": EARN_CHANNEL_TEXT, "url": EARN_CHANNEL_URL}])
    return {"inline_keyboard": rows}


def start_keyboard() -> dict:
    return video_action_keyboard(WATCH_VIDEO_TEXT)


def video_keyboard() -> dict:
    return video_action_keyboard(WATCH_NEXT_TEXT)


def main_reply_keyboard() -> dict:
    return {
        "keyboard": [[
            {"text": JOIN_CHANNELS_TEXT},
            {"text": WATCH_VIDEO_TEXT},
        ],[
            {"text": JOIN_CHANNELS_TEXT},
            {"text": WATCH_VIDEO_TEXT},
        ],[
            {"text": JOIN_CHANNELS_TEXT},
            {"text": WATCH_VIDEO_TEXT},
        ], [
            {"text": EARN_CHANNEL_TEXT},
        ]],
        "resize_keyboard": True,
        "is_persistent": True,
    }


def cancel_keyboard(job_id: str) -> dict:
    return {"inline_keyboard": [[{"text": "🛑 Cancel Broadcast", "callback_data": f"cancel_broadcast:{job_id}"}]]}


async def init_mongo() -> None:
    global mongo_client, mongo_db, users_col, channels_col
    if not MONGO_URI:
        logger.warning("MONGO_URI is empty. Users will be kept only in memory.")
        return
    if AsyncIOMotorClient is None:
        logger.warning("motor is not installed. Users will be kept only in memory.")
        return

    mongo_client = AsyncIOMotorClient(MONGO_URI)
    mongo_db = mongo_client[MONGO_DB_NAME]
    users_col = mongo_db[MONGO_USERS_COLLECTION]
    channels_col = mongo_db[MONGO_CHANNELS_COLLECTION]
    await users_col.create_index("user_id", unique=True)
    await users_col.create_index(f"bots.{BOT_KEY_SAFE}.last_seen_at")
    await users_col.create_index("bot_keys")
    logger.info("MongoDB connected: %s.%s", MONGO_DB_NAME, MONGO_USERS_COLLECTION)


async def save_user(user: dict) -> None:
    if not user:
        return
    user_id = int(user["id"])
    if users_col is None:
        memory_users.add(user_id)
        return

    now = utc_now()
    await users_col.update_one(
        {"user_id": user_id},
        {
            "$set": {
                "user_id": user_id,
                "username": user.get("username"),
                "first_name": user.get("first_name"),
                "last_name": user.get("last_name"),
                "updated_at": now,
                f"bots.{BOT_KEY_SAFE}.bot_key": BOT_KEY,
                f"bots.{BOT_KEY_SAFE}.bot_username": BOT_USERNAME,
                f"bots.{BOT_KEY_SAFE}.bot_id": BOT_ID,
                f"bots.{BOT_KEY_SAFE}.last_seen_at": now,
            },
            "$setOnInsert": {"created_at": now},
            "$addToSet": {"bot_keys": BOT_KEY},
        },
        upsert=True,
    )


async def remove_user_for_this_bot(user_id: int) -> None:
    if users_col is None:
        memory_users.discard(int(user_id))
        return

    await users_col.update_one(
        {"user_id": int(user_id)},
        {
            "$unset": {f"bots.{BOT_KEY_SAFE}": ""},
            "$pull": {"bot_keys": BOT_KEY},
            "$set": {"updated_at": utc_now()},
        },
    )
    doc = await users_col.find_one({"user_id": int(user_id)}, {"bot_keys": 1})
    if doc and not doc.get("bot_keys"):
        await users_col.delete_one({"user_id": int(user_id)})


async def count_users_for_this_bot() -> int:
    if users_col is None:
        return len(memory_users)
    return await users_col.count_documents({f"bots.{BOT_KEY_SAFE}.last_seen_at": {"$exists": True}})


async def iter_users_for_this_bot():
    if users_col is None:
        for user_id in list(memory_users):
            yield user_id
        return

    query = {f"bots.{BOT_KEY_SAFE}.last_seen_at": {"$exists": True}}
    page_size = 500
    last_id = None
    while True:
        page_query = dict(query)
        if last_id is not None:
            page_query["_id"] = {"$gt": last_id}
        docs = await (
            users_col.find(page_query, {"_id": 1, "user_id": 1})
            .sort("_id", 1)
            .limit(page_size)
            .to_list(length=page_size)
        )
        if not docs:
            return
        for doc in docs:
            last_id = doc["_id"]
            yield int(doc["user_id"])
        if len(docs) < page_size:
            return


async def set_channel_last_message(chat_id: int | str, message_id: int, username: str = "") -> None:
    keys = {chat_key(chat_id)}
    if username:
        keys.add(f"@{username.lstrip('@')}")

    if channels_col is None:
        for key in keys:
            memory_channel_state[key] = max(int(message_id), int(memory_channel_state.get(key, 0)))
        return

    now = utc_now()
    for key in keys:
        await channels_col.update_one(
            {"_id": key},
            {
                "$max": {"last_message_id": int(message_id)},
                "$set": {"updated_at": now, "chat_id": chat_key(chat_id), "username": username or None},
            },
            upsert=True,
        )


async def get_channel_last_message(chat_id: int | str) -> int:
    key = chat_key(chat_id)
    if channels_col is None:
        return int(memory_channel_state.get(key, 0))

    doc = await channels_col.find_one({"_id": key}, {"last_message_id": 1})
    return int((doc or {}).get("last_message_id") or 0)


async def get_max_library_message_id() -> int:
    if LIBRARY_MAX_MESSAGE_ID > 0:
        return LIBRARY_MAX_MESSAGE_ID
    return await get_channel_last_message(parse_chat_id(LIBRARY_TELEGRAM_CHANNEL))


async def set_library_max_message_id(message_id: int) -> None:
    await set_channel_last_message(parse_chat_id(LIBRARY_TELEGRAM_CHANNEL), int(message_id))


async def user_joined_required_channels(api: TelegramBotAPI, user_id: int) -> bool:
    check_channels = [c for c in JOIN_CHANNELS if c.get("check_chat")]
    if not check_channels:
        return True

    for channel in check_channels:
        try:
            member = await api.request("getChatMember", {
                "chat_id": parse_chat_id(channel["check_chat"]),
                "user_id": user_id,
            })
            if member.get("status") in {"left", "kicked"}:
                return False
        except TelegramAPIError as exc:
            logger.warning("Join check failed for %s: %s", channel["check_chat"], exc.description)
            return False
    return True


async def send_earn_channel(api: TelegramBotAPI, chat_id: int) -> None:
    if not EARN_CHANNEL_URL:
        return
    await api.request("sendMessage", {
        "chat_id": chat_id,
        "text": "💰 Join our earning channel:",
        "reply_markup": {"inline_keyboard": [[{"text": EARN_CHANNEL_TEXT, "url": EARN_CHANNEL_URL}]]},
    })


async def send_join_prompt(api: TelegramBotAPI, chat_id: int) -> None:
    await api.request("sendMessage", {
        "chat_id": chat_id,
        "text": "🔒 Please join the channel first, then tap Watch Video.",
        "reply_markup": start_keyboard(),
    })


async def send_main_reply_keyboard(api: TelegramBotAPI, chat_id: int) -> None:
    await api.request("sendMessage", {
        "chat_id": chat_id,
        "text": "👇 Menu",
        "reply_markup": main_reply_keyboard(),
    })


async def send_start(api: TelegramBotAPI, message: dict) -> None:
    user = user_from_message(message)
    chat_id = int(message["chat"]["id"])
    await save_user(user)

    if command_payload(message.get("text") or "").lower() == "video":
        await send_main_reply_keyboard(api, chat_id)
        await send_random_video(api, chat_id, user)
        return

    start_message_id = START_POST_MESSAGE_ID or await get_channel_last_message(parse_chat_id(FORWARD_TEXT_CHANNEL))
    if FORWARD_TEXT_CHANNEL and start_message_id:
        try:
            await api.request("copyMessage", {
                "chat_id": chat_id,
                "from_chat_id": parse_chat_id(FORWARD_TEXT_CHANNEL),
                "message_id": int(start_message_id),
                "reply_markup": start_keyboard(),
            })
            await send_main_reply_keyboard(api, chat_id)
            return
        except TelegramAPIError as exc:
            logger.warning("Could not copy /start post: %s", exc.description)

    await api.request("sendMessage", {
        "chat_id": chat_id,
        "text": "👋 Welcome!\n\nJoin the channel and tap Watch Video to get a random Diskwala link.",
        "reply_markup": start_keyboard(),
    })
    await send_main_reply_keyboard(api, chat_id)


def is_retryable_missing_message(exc: TelegramAPIError) -> bool:
    text = exc.description.lower()
    return (
        "message to copy not found" in text
        or "message_id_invalid" in text
        or "message identifier is not specified" in text
        or "message not found" in text
    )


async def send_typing_action(api: TelegramBotAPI, chat_id: int) -> None:
    try:
        await api.request("sendChatAction", {"chat_id": chat_id, "action": "typing"}, _retries=0)
    except TelegramAPIError:
        pass


async def send_random_video(api: TelegramBotAPI, chat_id: int, user: dict | None = None) -> None:
    if user:
        await save_user(user)

    if not await user_joined_required_channels(api, int(chat_id)):
        await send_join_prompt(api, int(chat_id))
        return

    if not LIBRARY_TELEGRAM_CHANNEL:
        await api.request("sendMessage", {
            "chat_id": chat_id,
            "text": "⚠️ LIBRARY_TELEGRAM_CHANNEL is not configured.",
        })
        return

    if mtproto_app is None:
        await api.request("sendMessage", {
            "chat_id": chat_id,
            "text": "⚠️ Telegram reader is not ready. Please try again in a moment.",
        })
        return

    max_message_id = await get_max_library_message_id()
    if max_message_id <= 0:
        await api.request("sendMessage", {
            "chat_id": chat_id,
            "text": "⚠️ Library max id is missing. Admin can set it with /setmax 20000.",
        })
        return

    asyncio.create_task(send_typing_action(api, chat_id))

    for _ in range(max(1, RANDOM_VIDEO_RETRIES)):
        message_id = random.randint(1, max_message_id)
        try:
            source_message = await mtproto_app.get_messages(parse_chat_id(LIBRARY_TELEGRAM_CHANNEL), message_id)
            if not source_message or getattr(source_message, "empty", False):
                continue

            source_text = getattr(source_message, "caption", None) or getattr(source_message, "text", None) or ""
            diskwala_link = extract_diskwala_link(source_text)
            if not diskwala_link or not pyrogram_message_has_media(source_message):
                continue

            await api.request("copyMessage", {
                "chat_id": chat_id,
                "from_chat_id": parse_chat_id(LIBRARY_TELEGRAM_CHANNEL),
                "message_id": message_id,
                "caption": video_caption(diskwala_link),
                "parse_mode": "HTML",
                "reply_markup": video_keyboard(),
            })
            return
        except TelegramAPIError as exc:
            if exc.retry_after:
                await asyncio.sleep(exc.retry_after)
                continue
            if is_retryable_missing_message(exc):
                continue
            logger.warning("Random video copy failed: %s", exc.description)
            break
        except Exception as exc:
            logger.warning("Random message lookup failed for %s: %s", message_id, exc)
            continue

    await api.request("sendMessage", {
        "chat_id": chat_id,
        "text": "😕 Could not find a random media post with a Diskwala link right now. Please try again.",
    })


def broadcast_text(stats: dict[str, int], running: bool = True) -> str:
    status = "🚀 Broadcast running" if running else "✅ Broadcast finished"
    if stats.get("cancelled"):
        status = "🛑 Broadcast cancelled"
    elapsed = max(1, int(time.time() - stats["started_at"]))
    speed = stats["done"] / elapsed
    return (
        f"{status}\n\n"
        f"👥 Total: {stats['total']}\n"
        f"📨 Done: {stats['done']}\n"
        f"✅ Sent: {stats['sent']}\n"
        f"❌ Failed: {stats['failed']}\n"
        f"🗑 Deleted: {stats['deleted']}\n"
        f"⚡ Speed: {speed:.1f}/sec"
    )


async def copy_or_send_broadcast(api: TelegramBotAPI, target_user_id: int, source: dict) -> None:
    if source["type"] == "copy":
        await api.request("copyMessage", {
            "chat_id": target_user_id,
            "from_chat_id": source["from_chat_id"],
            "message_id": source["message_id"],
        })
        return

    await api.request("sendMessage", {
        "chat_id": target_user_id,
        "text": source["text"],
        "disable_web_page_preview": False,
    })


def should_delete_user_after_failure(exc: TelegramAPIError) -> bool:
    text = exc.description.lower()
    return exc.status in {400, 403} and any(
        phrase in text
        for phrase in (
            "bot was blocked",
            "user is deactivated",
            "chat not found",
            "forbidden",
            "peer_id_invalid",
            "bad request: chat_id is empty",
        )
    )


async def run_broadcast(api: TelegramBotAPI, admin_chat_id: int, status_message_id: int, source: dict) -> None:
    job_id = str(status_message_id)
    cancel_event = asyncio.Event()
    stats = {
        "started_at": int(time.time()),
        "total": await count_users_for_this_bot(),
        "done": 0,
        "sent": 0,
        "failed": 0,
        "deleted": 0,
        "cancelled": 0,
    }
    broadcast_jobs[job_id] = {"cancel": cancel_event, "stats": stats}

    last_edit = 0.0
    try:
        try:
            await api.request("editMessageReplyMarkup", {
                "chat_id": admin_chat_id,
                "message_id": status_message_id,
                "reply_markup": cancel_keyboard(job_id),
            })
        except TelegramAPIError:
            pass

        async for user_id in iter_users_for_this_bot():
            if cancel_event.is_set():
                stats["cancelled"] = 1
                break

            try:
                await copy_or_send_broadcast(api, user_id, source)
                stats["sent"] += 1
            except TelegramAPIError as exc:
                if exc.retry_after:
                    await asyncio.sleep(exc.retry_after)
                    try:
                        await copy_or_send_broadcast(api, user_id, source)
                        stats["sent"] += 1
                    except TelegramAPIError as retry_exc:
                        stats["failed"] += 1
                        if should_delete_user_after_failure(retry_exc):
                            await remove_user_for_this_bot(user_id)
                            stats["deleted"] += 1
                else:
                    stats["failed"] += 1
                    if should_delete_user_after_failure(exc):
                        await remove_user_for_this_bot(user_id)
                        stats["deleted"] += 1

            stats["done"] += 1
            now = time.time()
            if now - last_edit >= 2 or stats["done"] == stats["total"]:
                last_edit = now
                try:
                    await api.request("editMessageText", {
                        "chat_id": admin_chat_id,
                        "message_id": status_message_id,
                        "text": broadcast_text(stats, running=True),
                        "reply_markup": cancel_keyboard(job_id),
                    })
                except TelegramAPIError:
                    pass

            await asyncio.sleep(0.04)
    finally:
        broadcast_jobs.pop(job_id, None)
        try:
            await api.request("editMessageText", {
                "chat_id": admin_chat_id,
                "message_id": status_message_id,
                "text": broadcast_text(stats, running=False),
            })
        except TelegramAPIError:
            pass


async def start_broadcast(api: TelegramBotAPI, message: dict) -> None:
    chat_id = int(message["chat"]["id"])
    user_id = int((message.get("from") or {}).get("id") or 0)
    if user_id not in OWNER_CHAT_IDS:
        return

    if broadcast_jobs:
        await api.request("sendMessage", {"chat_id": chat_id, "text": "⚠️ Broadcast already running."})
        return

    reply = message.get("reply_to_message")
    payload = command_payload(message.get("text") or message.get("caption") or "")
    if reply:
        source = {"type": "copy", "from_chat_id": chat_id, "message_id": int(reply["message_id"])}
    elif payload:
        source = {"type": "text", "text": payload}
    else:
        await api.request("sendMessage", {
            "chat_id": chat_id,
            "text": "📣 Reply to a post with /broadcast, or use /broadcast your message.",
        })
        return

    total = await count_users_for_this_bot()
    status = await api.request("sendMessage", {
        "chat_id": chat_id,
        "text": (
            f"🚀 Broadcast starting\n\n"
            f"🤖 Bot: @{BOT_USERNAME or BOT_KEY}\n"
            f"👥 Users: {total}"
        ),
        "reply_markup": cancel_keyboard("pending"),
    })
    asyncio.create_task(run_broadcast(api, chat_id, int(status["message_id"]), source))


async def handle_callback(api: TelegramBotAPI, callback: dict) -> None:
    data = callback.get("data") or ""
    callback_id = callback.get("id")
    user = callback.get("from") or {}
    message = callback.get("message") or {}
    chat_id = int((message.get("chat") or {}).get("id") or user.get("id") or 0)

    if data == "video":
        if callback_id:
            await api.request("answerCallbackQuery", {"callback_query_id": callback_id, "text": "🎬 Sending video..."})
        await send_random_video(api, chat_id, user)
        return

    if data.startswith("cancel_broadcast:"):
        if int(user.get("id") or 0) not in OWNER_CHAT_IDS:
            if callback_id:
                await api.request("answerCallbackQuery", {
                    "callback_query_id": callback_id,
                    "text": "Only admin can cancel.",
                    "show_alert": True,
                })
            return
        job_id = data.split(":", 1)[1]
        if job_id == "pending":
            if callback_id:
                await api.request("answerCallbackQuery", {"callback_query_id": callback_id, "text": "Starting..."})
            return
        job = broadcast_jobs.get(job_id)
        if job:
            job["cancel"].set()
            if callback_id:
                await api.request("answerCallbackQuery", {"callback_query_id": callback_id, "text": "🛑 Cancelling..."})
        elif callback_id:
            await api.request("answerCallbackQuery", {"callback_query_id": callback_id, "text": "No active broadcast."})


async def handle_channel_post(message: dict) -> None:
    chat = message.get("chat") or {}
    chat_id = chat.get("id")
    message_id = message.get("message_id")
    if chat_id is None or message_id is None:
        return
    if chat_matches_config(chat, LIBRARY_TELEGRAM_CHANNEL):
        await set_channel_last_message(chat_id, int(message_id), chat.get("username") or "")
    elif chat_matches_config(chat, FORWARD_TEXT_CHANNEL):
        await set_channel_last_message(chat_id, int(message_id), chat.get("username") or "")


async def handle_message(api: TelegramBotAPI, message: dict) -> None:
    text = message.get("text") or message.get("caption") or ""
    cmd = command_name(text)
    from_user_id = int((message.get("from") or {}).get("id") or 0)
    normalized_text = text.strip()

    if cmd == "start":
        await send_start(api, message)
    elif cmd == "video":
        await send_random_video(api, int(message["chat"]["id"]), user_from_message(message))
    elif normalized_text == JOIN_CHANNELS_TEXT:
        await send_start(api, message)
    elif normalized_text in {WATCH_VIDEO_TEXT, WATCH_NEXT_TEXT}:
        await send_random_video(api, int(message["chat"]["id"]), user_from_message(message))
    elif normalized_text == EARN_CHANNEL_TEXT:
        await send_earn_channel(api, int(message["chat"]["id"]))
    elif cmd == "broadcast":
        await start_broadcast(api, message)
    elif cmd == "setmax" and from_user_id in OWNER_CHAT_IDS:
        payload = command_payload(text)
        if not payload.isdigit() or int(payload) <= 0:
            await api.request("sendMessage", {
                "chat_id": int(message["chat"]["id"]),
                "text": "Use: /setmax 20000",
            })
            return
        await set_library_max_message_id(int(payload))
        await api.request("sendMessage", {
            "chat_id": int(message["chat"]["id"]),
            "text": f"✅ Library max message id saved: {int(payload)}",
        })
    elif cmd == "status" and from_user_id in OWNER_CHAT_IDS:
        await api.request("sendMessage", {
            "chat_id": int(message["chat"]["id"]),
            "text": f"📊 Users started for @{BOT_USERNAME or BOT_KEY}: {await count_users_for_this_bot()}",
        })
    elif cmd == "stats" and from_user_id in OWNER_CHAT_IDS:
        await api.request("sendMessage", {
            "chat_id": int(message["chat"]["id"]),
            "text": (
                f"📊 Users for @{BOT_USERNAME or BOT_KEY}: {await count_users_for_this_bot()}\n"
                f"🔢 Library max id: {await get_max_library_message_id()}"
            ),
        })
    elif cmd == "exportsession" and from_user_id in OWNER_CHAT_IDS:
        chat_id = int(message["chat"]["id"])
        if mtproto_app is None:
            await api.request("sendMessage", {"chat_id": chat_id, "text": "⚠️ MTProto reader is not running."})
            return
        try:
            session_str = await mtproto_app.export_session_string()
        except Exception as exc:
            await api.request("sendMessage", {"chat_id": chat_id, "text": f"❌ Export failed: {exc}"})
            return
        await api.request("sendMessage", {
            "chat_id": chat_id,
            "text": (
                "🔑 Session string (SECRET - grants full account access):\n\n"
                f"<code>{html_escape(session_str)}</code>\n\n"
                "Save this as the SESSION_STRING env var on Render, then redeploy. "
                "Delete this message after copying it."
            ),
            "parse_mode": "HTML",
        })


def extract_update_user_id(update: dict) -> int:
    message = update.get("message") or update.get("edited_message") or {}
    if message:
        return int((message.get("from") or {}).get("id") or 0)
    callback = update.get("callback_query") or {}
    if callback:
        return int((callback.get("from") or {}).get("id") or 0)
    return 0


async def process_update(api: TelegramBotAPI, update: dict) -> None:
    try:
        if "channel_post" in update:
            await handle_channel_post(update["channel_post"])
        elif "edited_channel_post" in update:
            await handle_channel_post(update["edited_channel_post"])
        elif "callback_query" in update:
            await handle_callback(api, update["callback_query"])
        elif "message" in update:
            await handle_message(api, update["message"])
    except TelegramAPIError as exc:
        logger.warning("Telegram API error in update %s: %s", update.get("update_id"), exc.description)
        if should_delete_user_after_failure(exc):
            user_id = extract_update_user_id(update)
            if user_id:
                await remove_user_for_this_bot(user_id)
                logger.info("Removed blocked/deactivated user %s from db", user_id)
    except Exception:
        logger.exception("Unhandled update error: %s", update.get("update_id"))


async def resolve_bot_identity(api: TelegramBotAPI) -> None:
    global BOT_KEY, BOT_KEY_SAFE, BOT_USERNAME, BOT_ID
    me = await api.request("getMe")
    BOT_USERNAME = (me.get("username") or "").lstrip("@")
    BOT_ID = int(me.get("id") or 0)
    if not BOT_KEY:
        BOT_KEY = BOT_USERNAME or str(BOT_ID)
    BOT_KEY_SAFE = safe_field(BOT_KEY)
    logger.info("Bot identity: @%s (%s), db key=%s", BOT_USERNAME, BOT_ID, BOT_KEY_SAFE)


async def notify_owner(api: TelegramBotAPI, text: str) -> None:
    for owner_id in OWNER_CHAT_IDS:
        try:
            await api.request("sendMessage", {"chat_id": owner_id, "text": text})
        except TelegramAPIError as exc:
            logger.warning("Owner notify failed for %s: %s", owner_id, exc.description)


async def start_health_server() -> aiohttp.web.AppRunner:
    port = int(os.getenv("PORT", "10000") or 10000)
    app = aiohttp.web.Application()
    app.router.add_get("/", lambda _request: aiohttp.web.Response(text="ok"))
    runner = aiohttp.web.AppRunner(app)
    await runner.setup()
    site = aiohttp.web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    logger.info("Health server listening on port %s", port)
    return runner


SESSION_STRING = os.getenv("SESSION_STRING", "").strip()

# Matches pyrogram.storage.storage.Storage.SESSION_STRING_FORMAT ('>BI?256sQ?').
# base64.urlsafe_b64decode silently drops invalid characters instead of raising,
# so a session string mangled by copy/paste (stray quotes, whitespace, a dropped
# line) decodes short instead of erroring - checking the byte length up front
# catches that before it reaches pyrogram's struct.unpack and crashes the boot.
_SESSION_STRING_BYTES = struct.calcsize(">BI?256sQ?")


def is_valid_session_string(value: str) -> bool:
    try:
        padded = value + "=" * (-len(value) % 4)
        return len(base64.urlsafe_b64decode(padded)) == _SESSION_STRING_BYTES
    except Exception:
        return False


async def start_mtproto_reader() -> None:
    global mtproto_app

    client_kwargs: dict[str, Any] = dict(
        api_id=API_ID,
        api_hash=API_HASH,
        bot_token=BOT_TOKEN,
        # A bot session can only learn a private channel's access_hash from a live
        # update (new post, being added, etc) - there is no bot-callable "resolve by
        # id" for a chat it has never seen. no_updates=True blocks that permanently,
        # so it must stay False for the library channel to ever become resolvable.
        no_updates=False,
    )
    client_kwargs["in_memory"] = True
    session_string = SESSION_STRING
    if session_string and not is_valid_session_string(session_string):
        logger.warning(
            "SESSION_STRING is malformed (wrong decoded length) - ignoring it and "
            "starting a fresh login instead. Re-copy it carefully (no quotes, no "
            "extra whitespace/line breaks) from /exportsession and reset the env var."
        )
        session_string = ""

    if session_string:
        client_kwargs["session_string"] = session_string
    else:
        logger.warning(
            "SESSION_STRING is not set or invalid. The MTProto peer cache will not "
            "survive a restart/redeploy; once resolved, use /exportsession to save it."
        )

    mtproto_app = Client("viralbot_reader", **client_kwargs)
    await mtproto_app.start()

    last_exc: Exception | None = None
    for attempt in range(5):
        try:
            await mtproto_app.get_chat(parse_chat_id(LIBRARY_TELEGRAM_CHANNEL))
            last_exc = None
            break
        except Exception as exc:
            last_exc = exc
            logger.warning("Peer resolve attempt %s/5 failed: %s", attempt + 1, exc)
            await asyncio.sleep(2 * (attempt + 1))

    if last_exc is not None:
        if SESSION_STRING:
            # A persisted session was expected to already have this resolved; failing
            # here means something actually broke (revoked, channel changed, etc).
            await mtproto_app.stop()
            raise RuntimeError(f"Could not resolve library channel peer after 5 attempts: {last_exc}")

        logger.warning(
            "Library channel peer still unresolved after boot: %s. This session has "
            "never seen a live update from it. Ask the channel admin to post anything "
            "new while the bot keeps running - the peer resolves automatically the "
            "moment an update arrives. Then run /exportsession and save the output as "
            "the SESSION_STRING env var so it never has to resolve cold again.",
            last_exc,
        )
        # Keep running rather than crash-looping: mtproto_app stays set and will
        # self-heal once a live update from the channel arrives.

    logger.info("MTProto reader started in memory; library channel peer resolved.")


async def main() -> None:
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN is required in .env")
    if not API_ID or not API_HASH:
        raise RuntimeError("API_ID and API_HASH are required to read random old library messages")
    if not LIBRARY_TELEGRAM_CHANNEL:
        raise RuntimeError("LIBRARY_TELEGRAM_CHANNEL is required in .env")
    if not FORWARD_TEXT_CHANNEL:
        raise RuntimeError("FORWARD_TEXT_CHANNEL is required in .env")

    api = TelegramBotAPI(BOT_TOKEN)
    await api.open()
    health_runner = await start_health_server()

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _request_stop() -> None:
        if not stop_event.is_set():
            logger.info("Shutdown signal received.")
            stop_event.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, _request_stop)
        except (NotImplementedError, RuntimeError):
            pass  # e.g. Windows, which lacks add_signal_handler support

    crashed_exc: Exception | None = None
    try:
        await resolve_bot_identity(api)
        await start_mtproto_reader()
        await init_mongo()
        await api.request("deleteWebhook", {"drop_pending_updates": DROP_PENDING_UPDATES})
        await notify_owner(api, f"✅ @{BOT_USERNAME or BOT_KEY} started. No session file used.")

        offset = 0
        while not stop_event.is_set():
            try:
                updates = await api.request("getUpdates", {
                    "offset": offset,
                    "timeout": POLL_TIMEOUT_SECONDS,
                    "allowed_updates": ["message", "callback_query", "channel_post", "edited_channel_post"],
                })
                for update in updates:
                    offset = max(offset, int(update["update_id"]) + 1)
                    asyncio.create_task(process_update(api, update))
            except TelegramAPIError as exc:
                if exc.retry_after:
                    await asyncio.sleep(exc.retry_after)
                else:
                    logger.warning("Polling Telegram error: %s", exc.description)
                    await asyncio.sleep(3)
            except aiohttp.ClientError as exc:
                logger.warning("Network error: %s", exc)
                await asyncio.sleep(3)
    except Exception as exc:
        crashed_exc = exc
        logger.exception("Bot crashed and is not responding")
    finally:
        if crashed_exc is not None:
            await notify_owner(api, f"🛑 @{BOT_USERNAME or BOT_KEY} crashed and is not responding: {crashed_exc}")
        else:
            await notify_owner(api, f"⏹ @{BOT_USERNAME or BOT_KEY} is stopping.")
        if mtproto_app:
            try:
                await mtproto_app.stop()
            except Exception as exc:
                logger.warning("MTProto client stop failed (continuing cleanup): %s", exc)
        await api.close()
        await health_runner.cleanup()
        if mongo_client:
            mongo_client.close()
        if crashed_exc is not None:
            raise crashed_exc


if __name__ == "__main__":
    asyncio.run(main())
