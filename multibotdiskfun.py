import asyncio
import logging
import os
import random
import re
import signal
import time
from datetime import datetime, timezone
from typing import Any

import aiohttp.web
from dotenv import load_dotenv
from telethon import TelegramClient, Button, events
from telethon.tl.types import MessageEmpty, MessageMediaWebPage, InputMediaWebPage
from telethon.errors import (
    ChatAdminRequiredError,
    ChatWriteForbiddenError,
    FloodWaitError,
    InputUserDeactivatedError,
    PeerIdInvalidError,
    UserDeactivatedBanError,
    UserDeactivatedError,
    UserIdInvalidError,
    UserIsBlockedError,
    UserNotParticipantError,
)

try:
    from motor.motor_asyncio import AsyncIOMotorClient
except Exception:
    AsyncIOMotorClient = None


load_dotenv()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("multibotdiskfun")


# ===== SHARED CONFIG (same for every bot) =====
def parse_owner_ids() -> set[int]:
    ids: set[int] = set()
    owner_chat_id = int(os.getenv("OWNER_CHAT_ID", "0") or 0)
    if owner_chat_id:
        ids.add(owner_chat_id)
    raw = os.getenv("OWNER_CHAT_IDS", "").strip()
    for part in re.split(r"[,;\s]+", raw):
        part = part.strip()
        if part.lstrip("-").isdigit():
            ids.add(int(part))
    return ids


OWNER_CHAT_IDS = parse_owner_ids()
API_ID = int(os.getenv("API_ID", "0") or 0)
API_HASH = os.getenv("API_HASH", "").strip()

# Diskwala shared channel configs
DISKWALA_LIBRARY_CHANNEL = (
    os.getenv("DISKWALA_LIBRARY_CHANNEL", "").strip()
    or os.getenv("LIBRARY_TELEGRAM_CHANNEL", "").strip()
)
DISKWALA_FORWARD_TEXT_CHANNEL = (
    os.getenv("DISKWALA_FORWARD_TEXT_CHANNEL", "").strip()
    or os.getenv("FORWARD_TEXT_CHANNEL", "").strip()
)
DISKWALA_START_POST_ID = int(
    os.getenv("DISKWALA_START_POST_MESSAGE_ID", "0")
    or os.getenv("START_POST_MESSAGE_ID", "0")
    or 0
)
DISKWALA_LIBRARY_MAX_ID = int(
    os.getenv("DISKWALA_LIBRARY_MAX_MESSAGE_ID", "0")
    or os.getenv("LIBRARY_MAX_MESSAGE_ID", "0")
    or 0
)
TUTORIAL_URL = os.getenv("TUTORIAL_URL", "https://t.me/howdisk/2").strip()
TRENDING_URL = os.getenv("TRENDING_URL", "bitly.cx/diskwala").strip()

# Terabox shared channel configs
TERABOX_LIBRARY_CHANNEL = (
    os.getenv("TERABOX_LIBRARY_CHANNEL", "").strip()
    or os.getenv("TERABOX_LIBRARY_TELEGRAM_CHANNEL", "").strip()
)
TERABOX_FORWARD_TEXT_CHANNEL = (
    os.getenv("TERABOX_FORWARD_TEXT_CHANNEL", "").strip()
    or os.getenv("FORWARD_TEXT_CHANNEL", "").strip()
)
TERABOX_START_POST_ID = int(os.getenv("TERABOX_START_POST_MESSAGE_ID", "0") or 0)
TERABOX_LIBRARY_MAX_ID = int(os.getenv("TERABOX_LIBRARY_MAX_MESSAGE_ID", "0") or 0)
PROVIDER = os.getenv("PROVIDER", "terabox").strip().lower()
LINK_PREVIEW = os.getenv("LINK_PREVIEW", "true").strip().lower() in ("true", "1", "yes")
FOOTER_TEXT = os.getenv("FOOTER_TEXT", "").strip()
HEADER_TEXT = os.getenv("HEADER_TEXT", "🎬 **Vdo 😍**\n\n**🔗🔗यह रहा वीडियो लिंक 👇**").strip()
SEND_SOURCE_MEDIA = os.getenv("SEND_SOURCE_MEDIA", "false").strip().lower() in ("true", "1", "yes")
USE_SOURCE_CAPTION = os.getenv("USE_SOURCE_CAPTION", "false").strip().lower() in ("true", "1", "yes")

# Database & shared settings
MONGO_URI = os.getenv("MONGO_URI", "").strip()
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "viralbots").strip()
MONGO_USERS_COLLECTION = os.getenv("MONGO_USERS_COLLECTION", "multi_bot_users").strip()
MONGO_CHANNELS_COLLECTION = os.getenv("MONGO_CHANNELS_COLLECTION", "channel_state").strip()
RANDOM_VIDEO_RETRIES = int(os.getenv("RANDOM_VIDEO_RETRIES", "35") or 35)

WATCH_VIDEO_TEXT = os.getenv("WATCH_VIDEO_TEXT", "🔞 Watch Now").strip() or "🎬 Watch Now"
WATCH_NEXT_TEXT = os.getenv("WATCH_NEXT_TEXT", "👙 Watch Next").strip() or "⏭ Watch Next"
JOIN_CHANNELS_TEXT = os.getenv("JOIN_CHANNELS_TEXT", "Join VIRALS").strip() or "📢 Join Viral Channel"

def parse_offer_channels() -> list[dict[str, str]]:
    """
    Each offer button is Text|URL, semicolon separated:
      OFFER_CHANNELS=💰 Earning Channel|https://t.me/+abc;🎰 Aviator Guide|https://t.me/+xyz
    You can also use OFFER_CHANNEL_1, OFFER_CHANNEL_2, ... instead/as well.
    Falls back to single OFFER_CHANNEL_URL / OFFER_CHANNEL_TEXT if none of the above are set.
    """
    raw_items: list[str] = []
    if os.getenv("OFFER_CHANNELS", "").strip():
        raw_items.extend(x.strip() for x in os.getenv("OFFER_CHANNELS", "").split(";") if x.strip())

    for idx in range(1, 21):
        item = os.getenv(f"OFFER_CHANNEL_{idx}", "").strip()
        if item:
            raw_items.append(item)

    channels: list[dict[str, str]] = []
    for item in raw_items:
        parts = [p.strip() for p in item.split("|") if p.strip()]
        if not parts:
            continue
        text, url = (parts[0], parts[1]) if len(parts) > 1 else ("💰 Daily Earning Channel", parts[0])
        channels.append({"text": text, "url": url})

    if not channels:
        url = os.getenv("OFFER_CHANNEL_URL", "https://t.me/+-WC_czgycbM5M2Rl").strip()
        text = os.getenv("OFFER_CHANNEL_TEXT", "💰 Daily Earning Channel").strip() or "💰 Daily Earning Channel"
        if url:
            channels.append({"text": text, "url": url})

    return channels


OFFER_CHANNELS = parse_offer_channels()
OFFER_CHANNEL_TEXT_MAP = {c["text"]: c for c in OFFER_CHANNELS}

mongo_client = None
mongo_db = None
users_col = None
channels_col = None
memory_channel_state: dict[str, int] = {}

# Errors that mean the user is gone for good (blocked the bot, deleted their
# account, etc) - safe to drop from the DB so future broadcasts skip them.
USER_GONE_ERRORS = (
    UserIsBlockedError,
    InputUserDeactivatedError,
    UserDeactivatedError,
    UserDeactivatedBanError,
    PeerIdInvalidError,
    UserIdInvalidError,
    ChatWriteForbiddenError,
)


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


LIVEGRAM_RE = re.compile(
    r"(?:livegram|livegrambot|bot\s+not\s+responds|this\s+bot\s+was\s+made\s+using)",
    re.IGNORECASE,
)


def should_delete_livegram_message(text: str) -> bool:
    return bool(LIVEGRAM_RE.search(text or ""))


async def delete_message_safely(event) -> None:
    try:
        await event.delete()
    except Exception as exc:
        logger.warning("Could not delete message %s: %s", event.id, exc)


def command_name(text: str) -> str:
    first = (text or "").split(maxsplit=1)[0].lower()
    if not first.startswith("/"):
        return ""
    return first[1:].split("@", 1)[0]


def command_payload(text: str) -> str:
    parts = (text or "").split(maxsplit=1)
    return parts[1].strip() if len(parts) > 1 else ""


TERABOX_DOMAINS_RE = re.compile(
    r"(?:https?://)?(?:www\.)?(?:[a-zA-Z0-9-]+\.)*(?:"
    r"terabox\.com|teraboxapp\.com|1024tera\.com|1024terabox\.com|"
    r"terasharefile\.com|tersharefile\.com|terasharelink\.com|teraboxlink\.com|"
    r"terafileshare\.com|4funbox\.com|mirrobox\.com|nephobox\.com|"
    r"momerybox\.com|tibibox\.com|freeterabox\.com|terabox\.app|terabox\.fun"
    r")/[^\s<>)\]]+",
    re.IGNORECASE,
)

TERABOX_KEYWORD_RE = re.compile(
    r"https?://\S*(?:terabox|1024tera|terashare|4funbox|mirrobox|nephobox|momerybox|tibibox)\S*",
    re.IGNORECASE,
)

GENERIC_URL_RE = re.compile(r"https?://[^\s<>)\]]+", re.IGNORECASE)


def extract_terabox_link(text: str) -> str:
    if not text:
        return ""
    match = TERABOX_DOMAINS_RE.search(text)
    if not match:
        match = TERABOX_KEYWORD_RE.search(text)
    if not match:
        return ""
    link = match.group(0).rstrip(".,;:!?)\"']")
    if not link.lower().startswith(("http://", "https://")):
        link = f"https://{link}"
    return link


def extract_diskwala_link(text: str) -> str:
    match = re.search(r"(?:https?://)?(?:www\.)?diskwala\.com/[^\s<>)\]]+", text or "", flags=re.IGNORECASE)
    if not match:
        return ""
    link = match.group(0).rstrip(".,;:!?)\"']")
    if not link.lower().startswith(("http://", "https://")):
        link = f"https://{link}"
    return link


def extract_target_link(text: str, message: Any = None, provider: str = "") -> str:
    active_provider = provider or PROVIDER
    raw = text or ""
    # 1. Terabox link directly in text
    tb = extract_terabox_link(raw)
    if tb:
        return tb

    # 2. Check entities for hyperlinked URLs (e.g. MessageEntityTextUrl)
    if message:
        entities = getattr(message, "entities", None) or []
        for ent in entities:
            url = getattr(ent, "url", None)
            if url:
                tb_ent = extract_terabox_link(url)
                if tb_ent:
                    return tb_ent

    # 3. Diskwala check if in diskwala mode
    if active_provider == "diskwala":
        dw = extract_diskwala_link(raw)
        if dw:
            return dw

    # 4. In Terabox mode, fallback to any valid URL in message if domain wasn't in regex
    if active_provider == "terabox":
        match = GENERIC_URL_RE.search(raw)
        if match:
            url = match.group(0).rstrip(".,;:!?)\"']")
            if not url.lower().startswith(("http://", "https://")):
                url = f"https://{url}"
            return url

    return ""


def format_post_caption(link: str, original_text: str = "", provider: str = "", footer_text: str = "") -> str:
    active_provider = provider or PROVIDER
    active_footer = footer_text if footer_text is not None else FOOTER_TEXT
    if active_provider == "diskwala" and not active_footer:
        tutorial = TUTORIAL_URL or "https://t.me/howdisk/2"
        trending = TRENDING_URL or "bitly.cx/diskwala"
        return (
            "🎬 **Vdo 😍**\n\n"
            "**🔗🔗यह रहा वीडियो लिंक 👇**\n\n"
            f"{link}\n\n"
            "**🤔 How to Open Links? | लिंक कैसे खोलें 👇**\n"
            f"[📖 View Tutorial]({tutorial})\n\n"
            "😉**Must Try Terabox Virals 👇**\n"
            f"{trending}\n"
            f"{trending}"
        )

    parts: list[str] = []
    if USE_SOURCE_CAPTION and original_text:
        parts.append(original_text.strip())
    else:
        if HEADER_TEXT:
            parts.append(HEADER_TEXT)
        if link:
            parts.append(link)
        elif original_text:
            parts.append(original_text.strip())

    # Footer is currently blank by default; will append if customized later via FOOTER_TEXT
    if active_footer:
        parts.append(active_footer)

    return "\n\n".join(parts)


def video_caption(diskwala_link: str) -> str:
    """Legacy alias for backward compatibility."""
    return format_post_caption(diskwala_link, provider="diskwala")


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


def cancel_keyboard(job_id: str) -> list:
    return [[Button.inline("🛑 Cancel Broadcast", f"cancel_broadcast:{job_id}".encode())]]


# ===== PER-BOT CONFIG =====
def parse_bot_configs() -> list[dict[str, Any]]:
    """
    Loads bots for BOTH Diskwala and Terabox:
    1. Diskwala bots:
       - Configured via DISKWALA_BOT_TOKENS or BOT_TOKENS (or BOT_TOKEN_1, BOT_TOKEN_2, ...)
       - Uses DISKWALA_LIBRARY_CHANNEL (or LIBRARY_TELEGRAM_CHANNEL)
       - Uses session diskfun_bot_{idx}
    2. Terabox bots:
       - Configured via TERABOX_BOT_TOKENS (or TERABOX_BOT_TOKEN_1, TERABOX_BOT_TOKEN_2, ...)
       - Uses TERABOX_LIBRARY_CHANNEL
       - Uses session terafun_bot_{idx}
    Both sets of bots run concurrently in the same process!
    """
    configs: list[dict[str, Any]] = []

    # 1. Diskwala Bots
    raw_disk = [
        t.strip()
        for t in re.split(r"[,;\r\n]+", os.getenv("DISKWALA_BOT_TOKENS", "").strip() or os.getenv("BOT_TOKENS", "").strip())
        if t.strip()
    ]
    if not raw_disk:
        idx = 1
        while True:
            t = os.getenv(f"BOT_TOKEN_{idx}", "").strip()
            if not t:
                break
            raw_disk.append(t)
            idx += 1
    if not raw_disk and not os.getenv("TERABOX_BOT_TOKENS", "").strip():
        single = os.getenv("BOT_TOKEN", "").strip()
        if single:
            raw_disk.append(single)

    for idx, token in enumerate(raw_disk, start=1):
        configs.append({
            "index": idx,
            "provider": "diskwala",
            "token": token,
            "session": os.getenv(f"BOT_SESSION_{idx}", f"diskfun_bot_{idx}").strip(),
            "bot_key": os.getenv(f"BOT_KEY_{idx}", "").strip().lstrip("@"),
            "library_channel": DISKWALA_LIBRARY_CHANNEL,
            "forward_channel": DISKWALA_FORWARD_TEXT_CHANNEL,
            "start_post_id": DISKWALA_START_POST_ID,
            "library_max_id": DISKWALA_LIBRARY_MAX_ID,
            "link_preview": False,
            "footer_text": "",
            "header_text": "",
            "tutorial_url": TUTORIAL_URL,
            "trending_url": TRENDING_URL,
            "send_source_media": True,
            "use_source_caption": False,
        })

    # 2. Terabox Bots
    raw_tera = [
        t.strip()
        for t in re.split(r"[,;\r\n]+", os.getenv("TERABOX_BOT_TOKENS", "").strip())
        if t.strip()
    ]
    if not raw_tera:
        idx = 1
        while True:
            t = os.getenv(f"TERABOX_BOT_TOKEN_{idx}", "").strip()
            if not t:
                break
            raw_tera.append(t)
            idx += 1
    if not raw_tera:
        single_tera = os.getenv("TERABOX_BOT_TOKEN", "").strip()
        if single_tera:
            raw_tera.append(single_tera)

    for idx, token in enumerate(raw_tera, start=1):
        configs.append({
            "index": idx,
            "provider": "terabox",
            "token": token,
            "session": os.getenv(f"TERABOX_BOT_SESSION_{idx}", f"terafun_bot_{idx}").strip(),
            "bot_key": os.getenv(f"TERABOX_BOT_KEY_{idx}", "").strip().lstrip("@"),
            "library_channel": TERABOX_LIBRARY_CHANNEL,
            "forward_channel": TERABOX_FORWARD_TEXT_CHANNEL,
            "start_post_id": TERABOX_START_POST_ID,
            "library_max_id": TERABOX_LIBRARY_MAX_ID,
            "link_preview": LINK_PREVIEW,
            "footer_text": FOOTER_TEXT,
            "header_text": HEADER_TEXT,
            "tutorial_url": "",
            "trending_url": "",
            "send_source_media": SEND_SOURCE_MEDIA,
            "use_source_caption": USE_SOURCE_CAPTION,
        })

    return configs


# ===== SHARED MONGO (users/channel state are shared across all bots) =====
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
    await users_col.create_index("bot_keys")
    logger.info("MongoDB connected: %s.%s", MONGO_DB_NAME, MONGO_USERS_COLLECTION)


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


async def get_max_library_message_id(channel_target: int | str = "", fallback_max: int = 0) -> int:
    if fallback_max > 0:
        return fallback_max
    target = channel_target or DISKWALA_LIBRARY_CHANNEL or TERABOX_LIBRARY_CHANNEL
    if not target:
        return 0
    return await get_channel_last_message(parse_chat_id(target))


async def set_library_max_message_id(channel_target: int | str, message_id: int) -> None:
    target = channel_target or DISKWALA_LIBRARY_CHANNEL or TERABOX_LIBRARY_CHANNEL
    if target:
        await set_channel_last_message(parse_chat_id(target), int(message_id))


def should_delete_user_after_failure(exc: Exception) -> bool:
    return isinstance(exc, USER_GONE_ERRORS)


# ===== PER-BOT RUNTIME =====
class BotRuntime:
    def __init__(self, config: dict[str, Any]) -> None:
        self.index = config["index"]
        self.provider = config.get("provider", "diskwala")
        self.token = config["token"]
        self.bot_key = config["bot_key"]
        self.session = config["session"]
        self.library_channel = config.get("library_channel", "")
        self.forward_channel = config.get("forward_channel", "")
        self.start_post_id = config.get("start_post_id", 0)
        self.max_library_id = config.get("library_max_id", 0)
        self.link_preview = config.get("link_preview", False)
        self.footer_text = config.get("footer_text", "")
        self.header_text = config.get("header_text", "")
        self.tutorial_url = config.get("tutorial_url", "")
        self.trending_url = config.get("trending_url", "")
        self.send_source_media = config.get("send_source_media", self.provider == "diskwala")
        self.use_source_caption = config.get("use_source_caption", False)

        self.bot_key_safe = ""
        self.bot_username = ""
        self.bot_id = 0
        self.client = TelegramClient(self.session, API_ID, API_HASH)
        self.memory_users: set[int] = set()
        self.broadcast_jobs: dict[str, dict[str, Any]] = {}

    def format_caption(self, target_link: str, source_text: str = "") -> str:
        if self.provider == "diskwala":
            tutorial = self.tutorial_url or TUTORIAL_URL or "https://t.me/howdisk/2"
            trending = self.trending_url or TRENDING_URL or "bitly.cx/diskwala"
            return (
                "🎬 **Vdo 😍**\n\n"
                "**🔗🔗यह रहा वीडियो लिंक 👇**\n\n"
                f"{target_link}\n\n"
                "**🤔 How to Open Links? | लिंक कैसे खोलें 👇**\n"
                f"[📖 View Tutorial]({tutorial})\n\n"
                "😉**Must Try Terabox Virals 👇**\n"
                f"{trending}\n"
                f"{trending}"
            )

        # Terabox caption
        parts: list[str] = []
        if self.use_source_caption and source_text:
            parts.append(source_text.strip())
        else:
            header = self.header_text or HEADER_TEXT or "🎬 **Vdo 😍**\n\n**🔗🔗यह रहा वीडियो लिंक 👇**"
            if header:
                parts.append(header)
            if target_link:
                parts.append(target_link)
            elif source_text:
                parts.append(source_text.strip())

        # Terabox footer: currently blank by default
        footer = self.footer_text or FOOTER_TEXT
        if footer:
            parts.append(footer)

        return "\n\n".join(parts)

    # ----- keyboards -----
    def video_action_keyboard(self, action_text: str) -> list:
        rows = [[Button.inline(action_text, b"video")]]
        for channel in OFFER_CHANNELS:
            rows.append([Button.url(channel["text"], channel["url"])])
        return rows

    def start_keyboard(self) -> list:
        return self.video_action_keyboard(WATCH_VIDEO_TEXT)

    def video_keyboard(self) -> list:
        return self.video_action_keyboard(WATCH_NEXT_TEXT)

    def main_reply_keyboard(self) -> list:
        # same Join/Watch action repeated a few times - just eye-catch, not distinct channels
        row = [Button.text(JOIN_CHANNELS_TEXT, resize=True), Button.text(WATCH_VIDEO_TEXT, resize=True)]
        rows = [row, row, row]
        for channel in OFFER_CHANNELS:
            rows.append([Button.text(channel["text"], resize=True)])
        return rows

    # ----- users (stored per-bot via bots.<bot_key_safe> + bot_keys) -----
    async def save_user(self, user_id: int, username: str = "", first_name: str = "", last_name: str = "") -> None:
        if not user_id:
            return
        if users_col is None:
            self.memory_users.add(int(user_id))
            return

        now = utc_now()
        await users_col.update_one(
            {"user_id": int(user_id)},
            {
                "$set": {
                    "user_id": int(user_id),
                    "username": username,
                    "first_name": first_name,
                    "last_name": last_name,
                    "updated_at": now,
                    f"bots.{self.bot_key_safe}.bot_key": self.bot_key,
                    f"bots.{self.bot_key_safe}.bot_username": self.bot_username,
                    f"bots.{self.bot_key_safe}.bot_id": self.bot_id,
                    f"bots.{self.bot_key_safe}.last_seen_at": now,
                },
                "$setOnInsert": {"created_at": now},
                "$addToSet": {"bot_keys": self.bot_key},
            },
            upsert=True,
        )

    async def save_user_from_event(self, event) -> None:
        try:
            sender = await event.get_sender()
        except Exception:
            sender = None
        if sender is None:
            return
        await self.save_user(
            sender.id,
            username=getattr(sender, "username", None) or "",
            first_name=getattr(sender, "first_name", None) or "",
            last_name=getattr(sender, "last_name", None) or "",
        )

    async def remove_user(self, user_id: int) -> None:
        if users_col is None:
            self.memory_users.discard(int(user_id))
            return

        await users_col.update_one(
            {"user_id": int(user_id)},
            {
                "$unset": {f"bots.{self.bot_key_safe}": ""},
                "$pull": {"bot_keys": self.bot_key},
                "$set": {"updated_at": utc_now()},
            },
        )
        doc = await users_col.find_one({"user_id": int(user_id)}, {"bot_keys": 1})
        if doc and not doc.get("bot_keys"):
            await users_col.delete_one({"user_id": int(user_id)})

    async def count_users(self) -> int:
        if users_col is None:
            return len(self.memory_users)
        return await users_col.count_documents({f"bots.{self.bot_key_safe}.last_seen_at": {"$exists": True}})

    async def iter_users(self):
        if users_col is None:
            for user_id in list(self.memory_users):
                yield user_id
            return

        query = {f"bots.{self.bot_key_safe}.last_seen_at": {"$exists": True}}
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

    # ----- join check -----
    async def user_joined_required_channels(self, user_id: int) -> bool:
        check_channels = [c for c in JOIN_CHANNELS if c.get("check_chat")]
        if not check_channels:
            return True

        for channel in check_channels:
            try:
                await self.client.get_permissions(parse_chat_id(channel["check_chat"]), user_id)
            except UserNotParticipantError:
                return False
            except ChatAdminRequiredError:
                logger.warning("[%s] Bot is not admin in %s, cannot verify join.", self.bot_key, channel["check_chat"])
                return False
            except Exception as exc:
                logger.warning("[%s] Join check failed for %s: %s", self.bot_key, channel["check_chat"], exc)
                return False
        return True

    # ----- messages -----
    async def send_offer_channel(self, chat_id: int, channel: dict[str, str]) -> None:
        await self.client.send_message(
            chat_id,
            "💰 Join our Daily Earning Channel:",
            buttons=[[Button.url(channel["text"], channel["url"])]],
        )

    async def send_join_prompt(self, chat_id: int) -> None:
        await self.client.send_message(
            chat_id,
            "🔒 Please join the channel first, then tap Watch Video.",
            buttons=self.start_keyboard(),
        )

    async def send_main_reply_keyboard(self, chat_id: int) -> None:
        await self.client.send_message(chat_id, "👇 Menu", buttons=self.main_reply_keyboard())

    async def send_start(self, event) -> None:
        chat_id = event.chat_id
        await self.save_user_from_event(event)

        if command_payload(event.raw_text or "").lower() == "video":
            await self.send_main_reply_keyboard(chat_id)
            await self.send_random_video(chat_id, event.sender_id)
            return

        start_message_id = self.start_post_id or (await get_channel_last_message(parse_chat_id(self.forward_channel)) if self.forward_channel else 0)
        if self.forward_channel and start_message_id:
            try:
                source = await self.client.get_messages(parse_chat_id(self.forward_channel), ids=int(start_message_id))
                if source:
                    await self.client.send_message(chat_id, source, buttons=self.start_keyboard())
                    await self.send_main_reply_keyboard(chat_id)
                    return
            except Exception as exc:
                logger.warning("[%s] Could not copy /start post: %s", self.bot_key, exc)

        service_label = "Terabox" if self.provider == "terabox" else "Diskwala"
        await self.client.send_message(
            chat_id,
            f"👋 Welcome!\n\nJoin the channel and tap Watch Video to get a random {service_label} video.",
            buttons=self.start_keyboard(),
        )
        await self.send_main_reply_keyboard(chat_id)

    async def send_random_video(self, chat_id: int, user_id: int | None = None) -> None:
        if user_id:
            await self.save_user(user_id)

        if not await self.user_joined_required_channels(int(chat_id)):
            await self.send_join_prompt(int(chat_id))
            return

        if not self.library_channel:
            await self.client.send_message(
                chat_id,
                f"⚠️ Library channel is not configured for {self.provider} bot.",
            )
            return

        max_message_id = await get_max_library_message_id(self.library_channel, self.max_library_id)
        if max_message_id <= 0:
            await self.client.send_message(
                chat_id,
                f"⚠️ Library max id is missing for {self.provider} channel. Admin can set it with /setmax 20000.",
            )
            return

        library_chat_id = parse_chat_id(self.library_channel)
        min_message_id = max(1, self.start_post_id) if (self.start_post_id and self.start_post_id < max_message_id) else 1

        for _ in range(max(1, RANDOM_VIDEO_RETRIES)):
            message_id = random.randint(min_message_id, max_message_id)
            try:
                source_message = await self.client.get_messages(library_chat_id, ids=message_id)
            except FloodWaitError as exc:
                await asyncio.sleep(exc.seconds)
                continue
            except Exception as exc:
                logger.warning("[%s] Random message lookup failed for %s: %s", self.bot_key, message_id, exc)
                continue

            if not source_message or getattr(source_message, "empty", False) or isinstance(source_message, MessageEmpty):
                continue

            source_text = source_message.raw_text or source_message.message or ""

            if self.provider == "diskwala":
                # Diskwala: requires media attached and valid Diskwala link
                if not source_message.media:
                    continue
                diskwala_link = extract_diskwala_link(source_text)
                if not diskwala_link:
                    continue
                caption_text = self.format_caption(diskwala_link, source_text)
                try:
                    await self.client.send_file(
                        chat_id,
                        file=source_message.media,
                        caption=caption_text,
                        buttons=self.video_keyboard(),
                        link_preview=False,
                    )
                    return
                except FloodWaitError as exc:
                    await asyncio.sleep(exc.seconds)
                    continue
                except Exception as exc:
                    logger.warning("[%s] Diskwala video send failed: %s", self.bot_key, exc)
                    break

            else:
                # Terabox: extracts Terabox link, link_preview=True, blank footer
                target_link = extract_target_link(source_text, source_message, provider=self.provider)
                if not target_link:
                    continue
                caption_text = self.format_caption(target_link, source_text)
                try:
                    is_real_media = bool(source_message.media and not isinstance(source_message.media, MessageMediaWebPage))
                    if self.send_source_media and is_real_media:
                        await self.client.send_file(
                            chat_id,
                            file=source_message.media,
                            caption=caption_text,
                            buttons=self.video_keyboard(),
                        )
                    elif self.link_preview and target_link:
                        try:
                            # Instruct Telegram to force a large preview card
                            await self.client.send_file(
                                chat_id,
                                file=InputMediaWebPage(url=target_link, force_large_media=True, optional=True),
                                caption=caption_text,
                                buttons=self.video_keyboard(),
                            )
                        except Exception as e:
                            logger.warning("[%s] Large preview fallback: %s", self.bot_key, e)
                            await self.client.send_message(
                                chat_id,
                                caption_text,
                                buttons=self.video_keyboard(),
                                link_preview=self.link_preview,
                            )
                    else:
                        await self.client.send_message(
                            chat_id,
                            caption_text,
                            buttons=self.video_keyboard(),
                            link_preview=self.link_preview,
                        )
                    return
                except FloodWaitError as exc:
                    await asyncio.sleep(exc.seconds)
                    continue
                except Exception as exc:
                    logger.warning("[%s] Terabox post send failed: %s", self.bot_key, exc)
                    break

        fail_msg = (
            "😕 Could not find a random media post with a Diskwala link right now. Please try again."
            if self.provider == "diskwala"
            else "😕 Could not find a random Terabox post right now. Please try again."
        )
        await self.client.send_message(chat_id, fail_msg)

    # ----- broadcast -----
    def broadcast_text(self, stats: dict[str, int], running: bool = True) -> str:
        status = "🚀 Broadcast running" if running else "✅ Broadcast finished"
        if stats.get("cancelled"):
            status = "🛑 Broadcast cancelled"
        elapsed = max(1, int(time.time() - stats["started_at"]))
        speed = stats["done"] / elapsed
        return (
            f"{status}\n\n"
            f"🤖 Bot: @{self.bot_username or self.bot_key}\n"
            f"👥 Total: {stats['total']}\n"
            f"📨 Done: {stats['done']}\n"
            f"✅ Sent: {stats['sent']}\n"
            f"❌ Failed: {stats['failed']}\n"
            f"🗑 Deleted: {stats['deleted']}\n"
            f"⚡ Speed: {speed:.1f}/sec"
        )

    async def copy_or_send_broadcast(self, target_user_id: int, source: dict) -> None:
        if source["type"] == "copy":
            await self.client.send_message(target_user_id, source["message"])
            return
        await self.client.send_message(target_user_id, source["text"])

    async def run_broadcast(self, admin_chat_id: int, status_message_id: int, source: dict) -> None:
        job_id = str(status_message_id)
        cancel_event = asyncio.Event()
        stats = {
            "started_at": int(time.time()),
            "total": await self.count_users(),
            "done": 0,
            "sent": 0,
            "failed": 0,
            "deleted": 0,
            "cancelled": 0,
        }
        self.broadcast_jobs[job_id] = {"cancel": cancel_event, "stats": stats}

        last_edit = 0.0
        try:
            try:
                await self.client.edit_message(admin_chat_id, status_message_id, buttons=cancel_keyboard(job_id))
            except Exception:
                pass

            async for user_id in self.iter_users():
                if cancel_event.is_set():
                    stats["cancelled"] = 1
                    break

                try:
                    await self.copy_or_send_broadcast(user_id, source)
                    stats["sent"] += 1
                except FloodWaitError as exc:
                    await asyncio.sleep(exc.seconds)
                    try:
                        await self.copy_or_send_broadcast(user_id, source)
                        stats["sent"] += 1
                    except Exception as retry_exc:
                        stats["failed"] += 1
                        if should_delete_user_after_failure(retry_exc):
                            await self.remove_user(user_id)
                            stats["deleted"] += 1
                except Exception as exc:
                    stats["failed"] += 1
                    if should_delete_user_after_failure(exc):
                        await self.remove_user(user_id)
                        stats["deleted"] += 1

                stats["done"] += 1
                now = time.time()
                if now - last_edit >= 2 or stats["done"] == stats["total"]:
                    last_edit = now
                    try:
                        await self.client.edit_message(
                            admin_chat_id,
                            status_message_id,
                            self.broadcast_text(stats, running=True),
                            buttons=cancel_keyboard(job_id),
                        )
                    except Exception:
                        pass

                await asyncio.sleep(0.04)
        finally:
            self.broadcast_jobs.pop(job_id, None)
            try:
                await self.client.edit_message(admin_chat_id, status_message_id, self.broadcast_text(stats, running=False))
            except Exception:
                pass

    async def start_broadcast(self, event) -> None:
        chat_id = event.chat_id
        user_id = event.sender_id
        if user_id not in OWNER_CHAT_IDS:
            return

        if self.broadcast_jobs:
            await self.client.send_message(chat_id, "⚠️ Broadcast already running.")
            return

        reply_msg = await event.get_reply_message() if event.is_reply else None
        payload = command_payload(event.raw_text or "")
        if reply_msg:
            source = {"type": "copy", "message": reply_msg}
        elif payload:
            source = {"type": "text", "text": payload}
        else:
            await self.client.send_message(chat_id, "📣 Reply to a post with /broadcast, or use /broadcast your message.")
            return

        total = await self.count_users()
        status = await self.client.send_message(
            chat_id,
            f"🚀 Broadcast starting\n\n🤖 Bot: @{self.bot_username or self.bot_key}\n👥 Users: {total}",
            buttons=cancel_keyboard("pending"),
        )
        asyncio.create_task(self.run_broadcast(chat_id, status.id, source))

    # ----- handlers -----
    async def handle_callback(self, event) -> None:
        data = (event.data or b"").decode()
        chat_id = event.chat_id

        if data == "video":
            try:
                await event.answer("🎬 Sending video...")
            except Exception:
                pass
            await self.send_random_video(chat_id, event.sender_id)
            return

        if data.startswith("cancel_broadcast:"):
            if event.sender_id not in OWNER_CHAT_IDS:
                try:
                    await event.answer("Only admin can cancel.", alert=True)
                except Exception:
                    pass
                return
            job_id = data.split(":", 1)[1]
            if job_id == "pending":
                try:
                    await event.answer("Starting...")
                except Exception:
                    pass
                return
            job = self.broadcast_jobs.get(job_id)
            if job:
                job["cancel"].set()
                try:
                    await event.answer("🛑 Cancelling...")
                except Exception:
                    pass
            else:
                try:
                    await event.answer("No active broadcast.")
                except Exception:
                    pass

    async def handle_message(self, event) -> None:
        text = event.raw_text or ""
        cmd = command_name(text)
        chat_id = event.chat_id
        from_user_id = event.sender_id
        normalized_text = text.strip()

        if cmd == "start":
            await self.send_start(event)
        elif cmd == "video":
            await self.save_user_from_event(event)
            await self.send_random_video(chat_id, from_user_id)
        elif normalized_text == JOIN_CHANNELS_TEXT:
            await self.send_start(event)
        elif normalized_text in {WATCH_VIDEO_TEXT, WATCH_NEXT_TEXT}:
            await self.save_user_from_event(event)
            await self.send_random_video(chat_id, from_user_id)
        elif normalized_text in OFFER_CHANNEL_TEXT_MAP:
            await self.send_offer_channel(chat_id, OFFER_CHANNEL_TEXT_MAP[normalized_text])
        elif cmd == "broadcast":
            await self.start_broadcast(event)
        elif cmd == "setmax" and from_user_id in OWNER_CHAT_IDS:
            payload = command_payload(text)
            if not payload.isdigit() or int(payload) <= 0:
                await self.client.send_message(chat_id, "Use: /setmax 20000")
                return
            if not self.library_channel:
                await self.client.send_message(chat_id, f"⚠️ No library channel configured for this {self.provider} bot.")
                return
            await set_library_max_message_id(self.library_channel, int(payload))
            await self.client.send_message(
                chat_id,
                f"✅ Library max message id saved for {self.provider} ({self.library_channel}): {int(payload)}",
            )
        elif cmd == "status" and from_user_id in OWNER_CHAT_IDS:
            await self.client.send_message(
                chat_id,
                f"📊 Users started for @{self.bot_username or self.bot_key} [{self.provider}]: {await self.count_users()}",
            )
        elif cmd == "stats" and from_user_id in OWNER_CHAT_IDS:
            max_id = await get_max_library_message_id(self.library_channel, self.max_library_id) if self.library_channel else 0
            await self.client.send_message(
                chat_id,
                (
                    f"📊 Users for @{self.bot_username or self.bot_key} [{self.provider}]: {await self.count_users()}\n"
                    f"📁 Library channel: {self.library_channel}\n"
                    f"🔢 Library max id: {max_id}"
                ),
            )

    def register_handlers(self) -> None:
        @self.client.on(events.NewMessage(incoming=True))
        async def on_new_message(event):
            try:
                if event.is_channel and not event.is_group:
                    library_chat_id = parse_chat_id(self.library_channel) if self.library_channel else None
                    forward_chat_id = parse_chat_id(self.forward_channel) if self.forward_channel else None
                    monitored_chats = {c for c in (library_chat_id, forward_chat_id) if c}
                    if event.chat_id in monitored_chats:
                        chat = await event.get_chat()
                        await set_channel_last_message(event.chat_id, event.id, getattr(chat, "username", "") or "")
                    return

                if not event.is_private:
                    return

                if should_delete_livegram_message(event.raw_text or ""):
                    await delete_message_safely(event)
                    return

                await self.handle_message(event)
            except USER_GONE_ERRORS:
                user_id = event.sender_id
                if user_id:
                    await self.remove_user(user_id)
                    logger.info("[%s] Removed blocked/deactivated user %s from db", self.bot_key, user_id)
            except Exception:
                logger.exception("[%s] Unhandled message error", self.bot_key)

        @self.client.on(events.CallbackQuery())
        async def on_callback(event):
            try:
                await self.handle_callback(event)
            except Exception:
                logger.exception("[%s] Unhandled callback error", self.bot_key)

    async def resolve_identity(self) -> None:
        me = await self.client.get_me()
        self.bot_username = me.username or ""
        self.bot_id = int(me.id)
        if not self.bot_key:
            self.bot_key = self.bot_username or str(self.bot_id)
        self.bot_key_safe = safe_field(self.bot_key)
        logger.info("Bot identity: @%s (%s), db key=%s [%s]", self.bot_username, self.bot_id, self.bot_key_safe, self.provider)

    async def notify_owner(self, text: str) -> None:
        for owner_id in OWNER_CHAT_IDS:
            try:
                await self.client.send_message(owner_id, text)
            except Exception:
                logger.warning(
                    "[%s] Owner notify failed for %s - owner must /start this bot at least once first.",
                    self.bot_key,
                    owner_id,
                )

    async def resolve_peers_before_run(self) -> None:
        # get_dialogs() is restricted for bot accounts (bots can't use it), so
        # channel peers can only be resolved by id directly - retry a few times.
        targets = []
        if self.library_channel:
            targets.append((parse_chat_id(self.library_channel), f"library ({self.provider})"))
        if self.forward_channel:
            targets.append((parse_chat_id(self.forward_channel), f"forward ({self.provider})"))

        for target, name in targets:
            for attempt in range(5):
                try:
                    await self.client.get_entity(target)
                    logger.info("[%s] Resolved %s channel peer.", self.bot_key, name)
                    break
                except Exception as exc:
                    logger.warning("[%s] %s channel peer resolve attempt %s/5 failed: %s", self.bot_key, name, attempt + 1, exc)
                    await asyncio.sleep(2 * (attempt + 1))
            else:
                logger.warning(
                    "[%s] %s channel peer still unresolved after boot. Will self-heal once "
                    "a live update from that channel arrives.",
                    self.bot_key,
                    name,
                )

    async def run(self) -> None:
        try:
            await self.client.start(bot_token=self.token)
            await self.resolve_identity()
            self.register_handlers()
            await self.resolve_peers_before_run()
            await self.notify_owner(f"✅ @{self.bot_username or self.bot_key} [{self.provider}] started.")
            await self.client.run_until_disconnected()
        except Exception as exc:
            logger.exception("[%s] Bot crashed and is not responding", self.bot_key)
            try:
                await self.notify_owner(f"🛑 @{self.bot_username or self.bot_key} [{self.provider}] crashed: {exc}")
            except Exception:
                pass
        finally:
            try:
                if self.client.is_connected():
                    await self.client.disconnect()
            except Exception as exc:
                logger.warning("[%s] Client disconnect failed (continuing cleanup): %s", self.bot_key, exc)


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


async def main() -> None:
    if not API_ID or not API_HASH:
        raise RuntimeError("API_ID and API_HASH are required to read random old library messages")

    bot_configs = parse_bot_configs()
    if not bot_configs:
        raise RuntimeError(
            "No bots configured! Set BOT_TOKENS (for Diskwala) and/or TERABOX_BOT_TOKENS (for Terabox) in .env"
        )

    diskwala_count = sum(1 for c in bot_configs if c["provider"] == "diskwala")
    terabox_count = sum(1 for c in bot_configs if c["provider"] == "terabox")

    if diskwala_count > 0 and not any(c["library_channel"] for c in bot_configs if c["provider"] == "diskwala"):
        raise RuntimeError("LIBRARY_TELEGRAM_CHANNEL is required in .env for Diskwala bots")

    if terabox_count > 0 and not any(c["library_channel"] for c in bot_configs if c["provider"] == "terabox"):
        raise RuntimeError("TERABOX_LIBRARY_CHANNEL is required in .env for Terabox bots")

    logger.info("Initializing multi-bot runner with %d bots (%d Diskwala, %d Terabox)...",
                len(bot_configs), diskwala_count, terabox_count)

    await init_mongo()
    health_runner = await start_health_server()

    runtimes = [BotRuntime(config) for config in bot_configs]

    stopping = asyncio.Event()

    def _request_stop() -> None:
        if not stopping.is_set():
            stopping.set()
            asyncio.create_task(_graceful_stop())

    async def _graceful_stop() -> None:
        logger.info("Shutdown signal received.")
        await asyncio.gather(
            *(runtime.notify_owner(f"⏹ @{runtime.bot_username or runtime.bot_key} is stopping.") for runtime in runtimes),
            return_exceptions=True,
        )
        await asyncio.gather(*(runtime.client.disconnect() for runtime in runtimes), return_exceptions=True)

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, _request_stop)
        except (NotImplementedError, RuntimeError):
            pass  # e.g. Windows, which lacks add_signal_handler support

    try:
        await asyncio.gather(*(runtime.run() for runtime in runtimes))
    finally:
        await health_runner.cleanup()
        if mongo_client:
            mongo_client.close()


if __name__ == "__main__":
    asyncio.run(main())
