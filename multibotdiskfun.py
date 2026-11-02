import asyncio
import logging
import os
import random
import re
import signal
import sys
import time
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlparse

if sys.platform.startswith("win"):
    try:
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    except Exception:
        pass

import aiohttp.web
from dotenv import load_dotenv
from telethon import TelegramClient, Button, events, utils
from telethon.tl.types import (
    MessageEmpty,
    MessageMediaWebPage,
    InputMediaWebPage,
    UpdateBotChatInviteRequester,
    PeerChannel,
    PeerChat,
    PeerUser,
    ChatInvite,
    ChatInviteAlready,
)
from telethon.tl.functions.messages import CheckChatInviteRequest, ExportChatInviteRequest
from telethon.errors import (
    ChatAdminRequiredError,
    ChatWriteForbiddenError,
    ChannelPrivateError,
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
MONGO_SETTINGS_COLLECTION = os.getenv("MONGO_SETTINGS_COLLECTION", "runtime_settings").strip()
MONGO_JOIN_REQUESTS_COLLECTION = os.getenv("MONGO_JOIN_REQUESTS_COLLECTION", "join_requests").strip()
RANDOM_VIDEO_RETRIES = int(os.getenv("RANDOM_VIDEO_RETRIES", "35") or 35)

WATCH_VIDEO_TEXT = os.getenv("WATCH_VIDEO_TEXT", "🔞 Watch Now").strip() or "🎬 Watch Now"
WATCH_NEXT_TEXT = os.getenv("WATCH_NEXT_TEXT", "👙 Watch Next").strip() or "⏭ Watch Next"
JOIN_CHANNELS_TEXT = os.getenv("JOIN_CHANNELS_TEXT", "Join VIRALS").strip() or "📢 Join Viral Channel"

# Join request TTL (hours converted to seconds; 0 = never expire until restart/clear)
FORCE_SUB_REQUEST_TTL_SECONDS = int(float(os.getenv("FORCE_SUB_REQUEST_TTL_HOURS", "0") or 0) * 3600)

# Global in-memory caches
_force_sub_id_cache: dict[str, int] = {}
_force_sub_title_cache: dict[str, str] = {}
_force_sub_url_cache: dict[str, str] = {}

# user_id -> {chat_id: timestamp} for pending join requests
_join_requests: dict[int, dict[int, float]] = {}

# All active BotRuntime instances in the current process
ALL_RUNTIMES: list["BotRuntime"] = []


def parse_initial_offer_channels() -> list[dict[str, str]]:
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


INITIAL_OFFER_CHANNELS = parse_initial_offer_channels()


def parse_initial_force_sub_channels() -> list[dict[str, str]]:
    """
    Parses initial channels from environment variables.
    Supports:
    1. TeraDl style:
       FORCE_SUB_CHANNELS / FORCE_SUB_CHANNEL (-1001234567890, @mychannel)
       FORCE_SUB_INVITE_URLS / FORCE_SUB_INVITE_URL (https://t.me/+xyz, ...)
    2. multibotdiskfun style:
       JOIN_CHANNELS (Title|URL|check_chat or @channel)
       JOIN_CHANNEL_1, JOIN_CHANNEL_2, ...
    """
    channels: list[dict[str, str]] = []

    # 1. TeraDl style
    raw_fsub = (os.getenv("FORCE_SUB_CHANNELS", "").strip() or os.getenv("FORCE_SUB_CHANNEL", "").strip())
    raw_invites = (os.getenv("FORCE_SUB_INVITE_URLS", "").strip() or os.getenv("FORCE_SUB_INVITE_URL", "").strip())

    fsub_list = [p.strip() for p in re.split(r"[,;\r\n]+", raw_fsub) if p.strip()]
    invite_list = [p.strip() for p in re.split(r"[,;\r\n]+", raw_invites) if p.strip()]

    for idx, ref in enumerate(fsub_list):
        inv = invite_list[idx] if idx < len(invite_list) else ""
        if not inv and ref.startswith("@"):
            inv = f"https://t.me/{ref.lstrip('@')}"
        title = ref if ref.startswith("@") else f"Channel {idx + 1}"
        channels.append({
            "channel": ref,
            "invite_url": inv,
            "title": title,
        })

    # 2. multibotdiskfun JOIN_CHANNELS style
    raw_join = os.getenv("JOIN_CHANNELS", "").strip()
    join_items: list[str] = [p.strip() for p in raw_join.split(";") if p.strip()]
    for idx in range(1, 21):
        item = os.getenv(f"JOIN_CHANNEL_{idx}", "").strip()
        if item:
            join_items.append(item)

    for item in join_items:
        parts = [p.strip() for p in item.split("|") if p.strip()]
        if not parts:
            continue
        if len(parts) == 1 and parts[0].startswith("@"):
            channels.append({
                "channel": parts[0],
                "invite_url": f"https://t.me/{parts[0].lstrip('@')}",
                "title": parts[0],
            })
        elif len(parts) == 1 and parts[0].startswith("http"):
            channels.append({
                "channel": "",
                "invite_url": parts[0],
                "title": JOIN_CHANNELS_TEXT,
            })
        else:
            title = parts[0]
            url = parts[1] if len(parts) > 1 else ""
            check_chat = parts[2] if len(parts) > 2 else ""
            channels.append({
                "channel": check_chat or url,
                "invite_url": url,
                "title": title,
            })

    # Deduplicate while preserving order
    seen = set()
    unique_channels: list[dict[str, str]] = []
    for c in channels:
        key = (str(c.get("channel") or "").strip().lower(), str(c.get("invite_url") or "").strip().lower())
        if key not in seen and (c.get("channel") or c.get("invite_url")):
            seen.add(key)
            unique_channels.append(c)

    return unique_channels


# Global Runtime Settings (persisted in MongoDB, cached in memory)
RUNTIME_SETTINGS: dict[str, Any] = {
    "fsub_enabled": True,
    "force_sub_channels": parse_initial_force_sub_channels(),
    "offer_channels": list(INITIAL_OFFER_CHANNELS),
    "join_channels_text": JOIN_CHANNELS_TEXT,
    "footer_text": FOOTER_TEXT,
    "terabox_footer_text": "",
    "diskwala_footer_text": "",
    "header_text": HEADER_TEXT,
    "tutorial_url": TUTORIAL_URL,
    "trending_url": TRENDING_URL,
}

mongo_client = None
mongo_db = None
users_col = None
channels_col = None
settings_col = None
join_requests_col = None
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


def extract_invite_hash(url: str) -> str:
    url = (url or "").strip()
    match = re.search(r"(?:t\.me|telegram\.me)/(?:\+|joinchat/)([a-zA-Z0-9_-]+)", url)
    return match.group(1) if match else ""


def normalize_channel_id(cid: int | str) -> int | None:
    raw = str(cid).strip()
    if raw.startswith("-100") and raw[4:].isdigit():
        return int(raw)
    if raw.isdigit():
        return -int(f"100{raw}")
    if raw.startswith("-") and raw[1:].isdigit():
        return int(raw)
    return None


# ===== RUNTIME SETTINGS HELPERS =====
def get_runtime_fsub_channels() -> list[dict[str, str]]:
    return RUNTIME_SETTINGS.get("force_sub_channels") or []


def get_runtime_offer_channels() -> list[dict[str, str]]:
    offers = RUNTIME_SETTINGS.get("offer_channels")
    if offers is not None:
        return offers
    return INITIAL_OFFER_CHANNELS


def get_runtime_footer(provider: str = "") -> str:
    p = provider.lower().strip()
    if p == "terabox":
        return RUNTIME_SETTINGS.get("terabox_footer_text") or RUNTIME_SETTINGS.get("footer_text") or ""
    elif p == "diskwala":
        return RUNTIME_SETTINGS.get("diskwala_footer_text") or RUNTIME_SETTINGS.get("footer_text") or ""
    return RUNTIME_SETTINGS.get("footer_text") or ""


def get_runtime_header() -> str:
    return RUNTIME_SETTINGS.get("header_text") or HEADER_TEXT or ""


def get_runtime_tutorial_url() -> str:
    return RUNTIME_SETTINGS.get("tutorial_url") or TUTORIAL_URL or "https://t.me/howdisk/2"


def get_runtime_trending_url() -> str:
    return RUNTIME_SETTINGS.get("trending_url") or TRENDING_URL or "bitly.cx/diskwala"


def get_runtime_join_channels_text() -> str:
    return RUNTIME_SETTINGS.get("join_channels_text") or JOIN_CHANNELS_TEXT or "Join VIRALS"


async def save_runtime_settings() -> None:
    if settings_col is not None:
        try:
            doc = dict(RUNTIME_SETTINGS)
            doc["_id"] = "global_config"
            doc["updated_at"] = utc_now()
            await settings_col.update_one({"_id": "global_config"}, {"$set": doc}, upsert=True)
            logger.info("Runtime settings saved to MongoDB.")
        except Exception as exc:
            logger.warning("Could not persist runtime settings to Mongo: %s", exc)


async def load_runtime_settings_from_mongo() -> None:
    if settings_col is None:
        return
    try:
        doc = await settings_col.find_one({"_id": "global_config"})
        if doc:
            for k, v in doc.items():
                if k not in ("_id", "updated_at"):
                    RUNTIME_SETTINGS[k] = v
            logger.info("Loaded runtime settings from MongoDB: %d FSUB channels, %d offer channels.",
                        len(RUNTIME_SETTINGS.get("force_sub_channels", [])),
                        len(RUNTIME_SETTINGS.get("offer_channels", [])))
        else:
            await save_runtime_settings()
    except Exception as exc:
        logger.warning("Could not load runtime settings from Mongo: %s", exc)


# ===== JOIN REQUEST MANAGEMENT =====
def record_join_request(user_id: int, chat_id: int) -> None:
    now = time.time()
    uid = int(user_id)
    cid = int(chat_id)
    _join_requests.setdefault(uid, {})[cid] = now
    if join_requests_col is not None:
        asyncio.create_task(_save_join_request_to_mongo(uid, cid, now))


async def _save_join_request_to_mongo(user_id: int, chat_id: int, ts: float) -> None:
    if join_requests_col is None:
        return
    try:
        await join_requests_col.update_one(
            {"user_id": user_id, "chat_id": chat_id},
            {"$set": {"requested_at": ts, "updated_at": utc_now()}},
            upsert=True,
        )
    except Exception as exc:
        logger.debug("Mongo join request save error: %s", exc)


async def load_join_requests_from_mongo() -> None:
    if join_requests_col is None:
        return
    try:
        cursor = join_requests_col.find({})
        count = 0
        async for doc in cursor:
            uid = doc.get("user_id")
            cid = doc.get("chat_id")
            ts = doc.get("requested_at", time.time())
            if uid and cid:
                _join_requests.setdefault(int(uid), {})[int(cid)] = float(ts)
                count += 1
        logger.info("Loaded %d pending join requests from MongoDB.", count)
    except Exception as exc:
        logger.warning("Could not load join requests from Mongo: %s", exc)


def has_join_request(user_id: int, channel_target: int | str) -> bool:
    try:
        uid = int(user_id)
        chats = _join_requests.get(uid) or {}
        if not chats:
            return False

        now = time.time()
        # Direct check if target is integer ID
        cid = normalize_channel_id(channel_target)
        if cid is not None:
            ts = chats.get(cid)
            if ts is not None:
                if FORCE_SUB_REQUEST_TTL_SECONDS <= 0 or (now - ts) <= FORCE_SUB_REQUEST_TTL_SECONDS:
                    return True

        # Check cached channel ID for username / reference
        ref_str = str(channel_target).strip()
        cached_id = _force_sub_id_cache.get(ref_str)
        if cached_id is not None:
            ts = chats.get(cached_id)
            if ts is not None:
                if FORCE_SUB_REQUEST_TTL_SECONDS <= 0 or (now - ts) <= FORCE_SUB_REQUEST_TTL_SECONDS:
                    return True
    except Exception:
        pass
    return False


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
    active_footer = footer_text if footer_text is not None else get_runtime_footer(active_provider)
    if active_provider == "diskwala" and not active_footer:
        tutorial = get_runtime_tutorial_url()
        trending = get_runtime_trending_url()
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
        header = get_runtime_header()
        if header:
            parts.append(header)
        if link:
            parts.append(link)
        elif original_text:
            parts.append(original_text.strip())

    if active_footer:
        parts.append(active_footer)

    return "\n\n".join(parts)


def video_caption(diskwala_link: str) -> str:
    """Legacy alias for backward compatibility."""
    return format_post_caption(diskwala_link, provider="diskwala")


def parse_join_channels() -> list[dict[str, str]]:
    """Legacy helper maintained for backward compatibility."""
    return get_runtime_fsub_channels()


def cancel_keyboard(job_id: str) -> list:
    return [[Button.inline("🛑 Cancel Broadcast", f"cancel_broadcast:{job_id}".encode())]]


# ===== PER-BOT CONFIG =====
def parse_bot_configs() -> list[dict[str, Any]]:
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
            "tutorial_url": TUTORIAL_URL,
            "trending_url": TRENDING_URL,
            "send_source_media": SEND_SOURCE_MEDIA,
            "use_source_caption": USE_SOURCE_CAPTION,
        })

    return configs


# ===== SHARED MONGO (users/channel state/settings shared across bots) =====
async def init_mongo() -> None:
    global mongo_client, mongo_db, users_col, channels_col, settings_col, join_requests_col
    if not MONGO_URI:
        logger.warning("MONGO_URI is empty. Users and settings will be kept only in memory.")
        return
    if AsyncIOMotorClient is None:
        logger.warning("motor is not installed. Users and settings will be kept only in memory.")
        return

    mongo_client = AsyncIOMotorClient(MONGO_URI)
    mongo_db = mongo_client[MONGO_DB_NAME]
    users_col = mongo_db[MONGO_USERS_COLLECTION]
    channels_col = mongo_db[MONGO_CHANNELS_COLLECTION]
    settings_col = mongo_db[MONGO_SETTINGS_COLLECTION]
    join_requests_col = mongo_db[MONGO_JOIN_REQUESTS_COLLECTION]

    try:
        await users_col.create_index("user_id", unique=True)
        await users_col.create_index("bot_keys")
        await join_requests_col.create_index([("user_id", 1), ("chat_id", 1)], unique=True)
    except Exception as exc:
        logger.debug("Mongo index creation notice: %s", exc)

    logger.info("MongoDB connected: %s", MONGO_DB_NAME)
    await load_runtime_settings_from_mongo()
    await load_join_requests_from_mongo()


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


async def count_total_unique_users() -> int:
    if users_col is not None:
        try:
            return await users_col.count_documents({})
        except Exception:
            pass
    total = set()
    for r in ALL_RUNTIMES:
        total.update(r.memory_users)
    return len(total)


def parse_addfsub_args(payload: str) -> tuple[str, str, str]:
    """
    Parses payload into (channel, invite_url, title).
    """
    parts = payload.strip().split()
    if not parts:
        return "", "", ""
    first = parts[0]
    second = parts[1] if len(parts) > 1 else ""
    title = " ".join(parts[2:]) if len(parts) > 2 else ""

    if first.startswith(("http://", "https://")):
        return "", first, " ".join(parts[1:]) if len(parts) > 1 else ""

    channel = first
    invite_url = second if second.startswith(("http://", "https://")) else ""
    if not invite_url and second:
        title = " ".join(parts[1:])
    return channel, invite_url, title


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
        ALL_RUNTIMES.append(self)

    def format_caption(self, target_link: str, source_text: str = "") -> str:
        if self.provider == "diskwala":
            tutorial = get_runtime_tutorial_url() or self.tutorial_url or "https://t.me/howdisk/2"
            trending = get_runtime_trending_url() or self.trending_url or "bitly.cx/diskwala"
            footer = get_runtime_footer("diskwala")
            base = (
                "🎬 **Vdo 😍**\n\n"
                "**🔗🔗यह रहा वीडियो लिंक 👇**\n\n"
                f"{target_link}\n\n"
                "**🤔 How to Open Links? | लिंक कैसे खोलें 👇**\n"
                f"[📖 View Tutorial]({tutorial})\n\n"
                "😉**Must Try Terabox Virals 👇**\n"
                f"{trending}\n"
                f"{trending}"
            )
            if footer:
                return f"{base}\n\n{footer}"
            return base

        # Terabox caption
        parts: list[str] = []
        if self.use_source_caption and source_text:
            parts.append(source_text.strip())
        else:
            header = get_runtime_header() or self.header_text or "🎬 **Vdo 😍**\n\n**🔗🔗यह रहा वीडियो लिंक 👇**"
            if header:
                parts.append(header)
            if target_link:
                parts.append(target_link)
            elif source_text:
                parts.append(source_text.strip())

        footer = get_runtime_footer("terabox") or self.footer_text
        if footer:
            parts.append(footer)

        return "\n\n".join(parts)

    # ----- keyboards -----
    def video_action_keyboard(self, action_text: str) -> list:
        rows = [[Button.inline(action_text, b"video")]]
        offers = get_runtime_offer_channels()
        for channel in offers:
            rows.append([Button.url(channel["text"], channel["url"])])
        return rows

    def start_keyboard(self) -> list:
        return self.video_action_keyboard(WATCH_VIDEO_TEXT)

    def video_keyboard(self) -> list:
        return self.video_action_keyboard(WATCH_NEXT_TEXT)

    def main_reply_keyboard(self) -> list:
        join_btn = get_runtime_join_channels_text()
        row = [Button.text(join_btn, resize=True), Button.text(WATCH_VIDEO_TEXT, resize=True)]
        rows = [row, row, row]
        offers = get_runtime_offer_channels()
        for channel in offers:
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

    # ----- Force Subscribe & Membership Checks -----
    async def resolve_force_sub_peers(self) -> None:
        """Warm up peer access hashes and title cache for all configured FSUB channels."""
        channels = get_runtime_fsub_channels()
        for ch in channels:
            ref = str(ch.get("channel") or "").strip()
            inv = str(ch.get("invite_url") or "").strip()
            if not ref and not inv:
                continue
            if ref:
                try:
                    target = parse_chat_id(ref)
                    entity = await self.client.get_entity(target)
                    _force_sub_id_cache[ref] = utils.get_peer_id(entity)
                    if getattr(entity, "title", None) and not ch.get("title"):
                        _force_sub_title_cache[ref] = entity.title
                    logger.debug("[%s] Resolved FSUB peer: %s", self.bot_key, ref)
                except Exception as exc:
                    logger.debug("[%s] Could not directly resolve %s: %s", self.bot_key, ref, exc)

            # Try resolving via invite hash if present
            hash_str = extract_invite_hash(inv)
            if hash_str and ref not in _force_sub_id_cache:
                try:
                    res = await self.client(CheckChatInviteRequest(hash_str))
                    if isinstance(res, ChatInviteAlready) and getattr(res, "chat", None):
                        c = res.chat
                        _force_sub_id_cache[ref or inv] = utils.get_peer_id(c)
                        if getattr(c, "title", None) and not ch.get("title"):
                            _force_sub_title_cache[ref or inv] = c.title
                    elif isinstance(res, ChatInvite) and getattr(res, "title", None) and not ch.get("title"):
                        _force_sub_title_cache[ref or inv] = res.title
                except Exception:
                    pass

    async def resolve_channel_invite_url(self, ch: dict) -> str:
        inv = (ch.get("invite_url") or "").strip()
        if inv.startswith(("http://", "https://")):
            return inv

        raw = str(ch.get("channel") or "").strip()
        if raw.startswith("@"):
            return f"https://t.me/{raw.lstrip('@')}"

        cached = _force_sub_url_cache.get(raw)
        if cached:
            return cached

        target = parse_chat_id(raw)
        if target:
            try:
                entity = await self.client.get_entity(target)
                if getattr(entity, "username", None):
                    link = f"https://t.me/{entity.username}"
                    _force_sub_url_cache[raw] = link
                    return link
                # Try export chat invite link if admin
                res = await self.client(ExportChatInviteRequest(entity))
                link = getattr(res, "link", "")
                if link:
                    _force_sub_url_cache[raw] = link
                    return link
            except Exception:
                pass
        return inv

    async def resolve_channel_title(self, ch: dict) -> str:
        if ch.get("title"):
            return ch["title"]
        raw = str(ch.get("channel") or "").strip()
        if raw in _force_sub_title_cache:
            return _force_sub_title_cache[raw]
        if raw.startswith("@"):
            return raw
        target = parse_chat_id(raw)
        if target:
            try:
                entity = await self.client.get_entity(target)
                title = getattr(entity, "title", "")
                if title:
                    _force_sub_title_cache[raw] = title
                    return title
            except Exception:
                pass
        return raw or "Channel"

    async def is_user_subscribed_to(self, user_id: int, ch: dict) -> bool:
        channel_ref = ch.get("channel")
        invite_url = ch.get("invite_url", "")

        # If channel ref is missing and only invite URL was given: unverifiable, pass
        if not channel_ref and invite_url:
            return True

        if not channel_ref:
            return True

        # 1. Check if user has a pending join request on record
        if has_join_request(user_id, channel_ref):
            return True

        # 2. Check Telethon membership permissions
        target = parse_chat_id(str(channel_ref))
        try:
            perms = await self.client.get_permissions(target, user_id)
            if perms:
                if getattr(perms, "is_banned", False) or getattr(perms, "has_left", False):
                    return False
                return True
        except UserNotParticipantError:
            return has_join_request(user_id, channel_ref)
        except ChatAdminRequiredError:
            logger.warning("[%s] Bot is not admin in %s, cannot verify participants.", self.bot_key, channel_ref)
            return has_join_request(user_id, channel_ref)
        except (ValueError, ChannelPrivateError):
            # Try resolving entity if not cached in session
            try:
                entity = await self.client.get_entity(target)
                perms = await self.client.get_permissions(entity, user_id)
                if perms:
                    if getattr(perms, "is_banned", False) or getattr(perms, "has_left", False):
                        return False
                    return True
            except Exception:
                pass
            return has_join_request(user_id, channel_ref)
        except Exception as exc:
            logger.warning("[%s] Error verifying subscription in %s: %s", self.bot_key, channel_ref, exc)
            return has_join_request(user_id, channel_ref)

        return False

    async def check_user_subscription(self, user_id: int) -> tuple[bool, list[dict]]:
        if not RUNTIME_SETTINGS.get("fsub_enabled", True):
            return True, []

        channels = get_runtime_fsub_channels()
        if not channels:
            return True, []

        missing = []
        for ch in channels:
            is_sub = await self.is_user_subscribed_to(user_id, ch)
            if not is_sub:
                missing.append(ch)

        return len(missing) == 0, missing

    async def user_joined_required_channels(self, user_id: int) -> bool:
        """Maintained for backward compatibility."""
        is_sub, _ = await self.check_user_subscription(user_id)
        return is_sub

    def format_force_sub_prompt_text(self, missing_channels: list[dict]) -> str:
        count = len(missing_channels)
        if count == 1:
            ch_name = missing_channels[0].get("title") or "our channel"
            return (
                "🔒 **Access Required!**\n\n"
                f"To watch videos, please join **{ch_name}** first.\n\n"
                "👉 Tap the button below to join, then tap **Check Joined**."
            )
        else:
            return (
                f"🔒 **Access Required ({count} Channels)**\n\n"
                "To watch videos, please join all the channels below:\n\n"
                "👉 Tap each button to join, then tap **Check Joined**."
            )

    async def build_force_sub_keyboard(self, missing_channels: list[dict]) -> list:
        rows = []
        for idx, ch in enumerate(missing_channels, start=1):
            url = await self.resolve_channel_invite_url(ch)
            title = ch.get("title") or await self.resolve_channel_title(ch) or f"Channel {idx}"
            if url:
                rows.append([Button.url(f"📢 Join {title}", url)])
        rows.append([Button.inline("✅ Check Joined", b"check_join")])
        return rows

    async def send_force_sub_prompt(self, chat_id: int, missing_channels: list[dict] | None = None) -> None:
        if missing_channels is None:
            _, missing_channels = await self.check_user_subscription(chat_id)
        if not missing_channels:
            return
        text = self.format_force_sub_prompt_text(missing_channels)
        buttons = await self.build_force_sub_keyboard(missing_channels)
        await self.client.send_message(chat_id, text, buttons=buttons)

    # ----- messages -----
    async def send_offer_channel(self, chat_id: int, channel: dict[str, str]) -> None:
        await self.client.send_message(
            chat_id,
            "💰 Join our Daily Earning Channel:",
            buttons=[[Button.url(channel["text"], channel["url"])]],
        )

    async def send_join_prompt(self, chat_id: int) -> None:
        await self.send_force_sub_prompt(chat_id)

    async def send_main_reply_keyboard(self, chat_id: int) -> None:
        await self.client.send_message(chat_id, "👇 Menu", buttons=self.main_reply_keyboard())

    async def send_start(self, event) -> None:
        chat_id = event.chat_id
        await self.save_user_from_event(event)

        if command_payload(event.raw_text or "").lower() == "video":
            await self.send_main_reply_keyboard(chat_id)
            is_sub, missing = await self.check_user_subscription(event.sender_id)
            if not is_sub:
                await self.send_force_sub_prompt(chat_id, missing)
                return
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
        target_uid = int(user_id or chat_id)
        if user_id:
            await self.save_user(user_id)

        # Force subscribe gate check
        is_sub, missing = await self.check_user_subscription(target_uid)
        if not is_sub:
            await self.send_force_sub_prompt(int(chat_id), missing)
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
                    except USER_GONE_ERRORS:
                        await self.remove_user(user_id)
                        stats["deleted"] += 1
                    except Exception:
                        stats["failed"] += 1
                except USER_GONE_ERRORS:
                    await self.remove_user(user_id)
                    stats["deleted"] += 1
                except Exception:
                    stats["failed"] += 1

                stats["done"] += 1
                now = time.time()
                if now - last_edit >= 3.0:
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
            try:
                await self.client.edit_message(
                    admin_chat_id,
                    status_message_id,
                    self.broadcast_text(stats, running=False),
                    buttons=None,
                )
            except Exception:
                pass
            self.broadcast_jobs.pop(job_id, None)

    async def start_broadcast(self, event) -> None:
        chat_id = event.chat_id
        if event.sender_id not in OWNER_CHAT_IDS:
            await self.client.send_message(chat_id, "Only owner can broadcast.")
            return

        source = None
        if event.is_reply:
            reply_msg = await event.get_reply_message()
            if reply_msg:
                source = {"type": "copy", "message": reply_msg}

        if source is None:
            text = command_payload(event.raw_text or "")
            if text:
                source = {"type": "text", "text": text}

        if source is None:
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
        user_id = event.sender_id

        # 1. Video button callback (Watch Now / Watch Next)
        if data == "video":
            is_sub, missing = await self.check_user_subscription(user_id)
            if not is_sub:
                try:
                    await event.answer("⚠️ Please join our channel(s) first to watch videos!", alert=True)
                except Exception:
                    pass
                await self.send_force_sub_prompt(chat_id, missing)
                return

            try:
                await event.answer("🎬 Sending video...")
            except Exception:
                pass
            await self.send_random_video(chat_id, user_id)
            return

        # 2. Check Joined verification button callback
        if data == "check_join":
            is_sub, missing = await self.check_user_subscription(user_id)
            if is_sub:
                try:
                    await event.answer("✅ Verification successful! Loading video...", alert=False)
                except Exception:
                    pass
                try:
                    await event.edit("🎉 **Verification successful! Loading your video now...**", buttons=None)
                except Exception:
                    pass
                await self.send_random_video(chat_id, user_id)
                return
            else:
                try:
                    await event.answer("❌ You haven't joined all required channels yet! Please join and try again.", alert=True)
                except Exception:
                    pass
                try:
                    await event.edit(
                        self.format_force_sub_prompt_text(missing),
                        buttons=await self.build_force_sub_keyboard(missing),
                    )
                except Exception:
                    pass
                return

        # 3. Broadcast cancellation
        if data.startswith("cancel_broadcast:"):
            if user_id not in OWNER_CHAT_IDS:
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
            return

        # 4. Admin control panel callbacks
        if data.startswith("admin:"):
            if user_id not in OWNER_CHAT_IDS:
                try:
                    await event.answer("⛔ Only owners can use admin controls.", alert=True)
                except Exception:
                    pass
                return
            await self.handle_admin_callback(event, data)
            return

    async def handle_admin_callback(self, event, data: str) -> None:
        if data == "admin:main":
            text_out, buttons = await build_admin_main_message_and_keyboard()
            await event.edit(text_out, buttons=buttons)
            return

        if data == "admin:fsub":
            text_out, buttons = await build_admin_fsub_message_and_keyboard()
            await event.edit(text_out, buttons=buttons)
            return

        if data == "admin:toggle_fsub":
            RUNTIME_SETTINGS["fsub_enabled"] = not RUNTIME_SETTINGS.get("fsub_enabled", True)
            await save_runtime_settings()
            state = "🟢 FSUB Enabled!" if RUNTIME_SETTINGS["fsub_enabled"] else "🔴 FSUB Disabled!"
            try:
                await event.answer(state, alert=False)
            except Exception:
                pass
            text_out, buttons = await build_admin_fsub_message_and_keyboard()
            await event.edit(text_out, buttons=buttons)
            return

        if data.startswith("admin:delfsub:"):
            idx_str = data.split(":", 2)[2]
            channels = RUNTIME_SETTINGS.get("force_sub_channels", [])
            if idx_str.isdigit():
                idx = int(idx_str) - 1
                if 0 <= idx < len(channels):
                    removed = channels.pop(idx)
                    await save_runtime_settings()
                    try:
                        await event.answer(f"✅ Removed {removed.get('title', 'Channel')}!", alert=False)
                    except Exception:
                        pass
            text_out, buttons = await build_admin_fsub_message_and_keyboard()
            await event.edit(text_out, buttons=buttons)
            return

        if data == "admin:clear_fsub":
            RUNTIME_SETTINGS["force_sub_channels"] = []
            await save_runtime_settings()
            try:
                await event.answer("✅ Cleared all FSUB channels!", alert=False)
            except Exception:
                pass
            text_out, buttons = await build_admin_fsub_message_and_keyboard()
            await event.edit(text_out, buttons=buttons)
            return

        if data == "admin:offer":
            text_out, buttons = build_admin_offer_message_and_keyboard()
            await event.edit(text_out, buttons=buttons)
            return

        if data.startswith("admin:deloffer:"):
            idx_str = data.split(":", 2)[2]
            offers = RUNTIME_SETTINGS.get("offer_channels", [])
            if idx_str.isdigit():
                idx = int(idx_str) - 1
                if 0 <= idx < len(offers):
                    removed = offers.pop(idx)
                    await save_runtime_settings()
                    try:
                        await event.answer(f"✅ Removed {removed['text']}!", alert=False)
                    except Exception:
                        pass
            text_out, buttons = build_admin_offer_message_and_keyboard()
            await event.edit(text_out, buttons=buttons)
            return

        if data == "admin:clear_offers":
            RUNTIME_SETTINGS["offer_channels"] = []
            await save_runtime_settings()
            try:
                await event.answer("✅ Cleared all offer buttons!", alert=False)
            except Exception:
                pass
            text_out, buttons = build_admin_offer_message_and_keyboard()
            await event.edit(text_out, buttons=buttons)
            return

        if data == "admin:footer":
            text_out, buttons = build_admin_footer_message_and_keyboard()
            await event.edit(text_out, buttons=buttons)
            return

        if data.startswith("admin:clear_footer:"):
            target = data.split(":", 2)[2]
            if target == "global":
                RUNTIME_SETTINGS["footer_text"] = ""
            elif target == "terabox":
                RUNTIME_SETTINGS["terabox_footer_text"] = ""
            elif target == "diskwala":
                RUNTIME_SETTINGS["diskwala_footer_text"] = ""
            elif target == "all":
                RUNTIME_SETTINGS["footer_text"] = ""
                RUNTIME_SETTINGS["terabox_footer_text"] = ""
                RUNTIME_SETTINGS["diskwala_footer_text"] = ""
            await save_runtime_settings()
            try:
                await event.answer(f"✅ Cleared footer ({target})!", alert=False)
            except Exception:
                pass
            text_out, buttons = build_admin_footer_message_and_keyboard()
            await event.edit(text_out, buttons=buttons)
            return

        if data == "admin:links":
            text_out, buttons = build_admin_links_message_and_keyboard()
            await event.edit(text_out, buttons=buttons)
            return

        if data == "admin:stats":
            text_out, buttons = await build_admin_stats_message_and_keyboard()
            await event.edit(text_out, buttons=buttons)
            return

        if data == "admin:help":
            text_out, buttons = build_admin_help_message_and_keyboard()
            await event.edit(text_out, buttons=buttons)
            return

        if data == "admin:close":
            try:
                await event.delete()
            except Exception:
                pass
            return

    async def handle_admin_command(self, event, chat_id: int, text: str) -> None:
        payload = command_payload(text).strip()
        if not payload:
            text_out, buttons = await build_admin_main_message_and_keyboard()
            await self.client.send_message(chat_id, text_out, buttons=buttons)
            return

        cmd_parts = payload.split(maxsplit=1)
        sub_cmd = cmd_parts[0].lower()
        sub_payload = cmd_parts[1].strip() if len(cmd_parts) > 1 else ""

        if sub_cmd in ("fsub", "listfsub"):
            text_out, buttons = await build_admin_fsub_message_and_keyboard()
            await self.client.send_message(chat_id, text_out, buttons=buttons)

        elif sub_cmd == "togglefsub":
            RUNTIME_SETTINGS["fsub_enabled"] = not RUNTIME_SETTINGS.get("fsub_enabled", True)
            await save_runtime_settings()
            state = "🟢 Enabled" if RUNTIME_SETTINGS["fsub_enabled"] else "🔴 Disabled"
            await self.client.send_message(chat_id, f"✅ Force Subscribe is now {state}!")

        elif sub_cmd == "addfsub":
            if not sub_payload:
                await self.client.send_message(
                    chat_id,
                    "⚠️ **Usage:**\n`/admin addfsub <channel_id_or_@username> [invite_link] [title]`\n\n"
                    "**Examples:**\n"
                    "• `/admin addfsub -1001234567890 https://t.me/+joinlink VIP Channel`\n"
                    "• `/admin addfsub @mychannel`\n"
                    "• `/admin addfsub @mychannel https://t.me/mychannel Official Backup`\n"
                    "• `/admin addfsub https://t.me/+joinlink My Private Link`"
                )
                return

            channel_ref, invite_url, title = parse_addfsub_args(sub_payload)
            if not channel_ref and invite_url:
                hash_str = extract_invite_hash(invite_url)
                if hash_str:
                    try:
                        res = await self.client(CheckChatInviteRequest(hash_str))
                        if isinstance(res, ChatInviteAlready) and getattr(res, "chat", None):
                            channel_ref = str(utils.get_peer_id(res.chat))
                            if not title and getattr(res.chat, "title", None):
                                title = res.chat.title
                        elif getattr(res, "title", None) and not title:
                            title = res.title
                    except Exception as e:
                        logger.debug("Invite check failed: %s", e)

            if not channel_ref and not invite_url:
                await self.client.send_message(chat_id, "❌ Could not parse channel or invite URL from arguments.")
                return

            if not invite_url and channel_ref.startswith("@"):
                invite_url = f"https://t.me/{channel_ref.lstrip('@')}"

            if channel_ref:
                try:
                    c_entity = await self.client.get_entity(parse_chat_id(channel_ref))
                    _force_sub_id_cache[str(channel_ref)] = utils.get_peer_id(c_entity)
                    if not title and getattr(c_entity, "title", None):
                        title = c_entity.title
                    if not invite_url and getattr(c_entity, "username", None):
                        invite_url = f"https://t.me/{c_entity.username}"
                except Exception as e:
                    logger.debug("Entity resolve notice: %s", e)

            title = title or (channel_ref if channel_ref.startswith("@") else f"Channel {len(RUNTIME_SETTINGS.get('force_sub_channels', [])) + 1}")

            new_ch = {
                "channel": channel_ref or invite_url,
                "invite_url": invite_url,
                "title": title,
            }
            RUNTIME_SETTINGS.setdefault("force_sub_channels", []).append(new_ch)
            await save_runtime_settings()

            for r in ALL_RUNTIMES:
                asyncio.create_task(r.resolve_force_sub_peers())

            await self.client.send_message(
                chat_id,
                f"✅ **Force Sub Channel Added!**\n\n"
                f"📌 **Title:** {title}\n"
                f"🆔 **Channel:** `{channel_ref or '(Invite-only)'}`\n"
                f"🔗 **Link:** {invite_url or '(Auto-generated when needed)'}\n\n"
                f"💡 Make sure the bot is an Administrator in this channel!"
            )

        elif sub_cmd == "delfsub":
            channels = RUNTIME_SETTINGS.get("force_sub_channels", [])
            if not sub_payload:
                await self.client.send_message(chat_id, "Usage: `/admin delfsub <number_or_channel_id>`\nExample: `/admin delfsub 1`")
                return
            removed = None
            if sub_payload.isdigit():
                idx = int(sub_payload) - 1
                if 0 <= idx < len(channels):
                    removed = channels.pop(idx)
            else:
                for idx, c in enumerate(channels):
                    if str(c.get("channel", "")).strip().lower() == sub_payload.strip().lower():
                        removed = channels.pop(idx)
                        break
            if removed:
                await save_runtime_settings()
                await self.client.send_message(chat_id, f"✅ Removed Force Sub channel: **{removed.get('title', 'Channel')}** (`{removed.get('channel')}`)")
            else:
                await self.client.send_message(chat_id, f"❌ Channel not found matching `{sub_payload}`.")

        elif sub_cmd == "setlink":
            parts = sub_payload.split(maxsplit=1)
            if len(parts) < 2:
                await self.client.send_message(chat_id, "Usage: `/admin setlink <number_or_channel_id> <new_invite_url>`\nExample: `/admin setlink 1 https://t.me/+newlink`")
                return
            target_key, new_link = parts[0], parts[1]
            channels = RUNTIME_SETTINGS.get("force_sub_channels", [])
            updated = None
            if target_key.isdigit():
                idx = int(target_key) - 1
                if 0 <= idx < len(channels):
                    channels[idx]["invite_url"] = new_link
                    updated = channels[idx]
            else:
                for c in channels:
                    if str(c.get("channel", "")).strip().lower() == target_key.strip().lower():
                        c["invite_url"] = new_link
                        updated = c
                        break
            if updated:
                await save_runtime_settings()
                await self.client.send_message(chat_id, f"✅ Updated invite link for **{updated.get('title', 'Channel')}**:\n`{new_link}`")
            else:
                await self.client.send_message(chat_id, f"❌ Channel not found matching `{target_key}`.")

        elif sub_cmd == "clearfsub":
            RUNTIME_SETTINGS["force_sub_channels"] = []
            await save_runtime_settings()
            await self.client.send_message(chat_id, "✅ Cleared all Force Sub channels.")

        elif sub_cmd in ("offer", "offers", "listoffers"):
            text_out, buttons = build_admin_offer_message_and_keyboard()
            await self.client.send_message(chat_id, text_out, buttons=buttons)

        elif sub_cmd == "addoffer":
            if "|" not in sub_payload:
                await self.client.send_message(chat_id, "Usage: `/admin addoffer Button Title|https://t.me/+link`\nExample: `/admin addoffer 💰 Daily Earning|https://t.me/+earning`")
                return
            t_part, u_part = sub_payload.split("|", 1)
            t_part, u_part = t_part.strip(), u_part.strip()
            if not t_part or not u_part:
                await self.client.send_message(chat_id, "Both Title and URL are required.")
                return
            RUNTIME_SETTINGS.setdefault("offer_channels", []).append({"text": t_part, "url": u_part})
            await save_runtime_settings()
            await self.client.send_message(chat_id, f"✅ Offer channel added:\n**{t_part}** -> `{u_part}`")

        elif sub_cmd == "deloffer":
            offers = RUNTIME_SETTINGS.get("offer_channels", [])
            if sub_payload.isdigit():
                idx = int(sub_payload) - 1
                if 0 <= idx < len(offers):
                    removed = offers.pop(idx)
                    await save_runtime_settings()
                    await self.client.send_message(chat_id, f"✅ Removed offer: **{removed['text']}**")
                    return
            await self.client.send_message(chat_id, "Usage: `/admin deloffer <number>`\nExample: `/admin deloffer 1`")

        elif sub_cmd == "clearoffers":
            RUNTIME_SETTINGS["offer_channels"] = []
            await save_runtime_settings()
            await self.client.send_message(chat_id, "✅ Cleared all offer channels.")

        elif sub_cmd == "setjointext":
            if not sub_payload:
                await self.client.send_message(chat_id, "Usage: `/admin setjointext <button_text>`\nExample: `/admin setjointext 📢 Join Viral Channel`")
                return
            RUNTIME_SETTINGS["join_channels_text"] = sub_payload
            await save_runtime_settings()
            await self.client.send_message(chat_id, f"✅ Join Channels button text updated to: **{sub_payload}**")

        elif sub_cmd == "footer":
            text_out, buttons = build_admin_footer_message_and_keyboard()
            await self.client.send_message(chat_id, text_out, buttons=buttons)

        elif sub_cmd == "setfooter":
            if not sub_payload:
                await self.client.send_message(chat_id, "Usage: `/admin setfooter <text>`")
                return
            RUNTIME_SETTINGS["footer_text"] = sub_payload
            await save_runtime_settings()
            await self.client.send_message(chat_id, f"✅ Global footer text updated:\n\n{sub_payload}")

        elif sub_cmd == "setteraboxfooter":
            if not sub_payload:
                await self.client.send_message(chat_id, "Usage: `/admin setteraboxfooter <text>`")
                return
            RUNTIME_SETTINGS["terabox_footer_text"] = sub_payload
            await save_runtime_settings()
            await self.client.send_message(chat_id, f"✅ Terabox footer text updated:\n\n{sub_payload}")

        elif sub_cmd == "setdiskfooter":
            if not sub_payload:
                await self.client.send_message(chat_id, "Usage: `/admin setdiskfooter <text>`")
                return
            RUNTIME_SETTINGS["diskwala_footer_text"] = sub_payload
            await save_runtime_settings()
            await self.client.send_message(chat_id, f"✅ Diskwala footer text updated:\n\n{sub_payload}")

        elif sub_cmd == "clearfooter":
            target = sub_payload.lower().strip() or "all"
            if target == "global":
                RUNTIME_SETTINGS["footer_text"] = ""
            elif target == "terabox":
                RUNTIME_SETTINGS["terabox_footer_text"] = ""
            elif target == "diskwala":
                RUNTIME_SETTINGS["diskwala_footer_text"] = ""
            else:
                RUNTIME_SETTINGS["footer_text"] = ""
                RUNTIME_SETTINGS["terabox_footer_text"] = ""
                RUNTIME_SETTINGS["diskwala_footer_text"] = ""
            await save_runtime_settings()
            await self.client.send_message(chat_id, f"✅ Cleared footer ({target}).")

        elif sub_cmd == "setheader":
            if not sub_payload:
                await self.client.send_message(chat_id, "Usage: `/admin setheader <text>`")
                return
            RUNTIME_SETTINGS["header_text"] = sub_payload
            await save_runtime_settings()
            await self.client.send_message(chat_id, f"✅ Header text updated:\n\n{sub_payload}")

        elif sub_cmd == "settutorial":
            if not sub_payload:
                await self.client.send_message(chat_id, "Usage: `/admin settutorial <url>`")
                return
            RUNTIME_SETTINGS["tutorial_url"] = sub_payload
            await save_runtime_settings()
            await self.client.send_message(chat_id, f"✅ Tutorial URL updated to: `{sub_payload}`")

        elif sub_cmd == "settrending":
            if not sub_payload:
                await self.client.send_message(chat_id, "Usage: `/admin settrending <url>`")
                return
            RUNTIME_SETTINGS["trending_url"] = sub_payload
            await save_runtime_settings()
            await self.client.send_message(chat_id, f"✅ Trending URL updated to: `{sub_payload}`")

        elif sub_cmd == "setmax":
            if not sub_payload.isdigit() or int(sub_payload) <= 0:
                await self.client.send_message(chat_id, "Usage: `/admin setmax 20000`")
                return
            if not self.library_channel:
                await self.client.send_message(chat_id, f"⚠️ No library channel configured for this {self.provider} bot.")
                return
            await set_library_max_message_id(self.library_channel, int(sub_payload))
            await self.client.send_message(
                chat_id,
                f"✅ Library max message id saved for {self.provider} ({self.library_channel}): {int(sub_payload)}",
            )

        elif sub_cmd in ("stats", "status"):
            text_out, buttons = await build_admin_stats_message_and_keyboard()
            await self.client.send_message(chat_id, text_out, buttons=buttons)

        elif sub_cmd == "clearjoinrequests":
            _join_requests.clear()
            if join_requests_col is not None:
                try:
                    await join_requests_col.delete_many({})
                except Exception:
                    pass
            await self.client.send_message(chat_id, "✅ Cleared all pending join requests from memory and MongoDB.")

        elif sub_cmd in ("help", "commands"):
            text_out, buttons = build_admin_help_message_and_keyboard()
            await self.client.send_message(chat_id, text_out, buttons=buttons)

        else:
            await self.client.send_message(
                chat_id,
                f"❓ Unknown admin command `{sub_cmd}`. Send `/admin` or `/admin help` for the list of available commands."
            )

    async def handle_message(self, event) -> None:
        text = event.raw_text or ""
        cmd = command_name(text)
        chat_id = event.chat_id
        from_user_id = event.sender_id
        normalized_text = text.strip()

        # Check offer button clicks from reply keyboard
        offers = get_runtime_offer_channels()
        matched_offer = next((c for c in offers if c["text"] == normalized_text), None)
        if matched_offer:
            await self.send_offer_channel(chat_id, matched_offer)
            return

        if cmd == "start":
            await self.send_start(event)
        elif cmd == "video":
            await self.save_user_from_event(event)
            is_sub, missing = await self.check_user_subscription(from_user_id)
            if not is_sub:
                await self.send_force_sub_prompt(chat_id, missing)
                return
            await self.send_random_video(chat_id, from_user_id)
        join_btn_text = get_runtime_join_channels_text()
        if normalized_text in {join_btn_text, JOIN_CHANNELS_TEXT, "Join VIRALS", "📢 Join Viral Channel"}:
            await self.send_start(event)
        elif normalized_text in {WATCH_VIDEO_TEXT, WATCH_NEXT_TEXT}:
            await self.save_user_from_event(event)
            is_sub, missing = await self.check_user_subscription(from_user_id)
            if not is_sub:
                await self.send_force_sub_prompt(chat_id, missing)
                return
            await self.send_random_video(chat_id, from_user_id)
        elif cmd == "broadcast":
            await self.start_broadcast(event)
        elif cmd == "admin":
            if from_user_id not in OWNER_CHAT_IDS:
                await self.client.send_message(chat_id, "⛔ You are not authorized to use /admin.")
                return
            await self.handle_admin_command(event, chat_id, text)
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

        @self.client.on(events.Raw(UpdateBotChatInviteRequester))
        async def on_raw_join_request(event):
            try:
                user_id = int(event.user_id)
                chat_id = utils.get_peer_id(event.peer)
                record_join_request(user_id, chat_id)
                logger.info("[%s] Captured join request: user %s in chat %s", self.bot_key, user_id, chat_id)
            except Exception as exc:
                logger.warning("[%s] Failed to record join request: %s", self.bot_key, exc)

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
                    "[%s] %s channel peer still unresolved after boot. Will self-heal once a live update from that channel arrives.",
                    self.bot_key,
                    name,
                )

        await self.resolve_force_sub_peers()

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


# ===== ADMIN DASHBOARD MESSAGE BUILDERS =====
async def build_admin_main_message_and_keyboard() -> tuple[str, list]:
    total_users = await count_total_unique_users()
    fsub_channels = get_runtime_fsub_channels()
    fsub_status = "🟢 ON" if RUNTIME_SETTINGS.get("fsub_enabled", True) else "🔴 OFF"
    offers = get_runtime_offer_channels()

    lines = [
        "🛠 **Admin Control Panel**\n",
        f"🤖 **Running Bots:** {len(ALL_RUNTIMES)}",
        f"👥 **Total Unique Users:** {total_users}",
        f"📢 **Force Sub:** {fsub_status} ({len(fsub_channels)} channels)",
        f"💰 **Offer Channels:** {len(offers)} active",
        f"📝 **Footer:** {'Configured' if RUNTIME_SETTINGS.get('footer_text') or RUNTIME_SETTINGS.get('terabox_footer_text') or RUNTIME_SETTINGS.get('diskwala_footer_text') else 'Default'}\n",
        "Select a section below to configure:",
    ]
    buttons = [
        [Button.inline("📢 Force Sub Channels", b"admin:fsub"), Button.inline("💰 Offer Channels", b"admin:offer")],
        [Button.inline("📝 Footer & Captions", b"admin:footer"), Button.inline("🔗 Tutorial & Links", b"admin:links")],
        [Button.inline("📊 Network Stats", b"admin:stats"), Button.inline("❓ Help Guide", b"admin:help")],
        [Button.inline("🔄 Refresh", b"admin:main"), Button.inline("❌ Close", b"admin:close")],
    ]
    return "\n".join(lines), buttons


async def build_admin_fsub_message_and_keyboard() -> tuple[str, list]:
    channels = get_runtime_fsub_channels()
    fsub_enabled = RUNTIME_SETTINGS.get("fsub_enabled", True)
    status_str = "🟢 **Enabled**" if fsub_enabled else "🔴 **Disabled**"

    lines = [
        "📢 **Force Subscribe Management**",
        f"Status: {status_str}\n",
    ]

    if not channels:
        lines.append("⚠️ *No channels configured yet. Bot allows all users.*")
    else:
        lines.append(f"**Configured Channels ({len(channels)}):**")
        for idx, ch in enumerate(channels, start=1):
            title = ch.get("title") or "Channel"
            cid = ch.get("channel") or "(Invite link only)"
            inv = ch.get("invite_url") or "(None - auto-generates)"
            lines.append(f"**{idx}. {title}**\n   • ID: `{cid}`\n   • Link: `{inv}`")

    lines.append("\n**Quick Commands:**")
    lines.append("• Add: `/admin addfsub <id_or_username> [invite_link] [title]`")
    lines.append("• Remove: `/admin delfsub <number_or_id>`")
    lines.append("• Set Link: `/admin setlink <number_or_id> <invite_url>`")
    lines.append("• Toggle: `/admin togglefsub`")

    buttons = []
    toggle_label = "🔴 Disable FSUB" if fsub_enabled else "🟢 Enable FSUB"
    buttons.append([Button.inline(toggle_label, b"admin:toggle_fsub")])

    if channels:
        del_row = []
        for idx in range(1, len(channels) + 1):
            del_row.append(Button.inline(f"❌ Del #{idx}", f"admin:delfsub:{idx}".encode()))
            if len(del_row) == 4:
                buttons.append(del_row)
                del_row = []
        if del_row:
            buttons.append(del_row)
        buttons.append([Button.inline("🗑 Clear All FSUB", b"admin:clear_fsub")])

    buttons.append([Button.inline("🔙 Back to Admin", b"admin:main")])
    return "\n".join(lines), buttons


def build_admin_offer_message_and_keyboard() -> tuple[str, list]:
    offers = get_runtime_offer_channels()
    lines = [
        "💰 **Offer Channels Management**",
        "These buttons appear on /start, under videos, and in reply keyboards.\n",
    ]

    if not offers:
        lines.append("⚠️ *No offer channels configured.*")
    else:
        lines.append(f"**Current Offers ({len(offers)}):**")
        for idx, of in enumerate(offers, start=1):
            lines.append(f"**{idx}. {of['text']}**\n   • URL: `{of['url']}`")

    lines.append("\n**Quick Commands:**")
    lines.append("• Add: `/admin addoffer <Title>|<URL>`")
    lines.append("• Remove: `/admin deloffer <number>`")
    lines.append("• Clear: `/admin clearoffers`")

    buttons = []
    if offers:
        del_row = []
        for idx in range(1, len(offers) + 1):
            del_row.append(Button.inline(f"❌ Del #{idx}", f"admin:deloffer:{idx}".encode()))
            if len(del_row) == 4:
                buttons.append(del_row)
                del_row = []
        if del_row:
            buttons.append(del_row)
        buttons.append([Button.inline("🗑 Clear All Offers", b"admin:clear_offers")])

    buttons.append([Button.inline("🔙 Back to Admin", b"admin:main")])
    return "\n".join(lines), buttons


def build_admin_footer_message_and_keyboard() -> tuple[str, list]:
    g_foot = RUNTIME_SETTINGS.get("footer_text") or "(None)"
    t_foot = RUNTIME_SETTINGS.get("terabox_footer_text") or "(Using Global)"
    d_foot = RUNTIME_SETTINGS.get("diskwala_footer_text") or "(Using Global)"
    header = RUNTIME_SETTINGS.get("header_text") or HEADER_TEXT

    lines = [
        "📝 **Footer & Captions Management**\n",
        f"• **Global Footer:**\n`{g_foot}`\n",
        f"• **Terabox Footer:**\n`{t_foot}`\n",
        f"• **Diskwala Footer:**\n`{d_foot}`\n",
        f"• **Header Text:**\n`{header}`\n",
        "**Quick Commands:**",
        "• `/admin setfooter <text>` (Global)",
        "• `/admin setteraboxfooter <text>` (Terabox)",
        "• `/admin setdiskfooter <text>` (Diskwala)",
        "• `/admin setheader <text>`",
        "• `/admin clearfooter [global|terabox|diskwala|all]`",
    ]

    buttons = [
        [Button.inline("🗑 Clear Global", b"admin:clear_footer:global"), Button.inline("🗑 Clear Terabox", b"admin:clear_footer:terabox")],
        [Button.inline("🗑 Clear Diskwala", b"admin:clear_footer:diskwala"), Button.inline("🗑 Clear All", b"admin:clear_footer:all")],
        [Button.inline("🔙 Back to Admin", b"admin:main")],
    ]
    return "\n".join(lines), buttons


def build_admin_links_message_and_keyboard() -> tuple[str, list]:
    tut = get_runtime_tutorial_url()
    trend = get_runtime_trending_url()

    lines = [
        "🔗 **Tutorial & Trending Links**\n",
        f"• **Tutorial URL:**\n`{tut}`\n",
        f"• **Trending URL:**\n`{trend}`\n",
        "**Quick Commands:**",
        "• `/admin settutorial <url>`",
        "• `/admin settrending <url>`",
    ]
    buttons = [[Button.inline("🔙 Back to Admin", b"admin:main")]]
    return "\n".join(lines), buttons


async def build_admin_stats_message_and_keyboard() -> tuple[str, list]:
    total_users = await count_total_unique_users()
    diskwala_bots = sum(1 for r in ALL_RUNTIMES if r.provider == "diskwala")
    terabox_bots = sum(1 for r in ALL_RUNTIMES if r.provider == "terabox")
    fsub_channels = get_runtime_fsub_channels()
    fsub_status = "🟢 Enabled" if RUNTIME_SETTINGS.get("fsub_enabled", True) else "🔴 Disabled"
    offers_count = len(get_runtime_offer_channels())
    pending_joins = len(_join_requests)

    lines = [
        "📊 **Bot Network Statistics**\n",
        f"🤖 **Active Bots:** {len(ALL_RUNTIMES)} total",
        f"   • Diskwala Bots: {diskwala_bots}",
        f"   • Terabox Bots: {terabox_bots}",
        f"👥 **Total Registered Users:** {total_users}",
        f"📢 **Force Subscribe:** {fsub_status} ({len(fsub_channels)} channels)",
        f"💰 **Offer Channels:** {offers_count} active",
        f"⏳ **Pending Join Requests in cache:** {pending_joins}",
        f"🗄 **Database:** {'Connected' if mongo_db is not None else 'In-Memory only'}",
    ]
    buttons = [
        [Button.inline("🔄 Refresh Stats", b"admin:stats")],
        [Button.inline("🔙 Back to Admin", b"admin:main")],
    ]
    return "\n".join(lines), buttons


def build_admin_help_message_and_keyboard() -> tuple[str, list]:
    lines = [
        "❓ **Admin Commands Reference**\n",
        "**📢 Force Subscribe:**",
        "• `/admin` - Open Admin Control Panel",
        "• `/admin fsub` - View FSUB channel list",
        "• `/admin togglefsub` - Toggle FSUB ON/OFF",
        "• `/admin addfsub <id_or_@username> [invite_link] [title]` - Add channel",
        "• `/admin delfsub <number_or_id>` - Remove channel",
        "• `/admin setlink <number_or_id> <invite_url>` - Change invite link",
        "• `/admin clearfsub` - Clear all FSUB channels\n",
        "**💰 Offer Channels:**",
        "• `/admin offer` - View offer buttons",
        "• `/admin addoffer <Title>|<URL>` - Add offer button",
        "• `/admin deloffer <number>` - Remove offer button",
        "• `/admin clearoffers` - Clear all offers\n",
        "• `/admin setjointext <text>` - Change 'Join Channels' button text\n",
        "**📝 Footer & Captions:**",
        "• `/admin footer` - View footer texts",
        "• `/admin setfooter <text>` - Set global footer",
        "• `/admin setteraboxfooter <text>` - Set Terabox footer",
        "• `/admin setdiskfooter <text>` - Set Diskwala footer",
        "• `/admin clearfooter [global|terabox|diskwala|all]` - Clear footer",
        "• `/admin setheader <text>` - Set header text\n",
        "**🔗 Links & Misc:**",
        "• `/admin settutorial <url>` - Set tutorial URL",
        "• `/admin settrending <url>` - Set trending URL",
        "• `/admin setmax <number>` - Set library max message ID",
        "• `/admin stats` - View network stats",
        "• `/admin clearjoinrequests` - Clear cached join requests",
        "• `/broadcast` - Broadcast message to all users",
    ]
    buttons = [[Button.inline("🔙 Back to Admin", b"admin:main")]]
    return "\n".join(lines), buttons


# ===== HEALTH SERVER & MAIN =====
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
            pass

    try:
        await asyncio.gather(*(runtime.run() for runtime in runtimes))
    finally:
        await health_runner.cleanup()
        if mongo_client:
            mongo_client.close()


if __name__ == "__main__":
    asyncio.run(main())
