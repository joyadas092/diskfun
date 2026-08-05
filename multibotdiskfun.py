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

LIBRARY_TELEGRAM_CHANNEL = os.getenv("LIBRARY_TELEGRAM_CHANNEL", "").strip()
FORWARD_TEXT_CHANNEL = os.getenv("FORWARD_TEXT_CHANNEL", "").strip()

MONGO_URI = os.getenv("MONGO_URI", "").strip()
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "viralbots").strip()
MONGO_USERS_COLLECTION = os.getenv("MONGO_USERS_COLLECTION", "multi_bot_users").strip()
MONGO_CHANNELS_COLLECTION = os.getenv("MONGO_CHANNELS_COLLECTION", "channel_state").strip()

START_POST_MESSAGE_ID = int(os.getenv("START_POST_MESSAGE_ID", "0") or 0)
LIBRARY_MAX_MESSAGE_ID = int(os.getenv("LIBRARY_MAX_MESSAGE_ID", "0") or 0)
RANDOM_VIDEO_RETRIES = int(os.getenv("RANDOM_VIDEO_RETRIES", "35") or 35)

WATCH_VIDEO_TEXT = os.getenv("WATCH_VIDEO_TEXT", "🔞 Watch Now").strip() or "🎬 Watch Now"
WATCH_NEXT_TEXT = os.getenv("WATCH_NEXT_TEXT", "👙 Watch Next").strip() or "⏭ Watch Next"
JOIN_CHANNELS_TEXT = os.getenv("JOIN_CHANNELS_TEXT", "Join VIRALS").strip() or "📢 Join Viral Channel"
TUTORIAL_URL = os.getenv("TUTORIAL_URL", "https://t.me/howdisk/2").strip()
TRENDING_URL = os.getenv("TRENDING_URL", "bitly.cx/diskwala").strip()

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
        text, url = (parts[0], parts[1]) if len(parts) > 1 else ("📢 Offer Channel", parts[0])
        channels.append({"text": text, "url": url})

    if not channels:
        url = os.getenv("OFFER_CHANNEL_URL", "").strip()
        text = os.getenv("OFFER_CHANNEL_TEXT", "📢 Offer Channel").strip() or "📢 Offer Channel"
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


def extract_diskwala_link(text: str) -> str:
    match = re.search(r"(?:https?://)?(?:www\.)?diskwala\.com/[^\s<>)\]]+", text or "", flags=re.IGNORECASE)
    if not match:
        return ""
    link = match.group(0).rstrip(".,;:!?)\"]'")
    if not link.lower().startswith(("http://", "https://")):
        link = f"https://{link}"
    return link


def video_caption(diskwala_link: str) -> str:
    tutorial = TUTORIAL_URL or "https://t.me/howdisk/2"
    trending = TRENDING_URL or "bitly.cx/diskwala"
    return (
        "🎬 **Vdo 😍**\n\n"
        "**🔗🔗यह रहा वीडियो लिंक 👇**\n\n"
        f"{diskwala_link}\n\n"
        "**🤔 How to Open Links? | लिंक कैसे खोलें 👇**\n"
        f"[📖 View Tutorial]({tutorial})\n\n"
        "😉**Daily Trending. Open 👇**\n"
        f"{trending}"
    )


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
    Put every bot token in one list, comma/semicolon/newline separated:
      BOT_TOKENS=111:AAA,222:BBB,333:CCC,444:DDD

    All bots share OFFER_CHANNEL_URL / OFFER_CHANNEL_TEXT. Session files and
    db bot_key are auto-derived (diskfun_bot_1, diskfun_bot_2, ... and the
    bot's own @username once resolved).

    Numbered BOT_TOKEN_1, BOT_TOKEN_2, ... or a single BOT_TOKEN are still
    supported as a fallback if BOT_TOKENS is not set.
    """
    tokens: list[str] = [t.strip() for t in re.split(r"[,;\r\n]+", os.getenv("BOT_TOKENS", "")) if t.strip()]

    if not tokens:
        idx = 1
        while True:
            token = os.getenv(f"BOT_TOKEN_{idx}", "").strip()
            if not token:
                break
            tokens.append(token)
            idx += 1

    if not tokens:
        token = os.getenv("BOT_TOKEN", "").strip()
        if token:
            tokens.append(token)

    return [
        {
            "index": idx,
            "token": token,
            "session": os.getenv(f"BOT_SESSION_{idx}", f"diskfun_bot_{idx}").strip(),
            "bot_key": os.getenv(f"BOT_KEY_{idx}", "").strip().lstrip("@"),
        }
        for idx, token in enumerate(tokens, start=1)
    ]


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


async def get_max_library_message_id() -> int:
    if LIBRARY_MAX_MESSAGE_ID > 0:
        return LIBRARY_MAX_MESSAGE_ID
    return await get_channel_last_message(parse_chat_id(LIBRARY_TELEGRAM_CHANNEL))


async def set_library_max_message_id(message_id: int) -> None:
    await set_channel_last_message(parse_chat_id(LIBRARY_TELEGRAM_CHANNEL), int(message_id))


def should_delete_user_after_failure(exc: Exception) -> bool:
    return isinstance(exc, USER_GONE_ERRORS)


# ===== PER-BOT RUNTIME =====
class BotRuntime:
    def __init__(self, config: dict[str, Any]) -> None:
        self.index = config["index"]
        self.token = config["token"]
        self.bot_key = config["bot_key"]
        self.bot_key_safe = ""
        self.bot_username = ""
        self.bot_id = 0
        self.client = TelegramClient(config["session"], API_ID, API_HASH)
        self.memory_users: set[int] = set()
        self.broadcast_jobs: dict[str, dict[str, Any]] = {}

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
            "📢 Check out our offer channel:",
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

        start_message_id = START_POST_MESSAGE_ID or await get_channel_last_message(parse_chat_id(FORWARD_TEXT_CHANNEL))
        if FORWARD_TEXT_CHANNEL and start_message_id:
            try:
                source = await self.client.get_messages(parse_chat_id(FORWARD_TEXT_CHANNEL), ids=int(start_message_id))
                if source:
                    await self.client.send_message(chat_id, source, buttons=self.start_keyboard())
                    await self.send_main_reply_keyboard(chat_id)
                    return
            except Exception as exc:
                logger.warning("[%s] Could not copy /start post: %s", self.bot_key, exc)

        await self.client.send_message(
            chat_id,
            "👋 Welcome!\n\nJoin the channel and tap Watch Video to get a random Diskwala link.",
            buttons=self.start_keyboard(),
        )
        await self.send_main_reply_keyboard(chat_id)

    async def send_random_video(self, chat_id: int, user_id: int | None = None) -> None:
        if user_id:
            await self.save_user(user_id)

        if not await self.user_joined_required_channels(int(chat_id)):
            await self.send_join_prompt(int(chat_id))
            return

        if not LIBRARY_TELEGRAM_CHANNEL:
            await self.client.send_message(chat_id, "⚠️ LIBRARY_TELEGRAM_CHANNEL is not configured.")
            return

        max_message_id = await get_max_library_message_id()
        if max_message_id <= 0:
            await self.client.send_message(chat_id, "⚠️ Library max id is missing. Admin can set it with /setmax 20000.")
            return

        library_chat_id = parse_chat_id(LIBRARY_TELEGRAM_CHANNEL)

        async with self.client.action(chat_id, "typing"):
            for _ in range(max(1, RANDOM_VIDEO_RETRIES)):
                message_id = random.randint(1, max_message_id)
                try:
                    source_message = await self.client.get_messages(library_chat_id, ids=message_id)
                except FloodWaitError as exc:
                    await asyncio.sleep(exc.seconds)
                    continue
                except Exception as exc:
                    logger.warning("[%s] Random message lookup failed for %s: %s", self.bot_key, message_id, exc)
                    continue

                if not source_message or not source_message.media:
                    continue

                source_text = source_message.raw_text or ""
                diskwala_link = extract_diskwala_link(source_text)
                if not diskwala_link:
                    continue

                try:
                    await self.client.send_file(
                        chat_id,
                        file=source_message.media,
                        caption=video_caption(diskwala_link),
                        buttons=self.video_keyboard(),
                        link_preview=False,
                    )
                    return
                except FloodWaitError as exc:
                    await asyncio.sleep(exc.seconds)
                    continue
                except Exception as exc:
                    logger.warning("[%s] Random video copy failed: %s", self.bot_key, exc)
                    break

        await self.client.send_message(
            chat_id,
            "😕 Could not find a random media post with a Diskwala link right now. Please try again.",
        )

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
            await set_library_max_message_id(int(payload))
            await self.client.send_message(chat_id, f"✅ Library max message id saved: {int(payload)}")
        elif cmd == "status" and from_user_id in OWNER_CHAT_IDS:
            await self.client.send_message(
                chat_id,
                f"📊 Users started for @{self.bot_username or self.bot_key}: {await self.count_users()}",
            )
        elif cmd == "stats" and from_user_id in OWNER_CHAT_IDS:
            await self.client.send_message(
                chat_id,
                (
                    f"📊 Users for @{self.bot_username or self.bot_key}: {await self.count_users()}\n"
                    f"🔢 Library max id: {await get_max_library_message_id()}"
                ),
            )

    def register_handlers(self) -> None:
        @self.client.on(events.NewMessage(incoming=True))
        async def on_new_message(event):
            try:
                if event.is_channel and not event.is_group:
                    library_chat_id = parse_chat_id(LIBRARY_TELEGRAM_CHANNEL)
                    forward_chat_id = parse_chat_id(FORWARD_TEXT_CHANNEL)
                    if event.chat_id in (library_chat_id, forward_chat_id):
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
        logger.info("Bot identity: @%s (%s), db key=%s", self.bot_username, self.bot_id, self.bot_key_safe)

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
        targets = (
            (parse_chat_id(LIBRARY_TELEGRAM_CHANNEL), "library"),
            (parse_chat_id(FORWARD_TEXT_CHANNEL), "forward"),
        )
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
            await self.notify_owner(f"✅ @{self.bot_username or self.bot_key} started.")
            await self.client.run_until_disconnected()
        except Exception as exc:
            logger.exception("[%s] Bot crashed and is not responding", self.bot_key)
            try:
                await self.notify_owner(f"🛑 @{self.bot_username or self.bot_key} crashed and is not responding: {exc}")
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
    if not LIBRARY_TELEGRAM_CHANNEL:
        raise RuntimeError("LIBRARY_TELEGRAM_CHANNEL is required in .env")
    if not FORWARD_TEXT_CHANNEL:
        raise RuntimeError("FORWARD_TEXT_CHANNEL is required in .env")

    bot_configs = parse_bot_configs()
    if not bot_configs:
        raise RuntimeError("No bots configured. Set BOT_TOKEN_1, BOT_TOKEN_2, ... in .env")

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
