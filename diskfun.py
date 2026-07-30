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
memory_users: set[int] = set()
memory_channel_state: dict[str, int] = {}
broadcast_jobs: dict[str, dict[str, Any]] = {}

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

client = TelegramClient("diskfun_bot", API_ID, API_HASH)


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


def video_action_keyboard(action_text: str) -> list:
    rows = [[Button.inline(action_text, b"video")]]
    if EARN_CHANNEL_URL:
        rows.append([Button.url(EARN_CHANNEL_TEXT, EARN_CHANNEL_URL)])
    return rows


def start_keyboard() -> list:
    return video_action_keyboard(WATCH_VIDEO_TEXT)


def video_keyboard() -> list:
    return video_action_keyboard(WATCH_NEXT_TEXT)


def main_reply_keyboard() -> list:
    return [
        [Button.text(JOIN_CHANNELS_TEXT, resize=True), Button.text(WATCH_VIDEO_TEXT, resize=True)],
        [Button.text(JOIN_CHANNELS_TEXT, resize=True), Button.text(WATCH_VIDEO_TEXT, resize=True)],
        [Button.text(JOIN_CHANNELS_TEXT, resize=True), Button.text(WATCH_VIDEO_TEXT, resize=True)],
        [Button.text(EARN_CHANNEL_TEXT, resize=True)],
    ]


def cancel_keyboard(job_id: str) -> list:
    return [[Button.inline("🛑 Cancel Broadcast", f"cancel_broadcast:{job_id}".encode())]]


# ===== MONGO =====
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


async def save_user(user_id: int, username: str = "", first_name: str = "", last_name: str = "") -> None:
    if not user_id:
        return
    if users_col is None:
        memory_users.add(int(user_id))
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


async def save_user_from_event(event) -> None:
    try:
        sender = await event.get_sender()
    except Exception:
        sender = None
    if sender is None:
        return
    await save_user(
        sender.id,
        username=getattr(sender, "username", None) or "",
        first_name=getattr(sender, "first_name", None) or "",
        last_name=getattr(sender, "last_name", None) or "",
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


# ===== BOT LOGIC =====
def should_delete_user_after_failure(exc: Exception) -> bool:
    return isinstance(exc, USER_GONE_ERRORS)


async def user_joined_required_channels(user_id: int) -> bool:
    check_channels = [c for c in JOIN_CHANNELS if c.get("check_chat")]
    if not check_channels:
        return True

    for channel in check_channels:
        try:
            await client.get_permissions(parse_chat_id(channel["check_chat"]), user_id)
        except UserNotParticipantError:
            return False
        except ChatAdminRequiredError:
            logger.warning("Bot is not admin in %s, cannot verify join.", channel["check_chat"])
            return False
        except Exception as exc:
            logger.warning("Join check failed for %s: %s", channel["check_chat"], exc)
            return False
    return True


async def send_earn_channel(chat_id: int) -> None:
    if not EARN_CHANNEL_URL:
        return
    await client.send_message(
        chat_id,
        "💰 Join our earning channel:",
        buttons=[[Button.url(EARN_CHANNEL_TEXT, EARN_CHANNEL_URL)]],
    )


async def send_join_prompt(chat_id: int) -> None:
    await client.send_message(
        chat_id,
        "🔒 Please join the channel first, then tap Watch Video.",
        buttons=start_keyboard(),
    )


async def send_main_reply_keyboard(chat_id: int) -> None:
    await client.send_message(chat_id, "👇 Menu", buttons=main_reply_keyboard())


async def send_start(event) -> None:
    chat_id = event.chat_id
    await save_user_from_event(event)

    if command_payload(event.raw_text or "").lower() == "video":
        await send_main_reply_keyboard(chat_id)
        await send_random_video(chat_id, event.sender_id)
        return

    start_message_id = START_POST_MESSAGE_ID or await get_channel_last_message(parse_chat_id(FORWARD_TEXT_CHANNEL))
    if FORWARD_TEXT_CHANNEL and start_message_id:
        try:
            source = await client.get_messages(parse_chat_id(FORWARD_TEXT_CHANNEL), ids=int(start_message_id))
            if source:
                await client.send_message(chat_id, source, buttons=start_keyboard())
                await send_main_reply_keyboard(chat_id)
                return
        except Exception as exc:
            logger.warning("Could not copy /start post: %s", exc)

    await client.send_message(
        chat_id,
        "👋 Welcome!\n\nJoin the channel and tap Watch Video to get a random Diskwala link.",
        buttons=start_keyboard(),
    )
    await send_main_reply_keyboard(chat_id)


async def send_random_video(chat_id: int, user_id: int | None = None) -> None:
    if user_id:
        await save_user(user_id)

    if not await user_joined_required_channels(int(chat_id)):
        await send_join_prompt(int(chat_id))
        return

    if not LIBRARY_TELEGRAM_CHANNEL:
        await client.send_message(chat_id, "⚠️ LIBRARY_TELEGRAM_CHANNEL is not configured.")
        return

    max_message_id = await get_max_library_message_id()
    if max_message_id <= 0:
        await client.send_message(chat_id, "⚠️ Library max id is missing. Admin can set it with /setmax 20000.")
        return

    library_chat_id = parse_chat_id(LIBRARY_TELEGRAM_CHANNEL)

    async with client.action(chat_id, "typing"):
        for _ in range(max(1, RANDOM_VIDEO_RETRIES)):
            message_id = random.randint(1, max_message_id)
            try:
                source_message = await client.get_messages(library_chat_id, ids=message_id)
            except FloodWaitError as exc:
                await asyncio.sleep(exc.seconds)
                continue
            except Exception as exc:
                logger.warning("Random message lookup failed for %s: %s", message_id, exc)
                continue

            if not source_message or not source_message.media:
                continue

            source_text = source_message.raw_text or ""
            diskwala_link = extract_diskwala_link(source_text)
            if not diskwala_link:
                continue

            try:
                await client.send_file(
                    chat_id,
                    file=source_message.media,
                    caption=video_caption(diskwala_link),
                    buttons=video_keyboard(),
                    link_preview=False,
                )
                return
            except FloodWaitError as exc:
                await asyncio.sleep(exc.seconds)
                continue
            except Exception as exc:
                logger.warning("Random video copy failed: %s", exc)
                break

    await client.send_message(
        chat_id,
        "😕 Could not find a random media post with a Diskwala link right now. Please try again.",
    )


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


async def copy_or_send_broadcast(target_user_id: int, source: dict) -> None:
    if source["type"] == "copy":
        await client.send_message(target_user_id, source["message"])
        return
    await client.send_message(target_user_id, source["text"])


async def run_broadcast(admin_chat_id: int, status_message_id: int, source: dict) -> None:
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
            await client.edit_message(admin_chat_id, status_message_id, buttons=cancel_keyboard(job_id))
        except Exception:
            pass

        async for user_id in iter_users_for_this_bot():
            if cancel_event.is_set():
                stats["cancelled"] = 1
                break

            try:
                await copy_or_send_broadcast(user_id, source)
                stats["sent"] += 1
            except FloodWaitError as exc:
                await asyncio.sleep(exc.seconds)
                try:
                    await copy_or_send_broadcast(user_id, source)
                    stats["sent"] += 1
                except Exception as retry_exc:
                    stats["failed"] += 1
                    if should_delete_user_after_failure(retry_exc):
                        await remove_user_for_this_bot(user_id)
                        stats["deleted"] += 1
            except Exception as exc:
                stats["failed"] += 1
                if should_delete_user_after_failure(exc):
                    await remove_user_for_this_bot(user_id)
                    stats["deleted"] += 1

            stats["done"] += 1
            now = time.time()
            if now - last_edit >= 2 or stats["done"] == stats["total"]:
                last_edit = now
                try:
                    await client.edit_message(
                        admin_chat_id,
                        status_message_id,
                        broadcast_text(stats, running=True),
                        buttons=cancel_keyboard(job_id),
                    )
                except Exception:
                    pass

            await asyncio.sleep(0.04)
    finally:
        broadcast_jobs.pop(job_id, None)
        try:
            await client.edit_message(admin_chat_id, status_message_id, broadcast_text(stats, running=False))
        except Exception:
            pass


async def start_broadcast(event) -> None:
    chat_id = event.chat_id
    user_id = event.sender_id
    if user_id not in OWNER_CHAT_IDS:
        return

    if broadcast_jobs:
        await client.send_message(chat_id, "⚠️ Broadcast already running.")
        return

    reply_msg = await event.get_reply_message() if event.is_reply else None
    payload = command_payload(event.raw_text or "")
    if reply_msg:
        source = {"type": "copy", "message": reply_msg}
    elif payload:
        source = {"type": "text", "text": payload}
    else:
        await client.send_message(chat_id, "📣 Reply to a post with /broadcast, or use /broadcast your message.")
        return

    total = await count_users_for_this_bot()
    status = await client.send_message(
        chat_id,
        f"🚀 Broadcast starting\n\n🤖 Bot: @{BOT_USERNAME or BOT_KEY}\n👥 Users: {total}",
        buttons=cancel_keyboard("pending"),
    )
    asyncio.create_task(run_broadcast(chat_id, status.id, source))


async def handle_callback(event) -> None:
    data = (event.data or b"").decode()
    chat_id = event.chat_id

    if data == "video":
        try:
            await event.answer("🎬 Sending video...")
        except Exception:
            pass
        await send_random_video(chat_id, event.sender_id)
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
        job = broadcast_jobs.get(job_id)
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


async def handle_message(event) -> None:
    text = event.raw_text or ""
    cmd = command_name(text)
    chat_id = event.chat_id
    from_user_id = event.sender_id
    normalized_text = text.strip()

    if cmd == "start":
        await send_start(event)
    elif cmd == "video":
        await save_user_from_event(event)
        await send_random_video(chat_id, from_user_id)
    elif normalized_text == JOIN_CHANNELS_TEXT:
        await send_start(event)
    elif normalized_text in {WATCH_VIDEO_TEXT, WATCH_NEXT_TEXT}:
        await save_user_from_event(event)
        await send_random_video(chat_id, from_user_id)
    elif normalized_text == EARN_CHANNEL_TEXT:
        await send_earn_channel(chat_id)
    elif cmd == "broadcast":
        await start_broadcast(event)
    elif cmd == "setmax" and from_user_id in OWNER_CHAT_IDS:
        payload = command_payload(text)
        if not payload.isdigit() or int(payload) <= 0:
            await client.send_message(chat_id, "Use: /setmax 20000")
            return
        await set_library_max_message_id(int(payload))
        await client.send_message(chat_id, f"✅ Library max message id saved: {int(payload)}")
    elif cmd == "status" and from_user_id in OWNER_CHAT_IDS:
        await client.send_message(
            chat_id,
            f"📊 Users started for @{BOT_USERNAME or BOT_KEY}: {await count_users_for_this_bot()}",
        )
    elif cmd == "stats" and from_user_id in OWNER_CHAT_IDS:
        await client.send_message(
            chat_id,
            (
                f"📊 Users for @{BOT_USERNAME or BOT_KEY}: {await count_users_for_this_bot()}\n"
                f"🔢 Library max id: {await get_max_library_message_id()}"
            ),
        )


# ===== EVENT HANDLERS =====
@client.on(events.NewMessage(incoming=True))
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

        await handle_message(event)
    except USER_GONE_ERRORS as exc:
        user_id = event.sender_id
        if user_id:
            await remove_user_for_this_bot(user_id)
            logger.info("Removed blocked/deactivated user %s from db", user_id)
    except Exception:
        logger.exception("Unhandled message error")


@client.on(events.CallbackQuery())
async def on_callback(event):
    try:
        await handle_callback(event)
    except Exception:
        logger.exception("Unhandled callback error")


async def resolve_bot_identity() -> None:
    global BOT_KEY, BOT_KEY_SAFE, BOT_USERNAME, BOT_ID
    me = await client.get_me()
    BOT_USERNAME = me.username or ""
    BOT_ID = int(me.id)
    if not BOT_KEY:
        BOT_KEY = BOT_USERNAME or str(BOT_ID)
    BOT_KEY_SAFE = safe_field(BOT_KEY)
    logger.info("Bot identity: @%s (%s), db key=%s", BOT_USERNAME, BOT_ID, BOT_KEY_SAFE)


async def notify_owner(text: str) -> None:
    for owner_id in OWNER_CHAT_IDS:
        try:
            await client.send_message(owner_id, text)
        except Exception as exc:
            logger.warning("Owner notify failed for %s: %s", owner_id, exc)


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


async def resolve_peers_before_run() -> None:
    """
    A bot's MTProto session only learns a private channel's internals from a
    live update or a prior resolution - there is no cold "resolve by id" for a
    chat it has never seen. Without a persisted session/access hash (by
    design here), this can only self-heal once the channel admin posts
    something new while the bot is running. Non-fatal either way: the bot
    keeps serving other commands regardless of the outcome.
    """
    targets = (
        (parse_chat_id(LIBRARY_TELEGRAM_CHANNEL), "library"),
        (parse_chat_id(FORWARD_TEXT_CHANNEL), "forward"),
    )
    for target, name in targets:
        for attempt in range(5):
            try:
                await client.get_entity(target)
                logger.info("Resolved %s channel peer.", name)
                break
            except Exception as exc:
                logger.warning("%s channel peer resolve attempt %s/5 failed: %s", name, attempt + 1, exc)
                await asyncio.sleep(2 * (attempt + 1))
        else:
            logger.warning(
                "%s channel peer still unresolved after boot. Will self-heal once "
                "a live update from that channel arrives.",
                name,
            )


async def main() -> None:
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN is required in .env")
    if not API_ID or not API_HASH:
        raise RuntimeError("API_ID and API_HASH are required to read random old library messages")
    if not LIBRARY_TELEGRAM_CHANNEL:
        raise RuntimeError("LIBRARY_TELEGRAM_CHANNEL is required in .env")
    if not FORWARD_TEXT_CHANNEL:
        raise RuntimeError("FORWARD_TEXT_CHANNEL is required in .env")

    health_runner = await start_health_server()

    stopping = asyncio.Event()

    def _request_stop() -> None:
        if not stopping.is_set():
            stopping.set()
            asyncio.create_task(_graceful_stop())

    async def _graceful_stop() -> None:
        logger.info("Shutdown signal received.")
        await notify_owner(f"⏹ @{BOT_USERNAME or BOT_KEY} is stopping.")
        await client.disconnect()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, _request_stop)
        except (NotImplementedError, RuntimeError):
            pass  # e.g. Windows, which lacks add_signal_handler support

    crashed_exc: Exception | None = None
    try:
        await client.start(bot_token=BOT_TOKEN)
        await resolve_bot_identity()
        await init_mongo()
        await resolve_peers_before_run()
        await notify_owner(f"✅ @{BOT_USERNAME or BOT_KEY} started.")
        await client.run_until_disconnected()
    except Exception as exc:
        crashed_exc = exc
        logger.exception("Bot crashed and is not responding")
    finally:
        if crashed_exc is not None:
            try:
                await notify_owner(f"🛑 @{BOT_USERNAME or BOT_KEY} crashed and is not responding: {crashed_exc}")
            except Exception:
                pass
        try:
            if client.is_connected():
                await client.disconnect()
        except Exception as exc:
            logger.warning("Client disconnect failed (continuing cleanup): %s", exc)
        await health_runner.cleanup()
        if mongo_client:
            mongo_client.close()
        if crashed_exc is not None:
            raise crashed_exc


if __name__ == "__main__":
    asyncio.run(main())
