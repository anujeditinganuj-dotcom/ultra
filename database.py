"""
MongoDB-backed persistence for the Diskwala bot (using motor, the async
MongoDB driver, so calls don't block pyrogram's event loop).

Collections:
  users        - _id: user_id (int)
                 first_seen, is_banned, premium_lifetime, premium_until
  daily_counts - _id: "{user_id}:{YYYY-MM-DD}"
                 user_id, day, count
  downloads    - one doc per completed download: user_id, ts
  file_cache   - _id: "{link}::{quality}"
                 link, quality, name, size, quality_label, cached_at, duration,
                 parts — list of per-part dicts, one entry per uploaded part
                 (single-file downloads just have one entry):
                   {file_id, size, duration, cache_chat_id, cache_message_id}
                 cache_chat_id/cache_message_id inside a part point at the
                 copy stored in CACHE_CHANNEL_ID, used to (re)send via
                 copy_message() for a fresh file_reference instead of the
                 raw file_id (which can go stale for chats that never
                 received the original upload). Older docs written before
                 split-download caching was supported only have top-level
                 file_id/cache_chat_id/cache_message_id fields instead of
                 "parts" — get_cached_file() normalizes those into a
                 single-element parts list on read.
  scheduled_deletions - _id: "{chat_id}:{message_id}"
                 chat_id, message_id, delete_at — persisted auto-delete
                 timers so they survive a restart/redeploy
"""

from datetime import datetime, timedelta

from motor.motor_asyncio import AsyncIOMotorClient

from config import MONGO_URI, MONGO_DB_NAME

_client = AsyncIOMotorClient(MONGO_URI)
_db = _client[MONGO_DB_NAME]

users_col = _db["users"]
daily_counts_col = _db["daily_counts"]
downloads_col = _db["downloads"]
file_cache_col = _db["file_cache"]
channels_col = _db["channels"]
scheduled_deletions_col = _db["scheduled_deletions"]


def _today() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d")


# ---------------------------------------------------------------------
# Users / registration
# ---------------------------------------------------------------------

async def register_user_if_new(user_id: int) -> bool:
    result = await users_col.update_one(
        {"_id": user_id},
        {"$setOnInsert": {
            "user_id": user_id,
            "first_seen": datetime.utcnow(),
            "is_banned": False,
            "premium_lifetime": False,
            "premium_until": None,
        }},
        upsert=True,
    )
    return result.upserted_id is not None


# ---------------------------------------------------------------------
# Bans
# ---------------------------------------------------------------------

async def set_banned(user_id: int, banned: bool):
    await users_col.update_one(
        {"_id": user_id},
        {"$set": {"is_banned": banned},
         "$setOnInsert": {"first_seen": datetime.utcnow(), "premium_lifetime": False, "premium_until": None}},
        upsert=True,
    )


async def is_banned(user_id: int) -> bool:
    doc = await users_col.find_one({"_id": user_id}, {"is_banned": 1})
    return bool(doc and doc.get("is_banned"))


# ---------------------------------------------------------------------
# Premium
# ---------------------------------------------------------------------

async def set_premium(user_id: int, days):
    """days=None means lifetime."""
    if days is None:
        update = {"premium_lifetime": True, "premium_until": None}
    else:
        update = {"premium_lifetime": False, "premium_until": datetime.utcnow() + timedelta(days=days)}

    await users_col.update_one(
        {"_id": user_id},
        {"$set": update,
         "$setOnInsert": {"first_seen": datetime.utcnow(), "is_banned": False}},
        upsert=True,
    )


async def remove_premium(user_id: int):
    await users_col.update_one(
        {"_id": user_id},
        {"$set": {"premium_lifetime": False, "premium_until": None}},
    )


async def get_premium_status(user_id: int) -> dict:
    doc = await users_col.find_one({"_id": user_id}, {"premium_lifetime": 1, "premium_until": 1})

    if not doc:
        return {"is_premium": False, "lifetime": False, "expires_at": None}

    if doc.get("premium_lifetime"):
        return {"is_premium": True, "lifetime": True, "expires_at": None}

    until = doc.get("premium_until")
    if until and until > datetime.utcnow():
        return {"is_premium": True, "lifetime": False, "expires_at": until}

    return {"is_premium": False, "lifetime": False, "expires_at": None}


# ---------------------------------------------------------------------
# Daily free-download limit
# ---------------------------------------------------------------------

async def get_daily_count(user_id: int) -> int:
    doc = await daily_counts_col.find_one({"_id": f"{user_id}:{_today()}"}, {"count": 1})
    return doc["count"] if doc else 0


async def bump_daily_count(user_id: int):
    day = _today()
    await daily_counts_col.update_one(
        {"_id": f"{user_id}:{day}"},
        {"$inc": {"count": 1}, "$setOnInsert": {"user_id": user_id, "day": day}},
        upsert=True,
    )


# ---------------------------------------------------------------------
# Download stats
# ---------------------------------------------------------------------

async def bump_total_downloads(user_id: int):
    await downloads_col.insert_one({"user_id": user_id, "ts": datetime.utcnow()})


async def get_user_total_downloads(user_id: int) -> int:
    return await downloads_col.count_documents({"user_id": user_id})


# ---------------------------------------------------------------------
# Custom caption (per-user override for the upload caption template)
# ---------------------------------------------------------------------

async def set_caption(user_id: int, caption: str):
    await users_col.update_one({"_id": user_id}, {"$set": {"caption": caption}}, upsert=True)


async def get_caption(user_id: int):
    doc = await users_col.find_one({"_id": user_id}, {"caption": 1})
    return (doc or {}).get("caption")


async def del_caption(user_id: int):
    await users_col.update_one({"_id": user_id}, {"$unset": {"caption": ""}})


# ---------------------------------------------------------------------
# Custom thumbnail (per-user override for the upload thumbnail)
# ---------------------------------------------------------------------

async def set_thumbnail(user_id: int, file_id: str):
    await users_col.update_one({"_id": user_id}, {"$set": {"thumbnail": file_id}}, upsert=True)


async def get_thumbnail(user_id: int):
    doc = await users_col.find_one({"_id": user_id}, {"thumbnail": 1})
    return (doc or {}).get("thumbnail")


async def del_thumbnail(user_id: int):
    await users_col.update_one({"_id": user_id}, {"$unset": {"thumbnail": ""}})


# ---------------------------------------------------------------------
# Per-user dump chat (personal backup destination for delivered files)
# ---------------------------------------------------------------------

async def set_dump_chat(user_id: int, chat_id):
    if chat_id is None:
        await users_col.update_one({"_id": user_id}, {"$unset": {"dump_chat": ""}})
    else:
        await users_col.update_one({"_id": user_id}, {"$set": {"dump_chat": chat_id}}, upsert=True)


async def get_dump_chat(user_id: int):
    doc = await users_col.find_one({"_id": user_id}, {"dump_chat": 1})
    return (doc or {}).get("dump_chat")


# ---------------------------------------------------------------------
# Index setup — safe to call every startup (create_index is idempotent).
# ---------------------------------------------------------------------

async def ensure_indexes():
    await daily_counts_col.create_index("user_id")
    await downloads_col.create_index("user_id")
    await users_col.create_index("is_banned")


async def get_stats_summary() -> dict:
    total_users = await users_col.count_documents({})
    banned_count = await users_col.count_documents({"is_banned": True})
    now = datetime.utcnow()
    premium_count = await users_col.count_documents({
        "$or": [
            {"premium_lifetime": True},
            {"premium_until": {"$ne": None, "$gt": now}},
        ]
    })
    total_downloads = await downloads_col.count_documents({})
    total_files_cached = await file_cache_col.count_documents({})

    return {
        "total_users": total_users,
        "premium_count": premium_count,
        "banned_count": banned_count,
        "total_downloads": total_downloads,
        "total_files_cached": total_files_cached,
    }


async def delete_user(user_id: int):
    await users_col.delete_one({"_id": user_id})


async def all_chat_ids() -> list:
    cursor = users_col.find({}, {"_id": 1})
    return [doc["_id"] async for doc in cursor]


async def get_all_users_full() -> list:
    """Full user docs for admin export (id, ban/premium status, first_seen)."""
    cursor = users_col.find({})
    return [doc async for doc in cursor]


# ---------------------------------------------------------------------
# File cache (instant resend via cached file_id)
# ---------------------------------------------------------------------

def _cache_key(link: str, quality: str) -> str:
    return f"{link}::{quality}"


async def get_cached_file(link: str, quality: str):
    doc = await file_cache_col.find_one({"_id": _cache_key(link, quality)})
    if not doc:
        return None

    parts = doc.get("parts")
    if not parts:
        # Legacy single-part doc written before split-download caching —
        # normalize it into the same "parts" shape so callers only ever
        # need to handle one format.
        parts = [{
            "file_id": doc.get("file_id"),
            "size": doc["size"],
            "duration": doc.get("duration", 0),
            "cache_chat_id": doc.get("cache_chat_id"),
            "cache_message_id": doc.get("cache_message_id"),
        }]

    return {
        "name": doc["name"],
        "size": doc["size"],
        "quality_label": doc["quality_label"],
        "duration": doc.get("duration", 0),
        "parts": parts,
    }


async def set_cached_file(link, quality, parts, name, size, quality_label, duration=0):
    """parts: list of {file_id, size, duration, cache_chat_id, cache_message_id},
    one entry per uploaded part in order (a single-file download just has
    one entry). Every part needs at least a file_id to be resendable, even
    if its cache-channel copy failed."""
    await file_cache_col.update_one(
        {"_id": _cache_key(link, quality)},
        {"$set": {
            "link": link,
            "quality": quality,
            "parts": parts,
            "name": name,
            "size": size,
            "quality_label": quality_label,
            "duration": duration,
            "cached_at": datetime.utcnow(),
        },
        # Drop the old flat fields on upsert-over-legacy-doc so a stale
        # top-level file_id can't linger next to the new "parts" list.
        "$unset": {
            "file_id": "",
            "cache_chat_id": "",
            "cache_message_id": "",
        }},
        upsert=True,
    )


async def delete_cached_file(link: str, quality: str):
    await file_cache_col.delete_one({"_id": _cache_key(link, quality)})


# ---------------------------------------------------------------------
# Backup-channel linking (admin-managed, dynamic — no redeploy needed)
# channels: _id: channel_id (int), added_at
# ---------------------------------------------------------------------

async def add_channel(channel_id: int) -> bool:
    """Link a channel/group so it receives a copy of every delivered file.
    Returns True if this was a new link, False if it was already linked."""
    result = await channels_col.update_one(
        {"_id": channel_id},
        {"$setOnInsert": {"added_at": datetime.utcnow()}},
        upsert=True,
    )
    return result.upserted_id is not None


async def remove_channel(channel_id: int) -> bool:
    """Unlink a single channel/group. Returns True if it was actually removed."""
    result = await channels_col.delete_one({"_id": channel_id})
    return result.deleted_count > 0


async def remove_all_channels() -> int:
    """Unlink every dynamically-added channel/group. Returns how many were removed."""
    result = await channels_col.delete_many({})
    return result.deleted_count


async def get_channels() -> list:
    """Return every dynamically-linked channel/group id."""
    cursor = channels_col.find({}, {"_id": 1})
    return [doc["_id"] async for doc in cursor]


# ---------------------------------------------------------------------
# Titanium Clone Mode — per-user connected @BotFather bot tokens.
# Stored on the user's own doc: titanium_bots: [
#   {token, username, bot_id, source, added_at, last_used}
# ]
# source: "manual" (/addbot with a pasted @BotFather token) or "managed"
# (Bot API 9.6 Managed Bots auto-create — see titanium.py).
# ---------------------------------------------------------------------

async def get_titanium_bots(user_id: int) -> list:
    doc = await users_col.find_one({"_id": user_id}, {"titanium_bots": 1})
    return (doc or {}).get("titanium_bots", [])


async def add_titanium_bot(user_id: int, token: str, username: str, bot_id: int = None, source: str = "manual"):
    entry = {
        "token": token,
        "username": username,
        "bot_id": bot_id,
        "source": source,
        "added_at": datetime.utcnow().isoformat(),
        "last_used": 0,
    }
    await users_col.update_one(
        {"_id": user_id},
        {
            "$push": {"titanium_bots": entry},
            "$setOnInsert": {
                "first_seen": datetime.utcnow(),
                "is_banned": False,
                "premium_lifetime": False,
                "premium_until": None,
            },
        },
        upsert=True,
    )


async def remove_titanium_bot(user_id: int, username: str) -> bool:
    result = await users_col.update_one(
        {"_id": user_id},
        {"$pull": {"titanium_bots": {"username": username}}},
    )
    return result.modified_count > 0


async def touch_titanium_bot(user_id: int, token: str):
    await users_col.update_one(
        {"_id": user_id, "titanium_bots.token": token},
        {"$set": {"titanium_bots.$.last_used": datetime.utcnow().timestamp()}},
    )


async def get_all_titanium_owners() -> list:
    """[(user_id, [bot_entry, ...]), ...] for every user with at least one
    connected Titanium bot — used to reconnect clones on process restart."""
    cursor = users_col.find(
        {"titanium_bots": {"$exists": True, "$ne": []}}, {"titanium_bots": 1}
    )
    return [(doc["_id"], doc.get("titanium_bots", [])) async for doc in cursor]


# ---------------------------------------------------------------------
# Scheduled deletions (auto-delete-after-N-seconds) — persisted so a
# redeploy/restart mid-wait doesn't silently lose the schedule the way a
# bare in-memory asyncio.sleep() would.
# _id: "{chat_id}:{message_id}"
# ---------------------------------------------------------------------

async def add_scheduled_deletion(chat_id: int, message_id: int, delete_at: datetime):
    await scheduled_deletions_col.update_one(
        {"_id": f"{chat_id}:{message_id}"},
        {"$set": {"chat_id": chat_id, "message_id": message_id, "delete_at": delete_at}},
        upsert=True,
    )


async def remove_scheduled_deletion(chat_id: int, message_id: int):
    await scheduled_deletions_col.delete_one({"_id": f"{chat_id}:{message_id}"})


async def get_all_scheduled_deletions() -> list:
    """[(chat_id, message_id, delete_at), ...] for every pending scheduled
    deletion — read once on boot to resume/catch up after a restart."""
    cursor = scheduled_deletions_col.find({})
    return [(doc["chat_id"], doc["message_id"], doc["delete_at"]) async for doc in cursor]
