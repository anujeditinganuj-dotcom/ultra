import os
from dotenv import load_dotenv

load_dotenv()

API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
BOT_TOKEN = os.environ["BOT_TOKEN"]
SESSION = os.environ["SESSION"]
OWNER_ID = int(os.environ["OWNER_ID"])

TG_BOT_WORKERS = int(os.getenv("TG_BOT_WORKERS", "4"))
DOWNLOAD_DIR = "downloads"
MAX_CONCURRENT_DOWNLOADS = 5

# Reserved concurrent-download slots for premium/admin users, entirely
# separate from MAX_CONCURRENT_DOWNLOADS (which is the free-user pool).
# Premium downloads have their own capacity, so they never sit in a queue
# behind free users — true "never wait" VIP treatment, not just a
# line-jump within a shared pool.
PREMIUM_RESERVED_SLOTS = int(os.getenv("PREMIUM_RESERVED_SLOTS", "2"))

# Max links accepted out of a single message. Free users are capped at
# FREE_LINK_LIMIT; premium/admin users get the higher PREMIUM_LINK_LIMIT.
FREE_LINK_LIMIT = int(os.getenv("FREE_LINK_LIMIT", "3"))
PREMIUM_LINK_LIMIT = int(os.getenv("PREMIUM_LINK_LIMIT", "20"))

# MongoDB connection. MONGO_URI is required (e.g. a MongoDB Atlas
# connection string). MONGO_DB_NAME defaults to "diskwala_bot".
MONGO_URI = os.environ["MONGO_URI"]
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "diskwala_bot")

# ---------------------------------------------------------------------
# Extra settings for premium / admin / cache features.
# ADMINS: comma-separated user ids in the ADMINS env var. OWNER_ID is
# always treated as an admin even if not listed.
# ---------------------------------------------------------------------
ADMINS = list({OWNER_ID, *[int(x) for x in os.getenv("ADMINS", "8931907813").split(",") if x.strip()]})

# Photo shown on /start. Can be a URL or a local file path.
START_PHOTO_URL = os.getenv("START_PHOTO_URL", "https://iili.io/nH8JrFI.jpg")

# Free (non-premium) users can download this many files per day (UTC).
DAILY_FREE_LIMIT = int(os.getenv("DAILY_FREE_LIMIT", "5"))

# If > 0, delivered videos are auto-deleted from the chat after this many
# seconds (the caption warns the user to forward it first). 0 disables it.
AUTO_DELETE_SECONDS = int(os.getenv("AUTO_DELETE_SECONDS", "0"))

# Optional: channel id (e.g. -100xxxxxxxxxx) where new-user/download logs
# are posted. Leave unset/empty to disable logging.
LOG_CHANNEL_ID = int(os.getenv("LOG_CHANNEL_ID", "-1003925649805")) or None

# Optional: comma-separated channel ids to also receive a copy of every
# delivered video (a simple off-site backup). Leave empty to disable.
BACKUP_CHANNEL_IDS = [int(x) for x in os.getenv("BACKUP_CHANNEL_IDS", "").split(",") if x.strip()]

# Optional: a private channel (bot must be admin there) where one copy of
# every freshly-uploaded video is stored. Cache hits are then served with
# copy_message() from this channel instead of a bare file_id — this gives
# Telegram a fresh file_reference for the recipient, which fixes cache
# hits failing/redownloading when a *different* user requests a link that
# someone else already downloaded (the old file_id is only guaranteed
# valid for the chat it was originally sent to). Leave unset to fall back
# to the old file_id-only behaviour.
CACHE_CHANNEL_ID = int(os.getenv("CACHE_CHANNEL_ID", "-1003925649805")) or None

# Telegram bots can't upload a single file bigger than ~2 GiB. Any
# downloaded video larger than this is auto-split into parts (each capped
# at SPLIT_PART_SIZE) before uploading. 1900 MiB per part leaves headroom
# below the 2000 MiB hard limit for container/encoding overhead.
SPLIT_THRESHOLD_BYTES = int(os.getenv("SPLIT_THRESHOLD_MB", "1950")) * 1024 * 1024
SPLIT_PART_SIZE_BYTES = int(os.getenv("SPLIT_PART_SIZE_MB", "1900")) * 1024 * 1024

# ---------------------------------------------------------------------
# Speed tuning
# ---------------------------------------------------------------------

# How many parallel MTProto connections pyrogram/kurigram opens for a
# single file transfer (upload OR pyrogram-side download, e.g. custom
# thumbnails). This is the single biggest lever for upload speed — with
# the default of 1, every part of a video goes out sequentially over one
# connection; bumping this lets several parts fly in parallel. Safe range
# is roughly 4-12; higher isn't free (more sockets/CPU for encryption),
# so it's tunable via env instead of hardcoded.
MAX_CONCURRENT_TRANSMISSIONS = int(os.getenv("MAX_CONCURRENT_TRANSMISSIONS", "6"))

# How many parallel HTTP Range connections to open against the source
# (Diskwala) CDN when downloading a video. Most CDNs throttle *per
# connection*, not per file, so splitting one big sequential download
# into N concurrent range requests is usually the biggest real-world
# download-speed win available. Set to 1 to force the old single-connection
# behaviour. Segmented downloads still fall back automatically to a plain
# single-connection stream whenever the source doesn't advertise Range
# support, so this is safe to leave on even against unknown/odd CDNs.
DOWNLOAD_CONNECTIONS = int(os.getenv("DOWNLOAD_CONNECTIONS", "8"))

# Below this size, segmentation overhead (extra HTTP handshakes, a sparse
# pre-allocated file) isn't worth it over a single connection.
MIN_SEGMENTED_DOWNLOAD_BYTES = int(os.getenv("MIN_SEGMENTED_DOWNLOAD_MB", "20")) * 1024 * 1024
