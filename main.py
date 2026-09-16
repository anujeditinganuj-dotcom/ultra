import os
import glob
import asyncio
import html
import inspect
import json
import logging
import random
import re
import time
import uuid
import requests
import aiohttp
from urllib.parse import quote
from datetime import datetime, timezone, timedelta

from pyrogram import Client, filters, idle
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton, BotCommand
from pyrogram.enums import ParseMode, ChatMemberStatus
from pyrogram.errors import FloodWait, InputUserDeactivated, UserIsBlocked, PeerIdInvalid

try:
    from pyrogram.enums import ButtonStyle
    BUTTON_STYLE_SUPPORTED = True
except ImportError:
    BUTTON_STYLE_SUPPORTED = False

from config import (
    API_ID, API_HASH, BOT_TOKEN, SESSION, OWNER_ID, TG_BOT_WORKERS,
    DOWNLOAD_DIR, MAX_CONCURRENT_DOWNLOADS, ADMINS, START_PHOTO_URL,
    DAILY_FREE_LIMIT, AUTO_DELETE_SECONDS, LOG_CHANNEL_ID, BACKUP_CHANNEL_IDS,
    SPLIT_THRESHOLD_BYTES, SPLIT_PART_SIZE_BYTES, CACHE_CHANNEL_ID,
    PREMIUM_RESERVED_SLOTS, FREE_LINK_LIMIT, PREMIUM_LINK_LIMIT,
    MAX_CONCURRENT_TRANSMISSIONS, DOWNLOAD_CONNECTIONS, MIN_SEGMENTED_DOWNLOAD_BYTES,
)
from diskwala import fetch_diskwala_video, extract_diskwala_links, DiskwalaAuthError, fetch_playlist_info, is_playlist_link, fetch_diskwala_preview, generate_flezen_cookie
from keep_alive import keep_alive, register_stream_proxy, get_stream_public_url
import titanium
from database import (
    register_user_if_new, is_banned, set_banned,
    get_premium_status, set_premium, remove_premium,
    get_daily_count, bump_daily_count, bump_total_downloads,
    get_user_total_downloads,
    set_caption, get_caption, del_caption,
    set_thumbnail, get_thumbnail, del_thumbnail,
    set_dump_chat, get_dump_chat,
    get_stats_summary, all_chat_ids, delete_user, get_all_users_full,
    get_cached_file, set_cached_file, delete_cached_file,
    add_channel, remove_channel, remove_all_channels, get_channels,
    add_scheduled_deletion, remove_scheduled_deletion, get_all_scheduled_deletions,
    ensure_indexes,
    set_referrer, increment_referral_count, get_referral_count,
    get_referral_rewards_claimed, mark_referral_reward_claimed,
    grant_referral_premium_days,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("diskwala_bot")

os.makedirs(DOWNLOAD_DIR, exist_ok=True)

_app_kwargs = dict(
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    workers=TG_BOT_WORKERS,
)
# max_concurrent_transmissions lets pyrogram/kurigram upload (and
# pyrogram-side download) several file parts over parallel MTProto
# connections instead of one at a time — the main lever for upload speed.
# Guarded with a signature check so this stays a no-op instead of a crash
# on a kurigram/pyrogram build that predates the parameter.
try:
    if "max_concurrent_transmissions" in inspect.signature(Client.__init__).parameters:
        _app_kwargs["max_concurrent_transmissions"] = MAX_CONCURRENT_TRANSMISSIONS
except Exception:
    pass

app = Client("diskwala_bot", **_app_kwargs)

class DownloadQueue:
    """Tracks who's actively downloading vs waiting for a free slot, so a
    waiting user gets a live queue position instead of silently hanging,
    and so /cancel can pull someone out of the queue before their turn
    even starts. Plain FIFO within one instance — premium gets VIP
    treatment by having its own separate DownloadQueue instance entirely
    (see premium_download_queue below), not by cutting in line here."""

    def __init__(self, max_concurrent: int):
        self.max_concurrent = max_concurrent
        self.active = 0
        self.waiters: list = []  # [(user_id, asyncio.Future), ...] in wait order

    async def acquire(self, user_id: int):
        # No await between the check and the increment below, so this is
        # atomic under asyncio's single-threaded cooperative scheduling —
        # no separate lock needed.
        if self.active < self.max_concurrent and not self.waiters:
            self.active += 1
            return
        fut = asyncio.get_running_loop().create_future()
        self.waiters.append((user_id, fut))
        try:
            await fut
        except asyncio.CancelledError:
            # Cancelled while still waiting (e.g. /cancel) — drop our own
            # entry if release() hasn't already popped it.
            self.waiters[:] = [w for w in self.waiters if w[1] is not fut]
            raise

    def release(self):
        if self.waiters:
            _, fut = self.waiters.pop(0)
            if not fut.done():
                fut.set_result(None)
            # Slot is handed straight to the next waiter; active count
            # (i.e. number of concurrently-running downloads) is unchanged.
        else:
            self.active = max(0, self.active - 1)

    def position(self, user_id: int) -> int:
        """1-based queue position among waiters, 0 if not waiting."""
        for i, (uid, _) in enumerate(self.waiters):
            if uid == user_id:
                return i + 1
        return 0

    def cancel_wait(self, user_id: int) -> bool:
        for uid, fut in self.waiters:
            if uid == user_id and not fut.done():
                fut.cancel()
                return True
        return False


# Two entirely separate pools: free users share MAX_CONCURRENT_DOWNLOADS
# slots, premium/admin users have their own PREMIUM_RESERVED_SLOTS on top
# of that. A premium download never queues behind a free one — it only
# ever waits behind other premium downloads that are already running,
# which is real "never wait (because of free users)" VIP treatment rather
# than just a priority line-jump within a shared pool.
free_download_queue = DownloadQueue(MAX_CONCURRENT_DOWNLOADS)
premium_download_queue = DownloadQueue(PREMIUM_RESERVED_SLOTS)
ACTIVE_TASKS: dict = {}  # user_id -> asyncio.Task, tracks the in-flight download/upload so /cancel can stop it

_auth_cache = {"token": None, "expires": 0}

# Maps a short id (used in callback_data, which has a size limit) -> the
# original link. Entries are created when a link is received and cleaned
# up lazily; they aren't meant to survive a bot restart.
LINK_CACHE: dict[str, str] = {}

# ---------------------------------------------------------------------
# Safe, colored InlineKeyboardButton builder
# ---------------------------------------------------------------------
# Uses Telegram's colored inline-button style (blue "primary" for normal
# actions, red "danger" for cancel/close/destructive actions) when the
# installed pyrogram build supports it. Falls back to a plain button
# automatically on older pyrogram versions, so this never breaks the bot.

def make_button(text: str, callback_data: str = None, url: str = None,
                 style: "ButtonStyle" = None) -> InlineKeyboardButton:
    kwargs = {"text": text}
    if callback_data:
        kwargs["callback_data"] = callback_data
    if url:
        kwargs["url"] = url
    if BUTTON_STYLE_SUPPORTED and style is not None:
        kwargs["style"] = style
    return InlineKeyboardButton(**kwargs)


# Shorthand style constants (None on unsupported pyrogram builds, so
# passing them into make_button() is always safe).
BTN_PRIMARY = ButtonStyle.PRIMARY if BUTTON_STYLE_SUPPORTED else None
BTN_DANGER = ButtonStyle.DANGER if BUTTON_STYLE_SUPPORTED else None


def make_reply_button(text: str, style: "ButtonStyle" = None):
    """Same idea as make_button() but for the reply keyboard (the one
    pinned above the message box, not the inline buttons under a
    message). Falls back to a plain string button — which is exactly
    what Pyrogram expects for an unstyled reply-keyboard button — if
    the installed pyrogram build's KeyboardButton doesn't accept a
    style kwarg yet."""
    if BUTTON_STYLE_SUPPORTED and style is not None:
        try:
            return KeyboardButton(text=text, style=style)
        except TypeError:
            pass
    return text


# ---------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------

async def get_effective_premium_status(user_id: int) -> dict:
    """Same shape as database.get_premium_status(), but bot admins (see
    ADMINS in config.py) are always treated as Lifetime Premium — they
    never need to buy/be granted premium separately, and are never
    subject to the daily free-download limit."""
    if user_id in ADMINS:
        return {"is_premium": True, "lifetime": True, "expires_at": None}
    return await get_premium_status(user_id)


def human_size(n: float) -> str:
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if n < 1024:
            return f"{n:.2f} {unit}"
        n /= 1024
    return f"{n:.2f} PB"


def human_speed(n: float) -> str:
    for unit in ["B/s", "KB/s", "MB/s", "GB/s"]:
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB/s"


def human_time(seconds: float) -> str:
    seconds = max(0, int(seconds))
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{h}h {m}m {s}s"
    if m:
        return f"{m}m {s}s"
    return f"{s}s"


def hms(seconds: float) -> str:
    """Format seconds as H:MM:SS (e.g. 0:06:54)."""
    seconds = max(0, int(round(seconds)))
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    return f"{h}:{m:02d}:{s:02d}"


_SMALLCAPS_MAP = str.maketrans(
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ",
    "ᴀʙᴄᴅᴇғɢʜɪᴊᴋʟᴍɴᴏᴘǫʀsᴛᴜᴠᴡxʏᴢᴀʙᴄᴅᴇғɢʜɪᴊᴋʟᴍɴᴏᴘǫʀsᴛᴜᴠᴡxʏᴢ",
)


def smallcaps(text: str) -> str:
    return text.translate(_SMALLCAPS_MAP)


# Matches a Telegram-style @username/@bot mention (letters/digits/
# underscores, must start with a letter, 4-32 chars after the @).
_TAG_OR_MENTION_RE = re.compile(r"(<[^>]+>|@[A-Za-z][A-Za-z0-9_]{3,31})")


def smallcaps_html(text):
    """Small-caps every plain-text word in a message, but leaves HTML tags
    (<b>, <a href="...">, <blockquote>, etc.), the contents of
    <code>...</code>, and @username/@bot mentions completely untouched —
    so commands, UPI IDs, chat IDs, {placeholder} examples, and bot
    usernames the user needs to copy-paste or tap stay exactly as typed,
    while everything else gets the small-caps look. Mentions specifically
    have to stay literal ASCII, not just readable: Telegram only
    auto-links a plain @username as a tappable mention when the text is
    an exact match, so small-caps glyphs there would silently turn a
    clickable bot link into dead text.
    Safe to call on non-strings (e.g. None) — passes them through as-is."""
    if not isinstance(text, str):
        return text
    parts = _TAG_OR_MENTION_RE.split(text)
    out = []
    in_code = 0
    for part in parts:
        if part.startswith("<") and part.endswith(">"):
            lower = part.lower()
            if lower.startswith("<code"):
                in_code += 1
            elif lower.startswith("</code"):
                in_code = max(0, in_code - 1)
            out.append(part)
        elif part.startswith("@"):
            out.append(part)
        else:
            out.append(part if in_code else part.translate(_SMALLCAPS_MAP))
    return "".join(out)


SC = smallcaps_html

POWERED_BY = "Anuj Kumar"  # change this to whatever name/credit you want shown
POWERED_BY_URL = "https://t.me/anujedits97"  # change this to the profile/channel to link to


async def build_caption(name: str, size_bytes: int, dl_seconds: float, ul_seconds: float,
                         user_id: int, source_link: str, quality_label: str = "Auto (Best)",
                         duration_seconds: float = 0) -> str:
    auto_delete_note = ""
    if AUTO_DELETE_SECONDS > 0:
        auto_delete_note = (
            f"⚠️ This file will auto-delete from here in {human_time(AUTO_DELETE_SECONDS)}.\n"
            "📤 Please forward it to any other chat to save it permanently.\n"
        )

    custom_caption = await get_caption(user_id)
    if custom_caption:
        text = (
            custom_caption
            .replace("{filename}", name)
            .replace("{size}", human_size(size_bytes))
            .replace("{quality}", quality_label)
            .replace("{source}", source_link)
            .replace("{duration}", hms(duration_seconds) if duration_seconds else "Unknown")
        )
        if auto_delete_note:
            text += f"\n\n{auto_delete_note}"
        return text

    powered_text = smallcaps(POWERED_BY)
    powered_html = f'<a href="{POWERED_BY_URL}">{powered_text}</a>' if POWERED_BY_URL else powered_text

    source_text = smallcaps("Flezen Link") if source_link and "flezen.com" in source_link.lower() else smallcaps("Diskwala Link")
    source_html = f'<a href="{source_link}">{source_text}</a>' if source_link else source_text

    return (
        "<blockquote>"
        f"📄 {smallcaps('File Name')}: {smallcaps(name)}\n"
        f"📦 {smallcaps('Size')}: {human_size(size_bytes)}\n"
        f"🎞️ {smallcaps('Quality')}: {smallcaps(quality_label)}\n"
        f"⏱️ {smallcaps('Duration')}: {hms(duration_seconds) if duration_seconds else smallcaps('Unknown')}\n"
        f"⬇️ {smallcaps('Downloaded in')}: {hms(dl_seconds)} sec\n"
        f"⬆️ {smallcaps('Uploaded in')}: {hms(ul_seconds)} sec\n"
        f"🙋 {smallcaps('Uploaded by')}: {user_id}\n"
        f"🔗 {smallcaps('Source')}: {source_html}\n"
        f"{auto_delete_note}"
        "</blockquote>\n\n"
        f"⚡ {smallcaps('Powered by')} {powered_html}"
    )


def progress_bar(pct: float, width: int = 10) -> str:
    filled = min(width, int(width * pct / 100))
    return "⬢" * filled + "⬡" * (width - filled)


class ProgressTracker:
    """Throttled Telegram status-message updater for download/upload progress."""

    def __init__(self, status_msg: Message, label: str, name: str,
                 interval: float = 3.0, quality: str = None):
        self.status_msg = status_msg
        self.label = label
        self.name = name
        self.interval = interval
        self.quality = quality
        self.start_time = time.time()
        self.last_edit_time = 0.0

    async def update(self, current: int, total: int):
        now = time.time()
        is_done = total and current >= total
        if not is_done and (now - self.last_edit_time) < self.interval:
            return
        self.last_edit_time = now

        elapsed = now - self.start_time
        speed = current / elapsed if elapsed > 0 else 0
        pct = (current / total * 100) if total else 0
        eta = (total - current) / speed if speed > 0 and total else 0

        emoji = "📥" if "download" in self.label.lower() else "📤"
        quality_line = f"┣⪼ 🎞 Quality: {self.quality}\n" if self.quality else ""
        try:
            await self.status_msg.edit_text(
                SC(f"{emoji} <b>{self.label}...</b>\n\n"
                "╭━━━━❰Progress❱━➣\n"
                f"┣⪼ 🎬 File: <code>{self.name}</code>\n"
                f"{quality_line}"
                f"┣⪼ [{progress_bar(pct)}]\n"
                f"┣⪼ ✅ {pct:.1f}%\n"
                f"┣⪼ 💾 {human_size(current)} / {human_size(total)}\n"
                f"┣⪼ ⚡ {human_speed(speed)}\n"
                f"┣⪼ 🕐 Elapsed: {human_time(elapsed)}\n"
                f"┣⪼ ⏳ ETA: {human_time(eta)}\n"
                "╰━━━━━━━━━━━━━━━➣"),
                parse_mode=ParseMode.HTML,
            )
        except Exception:
            pass


# ---------------------------------------------------------------------
# Menus / keyboards
# ---------------------------------------------------------------------

MAIN_MENU_KB = ReplyKeyboardMarkup(
    [
        [make_reply_button("💎 ᴘʟᴀɴs", style=BTN_PRIMARY), make_reply_button("📊 ᴍʏ sᴛᴀᴛᴜs", style=BTN_PRIMARY)],
        [make_reply_button("❓ ʜᴇʟᴘ", style=BTN_PRIMARY), make_reply_button("☎️ sᴜᴘᴘᴏʀᴛ", style=BTN_PRIMARY)],
    ],
    resize_keyboard=True,
)

MENU_BUTTON_TEXTS = {"💎 ᴘʟᴀɴs", "📊 ᴍʏ sᴛᴀᴛᴜs", "❓ ʜᴇʟᴘ", "☎️ sᴜᴘᴘᴏʀᴛ"}


# ---------------------------------------------------------------------
# Menu button matching — robust to variation-selector drift
# ---------------------------------------------------------------------
# Some Telegram clients don't echo back the invisible U+FE0F "variation
# selector" byte the same way it was sent in a button label (☎️/❓ are
# emoji-presentation sequences that use it). That means a plain
# `filters.regex(r"^☎️ sᴜᴘᴘᴏʀᴛ$")` exact match can silently fail on some
# devices even though the button visually looks identical — the tap then
# falls through to the generic "not a link" handler and looks like the
# button "does nothing". We normalize away U+FE0F (and stray whitespace)
# on both sides before comparing so taps match regardless of client quirks.
def _normalize_menu_text(s: str) -> str:
    return (s or "").replace("\ufe0f", "").strip()


_NORMALIZED_MENU_BUTTON_TEXTS = {_normalize_menu_text(t) for t in MENU_BUTTON_TEXTS}


def menu_text_filter(expected: str):
    """Filter that matches a message/caption equal to `expected`, ignoring
    variation-selector (U+FE0F) differences and surrounding whitespace."""
    norm_expected = _normalize_menu_text(expected)

    async def _func(_, __, m):
        text = m.text or m.caption or ""
        return _normalize_menu_text(text) == norm_expected

    return filters.create(_func)


async def _not_menu_button_func(_, __, m):
    text = m.text or m.caption or ""
    return _normalize_menu_text(text) not in _NORMALIZED_MENU_BUTTON_TEXTS


NOT_MENU_BUTTON_FILTER = filters.create(_not_menu_button_func)

# ---------------------------------------------------------------------
# Premium plans — informational only, there's no payment gateway wired
# up here. A plan tap tells the user how to contact the admin to get it
# activated manually via /addpremium.
# ---------------------------------------------------------------------
PLANS = [
    (19, "12 Days"),
    (29, "21 Days"),
    (45, "35 Days"),
    (99, "99 Days"),
    (999, "Lifetime Access"),
]

PLANS_PHOTO_URL = "https://iili.io/nHyIqox.jpg"

PLANS_TEXT = (
    "💎 <b>ᴘʀᴇᴍɪᴜᴍ ᴍᴇᴍʙᴇʀsʜɪᴘ ᴘʟᴀɴs</b>\n"
    "✨ Unlock Unlimited Access & Advanced Features!\n\n"
    "• ₹19 → 12 Days\n"
    "• ₹29 → 21 Days\n"
    "• ₹45 → 35 Days\n"
    "• ₹99 → 99 Days\n"
    "• ₹999 → Lifetime Access ♾️\n\n"
    "🔒 <b>sᴇᴄᴜʀᴇ ᴘᴀʏᴍᴇɴᴛ:</b>\n"
    "⚡️ ᴜᴘɪ ɪᴅ: <code>971916880@ybl</code>\n"
    "🔗 ǫʀ ᴄᴏᴅᴇ: <a href=\"https://iili.io/nHyIqox.jpg\">Scan to Pay</a>\n"
    "💡 After Payment: Send Screenshot to Admin for Instant Activation.\n\n"
    "👇 Plan pe tap karo — shuru ho jao!"
)


def plans_keyboard() -> InlineKeyboardMarkup:
    rows = []
    for price, label in PLANS:
        tag = " ♾️" if price == 999 else ""
        rows.append([make_button(SC(f"💎 ₹{price} - {label}{tag}"), callback_data=f"plan_{price}", style=BTN_PRIMARY)])
    rows.append([make_button(SC("📸 Send Payment Proof"), url=POWERED_BY_URL, style=BTN_PRIMARY)])
    rows.append([make_button(SC("⬅️ Back"), callback_data="plans_back", style=BTN_DANGER)])
    return InlineKeyboardMarkup(rows)


@app.on_callback_query(filters.regex(r"^plans_back$"))
async def plans_back_cb(client: Client, query):
    try:
        await query.message.delete()
    except Exception:
        pass
    await query.answer()


async def send_plans_message(m: Message):
    """Sends the QR/payment photo with the plans text as its caption,
    falling back to plain text if the photo can't be fetched/sent."""
    try:
        await m.reply_photo(
            PLANS_PHOTO_URL,
            caption=SC(PLANS_TEXT),
            reply_markup=plans_keyboard(),
            parse_mode=ParseMode.HTML,
        )
    except Exception as e:
        logger.warning(f"plans photo failed, falling back to text: {e}")
        await m.reply(SC(PLANS_TEXT), reply_markup=plans_keyboard(), parse_mode=ParseMode.HTML)


def status_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [make_button(SC("💎 View Plans"), callback_data="show_plans", style=BTN_PRIMARY)],
        [make_button(SC("📞 Contact Admin"), url=POWERED_BY_URL, style=BTN_PRIMARY)],
    ])


def link_menu_markup(link_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [make_button(SC("🔽 Download"), callback_data=f"dlq|{link_id}", style=BTN_PRIMARY)],
        [make_button(SC("🔗 Stream Link"), callback_data=f"stream|{link_id}", style=BTN_PRIMARY)],
        [make_button(SC("❌ Cancel"), callback_data=f"cancel|{link_id}", style=BTN_DANGER)],
    ])


# ---------------------------------------------------------------------
# Auth (Diskwala mini-app token via Telethon)
# ---------------------------------------------------------------------

# Persistent Telethon client — created once, reused for every token fetch.
# Creating a new client + connect + disconnect per request adds 3-5s of
# overhead; a persistent client reduces this to a single async call.
_telethon_client = None
_telethon_lock = asyncio.Lock()


async def _get_persistent_telethon_client():
    """Return a connected Telethon client, creating/reconnecting if needed."""
    global _telethon_client
    from telethon import TelegramClient
    from telethon.sessions import StringSession
    if _telethon_client is None:
        _telethon_client = TelegramClient(StringSession(SESSION), API_ID, API_HASH)
    if not _telethon_client.is_connected():
        await _telethon_client.connect()
    return _telethon_client


async def get_auth_token() -> str:
    if _auth_cache["token"] and time.time() < _auth_cache["expires"]:
        return _auth_cache["token"]

    async with _telethon_lock:
        # Double-check after acquiring lock — another coroutine may have refreshed
        if _auth_cache["token"] and time.time() < _auth_cache["expires"]:
            return _auth_cache["token"]

        from telethon.tl.functions.messages import RequestAppWebViewRequest
        from telethon.tl.types import InputBotAppShortName, InputPeerSelf, DataJSON
        from urllib.parse import urlparse, unquote

        client = await _get_persistent_telethon_client()
        bot = await client.get_input_entity("sky577bot")
        r = await client(RequestAppWebViewRequest(
            peer=InputPeerSelf(),
            app=InputBotAppShortName(bot_id=bot, short_name="open"),
            platform="android",
            write_allowed=True,
            start_param="",
            theme_params=DataJSON("{}"),
        ))
        token = unquote(urlparse(r.url).fragment.split("tgWebAppData=", 1)[1].split("&tgWebAppVersion=", 1)[0])
        _auth_cache["token"] = token
        _auth_cache["expires"] = time.time() + 3600  # 1 hour — same as TeraBox project
        return token


async def resolve_diskwala_with_retry(link: str) -> dict:
    """Fetch Diskwala/Flezen video info, auto-refreshing the auth token once
    if the API rejects it (HTTP 401/403) instead of failing until the
    30-minute cache naturally expires."""
    auth = await get_auth_token()
    try:
        return await asyncio.to_thread(fetch_diskwala_video, link, auth)
    except DiskwalaAuthError as e:
        logger.warning(f"Diskwala auth token rejected ({e}), refreshing and retrying once...")
        _auth_cache["token"] = None
        _auth_cache["expires"] = 0
        auth = await get_auth_token()
        return await asyncio.to_thread(fetch_diskwala_video, link, auth)


# Short-lived cache of resolved link info, keyed by the link itself. The
# link-received preview (process_link) resolves the link just to grab its
# thumbnail; caching that result means tapping Download/Stream Link right
# after doesn't hit the Diskwala API a second time for the same link.
_LINK_INFO_CACHE: dict = {}  # link -> (fetched_at, video_info)
_LINK_INFO_TTL = 180  # seconds — download URLs from the API can be short-lived, don't reuse for too long


async def resolve_diskwala_cached(link: str) -> dict:
    cached = _LINK_INFO_CACHE.get(link)
    if cached and time.time() - cached[0] < _LINK_INFO_TTL:
        return cached[1]
    video_info = await resolve_diskwala_with_retry(link)
    _LINK_INFO_CACHE[link] = (time.time(), video_info)
    if len(_LINK_INFO_CACHE) > 1000:
        cutoff = time.time() - _LINK_INFO_TTL
        for k in [k for k, (ts, _) in _LINK_INFO_CACHE.items() if ts < cutoff]:
            _LINK_INFO_CACHE.pop(k, None)
    return video_info


# ---------------------------------------------------------------------
# Misc helpers: auto-delete, backup channels, admin log
# ---------------------------------------------------------------------

DELETE_NOTICE_PHOTO = "https://iili.io/CyoYhNI.jpg"
DELETE_NOTICE_TEXT = (
    "Your video / file has been deleted due to restriction.\n\n"
    "if you want to see it again please re download and save.\n\n"
    "आपका विडियो / फाइल डिलीट कर दी गयी है आपको फिर से देखनी है तो फिर से डाउनलोड कर सकते है धन्यवाद!"
)


async def schedule_delete(client: Client, chat_id: int, message_id: int):
    """Delete a delivered file after AUTO_DELETE_SECONDS and drop a notice in
    its place.

    The scheduled deletion is persisted to MongoDB (add_scheduled_deletion)
    before the wait starts. Previously this only lived in an in-memory
    asyncio.sleep() — if the bot process restarted for ANY reason before the
    hour was up (a redeploy, Render's free-tier instance spinning down and
    back up, a crash), that task was simply gone and the video would never
    get auto-deleted at all. resume_scheduled_deletions() (called on every
    boot) reads this same collection and picks up exactly where it left off.
    """
    if AUTO_DELETE_SECONDS <= 0:
        return
    delete_at = datetime.utcnow() + timedelta(seconds=AUTO_DELETE_SECONDS)
    await add_scheduled_deletion(chat_id, message_id, delete_at)
    await asyncio.sleep(AUTO_DELETE_SECONDS)
    await _execute_scheduled_deletion(client, chat_id, message_id)


async def _execute_scheduled_deletion(client: Client, chat_id: int, message_id: int):
    """Delete the message + drop the notice, then clear its persisted
    record. Shared by the live wait in schedule_delete() and the
    startup catch-up sweep in resume_scheduled_deletions()."""
    try:
        await client.delete_messages(chat_id, message_id)
    except Exception as e:
        # Message may already be gone (user deleted it, chat cleared, etc.) —
        # nothing to notify about in that case.
        logger.warning(f"Auto-delete failed for {chat_id}/{message_id}: {e}")
        await remove_scheduled_deletion(chat_id, message_id)
        return

    try:
        await asyncio.wait_for(
            client.send_photo(
                chat_id=chat_id,
                photo=DELETE_NOTICE_PHOTO,
                caption=DELETE_NOTICE_TEXT,
                parse_mode=ParseMode.HTML,
            ),
            timeout=10,
        )
    except Exception as e:
        logger.warning(f"Couldn't send delete-notice photo to {chat_id}, falling back to text: {e}")
        try:
            await client.send_message(chat_id, SC(DELETE_NOTICE_TEXT), parse_mode=ParseMode.HTML)
        except Exception:
            pass
    await remove_scheduled_deletion(chat_id, message_id)


async def resume_scheduled_deletions(client: Client):
    """Called once on every boot. Reads every pending scheduled-deletion
    record left in MongoDB and either deletes it immediately (if its time
    already passed while the bot was down) or schedules the remaining wait
    — so a redeploy/restart mid-wait no longer means the video sits
    undeleted forever."""
    try:
        pending = await get_all_scheduled_deletions()
    except Exception as e:
        logger.warning(f"resume_scheduled_deletions: failed to load pending deletions: {e}")
        return

    for chat_id, message_id, delete_at in pending:
        remaining = (delete_at - datetime.utcnow()).total_seconds()

        async def _run(chat_id=chat_id, message_id=message_id, remaining=remaining):
            if remaining > 0:
                await asyncio.sleep(remaining)
            await _execute_scheduled_deletion(client, chat_id, message_id)

        asyncio.create_task(_run())

    if pending:
        logger.info(f"resume_scheduled_deletions: resumed {len(pending)} pending auto-delete(s).")


async def backup_to_linked_channels(client: Client, chat_id: int, message_id: int):
    """Best-effort copy of a delivered file into every linked backup channel —
    both the static ones from config (BACKUP_CHANNEL_IDS) and the ones admins
    have linked dynamically via /set_channel_id. Failures (bot not admin
    there, channel deleted, etc.) are logged and otherwise ignored; this must
    never break the user-facing download flow."""
    dynamic_ids = await get_channels()
    all_ids = set(BACKUP_CHANNEL_IDS) | set(dynamic_ids)
    for channel_id in all_ids:
        try:
            await client.copy_message(chat_id=channel_id, from_chat_id=chat_id, message_id=message_id)
        except Exception as e:
            logger.warning(f"Backup to {channel_id} failed: {e}")


async def forward_to_dump_chat(client: Client, chat_id: int, message_id: int):
    """Best-effort copy of a delivered file into this user's own personal
    dump chat (set via /setchat), if they have one configured."""
    dump_chat = await get_dump_chat(chat_id)
    if not dump_chat:
        return
    try:
        await client.copy_message(chat_id=dump_chat, from_chat_id=chat_id, message_id=message_id)
    except Exception as e:
        logger.warning(f"Dump-chat forward to {dump_chat} for user {chat_id} failed: {e}")


async def log_event(client: Client, text: str):
    if not LOG_CHANNEL_ID:
        return
    try:
        await client.send_message(LOG_CHANNEL_ID, text, parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.warning(f"Log event failed: {e}")


CACHE_CHANNEL_HEALTH_CHECK_INTERVAL = 30 * 60  # seconds
_cache_channel_last_ok = True  # tracks state so we alert once on failure, once on recovery


async def _alert_admins(client: Client, text: str):
    """Best-effort DM to every admin, on top of the log channel — cache
    breakage is the kind of thing an admin should notice even if they
    don't have LOG_CHANNEL_ID open."""
    await log_event(client, text)
    for admin_id in ADMINS:
        try:
            await client.send_message(admin_id, text, parse_mode=ParseMode.HTML)
        except Exception:
            pass


async def check_cache_channel_health(client: Client) -> bool:
    """Verify the bot can still post into CACHE_CHANNEL_ID (admin rights
    intact, channel still exists). Previously a removed/deleted cache
    channel would only surface as a per-request warning in the logs —
    this makes the failure visible to admins instead of silent."""
    global _cache_channel_last_ok
    if not CACHE_CHANNEL_ID:
        return True

    ok = False
    reason = ""
    try:
        member = await client.get_chat_member(CACHE_CHANNEL_ID, "me")
        if member.status == ChatMemberStatus.ADMINISTRATOR:
            ok = member.privileges is None or getattr(member.privileges, "can_post_messages", True)
            if not ok:
                reason = "bot is admin in the cache channel but lacks post-message permission"
        elif member.status == ChatMemberStatus.OWNER:
            ok = True
        else:
            reason = f"bot is no longer an admin in the cache channel (status: {member.status})"
    except Exception as e:
        reason = f"cache channel unreachable ({e})"

    if ok and not _cache_channel_last_ok:
        await _alert_admins(client, "✅ <b>Cache channel recovered</b> — caching is back to normal.")
    elif not ok and _cache_channel_last_ok:
        await _alert_admins(
            client,
            "🚨 <b>Cache channel broken!</b>\n\n"
            f"Reason: {reason}\n\n"
            "Every cache write/hit that needs a fresh file_reference will fail "
            "silently until this is fixed (bot removed as admin, or the "
            "channel was deleted). Re-add the bot as admin, or update "
            "CACHE_CHANNEL_ID and restart.",
        )
    _cache_channel_last_ok = ok
    return ok


async def cache_channel_health_check_loop(client: Client):
    if not CACHE_CHANNEL_ID:
        return
    while True:
        try:
            await check_cache_channel_health(client)
        except Exception as e:
            logger.warning(f"Cache-channel health check loop error: {e}")
        await asyncio.sleep(CACHE_CHANNEL_HEALTH_CHECK_INTERVAL)


# ---------------------------------------------------------------------
# Random reaction on /start or any command
# ---------------------------------------------------------------------

REACTIONS = [
    # ── Telegram Official Reactions ──────────────────────
    "👍", "👎", "❤️", "🔥", "🥰", "👏", "😁", "🤔",
    "🤯", "😱", "🤬", "😢", "🎉", "🤩",
    "🙏", "👌", "🕊", "🤡", "🥱", "🥴", "😍", "🐳",
    "❤️‍🔥", "🌚", "🌭", "💯", "🤣", "⚡", "🍌", "🏆",
    "💔", "🤨", "😐", "🍓", "🍾", "💋", "😈", "😴",
    "😭", "🤓", "👻", "👨‍💻", "👀", "🎃", "🙈", "😇",
    "😨", "🤝", "✍", "🤗", "🫡", "🎅", "🎄", "☃",
    "💅", "🤪", "🗿", "🆒", "💘", "🙉", "🦄", "😘",
    "💊", "🙊", "😎", "👾", "🤷‍♂️", "🤷‍♀️", "😡",
    # ── Premium / Money / Diamond vibes ──────────────────
    "💎", "👑", "💰", "🪙", "💵", "💴", "💶", "💷",
    "💸", "💳", "🏦", "🤑", "💹", "📈", "🏅", "🥇",
    "🎖", "⚜️", "🔱", "♾️",
    # ── Fire / Energy / Power ────────────────────────────
    "🌟", "✨", "💫", "🌠", "☄️", "💥", "⭐", "🌙",
    "🌈", "🪄", "🎯", "🛡", "🚀", "⚔️", "🗡", "🔥",
    # ── Cute / Fun ───────────────────────────────────────
    "🥹", "🫶", "🫠", "🫣", "🥺", "🤭", "🫢", "🤌",
    "🤙", "🤞", "🫰", "🤟", "🫵", "✌️", "🤘",
    # ── Hearts ───────────────────────────────────────────
    "🧡", "💛", "💚", "💙", "💜", "🖤", "🤍", "🤎",
    "💝", "💖", "💗", "💓", "💞", "💕", "💟", "❣️",
]


def _is_command_message(_, __, m: Message) -> bool:
    """True for any message that starts with a bot command, e.g. /start, /help."""
    return bool(m.text and m.text.startswith("/"))


# group=-1 so this runs *before* the normal handlers (group 0) below, and
# since it doesn't call stop_propagation(), every matching command still
# reaches its real handler afterwards as usual.
@app.on_message(filters.create(_is_command_message) & filters.private, group=-1)
async def react_to_any_command(client: Client, m: Message):
    try:
        await client.send_reaction(
            chat_id=m.chat.id,
            message_id=m.id,
            emoji=random.choice(REACTIONS),
        )
    except Exception as e:
        # Reactions can fail (e.g. emoji not supported in this chat/region,
        # or rate limits) — never let that break the actual command.
        logger.debug(f"send_reaction failed for {m.command}: {e}")


# ---------------------------------------------------------------------
# /start, /help, menu buttons
# ---------------------------------------------------------------------

# ---------------------------------------------------------------------
# Referral system — shown when a free user hits the daily download limit
# (which is also what happens the moment their premium expires, since
# they fall back to the same free-limit gating at that point).
# ---------------------------------------------------------------------

REFERRAL_PHOTO_URL = START_PHOTO_URL
# (referral-count threshold, days of premium granted when that threshold
# is first reached). Rewards stack: reaching 10 grants a *further* 1 day
# on top of the 1 already granted at 5, for 2 days total.
REFERRAL_REWARDS = [(5, 1), (10, 1)]
REFERRAL_GOAL = REFERRAL_REWARDS[-1][0]


async def get_bot_username(client: Client) -> str:
    cached = getattr(client, "_cached_username", None)
    if cached:
        return cached
    me = await client.get_me()
    client._cached_username = me.username
    return me.username or ""


def referral_link(bot_username: str, user_id: int) -> str:
    return f"https://t.me/{bot_username}?start=ref_{user_id}"


def referral_text(referral_count: int) -> str:
    return (
        "🎁 ʀᴇғᴇʀ & ᴇᴀʀɴ ᴘʀᴇᴍɪᴜᴍ\n\n"
        "⚡ ʏᴏᴜʀ ᴅᴀɪʟʏ ᴅᴏᴡɴʟᴏᴀᴅ ʟɪᴍɪᴛ ʜᴀs ʙᴇᴇɴ ʀᴇᴀᴄʜᴇᴅ\n"
        "ᴏʀ ʏᴏᴜʀ ᴘʀᴇᴍɪᴜᴍ ᴘʟᴀɴ ʜᴀs ᴇxᴘɪʀᴇᴅ.\n\n"
        "💎 ᴡᴀɴᴛ ᴛᴏ ɢᴇᴛ ᴘʀᴇᴍɪᴜᴍ ғᴏʀ ғʀᴇᴇ?\n\n"
        "👥 ɪɴᴠɪᴛᴇ ғʀɪᴇɴᴅs ᴜsɪɴɢ ʏᴏᴜʀ ᴜɴɪǫᴜᴇ ʀᴇғᴇʀʀᴀʟ ʟɪɴᴋ.\n\n"
        "🏆 ʀᴇᴡᴀʀᴅs\n\n"
        "🎯 5 ʀᴇғᴇʀʀᴀʟs → 💎 1 ᴅᴀʏ ᴘʀᴇᴍɪᴜᴍ\n"
        "🎯 10 ʀᴇғᴇʀʀᴀʟs → 💎 2 ᴅᴀʏs ᴘʀᴇᴍɪᴜᴍ\n\n"
        f"📊 ʏᴏᴜʀ ʀᴇғᴇʀʀᴀʟs: {referral_count}/{REFERRAL_GOAL}\n\n"
        "🔗 sʜᴀʀᴇ ʏᴏᴜʀ ʟɪɴᴋ & ᴇᴀʀɴ ᴘʀᴇᴍɪᴜᴍ!"
    )


def referral_keyboard(bot_username: str, user_id: int, referral_count: int) -> InlineKeyboardMarkup:
    link = referral_link(bot_username, user_id)
    share_text = quote("🎬 Download Diskwala videos fast via Telegram — try this bot!")
    share_url = f"https://t.me/share/url?url={quote(link, safe='')}&text={share_text}"
    return InlineKeyboardMarkup([
        [make_button(SC("🔗 Get Referral Link"), callback_data="ref_getlink", style=BTN_PRIMARY)],
        [make_button(SC("📤 Share Referral Link"), url=share_url, style=BTN_PRIMARY)],
        [make_button(SC(f"👥 Referrals: {referral_count}"), callback_data="ref_count", style=BTN_PRIMARY)],
        [make_button(SC("💎 Premium Rewards"), callback_data="ref_rewards", style=BTN_PRIMARY)],
    ])


async def send_referral_prompt(client: Client, chat_id: int):
    """Shown in place of a plain 'limit reached' message — offers the
    referral photo/text/buttons instead so a blocked user has an
    immediate free path back to downloading."""
    bot_username = await get_bot_username(client)
    referral_count = await get_referral_count(chat_id)
    keyboard = referral_keyboard(bot_username, chat_id, referral_count)
    try:
        await client.send_photo(
            chat_id, REFERRAL_PHOTO_URL,
            caption=SC(referral_text(referral_count)),
            reply_markup=keyboard,
            parse_mode=ParseMode.HTML,
        )
    except Exception as e:
        logger.warning(f"referral photo failed, falling back to text: {e}")
        await client.send_message(
            chat_id, SC(referral_text(referral_count)),
            reply_markup=keyboard, parse_mode=ParseMode.HTML,
        )


@app.on_message(filters.command("referral") & filters.private)
async def referral_cmd(client: Client, m: Message):
    await send_referral_prompt(client, m.from_user.id)


@app.on_callback_query(filters.regex(r"^ref_getlink$"))
async def ref_getlink_cb(client: Client, query):
    bot_username = await get_bot_username(client)
    link = referral_link(bot_username, query.from_user.id)
    await query.answer()
    await client.send_message(
        query.from_user.id,
        SC(f"🔗 <b>Your referral link:</b>\n\n<code>{link}</code>\n\n"
           "sʜᴀʀᴇ ᴛʜɪs ᴡɪᴛʜ ꜰʀɪᴇɴᴅs — ᴡʜᴇɴ ᴛʜᴇʏ ꜱᴛᴀʀᴛ ᴛʜᴇ ʙᴏᴛ ᴜsɪɴɢ ɪᴛ, ʏᴏᴜ ɢᴇᴛ ᴄʀᴇᴅɪᴛ."),
        parse_mode=ParseMode.HTML,
    )


@app.on_callback_query(filters.regex(r"^ref_count$"))
async def ref_count_cb(client: Client, query):
    count = await get_referral_count(query.from_user.id)
    await query.answer(f"👥 Your referrals: {count}/{REFERRAL_GOAL}", show_alert=True)


@app.on_callback_query(filters.regex(r"^ref_rewards$"))
async def ref_rewards_cb(client: Client, query):
    await query.answer(
        "🎯 5 referrals → 1 day premium\n🎯 10 referrals → 2 days premium",
        show_alert=True,
    )


@app.on_message(filters.command("start") & filters.private)
async def start_handler(client: Client, m: Message):
    display_name = smallcaps(m.from_user.first_name or "there")
    powered_link = f'<a href="{POWERED_BY_URL}">{smallcaps(POWERED_BY)}</a>'
    me = await client.get_me()
    bot_username = me.username or ""
    bot_name = me.first_name or "Diskwala Bot"
    start_txt = (
        f"<b>👋 Hello {display_name},</b>\n"
        f"<b>🤖 I am <a href=https://t.me/{bot_username}>{bot_name}</a></b>\n\n"
    )
    caption = (
        f"{start_txt}"
        "⚡ ɪ'ᴍ ᴀ ᴠᴇʀʏ ᴘᴏᴡᴇʀꜰᴜʟ ᴅɪꜱᴋᴡᴀʟᴀ ᴅᴏᴡɴʟᴏᴀᴅᴇʀ ʙᴏᴛ.\n\n"
        "📥 ꜱɪᴍᴘʟʏ ꜱᴇɴᴅ ᴍᴇ ᴀɴʏ ᴅɪꜱᴋᴡᴀʟᴀ ᴜʀʟ, ᴀɴᴅ ɪ'ʟʟ ꜰᴇᴛᴄʜ ᴛʜᴇ ᴅɪʀᴇᴄᴛ ᴠɪᴅᴇᴏ ꜰᴏʀ ʏᴏᴜ ɪɴ ꜱᴇᴄᴏɴᴅꜱ.\n\n"
        "🚀 ᴜʟᴛʀᴀ-ꜰᴀꜱᴛ ᴘʀᴏᴄᴇꜱꜱɪɴɢ\n"
        "🎬 ɪɴꜱᴛᴀɴᴛ ᴠɪᴅᴇᴏ ᴇxᴛʀᴀᴄᴛɪᴏɴ\n"
        "⚡ ʟɪɢʜᴛɴɪɴɢ-ꜱᴘᴇᴇᴅ ᴅᴏᴡɴʟᴏᴀᴅꜱ\n"
        "🛡️ ʀᴇʟɪᴀʙʟᴇ & ꜱᴛᴀʙʟᴇ ꜱᴇʀᴠɪᴄᴇ\n"
        "💎 ᴘʀᴇᴍɪᴜᴍ ᴘʟᴀɴꜱ ᴀᴠᴀɪʟᴀʙʟᴇ\n"
        "🔗 ᴊᴜꜱᴛ ᴘᴀꜱᴛᴇ ʏᴏᴜʀ ᴅɪꜱᴋᴡᴀʟᴀ ʟɪɴᴋ ʙᴇʟᴏᴡ ᴀɴᴅ ʟᴇᴛ ᴛʜᴇ ᴍᴀɢɪᴄ ʙᴇɢɪɴ!\n\n"
        "✅ ʏᴇ ʟɪɴᴋꜱ ꜱᴜᴘᴘᴏʀᴛᴇᴅ ʜᴀɪ:\n"
        "• <code>https://www.diskwala.com/app/...</code>\n"
        "• <code>https://diskwala.com/app/...</code>\n"
        "• <code>https://thediskwala.com/app/...</code>\n"
        "  ᴡᴀᴀʟᴇ ʟɪɴᴋꜱ\n\n"
        "━━━━━━━━━━━━━━━ \n"
        f"👑 ᴘᴏᴡᴇʀᴇᴅ ʙʏ {powered_link}\n"
        "⚡ ꜱᴘᴇᴇᴅ • ᴘᴇʀꜰᴏʀᴍᴀɴᴄᴇ • ʀᴇʟɪᴀʙɪʟɪᴛʏ\n"
        "━━━━━━━━━━━━━━━"
    )
    try:
        await m.reply_photo(START_PHOTO_URL, caption=SC(caption), reply_markup=fallback_keyboard(), parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.warning(f"start photo failed, falling back to text: {e}")
        await m.reply(SC(caption), reply_markup=fallback_keyboard(), parse_mode=ParseMode.HTML)

    await m.reply(SC(FALLBACK_TEXT), reply_markup=MAIN_MENU_KB)

    is_new = await register_user_if_new(m.from_user.id)
    if is_new:
        uname = f"@{m.from_user.username}" if m.from_user.username else "(no username)"
        await log_event(
            client,
            "🆕 <b>New User</b>\n\n"
            f"👤 Name: {m.from_user.first_name}\n"
            f"🔗 Username: {uname}\n"
            f"🆔 ID: <code>{m.from_user.id}</code>",
        )

        referrer_id = None
        if len(m.command) > 1 and m.command[1].startswith("ref_"):
            try:
                referrer_id = int(m.command[1][4:])
            except ValueError:
                referrer_id = None

        if referrer_id and referrer_id != m.from_user.id:
            credited = await set_referrer(m.from_user.id, referrer_id)
            if credited:
                new_count = await increment_referral_count(referrer_id)
                try:
                    await client.send_message(
                        referrer_id,
                        SC(f"🎁 sᴏᴍᴇᴏɴᴇ ᴊᴏɪɴᴇᴅ ᴜsɪɴɢ ʏᴏᴜʀ ʀᴇғᴇʀʀᴀʟ ʟɪɴᴋ!\n"
                           f"👥 ᴛᴏᴛᴀʟ ʀᴇғᴇʀʀᴀʟs: {new_count}/{REFERRAL_GOAL}"),
                        parse_mode=ParseMode.HTML,
                    )
                except Exception:
                    pass

                claimed = await get_referral_rewards_claimed(referrer_id)
                for threshold, days in REFERRAL_REWARDS:
                    if new_count >= threshold and threshold not in claimed:
                        await grant_referral_premium_days(referrer_id, days)
                        await mark_referral_reward_claimed(referrer_id, threshold)
                        try:
                            await client.send_message(
                                referrer_id,
                                SC(f"🎉 ʀᴇғᴇʀʀᴀʟ ʀᴇᴡᴀʀᴅ!\n\n"
                                   f"👥 ʏᴏᴜ ʀᴇᴀᴄʜᴇᴅ {threshold} ʀᴇғᴇʀʀᴀʟs — 💎 {days} ᴅᴀʏ(s) ᴘʀᴇᴍɪᴜᴍ ᴀᴅᴅᴇᴅ!"),
                                parse_mode=ParseMode.HTML,
                            )
                        except Exception:
                            pass


@app.on_message((filters.command("help") | menu_text_filter("❓ ʜᴇʟᴘ")) & filters.private)
async def help_handler(client: Client, m: Message):
    await m.reply(
        SC("ℹ️ <b>ʜᴏᴡ ᴛᴏ ᴜsᴇ</b>\n\n"
        "🔹 <b>ᴊᴜsᴛ sᴇɴᴅ ᴛʜᴇ ʟɪɴᴋ:</b>\n"
        "ᴘᴀsᴛᴇ ᴀɴʏ ᴅɪsᴋᴡᴀʟᴀ ᴜʀʟ ᴅɪʀᴇᴄᴛʟʏ ɪɴ ᴛʜᴇ ᴄʜᴀᴛ.\n\n"
        "🔹 <b>sᴜᴘᴘᴏʀᴛᴇᴅ ᴜʀʟ ꜰᴏʀᴍᴀᴛs:</b>\n"
        "<code>https://www.diskwala.com/app/...</code>\n"
        "<code>https://diskwala.com/app/...</code>\n"
        "<code>https://thediskwala.com/app/...</code>\n\n"
        "📌 <b>ᴇxᴀᴍᴘʟᴇ:</b>\n"
        "<code>https://www.diskwala.com/app/6a996d5006ba7ea03db225c2</code>\n\n"
        "💡 <b>ᴛɪᴘs:</b>\n"
        "• ꜰɪʟᴇs ᴜᴘ ᴛᴏ 2 ɢʙ ᴀʀᴇ ᴜᴘʟᴏᴀᴅᴇᴅ ᴅɪʀᴇᴄᴛʟʏ\n"
        "• ᴘʀᴇᴠɪᴏᴜsʟʏ ᴅᴏᴡɴʟᴏᴀᴅᴇᴅ ʟɪɴᴋs ᴀʀᴇ ᴄᴀᴄʜᴇᴅ — ɪɴsᴛᴀɴᴛ!\n"
        "• ɪꜰ ᴅᴏᴡɴʟᴏᴀᴅ ꜰᴀɪʟs, ᴜsᴇ ᴛʜᴇ ʀᴇᴛʀʏ ʙᴜᴛᴛᴏɴ\n"
        "• ᴜsᴇ <code>/cancel</code> ᴛᴏ sᴛᴏᴘ ᴀɴ ᴀᴄᴛɪᴠᴇ ᴅᴏᴡɴʟᴏᴀᴅ\n\n"
        "ʜᴀᴠɪɴɢ ᴛʀᴏᴜʙʟᴇ? ᴍᴀᴋᴇ sᴜʀᴇ ʏᴏᴜ'ʀᴇ sᴇɴᴅɪɴɢ ᴀ ᴠᴀʟɪᴅ ᴅɪsᴋᴡᴀʟᴀ ʟɪɴᴋ."),
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
    )


ABOUT_TEXT = (
    f"💠 {smallcaps('About This Bot')} 💠\n\n"
    f"╭────[ ✨ {smallcaps('Anuj')} ]────⍟\n"
    f"├⍟ 🚀 {smallcaps('Bot Name')}  : <a href=\"https://t.me/diskwala_downloaderr_bot\">{smallcaps('Diskwala Downloader Bot')}</a>\n"
    f"├⍟ 👨‍💻 {smallcaps('Developer')}  : <a href=\"https://t.me/anujedits97\">{smallcaps('Anuj Kumar')}</a>\n"
    f"├⍟ 🔗 {smallcaps('Library')}  : <a href=\"https://docs.pyrogram.org/\">{smallcaps('Pyrogram Async')}</a>\n"
    f"├⍟ ⚡️ {smallcaps('Language')}  : <a href=\"https://www.python.org/\">{smallcaps('Python')} 3.11+</a>\n"
    f"├⍟ ⚙️ {smallcaps('Database')}  : <a href=\"https://www.mongodb.com/\">{smallcaps('MongoDB')}</a>\n"
    f"├⍟ ⭐️ {smallcaps('Hosting')}  :  {smallcaps('Dedicated High-Speed VPS')}\n"
    "╰───────────────⍟"
)


def about_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[make_button(SC("❌ Close"), callback_data="about_close", style=BTN_DANGER)]])


@app.on_message(filters.command("about") & filters.private)
async def about_cmd(client: Client, m: Message):
    await m.reply(SC(ABOUT_TEXT), reply_markup=about_keyboard(), parse_mode=ParseMode.HTML)


@app.on_callback_query(filters.regex(r"^about_close$"))
async def about_close_cb(client: Client, query):
    try:
        await query.message.delete()
    except Exception:
        pass
    await query.answer()


@app.on_message(filters.command("cancel") & filters.private)
async def cancel_cmd(client: Client, m: Message):
    task = ACTIVE_TASKS.get(m.from_user.id)
    if task and not task.done():
        task.cancel()
        await m.reply(SC("<b>🛑 Cancelling your active download...</b>"), parse_mode=ParseMode.HTML)
    else:
        await m.reply(SC("<b>⚠️ No active download to cancel.</b>"), parse_mode=ParseMode.HTML)


FALLBACK_TEXT = "👇 Apna Diskwala link bhejo boss!"

NOT_A_LINK_TEXT = (
    "🤨 <b>Bhai ye kaunsa link hai? Diskwala ka toh nahi lagta!</b>\n\n"
    "Agar lagta hai Diskwala ka hai aur error aa rha, toh screenshot ke saath "
    "idhar report kro 👉 <a href=\"https://t.me/anujedits97\">Anuj Kumar</a>\n\n"
    "📌 <b>Example:</b>\n"
    "<code>https://www.diskwala.com/app/6a996d5006ba7ea03db225c2</code>"
)


def fallback_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [make_button(SC("📥 Download"), callback_data="fallback_download", style=BTN_PRIMARY),
         make_button(SC("📊 Status"), callback_data="fallback_status", style=BTN_PRIMARY)],
    ])


@app.on_callback_query(filters.regex(r"^fallback_download$"))
async def fallback_download_cb(client: Client, query):
    await query.answer(SC("👇 Apna Diskwala link bhejo boss!"), show_alert=True)


@app.on_callback_query(filters.regex(r"^fallback_status$"))
async def fallback_status_cb(client: Client, query):
    text, kb = await build_status_text(query.from_user.id)
    await query.message.reply(text, reply_markup=kb, parse_mode=ParseMode.HTML)
    await query.answer()


@app.on_message(filters.private & filters.text & menu_text_filter("☎️ sᴜᴘᴘᴏʀᴛ"))
async def support_handler(client: Client, m: Message):
    await m.reply(
        SC("📞 <b>Support</b>\n\n"
        "Koi problem? Idhar baat karo:\n\n"
        f"👤 Admin: <a href=\"{POWERED_BY_URL}\">Anuj Kumar</a>\n\n"
        "⏰ 24 ghante ke andar reply, pakka!"),
        parse_mode=ParseMode.HTML,
        disable_web_page_preview=True,
    )


@app.on_message(filters.private & filters.text & menu_text_filter("💎 ᴘʟᴀɴs"))
async def plans_menu_handler(client: Client, m: Message):
    await send_plans_message(m)


@app.on_message(filters.command(["plans", "premium"]) & filters.private)
async def plans_cmd(client: Client, m: Message):
    await send_plans_message(m)


# ---------------------------------------------------------------------
# Custom caption
# ---------------------------------------------------------------------

@app.on_message(filters.command("set_caption") & filters.private)
async def set_caption_cmd(client: Client, m: Message):
    if len(m.command) < 2:
        return await m.reply(
            SC("⚠️ <b>Usage Error</b>\n\n"
            "Please provide the caption text after the command.\n\n"
            "<b>Correct Format:</b>\n"
            "<code>/set_caption Your Caption Here</code>\n\n"
            "<b>Supported Placeholders:</b>\n"
            "• <code>{filename}</code> : File name\n"
            "• <code>{size}</code> : File size\n"
            "• <code>{quality}</code> : Quality label\n"
            "• <code>{source}</code> : Source Diskwala link\n\n"
            "<i>Example:</i> <code>/set_caption File: {filename} | Size: {size}</code>"),
            parse_mode=ParseMode.HTML,
        )
    caption = m.text.split(" ", 1)[1].strip()
    await set_caption(m.from_user.id, caption)
    await m.reply(
        SC("✅ <b>Custom Caption Saved!</b>\n\n"
        f"<b>Preview:</b>\n<code>{caption}</code>\n\n"
        "<i>This caption will be applied to your future downloads.</i>"),
        parse_mode=ParseMode.HTML,
    )


@app.on_message(filters.command("see_caption") & filters.private)
async def see_caption_cmd(client: Client, m: Message):
    caption = await get_caption(m.from_user.id)
    if caption:
        await m.reply(
            SC("📝 <b>Your Custom Caption</b>\n\n"
            f"<code>{caption}</code>\n\n"
            "<i>To remove this, use /del_caption</i>"),
            parse_mode=ParseMode.HTML,
        )
    else:
        await m.reply(
            SC("❌ <b>No Caption Set</b>\n\n"
            "You are currently using the default bot caption.\n"
            "<i>Use /set_caption to customize it.</i>"),
            parse_mode=ParseMode.HTML,
        )


@app.on_message(filters.command("del_caption") & filters.private)
async def del_caption_cmd(client: Client, m: Message):
    caption = await get_caption(m.from_user.id)
    if not caption:
        return await m.reply(
            SC("⚠️ <b>No Caption Found</b>\n\nYou don't have a custom caption set."),
            parse_mode=ParseMode.HTML,
        )
    await del_caption(m.from_user.id)
    await m.reply(
        SC("🗑 <b>Custom Caption Removed</b>\n\n<i>Your uploads will now use the default bot caption.</i>"),
        parse_mode=ParseMode.HTML,
    )


# ---------------------------------------------------------------------
# Custom thumbnail
# ---------------------------------------------------------------------

@app.on_message(filters.command("set_thumb") & filters.private)
async def set_thumb_cmd(client: Client, m: Message):
    reply = m.reply_to_message
    if not reply or not reply.photo:
        return await m.reply(
            SC("🖼 <b>Set Custom Thumbnail</b>\n\n"
            "<i>Reply to any photo with /set_thumb to use it as your default thumbnail.</i>\n\n"
            "<b>Usage:</b> Reply to a photo → <code>/set_thumb</code>"),
            parse_mode=ParseMode.HTML,
        )
    file_id = reply.photo.file_id
    await set_thumbnail(m.from_user.id, file_id)
    await m.reply_photo(
        file_id,
        caption=(
            SC("✅ <b>Custom Thumbnail Set Successfully!</b>\n\n"
            "<i>This thumbnail will be used for all your future uploads.</i>\n"
            "<i>Use /view_thumb to preview • /del_thumb to remove</i>")
        ),
        parse_mode=ParseMode.HTML,
    )


@app.on_message(filters.command(["view_thumb", "see_thumb"]) & filters.private)
async def view_thumb_cmd(client: Client, m: Message):
    thumb_id = await get_thumbnail(m.from_user.id)
    if not thumb_id:
        return await m.reply(
            SC("❌ <b>No Custom Thumbnail Found</b>\n\n"
            "<i>Reply to a photo with /set_thumb to add one.</i>"),
            parse_mode=ParseMode.HTML,
        )
    try:
        await m.reply_photo(
            thumb_id,
            caption=(
                SC("🖼 <b>Your Current Custom Thumbnail</b>\n\n"
                "<i>This is applied to all uploads.</i>\n"
                "<i>To delete, use /del_thumb</i>")
            ),
            parse_mode=ParseMode.HTML,
        )
    except Exception as e:
        await m.reply(SC(f"❌ Error loading thumbnail: {e}\nPlease set a new one."))


@app.on_message(filters.command(["del_thumb", "delete_thumb"]) & filters.private)
async def del_thumb_cmd(client: Client, m: Message):
    thumb_id = await get_thumbnail(m.from_user.id)
    if not thumb_id:
        return await m.reply(SC("ℹ️ You don't have a custom thumbnail set."))
    await del_thumbnail(m.from_user.id)
    await m.reply(
        SC("🗑 <b>Custom Thumbnail Deleted</b>\n\n"
        "<i>Your uploads will now use the default video thumbnail.</i>"),
        parse_mode=ParseMode.HTML,
    )


@app.on_message(filters.command("thumb_mode") & filters.private)
async def thumb_mode_cmd(client: Client, m: Message):
    thumb_id = await get_thumbnail(m.from_user.id)
    if thumb_id:
        status, extra = "🟢 Custom Thumbnail Active", "<i>Use /view_thumb to preview</i>"
    else:
        status, extra = "🔴 No Custom Thumbnail", "<i>Use /set_thumb (reply to photo) to enable</i>"
    await m.reply(SC(f"🖼 <b>Thumbnail Status</b>\n\n{status}\n{extra}"), parse_mode=ParseMode.HTML)


# ---------------------------------------------------------------------
# Per-user dump chat
# ---------------------------------------------------------------------

@app.on_message(filters.command("setchat") & filters.private)
async def setchat_cmd(client: Client, m: Message):
    if len(m.command) < 2:
        return await m.reply(
            SC("🗑 <b>Set Dump Chat</b>\n\n"
            "<b>Usage:</b>\n"
            "<code>/setchat &lt;chat_id&gt;</code> → every file you download also gets copied here\n"
            "<code>/setchat clear</code> → remove it\n\n"
            "<i>Example: /setchat -1001234567890</i>\n"
            "ℹ️ I must already be an admin in that channel/group."),
            parse_mode=ParseMode.HTML,
        )
    arg = m.command[1].strip().lower()
    if arg == "clear":
        await set_dump_chat(m.from_user.id, None)
        return await m.reply(SC("✅ <b>Dump Chat Cleared.</b>"), parse_mode=ParseMode.HTML)
    try:
        chat_id = int(m.command[1].strip())
    except ValueError:
        return await m.reply(
            SC("❌ <b>Invalid Chat ID</b>\n\n<i>Must be a number (e.g., -1001234567890)</i>"),
            parse_mode=ParseMode.HTML,
        )
    try:
        chat = await client.get_chat(chat_id)
        chat_title = chat.title or "Private Chat"
    except Exception as e:
        return await m.reply(SC(f"❌ <b>Unable to Access Chat</b>\n<i>{e}</i>"), parse_mode=ParseMode.HTML)
    await set_dump_chat(m.from_user.id, chat_id)
    await m.reply(
        SC(f"✅ <b>Dump Chat Set Successfully</b>\n\n"
        f"<b>Forward To:</b> <code>{chat_id}</code>\n"
        f"<b>Title:</b> {chat_title}"),
        parse_mode=ParseMode.HTML,
    )


@app.on_message(filters.command("set_dump") & filters.private & filters.user(ADMINS))
async def admin_set_dump_cmd(client: Client, m: Message):
    if len(m.command) < 3:
        return await m.reply(
            SC("⚠️ <b>Usage:</b> <code>/set_dump &lt;user_id&gt; &lt;chat_id&gt;</code>\n"
            "<code>/set_dump &lt;user_id&gt; clear</code> → remove it"),
            parse_mode=ParseMode.HTML,
        )
    try:
        target_id = int(m.command[1])
    except ValueError:
        return await m.reply(SC("⚠️ user_id must be a number."))

    arg = m.command[2].strip().lower()
    if arg == "clear":
        await set_dump_chat(target_id, None)
        return await m.reply(SC(f"✅ Dump chat cleared for <code>{target_id}</code>."), parse_mode=ParseMode.HTML)

    try:
        chat_id = int(m.command[2].strip())
    except ValueError:
        return await m.reply(SC("⚠️ chat_id must be a number."))

    try:
        chat = await client.get_chat(chat_id)
        chat_title = chat.title or "Private Chat"
    except Exception as e:
        return await m.reply(SC(f"❌ <b>Unable to Access Chat</b>\n<i>{e}</i>"), parse_mode=ParseMode.HTML)

    await set_dump_chat(target_id, chat_id)
    await m.reply(
        SC(f"✅ Dump chat set for <code>{target_id}</code> → <code>{chat_id}</code> ({chat_title})"),
        parse_mode=ParseMode.HTML,
    )


# ---------------------------------------------------------------------
# /settings — one menu tying caption/thumbnail/dump-chat/stats together
# ---------------------------------------------------------------------

def settings_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [make_button(SC("📊 My Usage Stats"), callback_data="settings_stats", style=BTN_PRIMARY)],
        [make_button(SC("🗑 Dump Chat"), callback_data="settings_dump", style=BTN_PRIMARY)],
        [
            make_button(SC("🖼 Thumbnail"), callback_data="settings_thumb", style=BTN_PRIMARY),
            make_button(SC("📝 Caption"), callback_data="settings_caption", style=BTN_PRIMARY),
        ],
        [make_button(SC("⚡ Titanium Clone Mode"), callback_data="titanium_status", style=BTN_PRIMARY)],
        [make_button(SC("❌ Close"), callback_data="settings_close", style=BTN_DANGER)],
    ])


def settings_back_close_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [make_button(SC("⬅️ Back"), callback_data="settings_back", style=BTN_PRIMARY),
         make_button(SC("❌ Close"), callback_data="settings_close", style=BTN_DANGER)],
    ])


async def settings_text(user_id: int) -> str:
    premium = await get_effective_premium_status(user_id)
    badge = "💎 Premium Member" if premium["is_premium"] else "👤 Free User"
    return (
        "⚙️ <b>Settings Panel</b>\n"
        "━━━━━━━━━━━━━━━━━━\n"
        f"<b>Account:</b> {badge}\n"
        f"<b>User ID:</b> <code>{user_id}</code>\n\n"
        "<i>Select an option below to customize your experience.</i>"
    )


@app.on_message(filters.command("settings") & filters.private)
async def settings_cmd(client: Client, m: Message):
    try:
        await m.reply(SC(await settings_text(m.from_user.id)), reply_markup=settings_keyboard(), parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.warning(f"/settings command failed for user {m.from_user.id}: {e}")
        try:
            await m.reply(SC("⚠️ Couldn't load settings, try again."), parse_mode=ParseMode.HTML)
        except Exception:
            pass


@app.on_callback_query(filters.regex(r"^settings_(stats|dump|thumb|caption|back|close)$"))
async def settings_callbacks(client: Client, query):
    try:
        await _settings_callbacks_impl(client, query)
    except Exception as e:
        logger.warning(f"Settings sub-menu '{query.data}' failed for user {query.from_user.id}: {e}")
        try:
            await query.answer("⚠️ Something went wrong, try again.", show_alert=True)
        except Exception:
            pass


async def _settings_callbacks_impl(client: Client, query):
    data = query.data
    user_id = query.from_user.id

    if data == "settings_stats":
        text, _ = await build_status_text(user_id)
        await query.message.edit_text(text, reply_markup=settings_back_close_keyboard(), parse_mode=ParseMode.HTML)

    elif data == "settings_dump":
        current = await get_dump_chat(user_id)
        if current:
            try:
                chat = await client.get_chat(current)
                title = chat.title or "Private Chat"
            except Exception:
                title = "Unknown (Inaccessible)"
            text = (
                "🗑 <b>Current Dump Chat</b>\n\n"
                f"<b>Chat ID:</b> <code>{current}</code>\n"
                f"<b>Title:</b> {title}\n\n"
                "<i>All your delivered files are copied here.</i>\n"
                "<i>Use /setchat to change or clear.</i>"
            )
        else:
            text = (
                "🗑 <b>No Dump Chat Set</b>\n\n"
                "<i>Delivered files only appear in this chat.</i>\n"
                "<i>Use /setchat &lt;chat_id&gt; to enable forwarding.</i>"
            )
        await query.message.edit_text(text, reply_markup=settings_back_close_keyboard(), parse_mode=ParseMode.HTML)

    elif data == "settings_thumb":
        thumb_id = await get_thumbnail(user_id)
        if thumb_id:
            await query.message.reply_photo(
                thumb_id,
                caption=SC("🖼 <b>Your Current Custom Thumbnail</b>\n\n<i>Use /set_thumb (reply to photo) to update, /del_thumb to remove</i>"),
                parse_mode=ParseMode.HTML,
            )
            await query.answer(SC("Thumbnail preview sent below 👇"))
            return
        else:
            await query.message.edit_text(
                SC("🖼 <b>No Custom Thumbnail Set</b>\n\n"
                "<i>Reply to a photo with /set_thumb to add one.</i>"),
                reply_markup=settings_back_close_keyboard(),
                parse_mode=ParseMode.HTML,
            )

    elif data == "settings_caption":
        caption = await get_caption(user_id)
        if caption:
            text = (
                "📝 <b>Current Custom Caption</b>\n\n"
                f"<code>{caption}</code>\n\n"
                "<i>Placeholders: {filename}, {size}, {quality}, {source}</i>\n"
                "<i>/set_caption &lt;text&gt; to change • /del_caption to remove</i>"
            )
        else:
            text = (
                "📝 <b>No Custom Caption Set</b>\n\n"
                "<i>Use /set_caption &lt;text&gt; to set one.</i>"
            )
        await query.message.edit_text(text, reply_markup=settings_back_close_keyboard(), parse_mode=ParseMode.HTML)

    elif data == "settings_back":
        await query.message.edit_text(
            SC(await settings_text(user_id)), reply_markup=settings_keyboard(), parse_mode=ParseMode.HTML
        )

    elif data == "settings_close":
        try:
            await query.message.delete()
        except Exception:
            pass

    await query.answer()


@app.on_message(filters.private & filters.text & menu_text_filter("📊 ᴍʏ sᴛᴀᴛᴜs"))
async def status_menu_handler(client: Client, m: Message):
    await show_my_status(client, m)


@app.on_message(filters.command("myplan") & filters.private)
async def myplan_cmd(client: Client, m: Message):
    await show_my_status(client, m)


async def build_status_text(chat_id: int):
    """Returns (text, keyboard_or_None) describing chat_id's plan/usage."""
    premium = await get_effective_premium_status(chat_id)
    if premium["lifetime"]:
        type_line = "💎 Premium (Lifetime ♾️)"
    elif premium["is_premium"]:
        days_left = (premium["expires_at"] - datetime.utcnow()).days + 1
        type_line = f"💎 Premium ({days_left} day{'s' if days_left != 1 else ''} left)"
    else:
        type_line = "Free"

    total_downloads = await get_user_total_downloads(chat_id)

    text = (
        "<b>📊 Your Status</b>\n\n"
        f"User ID: <code>{chat_id}</code>\n"
        f"Plan: <code>{type_line}</code>\n"
        f"Total Downloads: <code>{total_downloads}</code>\n"
    )
    if not premium["is_premium"]:
        used_today = await get_daily_count(chat_id)
        remaining = max(0, DAILY_FREE_LIMIT - used_today)
        text += f"Today's downloads: {used_today}/{DAILY_FREE_LIMIT} ({remaining} left)\n\n"
        text += "💎 Premium lo — unlimited downloads ka maza lo!"
        return text, status_keyboard()
    return text, status_keyboard()


async def show_my_status(client: Client, m: Message):
    text, kb = await build_status_text(m.from_user.id)
    await m.reply(text, reply_markup=kb, parse_mode=ParseMode.HTML)


@app.on_callback_query(filters.regex(r"^show_plans$"))
async def show_plans_cb(client: Client, query):
    await send_plans_message(query.message)
    await query.answer()


PLAN_LABELS = dict(PLANS)


def payment_keyboard(price: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [make_button(SC("✅ I've Paid"), callback_data=f"paid_{price}", style=BTN_PRIMARY)],
        [make_button(SC("📸 Send Payment Proof"), url=POWERED_BY_URL, style=BTN_PRIMARY)],
        [make_button(SC("⬅️ Back"), callback_data="plans_back", style=BTN_DANGER)],
    ])


@app.on_callback_query(filters.regex(r"^plan_\d+$"))
async def plan_selected_cb(client: Client, query):
    price = int(query.data.split("_", 1)[1])
    label = PLAN_LABELS.get(price, "")
    tag = " ♾️" if price == 999 else ""
    text = (
        f"💳 <b>{label}{tag}</b> ke liye Payment\n\n"
        f"Amount: ₹{price}\n\n"
        "📱 QR scan karo kisi bhi UPI app se:\n"
        "• PhonePe\n"
        "• GPay\n"
        "• Paytm\n"
        "• Koi bhi UPI app\n\n"
        "⏰ Time limit: 15 minutes\n\n"
        "Payment ke baad 'I've Paid' dabao — verify instant! ⚡"
    )
    try:
        await query.message.reply_photo(
            PLANS_PHOTO_URL,
            caption=text,
            reply_markup=payment_keyboard(price),
            parse_mode=ParseMode.HTML,
        )
    except Exception as e:
        logger.warning(f"payment photo failed, falling back to text: {e}")
        await query.message.reply(text, reply_markup=payment_keyboard(price), parse_mode=ParseMode.HTML)
    await query.answer()


@app.on_callback_query(filters.regex(r"^paid_\d+$"))
async def paid_cb(client: Client, query):
    price = int(query.data.split("_", 1)[1])
    label = PLAN_LABELS.get(price, "")
    user = query.from_user
    display_name = f"@{user.username}" if user.username else (user.first_name or str(user.id))
    uname = f'<a href="tg://user?id={user.id}"><code>{html.escape(display_name)}</code></a>'

    for admin_id in ADMINS:
        try:
            await client.send_message(
                admin_id,
                SC("🔔 <b>Payment Claim</b>\n\n"
                f"User: {uname} (<code>{user.id}</code>)\n"
                f"Plan: ₹{price} - {label}\n\n"
                f"Verify the screenshot, then run:\n<code>/addpremium {user.id} &lt;days&gt;</code>"),
                parse_mode=ParseMode.HTML,
            )
        except Exception as e:
            logger.warning(f"failed to notify admin {admin_id}: {e}")

    await query.answer(SC("✅ Admin ko notify kar diya!"), show_alert=True)
    await query.message.reply(
        SC("✅ Aapka payment claim admin ko bhej diya gaya hai.\n"
        f"Jaldi verification ke liye screenshot bhi bhej do: {POWERED_BY_URL}"),
    )


# ---------------------------------------------------------------------
# Admin-only premium/ban management
# ---------------------------------------------------------------------

@app.on_message(filters.command("addpremium") & filters.private & filters.user(ADMINS))
async def addpremium_cmd(client: Client, m: Message):
    args = m.command[1:]
    if len(args) < 2:
        return await m.reply(
            SC("⚠️ <b>Usage:</b> <code>/addpremium &lt;user_id&gt; &lt;days|lifetime&gt;</code>"),
            parse_mode=ParseMode.HTML,
        )
    try:
        target_id = int(args[0])
    except ValueError:
        return await m.reply(SC("⚠️ user_id must be a number."))

    if args[1].lower() == "lifetime":
        await set_premium(target_id, None)
        note = "Lifetime ♾️"
    else:
        try:
            days = int(args[1])
        except ValueError:
            return await m.reply(SC("⚠️ days must be a number, or 'lifetime'."))
        if days < 1:
            return await m.reply(SC("⚠️ days must be at least 1."))
        await set_premium(target_id, days)
        note = f"{days} day{'s' if days != 1 else ''}"

    await m.reply(SC(f"✅ Premium granted to <code>{target_id}</code> — {note}."), parse_mode=ParseMode.HTML)
    try:
        await client.send_message(target_id, SC(f"🎉 You've been given Premium ({note}) by the admin!"))
    except Exception as e:
        logger.warning(f"Couldn't notify {target_id} about premium grant: {e}")


@app.on_message(filters.command("removepremium") & filters.private & filters.user(ADMINS))
async def removepremium_cmd(client: Client, m: Message):
    args = m.command[1:]
    if len(args) < 1:
        return await m.reply(
            SC("⚠️ <b>Usage:</b> <code>/removepremium &lt;user_id&gt;</code>"),
            parse_mode=ParseMode.HTML,
        )
    try:
        target_id = int(args[0])
    except ValueError:
        return await m.reply(SC("⚠️ user_id must be a number."))

    await remove_premium(target_id)
    await m.reply(SC(f"✅ Premium removed for <code>{target_id}</code>."), parse_mode=ParseMode.HTML)


@app.on_message(filters.command("ban") & filters.private & filters.user(ADMINS))
async def ban_cmd(client: Client, m: Message):
    args = m.command[1:]
    if len(args) < 1:
        return await m.reply(SC("⚠️ <b>Usage:</b> <code>/ban &lt;user_id&gt;</code>"), parse_mode=ParseMode.HTML)
    try:
        target_id = int(args[0])
    except ValueError:
        return await m.reply(SC("⚠️ user_id must be a number."))
    if target_id in ADMINS:
        return await m.reply(SC("⚠️ Can't ban an admin."))

    await set_banned(target_id, True)
    await m.reply(SC(f"🚫 <code>{target_id}</code> has been banned."), parse_mode=ParseMode.HTML)
    try:
        await client.send_message(target_id, SC("🚫 You've been banned from using this bot."))
    except Exception as e:
        logger.warning(f"Couldn't notify {target_id} about ban: {e}")


@app.on_message(filters.command("unban") & filters.private & filters.user(ADMINS))
async def unban_cmd(client: Client, m: Message):
    args = m.command[1:]
    if len(args) < 1:
        return await m.reply(SC("⚠️ <b>Usage:</b> <code>/unban &lt;user_id&gt;</code>"), parse_mode=ParseMode.HTML)
    try:
        target_id = int(args[0])
    except ValueError:
        return await m.reply(SC("⚠️ user_id must be a number."))

    await set_banned(target_id, False)
    await m.reply(SC(f"✅ <code>{target_id}</code> has been unbanned."), parse_mode=ParseMode.HTML)
    try:
        await client.send_message(target_id, SC("✅ You've been unbanned — you can use the bot again."))
    except Exception as e:
        logger.warning(f"Couldn't notify {target_id} about unban: {e}")


@app.on_message(filters.command("stats") & filters.private & filters.user(ADMINS))
async def stats_cmd(client: Client, m: Message):
    s = await get_stats_summary()
    if not s:
        return await m.reply(SC("❌ Couldn't fetch stats."))
    text = (
        "📊 <b>Bot Stats</b>\n\n"
        f"👥 <b>Total Users:</b> {s['total_users']}\n"
        f"💎 <b>Premium Users:</b> {s['premium_count']}\n"
        f"🚫 <b>Banned Users:</b> {s['banned_count']}\n\n"
        f"📦 <b>Total Downloads:</b> {s['total_downloads']}\n"
        f"🗂️ <b>Unique Files Cached:</b> {s['total_files_cached']}\n\n"
        "🚦 <b>Live Queues</b>\n"
        f"🆓 Free: {free_download_queue.active}/{free_download_queue.max_concurrent} active, "
        f"{len(free_download_queue.waiters)} waiting\n"
        f"💎 Premium: {premium_download_queue.active}/{premium_download_queue.max_concurrent} active, "
        f"{len(premium_download_queue.waiters)} waiting\n"
        f"🔁 In-flight unique links: {len(INFLIGHT_DOWNLOADS)}"
    )
    await m.reply(text, parse_mode=ParseMode.HTML)


async def _broadcast_one(client: Client, cid: int, broadcast_text, reply, from_chat_id: int):
    try:
        if broadcast_text is not None:
            await client.send_message(cid, broadcast_text)
        else:
            await client.copy_message(chat_id=cid, from_chat_id=from_chat_id, message_id=reply.id)
        return "success"
    except FloodWait as e:
        await asyncio.sleep(e.value)
        return await _broadcast_one(client, cid, broadcast_text, reply, from_chat_id)
    except (InputUserDeactivated, UserIsBlocked, PeerIdInvalid):
        await delete_user(cid)
        return "removed"
    except Exception as e:
        logger.warning(f"broadcast failed for {cid}: {e}")
        return "failed"


@app.on_message(filters.command("broadcast") & filters.private & filters.user(ADMINS))
async def broadcast_cmd(client: Client, m: Message):
    reply = m.reply_to_message
    broadcast_text = None
    if len(m.command) >= 2:
        broadcast_text = m.text.split(None, 1)[1]
    elif not reply:
        return await m.reply(
            SC("⚠️ <b>Usage:</b> <code>/broadcast &lt;message&gt;</code>\n"
            "(or reply to a message with just <code>/broadcast</code> to forward that)"),
            parse_mode=ParseMode.HTML,
        )

    chat_ids = await all_chat_ids()
    total = len(chat_ids)
    status_msg = await m.reply(SC(f"📣 Broadcasting to {total} users..."))

    done = success = removed = failed = 0
    for cid in chat_ids:
        result = await _broadcast_one(client, cid, broadcast_text, reply, m.chat.id)
        if result == "success":
            success += 1
        elif result == "removed":
            removed += 1
        else:
            failed += 1
        done += 1

        if done % 20 == 0 or done == total:
            try:
                await status_msg.edit_text(
                    SC("📣 <b>Broadcast in progress...</b>\n\n"
                    f"👥 Total: {total}\n"
                    f"💫 Done: {done}/{total}\n"
                    f"✅ Success: {success}\n"
                    f"🚫 Removed (blocked/deleted): {removed}\n"
                    f"❌ Failed: {failed}"),
                    parse_mode=ParseMode.HTML,
                )
            except Exception:
                pass
        await asyncio.sleep(0.05)

    await status_msg.edit_text(
        SC("📣 <b>Broadcast done.</b>\n\n"
        f"✅ Success: {success}\n"
        f"🚫 Removed (blocked/deleted): {removed}\n"
        f"❌ Failed: {failed}"),
        parse_mode=ParseMode.HTML,
    )


@app.on_message(filters.command("users") & filters.private & filters.user(ADMINS))
async def users_export_cmd(client: Client, m: Message):
    status = await m.reply(SC("⏳ Gathering user data..."))
    users = await get_all_users_full()

    tmp_path = f"/tmp/diskwala_users_{m.chat.id}.json"
    export = [
        {
            "id": u.get("_id"),
            "is_banned": u.get("is_banned", False),
            "is_premium": bool(u.get("premium_lifetime") or u.get("premium_until")),
            "first_seen": u.get("first_seen").isoformat() if u.get("first_seen") else None,
        }
        for u in users
    ]
    try:
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(export, f, indent=2, ensure_ascii=False)
        await status.edit_text(SC(f"👥 <b>Total Users:</b> {len(export)}"), parse_mode=ParseMode.HTML)
        await m.reply_document(tmp_path, caption=SC(f"📄 {len(export)} users exported."))
    except Exception as e:
        await status.edit_text(SC(f"⚠️ Error exporting users: {e}"))
    finally:
        try:
            os.remove(tmp_path)
        except Exception:
            pass


# ---------------------------------------------------------------------
# Admin-only dynamic backup-channel linking (no redeploy/env change needed)
# ---------------------------------------------------------------------

@app.on_message(filters.command("set_channel_id") & filters.private & filters.user(ADMINS))
async def set_channel_id_cmd(client: Client, m: Message):
    args = m.command[1:]
    if len(args) < 1:
        return await m.reply(
            SC("⚠️ <b>Usage:</b> <code>/set_channel_id -100xxxxxxxxxx</code>\n"
            "<b>Example:</b> <code>/set_channel_id -1001234567890</code>\n\n"
            "ℹ️ The ID must start with <code>-100</code>, and I must already be an admin "
            "in that channel/group. You can get a channel's ID by forwarding any message "
            "from it to @MissRose_bot.\n\n"
            "You can link more than one channel/group — just run this command again with "
            "a different ID. Use /channel_id to see everything linked, and /del_channel_id "
            "&lt;id&gt; to unlink one (or with no id to unlink all)."),
            parse_mode=ParseMode.HTML,
        )

    raw = args[0]
    if not raw.startswith("-100") or not raw.lstrip("-").isdigit():
        return await m.reply(
            SC("⚠️ The ID must start with <code>-100</code>, e.g. <code>-1001234567890</code>."),
            parse_mode=ParseMode.HTML,
        )

    channel_id = int(raw)
    try:
        chat = await client.get_chat(channel_id)
        member = await client.get_chat_member(channel_id, "me")
        if member.status not in (ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.OWNER):
            return await m.reply(SC("⚠️ I'm in that chat but I'm not an admin there. Please promote me first."))
    except Exception as e:
        return await m.reply(
            SC(f"⚠️ Couldn't verify that chat — make sure I've already been added there.\n<code>{str(e)[:300]}</code>"),
            parse_mode=ParseMode.HTML,
        )

    is_new = await add_channel(channel_id)
    title = getattr(chat, "title", None) or str(channel_id)
    note = "Linked" if is_new else "Already linked"
    await m.reply(SC(f"✅ {note}: <b>{title}</b> (<code>{channel_id}</code>)."), parse_mode=ParseMode.HTML)


@app.on_message(filters.command("channel_id") & filters.private & filters.user(ADMINS))
async def channel_id_cmd(client: Client, m: Message):
    dynamic_ids = await get_channels()
    static_ids = [c for c in BACKUP_CHANNEL_IDS if c not in dynamic_ids]
    if not dynamic_ids and not static_ids:
        return await m.reply(
            SC("❌ No backup channels linked yet.\n\n"
            "To link one: <code>/set_channel_id -100xxxxxxxxxx</code>"),
            parse_mode=ParseMode.HTML,
        )

    lines = []
    for cid in dynamic_ids:
        try:
            chat = await client.get_chat(cid)
            title = getattr(chat, "title", None) or str(cid)
        except Exception:
            title = "(unreachable)"
        lines.append(f"• <b>{title}</b> — <code>{cid}</code>")
    for cid in static_ids:
        lines.append(f"• <code>{cid}</code> — from config (not removable via /del_channel_id)")

    await m.reply(SC("🔗 <b>Linked Backup Channels</b>\n\n" + "\n".join(lines)), parse_mode=ParseMode.HTML)


@app.on_message(filters.command("del_channel_id") & filters.private & filters.user(ADMINS))
async def del_channel_id_cmd(client: Client, m: Message):
    args = m.command[1:]
    if not args:
        count = await remove_all_channels()
        return await m.reply(SC(f"🗑️ Unlinked all dynamically-added channels ({count} removed)."))

    try:
        channel_id = int(args[0])
    except ValueError:
        return await m.reply(
            SC("⚠️ Channel ID must be a number, e.g. <code>-1001234567890</code>."),
            parse_mode=ParseMode.HTML,
        )

    removed = await remove_channel(channel_id)
    if removed:
        await m.reply(SC(f"🗑️ Unlinked <code>{channel_id}</code>."), parse_mode=ParseMode.HTML)
    else:
        await m.reply(
            SC(f"⚠️ <code>{channel_id}</code> wasn't linked (or it's set via config, not removable here)."),
            parse_mode=ParseMode.HTML,
        )


@app.on_message(filters.command("refresh_flezen_cookie") & filters.private & filters.user(ADMINS))
async def refresh_flezen_cookie_cmd(client: Client, m: Message):
    """Auto-generate a fresh Flezen account + cookie via mail.tm.

    Creates a disposable email, registers on flezen.com, verifies it,
    completes onboarding, and sets FLEZEN_COOKIE for this session.
    The new cookie is also printed so you can save it to Render env vars."""
    status = await m.reply(SC(
        "⏳ <b>Generating fresh Flezen cookie...</b>\n\n"
        "Creating disposable inbox → Registering → Verifying email → Extracting cookie\n"
        "<i>(takes ~30-60 seconds)</i>"
    ), parse_mode=ParseMode.HTML)

    try:
        result = await asyncio.to_thread(generate_flezen_cookie)
    except Exception as e:
        await status.edit(SC(f"❌ <b>Cookie generation failed:</b>\n<code>{e}</code>"),
                          parse_mode=ParseMode.HTML)
        return

    if not result:
        await status.edit(SC(
            "❌ <b>Failed to generate Flezen cookie.</b>\n\n"
            "Possible reasons:\n"
            "• mail.tm is down or rate-limiting\n"
            "• Flezen blocked disposable email registration\n"
            "• Verification email not received\n\n"
            "Try again in a few minutes."
        ), parse_mode=ParseMode.HTML)
        return

    cookie_str, email = result

    # Set in current process environment so it takes effect immediately
    import os as _os
    _os.environ["FLEZEN_COOKIE"] = cookie_str

    # Also update the module-level variable in diskwala.py
    import diskwala as _dw
    _dw.FLEZEN_COOKIE = cookie_str

    await status.edit(SC(
        f"✅ <b>Flezen cookie refreshed!</b>\n\n"
        f"📧 <b>Account:</b> <code>{email}</code>\n\n"
        f"🍪 <b>Cookie:</b>\n<code>{cookie_str[:200]}{'...' if len(cookie_str) > 200 else ''}</code>\n\n"
        f"<b>⚠️ Save to Render:</b>\n"
        f"Dashboard → Environment → <code>FLEZEN_COOKIE</code> → paste full cookie\n"
        f"<i>(Current session already using new cookie — restart will reset unless saved)</i>"
    ), parse_mode=ParseMode.HTML)
# ---------------------------------------------------------------------

LINK_HANDLER_FILTER = (
    filters.private & ~filters.service
    & ~filters.command(["start", "help", "myplan", "plans", "premium", "addpremium",
                                          "removepremium", "ban", "unban", "stats", "broadcast", "users",
                                          "set_channel_id", "channel_id", "del_channel_id",
                                          "set_caption", "see_caption", "del_caption",
                                          "set_thumb", "view_thumb", "see_thumb",
                                          "del_thumb", "delete_thumb", "thumb_mode",
                                          "setchat", "settings", "set_dump", "cancel",
                                          "addbot", "delbot", "titanium"])
    & NOT_MENU_BUTTON_FILTER
)

# Matches a @BotFather-issued bot token (e.g. 8504787296:AAxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx).
# Telegram's "Managed Bots" auto-create flow (see titanium.py's /addbot ->
# Auto-Create button) delivers its own creation confirmation as a real
# message in this chat, separately from our own "Bot added successfully!"
# reply — and that confirmation isn't a command or a Diskwala link, so it
# used to fall through to the generic "not a link" reply below, making
# every auto-created clone show BOTH messages together. A token-shaped
# message is never something the user is asking us to download, so it's
# safe (and much less confusing) to just ignore it silently here instead.
_BOTFATHER_TOKEN_RE = re.compile(r"\d[0-9]{8,10}:[0-9A-Za-z_-]{35}")


@app.on_message(LINK_HANDLER_FILTER)
async def link_handler(client: Client, m: Message):
    if await is_banned(m.from_user.id):
        return await m.reply(SC("🚫 You are banned from using this bot."))

    text = m.text or m.caption or ""
    links = extract_diskwala_links(text)
    if not links:
        if text.startswith("/"):
            return await m.reply(
                SC("<b>❓ Unknown command.</b>\nType /help to see available commands."),
                parse_mode=ParseMode.HTML,
            )
        if _BOTFATHER_TOKEN_RE.search(text):
            return  # leftover bot-token/creation text, not a real query — ignore silently
        return await m.reply(NOT_A_LINK_TEXT, parse_mode=ParseMode.HTML)

    premium = await get_effective_premium_status(m.from_user.id)
    link_limit = PREMIUM_LINK_LIMIT if premium["is_premium"] else FREE_LINK_LIMIT
    if len(links) > link_limit:
        await m.reply(
            SC(f"<b>⚠️ Too many links at once ({len(links)}/{link_limit} max).</b>\n\n"
               f"Only the first {link_limit} will be processed.\n"
               + ("💎 Get Premium for up to "
                  f"{PREMIUM_LINK_LIMIT} links at once." if not premium["is_premium"] else "")),
            reply_markup=status_keyboard() if not premium["is_premium"] else None,
            parse_mode=ParseMode.HTML,
        )
        links = links[:link_limit]

    for i, link in enumerate(links):
        tag = f"[{i+1}/{len(links)}]" if len(links) > 1 else ""
        if is_playlist_link(link):
            await _process_playlist_link(client, m, link, tag)
        else:
            await process_link(client, m, link, tag)


async def process_link(client: Client, m: Message, link: str, tag: str):
    # ── Playlist link: fetch all files and process each one ──────────────
    if is_playlist_link(link):
        await _process_playlist_link(client, m, link, tag)
        return

    link_id = uuid.uuid4().hex[:10]
    LINK_CACHE[link_id] = link

    # Fetch video info — two parallel calls:
    # 1. resolve_diskwala_cached: gets downloadUrl, size (needs auth)
    # 2. fetch_diskwala_preview: scrapes HTML for title, author, duration, thumb (no auth)
    video_info = {}
    preview = {}
    try:
        video_info = await resolve_diskwala_cached(link)
    except Exception as e:
        logger.warning(f"Link-preview API fetch failed for {link}: {e}")

    try:
        preview = await asyncio.to_thread(fetch_diskwala_preview, link)
    except Exception as e:
        logger.warning(f"Link-preview HTML scrape failed for {link}: {e}")

    # Title + thumb + size → API (reliable, always set)
    raw_name   = video_info.get("name", "") or ""
    title      = raw_name.rsplit(".", 1)[0] if "." in raw_name else raw_name
    title      = title.replace("_", " ").replace("-", " ").strip()
    for ext in (".mp4", ".mkv", ".webm", ".mov", ".avi"):
        if title.lower().endswith(ext):
            title = title[:-len(ext)].strip()

    thumb_url  = video_info.get("thumb")
    size_bytes = video_info.get("size", 0) or 0

    # Author / duration / views / likes / description / upload_date / category
    # Priority: video_info (API) → preview (HTML scrape / temp_info)
    # video_info now includes these fields from the decrypted API response
    # (extracted in diskwala._fetch_diskwala_video_via_api) — preview is
    # kept as fallback for any field the API response didn't include.
    author      = video_info.get("creator")      or preview.get("author")      or ""
    dur_secs    = video_info.get("duration_seconds") or preview.get("duration_seconds") or 0
    views       = video_info.get("views")        or preview.get("views")
    likes       = video_info.get("likes")        or preview.get("likes")
    description = video_info.get("description")  or preview.get("description")
    upload_date = video_info.get("upload_date")  or preview.get("upload_date")
    # category resolved in caption block below (with extension fallback)
    # Thumb + size: API is primary but fall back to scrape if API returned None
    if not thumb_url:
        thumb_url  = preview.get("thumb")
    if not size_bytes:
        size_bytes = preview.get("size") or 0

    # Extension — from API field or from filename
    extension   = video_info.get("extension") or (raw_name.rsplit(".", 1)[-1].lower() if "." in raw_name else None)
    # Category — from API / scrape, already inferred in diskwala.py if extension known
    category    = video_info.get("category") or preview.get("category")
    if not category and extension:
        _V = {"mp4","mkv","avi","mov","webm","flv","wmv","m4v","ts","3gp","m2ts","vob"}
        _A = {"mp3","flac","aac","ogg","m4a","wav","opus","wma","alac","aiff"}
        _I = {"jpg","jpeg","png","gif","webp","bmp","tiff","tif","svg","heic","heif","avif"}
        _D = {"pdf","doc","docx","ppt","pptx","xls","xlsx","txt","zip","rar","7z","tar","gz"}
        category = ("Video" if extension in _V else
                    "Audio" if extension in _A else
                    "Photo" if extension in _I else
                    "Document" if extension in _D else None)

    # Build caption lines
    title_line  = f"🎬 {SC(title)}\n\n"           if title    else ""
    author_line = f"👤 {SC('By')}: {SC(author)}\n" if author   else ""
    dur_line    = f"⏱ {SC('Duration')}: {hms(dur_secs)}\n" if dur_secs else ""
    size_line   = f"📦 {SC('Size')}: {human_size(size_bytes)}\n" if size_bytes else ""
    ext_line    = f"📎 {SC('Extension')}: .{extension.upper()}\n" if extension else ""
    views_line  = f"👁 {SC('Views')}: {views:,}\n" if isinstance(views, (int, float)) else (f"👁 {SC('Views')}: {views}\n" if views else "")
    likes_line  = f"👍 {SC('Likes')}: {likes:,}\n" if isinstance(likes, (int, float)) else (f"👍 {SC('Likes')}: {likes}\n" if likes else "")
    date_line   = f"📅 {SC('Uploaded')}: {upload_date}\n" if upload_date else ""
    category_line = f"🏷 {SC('Category')}: {SC(str(category))}\n" if category else ""
    desc_line   = f"📝 {SC(str(description)[:200])}\n" if description else ""

    preview_caption = (
        f"{title_line}"
        f"{author_line}"
        f"{dur_line}"
        f"{size_line}"
        f"{ext_line}"
        f"{views_line}"
        f"{likes_line}"
        f"{date_line}"
        f"{category_line}"
        f"{desc_line}"
        f"\n{SC('Available Qualities')}:\n"
        f"✅ {SC('Auto (Best)')}\n"
        f"\n{SC('Tap a quality below to download')}:"
    )

    try:
        if thumb_url:
            await m.reply_photo(
                thumb_url,
                caption=SC(preview_caption),
                parse_mode=ParseMode.HTML,
                reply_markup=link_menu_markup(link_id),
            )
        else:
            raise ValueError("no thumb")
    except Exception:
        # Fallback: plain text if photo fails
        await m.reply(
            SC(f"<b>🔗 {tag}</b>\n\n{preview_caption}"),
            reply_markup=link_menu_markup(link_id),
            parse_mode=ParseMode.HTML,
        )

async def _process_playlist_link(client: Client, m: Message, playlist_url: str, tag: str):
    """Handle a diskwala.com/playlist/... URL — fetch all file links and
    queue each one as an individual download."""
    status = await m.reply(
        SC(f"<b>📂 Fetching playlist{' ' + tag if tag else ''}...</b>\n<code>{playlist_url}</code>"),
        parse_mode=ParseMode.HTML,
    )
    try:
        auth = await get_auth_token()
        playlist = await asyncio.to_thread(fetch_playlist_info, playlist_url, auth)
    except DiskwalaAuthError:
        auth = await get_auth_token()
        try:
            playlist = await asyncio.to_thread(fetch_playlist_info, playlist_url, auth)
        except Exception as e:
            await status.edit_text(SC(f"<b>❌ Playlist fetch failed:</b> {e}"), parse_mode=ParseMode.HTML)
            return
    except Exception as e:
        await status.edit_text(SC(f"<b>❌ Playlist fetch failed:</b> {e}"), parse_mode=ParseMode.HTML)
        return

    files = playlist.get("files") or []
    title = playlist.get("title") or "Playlist"
    if not files:
        await status.edit_text(SC(f"<b>❌ No files found in playlist.</b>"), parse_mode=ParseMode.HTML)
        return

    await status.edit_text(
        SC(f"<b>📂 {title}</b>\n"
           f"Found <b>{len(files)}</b> file(s) — queuing all for download..."),
        parse_mode=ParseMode.HTML,
    )

    # Process each file as an individual link
    for i, file_info in enumerate(files):
        file_link = file_info.get("link", "")
        if not file_link:
            continue
        file_tag = f"[{i+1}/{len(files)}]"
        try:
            await process_link(client, m, file_link, file_tag)
            await asyncio.sleep(0.5)  # small delay to avoid flooding
        except Exception as e:
            logger.warning(f"Playlist file {i+1} failed: {e}")
            await m.reply(
                SC(f"<b>❌ File {i+1}/{len(files)} failed:</b> {e}"),
                parse_mode=ParseMode.HTML,
            )


CALLBACK_HANDLER_FILTER = filters.regex(r"^(dlq|stream|cancel)\|")


@app.on_callback_query(CALLBACK_HANDLER_FILTER)
async def callback_handler(client: Client, query):
    parts = query.data.split("|")
    action = parts[0]
    link_id = parts[1]
    link = LINK_CACHE.get(link_id)

    if action == "cancel":
        await query.message.edit_text(SC("<b>Cancelled.</b>"), parse_mode=ParseMode.HTML)
        LINK_CACHE.pop(link_id, None)
        return

    if not link:
        await query.answer(SC("Link expired, please resend it."), show_alert=True)
        return

    if await is_banned(query.from_user.id):
        await query.answer(SC("You are banned from using this bot."), show_alert=True)
        return

    if action == "dlq":
        existing_task = ACTIVE_TASKS.get(query.from_user.id)
        if existing_task and not existing_task.done():
            # Guards against a fast double-tap on the Download button
            # spawning a second parallel task for the same link — without
            # this, the second tap's ACTIVE_TASKS[chat_id] write would
            # silently overwrite the first task's entry, breaking /cancel
            # for the original download and racing two uploads at once.
            await query.answer(
                SC("⏳ You already have a download in progress. Use /cancel to stop it first."),
                show_alert=True,
            )
            return
        await download_video(client, query, link)
    elif action == "stream":
        await send_stream_link(client, query, link)


# ---------------------------------------------------------------------
# The "/" command menu (BotFather-style) shown in Telegram's chat UI.
# Kept as one reusable constant so the main bot AND every Titanium clone
# set the exact same menu — see set_bot_commands_list() below and
# titanium.py's _get_clone_client(), which applies this same list to
# each clone right after it starts.
# ---------------------------------------------------------------------
BOT_COMMANDS_LIST = [
    BotCommand("start",           "🚀 Start the bot"),
    BotCommand("help",            "❓ How to use the bot"),
    BotCommand("about",           "ℹ️ About this bot"),
    BotCommand("cancel",          "🚫 Cancel current active download"),
    BotCommand("plans",           "💎 View premium plans"),
    BotCommand("premium",         "💎 View premium plans"),
    BotCommand("myplan",          "📋 Check your plan & usage"),
    BotCommand("referral",        "🎁 Get your referral link & rewards"),
    BotCommand("settings",        "⚙️ Open your settings menu"),
    BotCommand("addbot",          "⚡ Connect a Titanium clone bot"),
    BotCommand("delbot",          "⚡ Disconnect a Titanium clone bot"),
    BotCommand("titanium",        "⚡ Titanium Clone Mode panel"),
    BotCommand("set_caption",     "✏️ Set a custom caption"),
    BotCommand("see_caption",     "📄 View your custom caption"),
    BotCommand("del_caption",     "❌ Delete your custom caption"),
    BotCommand("set_thumb",       "🖼️ Set a custom thumbnail (reply to photo)"),
    BotCommand("view_thumb",      "👁️ View your custom thumbnail"),
    BotCommand("see_thumb",       "👁️ View your custom thumbnail"),
    BotCommand("del_thumb",       "🗑️ Delete your custom thumbnail"),
    BotCommand("delete_thumb",    "🗑️ Delete your custom thumbnail"),
    BotCommand("thumb_mode",      "🖼️ Check thumbnail status"),
    BotCommand("setchat",         "💬 Set/clear your personal dump chat"),
    BotCommand("set_dump",        "💬 Set global dump chat (admin only)"),
    BotCommand("set_channel_id",  "📡 Link a backup channel/group (admin)"),
    BotCommand("channel_id",      "📋 List linked backup channels (admin)"),
    BotCommand("del_channel_id",  "🗑 Unlink a backup channel (admin)"),
    BotCommand("addpremium",      "👑 Grant premium to a user (admin)"),
    BotCommand("removepremium",   "💔 Remove premium from a user (admin)"),
    BotCommand("ban",             "🔨 Ban a user (admin)"),
    BotCommand("unban",           "✅ Unban a user (admin)"),
    BotCommand("stats",           "📊 Bot-wide stats (admin)"),
    BotCommand("broadcast",       "📢 Broadcast a message to all users (admin)"),
    BotCommand("users",           "👥 Export all users as JSON (admin)"),
][:100]


# ---------------------------------------------------------------------
# Titanium Clone Mode — wire the main bot's own download handlers onto
# any clone bot a user connects. See titanium.py for the full picture.
#
# The clone reuses these exact handler functions (not copies) so its
# /start screen, menu buttons and inline buttons behave identically to
# the main bot's — same photo, same caption, same Plans/Status/Help/
# Support flow.
# ---------------------------------------------------------------------

TITANIUM_MENU_MESSAGE_HANDLERS = [
    (help_handler, filters.command("help") | menu_text_filter("❓ ʜᴇʟᴘ")),
    (plans_menu_handler, filters.text & menu_text_filter("💎 ᴘʟᴀɴs")),
    (plans_cmd, filters.command(["plans", "premium"])),
    (status_menu_handler, filters.text & menu_text_filter("📊 ᴍʏ sᴛᴀᴛᴜs")),
    (myplan_cmd, filters.command("myplan")),
    (support_handler, filters.text & menu_text_filter("☎️ sᴜᴘᴘᴏʀᴛ")),
]

TITANIUM_MENU_CALLBACK_HANDLERS = [
    (fallback_download_cb, filters.regex(r"^fallback_download$")),
    (fallback_status_cb, filters.regex(r"^fallback_status$")),
    (show_plans_cb, filters.regex(r"^show_plans$")),
    (plan_selected_cb, filters.regex(r"^plan_\d+$")),
    (paid_cb, filters.regex(r"^paid_\d+$")),
    (plans_back_cb, filters.regex(r"^plans_back$")),
]

titanium.register_titanium_handlers(
    app,
    make_button=make_button,
    BTN_PRIMARY=BTN_PRIMARY,
    BTN_DANGER=BTN_DANGER,
    SC=SC,
    ParseMode=ParseMode,
    link_handler=link_handler,
    callback_handler=callback_handler,
    link_filter=LINK_HANDLER_FILTER,
    callback_filter=CALLBACK_HANDLER_FILTER,
    cancel_handler=cancel_cmd,
    start_handler=start_handler,
    get_username=lambda: getattr(app, "_cached_username", None),
    smallcaps=smallcaps,
    start_photo_url=START_PHOTO_URL,
    fallback_keyboard=fallback_keyboard,
    fallback_text=FALLBACK_TEXT,
    main_menu_kb=MAIN_MENU_KB,
    powered_by=POWERED_BY,
    powered_by_url=POWERED_BY_URL,
    menu_message_handlers=TITANIUM_MENU_MESSAGE_HANDLERS,
    menu_callback_handlers=TITANIUM_MENU_CALLBACK_HANDLERS,
    bot_commands=BOT_COMMANDS_LIST,
)


# ---------------------------------------------------------------------
# Thumbnails / transcoding helpers
# ---------------------------------------------------------------------

def generate_thumbnail(video_path: str, thumb_path: str) -> bool:
    """Extract a frame from the video as a fallback thumbnail using ffmpeg."""
    try:
        import subprocess
        result = subprocess.run(
            [
                "ffmpeg", "-y", "-i", video_path,
                "-ss", "00:00:01", "-vframes", "1",
                "-vf", "scale=320:-1",
                thumb_path,
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=30,
        )
        return result.returncode == 0 and os.path.exists(thumb_path)
    except Exception as e:
        logger.warning(f"ffmpeg thumbnail generation failed: {e}")
        return False


def download_thumb(thumb_url: str, thumb_path: str) -> bool:
    """Download a thumbnail image from a URL."""
    try:
        r = requests.get(thumb_url, timeout=30)
        r.raise_for_status()
        with open(thumb_path, "wb") as f:
            f.write(r.content)
        return True
    except Exception as e:
        logger.warning(f"Thumbnail download failed: {e}")
        return False


def get_video_metadata(video_path: str):
    """Return (duration_seconds, width, height) using ffprobe, or (0, None, None) on failure."""
    try:
        import subprocess, json
        result = subprocess.run(
            [
                "ffprobe", "-v", "error", "-select_streams", "v:0",
                "-show_entries", "stream=width,height:format=duration",
                "-of", "json",
                video_path,
            ],
            capture_output=True, text=True, timeout=30,
        )
        data = json.loads(result.stdout)
        stream = data["streams"][0]
        width = int(stream.get("width") or 0) or None
        height = int(stream.get("height") or 0) or None
        duration = int(float(data["format"]["duration"]))
        return duration, width, height
    except Exception as e:
        logger.warning(f"ffprobe metadata failed: {e}")
        return 0, None, None


def split_video_by_size(video_path: str, duration: int, total_size: int,
                         part_target_bytes: int = SPLIT_PART_SIZE_BYTES, _depth: int = 0) -> list[str]:
    """Split a video into parts of roughly `part_target_bytes` each, using
    ffmpeg's segment muxer with stream copy (no re-encoding, so it's fast
    and lossless). Splitting is duration-based (size / bitrate), since
    ffmpeg's segment muxer can't cut by byte size directly for most
    containers — so a segment can occasionally still land over target if
    the video's bitrate spikes; any oversized part is re-split once more
    with a shorter duration as a safety net.

    Returns a list of part file paths (already sorted), or [video_path]
    unchanged if splitting isn't possible/needed.
    """
    import subprocess

    if total_size <= 0 or duration <= 1:
        return [video_path]

    num_parts = max(2, -(-total_size // part_target_bytes))  # ceil division
    segment_time = max(1, duration // num_parts)

    base, ext = os.path.splitext(video_path)
    out_pattern = f"{base}_part%03d{ext}"

    try:
        result = subprocess.run(
            [
                "ffmpeg", "-y", "-i", video_path,
                "-c", "copy", "-map", "0",
                "-f", "segment", "-segment_time", str(segment_time),
                "-reset_timestamps", "1",
                out_pattern,
            ],
            capture_output=True, text=True, timeout=1800,
        )
        parts = sorted(
            f for f in glob.glob(f"{base}_part*{ext}")
        )
        if result.returncode != 0 or not parts:
            logger.warning(f"ffmpeg split failed (rc={result.returncode}): {result.stderr[-500:]}")
            return [video_path]
    except Exception as e:
        logger.warning(f"ffmpeg split raised: {e}")
        return [video_path]

    # Safety net: if any part still exceeds the hard limit (bitrate spike),
    # re-split just that part with a shorter segment time. Bail out after a
    # couple of recursion levels to avoid ever looping forever.
    final_parts = []
    for part in parts:
        part_size = os.path.getsize(part)
        if part_size > part_target_bytes and _depth < 2:
            part_duration, _, _ = get_video_metadata(part)
            sub_parts = split_video_by_size(part, part_duration, part_size, part_target_bytes, _depth + 1)
            if sub_parts != [part]:
                try:
                    os.remove(part)
                except Exception:
                    pass
                final_parts.extend(sub_parts)
                continue
        final_parts.append(part)

    return final_parts


def transcode_video(src_path: str, dst_path: str, target_height: int) -> bool:
    """Downscale a video to target_height using ffmpeg (blocking, kept for reference)."""
    try:
        import subprocess
        result = subprocess.run(
            [
                "ffmpeg", "-y", "-i", src_path,
                "-vf", f"scale=-2:{target_height}",
                "-c:v", "libx264", "-preset", "veryfast", "-crf", "23",
                "-c:a", "aac", "-b:a", "128k",
                "-movflags", "+faststart",
                dst_path,
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=3600,
        )
        return result.returncode == 0 and os.path.exists(dst_path)
    except Exception as e:
        logger.warning(f"ffmpeg transcode failed: {e}")
        return False


async def transcode_video_async(
    src_path: str, dst_path: str, target_height: int,
    total_duration: int, status_msg: Message, name: str,
) -> bool:
    """Downscale a video to target_height using ffmpeg WITHOUT blocking the event loop.

    Runs ffmpeg as a real async subprocess and parses its `-progress` output
    to update the Telegram status message with a live progress bar, so the
    bot stays responsive to other commands/users while a conversion runs.
    """
    cmd = [
        "ffmpeg", "-y", "-i", src_path,
        "-vf", f"scale=-2:{target_height}",
        "-c:v", "libx264", "-preset", "veryfast", "-crf", "23",
        "-c:a", "aac", "-b:a", "128k",
        "-movflags", "+faststart",
        "-progress", "pipe:1", "-nostats",
        dst_path,
    ]
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
    except Exception as e:
        logger.warning(f"ffmpeg spawn failed: {e}")
        return False

    start_time = time.time()
    last_edit_time = 0.0
    out_time_secs = 0

    try:
        while True:
            line = await proc.stdout.readline()
            if not line:
                break
            line = line.decode(errors="ignore").strip()

            if line.startswith("out_time_ms="):
                try:
                    out_time_secs = int(line.split("=", 1)[1]) / 1_000_000
                except ValueError:
                    pass
            elif line.startswith("out_time="):
                # HH:MM:SS.microseconds fallback if out_time_ms is unavailable
                try:
                    h, m, s = line.split("=", 1)[1].split(":")
                    out_time_secs = int(h) * 3600 + int(m) * 60 + float(s)
                except ValueError:
                    pass

            now = time.time()
            if now - last_edit_time >= 3.0:
                last_edit_time = now
                pct = min(99.0, (out_time_secs / total_duration * 100)) if total_duration else 0
                elapsed = now - start_time
                speed_x = (out_time_secs / elapsed) if elapsed > 0 else 0
                try:
                    await status_msg.edit_text(
                        SC(f"🎞️ <b>Converting to {target_height}p...</b>\n"
                        f"<code>{name}</code>\n\n"
                        "╭━━━━❰Progress❱━➣\n"
                        f"┣⪼ [{progress_bar(pct)}]\n"
                        f"┣⪼ ✅ Done: {pct:.1f}%\n"
                        f"┣⪼ ⏱️ Processed: {human_time(out_time_secs)} / {human_time(total_duration)}\n"
                        f"┣⪼ ⚡ Speed: {speed_x:.2f}x\n"
                        "╰━━━━━━━━━━━━━━━➣"),
                        parse_mode=ParseMode.HTML,
                    )
                except Exception:
                    pass

        returncode = await proc.wait()
        return returncode == 0 and os.path.exists(dst_path)
    except Exception as e:
        logger.warning(f"ffmpeg transcode (async) failed: {e}")
        try:
            proc.kill()
        except Exception:
            pass
        return False


# ---------------------------------------------------------------------
# Download / cache / upload
# ---------------------------------------------------------------------

async def _send_cached_part(client: Client, chat_id: int, part: dict, caption: str) -> "Message | None":
    """Send a single cached part. Prefers copy_message() from its
    CACHE_CHANNEL_ID copy (gives Telegram a fresh file_reference for the
    recipient — this is what fixes cache hits failing/redownloading when
    a *different* user requests a link someone else already downloaded).
    Falls back to the raw file_id if there's no cache-channel copy on
    record or the copy fails. Returns None if both attempts fail."""
    sent_msg = None
    if CACHE_CHANNEL_ID and part.get("cache_chat_id") and part.get("cache_message_id"):
        try:
            sent_msg = await client.copy_message(
                chat_id=chat_id,
                from_chat_id=part["cache_chat_id"],
                message_id=part["cache_message_id"],
                caption=caption,
                parse_mode=ParseMode.HTML,
            )
        except Exception as e:
            logger.warning(f"Cache-channel copy failed, falling back to file_id: {e}")
            sent_msg = None

    if sent_msg is None and part.get("file_id"):
        try:
            sent_msg = await client.send_video(
                chat_id, part["file_id"],
                caption=caption,
                parse_mode=ParseMode.HTML,
                supports_streaming=True,
                duration=part.get("duration", 0),
            )
        except Exception as e:
            logger.warning(f"Cached file_id send failed: {e}")
            sent_msg = None

    return sent_msg


# ---------------------------------------------------------------------
# Fast multi-connection source download
#
# The old download path was a single sequential HTTP stream — one TCP
# connection for the whole file. Most CDNs (Diskwala's included) throttle
# per-connection rather than per-file, so that one connection often caps
# out well below what the link can actually deliver. Opening several
# connections in parallel, each pulling a disjoint byte range of the same
# file, is the standard fix (this is exactly what download managers like
# aria2/IDM do) and is usually the single biggest real-world speed win
# available here. Falls back automatically to the old single-connection
# stream for any source that doesn't advertise Range support.
# ---------------------------------------------------------------------

async def _probe_range_support(session: "aiohttp.ClientSession", url: str):
    """Returns (size_bytes, supports_ranges). size is 0 if unknown."""
    try:
        async with session.head(url, allow_redirects=True, timeout=aiohttp.ClientTimeout(total=20)) as resp:
            if resp.status < 400:
                size = int(resp.headers.get("Content-Length", 0))
                supports = resp.headers.get("Accept-Ranges", "").lower() == "bytes"
                if size:
                    return size, supports
    except Exception:
        pass
    # Some CDNs reject or ignore HEAD requests — probe with a tiny ranged
    # GET instead, which is a more reliable signal anyway (a 206 response
    # is unambiguous proof of range support, unlike the advisory header).
    try:
        async with session.get(url, headers={"Range": "bytes=0-0"}, timeout=aiohttp.ClientTimeout(total=20)) as resp:
            supports = resp.status == 206
            size = 0
            content_range = resp.headers.get("Content-Range", "")
            if "/" in content_range:
                try:
                    size = int(content_range.rsplit("/", 1)[-1])
                except ValueError:
                    size = 0
            if not size:
                size = int(resp.headers.get("Content-Length", 0))
            await resp.read()
            return size, supports
    except Exception:
        return 0, False


async def _download_one_segment(session: "aiohttp.ClientSession", url: str, start: int, end: int,
                                 out_path: str, seg_id: int, progress: dict, retries: int = 2):
    last_exc = None
    for attempt in range(retries + 1):
        seg_downloaded = 0
        try:
            headers = {"Range": f"bytes={start + seg_downloaded}-{end}"}
            async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=600)) as resp:
                if resp.status not in (200, 206):
                    raise RuntimeError(f"segment {seg_id} bad status {resp.status}")
                with open(out_path, "r+b") as f:
                    f.seek(start + seg_downloaded)
                    async for chunk in resp.content.iter_chunked(256 * 1024):
                        if chunk:
                            f.write(chunk)
                            seg_downloaded += len(chunk)
                            progress["downloaded"] += len(chunk)
            return  # success
        except Exception as e:
            last_exc = e
            # Roll back this segment's partial credit before retrying it
            # from scratch, so the shared progress total stays accurate.
            progress["downloaded"] -= seg_downloaded
            if attempt < retries:
                await asyncio.sleep(2)
    raise last_exc


async def _segmented_download(download_url: str, out_path: str, progress: dict) -> bool:
    """Multi-connection ranged download. Returns True on success. Raises
    on anything that means segmentation isn't viable here (no Range
    support, unknown size, file too small) — the caller falls back to a
    plain single-connection stream in every one of those cases, so this
    never needs to be perfectly reliable on its own."""
    connections = max(1, DOWNLOAD_CONNECTIONS)
    if connections <= 1:
        raise RuntimeError("segmented download disabled (DOWNLOAD_CONNECTIONS<=1)")

    timeout = aiohttp.ClientTimeout(total=None, sock_connect=30, sock_read=120)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        size, supports_ranges = await _probe_range_support(session, download_url)
        if not size or not supports_ranges:
            raise RuntimeError(f"server doesn't support ranged downloads (size={size}, ranges={supports_ranges})")
        if size < MIN_SEGMENTED_DOWNLOAD_BYTES:
            raise RuntimeError("file too small to bother segmenting")

        progress["total"] = size
        progress["downloaded"] = 0

        # Pre-allocate a sparse file of the full size so each segment can
        # seek to its own offset and write independently of the others.
        with open(out_path, "wb") as f:
            f.truncate(size)

        seg_size = size // connections
        boundaries = []
        seg_start = 0
        for i in range(connections):
            seg_end = size - 1 if i == connections - 1 else seg_start + seg_size - 1
            boundaries.append((seg_start, seg_end))
            seg_start = seg_end + 1

        tasks = [
            asyncio.create_task(_download_one_segment(session, download_url, s, e, out_path, i, progress))
            for i, (s, e) in enumerate(boundaries)
        ]
        try:
            await asyncio.gather(*tasks)
        except Exception:
            # One segment gave up for good — stop the rest immediately
            # instead of letting them keep writing into a file we're
            # about to abandon for the single-connection fallback.
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            raise

    progress["done"] = True
    return True


async def _run_download(download_url: str, out_path: str, progress: dict):
    """Try the fast segmented download first; fall back to the original
    single-connection stream (run in a thread so it never blocks the
    event loop) if segmentation isn't viable or fails outright."""
    try:
        if await _segmented_download(download_url, out_path, progress):
            return
    except Exception as e:
        logger.warning(f"Segmented download not used, falling back to single-connection: {e}")
        # Reset so the fallback's own progress reporting starts clean
        # instead of showing a stale partial percentage from the attempt
        # above.
        progress["downloaded"] = 0
        progress["total"] = 0

    def _blocking_download():
        try:
            resp = requests.get(download_url, stream=True, timeout=300, allow_redirects=True)
            resp.raise_for_status()
            progress["total"] = int(resp.headers.get("content-length", 0))
            with open(out_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)
                        progress["downloaded"] += len(chunk)
        except Exception as exc:
            progress["error"] = exc
        finally:
            progress["done"] = True

    await asyncio.to_thread(_blocking_download)


async def send_cached_video(client: Client, query, status_msg, link: str, cached: dict) -> bool:
    """Try to resend a previously uploaded video instantly from the cache.
    Handles both single-file and split (multi-part) cache entries — every
    part is sent in order, each with its own "(Part i/N)" caption, exactly
    like the parts were originally uploaded.
    Returns True on success, False if the cache entry is stale (any part
    failed to send) and a fresh download is needed."""
    await status_msg.edit_text(SC("<b>⚡ Found in cache, sending instantly...</b>"), parse_mode=ParseMode.HTML)

    chat_id = query.from_user.id
    parts = cached["parts"]
    num_parts = len(parts)
    sent_messages = []

    for idx, part in enumerate(parts, start=1):
        caption = await build_caption(
            name=cached["name"] if num_parts == 1 else f"{cached['name']} (Part {idx}/{num_parts})",
            size_bytes=part.get("size") or cached["size"],
            dl_seconds=0,
            ul_seconds=0,
            user_id=chat_id,
            source_link=link,
            quality_label=cached["quality_label"],
            duration_seconds=part.get("duration") or cached.get("duration", 0),
        )
        sent_msg = await _send_cached_part(client, chat_id, part, caption)
        if sent_msg is None:
            # Whatever's already been sent stays with the user (no point
            # deleting a good partial delivery); the cache entry itself
            # gets dropped by the caller so the next request redownloads
            # cleanly instead of repeating this same partial failure.
            logger.warning(f"Cache hit failed on part {idx}/{num_parts} — treating whole entry as stale.")
            return False
        sent_messages.append(sent_msg)

    try:
        await status_msg.delete()
        premium = await get_effective_premium_status(chat_id)
        if not premium["is_premium"]:
            await bump_daily_count(chat_id)
        await bump_total_downloads(chat_id)
        for sm in sent_messages:
            asyncio.create_task(schedule_delete(client, chat_id, sm.id))
            asyncio.create_task(backup_to_linked_channels(client, chat_id, sm.id))
            asyncio.create_task(forward_to_dump_chat(client, chat_id, sm.id))
        asyncio.create_task(log_event(
            client,
            "📥 <b>Download (cache hit)</b>\n\n"
            f"👤 User: <code>{chat_id}</code>\n"
            f"📄 Name: {cached['name']}\n"
            f"🔗 Link: {link}",
        ))
        return True
    except Exception as e:
        # The video(s) themselves already sent successfully at this point
        # — only the bookkeeping below failed. Don't report False here, or
        # the caller will delete a perfectly good cache entry and force a
        # pointless redownload for a file the user already received.
        logger.warning(f"Cache-hit post-send bookkeeping failed (delivery itself succeeded): {e}")
        return True


async def _queue_position_ticker(status_msg, chat_id: int, queue: "DownloadQueue", is_premium: bool):
    """While a user waits for a free download slot, periodically refresh
    the status message with their live queue position (people ahead of
    them may finish or /cancel at any time, so the position keeps moving)."""
    last_shown = None
    try:
        while True:
            pos = queue.position(chat_id)
            if pos == 0:
                break  # slot acquired, or fell out of the queue entirely
            if pos != last_shown:
                label = "💎 Priority queue" if is_premium else "⏳ Queued"
                note = (
                    "You have your own reserved premium slots — this wait is only "
                    "behind other premium downloads, never free ones.\n\n"
                    if is_premium else
                    f"Up to {queue.max_concurrent} downloads run at once — "
                    "yours will start automatically as soon as a slot frees up.\n\n"
                )
                try:
                    await status_msg.edit_text(
                        SC(f"<b>{label} — position {pos}</b>\n\n"
                           f"{note}"
                           "Send /cancel to leave the queue."),
                        parse_mode=ParseMode.HTML,
                    )
                except Exception:
                    pass
                last_shown = pos
            await asyncio.sleep(3)
    except asyncio.CancelledError:
        pass


INFLIGHT_DOWNLOADS: dict = {}  # "{link}::{quality}" -> asyncio.Event, set by the
# leader once that link+quality's attempt is fully done (success or failure).
# Lets a second/third/... request for the exact same content join the first
# one instead of kicking off a fully redundant parallel download+ffmpeg+upload.


async def download_video(client: Client, query, link: str, quality: str = "auto"):
    status_msg = query.message
    chat_id = query.from_user.id
    # Registered *before* we even try to acquire a slot, so /cancel works
    # while someone is still sitting in the queue, not just once their
    # download is actively running.
    ACTIVE_TASKS[chat_id] = asyncio.current_task()

    key = f"{link}::{quality}"
    my_event = None
    try:
        # Join an already-in-flight download for this exact link+quality
        # instead of starting a redundant one — if two users hit the same
        # popular link within the same few minutes, only the first one
        # actually downloads/transcodes/uploads; everyone else just waits
        # and then gets served instantly from the cache the first one fills.
        # No await happens between the dict lookup and registering
        # ourselves as leader below, so this is race-free the same way
        # DownloadQueue.acquire() is.
        told_waiting = False
        while True:
            existing_event = INFLIGHT_DOWNLOADS.get(key)
            if existing_event is None:
                my_event = asyncio.Event()
                INFLIGHT_DOWNLOADS[key] = my_event
                break  # we're the leader — proceed to the real download below
            if not told_waiting:
                try:
                    await status_msg.edit_text(
                        SC("<b>👥 Someone else just requested this same file — "
                           "waiting for their download to finish so you can "
                           "get it instantly from cache...</b>"),
                        parse_mode=ParseMode.HTML,
                    )
                except Exception:
                    pass
                told_waiting = True
            await existing_event.wait()
            cached = await get_cached_file(link, quality)
            if cached:
                ok = await send_cached_video(client, query, status_msg, link, cached)
                if ok:
                    return
            # The leader's attempt didn't leave a usable cache entry
            # (failed, or the cache write itself failed) — loop around:
            # either someone else already grabbed the leader slot in the
            # meantime (wait on them too), or it's free and we take it.

        # Premium/admin users get their own reserved pool (premium_download_queue)
        # entirely separate from the free pool — real VIP treatment: they never
        # wait because of free-user traffic, only ever behind other premium
        # downloads already in progress.
        premium = await get_effective_premium_status(chat_id)
        is_premium = premium["is_premium"]
        queue = premium_download_queue if is_premium else free_download_queue

        will_queue = queue.active >= queue.max_concurrent
        ticker_task = asyncio.create_task(_queue_position_ticker(status_msg, chat_id, queue, is_premium)) if will_queue else None
        acquired = False
        try:
            try:
                await queue.acquire(chat_id)
                acquired = True
            finally:
                if ticker_task:
                    ticker_task.cancel()

            await _download_video_inner(client, query, link, quality, status_msg, chat_id)
        finally:
            if acquired:
                queue.release()
    except asyncio.CancelledError:
        try:
            await status_msg.edit_text(SC("<b>❌ Cancelled.</b>"), parse_mode=ParseMode.HTML)
        except Exception:
            pass
    finally:
        if my_event is not None:
            # Pop before set(): any follower waking up from .wait() must
            # see the key already gone if it needs to take over as leader.
            INFLIGHT_DOWNLOADS.pop(key, None)
            my_event.set()
        ACTIVE_TASKS.pop(chat_id, None)


async def _download_video_inner(client: Client, query, link: str, quality: str, status_msg, chat_id: int):
    if True:

        # Enforce the daily free-download limit before doing any work.
        premium = await get_effective_premium_status(chat_id)
        if not premium["is_premium"]:
            used_today = await get_daily_count(chat_id)
            if used_today >= DAILY_FREE_LIMIT:
                try:
                    await status_msg.delete()
                except Exception:
                    pass
                await send_referral_prompt(client, chat_id)
                return

        cached = await get_cached_file(link, quality)
        if cached:
            ok = await send_cached_video(client, query, status_msg, link, cached)
            if ok:
                return
            await delete_cached_file(link, quality)

        await status_msg.edit_text(SC("<b>Starting download...</b>"), parse_mode=ParseMode.HTML)
        try:
            video_info = await resolve_diskwala_cached(link)

            name = video_info.get("name", "video.mp4")
            download_url = video_info.get("downloadUrl")
            thumb_url = video_info.get("thumb")

            if not download_url:
                await status_msg.edit_text(SC("<b>No download URL found</b>"))
                return

            if "." not in name:
                name += ".mp4"
            name = "".join(c for c in name if c.isalnum() or c in " ._-"[:])
            out_path = os.path.join(DOWNLOAD_DIR, name)

            await status_msg.edit_text(SC(f"<b>Downloading...</b>\n<code>{name}</code>"), parse_mode=ParseMode.HTML)

            # Downloading runs fully off the main coroutine's critical path:
            # either the fast multi-connection segmented path or the
            # single-connection fallback (via asyncio.to_thread) — neither
            # ever blocks the event loop, which the whole bot (main bot +
            # every Titanium clone) shares. This coroutine just polls a
            # shared dict once a second to update the progress message.
            dl_start = time.time()
            max_download_attempts = 4
            dl_last_error = None

            for dl_attempt in range(1, max_download_attempts + 1):
                _progress = {"downloaded": 0, "total": 0, "done": False, "error": None}

                dl_tracker = ProgressTracker(status_msg, "Downloading", name, quality="Auto (Best)")
                dl_task = asyncio.create_task(_run_download(download_url, out_path, _progress))
                while not _progress["done"]:
                    await dl_tracker.update(_progress["downloaded"], _progress["total"])
                    await asyncio.sleep(1)
                await dl_task

                if _progress["error"] is None:
                    dl_last_error = None
                    break

                dl_last_error = _progress["error"]
                logger.warning(f"Download attempt {dl_attempt}/{max_download_attempts} failed: {dl_last_error}")
                if dl_attempt < max_download_attempts:
                    try:
                        await status_msg.edit_text(
                            SC(f"<b>⚠️ Download failed, retrying... ({dl_attempt + 1}/{max_download_attempts})</b>"),
                            parse_mode=ParseMode.HTML,
                        )
                    except Exception:
                        pass
                    await asyncio.sleep(3)

            if dl_last_error is not None:
                raise dl_last_error

            downloaded, total = _progress["downloaded"], _progress["total"]
            # ensure a final 100% update
            await dl_tracker.update(downloaded, total or downloaded)
            dl_seconds = time.time() - dl_start

            # Direct download only — no transcoding/quality selection anymore.
            quality_label = "Auto (Best)"
            upload_size = total or downloaded

            # Prepare thumbnail: custom user thumb takes priority, then
            # API-provided thumb, then extract a frame from the video
            thumb_path = out_path + "_thumb.jpg"
            got_thumb = False
            custom_thumb_id = await get_thumbnail(chat_id)
            if custom_thumb_id:
                try:
                    await client.download_media(custom_thumb_id, file_name=thumb_path)
                    got_thumb = os.path.exists(thumb_path)
                except Exception as e:
                    logger.warning(f"custom thumb download failed: {e}")
            if not got_thumb and thumb_url:
                got_thumb = await asyncio.to_thread(download_thumb, thumb_url, thumb_path)
            if not got_thumb:
                got_thumb = await asyncio.to_thread(generate_thumbnail, out_path, thumb_path)

            # Extract real duration/width/height so Telegram shows correct video length
            duration, vid_width, vid_height = await asyncio.to_thread(get_video_metadata, out_path)

            needs_split = upload_size > SPLIT_THRESHOLD_BYTES
            part_paths = [out_path]
            if needs_split:
                await status_msg.edit_text(
                    SC(f"<b>File is {human_size(upload_size)} — splitting into parts...</b>"),
                    parse_mode=ParseMode.HTML,
                )
                part_paths = await asyncio.to_thread(
                    split_video_by_size, out_path, duration, upload_size
                )
                if len(part_paths) <= 1:
                    logger.warning("Split produced a single part (ffmpeg unavailable/failed) — trying direct upload anyway.")
                    needs_split = False

            sent_messages = []
            part_durations = []
            ul_seconds_total = 0.0
            num_parts = len(part_paths)

            for idx, part_path in enumerate(part_paths, start=1):
                part_size = os.path.getsize(part_path)
                if part_path == out_path:
                    part_duration, part_width, part_height = duration, vid_width, vid_height
                else:
                    part_duration, part_width, part_height = await asyncio.to_thread(get_video_metadata, part_path)

                label = f"Uploading" if num_parts == 1 else f"Uploading (Part {idx}/{num_parts})"
                await status_msg.edit_text(SC(f"<b>{label}...</b>"), parse_mode=ParseMode.HTML)
                ul_tracker = ProgressTracker(status_msg, label, name, quality=quality_label)
                ul_start = time.time()

                part_caption = await build_caption(
                    name=name if num_parts == 1 else f"{name} (Part {idx}/{num_parts})",
                    size_bytes=part_size,
                    dl_seconds=dl_seconds,
                    ul_seconds=0,
                    user_id=chat_id,
                    source_link=link,
                    quality_label=quality_label,
                    duration_seconds=part_duration,
                )

                sent_msg = await client.send_video(
                    chat_id, part_path,
                    caption=part_caption,
                    parse_mode=ParseMode.HTML,
                    supports_streaming=True,
                    thumb=thumb_path if got_thumb else None,
                    duration=part_duration,
                    width=part_width,
                    height=part_height,
                    progress=ul_tracker.update,
                )
                ul_seconds_total += time.time() - ul_start
                sent_messages.append(sent_msg)
                part_durations.append(part_duration)

            ul_seconds = ul_seconds_total
            sent_msg = sent_messages[-1]

            # Cache every part's file_id so identical future requests
            # (single-file or split) can be sent instantly instead of
            # redownloading. Each part gets its own cache-channel copy so
            # a cache hit can copy_message() every part with a fresh
            # file_reference, the same way single-file cache hits already
            # worked.
            if all(sm.video for sm in sent_messages):
                cache_parts = []
                for idx, sm in enumerate(sent_messages):
                    part_cache_chat_id = None
                    part_cache_message_id = None
                    if CACHE_CHANNEL_ID:
                        try:
                            cache_copy = await client.copy_message(
                                chat_id=CACHE_CHANNEL_ID,
                                from_chat_id=chat_id,
                                message_id=sm.id,
                            )
                            part_cache_chat_id = CACHE_CHANNEL_ID
                            part_cache_message_id = cache_copy.id
                        except Exception as e:
                            logger.warning(f"Cache-channel copy after fresh upload failed (part {idx + 1}/{num_parts}): {e}")
                            # Don't wait for the next scheduled health check —
                            # a copy failure right here is the strongest
                            # possible signal something's wrong with the
                            # cache channel, so verify (and alert if so) now.
                            asyncio.create_task(check_cache_channel_health(client))

                    cache_parts.append({
                        "file_id": sm.video.file_id,
                        "size": os.path.getsize(part_paths[idx]) if idx < len(part_paths) else upload_size,
                        "duration": part_durations[idx] if idx < len(part_durations) else duration,
                        "cache_chat_id": part_cache_chat_id,
                        "cache_message_id": part_cache_message_id,
                    })

                await set_cached_file(
                    link, quality,
                    parts=cache_parts,
                    name=name,
                    size=upload_size,
                    quality_label=quality_label,
                    duration=duration,
                )

            # Update caption(s) now that we know the real upload duration
            for idx, sm in enumerate(sent_messages, start=1):
                try:
                    await sm.edit_caption(
                        caption=SC(await build_caption(
                            name=name if num_parts == 1 else f"{name} (Part {idx}/{num_parts})",
                            size_bytes=os.path.getsize(part_paths[idx - 1]) if idx - 1 < len(part_paths) else upload_size,
                            dl_seconds=dl_seconds,
                            ul_seconds=ul_seconds,
                            user_id=chat_id,
                            source_link=link,
                            quality_label=quality_label,
                            duration_seconds=part_durations[idx - 1] if idx - 1 < len(part_durations) else duration,
                        )),
                        parse_mode=ParseMode.HTML,
                    )
                except Exception:
                    pass
            await status_msg.delete()
            for part_path in part_paths:
                try:
                    os.remove(part_path)
                except Exception:
                    pass
            if got_thumb:
                try:
                    os.remove(thumb_path)
                except Exception:
                    pass

            if not premium["is_premium"]:
                await bump_daily_count(chat_id)
            await bump_total_downloads(chat_id)
            for sm in sent_messages:
                asyncio.create_task(schedule_delete(client, chat_id, sm.id))
                asyncio.create_task(backup_to_linked_channels(client, chat_id, sm.id))
                asyncio.create_task(forward_to_dump_chat(client, chat_id, sm.id))
            asyncio.create_task(log_event(
                client,
                "📥 <b>Download (fresh)</b>\n\n"
                f"👤 User: <code>{chat_id}</code>\n"
                f"📄 Name: {name}\n"
                + (f"📦 Split into {num_parts} parts\n" if num_parts > 1 else "")
                + f"🔗 Link: {link}",
            ))

        except Exception as e:
            logger.error(f"Download error: {e}")
            await status_msg.edit_text(
                SC(f"<b>Download failed</b>\n<code>{str(e)[:500]}</code>"),
                parse_mode=ParseMode.HTML,
            )


async def send_stream_link(client: Client, query, link: str):
    try:
        video_info = await resolve_diskwala_cached(link)

        # Same second call process_link already makes for the preview
        # caption — was missing here entirely, so "Stream Link Ready"
        # only ever showed Name + Size even when this exact link's
        # preview message (sent moments earlier) had author/duration/
        # views/likes/upload date/category too.
        preview = {}
        try:
            preview = await asyncio.to_thread(fetch_diskwala_preview, link)
        except Exception as e:
            logger.warning(f"Stream-link preview HTML scrape failed for {link}: {e}")

        name = video_info.get("name", "video.mp4")
        size = video_info.get("size", 0) or preview.get("size") or 0

        # Author / duration / views / likes / description / upload_date /
        # category — same priority order as process_link: video_info
        # (API/direct-resolve) first, preview (HTML scrape / temp_info)
        # fills any gap.
        author      = video_info.get("creator")      or preview.get("author")      or ""
        dur_secs    = video_info.get("duration_seconds") or preview.get("duration_seconds") or 0
        views       = video_info.get("views")        or preview.get("views")
        likes       = video_info.get("likes")        or preview.get("likes")
        description = video_info.get("description")  or preview.get("description")
        upload_date = video_info.get("upload_date")  or preview.get("upload_date")
        extension   = video_info.get("extension") or (name.rsplit(".", 1)[-1].lower() if "." in name else None)
        category    = video_info.get("category") or preview.get("category")
        if not category and extension:
            _V = {"mp4","mkv","avi","mov","webm","flv","wmv","m4v","ts","3gp","m2ts","vob"}
            _A = {"mp3","flac","aac","ogg","m4a","wav","opus","wma","alac","aiff"}
            _I = {"jpg","jpeg","png","gif","webp","bmp","tiff","tif","svg","heic","heif","avif"}
            _D = {"pdf","doc","docx","ppt","pptx","xls","xlsx","txt","zip","rar","7z","tar","gz"}
            category = ("Video" if extension in _V else
                        "Audio" if extension in _A else
                        "Photo" if extension in _I else
                        "Document" if extension in _D else None)

        # Raw CDN URL from Flezen — ends in .bin, forces browser download.
        # We register it under a short code and serve it via /stream/<code>
        # with proper Content-Type and Range support so it plays inline.
        cdn_url = video_info.get("streamUrl") or video_info.get("downloadUrl")

        if not cdn_url:
            await query.message.edit_text(SC("<b>No stream URL found</b>"))
            return

        # Make sure name has a video extension (Flezen sometimes omits it)
        if not any(name.lower().endswith(ext) for ext in (
            ".mp4", ".mkv", ".webm", ".mov", ".avi", ".m4v", ".ts", ".flv"
        )):
            name += ".mp4"

        # Register proxy — short hash of the cdn_url as the code
        import hashlib
        code = hashlib.sha1(cdn_url.encode()).hexdigest()[:12]
        register_stream_proxy(code, cdn_url, name, size)
        stream_url = get_stream_public_url(code)

        size_str = human_size(size) if size else "Unknown"

        author_line   = f"👤 {SC('By')}: {SC(author)}\n" if author else ""
        dur_line      = f"⏱ {SC('Duration')}: {hms(dur_secs)}\n" if dur_secs else ""
        ext_line      = f"📎 {SC('Extension')}: .{extension.upper()}\n" if extension else ""
        views_line    = f"👁 {SC('Views')}: {views:,}\n" if isinstance(views, (int, float)) else (f"👁 {SC('Views')}: {views}\n" if views else "")
        likes_line    = f"👍 {SC('Likes')}: {likes:,}\n" if isinstance(likes, (int, float)) else (f"👍 {SC('Likes')}: {likes}\n" if likes else "")
        date_line     = f"📅 {SC('Uploaded')}: {upload_date}\n" if upload_date else ""
        category_line = f"🏷 {SC('Category')}: {SC(str(category))}\n" if category else ""
        desc_line     = f"📝 {SC(str(description)[:200])}\n" if description else ""

        await query.message.edit_text(
            SC(f"<b>Stream Link Ready</b>\n\n"
            f"Name: <code>{name}</code>\n"
            f"Size: <code>{size_str}</code>\n"
            f"{author_line}"
            f"{dur_line}"
            f"{ext_line}"
            f"{views_line}"
            f"{likes_line}"
            f"{date_line}"
            f"{category_line}"
            f"{desc_line}"),
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([
                [make_button(SC("▶️ Stream / Play"), url=stream_url, style=BTN_PRIMARY)],
            ]),
        )
    except Exception as e:
        logger.error(f"Stream error: {e}")
        await query.message.edit_text(
            SC(f"<b>Stream link failed</b>\n<code>{str(e)[:500]}</code>"),
            parse_mode=ParseMode.HTML,
        )


async def set_bot_commands_list():
    await app.set_bot_commands(BOT_COMMANDS_LIST)


async def _startup_log():
    try:
        me = await app.get_me()
        app._cached_username = me.username
        stats = await get_stats_summary()
        ist_time = datetime.now(timezone(timedelta(hours=5, minutes=30))).strftime("%I:%M %p IST")
        await log_event(
            app,
            "🚀 <b>Bot successfully started!</b>\n\n"
            f"⭐ Bot: @{me.username}\n"
            f"👥 Users: {stats['total_users']}\n"
            f"⏳ Time: {ist_time}\n\n"
            f"👑 Developed by {POWERED_BY_URL.replace('https://t.me/', '@')}",
        )
    except Exception as e:
        logger.warning(f"Startup log failed: {e}")


def _run_maybe_async(value, loop: asyncio.AbstractEventLoop):
    """Run the result of a library call that may or may not be a
    coroutine, depending on the installed pyrogram/kurigram version.

    Newer kurigram releases have, at times, changed Client.start()/
    stop() to already run to completion synchronously (returning None)
    instead of returning a coroutine — which makes a bare
    `loop.run_until_complete(app.start())` crash with:
    'TypeError: An asyncio.Future, a coroutine or an awaitable is
    required', even though everything actually started up fine. This
    checks which behavior we got and only hands the loop a real
    awaitable."""
    if inspect.isawaitable(value):
        return loop.run_until_complete(value)
    return value


if __name__ == "__main__":
    logger.info("Starting Diskwala Bot...")
    keep_alive()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(ensure_indexes())

    # FloodWait on auth.ImportBotAuthorization happens when the bot restarts
    # too frequently (e.g. repeated failed deploys). Instead of crashing and
    # letting Render immediately restart (which triggers another FloodWait),
    # we catch it, wait the required time, then retry once.
    _start_attempts = 0
    while True:
        try:
            _run_maybe_async(app.start(), loop)
            break  # started successfully
        except FloodWait as fw:
            _start_attempts += 1
            wait_sec = fw.value + 5  # add 5s buffer
            logger.warning(
                "Telegram FloodWait on bot start (attempt %d) — "
                "waiting %d seconds before retry...",
                _start_attempts, wait_sec,
            )
            if _start_attempts >= 3:
                logger.error("Too many FloodWait retries — giving up.")
                raise
            time.sleep(wait_sec)
            logger.info("FloodWait done — retrying bot start...")

    loop.run_until_complete(set_bot_commands_list())
    loop.run_until_complete(_startup_log())
    loop.run_until_complete(titanium.boot_titanium_bots())
    loop.run_until_complete(resume_scheduled_deletions(app))
    loop.create_task(cache_channel_health_check_loop(app))
    idle()
    _run_maybe_async(app.stop(), loop)
