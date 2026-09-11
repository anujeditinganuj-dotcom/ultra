# Titanium Clone Mode — ported from the "Most Powerful Src Bot" project's
# Akbots/titanium.py, trimmed down to fit Diskwala bot's single-file
# architecture.
#
# What it does:
#   Lets a user connect their own @BotFather bot token. The token gets a
#   real, running pyrogram Client of its own ("the clone"), which is
#   wired up with the EXACT SAME link_handler/callback_handler functions
#   main.py already uses for downloads — so the clone can fetch/stream
#   Diskwala links exactly like the main bot, just on its own bot
#   token/flood-limit pool instead of sharing the main bot's with every
#   other user.
#
# How "owner-only" is enforced:
#   _register_owner_gate() adds one filter-less handler, in a group
#   number (OWNER_GATE_GROUP) lower than every other handler registered
#   on the clone. It runs before anything else on every message/callback
#   the clone receives:
#     - sender is the owner -> raise ContinuePropagation, so pyrogram
#       moves on to the real handlers exactly as normal.
#     - anyone else          -> return with no reply, so nothing else on
#       the clone ever runs for them.
#   This is the entire safety boundary — one handler at the front of the
#   queue, not a per-handler audit.
#
# Two ways to connect a clone bot:
#   1. Manual (/addbot <token>) — paste a @BotFather token directly.
#   2. Auto-create (Bot API 9.6 "Managed Bots", added April 2026) — tap a
#      button, Telegram shows a native "Create Bot?" dialog with a
#      pre-filled name/username, tap Create, done. See
#      _titanium_autocreate_cb / _handle_managed_bot_created below for how
#      this is wired. REQUIRES a one-time manual setup step this code
#      can't do for you: open @BotFather's Mini App → enable "Bot
#      Management Mode" for this bot. Needs a recent kurigram build with
#      the April-2026 TL additions generated — see _managed_bots_available()
#      below; on an older build this just falls back to /addbot only.
#
# Managed-bot token revocation (reference's _revoke_managed_bot_token,
# Bot API 9.6 replaceManagedBotToken) — ported and wired into two spots:
#   1. Remove-cleanup — /delbot and the ❌ Remove button both revoke a
#      "managed" (Auto-Create) bot's token with Telegram before wiping it
#      from storage, so a removed clone's token can't keep working
#      elsewhere. Manual (/addbot) bots skip this — this bot has no API
#      access to revoke a @BotFather-issued token, only /revoke on
#      BotFather itself does that.
#   2. Auto token-rotation — a 🔄 Rotate Token button (bot-details view,
#      managed bots only) that revokes the current token AND gets a
#      fresh one back in the SAME Telegram call, verifies it, swaps the
#      stored entry and restarts the clone. No manual @BotFather paste
#      needed, unlike a manual bot (which has no equivalent here — just
#      /delbot then /addbot with a new token).
#
# What was intentionally left out (vs. the reference implementation):
#   - A full admin/settings menu on the clone — the clone's whole point
#     is personal downloading, so it only gets /start plus the same
#     download/stream/cancel handlers as the main bot.
#   - The reference's purpose="custom_bot" branch / _AUTOCREATE_PURPOSE
#     dict — that's for Akbots' separate "My Bots → Add Auto Bot" plain
#     forwarding-bot feature, which this project doesn't have. Every
#     auto-created bot here becomes a Titanium clone, full stop.
#   - get_job_client()'s flood-pool auto-selection (main bot picking a
#     clone for a forward job) — Akbots-specific concept for its
#     multi-chat forward jobs; doesn't apply to Diskwala's link-download
#     model, where the clone is only ever used directly by its owner.

import asyncio
import logging
import re
import time
from datetime import datetime
from urllib.parse import quote

import aiohttp
from pyrogram import Client, filters, raw, ContinuePropagation
from pyrogram.handlers import MessageHandler, CallbackQueryHandler, RawUpdateHandler
from pyrogram.types import CallbackQuery, InlineKeyboardMarkup, Message

from config import API_ID, API_HASH, BOT_TOKEN
from database import (
    get_titanium_bots, add_titanium_bot, remove_titanium_bot,
    get_all_titanium_owners, touch_titanium_bot,
)

logger = logging.getLogger("diskwala_bot")

# Lower than any other group used in this bot (main.py's lowest is -1,
# for react_to_any_command), so the gate always runs first on a clone.
OWNER_GATE_GROUP = -1000

_CLONE_CACHE = {}  # token -> connected, owner-gated clone Client

# owner_id -> {"prompt_id": int, "expires": float} — set while waiting for
# a pasted @BotFather token after /addbot (no args) or the ➕ Add Bot
# button. Checked by the group=-5 catch-all handlers below.
_pending_addbot = {}
ADDBOT_TIMEOUT = 120
_TOKEN_RE = re.compile(r"\d[0-9]{8,10}:[0-9A-Za-z_-]{35}")

# owner_id -> {"old_username": str, "prompt_msg": Message, "expires": float} —
# set while waiting for a pasted replacement @BotFather token after tapping
# 🔄 Replace Bot on a manual (non-managed) clone. Checked by the same
# group=-5 catch-all machinery as _pending_addbot.
_pending_replace = {}
REPLACE_TIMEOUT = 180

# Filled in by register_titanium_handlers() so boot_titanium_bots() (called
# separately, before the event loop starts handling updates) can reuse the
# exact same handler wiring.
_wiring = {}


def _register_owner_gate(clone: Client, owner_id: int):
    async def _gate(client, update):
        sender = getattr(update, "from_user", None)
        if sender is not None and sender.id == owner_id:
            raise ContinuePropagation
        # not the owner: swallow the update, nothing further runs for them

    clone.add_handler(MessageHandler(_gate, filters.all), group=OWNER_GATE_GROUP)
    clone.add_handler(CallbackQueryHandler(_gate, filters.all), group=OWNER_GATE_GROUP)


def _clone_all_app_handlers(clone: Client):
    """Copies EVERY MessageHandler/CallbackQueryHandler registered on the
    MAIN app onto `clone`, at their original group numbers. This is what
    makes a Titanium clone a FULL personal copy of the bot — every
    command, every menu button, every inline callback — matching the
    reference project's `plugins=dict(root="Akbots")` approach (which
    loads the exact same plugin package on the clone as the main bot).

    This project has no separate plugin package to reload — everything
    lives in one main.py — so the equivalent here is copying the already-
    registered handler *objects* straight from app.dispatcher.groups.
    Pyrogram handler objects are just callback+filter wrappers with no
    state tying them to a specific Client, so they're safe to reuse
    across multiple Clients this way.

    Groups < 0 are skipped — those are this project's own internal
    plumbing on the main app (e.g. the -5 /addbot token-catch handlers,
    which are meaningless on a clone since Titanium itself isn't
    re-offered inside a clone) and are handled separately/explicitly
    where a clone-specific version is actually wanted (see the -1
    /start & /cancel overrides right below this function's call site).
    RawUpdateHandlers (e.g. the managed-bot-creation listener) are
    likewise not copied — a clone doesn't need to be able to create
    further clones of itself.

    The main app's OWN /start and /cancel handlers (registered at the
    default group 0, same as everything else) are also explicitly
    skipped here — even though group -1 already registers clone-specific
    overrides for exactly these two commands, relying on "group -1 runs
    first so it wins" was not actually enough to stop the group-0 copy
    from *also* firing on every /start (users were seeing two replies:
    the clone's own welcome message, plus the main bot's start message
    copied verbatim). Skipping the copy outright removes the duplicate
    regardless of dispatcher group/propagation quirks, rather than
    depending on ordering to suppress it.
    """
    main_app = _wiring["app"]
    groups = getattr(getattr(main_app, "dispatcher", None), "groups", None)
    if not groups:
        # Shouldn't happen on any pyrogram/kurigram build actually running
        # this bot — dispatcher.groups is where every @app.on_message /
        # @app.on_callback_query handler ends up. Logged loudly rather than
        # silently degrading, since without this the clone is back to
        # being a stripped-down copy instead of a full one.
        logger.error(
            "Titanium: couldn't read app.dispatcher.groups — clone will be "
            "missing most of the main bot's commands/buttons. Check the "
            "installed pyrogram/kurigram version."
        )
        return
    skip_callbacks = {
        cb for cb in (_wiring.get("start_handler"), _wiring.get("cancel_handler")) if cb is not None
    }
    for group, handlers in groups.items():
        if group < 0:
            continue
        for h in handlers:
            if isinstance(h, (MessageHandler, CallbackQueryHandler)):
                if getattr(h, "callback", None) in skip_callbacks:
                    continue
                clone.add_handler(h, group)


async def _get_clone_client(token: str, owner_id: int) -> Client:
    """Returns a connected, owner-gated clone Client for `token`, creating
    and starting one if it isn't already cached. This is the ONLY place
    clone Clients get constructed."""
    cached = _CLONE_CACHE.get(token)
    if cached is not None and cached.is_connected:
        try:
            await touch_titanium_bot(owner_id, token)
        except Exception as e:
            logger.debug(f"touch_titanium_bot (cached) failed: {e}")
        return cached

    clone_start_caption = _wiring["clone_start_caption"]
    SC = _wiring["SC"]
    ParseMode = _wiring["ParseMode"]
    start_photo_url = _wiring["start_photo_url"]
    fallback_keyboard = _wiring["fallback_keyboard"]
    fallback_text = _wiring["fallback_text"]
    main_menu_kb = _wiring["main_menu_kb"]

    clone = Client(
        f"titanium_{token[:10]}",
        api_id=API_ID,
        api_hash=API_HASH,
        bot_token=token,
        in_memory=True,
    )

    # Gate must be registered before start() — add_handler() is safe pre-start.
    # Runs at OWNER_GATE_GROUP (-1000), before anything else below.
    _register_owner_gate(clone, owner_id)

    async def _clone_start(client, m: Message):
        # Mirrors the main bot's /start screen exactly: same photo, same
        # feature caption, same Download/Status inline buttons, then the
        # same "send a link" nudge with the Plans/Status/Help/Support
        # reply keyboard underneath — just addressed using the clone's
        # own username instead of the main bot's.
        caption = await clone_start_caption(client, m)
        try:
            await m.reply_photo(start_photo_url, caption=SC(caption), reply_markup=fallback_keyboard(), parse_mode=ParseMode.HTML)
        except Exception as e:
            logger.warning(f"Titanium clone start photo failed, falling back to text: {e}")
            await m.reply(SC(caption), reply_markup=fallback_keyboard(), parse_mode=ParseMode.HTML)
        await m.reply(SC(fallback_text), reply_markup=main_menu_kb)

    # group=-1: these run BEFORE the full handler copy below (group 0+),
    # so they override the copies of the main app's own /start & /cancel
    # handlers instead of racing with them.
    clone.add_handler(MessageHandler(_clone_start, filters.command("start") & filters.private), group=-1)
    clone.add_handler(MessageHandler(_wiring["cancel_handler"], filters.command("cancel") & filters.private), group=-1)

    # FULL clone: every other command, menu button and inline callback the
    # main bot has — /help, /plans, /myplan, settings, caption/thumbnail
    # tools, the download & stream flow, all of it — now works identically
    # on the clone, exactly like the reference project's full plugin clone.
    _clone_all_app_handlers(clone)

    await clone.start()
    _CLONE_CACHE[token] = clone

    # Same "/" command menu (BotFather-style) as the main bot — see
    # BOT_COMMANDS_LIST in main.py, threaded through here via _wiring.
    # Best-effort: a failure here (flood-wait, transient API hiccup)
    # shouldn't stop the clone from working, just leave its menu empty.
    bot_commands = _wiring.get("bot_commands")
    if bot_commands:
        try:
            await clone.set_bot_commands(bot_commands)
        except Exception as e:
            logger.warning(f"Titanium: set_bot_commands failed for owner {owner_id}: {e}")

    try:
        await touch_titanium_bot(owner_id, token)
    except Exception as e:
        logger.debug(f"touch_titanium_bot (start) failed: {e}")
    logger.info(f"Titanium: full clone started for owner {owner_id} (token ...{token[-6:]})")
    return clone


async def boot_titanium_bots():
    """Reconnects every user's saved Titanium bots as full, owner-gated
    clones. Call once at startup — without this, a connected clone stops
    answering after every process restart until /addbot or a settings-panel
    ping happens to touch it again."""
    connected = 0
    try:
        owners = await get_all_titanium_owners()
        for owner_id, bots in owners:
            for b in bots:
                try:
                    await _get_clone_client(b["token"], owner_id)
                    connected += 1
                except Exception as e:
                    logger.warning(f"Titanium boot: couldn't reconnect @{b.get('username', '?')} for {owner_id}: {e}")
    except Exception as e:
        logger.error(f"Titanium boot_titanium_bots failed: {e}")
    if connected:
        logger.info(f"Titanium: {connected} personal clone bot(s) reconnected on boot.")
    return connected


def _managed_bots_available() -> bool:
    """Whether this pyrogram/kurigram build's raw-API layer has the Bot
    API 9.6 "Managed Bots" TL constructors generated yet
    (RequestPeerTypeCreateBot, InputKeyboardButtonRequestPeer,
    MessageActionManagedBotCreated, messages.SendBotRequestedPeer). This
    is a very new (April 2026) addition to the MTProto schema — checked
    defensively with hasattr() rather than importing directly, so an
    outdated kurigram build degrades to "auto-create unavailable" instead
    of an ImportError crashing this module. Run `pip install -U kurigram`
    if this unexpectedly returns False."""
    return (
        hasattr(raw.types, "RequestPeerTypeCreateBot")
        and hasattr(raw.types, "InputKeyboardButtonRequestPeer")
        and hasattr(raw.types, "MessageActionManagedBotCreated")
        and hasattr(raw.types, "UpdateManagedBot")
        and hasattr(raw.functions.messages, "SendBotRequestedPeer")
    )


async def _get_managed_bot_token(bot_id: int) -> str | None:
    """Retrieves the actual bot token for a just-created managed bot, via
    the HTTP Bot API's getManagedBotToken method — called with THIS bot's
    own token for auth, independent of the MTProto (Pyrogram) connection
    used for everything else in this file, since no separate MTProto
    raw-API method for this was found.

    CAVEAT: the *exact* HTTP parameter name below (user_id) is inferred
    from third-party documentation/reference implementations, not
    directly confirmed against Telegram's own parameter table for this
    specific method — if this starts failing, check the "description"
    field of the returned error first; Telegram's Bot API errors are
    normally specific enough to show the right parameter name to fix
    here. Bots are represented as User objects in Telegram's data model,
    which is why a bot's own id is passed as "user_id" here."""
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/getManagedBotToken"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json={"user_id": bot_id}, timeout=aiohttp.ClientTimeout(total=20)) as resp:
                data = await resp.json()
                if not data.get("ok"):
                    logger.warning(f"getManagedBotToken failed for bot_id={bot_id}: {data.get('description')}")
                    return None
                result = data["result"]
                # Result shape isn't confirmed either — handle both a bare
                # token string and an object with a "token" field.
                return result if isinstance(result, str) else result.get("token")
    except Exception as e:
        logger.warning(f"getManagedBotToken request failed for bot_id={bot_id}: {e}")
        return None


# bot_id -> True, populated right before we intentionally revoke a managed
# bot's token as part of disabling/removing it. Telegram's
# replaceManagedBotToken (used by _revoke_managed_bot_token below) doesn't
# just revoke — it hands back a fresh token for the SAME bot_id, and
# Telegram delivers that as its own "managed bot" event, exactly like a
# brand-new Auto-Create. _handle_managed_bot_created() checks this set
# first (before its DB-based de-dupe check, which only works if the bot
# hasn't been removed from storage yet — and every disable/delbot path
# removes it first) so a bot we just disabled doesn't get silently
# reactivated by its own revoke call's side-effect event.
_DISABLING_BOT_IDS: set[int] = set()


async def _revoke_managed_bot_token(bot_id: int) -> str | None:
    """Invalidates a Managed-Bots-API bot's CURRENT token via the HTTP Bot
    API's replaceManagedBotToken method (Bot API 9.6), so a leaked/old
    token stops working — and returns the FRESH token Telegram generates
    in the same call, so callers that want automatic rotation (not just
    revocation) don't need a second round-trip or a manually pasted
    @BotFather token.

    Only works for source="managed" bots (created via Auto-Create). A
    manually /addbot-ed bot's token comes from @BotFather directly — this
    bot has no API access to revoke that one; only the bot's own
    BotFather /revoke command can regenerate it.

    Same caveat as _get_managed_bot_token: the exact parameter name
    (user_id) and result shape are inferred, not confirmed against
    Telegram's own docs for this specific method — check the error
    "description" first if this starts failing."""
    if not bot_id:
        return None
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/replaceManagedBotToken"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, json={"user_id": bot_id}, timeout=aiohttp.ClientTimeout(total=20)) as resp:
                data = await resp.json()
                if not data.get("ok"):
                    logger.warning(f"replaceManagedBotToken failed for bot_id={bot_id}: {data.get('description')}")
                    return None
                result = data.get("result")
                return result if isinstance(result, str) else (result or {}).get("token")
    except Exception as e:
        logger.warning(f"replaceManagedBotToken request failed for bot_id={bot_id}: {e}")
        return None


def _panel_text(bots) -> str:
    if not bots:
        lines = ["⚡ <b>Titanium Clone Mode</b>", ""]
        lines += [
            "Connect your own @BotFather bot so your downloads run on a "
            "separate flood-limit pool instead of sharing the main bot's "
            "with everyone else.",
            "",
            "Your clone becomes a full personal copy of this bot — reachable "
            "only by you, on your own token — and downloads/streams Diskwala "
            "links exactly like the main bot.",
            "",
            "<code>/addbot &lt;token&gt;</code> — get a token from @BotFather "
            "→ /newbot, then send it here, or use Auto-Create below.",
        ]
        return "\n".join(lines).rstrip()

    # Multi-bot view — a block per connected clone (BOT/SOURCE/STATUS),
    # not just the first one.
    lines = ["⚡ <b>TITANIUM CLONE MODE</b>", ""]
    for b in bots:
        try:
            token = b["token"]
            username = b["username"]
            running = bool(_CLONE_CACHE.get(token) and _CLONE_CACHE[token].is_connected)
            status = "🟢 RUNNING (active)" if running else "🔴 STOPPED"
            src = "🤖 Managed Bots API" if b.get("source") == "managed" else "🔑 Manual (@BotFather)"
            lines.append(f"<b>BOT:</b> @{username}")
            lines.append(f"<b>SOURCE:</b> {src}")
            lines.append(f"<b>STATUS:</b> {status}")
            lines.append("")
        except Exception as e:
            # A single corrupt/incomplete bot entry (e.g. missing
            # "username") shouldn't take down the whole panel — skip it.
            logger.warning(f"Titanium panel text: skipping corrupt bot entry {b!r}: {e}")
            continue
    lines.append(
        "Your clone bot handles downloads independently, preventing "
        "floodwait on the main bot."
    )
    return "\n".join(lines).rstrip()


def _panel_buttons(bots, make_button, BTN_PRIMARY, BTN_DANGER):
    if not bots:
        rows = [[make_button("➕ Add Bot", callback_data="titanium_addbot", style=BTN_PRIMARY)]]
        if _managed_bots_available():
            rows.append([make_button("🤖 Auto-Create Bot", callback_data="titanium_autocreate", style=BTN_PRIMARY)])
        rows.append([
            make_button("⬅️ Back", callback_data="settings_back", style=BTN_PRIMARY),
            make_button("❌ Close", callback_data="settings_close", style=BTN_DANGER),
        ])
        return InlineKeyboardMarkup(rows)

    # Whole-account controls instead of one row per bot: Status/Ping act
    # on every connected clone at once, and Replace/Disable open a
    # per-bot picker only when there's more than one bot to choose from.
    rows = [
        [
            make_button("📊 STATUS", callback_data="titanium_status_all", style=BTN_PRIMARY),
            make_button("🏓 PING", callback_data="titanium_ping", style=BTN_PRIMARY),
        ],
        [
            make_button("➕ ADD BOT", callback_data="titanium_addbot", style=BTN_PRIMARY),
            make_button("🔄 REPLACE BOT", callback_data="titanium_replace_pick", style=BTN_PRIMARY),
        ],
    ]
    if _managed_bots_available():
        rows.append([make_button("🤖 AUTO-CREATE BOT", callback_data="titanium_autocreate", style=BTN_PRIMARY)])
    rows.append([make_button("❌ DISABLE TITANIUM", callback_data="titanium_disable_pick", style=BTN_DANGER)])
    rows.append([make_button("⬅️ BACK", callback_data="settings_back", style=BTN_PRIMARY)])
    return InlineKeyboardMarkup(rows)


def register_titanium_handlers(app, *, make_button, BTN_PRIMARY, BTN_DANGER, SC, ParseMode,
                                link_handler, callback_handler, link_filter, callback_filter,
                                cancel_handler, get_username, smallcaps, start_photo_url,
                                fallback_keyboard, fallback_text, main_menu_kb,
                                powered_by, powered_by_url,
                                menu_message_handlers, menu_callback_handlers,
                                start_handler=None, bot_commands=None):
    """Wires up every /addbot, /delbot and titanium_* callback handler on
    the MAIN app, and stores what boot_titanium_bots()/_get_clone_client()
    need to build clones later. Call this once, after `app` and the
    download handlers exist, and call boot_titanium_bots() at startup
    (after this) to reconnect previously-saved clones."""

    async def clone_start_caption(client, m: Message) -> str:
        """Builds the exact same styled /start caption as the main bot's
        own start_handler — photo caption, feature list and "Powered by"
        footer — just addressed to the clone's own username/name, plus an
        extra "Created & managed by @main_bot" line (clone-only) so users
        can always find their way back to the main bot."""
        display_name = smallcaps(m.from_user.first_name or "there")
        powered_link = f'<a href="{powered_by_url}">{smallcaps(powered_by)}</a>'
        me = await client.get_me()
        bot_username = me.username or ""
        bot_name = me.first_name or "Diskwala Bot"
        start_txt = (
            f"<b>👋 Hello {display_name},</b>\n"
            f"<b>🤖 I am <a href=https://t.me/{bot_username}>{bot_name}</a></b>\n\n"
        )

        # Resolve the MAIN bot's own username (cached at startup by
        # _startup_log() in main.py) so the clone can credit/link back to
        # it. Falls back to a live lookup + cache if that isn't set yet.
        main_username = getattr(app, "_cached_username", None)
        if not main_username:
            try:
                main_me = await app.get_me()
                main_username = main_me.username
                app._cached_username = main_username
            except Exception as e:
                logger.debug(f"clone_start_caption: couldn't resolve main bot username: {e}")
                main_username = None
        managed_by_line = (
            f'\n⚡ Created & managed by <a href="https://t.me/{main_username}">@{main_username}</a>'
            if main_username else ""
        )

        return (
            f"{start_txt}"
            "⚡ ɪ'ᴍ ᴀ ᴠᴇʀʏ ᴘᴏᴡᴇʀꜰᴜʟ ᴅɪꜱᴋᴡᴀʟᴀ ᴅᴏᴡɴʟᴏᴀᴅᴇʀ ʙᴏᴛ.\n\n"
            "📥 ꜱɪᴍᴘʟʏ ꜱᴇɴᴅ ᴍᴇ ᴀɴʏ ᴅɪꜱᴋᴡᴀʟᴀ ᴜʀʟ, ᴀɴᴅ ɪ'ʟʟ ꜰᴇᴛᴄʜ ᴛʜᴇ ᴅɪʀᴇᴄᴛ ᴠɪᴅᴇᴏ ꜰᴏʀ ʏᴏᴜ ɪɴ ꜱᴇᴄᴏɴᴅꜱ.\n\n"
            "🚀 ᴜʟᴛʀᴀ-ꜰᴀꜱᴛ ᴘʀᴏᴄᴇꜱꜱɪɴɢ\n"
            "🎬 ɪɴꜱᴛᴀɴᴛ ᴠɪᴅᴇᴏ ᴇxᴛʀᴀᴄᴛɪᴏɴ\n"
            "⚡ ʟɪɢʜᴛɴɪɴɢ-ꜱᴘᴇᴇᴅ ᴅᴏᴡɴʟᴏᴀᴅꜱ\n"
            "🛡️ ʀᴇʟɪᴀʙʟᴇ & ꜱᴛᴀʙʟᴇ ꜱᴇʀᴠɪᴄᴇ\n"
            "💎 ᴘʀᴇᴍɪᴜᴍ ᴘʟᴀɴꜱ ᴀᴠᴀɪʟᴀʙʟᴇ\n"
            "🔗 ᴊᴜꜱᴛ ᴘᴀꜱᴛᴇ ʏᴏᴜʀ ᴅɪꜱᴋᴡᴀʟᴀ ʟɪɴᴋ ʙᴇʟᴏᴡ ᴀɴᴅ ʟᴇᴛ ᴛʜᴇ ᴍᴀɢɪᴄ ʙᴇɢɪɴ!\n\n"
            "━━━━━━━━━━━━━━━ \n"
            f"👑 ᴘᴏᴡᴇʀᴇᴅ ʙʏ {powered_link}\n"
            "⚡ ꜱᴘᴇᴇᴅ • ᴘᴇʀꜰᴏʀᴍᴀɴᴄᴇ • ʀᴇʟɪᴀʙɪʟɪᴛʏ\n"
            "━━━━━━━━━━━━━━━"
            f"{managed_by_line}"
        )

    _wiring.update(dict(
        app=app,
        link_handler=link_handler, callback_handler=callback_handler,
        link_filter=link_filter, callback_filter=callback_filter,
        cancel_handler=cancel_handler, start_handler=start_handler,
        clone_start_caption=clone_start_caption, SC=SC, ParseMode=ParseMode,
        start_photo_url=start_photo_url, fallback_keyboard=fallback_keyboard,
        fallback_text=fallback_text, main_menu_kb=main_menu_kb,
        menu_message_handlers=menu_message_handlers,
        menu_callback_handlers=menu_callback_handlers,
        bot_commands=bot_commands,
    ))
    # NOTE: link_handler/callback_handler/menu_message_handlers/
    # menu_callback_handlers are kept in _wiring for backward compatibility
    # (main.py still passes them) but are no longer read by
    # _get_clone_client — it now copies ALL of `app`'s registered handlers
    # onto every clone instead of this hand-picked subset, so a clone gets
    # full feature parity with the main bot automatically, including any
    # commands added to main.py later. See _clone_all_app_handlers().

    def panel_buttons(bots):
        return _panel_buttons(bots, make_button, BTN_PRIMARY, BTN_DANGER)

    async def _do_add_bot(client, user_id: int, token: str, status: Message, prompt_msg: Message = None):
        """Shared verify → save → boot pipeline for a pasted/typed bot
        token. Used by /addbot <token>, the interactive /addbot (no args)
        prompt, and the ➕ Add Bot button's prompt. `prompt_msg`, if given,
        is the bot's own "send me your token" bubble — deleted on success
        to keep the chat clean."""
        bots = await get_titanium_bots(user_id)
        if any(b["token"] == token for b in bots):
            return await status.edit_text(SC("ℹ️ That bot is already connected."), parse_mode=ParseMode.HTML)

        try:
            test_client = Client(
                f"titanium_verify_{user_id}_{int(time.time())}",
                api_id=API_ID, api_hash=API_HASH, bot_token=token, in_memory=True,
            )
            await test_client.start()
            me = await test_client.get_me()
            await test_client.stop()
        except Exception as e:
            return await status.edit_text(SC(f"⚠️ <b>Invalid bot token:</b> <code>{e}</code>"), parse_mode=ParseMode.HTML)

        if any(b["username"] == me.username for b in bots):
            return await status.edit_text(SC(f"ℹ️ @{me.username} is already connected."), parse_mode=ParseMode.HTML)

        await status.edit_text(SC("💾 Saving bot..."), parse_mode=ParseMode.HTML)
        await add_titanium_bot(user_id, token, me.username, bot_id=me.id, source="manual")
        clone_ok = False
        try:
            clone = await _get_clone_client(token, user_id)
            clone_ok = bool(clone and clone.is_connected)
        except Exception as e:
            logger.warning(f"addbot: clone start failed for @{me.username}: {e}")

        if prompt_msg is not None:
            try:
                await prompt_msg.delete()
            except Exception:
                pass

        back_btn = InlineKeyboardMarkup([[make_button("⬅️ Titanium", callback_data="titanium_status", style=BTN_PRIMARY)]])
        if clone_ok:
            await status.edit_text(
                SC(
                    f"✅ <b>Bot added successfully!</b>\n\n"
                    f"<b>Username:</b> @{me.username}\n"
                    f"<b>Bot ID:</b> <code>{me.id}</code>\n\n"
                    f"It's now running as your personal Titanium clone."
                ),
                reply_markup=back_btn,
                parse_mode=ParseMode.HTML,
            )
        else:
            await status.edit_text(
                SC(
                    f"⚠️ <b>Bot @{me.username} was saved, but the clone failed "
                    f"to start.</b>\n\nTry /titanium → @{me.username} → 🏓 Ping "
                    f"to retry, or check the logs."
                ),
                reply_markup=back_btn,
                parse_mode=ParseMode.HTML,
            )

    async def _do_replace_bot(client, owner_id: int, old_username: str, new_token: str, status: Message):
        """Verify a pasted replacement token, swap it in for old_username,
        and restart the clone under the new token — the manual-bot
        counterpart to the managed-bot auto-rotate flow in
        _titanium_replace_cb."""
        bots = await get_titanium_bots(owner_id)
        if any(b["token"] == new_token for b in bots):
            return await status.edit_text(SC("ℹ️ That bot is already connected."), parse_mode=ParseMode.HTML)

        try:
            test_client = Client(
                f"titanium_verify_{owner_id}_{int(time.time())}",
                api_id=API_ID, api_hash=API_HASH, bot_token=new_token, in_memory=True,
            )
            await test_client.start()
            me = await test_client.get_me()
            await test_client.stop()
        except Exception as e:
            return await status.edit_text(SC(f"⚠️ <b>Invalid bot token:</b> <code>{e}</code>"), parse_mode=ParseMode.HTML)

        if any(b["username"] == me.username for b in bots if b["username"] != old_username):
            return await status.edit_text(
                SC(f"ℹ️ @{me.username} is already connected as a different bot."), parse_mode=ParseMode.HTML,
            )

        old_match = next((b for b in bots if b["username"] == old_username), None)

        await status.edit_text(SC("🔄 <b>Replacing bot...</b>\n\n🛑 <i>Stopping old clone...</i>"), parse_mode=ParseMode.HTML)
        if old_match:
            await remove_titanium_bot(owner_id, old_username)
            cached = _CLONE_CACHE.pop(old_match["token"], None)
            if cached is not None:
                try:
                    await cached.stop()
                except Exception as e:
                    logger.debug(f"replace: stop() on old clone failed (likely already stopped): {e}")

        await status.edit_text(SC("🔄 <b>Replacing bot...</b>\n\n💾 <i>Saving new bot...</i>"), parse_mode=ParseMode.HTML)
        await add_titanium_bot(owner_id, new_token, me.username, bot_id=me.id, source="manual")

        await status.edit_text(SC("🔄 <b>Replacing bot...</b>\n\n🚀 <i>Starting new clone...</i>"), parse_mode=ParseMode.HTML)
        clone_ok = False
        try:
            clone = await _get_clone_client(new_token, owner_id)
            clone_ok = bool(clone and clone.is_connected)
        except Exception as e:
            logger.warning(f"replace_bot: clone start failed for @{me.username}: {e}")

        back_btn = InlineKeyboardMarkup([[make_button("⬅️ Titanium", callback_data="titanium_status", style=BTN_PRIMARY)]])
        if clone_ok:
            await status.edit_text(
                SC(
                    f"✅ <b>Bot replaced successfully!</b>\n\n"
                    f"<b>Old:</b> @{old_username}\n"
                    f"<b>New:</b> @{me.username}\n\n"
                    f"It's now running as your personal Titanium clone."
                ),
                reply_markup=back_btn,
                parse_mode=ParseMode.HTML,
            )
        else:
            await status.edit_text(
                SC(
                    f"⚠️ <b>@{me.username} was saved, but the clone failed to "
                    f"start.</b> Try /titanium → @{me.username} → 🏓 Ping to retry."
                ),
                reply_markup=back_btn,
                parse_mode=ParseMode.HTML,
            )

    async def _prompt_for_token(client, chat_id: int, owner_id: int):
        """Sends the "send me your token" bubble and marks owner_id as
        pending — checked by the group=-5 catch-all handlers below. The
        same bubble gets reused (edited) for progress/final status once
        the token arrives, instead of leaving it dangling. Auto-expires
        after ADDBOT_TIMEOUT seconds.

        `client` MUST be the Client that actually received the /addbot
        command or ➕ Add Bot tap — this used to be hardcoded to the main
        `app`, which meant a clone bot's /addbot silently sent its "send
        me your token" prompt through the MAIN bot instead (a private
        chat_id is just the user's own Telegram ID, so app.send_message
        happily delivered it — just to the wrong bot's chat). Since this
        handler is copied onto every clone as-is, always use whichever
        client the update actually arrived on.
        """
        ask = await client.send_message(
            chat_id,
            SC(
                "📤 <b>Send me your bot token from @BotFather.</b>\n\n"
                "<b>Example:</b> <code>1234567890:ABCdefGhIJKlmNoPQRsTUVwxyZ</code>\n\n"
                "Send /cancel to abort."
            ),
            parse_mode=ParseMode.HTML,
        )
        _pending_addbot[owner_id] = {"prompt_msg": ask, "expires": time.monotonic() + ADDBOT_TIMEOUT}
        asyncio.create_task(_expire_pending_addbot(owner_id, ask.id))
        return ask

    async def _expire_pending_addbot(owner_id: int, prompt_id: int):
        await asyncio.sleep(ADDBOT_TIMEOUT)
        pending = _pending_addbot.get(owner_id)
        if pending and pending["prompt_msg"].id == prompt_id:
            _pending_addbot.pop(owner_id, None)
            try:
                await pending["prompt_msg"].edit_text(SC("⏳ Timed out — nothing changed."), parse_mode=ParseMode.HTML)
            except Exception:
                pass

    async def _expire_pending_replace(owner_id: int, prompt_id: int):
        await asyncio.sleep(REPLACE_TIMEOUT)
        pending = _pending_replace.get(owner_id)
        if pending and pending["prompt_msg"].id == prompt_id:
            _pending_replace.pop(owner_id, None)
            try:
                await pending["prompt_msg"].edit_text(SC("⏳ Timed out — nothing changed."), parse_mode=ParseMode.HTML)
            except Exception:
                pass

    @app.on_message(filters.command("titanium") & filters.private)
    async def _titanium_cmd(client, m: Message):
        try:
            bots = await get_titanium_bots(m.from_user.id)
            await m.reply(SC(_panel_text(bots)), reply_markup=panel_buttons(bots), parse_mode=ParseMode.HTML)
        except Exception as e:
            logger.warning(f"/titanium command failed for user {m.from_user.id}: {e}")
            try:
                await m.reply(SC("⚠️ Couldn't load Titanium panel, try again."), parse_mode=ParseMode.HTML)
            except Exception:
                pass

    @app.on_callback_query(filters.regex(r"^titanium_status$"))
    async def _titanium_status_cb(client, query: CallbackQuery):
        try:
            bots = await get_titanium_bots(query.from_user.id)
            await query.message.edit_text(
                SC(_panel_text(bots)), reply_markup=panel_buttons(bots), parse_mode=ParseMode.HTML,
            )
            await query.answer()
        except Exception as e:
            if "MESSAGE_NOT_MODIFIED" in str(e):
                await query.answer()
            else:
                logger.warning(f"Titanium panel refresh failed: {e}")
                await query.answer("Couldn't refresh, try again.", show_alert=True)

    @app.on_callback_query(filters.regex(r"^titanium_status_all$"))
    async def _titanium_status_all_cb(client, query: CallbackQuery):
        """📊 Status button on the main panel — same per-bot block as
        titanium_details:, but for every connected bot at once instead of
        one picked by username."""
        await query.answer()
        try:
            owner_id = query.from_user.id
            bots = await get_titanium_bots(owner_id)
            back_btn = InlineKeyboardMarkup([[make_button("⬅️ Titanium", callback_data="titanium_status", style=BTN_PRIMARY)]])
            if not bots:
                return await query.message.edit_text(
                    SC("❌ No bots connected."), reply_markup=back_btn, parse_mode=ParseMode.HTML,
                )
            blocks = []
            for b in bots:
                try:
                    username = b["username"]
                    running = bool(_CLONE_CACHE.get(b["token"]) and _CLONE_CACHE[b["token"]].is_connected)
                    is_managed = b.get("source") == "managed"
                    src = "🤖 Managed Bots API (Bot API 9.6)" if is_managed else "🔑 Manual (@BotFather)"
                    added_raw = b.get("added_at") or "—"
                    added_disp = added_raw.replace("T", " ") if isinstance(added_raw, str) else str(added_raw)
                    last_used = b.get("last_used") or 0
                    last_disp = (
                        datetime.fromtimestamp(last_used).strftime("%Y-%m-%d %H:%M:%S.%f")
                        if last_used else added_disp
                    )
                    blocks.append(
                        f"<b>BOT:</b> @{username}\n"
                        f"<b>SOURCE:</b> {src}\n"
                        f"<b>RUNNING:</b> {'✅ YES' if running else '❌ NO'}\n"
                        "<b>DB STATUS:</b> active\n"
                        f"<b>CREATED:</b> {added_disp}\n"
                        f"<b>LAST VALIDATED:</b> {last_disp}"
                    )
                except Exception as e:
                    # Corrupt entry — skip it, don't blow up the whole
                    # combined status message.
                    logger.warning(f"Titanium status_all: skipping corrupt bot entry {b!r}: {e}")
                    continue
            text = "📊 <b>TITANIUM STATUS</b>\n\n" + "\n\n".join(blocks)
            await query.message.edit_text(SC(text), reply_markup=back_btn, parse_mode=ParseMode.HTML)
        except Exception as e:
            logger.exception("titanium_status_all_cb failed")
            try:
                await query.message.reply(SC(f"⚠️ Couldn't load status: {e}"), parse_mode=ParseMode.HTML)
            except Exception:
                pass

    @app.on_callback_query(filters.regex(r"^titanium_details:"))
    async def _titanium_details_cb(client, query: CallbackQuery):
        await query.answer()
        try:
            owner_id = query.from_user.id
            username = query.data.split(":", 1)[1]
            bots = await get_titanium_bots(owner_id)
            b = next((x for x in bots if x["username"] == username), None)
            back_btn = InlineKeyboardMarkup([[make_button("⬅️ Titanium", callback_data="titanium_status", style=BTN_PRIMARY)]])
            if not b:
                return await query.message.edit_text(
                    SC("❌ That bot is no longer connected."), reply_markup=back_btn, parse_mode=ParseMode.HTML,
                )
            running = bool(_CLONE_CACHE.get(b["token"]) and _CLONE_CACHE[b["token"]].is_connected)
            is_managed = b.get("source") == "managed"
            src = "🤖 Managed Bots API (Bot API 9.6)" if is_managed else "🔑 Manual (@BotFather)"
            added_raw = b.get("added_at") or "—"
            added_disp = added_raw.replace("T", " ") if isinstance(added_raw, str) else str(added_raw)
            last_used = b.get("last_used") or 0
            last_disp = (
                datetime.fromtimestamp(last_used).strftime("%Y-%m-%d %H:%M:%S.%f")
                if last_used else added_disp
            )
            text = (
                "📊 <b>TITANIUM STATUS</b>\n\n"
                f"<b>BOT:</b> @{b['username']}\n"
                f"<b>SOURCE:</b> {src}\n"
                f"<b>RUNNING:</b> {'✅ YES' if running else '❌ NO'}\n"
                "<b>DB STATUS:</b> active\n"
                f"<b>CREATED:</b> {added_disp}\n"
                f"<b>LAST VALIDATED:</b> {last_disp}"
            )
            await query.message.edit_text(SC(text), reply_markup=back_btn, parse_mode=ParseMode.HTML)
        except Exception as e:
            logger.exception("titanium_details_cb failed")
            try:
                await query.message.reply(SC(f"⚠️ Couldn't load status: {e}"), parse_mode=ParseMode.HTML)
            except Exception:
                pass

    @app.on_callback_query(filters.regex(r"^titanium_disable_confirm:"))
    async def _titanium_disable_confirm_cb(client, query: CallbackQuery):
        await query.answer()
        username = query.data.split(":", 1)[1]
        text = (
            f"❌ Disconnect @{username}?\n\n"
            "<i>This bot will stop responding. Your other connected bot(s) "
            "keep running. You can reconnect anytime with /addbot or "
            "Auto-Create.</i>"
        )
        rows = [[
            make_button("✅ Yes, Disable", callback_data=f"titanium_remove:{username}", style=BTN_DANGER),
            make_button("❌ Cancel", callback_data="titanium_status", style=BTN_PRIMARY),
        ]]
        try:
            await query.message.edit_text(SC(text), reply_markup=InlineKeyboardMarkup(rows), parse_mode=ParseMode.HTML)
        except Exception as e:
            logger.warning(f"titanium_disable_confirm edit failed: {e}")

    @app.on_callback_query(filters.regex(r"^titanium_replace_pick$"))
    async def _titanium_replace_pick_cb(client, query: CallbackQuery):
        """🔄 Replace Bot on the main panel — asks which connected bot to
        replace, then hands off to the existing titanium_replace: flow."""
        await query.answer()
        try:
            owner_id = query.from_user.id
            bots = await get_titanium_bots(owner_id)
            if not bots:
                return await query.answer("No bots connected.", show_alert=True)
            rows = [
                [make_button(f"@{b['username']}", callback_data=f"titanium_replace:{b['username']}", style=BTN_PRIMARY)]
                for b in bots if b.get("username")
            ]
            rows.append([make_button("⬅️ BACK", callback_data="titanium_status", style=BTN_PRIMARY)])
            await query.message.edit_text(
                SC("🔄 <b>Which connected bot do you want to replace?</b>"),
                reply_markup=InlineKeyboardMarkup(rows), parse_mode=ParseMode.HTML,
            )
        except Exception as e:
            logger.exception("titanium_replace_pick_cb failed")
            try:
                await query.message.reply(SC(f"⚠️ Couldn't open replace menu: {e}"), parse_mode=ParseMode.HTML)
            except Exception:
                pass

    @app.on_callback_query(filters.regex(r"^titanium_disable_pick$"))
    async def _titanium_disable_pick_cb(client, query: CallbackQuery):
        """❌ Disable Titanium on the main panel — asks which connected
        bot to disable, plus a Disable All shortcut for everything at
        once. Individual picks reuse the existing titanium_disable_confirm:
        flow."""
        await query.answer()
        try:
            owner_id = query.from_user.id
            bots = await get_titanium_bots(owner_id)
            if not bots:
                return await query.answer("No bots connected.", show_alert=True)
            rows = [
                [make_button(f"@{b['username']}", callback_data=f"titanium_disable_confirm:{b['username']}", style=BTN_PRIMARY)]
                for b in bots if b.get("username")
            ]
            rows.append([make_button("❌ DISABLE ALL", callback_data="titanium_disable_all_confirm", style=BTN_DANGER)])
            rows.append([make_button("⬅️ BACK", callback_data="titanium_status", style=BTN_PRIMARY)])
            await query.message.edit_text(
                SC("❌ <b>Which connected bot do you want to disable?</b>"),
                reply_markup=InlineKeyboardMarkup(rows), parse_mode=ParseMode.HTML,
            )
        except Exception as e:
            logger.exception("titanium_disable_pick_cb failed")
            try:
                await query.message.reply(SC(f"⚠️ Couldn't open disable menu: {e}"), parse_mode=ParseMode.HTML)
            except Exception:
                pass

    @app.on_callback_query(filters.regex(r"^titanium_disable_all_confirm$"))
    async def _titanium_disable_all_confirm_cb(client, query: CallbackQuery):
        await query.answer()
        text = (
            "❌ Disconnect ALL your Titanium bots?\n\n"
            "<i>Every connected clone will stop responding. You can "
            "reconnect anytime with /addbot or Auto-Create.</i>"
        )
        rows = [[
            make_button("✅ Yes, Disable All", callback_data="titanium_disable_all", style=BTN_DANGER),
            make_button("❌ Cancel", callback_data="titanium_status", style=BTN_PRIMARY),
        ]]
        try:
            await query.message.edit_text(SC(text), reply_markup=InlineKeyboardMarkup(rows), parse_mode=ParseMode.HTML)
        except Exception as e:
            logger.warning(f"titanium_disable_all_confirm edit failed: {e}")

    @app.on_callback_query(filters.regex(r"^titanium_disable_all$"))
    async def _titanium_disable_all_cb(client, query: CallbackQuery):
        await query.answer()
        owner_id = query.from_user.id
        try:
            bots = await get_titanium_bots(owner_id)
            for b in bots:
                username = b.get("username")
                token = b.get("token")
                try:
                    if username:
                        removed = await remove_titanium_bot(owner_id, username)
                        if not removed:
                            continue
                    if token:
                        cached = _CLONE_CACHE.pop(token, None)
                        if cached is not None and cached is not client:
                            try:
                                await cached.stop()
                            except Exception as e:
                                logger.debug(f"titanium_disable_all: stop() failed for @{username}: {e}")
                    if b.get("source") == "managed" and b.get("bot_id"):
                        try:
                            _DISABLING_BOT_IDS.add(b.get("bot_id"))
                            await _revoke_managed_bot_token(b.get("bot_id"))
                        except Exception as e:
                            logger.warning(f"titanium_disable_all: token revoke failed for @{username}: {e}")
                except Exception as e:
                    # One bot failing to disconnect shouldn't stop the
                    # rest from being cleaned up.
                    logger.warning(f"titanium_disable_all: failed removing @{username}: {e}")
                    continue
            remaining = await get_titanium_bots(owner_id)
            await query.message.edit_text(
                SC(_panel_text(remaining)), reply_markup=panel_buttons(remaining), parse_mode=ParseMode.HTML,
            )
        except Exception as e:
            logger.exception("titanium_disable_all_cb failed")
            try:
                await query.message.reply(SC(f"⚠️ Couldn't disable all bots: {e}"), parse_mode=ParseMode.HTML)
            except Exception:
                pass

    @app.on_callback_query(filters.regex(r"^titanium_addbot$"))
    async def _titanium_addbot_cb(client, query: CallbackQuery):
        await query.answer()
        owner_id = query.from_user.id
        try:
            await _prompt_for_token(client, query.message.chat.id, owner_id)
        except Exception as e:
            logger.exception("titanium_addbot_cb: prompt send failed")
            try:
                await query.message.reply(SC(f"⚠️ Couldn't start add-bot: {e}"), parse_mode=ParseMode.HTML)
            except Exception:
                pass

    # group=-5: early enough to intercept a pending token/cancel before
    # any other text/command handler, but after the -1000 clone owner
    # gate (which only runs on clones, not the main app anyway).
    @app.on_message(filters.private & filters.text & filters.command("cancel"), group=-5)
    async def _titanium_addbot_cancel(client, m: Message):
        owner_id = m.from_user.id
        pending = _pending_addbot.pop(owner_id, None)
        if pending:
            try:
                await pending["prompt_msg"].edit_text(SC("ℹ️ Add-bot cancelled."), parse_mode=ParseMode.HTML)
            except Exception:
                pass
            return
        pending_r = _pending_replace.pop(owner_id, None)
        if pending_r:
            try:
                await pending_r["prompt_msg"].edit_text(SC("ℹ️ Replace-bot cancelled."), parse_mode=ParseMode.HTML)
            except Exception:
                pass
            return
        raise ContinuePropagation

    @app.on_message(
        filters.private & filters.text & ~filters.command(["addbot", "delbot", "titanium", "cancel", "start"]),
        group=-5,
    )
    async def _titanium_addbot_token_catch(client, m: Message):
        owner_id = m.from_user.id
        pending = _pending_addbot.get(owner_id)
        if not pending:
            raise ContinuePropagation
        _pending_addbot.pop(owner_id, None)
        status = pending["prompt_msg"]

        text = (m.text or "").strip()
        match = _TOKEN_RE.search(text)
        if not match:
            await status.edit_text(SC("⚠️ <b>That doesn't look like a valid @BotFather token.</b> Try /addbot again."), parse_mode=ParseMode.HTML)
            return

        await status.edit_text(SC("⏳ Verifying bot token..."), parse_mode=ParseMode.HTML)
        await _do_add_bot(client, owner_id, match.group(0), status, prompt_msg=m)

    @app.on_message(
        filters.private & filters.text & ~filters.command(["addbot", "delbot", "titanium", "cancel", "start"]),
        group=-5,
    )
    async def _titanium_replace_token_catch(client, m: Message):
        owner_id = m.from_user.id
        pending = _pending_replace.get(owner_id)
        if not pending:
            raise ContinuePropagation
        _pending_replace.pop(owner_id, None)
        status = pending["prompt_msg"]
        old_username = pending["old_username"]

        text = (m.text or "").strip()
        match = _TOKEN_RE.search(text)
        if not match:
            await status.edit_text(
                SC("⚠️ <b>That doesn't look like a valid @BotFather token.</b> Try 🔄 Replace Bot again."),
                parse_mode=ParseMode.HTML,
            )
            return

        await status.edit_text(SC("⏳ Verifying new bot token..."), parse_mode=ParseMode.HTML)
        try:
            await m.delete()
        except Exception:
            pass
        await _do_replace_bot(client, owner_id, old_username, match.group(0), status)

    @app.on_callback_query(filters.regex(r"^titanium_autocreate$"))
    async def _titanium_autocreate_cb(client, query: CallbackQuery):
        if not _managed_bots_available():
            return await query.answer(
                "This bot's kurigram build doesn't have Managed Bots support "
                "yet (needs a very recent version). Use /addbot with a "
                "@BotFather token instead for now.",
                show_alert=True,
            )
        await query.answer()
        owner_id = query.from_user.id
        me = await client.get_me()
        suggested_username = f"Diskwala_{owner_id}_{int(time.time()) % 100000}bot"
        deep_link = (
            f"https://t.me/newbot/{me.username}/{suggested_username}"
            f"?name={quote('Diskwala Downloader Bot')}"
        )
        markup = InlineKeyboardMarkup([[
            make_button("🤖 Create my clone bot", url=deep_link, style=BTN_PRIMARY)
        ]])
        try:
            await query.message.reply_text(
                SC(
                    "⚡ <b>Auto-Create Your Titanium Bot</b>\n\n"
                    "Tap the button below. Telegram will show a pre-filled "
                    "name and username for your clone bot — edit them if "
                    "you like, then tap Create.\n\n"
                    "Your bot will be activated automatically — no token "
                    "copying needed.\n\n"
                    "⚠️ Requires \"Bot Management Mode\" enabled for this "
                    "bot in @BotFather (Mini App → Bot Settings). If "
                    "Telegram shows \"CREATE_BOT_BLOCKED\" when you tap "
                    "Create, that setting isn't on yet — enable it and try "
                    "again, or use /addbot instead, which always works."
                ),
                reply_markup=markup,
                parse_mode=ParseMode.HTML,
            )
        except Exception as e:
            logger.warning(f"titanium_autocreate: request-peer button send failed: {e}")
            await query.message.reply_text(
                SC(f"⚠️ <b>Couldn't start auto-create:</b> <code>{e}</code>"),
                parse_mode=ParseMode.HTML,
            )

    async def _handle_managed_bot_created(client, update, users, chats):
        """Raw-update handler for the auto-create flow. Telegram actually
        signals a freshly-created managed bot TWO different ways, and
        which one actually arrives for the https://t.me/newbot/... deep
        link flow isn't reliably documented, so both are handled:

          1. UpdateManagedBot — a top-level raw Update (added alongside
             ManagedBotUpdated/Update.managed_bot in Bot API 9.6) that
             carries user_id (owner) and bot_id (new bot) DIRECTLY, no
             message parsing needed. This is the primary, most reliable
             path — added after discovering the original message-action-
             only listener below was never firing for deep-link-created
             bots, silently dropping every auto-create.
          2. messageActionManagedBotCreated — a service-message action
             (Message.managed_bot_created in Bot API terms). Kept as a
             fallback/secondary path in case a given client/flow variant
             delivers it this way instead; the bot_id-already-processed
             check right below de-dupes if both ever fire for the same
             bot."""
        bot_id = None
        owner_id = None

        if isinstance(update, raw.types.UpdateManagedBot):
            bot_id = update.bot_id
            owner_id = update.user_id
        else:
            upd_msg = getattr(update, "message", None)
            if upd_msg is not None and isinstance(getattr(upd_msg, "action", None), raw.types.MessageActionManagedBotCreated):
                bot_id = upd_msg.action.bot_id
                owner = getattr(upd_msg, "from_id", None)
                owner_id = getattr(owner, "user_id", None) if owner else None
                if owner_id is None:
                    # Fallback: in a private chat the peer_id IS the other party.
                    peer = getattr(upd_msg, "peer_id", None)
                    owner_id = getattr(peer, "user_id", None)

        if bot_id is None:
            return  # not a managed-bot-creation update at all

        if owner_id is None:
            logger.warning(f"managed_bot_created: couldn't determine owner for new bot_id={bot_id}")
            return

        if bot_id in _DISABLING_BOT_IDS:
            # This is the side-effect "new bot" event from our OWN
            # replaceManagedBotToken call during a disable/delbot/remove —
            # not a real Auto-Create. Consume the flag and ignore it,
            # otherwise the bot we just disabled comes right back.
            _DISABLING_BOT_IDS.discard(bot_id)
            return

        bots = await get_titanium_bots(owner_id)
        if any(b.get("bot_id") == bot_id for b in bots):
            return  # already processed (raw updates can be re-delivered)

        status_msg = None
        try:
            status_msg = await client.send_message(
                owner_id,
                SC("⚡ <b>Creating your Titanium bot...</b>\n\n🔗 <i>Fetching token from Telegram...</i>"),
                parse_mode=ParseMode.HTML,
            )
        except Exception:
            status_msg = None

        token = await _get_managed_bot_token(bot_id)
        if not token:
            text = SC(
                "⚠️ <b>Your clone bot was created, but retrieving its token "
                "failed</b> (getManagedBotToken). Try /addbot with a manual "
                "@BotFather token instead."
            )
            try:
                if status_msg is not None:
                    await status_msg.edit_text(text, parse_mode=ParseMode.HTML)
                else:
                    await client.send_message(owner_id, text, parse_mode=ParseMode.HTML)
            except Exception:
                pass
            return

        if status_msg is not None:
            try:
                await status_msg.edit_text(
                    SC("⚡ <b>Creating your Titanium bot...</b>\n\n🔒 <i>Verifying token...</i>"),
                    parse_mode=ParseMode.HTML,
                )
            except Exception:
                pass

        try:
            verify_client = Client(
                f"titanium_verify_{owner_id}_{int(time.time())}",
                api_id=API_ID, api_hash=API_HASH, bot_token=token, in_memory=True,
            )
            await verify_client.start()
            me = await verify_client.get_me()
            await verify_client.stop()
        except Exception as e:
            logger.warning(f"managed_bot_created: token verification failed for bot_id={bot_id}: {e}")
            text = SC(
                "⚠️ <b>Your clone bot was created, but its token failed to "
                "verify.</b>\n\nTry /addbot with a manual @BotFather token instead."
            )
            try:
                if status_msg is not None:
                    await status_msg.edit_text(text, parse_mode=ParseMode.HTML)
                else:
                    await client.send_message(owner_id, text, parse_mode=ParseMode.HTML)
            except Exception:
                pass
            return

        if any(b["token"] == token for b in bots):
            return

        if status_msg is not None:
            try:
                await status_msg.edit_text(
                    SC("⚡ <b>Creating your Titanium bot...</b>\n\n⚙️ <i>Starting clone...</i>"),
                    parse_mode=ParseMode.HTML,
                )
            except Exception:
                pass

        await add_titanium_bot(owner_id, token, me.username, bot_id=me.id, source="managed")
        clone_ok = False
        try:
            clone = await _get_clone_client(token, owner_id)
            clone_ok = bool(clone and clone.is_connected)
        except Exception as e:
            logger.warning(f"managed_bot_created: clone start failed for @{me.username}: {e}")

        if clone_ok:
            final_text = SC(
                f"✅ <b>Titanium clone bot activated!</b>\n\n"
                f"<b>Bot:</b> @{me.username}\n"
                f"<b>Created via:</b> Managed Bots API (Bot API 9.6)\n\n"
                f"It's now running as your personal Titanium clone. Manage it "
                f"from /titanium → your bot."
            )
        else:
            final_text = SC(
                f"⚠️ <b>Bot @{me.username} was created and saved, but the clone "
                f"failed to start.</b>\n\nTry /titanium → @{me.username} → 🏓 Ping "
                f"to retry, or check the logs."
            )
        try:
            if status_msg is not None:
                await status_msg.edit_text(final_text, parse_mode=ParseMode.HTML)
            else:
                await client.send_message(owner_id, final_text, parse_mode=ParseMode.HTML)
        except Exception:
            pass

    if _managed_bots_available():
        app.add_handler(RawUpdateHandler(_handle_managed_bot_created))
        logger.info("Titanium: Managed Bots auto-create handler registered.")
    else:
        logger.info("Titanium: Managed Bots auto-create not available on this kurigram build — /addbot still works.")

    @app.on_callback_query(filters.regex(r"^titanium_ping$"))
    async def _titanium_ping_cb(client, query: CallbackQuery):
        # Answer FIRST, before touching the network. A callback query is
        # only valid for a short window (well under the 15+10s worth of
        # per-bot timeouts _ping_one() can take below), and the previous
        # version only called query.answer() at the very end, after every
        # clone's get_me() (including a cold _get_clone_client() connect
        # for any bot not already cached — a real Telegram handshake, not
        # instant) had finished. On the MAIN bot process — where a clone
        # is much more likely to be cold than when pinging from inside
        # that same already-running clone — that reliably ran past the
        # window, so the final query.answer() itself raised (expired
        # query), got swallowed by the except below, and the button
        # looked completely dead: no popup, no error, nothing. Answering
        # immediately removes that race in every case; results are shown
        # via a message reply instead, since a callback query can only be
        # answered once and the immediate answer above already used it up.
        try:
            await query.answer("🏓 Pinging your bot(s)...")
        except Exception as e:
            logger.debug(f"titanium_ping_cb: initial answer failed (query likely already expired): {e}")

        try:
            owner_id = query.from_user.id
            bots = await get_titanium_bots(owner_id)
            if not bots:
                return await query.message.reply(SC("No bots connected."), parse_mode=ParseMode.HTML)

            async def _ping_one(b):
                start = time.monotonic()
                try:
                    clone = await asyncio.wait_for(_get_clone_client(b["token"], owner_id), timeout=15)
                    await asyncio.wait_for(clone.get_me(), timeout=10)
                    ms = int((time.monotonic() - start) * 1000)
                    return f"🏓 @{b['username']}: {ms}ms"
                except asyncio.TimeoutError:
                    return f"🏓 @{b['username']}: timed out"
                except Exception as e:
                    return f"🏓 @{b['username']}: unreachable ({e})"

            # Concurrent, not sequential — one slow/cold bot no longer
            # delays the ping result for every other bot in the list.
            lines = await asyncio.gather(*(_ping_one(b) for b in bots))
            await query.message.reply(
                SC("🏓 <b>Ping results</b>\n\n" + "\n".join(lines)), parse_mode=ParseMode.HTML,
            )
        except Exception as e:
            logger.exception("titanium_ping_cb failed")
            try:
                await query.message.reply(SC(f"⚠️ Ping failed: {e}"), parse_mode=ParseMode.HTML)
            except Exception:
                pass

    @app.on_callback_query(filters.regex(r"^titanium_view:"))
    async def _titanium_view_cb(client, query: CallbackQuery):
        await query.answer()
        try:
            owner_id = query.from_user.id
            username = query.data.split(":", 1)[1]
            bots = await get_titanium_bots(owner_id)
            b = next((x for x in bots if x["username"] == username), None)
            if not b:
                return await query.message.edit_text(
                    SC("❌ That bot is no longer connected."),
                    reply_markup=InlineKeyboardMarkup([[make_button("⬅️ Back", callback_data="titanium_status", style=BTN_PRIMARY)]]),
                    parse_mode=ParseMode.HTML,
                )
            running = bool(_CLONE_CACHE.get(b["token"]) and _CLONE_CACHE[b["token"]].is_connected)
            status = "🟢 Running" if running else "🔴 Stopped"
            is_managed = b.get("source") == "managed"
            src = "🤖 Managed Bots API" if is_managed else "🔑 Manual (@BotFather)"
            rows = []
            if is_managed:
                rows.append([make_button("🔄 Rotate Token", callback_data=f"titanium_replace:{username}", style=BTN_PRIMARY)])
            rows.append([make_button("❌ Remove", callback_data=f"titanium_remove:{username}", style=BTN_DANGER)])
            rows.append([make_button("⬅️ Back", callback_data="titanium_status", style=BTN_PRIMARY)])
            buttons = InlineKeyboardMarkup(rows)
            last_used = b.get("last_used") or 0
            last_active = (
                datetime.fromtimestamp(last_used).strftime("%d %b %Y, %H:%M UTC")
                if last_used else "Never"
            )
            await query.message.edit_text(
                SC(
                    f"📄 <b>Bot Details</b>\n\n"
                    f"<b>Username:</b> @{b['username']}\n"
                    f"<b>Bot ID:</b> <code>{b.get('bot_id', '—')}</code>\n"
                    f"<b>Status:</b> {status}\n"
                    f"<b>Source:</b> {src}\n"
                    f"<b>Added:</b> {b.get('added_at', '—')}\n"
                    f"<b>Last active:</b> {last_active}"
                ),
                reply_markup=buttons,
                parse_mode=ParseMode.HTML,
            )
        except Exception as e:
            logger.exception("titanium_view_cb failed")
            try:
                await query.message.reply(SC(f"⚠️ Couldn't open bot details: {e}"), parse_mode=ParseMode.HTML)
            except Exception:
                pass

    @app.on_callback_query(filters.regex(r"^titanium_replace:"))
    async def _titanium_replace_cb(client, query: CallbackQuery):
        """Managed (Auto-Create) bots get fully automatic token rotation —
        replaceManagedBotToken kills the old token AND hands back a fresh
        one in the same call. Manual (/addbot) bots have no such API, so
        those fall through to a paste-a-new-token prompt instead (handled
        by _titanium_replace_token_catch above).

        The whole thing is wrapped in one try/except now — previously
        NOTHING in this handler was guarded, so any single failure
        anywhere in the ~10-step rotation (a DB hiccup, a Telegram
        FloodWait, the revoke call, the new token's verify connect, the
        clone restart) killed the handler silently: no popup, no message,
        the button just looked completely dead. That's much more likely
        to surface on the main bot process (heavier load, more concurrent
        clients, more chances for one of those calls to hit a transient
        error) than when poking the same button from inside a single
        already-running clone — which made it look clone-only, but the
        actual bug was "no error handling", not "only works on clones"."""
        try:
            await query.answer()
            owner_id = query.from_user.id
            username = query.data.split(":", 1)[1]
            bots = await get_titanium_bots(owner_id)
            match = next((b for b in bots if b["username"] == username), None)
            if not match:
                return await query.message.reply(SC("ℹ️ Already disconnected."), parse_mode=ParseMode.HTML)

            if match.get("source") != "managed" or not match.get("bot_id"):
                ask = await client.send_message(
                    query.message.chat.id,
                    SC(
                        f"🔄 <b>Replacing @{username}.</b>\n\n"
                        f"Send the new @BotFather token now (paste it as a plain "
                        f"message). You have {REPLACE_TIMEOUT} seconds — send "
                        f"/cancel to abort."
                    ),
                    parse_mode=ParseMode.HTML,
                )
                _pending_replace[owner_id] = {
                    "old_username": username,
                    "prompt_msg": ask,
                    "expires": time.monotonic() + REPLACE_TIMEOUT,
                }
                asyncio.create_task(_expire_pending_replace(owner_id, ask.id))
                return

            status = query.message
            await status.edit_text(
                SC("🔄 <b>Rotating token...</b>\n\n🔑 <i>Revoking old token...</i>"),
                parse_mode=ParseMode.HTML,
            )
            new_token = await _revoke_managed_bot_token(match.get("bot_id"))
            if not new_token:
                return await status.edit_text(
                    SC(
                        "⚠️ <b>Couldn't rotate the token automatically.</b>\n\n"
                        "Telegram's replaceManagedBotToken call failed — try again "
                        "in a bit, or /delbot and /addbot with a manual token instead."
                    ),
                    reply_markup=InlineKeyboardMarkup([[make_button("⬅️ Back", callback_data=f"titanium_details:{username}", style=BTN_PRIMARY)]]),
                    parse_mode=ParseMode.HTML,
                )

            await status.edit_text(
                SC("🔄 <b>Rotating token...</b>\n\n📡 <i>Verifying new token...</i>"),
                parse_mode=ParseMode.HTML,
            )
            try:
                test_client = Client(
                    f"titanium_verify_{owner_id}_{int(time.time())}",
                    api_id=API_ID, api_hash=API_HASH, bot_token=new_token, in_memory=True,
                )
                await test_client.start()
                me = await test_client.get_me()
                await test_client.stop()
            except Exception as e:
                return await status.edit_text(
                    SC(f"⚠️ <b>Got a new token but it didn't verify:</b> <code>{e}</code>"),
                    parse_mode=ParseMode.HTML,
                )

            await remove_titanium_bot(owner_id, username)
            cached = _CLONE_CACHE.pop(match["token"], None)
            if cached is not None:
                try:
                    await cached.stop()
                except Exception as e:
                    logger.debug(f"titanium_replace: stop() on old clone failed (likely already stopped): {e}")

            await add_titanium_bot(owner_id, new_token, me.username, bot_id=me.id, source="managed")

            await status.edit_text(
                SC("🔄 <b>Rotating token...</b>\n\n🚀 <i>Restarting clone...</i>"),
                parse_mode=ParseMode.HTML,
            )
            try:
                await _get_clone_client(new_token, owner_id)
            except Exception as e:
                logger.warning(f"titanium_replace: clone start failed for @{me.username}: {e}")

            await status.edit_text(
                SC(
                    f"✅ <b>TOKEN ROTATED AUTOMATICALLY.</b>\n\n"
                    f"<b>BOT:</b> @{me.username}\n\n"
                    f"The old token was revoked and a new one is active — no "
                    f"manual @BotFather step needed.\n\n"
                    f"Manage it via /settings → ⚡ Titanium Clone Mode."
                ),
                reply_markup=InlineKeyboardMarkup([[make_button("⬅️ Titanium", callback_data="titanium_status", style=BTN_PRIMARY)]]),
                parse_mode=ParseMode.HTML,
            )
        except Exception as e:
            logger.exception("titanium_replace_cb failed")
            try:
                await query.message.reply(SC(f"⚠️ Replace failed: {e}"), parse_mode=ParseMode.HTML)
            except Exception:
                pass


    @app.on_callback_query(filters.regex(r"^titanium_remove:"))
    async def _titanium_remove_cb(client, query: CallbackQuery):
        owner_id = query.from_user.id
        username = query.data.split(":", 1)[1]
        try:
            bots = await get_titanium_bots(owner_id)
            match = next((b for b in bots if b["username"] == username), None)
            removed = await remove_titanium_bot(owner_id, username)
            if not removed:
                return await query.answer("Already disconnected.", show_alert=True)
            await query.answer("Disconnected.")
            if match:
                cached = _CLONE_CACHE.pop(match["token"], None)
                if cached is not None and cached is not client:
                    try:
                        await cached.stop()
                    except Exception as e:
                        logger.debug(f"titanium_remove: stop() failed (likely already stopped): {e}")
                if match.get("source") == "managed" and match.get("bot_id"):
                    try:
                        _DISABLING_BOT_IDS.add(match.get("bot_id"))
                        await _revoke_managed_bot_token(match.get("bot_id"))
                    except Exception as e:
                        logger.warning(f"titanium_remove: token revoke failed for @{username}: {e}")
            remaining = await get_titanium_bots(owner_id)
            await query.message.edit_text(
                SC(_panel_text(remaining)), reply_markup=panel_buttons(remaining), parse_mode=ParseMode.HTML,
            )
        except Exception as e:
            logger.exception("titanium_remove_cb failed")
            try:
                await query.message.reply(SC(f"⚠️ Couldn't remove @{username}: {e}"), parse_mode=ParseMode.HTML)
            except Exception:
                pass

    @app.on_message(filters.command("addbot") & filters.private)
    async def _addbot_cmd(client, m: Message):
        user_id = m.from_user.id
        try:
            if len(m.command) < 2:
                return await _prompt_for_token(client, m.chat.id, user_id)

            token = m.command[1].strip()
            status = await m.reply(SC("⏳ Verifying bot token..."), parse_mode=ParseMode.HTML)
            await _do_add_bot(client, user_id, token, status)
        except Exception as e:
            # Previously unguarded — any failure here (flood-wait, a
            # transient network hiccup, etc.) meant /addbot replied with
            # nothing at all instead of a visible error.
            logger.exception("addbot_cmd failed")
            try:
                await m.reply(SC(f"⚠️ /addbot failed: {e}"), parse_mode=ParseMode.HTML)
            except Exception:
                pass

    @app.on_message(filters.command("delbot") & filters.private)
    async def _delbot_cmd(client, m: Message):
        # Wrapped in try/except like every other titanium_* handler now —
        # this one wasn't, so a single failure anywhere inside (DB error,
        # the managed-bot token-revoke API call, cached.stop() throwing
        # something not already caught) meant the whole handler died
        # silently before ever reaching the final m.reply(), and /delbot
        # looked like it did nothing. That's the "no response on original
        # bot" bug — it just happened not to hit that failure path when
        # tested from the clone.
        user_id = m.from_user.id
        try:
            if len(m.command) < 2:
                return await m.reply(SC("ℹ️ <b>Usage:</b> <code>/delbot username</code>"), parse_mode=ParseMode.HTML)
            username = m.command[1].strip().lstrip("@")
            bots = await get_titanium_bots(user_id)
            match = next((b for b in bots if b["username"] == username), None)
            removed = await remove_titanium_bot(user_id, username)
            if not removed:
                return await m.reply(SC("ℹ️ No connected bot found with that username."), parse_mode=ParseMode.HTML)

            is_self = False
            cached = None
            if match:
                cached = _CLONE_CACHE.pop(match["token"], None)
                # Deleting the very bot this command is currently running
                # on (e.g. /delbot'ing a clone from inside that clone) —
                # stopping it right here, mid-handler, could kill the
                # connection before the confirmation below ever gets sent.
                is_self = cached is not None and cached is client
                if cached is not None and not is_self:
                    try:
                        await cached.stop()
                    except Exception as e:
                        logger.debug(f"delbot: stop() failed (likely already stopped): {e}")
                if match.get("source") == "managed" and match.get("bot_id"):
                    try:
                        _DISABLING_BOT_IDS.add(match.get("bot_id"))
                        await _revoke_managed_bot_token(match.get("bot_id"))
                    except Exception as e:
                        logger.warning(f"delbot: token revoke failed for @{username}: {e}")

            await m.reply(SC(f"✅ Disconnected @{username}."), parse_mode=ParseMode.HTML)

            if is_self:
                async def _self_stop():
                    await asyncio.sleep(1)
                    try:
                        await cached.stop()
                    except Exception as e:
                        logger.debug(f"delbot: self-stop failed: {e}")
                asyncio.create_task(_self_stop())
        except Exception as e:
            logger.exception("delbot_cmd failed")
            try:
                await m.reply(SC(f"⚠️ /delbot failed: {e}"), parse_mode=ParseMode.HTML)
            except Exception:
                pass

    return panel_buttons
