"""
In-memory training lobby flow (V1): presets, queue, temporary channels.
"""

from __future__ import annotations

import asyncio
import dataclasses
import secrets
import time
from typing import Literal

import discord

from . import config


# --- Presets & display names -------------------------------------------------

SLOT_LABELS: dict[str, str] = {
    "keeping_1v1": "Keeping 1v1",
    "shooting_1v1": "Shooting 1v1",
    "crossing": "Crossing",
    "shooting": "Shooting",
    "keeping": "Keeping",
    "blocking": "Blocking",
    "dribbling": "Dribbling",
    "tackling": "Tackling",
}


@dataclasses.dataclass
class Preset:
    key: str
    name: str
    required_keys: tuple[str, ...]
    optional_keys: tuple[str, ...]


PRESETS: dict[str, Preset] = {
    "preset_1": Preset(
        key="preset_1",
        name="🆚 Ones: Keeping 1v1 + Shooting 1v1",
        required_keys=("keeping_1v1", "shooting_1v1"),
        optional_keys=(),
    ),
    "preset_2": Preset(
        key="preset_2",
        name="🪛 Drills: Crossing + Shooting + Blocking (Optional) + Keeping (Optional)",
        required_keys=("crossing", "shooting"),
        optional_keys=("keeping", "blocking"),
    ),
    "preset_3": Preset(
        key="preset_3",
        name="⚔️ Duels: Dribbling + Tackling + Keeping 1v1 (Optional)",
        required_keys=("dribbling", "tackling"),
        optional_keys=("keeping_1v1",),
    ),
}


@dataclasses.dataclass
class LobbySlot:
    key: str
    display_name: str
    user_id: int | None = None


LobbyStatus = Literal["open", "full", "closed", "expired"]

LOBBY_EXPIRE_SECONDS = 15 * 60
VOICE_EMPTY_SECONDS = 60


@dataclasses.dataclass
class TrainingLobby:
    lobby_id: str
    creator_id: int
    preset_key: str
    preset_name: str
    slots: list[LobbySlot]
    original_channel_id: int
    original_message_id: int
    guild_id: int
    temp_text_channel_id: int | None = None
    temp_voice_channel_ids: tuple[int, ...] = ()
    status: LobbyStatus = "open"
    session_closed_auto: bool = False
    # Session message (temp text channel) + delayed close
    session_message_id: int | None = None
    session_channel_id: int | None = None
    close_abort_event: asyncio.Event | None = None
    session_close_task: asyncio.Task | None = None
    expire_task: asyncio.Task | None = None
    voice_empty_task: asyncio.Task | None = None


_lobbies: dict[str, TrainingLobby] = {}
_lock = asyncio.Lock()


def _dual_voice_drills_session(lobby: TrainingLobby) -> bool:
    """Four-player drills (preset 2 with both optional roles) use two parallel voice channels."""
    return lobby.preset_key == "preset_2" and len(lobby.slots) == 4


def _slot_line(slot: LobbySlot) -> str:
    if slot.user_id is None:
        return "Empty"
    return f"<@{slot.user_id}>"


def lobby_status_line(lobby: TrainingLobby) -> str:
    if lobby.status == "open":
        return "🔓 Open — pick a role below"
    if lobby.status == "expired":
        return "❌ Lobby expired due to inactivity."
    if lobby.status == "full":
        return "✅ Lobby ready"
    if lobby.status == "closed":
        if lobby.session_closed_auto:
            return "✅ Session closed automatically."
        return "✅ Session closed."
    return "—"


def _training_ping_content(lobby: TrainingLobby) -> str:
    """Role mentions for slots that are still empty; only keys in TRAINING_PING_ROLES."""
    parts: list[str] = []
    mapping = config.TRAINING_PING_ROLES
    for slot in lobby.slots:
        if slot.user_id is not None:
            continue
        role_id = mapping.get(slot.key.lower())
        if role_id is not None:
            parts.append(f"<@&{role_id}>")
    return " ".join(parts)


def build_lobby_embed(lobby: TrainingLobby) -> discord.Embed:
    creator = f"<@{lobby.creator_id}>"
    embed = discord.Embed(
        title=f"⚽ Training Lobby — {lobby.preset_name}",
        description=(
            f"{creator} created a training lobby. Use **Join** below to claim a remaining role.\n\n"
            f"**Status:** {lobby_status_line(lobby)}"
        ),
        color=0xBE629B,
    )
    for slot in lobby.slots:
        filled = 1 if slot.user_id is not None else 0
        embed.add_field(
            name=f"{slot.display_name} ({filled}/1)",
            value=_slot_line(slot),
            inline=False,
        )
    return embed


def build_session_control_embed(lobby: TrainingLobby) -> discord.Embed:
    participants = [s.user_id for s in lobby.slots if s.user_id is not None]
    mentions = " ".join(f"<@{uid}>" for uid in participants)
    vc_blurb = (
        "Use this text channel and the **two voice channels** for your session (split drills).\n\n"
        "If **nobody is in either voice channel** for **1 minute**, this session closes automatically "
        "(channels removed)."
        if _dual_voice_drills_session(lobby)
        else (
            "Use this text channel and voice channel for your session.\n\n"
            "If nobody is in the voice channel for **1 minute**, this session closes automatically "
            "(channels removed)."
        )
    )
    return discord.Embed(
        title="Training session",
        description=f"✅ Your training lobby is ready.\n\nPlayers:\n{mentions}\n\n{vc_blurb}",
        color=0xBE629B,
    )


def lobby_session_active(lobby: TrainingLobby) -> bool:
    """True while lobby is open for queue changes; False once session is ready (full) or ended."""
    return lobby.status == "open"


def build_lobby_view(lobby_id: str) -> "TrainingLobbyView":
    lobby = _lobbies.get(lobby_id)
    return TrainingLobbyView(lobby_id, lobby)


async def update_lobby_message(bot: discord.Client, lobby: TrainingLobby) -> None:
    channel = bot.get_channel(lobby.original_channel_id)
    if channel is None:
        try:
            guild = bot.get_guild(lobby.guild_id)
            if guild:
                channel = await guild.fetch_channel(lobby.original_channel_id)
        except discord.DiscordException:
            return
    if channel is None or not hasattr(channel, "fetch_message"):
        return
    try:
        message = await channel.fetch_message(lobby.original_message_id)  # type: ignore[union-attr]
        ping = _training_ping_content(lobby) if lobby.status == "open" else ""
        content: str | None = ping if ping else None
        if lobby.status != "open":
            content = None
        am = (
            discord.AllowedMentions(everyone=False, roles=True, users=True)
            if content
            else discord.AllowedMentions.none()
        )
        await message.edit(
            content=content,
            embed=build_lobby_embed(lobby),
            view=build_lobby_view(lobby.lobby_id),
            allowed_mentions=am,
        )
    except discord.DiscordException:
        pass


def user_slot_index(lobby: TrainingLobby, user_id: int) -> int | None:
    for i, s in enumerate(lobby.slots):
        if s.user_id == user_id:
            return i
    return None


def _active_lobby_for_user(user_id: int) -> TrainingLobby | None:
    """
    Lobby this user is still tied to while open/full (creator or slot occupant).
    Call only while holding _lock.
    """
    for lob in _lobbies.values():
        if lob.status not in ("open", "full"):
            continue
        if lob.creator_id == user_id or user_slot_index(lob, user_id) is not None:
            return lob
    return None


def all_slots_filled(lobby: TrainingLobby) -> bool:
    return all(s.user_id is not None for s in lobby.slots)


async def _lobby_expire_after(bot: discord.Client, lobby_id: str) -> None:
    try:
        await asyncio.sleep(LOBBY_EXPIRE_SECONDS)
    except asyncio.CancelledError:
        return
    async with _lock:
        lobby = _lobbies.get(lobby_id)
        if lobby is None or lobby.status != "open":
            return
        lobby.status = "expired"
        lobby.expire_task = None
    await update_lobby_message(bot, lobby)


def _human_voice_members(voice_channel: discord.VoiceChannel) -> int:
    return sum(1 for m in voice_channel.members if not m.bot)


def _session_voice_channels_all_empty(guild: discord.Guild, lobby: TrainingLobby) -> bool:
    if not lobby.temp_voice_channel_ids:
        return False
    for vc_id in lobby.temp_voice_channel_ids:
        ch = guild.get_channel(vc_id)
        if isinstance(ch, discord.VoiceChannel) and _human_voice_members(ch) > 0:
            return False
    return True


async def _voice_empty_delayed_close(bot: discord.Client, lobby_id: str) -> None:
    try:
        await asyncio.sleep(VOICE_EMPTY_SECONDS)
    except asyncio.CancelledError:
        return
    guild_id: int | None = None
    async with _lock:
        lobby = _lobbies.get(lobby_id)
        if lobby is None or lobby.status != "full":
            if lobby is not None:
                lobby.voice_empty_task = None
            return
        guild_id = lobby.guild_id
        lobby.voice_empty_task = None

    guild = bot.get_guild(guild_id) if guild_id else None
    if guild is None:
        await execute_session_close(bot, lobby_id, auto_session_idle=True)
        return
    async with _lock:
        lo = _lobbies.get(lobby_id)
    if lo is None or lo.status != "full":
        return
    if not _session_voice_channels_all_empty(guild, lo):
        return
    await execute_session_close(bot, lobby_id, auto_session_idle=True)


async def _resync_voice_empty_timer(bot: discord.Client, lobby_id: str) -> None:
    async with _lock:
        lobby = _lobbies.get(lobby_id)
        if lobby is None or lobby.status != "full":
            return
        guild = bot.get_guild(lobby.guild_id)
        if guild is None or not lobby.temp_voice_channel_ids:
            return
        if lobby.voice_empty_task is not None and not lobby.voice_empty_task.done():
            lobby.voice_empty_task.cancel()
            lobby.voice_empty_task = None
        if _session_voice_channels_all_empty(guild, lobby):
            lobby.voice_empty_task = asyncio.create_task(_voice_empty_delayed_close(bot, lobby_id))


async def handle_training_voice_state_update(
    member: discord.Member,
    before: discord.VoiceState,
    after: discord.VoiceState,
    bot: discord.Client,
) -> None:
    if member.bot:
        return
    guild = member.guild
    touched: set[int] = set()
    if before.channel is not None:
        touched.add(before.channel.id)
    if after.channel is not None:
        touched.add(after.channel.id)
    if not touched:
        return

    lobby_ids: list[str] = []
    async with _lock:
        for lob in _lobbies.values():
            if lob.guild_id != guild.id or lob.status != "full":
                continue
            vids = lob.temp_voice_channel_ids
            if vids and touched.intersection(vids):
                lobby_ids.append(lob.lobby_id)

    for lid in lobby_ids:
        await _resync_voice_empty_timer(bot, lid)


def _parse_yes_no(value: str) -> bool | None:
    """Return True/False, or None if the user input is invalid."""
    s = (value or "").strip().lower()
    if s == "":
        return True
    if s in ("y", "yes", "1", "true"):
        return True
    if s in ("n", "no", "0", "false"):
        return False
    return None


async def finalize_full_lobby(interaction: discord.Interaction, lobby: TrainingLobby, display: str) -> None:
    guild = interaction.guild
    if guild is None:
        await interaction.response.send_message(
            f"✅ You joined as: **{display}**\n"
            "✅ Lobby ready. Private channels have been created.",
            ephemeral=True,
        )
        return

    me = guild.me
    if me is None:
        await interaction.response.send_message(
            f"✅ You joined as: **{display}**\n"
            "✅ Lobby ready. Private channels have been created.",
            ephemeral=True,
        )
        return

    parent: discord.CategoryChannel | None = None
    ch = guild.get_channel(lobby.original_channel_id)
    if isinstance(ch, discord.TextChannel) and ch.category is not None:
        parent = ch.category

    needed = discord.Permissions(manage_channels=True, manage_roles=True)
    perm_src = parent or guild
    if not perm_src.permissions_for(me).is_superset(needed):
        await interaction.response.send_message(
            f"✅ You joined as: **{display}**\n"
            "✅ Lobby ready. I could not create private channels (missing **Manage Channels** / "
            "**Manage Permissions**). Ask an admin to grant them.",
            ephemeral=True,
        )
        return

    participants = [s.user_id for s in lobby.slots if s.user_id is not None]
    overwrites = _training_channel_permissions(guild, participants)

    safe_tag = lobby.lobby_id[:6].lower()
    text_name = f"training-{safe_tag}-text"
    base_vc_name = f"training-{safe_tag}-vc"
    dual_vc = _dual_voice_drills_session(lobby)

    reason = "Training lobby session"

    text_ch: discord.TextChannel | None = None
    voice_created: list[discord.VoiceChannel] = []

    async def rollback_partial() -> None:
        if text_ch is not None:
            try:
                await text_ch.delete(reason="Training setup failed")
            except discord.DiscordException:
                pass
        for vc in voice_created:
            try:
                await vc.delete(reason="Training setup failed")
            except discord.DiscordException:
                pass

    try:
        text_ch = await guild.create_text_channel(
            text_name,
            category=parent,
            overwrites=overwrites,
            reason=reason,
        )
        if dual_vc:
            voice_created.append(
                await guild.create_voice_channel(
                    f"{base_vc_name}-1",
                    category=parent,
                    overwrites=overwrites,
                    reason=reason,
                )
            )
            voice_created.append(
                await guild.create_voice_channel(
                    f"{base_vc_name}-2",
                    category=parent,
                    overwrites=overwrites,
                    reason=reason,
                )
            )
        else:
            voice_created.append(
                await guild.create_voice_channel(
                    base_vc_name,
                    category=parent,
                    overwrites=overwrites,
                    reason=reason,
                )
            )
    except discord.Forbidden:
        await rollback_partial()
        await interaction.response.send_message(
            f"✅ You joined as: **{display}**\n"
            "✅ Lobby ready. I could not create private channels (missing permissions).",
            ephemeral=True,
        )
        return
    except discord.HTTPException:
        await rollback_partial()
        await interaction.response.send_message(
            f"✅ You joined as: **{display}**\n"
            "✅ Lobby ready. Creating channels failed; try again or contact staff.",
            ephemeral=True,
        )
        return

    lobby.temp_text_channel_id = text_ch.id
    lobby.temp_voice_channel_ids = tuple(vc.id for vc in voice_created)
    await update_lobby_message(interaction.client, lobby)

    mentions = " ".join(f"<@{uid}>" for uid in participants if uid is not None)
    session_embed = build_session_control_embed(lobby)
    view = CloseSessionView(lobby.lobby_id)
    try:
        session_msg = await text_ch.send(
            content=mentions,
            embed=session_embed,
            view=view,
            allowed_mentions=discord.AllowedMentions(users=True),
        )
        lobby.session_message_id = session_msg.id
        lobby.session_channel_id = text_ch.id
    except discord.DiscordException:
        pass

    await _resync_voice_empty_timer(interaction.client, lobby.lobby_id)

    await interaction.response.send_message(
        f"✅ You joined as: **{display}**\n"
        "✅ Lobby ready. Private channels have been created.",
        ephemeral=True,
    )


def _can_close_session(member: discord.Member, lobby: TrainingLobby) -> bool:
    if member.guild_permissions.administrator or member.guild_permissions.manage_guild:
        return True
    return user_slot_index(lobby, member.id) is not None


def _training_channel_permissions(
    guild: discord.Guild,
    participants: list[int],
) -> dict[discord.abc.Snowflake, discord.PermissionOverwrite]:
    overwrites: dict[discord.abc.Snowflake, discord.PermissionOverwrite] = {
        guild.default_role: discord.PermissionOverwrite(view_channel=False),
        guild.me: discord.PermissionOverwrite(
            view_channel=True,
            send_messages=True,
            read_message_history=True,
            manage_channels=True,
            connect=True,
            speak=True,
        ),
    }
    for uid in participants:
        m = guild.get_member(uid)
        target: discord.abc.Snowflake = m if m is not None else discord.Object(id=uid)
        overwrites[target] = discord.PermissionOverwrite(
            view_channel=True,
            send_messages=True,
            read_message_history=True,
            connect=True,
            speak=True,
        )
    return overwrites


async def execute_session_close(
    bot: discord.Client,
    lobby_id: str,
    *,
    auto_session_idle: bool = False,
) -> None:
    async with _lock:
        lobby = _lobbies.get(lobby_id)
        if lobby is None or lobby.status == "closed":
            return
        if lobby.expire_task is not None:
            et = lobby.expire_task
            lobby.expire_task = None
            if not et.done():
                et.cancel()
        cur = asyncio.current_task()
        if lobby.voice_empty_task is not None:
            vt = lobby.voice_empty_task
            lobby.voice_empty_task = None
            if vt is not cur and not vt.done():
                vt.cancel()
        lobby.session_closed_auto = bool(auto_session_idle)
        lobby.status = "closed"
        lobby.close_abort_event = None
        lobby.session_close_task = None
        guild_id = lobby.guild_id
        to_delete = (lobby.temp_text_channel_id,) + lobby.temp_voice_channel_ids
        lobby.temp_text_channel_id = None
        lobby.temp_voice_channel_ids = ()
        lobby.session_message_id = None
        lobby.session_channel_id = None
        saved = lobby

    guild = bot.get_guild(guild_id)
    if guild is not None:
        for cid in to_delete:
            if cid is None:
                continue
            ch = guild.get_channel(cid)
            if ch is None:
                try:
                    ch = await guild.fetch_channel(cid)
                except discord.DiscordException:
                    ch = None
            if ch is not None:
                try:
                    await ch.delete(reason="Training session closed")
                except discord.DiscordException:
                    pass

    await update_lobby_message(bot, saved)


async def _session_close_wait_task(bot: discord.Client, lobby_id: str) -> None:
    async with _lock:
        lo = _lobbies.get(lobby_id)
        event = lo.close_abort_event if lo else None
    if event is None:
        return
    try:
        await asyncio.wait_for(event.wait(), timeout=60.0)
    except asyncio.TimeoutError:
        await execute_session_close(bot, lobby_id)
    else:
        async with _lock:
            lo2 = _lobbies.get(lobby_id)
            if lo2 is not None:
                lo2.close_abort_event = None
                lo2.session_close_task = None


async def handle_take_slot(interaction: discord.Interaction, lobby_id: str, slot_key: str) -> None:
    bot = interaction.client

    lobby: TrainingLobby | None
    became_full = False
    display = ""
    err: str | None = None

    async with _lock:
        lobby = _lobbies.get(lobby_id)
        if lobby is None:
            err = "gone"
        elif lobby.status == "closed":
            err = "closed"
        elif lobby.status == "expired":
            err = "expired"
        elif lobby.status == "full":
            err = "full"
        elif lobby.status != "open":
            err = "closed"
        elif user_slot_index(lobby, interaction.user.id) is not None:
            err = "already"
        else:
            blocker = _active_lobby_for_user(interaction.user.id)
            if blocker is not None and blocker.lobby_id != lobby_id:
                err = "other"
            else:
                slot = next((s for s in lobby.slots if s.key == slot_key), None)
                if slot is None or slot.user_id is not None:
                    err = "slot"
                else:
                    slot.user_id = interaction.user.id
                    display = slot.display_name
                    became_full = all_slots_filled(lobby)
                    if became_full:
                        lobby.status = "full"
                        if lobby.expire_task and not lobby.expire_task.done():
                            lobby.expire_task.cancel()
                        lobby.expire_task = None

    if err == "expired":
        await safe_reply(interaction, "This training lobby has expired.", ephemeral=True)
        return
    if err == "gone":
        await safe_reply(interaction, "This training lobby is no longer available.", ephemeral=True)
        return
    if err == "closed":
        await safe_reply(interaction, "This training session has ended.", ephemeral=True)
        return
    if err == "full":
        await safe_reply(interaction, "This training lobby is already full.", ephemeral=True)
        return
    if err == "already":
        await safe_reply(
            interaction,
            "You are already in this training lobby. Leave first if you want to switch roles.",
            ephemeral=True,
        )
        return
    if err == "other":
        await safe_reply(
            interaction,
            "You're already in another training lobby. Leave it or wait for it to end before joining this one.",
            ephemeral=True,
        )
        return
    if err == "slot":
        await safe_reply(interaction, "That slot was just filled. Try again.", ephemeral=True)
        return

    if lobby is None:
        return

    await update_lobby_message(bot, lobby)

    if became_full:
        await finalize_full_lobby(interaction, lobby, display)
    else:
        await interaction.response.send_message(
            f"✅ You joined as: **{display}**",
            ephemeral=True,
        )


async def safe_reply(
    interaction: discord.Interaction,
    content: str,
    *,
    ephemeral: bool = True,
) -> None:
    try:
        if interaction.response.is_done():
            await interaction.followup.send(content, ephemeral=ephemeral)
        else:
            await interaction.response.send_message(content, ephemeral=ephemeral)
    except discord.NotFound:
        pass
    except discord.HTTPException:
        try:
            await interaction.followup.send(content, ephemeral=ephemeral)
        except discord.DiscordException:
            pass


# --- Views: Training Hub -----------------------------------------------------


class TrainingHubView(discord.ui.View):
    def __init__(self) -> None:
        super().__init__(timeout=None)

    @discord.ui.button(
        label="Create Lobby",
        style=discord.ButtonStyle.primary,
        custom_id="rematchhq:training:hub:create",
    )
    async def create_training_lobby(self, interaction: discord.Interaction, _: discord.ui.Button):
        if not interaction.guild or not interaction.channel:
            await safe_reply(interaction, "Run this in the server.", ephemeral=True)
            return

        if not isinstance(interaction.channel, discord.TextChannel):
            await safe_reply(interaction, "Use this in a text channel.", ephemeral=True)
            return

        async with _lock:
            blocked = _active_lobby_for_user(interaction.user.id) is not None
        if blocked:
            await safe_reply(
                interaction,
                "You already have an active training lobby or are in one. Finish it or leave before starting another.",
                ephemeral=True,
            )
            return

        embed = discord.Embed(
            title="Choose a training preset",
            description="Pick one of the presets below.",
            color=0xBE629B,
        )
        await interaction.response.send_message(
            embed=embed,
            view=PresetPickerView(),
            ephemeral=True,
        )


async def _fetch_training_posts_channel(guild: discord.Guild, channel_id: int) -> discord.TextChannel | None:
    ch = guild.get_channel(channel_id)
    if isinstance(ch, discord.TextChannel):
        return ch
    try:
        fetched = await guild.fetch_channel(channel_id)
    except discord.DiscordException:
        return None
    return fetched if isinstance(fetched, discord.TextChannel) else None


# --- Creation flow -----------------------------------------------------------


async def create_training_lobby_post(
    interaction: discord.Interaction,
    preset_key: str,
    included_optional: list[str],
    *,
    creator_slot_key: str,
) -> str | None:
    """
    Create the public lobby message after the host picks their role privately.
    The chosen slot is pre-filled before the embed posts.
    Returns None on success, or a short error message on failure.
    """
    if not interaction.guild or not interaction.channel:
        return "Run this in the server."
    if not isinstance(interaction.channel, discord.TextChannel):
        return "Use this in a text channel."

    post_cid = config.training_pings_channel_id_for_guild(interaction.guild.id)
    if post_cid is None:
        return "Add `TRAINING_PINGS_CHANNEL_ID` for this server in config.yaml (or set the `TRAINING_PINGS_CHANNEL_ID` environment variable)."

    post_channel = await _fetch_training_posts_channel(interaction.guild, post_cid)
    if post_channel is None:
        return "Could not access the training lobbies channel. Check `TRAINING_PINGS_CHANNEL_ID` and that the bot can post there."

    preset = PRESETS[preset_key]
    slot_keys_ordered = list(preset.required_keys) + [k for k in preset.optional_keys if k in included_optional]
    if creator_slot_key not in slot_keys_ordered:
        return "That role isn’t available for this lobby configuration."

    slots = [LobbySlot(key=k, display_name=SLOT_LABELS[k]) for k in slot_keys_ordered]
    for s in slots:
        if s.key == creator_slot_key:
            s.user_id = interaction.user.id
            break

    lobby_id = secrets.token_hex(6)
    lobby = TrainingLobby(
        lobby_id=lobby_id,
        creator_id=interaction.user.id,
        preset_key=preset_key,
        preset_name=preset.name,
        slots=slots,
        original_channel_id=0,
        original_message_id=0,
        guild_id=interaction.guild.id,
    )

    async with _lock:
        if _active_lobby_for_user(interaction.user.id) is not None:
            return "You already have an active training lobby or are in one. Finish it or leave before creating another."
        _lobbies[lobby_id] = lobby

    embed = build_lobby_embed(lobby)
    view = build_lobby_view(lobby_id)
    ping = _training_ping_content(lobby)
    am = (
        discord.AllowedMentions(everyone=False, roles=True, users=True)
        if ping
        else discord.AllowedMentions.none()
    )
    bot = interaction.client
    try:
        msg = await post_channel.send(
            content=ping if ping else None,
            embed=embed,
            view=view,
            allowed_mentions=am,
        )
    except discord.HTTPException:
        try:
            msg = await post_channel.send(embed=embed, view=view)
        except (discord.Forbidden, discord.HTTPException):
            async with _lock:
                _lobbies.pop(lobby_id, None)
            return "I need permission to send messages in this channel (or to mention training roles)."

    lobby.original_channel_id = msg.channel.id
    lobby.original_message_id = msg.id
    lobby.expire_task = asyncio.create_task(_lobby_expire_after(bot, lobby_id))
    return None


def _preset_options() -> list[discord.SelectOption]:
    return [
        discord.SelectOption(label=p.name, value=p.key, description=None) for p in PRESETS.values()
    ]


def _creator_role_pick_embed(preset_key: str) -> discord.Embed:
    preset = PRESETS[preset_key]
    return discord.Embed(
        title="Your role",
        description=(
            f"**{preset.name}**\n\n"
            "Choose **your role** in this lobby. The public recruiting message posts after you pick "
            "(others only see roles that are still open)."
        ),
        color=0xBE629B,
    )


class CreatorRoleSelect(discord.ui.Select):
    def __init__(self, preset_key: str, included_optional: list[str]) -> None:
        preset = PRESETS[preset_key]
        ordered = list(preset.required_keys) + [k for k in preset.optional_keys if k in included_optional]
        opts: list[discord.SelectOption] = []
        for k in ordered[:25]:
            label = SLOT_LABELS[k]
            if len(label) > 100:
                label = label[:97] + "…"
            opts.append(discord.SelectOption(label=label, value=k))
        super().__init__(
            placeholder="Select your role…",
            min_values=1,
            max_values=1,
            options=opts,
        )
        self.preset_key = preset_key
        self.included_optional = included_optional.copy()

    async def callback(self, interaction: discord.Interaction) -> None:
        role_key = self.values[0]
        await interaction.response.defer(ephemeral=True)
        err = await create_training_lobby_post(
            interaction,
            self.preset_key,
            self.included_optional,
            creator_slot_key=role_key,
        )
        if err:
            await interaction.followup.send(err, ephemeral=True)
            return
        await interaction.edit_original_response(content="✅ Training lobby created.", embed=None, view=None)


class CreatorRolePickView(discord.ui.View):
    def __init__(self, preset_key: str, included_optional: list[str]) -> None:
        super().__init__(timeout=600)
        self.add_item(CreatorRoleSelect(preset_key, included_optional))


class OptionalSlotsModal(discord.ui.Modal):
    def __init__(self, preset_key: str) -> None:
        preset = PRESETS[preset_key]
        super().__init__(title="Optional slots — Yes or No")
        self.preset_key = preset_key
        for key in preset.optional_keys:
            label = f"{SLOT_LABELS[key]} — Yes or No"
            if len(label) > 45:
                label = label[:42] + "…"
            self.add_item(
                discord.ui.TextInput(
                    label=label,
                    placeholder="Yes (default) = include | No = skip",
                    style=discord.TextStyle.short,
                    required=False,
                    max_length=12,
                )
            )

    async def on_submit(self, interaction: discord.Interaction) -> None:
        preset = PRESETS[self.preset_key]
        optional_keys = list(preset.optional_keys)
        text_inputs = [c for c in self.children if isinstance(c, discord.ui.TextInput)]
        included: list[str] = []
        for key, field in zip(optional_keys, text_inputs):
            parsed = _parse_yes_no(field.value)
            if parsed is None:
                await interaction.response.send_message(
                    f"For **{SLOT_LABELS[key]}**, type **Yes** or **No** only.",
                    ephemeral=True,
                )
                return
            if parsed:
                included.append(key)

        await interaction.response.defer(ephemeral=True)
        embed = _creator_role_pick_embed(self.preset_key)
        try:
            await interaction.followup.send(embed=embed, view=CreatorRolePickView(self.preset_key, included), ephemeral=True)
        except discord.HTTPException:
            await interaction.followup.send(
                "Could not show role picker. Try **Create Lobby** again.",
                ephemeral=True,
            )
            return


class PresetPickerView(discord.ui.View):
    def __init__(self) -> None:
        super().__init__(timeout=600)
        self.add_item(PresetSelect())


class PresetSelect(discord.ui.Select):
    def __init__(self) -> None:
        super().__init__(
            placeholder="Select a training preset…",
            min_values=1,
            max_values=1,
            options=_preset_options(),
        )

    async def callback(self, interaction: discord.Interaction):
        preset_key = self.values[0]
        preset = PRESETS[preset_key]
        if preset.optional_keys:
            await interaction.response.send_modal(OptionalSlotsModal(preset_key))
            return

        embed = _creator_role_pick_embed(preset_key)
        await interaction.response.edit_message(embed=embed, view=CreatorRolePickView(preset_key, []))


# --- Lobby join / leave ------------------------------------------------------


def _make_take_slot_handler(lobby_id: str, slot_key: str):
    async def handler(interaction: discord.Interaction) -> None:
        await handle_take_slot(interaction, lobby_id, slot_key)

    return handler


class TrainingLobbyView(discord.ui.View):
    def __init__(self, lobby_id: str, lobby: TrainingLobby | None) -> None:
        super().__init__(timeout=None)
        self.lobby_id = lobby_id
        queue_open = lobby is not None and lobby_session_active(lobby)

        slot_idx = 0
        if queue_open and lobby is not None:
            for slot in lobby.slots:
                if slot.user_id is not None:
                    continue
                row = min(slot_idx // 5, 3)
                label = slot.display_name
                if len(label) > 64:
                    label = label[:61] + "…"
                btn = discord.ui.Button(
                    label=f"Join: {label}"[:80],
                    style=discord.ButtonStyle.secondary,
                    row=row,
                )
                btn.callback = _make_take_slot_handler(lobby_id, slot.key)
                self.add_item(btn)
                slot_idx += 1

        leave_row = min((slot_idx + 4) // 5, 4)
        leave = discord.ui.Button(
            label="❌ Leave Queue",
            style=discord.ButtonStyle.danger,
            custom_id=f"rematchhq:training:lobby:leave:{lobby_id}",
            disabled=not queue_open,
            row=leave_row,
        )
        leave.callback = self._leave
        self.add_item(leave)

    async def _leave(self, interaction: discord.Interaction):
        bot = interaction.client
        async with _lock:
            lobby = _lobbies.get(self.lobby_id)

        if lobby is None:
            await safe_reply(interaction, "This training lobby is no longer available.", ephemeral=True)
            return

        idx = user_slot_index(lobby, interaction.user.id)
        if idx is None:
            await safe_reply(interaction, "You are not currently in this training lobby.", ephemeral=True)
            return

        lobby.slots[idx].user_id = None
        await update_lobby_message(bot, lobby)
        await safe_reply(interaction, "✅ You have left the training lobby.", ephemeral=True)


class CloseSessionView(discord.ui.View):
    def __init__(self, lobby_id: str) -> None:
        super().__init__(timeout=None)
        self.lobby_id = lobby_id
        btn = discord.ui.Button(
            label="🔒 Close Session",
            style=discord.ButtonStyle.danger,
            custom_id=f"rematchhq:training:session:close:{lobby_id}",
        )
        btn.callback = self._close
        self.add_item(btn)

    async def _close(self, interaction: discord.Interaction):
        if not interaction.guild or not isinstance(interaction.user, discord.Member):
            await safe_reply(interaction, "Run this inside the server.", ephemeral=True)
            return

        close_err: str | None = None
        bot = interaction.client
        async with _lock:
            lobby = _lobbies.get(self.lobby_id)
            if lobby is None or lobby.status == "closed":
                close_err = "gone"
            elif not _can_close_session(interaction.user, lobby):
                close_err = "perms"
            elif lobby.session_close_task is not None and not lobby.session_close_task.done():
                close_err = "pending"
            else:
                lobby.close_abort_event = asyncio.Event()
                lobby.session_close_task = asyncio.create_task(
                    _session_close_wait_task(bot, self.lobby_id)
                )

        if close_err == "gone":
            await safe_reply(interaction, "This session is already closed.", ephemeral=True)
            return
        if close_err == "perms":
            await safe_reply(interaction, "Only participants or moderators can close this session.", ephemeral=True)
            return
        if close_err == "pending":
            await safe_reply(
                interaction,
                "A close is already scheduled. Use **Cancel** on the session message or wait for it to finish.",
                ephemeral=True,
            )
            return

        closes_at = int(time.time()) + 60
        lobby_for_embed = _lobbies.get(self.lobby_id)
        delete_detail = (
            "The private **text** channel and **two voice** channels will be deleted."
            if lobby_for_embed is not None and _dual_voice_drills_session(lobby_for_embed)
            else "The private **text** channel and **voice** channel will be deleted."
        )
        embed = discord.Embed(
            title="Closing session",
            description=(
                f"This session will close **<t:{closes_at}:R>**.\n"
                f"{delete_detail}\n\n"
                "Tap **Cancel** if you clicked by mistake."
            ),
            color=0xBE629B,
        )
        view = CloseSessionPendingView(self.lobby_id)
        await interaction.response.edit_message(embed=embed, view=view)


class CloseSessionPendingView(discord.ui.View):
    def __init__(self, lobby_id: str) -> None:
        super().__init__(timeout=None)
        self.lobby_id = lobby_id
        cancel_btn = discord.ui.Button(
            label="Cancel",
            style=discord.ButtonStyle.secondary,
            custom_id=f"rematchhq:training:session:cancel:{lobby_id}",
        )
        cancel_btn.callback = self._cancel
        self.add_item(cancel_btn)

    async def _cancel(self, interaction: discord.Interaction):
        if not isinstance(interaction.user, discord.Member):
            await safe_reply(interaction, "Run this inside the server.", ephemeral=True)
            return

        allowed = False
        async with _lock:
            lobby = _lobbies.get(self.lobby_id)
            if lobby is not None and _can_close_session(interaction.user, lobby):
                allowed = True
                if lobby.close_abort_event is not None:
                    lobby.close_abort_event.set()

        if not allowed:
            await safe_reply(interaction, "You cannot cancel this close.", ephemeral=True)
            return

        lobby = _lobbies.get(self.lobby_id)
        embed = build_session_control_embed(lobby) if lobby else discord.Embed(
            title="Training session",
            description="Session control restored.",
            color=0xBE629B,
        )
        await interaction.response.edit_message(embed=embed, view=CloseSessionView(self.lobby_id))

