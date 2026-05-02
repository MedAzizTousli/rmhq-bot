try:
    import asyncio
    import random
    from datetime import datetime, timedelta
    from zoneinfo import ZoneInfo

    import discord
    from discord import app_commands
    from discord.ext import commands
except ImportError as e:
    raise SystemExit(
        "Missing dependency: discord.py\n"
        "Install it with: pip install -r requirements.txt"
    ) from e

if not hasattr(discord, "app_commands"):
    raise SystemExit(
        "Slash commands require discord.py v2.x.\n"
        "Fix with:\n"
        "  pip uninstall -y discord\n"
        "  pip install -U discord.py"
    )

from . import birthdays, config, giveaways
from . import emergency_subs
from .views import BirthdaySetupView, EmergencyPlayersView, EmergencyTeamsView, GiveawayEntryView, SetupPartView, SetupView


class RematchHQBot(commands.Bot):
    def __init__(self):
        intents = discord.Intents.default()
        intents.message_content = True
        intents.members = True
        super().__init__(command_prefix="!", intents=intents)
        self._emergency_reset_task: asyncio.Task | None = None
        self._birthday_announcement_task: asyncio.Task | None = None
        self._giveaway_task: asyncio.Task | None = None
        self._birthday_role_lock = asyncio.Lock()

    async def setup_hook(self):
        self.add_view(SetupView())
        self.add_view(SetupPartView())
        self.add_view(BirthdaySetupView())
        self.add_view(EmergencyPlayersView())
        self.add_view(EmergencyTeamsView())
        self._emergency_reset_task = asyncio.create_task(self._emergency_midnight_reset_loop())
        self._birthday_announcement_task = asyncio.create_task(self._birthday_announcement_loop())
        self._giveaway_task = asyncio.create_task(self._giveaway_loop())

        if not config.SYNC_COMMANDS_ON_STARTUP:
            print("Skipping slash command sync (SYNC_COMMANDS_ON_STARTUP=0).")
            return

        await self._sync_guild_commands()
        await self._clear_remote_global_commands()

    async def _application_id(self) -> int:
        if self.application_id is not None:
            return int(self.application_id)
        info = await self.application_info()
        return int(info.id)

    async def _clear_remote_global_commands(self) -> None:
        if not config.GUILD_IDS:
            return

        app_id = await self._application_id()
        cleared = await self.http.bulk_upsert_global_commands(app_id, payload=[])
        print(
            "Cleared remote global slash commands "
            f"for application {app_id}; {len(cleared)} global command(s) remain."
        )

    async def _sync_guild_commands(self) -> dict[int, list[app_commands.AppCommand]]:
        if not config.GUILD_IDS:
            print(
                "Skipping slash command sync: set DISCORD_GUILD_ID or GUILD_ID for instant guild command updates."
            )
            return {}

        synced_by_guild: dict[int, list[app_commands.AppCommand]] = {}
        for guild_id in config.GUILD_IDS:
            guild = discord.Object(id=guild_id)

            # Commands are registered once locally, then copied into each configured guild
            # before syncing so restarts make updates appear immediately in every server.
            self.tree.copy_global_to(guild=guild)
            registered = self.tree.get_commands(guild=guild)
            print(
                "Syncing guild slash commands to "
                f"{guild_id}: {', '.join(cmd.name for cmd in registered) or '(none)'}"
            )
            synced = await self.tree.sync(guild=guild)
            synced_by_guild[guild_id] = synced
            print(
                "Synced "
                f"{len(synced)} guild command(s) to {guild_id}: "
                f"{', '.join(cmd.name for cmd in synced) or '(none)'}"
            )
        return synced_by_guild

    async def close(self):
        if self._emergency_reset_task is not None:
            self._emergency_reset_task.cancel()
        if self._birthday_announcement_task is not None:
            self._birthday_announcement_task.cancel()
        if self._giveaway_task is not None:
            self._giveaway_task.cancel()
        await super().close()

    async def _emergency_midnight_reset_loop(self) -> None:
        await self.wait_until_ready()
        tz = ZoneInfo("Europe/Paris")
        while not self.is_closed():
            now = datetime.now(tz)
            next_midnight = datetime(now.year, now.month, now.day, tzinfo=tz) + timedelta(days=1)
            await asyncio.sleep(max(1.0, (next_midnight - now).total_seconds()))
            try:
                await self._reset_emergency_roles_and_rows()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                print("Emergency midnight reset failed:", repr(e))

    async def _reset_emergency_roles_and_rows(self) -> None:
        role_ids = set(config.EMERGENCY_SUBS_ROLES.values())
        if not role_ids:
            await emergency_subs.clearAllEmergencyRows()
            print("Emergency midnight reset: cleared DB rows; no emergency roles configured.")
            return

        removed = 0
        for guild in self.guilds:
            roles = [role for role_id in role_ids if (role := guild.get_role(int(role_id))) is not None]
            if not roles:
                continue

            try:
                members = [member async for member in guild.fetch_members(limit=None)]
            except discord.DiscordException:
                members = list(guild.members)

            for member in members:
                member_roles = [role for role in roles if role in getattr(member, "roles", [])]
                if not member_roles:
                    continue
                try:
                    await member.remove_roles(*member_roles, reason="Emergency sub daily reset")
                    removed += len(member_roles)
                except discord.Forbidden:
                    print(
                        "Emergency midnight reset: missing permission to remove emergency roles "
                        f"from {member} in {guild.name}."
                    )
                except discord.HTTPException as e:
                    print(
                        "Emergency midnight reset: failed to remove emergency roles "
                        f"from {member} in {guild.name}: {e!r}"
                    )

        await emergency_subs.clearAllEmergencyRows()
        print(f"Emergency midnight reset: removed {removed} role assignment(s) and cleared DB rows.")

    async def _birthday_announcement_loop(self) -> None:
        await self.wait_until_ready()
        tz = ZoneInfo("Europe/Paris")
        startup_now = datetime.now(tz)
        if startup_now.hour == 0 and startup_now.minute < 10:
            try:
                await self.send_birthday_announcements_for_today(force=False)
            except Exception as e:
                print("Birthday startup announcement check failed:", repr(e))

        while not self.is_closed():
            now = datetime.now(tz)
            next_midnight = datetime(now.year, now.month, now.day, tzinfo=tz) + timedelta(days=1)
            await asyncio.sleep(max(1.0, (next_midnight - now).total_seconds()))
            try:
                await self.send_birthday_announcements_for_today(force=False)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                print("Birthday announcement check failed:", repr(e))

    async def _clear_birthday_role_for_guild(self, guild: discord.Guild) -> None:
        if config.BIRTHDAYS_ROLE_ID is None:
            print("Birthday role clear skipped: missing BIRTHDAYS_ROLE_ID.")
            return

        role = guild.get_role(config.BIRTHDAYS_ROLE_ID)
        if role is None:
            print(f"Birthday role clear skipped: role {config.BIRTHDAYS_ROLE_ID} was not found in {guild.name}.")
            return

        async with self._birthday_role_lock:
            removed = 0
            try:
                members = [member async for member in guild.fetch_members(limit=None)]
            except discord.DiscordException:
                members = list(guild.members)

            for member in members:
                if role not in getattr(member, "roles", []):
                    continue
                try:
                    await member.remove_roles(role, reason="Birthday day expired")
                    removed += 1
                except discord.Forbidden:
                    print(
                        "Birthday role cleanup: missing permissions or role hierarchy prevents removing "
                        f"role {role.id} from user {member.id}."
                    )
                except discord.HTTPException as e:
                    print(
                        "Birthday role cleanup: Discord API error removing "
                        f"role {role.id} from user {member.id}: {e!r}"
                    )
            await birthdays.clear_role_assignments_for_guild_role(guild.id, role.id)
            print(f"Birthday role cleanup: removed {removed} expired role assignment(s) in {guild.name}.")

    async def send_birthday_announcements_for_today(self, *, force: bool = False) -> str:
        if config.BIRTHDAYS_CHANNEL_ID is None:
            msg = "Birthday announcement skipped: missing BIRTHDAYS_CHANNEL_ID."
            print(msg)
            return msg

        tz = ZoneInfo("Europe/Paris")
        today = datetime.now(tz).date()
        date_text = today.isoformat()

        channel = self.get_channel(config.BIRTHDAYS_CHANNEL_ID)
        if channel is None:
            channel = await self.fetch_channel(config.BIRTHDAYS_CHANNEL_ID)
        if not hasattr(channel, "send"):
            raise RuntimeError(f"Configured birthday channel is not sendable: {config.BIRTHDAYS_CHANNEL_ID}")
        guild = getattr(channel, "guild", None)
        if guild is None:
            raise RuntimeError("Configured birthday channel is not in a server.")

        todays_birthdays = await birthdays.birthdays_for(today.day, today.month)
        print(f"Birthday announcement check for {date_text}: {len(todays_birthdays)} birthday row(s) found.")
        if not todays_birthdays:
            await self._clear_birthday_role_for_guild(guild)
            msg = "Birthday announcement skipped: no birthdays today."
            print(msg)
            return msg

        birthday_members: list[discord.Member] = []
        removed_user_ids: list[str] = []
        for birthday in todays_birthdays:
            member = guild.get_member(int(birthday.user_id))
            if member is None:
                try:
                    member = await guild.fetch_member(int(birthday.user_id))
                except discord.NotFound:
                    member = None
                except discord.HTTPException as e:
                    print(f"Birthday announcement: failed to fetch member {birthday.user_id}: {e!r}")
                    member = None

            if member is None:
                removed_user_ids.append(birthday.user_id)
            else:
                birthday_members.append(member)

        if removed_user_ids:
            removed_count = await birthdays.delete_birthdays(removed_user_ids)
            print(
                "Birthday announcement: removed users who left server "
                f"({removed_count} row(s)): {', '.join(removed_user_ids)}"
            )

        if not birthday_members:
            await self._clear_birthday_role_for_guild(guild)
            msg = "Birthday announcement skipped: no birthday users are still in the server."
            print(msg)
            return msg

        if not force and not await birthdays.claim_announcement_date(date_text):
            msg = f"Birthday announcement skipped: already sent for {date_text}."
            print(msg)
            return msg

        try:
            await self._clear_birthday_role_for_guild(guild)
            await self._assign_birthday_roles(guild, birthday_members)
            user_ids = [str(member.id) for member in birthday_members]
            mentions = " ".join(f"<@{user_id}>" for user_id in user_ids)
            message = await channel.send(
                "# <:RMHQ:1474023595717165076> RMHQ — Happy Birthday!\n\n"
                f"Today we’re celebrating {mentions} 🥳\n"
                "Wishing you an amazing birthday filled with good vibes, great games, and unforgettable moments!\n\n"
                "Thank you for being part of the RMHQ community — enjoy your day and make it a special one 👑",
                allowed_mentions=discord.AllowedMentions(everyone=False, roles=False, users=True),
            )
            try:
                await message.add_reaction("<:Birthday:1500165275008630925>")
                await message.create_thread(name=self._birthday_thread_title(birthday_members))
            except discord.DiscordException as e:
                print("Birthday announcement post-send action failed:", repr(e))
            msg = f"Birthday announcement sent for {date_text}: {', '.join(user_ids)}"
            print(msg)
            return msg
        except Exception:
            if not force:
                await birthdays.release_announcement_date(date_text)
            raise

    async def _assign_birthday_roles(self, guild: discord.Guild, members: list[discord.Member]) -> None:
        if config.BIRTHDAYS_ROLE_ID is None:
            print("Birthday role skipped: missing BIRTHDAYS_ROLE_ID.")
            return

        role = guild.get_role(config.BIRTHDAYS_ROLE_ID)
        if role is None:
            print(f"Birthday role skipped: role {config.BIRTHDAYS_ROLE_ID} was not found in {guild.name}.")
            return

        tz = ZoneInfo("Europe/Paris")
        now = datetime.now(tz)
        remove_at = datetime(now.year, now.month, now.day, tzinfo=tz) + timedelta(days=1)
        roles_added = 0
        async with self._birthday_role_lock:
            for member in members:
                try:
                    if role not in getattr(member, "roles", []):
                        await member.add_roles(role, reason="Birthday announcement")
                        roles_added += 1
                    await birthdays.record_role_assignment(guild.id, member.id, role.id, remove_at)
                except discord.Forbidden:
                    print(
                        "Birthday role add failed: missing permissions or role hierarchy prevents adding "
                        f"{role.id} to {member.id}."
                    )
                except discord.HTTPException as e:
                    print(f"Birthday role add failed for user {member.id}: {e!r}")
        print(f"Birthday role assignment: added {roles_added} role(s), tracked {len(members)} member(s).")

    def _birthday_thread_title(self, members: list[discord.Member]) -> str:
        names: list[str] = []
        for member in members:
            name = member.display_name
            safe_name = " ".join(str(name).replace("@", "").split())
            names.append(f"@{safe_name or member.id}")

        title = f"Happy birthday {' '.join(names)} 🎉"
        return title if len(title) <= 100 else title[:97].rstrip() + "..."

    async def _giveaway_loop(self) -> None:
        await self.wait_until_ready()
        try:
            await self._register_active_giveaway_views()
        except Exception as e:
            print("Giveaway startup registration failed:", repr(e))

        while not self.is_closed():
            try:
                await self._end_due_giveaways()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                print("Giveaway ending loop failed:", repr(e))
            await asyncio.sleep(60)

    async def _register_active_giveaway_views(self) -> None:
        active = await giveaways.active_giveaways()
        registered = 0
        for giveaway in active:
            if not giveaway.message_id:
                continue
            try:
                self.add_view(GiveawayEntryView(giveaway.id), message_id=int(giveaway.message_id))
                registered += 1
            except ValueError:
                pass
        print(f"Giveaway startup: registered {registered} active giveaway view(s).")

    async def _end_due_giveaways(self) -> None:
        due = await giveaways.due_giveaways()
        if not due:
            return
        print(f"Giveaway ending: {len(due)} giveaway(s) due.")
        for giveaway in due:
            await self._end_giveaway(giveaway)

    async def _end_giveaway(self, giveaway: giveaways.Giveaway) -> str:
        if not giveaway.message_id:
            msg = f"Giveaway {giveaway.id}: missing message ID; marking ended."
            print(msg)
            await giveaways.mark_ended(giveaway.id)
            return msg

        entries = await giveaways.entries(giveaway.id)
        entries_count = len(entries)
        winners_count = min(giveaway.winners_count, entries_count)
        winner_ids = random.sample(entries, winners_count) if winners_count else []
        winners_text = " ".join(f"<@{user_id}>" for user_id in winner_ids) if winner_ids else "No winners"

        channel = self.get_channel(int(giveaway.channel_id))
        if channel is None:
            try:
                channel = await self.fetch_channel(int(giveaway.channel_id))
            except discord.NotFound:
                msg = f"Giveaway {giveaway.id}: channel not found; marking ended."
                print(msg)
                await giveaways.mark_ended(giveaway.id)
                return msg
        if not hasattr(channel, "fetch_message"):
            msg = f"Giveaway {giveaway.id}: channel is not message-fetchable."
            print(msg)
            return msg

        try:
            message = await channel.fetch_message(int(giveaway.message_id))  # type: ignore[attr-defined]
        except discord.NotFound:
            msg = f"Giveaway {giveaway.id}: message not found; marking ended."
            print(msg)
            await giveaways.mark_ended(giveaway.id)
            return msg
        except discord.DiscordException as e:
            msg = f"Giveaway {giveaway.id}: failed to fetch message: {e!r}"
            print(msg)
            return msg

        try:
            if winner_ids:
                await message.reply(
                    f"Congratulations {winners_text}! You won the **{giveaway.prize}** 🎉",
                    allowed_mentions=discord.AllowedMentions(everyone=False, roles=False, users=True),
                )
            else:
                await message.reply(
                    "No valid entries were submitted for this giveaway.",
                    allowed_mentions=discord.AllowedMentions.none(),
                )

            await message.edit(
                embed=giveaways.giveaway_embed(
                    giveaway,
                    entries_count=entries_count,
                    winners_text=winners_text,
                ),
                view=GiveawayEntryView(giveaway.id, disabled=True),
            )
            await giveaways.mark_ended(giveaway.id)
            msg = f"Giveaway {giveaway.id}: ended with {entries_count} entries and {len(winner_ids)} winner(s)."
            print(msg)
            return msg
        except discord.Forbidden:
            msg = f"Giveaway {giveaway.id}: missing permissions to reply/edit giveaway message."
            print(msg)
            return msg
        except discord.DiscordException as e:
            msg = f"Giveaway {giveaway.id}: Discord error while ending giveaway: {e!r}"
            print(msg)
            return msg

    async def end_giveaway_now(self, giveaway_id: int) -> str:
        giveaway = await giveaways.get_giveaway(int(giveaway_id))
        if giveaway is None:
            return "That giveaway does not exist."
        if giveaway.ended:
            return "That giveaway has already ended."
        return await self._end_giveaway(giveaway)


bot = RematchHQBot()

_setup_kwargs: dict = {}


async def _require_guild_administrator(interaction: discord.Interaction, guild: discord.Guild) -> bool:
    """Send an ephemeral reply and return False if the user is not a guild Administrator."""
    member = interaction.user
    if not isinstance(member, discord.Member):
        try:
            member = await guild.fetch_member(interaction.user.id)
        except discord.NotFound:
            await interaction.response.send_message(
                "Could not verify your permissions for this server.",
                ephemeral=True,
            )
            return False
    if not member.guild_permissions.administrator:
        await interaction.response.send_message(
            "You need the **Administrator** permission to use this command.",
            ephemeral=True,
        )
        return False
    return True


@bot.command(name="sync")
@commands.guild_only()
async def sync_prefix(ctx: commands.Context):
    """
    Manually sync slash commands.

    Useful when SYNC_COMMANDS_ON_STARTUP=0 but you added/changed commands.
    """
    if not ctx.guild or not ctx.channel:
        return

    if ctx.guild and not config.is_allowed_setup_channel(guild_id=ctx.guild.id, channel_id=ctx.channel.id):
        server = config.server_for_guild_id(ctx.guild.id)
        required = server.setup_channel_id if server else None
        if required is not None:
            await ctx.reply(f"Use this in <#{required}>.", mention_author=False)
            return

    try:
        synced_by_guild = await bot._sync_guild_commands()
        if not config.GUILD_IDS:
            await ctx.reply(
                "Set `DISCORD_GUILD_ID` or `GUILD_ID` to sync slash commands instantly.",
                mention_author=False,
            )
            return
        total = sum(len(commands) for commands in synced_by_guild.values())
        guilds = ", ".join(str(guild_id) for guild_id in synced_by_guild)
        await ctx.reply(
            f"Synced {total} guild command(s) across {len(synced_by_guild)} server(s): {guilds}",
            mention_author=False,
        )
        await bot._clear_remote_global_commands()
    except discord.DiscordException as e:
        await ctx.reply(f"Sync failed: {e!r}", mention_author=False)


@app_commands.default_permissions(administrator=True)
@app_commands.guild_only()
@bot.tree.command(name="setup", description="Post the Rematch HQ setup panel", **_setup_kwargs)
async def setup(interaction: discord.Interaction):
    if not interaction.guild or not interaction.channel:
        await interaction.response.send_message("Run this in the server.", ephemeral=True)
        return

    if not await _require_guild_administrator(interaction, interaction.guild):
        return

    if not config.is_allowed_setup_channel(guild_id=interaction.guild.id, channel_id=interaction.channel.id):
        server = config.server_for_guild_id(interaction.guild.id)
        required = server.setup_channel_id if server else None
        if required is not None:
            await interaction.response.send_message(f"Use this in <#{required}>.", ephemeral=True)
            return

    embed = discord.Embed(
        title="Rematch HQ Setup",
        description=(
            "💖 **Compliment:** Tag someone to post a compliment.\n"
            "📅 **Tournament Today:** Post today's tournaments.\n\n"
            "🏆 **Tournament Results:** Post the results of a tournament.\n\n"
            "📊 **Leaderboard:** Post the current leaderboard (top 30).\n"
            "👑 **Rosters:** Post the current rosters (top 8).\n"
            "💶 **Earnings:** Calculate prize earnings from Notion.\n\n"
            "🔮 **Add Prediction:** Pick the correct answer from a finished poll.\n"
            "📈 **Calculate Predictions:** Show the top predictors for a given month.\n\n"
            "🎉 **Add Giveaway:** Create a giveaway.\n"
            "🎁 **List Giveaways:** View and end active giveaways."
        ),
        color=0xbe629b,
    )
    await interaction.response.send_message(embed=embed, view=SetupView())


@app_commands.default_permissions(administrator=True)
@app_commands.guild_only()
@bot.tree.command(name="setup_birthday", description="Post the birthday registration panel", **_setup_kwargs)
async def setup_birthday(interaction: discord.Interaction):
    if not interaction.guild or not interaction.channel:
        await interaction.response.send_message("Run this in the server.", ephemeral=True)
        return

    if not await _require_guild_administrator(interaction, interaction.guild):
        return

    if not config.is_allowed_setup_channel(guild_id=interaction.guild.id, channel_id=interaction.channel.id):
        server = config.server_for_guild_id(interaction.guild.id)
        required = server.setup_channel_id if server else None
        if required is not None:
            await interaction.response.send_message(f"Use this in <#{required}>.", ephemeral=True)
            return

    embed = discord.Embed(
        title="🎂 Birthdays",
        description=(
            "✅ **Add/Update Birthday:** Save or update your birthday.\n"
            "❌ **Remove Birthday:** Delete your saved birthday.\n"
            "📋 **List Birthdays:** Download the registered birthdays list.\n"
            "🔔 **Ping Today's Birthday:** [Admin-only] Test/recovery birthday announcement."
        ),
        color=0xbe629b,
    )
    await interaction.response.send_message(embed=embed, view=BirthdaySetupView())


@app_commands.default_permissions(administrator=True)
@app_commands.guild_only()
@bot.tree.command(name="setup_part", description="Post the Rematch HQ setup-part panel", **_setup_kwargs)
async def setup_part(interaction: discord.Interaction):
    if not interaction.guild or not interaction.channel:
        await interaction.response.send_message("Run this in the server.", ephemeral=True)
        return

    if not await _require_guild_administrator(interaction, interaction.guild):
        return

    if not config.is_allowed_setup_channel(guild_id=interaction.guild.id, channel_id=interaction.channel.id):
        server = config.server_for_guild_id(interaction.guild.id)
        required = server.setup_channel_id if server else None
        if required is not None:
            await interaction.response.send_message(f"Use this in <#{required}>.", ephemeral=True)
            return

    embed = discord.Embed(
        title="PART Setup",
        description="""🏆 **Tournament Info:** Create a tournament info embed.
                    🥇 **Hall of Fame:** Create a hall of fame embed.
                    📊 **Leaderboard:** Post the current leaderboard.
                    💰 **Sponsors:** Create a sponsors embed.
                    ✌️ **Calculate GGs:** Monthly GG message leaderboard (Hall of Fame).
        """,
        color=0xbe629b,
    )
    await interaction.response.send_message(embed=embed, view=SetupPartView())


@app_commands.default_permissions(administrator=True)
@app_commands.guild_only()
@bot.tree.command(name="setup_emergency", description="Post the emergency substitution panel", **_setup_kwargs)
async def setup_emergency(interaction: discord.Interaction):
    if not interaction.guild or not interaction.channel:
        await interaction.response.send_message("Run this in the server.", ephemeral=True)
        return

    if not await _require_guild_administrator(interaction, interaction.guild):
        return

    if not config.is_allowed_setup_channel(guild_id=interaction.guild.id, channel_id=interaction.channel.id):
        server = config.server_for_guild_id(interaction.guild.id)
        required = server.setup_channel_id if server else None
        if required is not None:
            await interaction.response.send_message(f"Use this in <#{required}>.", ephemeral=True)
            return

    player_embed = discord.Embed(
        title="👤 Emergency Subs — Players",
        description="Register yourself as available for today, view teams looking for subs, or cancel your availability.",
        color=0xbe629b,
    )
    team_embed = discord.Embed(
        title="👥 Emergency Subs — Teams",
        description="Request an emergency sub for your team, view available players, or cancel your request.",
        color=0xbe629b,
    )
    await interaction.response.send_message(embed=player_embed, view=EmergencyPlayersView())
    await interaction.followup.send(embed=team_embed, view=EmergencyTeamsView())


def run():
    bot.run(config.TOKEN)

