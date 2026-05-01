try:
    import asyncio
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

from . import config
from . import emergency_subs
from .views import EmergencyPlayersView, EmergencyTeamsView, SetupPartView, SetupView


class RematchHQBot(commands.Bot):
    def __init__(self):
        intents = discord.Intents.default()
        intents.message_content = True
        intents.members = True
        super().__init__(command_prefix="!", intents=intents)
        self._emergency_reset_task: asyncio.Task | None = None

    async def setup_hook(self):
        self.add_view(SetupView())
        self.add_view(SetupPartView())
        self.add_view(EmergencyPlayersView())
        self.add_view(EmergencyTeamsView())
        self._emergency_reset_task = asyncio.create_task(self._emergency_midnight_reset_loop())

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
            "📈 **Calculate Predictions:** Show the top predictors for a given month."
        ),
        color=0xbe629b,
    )
    await interaction.response.send_message(embed=embed, view=SetupView())


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

