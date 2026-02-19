try:
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
from .views import AcademySetupView, SetupPartView, SetupView


class RematchHQBot(commands.Bot):
    def __init__(self):
        intents = discord.Intents.default()
        super().__init__(command_prefix="!", intents=intents)

    async def setup_hook(self):
        self.add_view(SetupView())
        self.add_view(SetupPartView())
        self.add_view(AcademySetupView())

        if not config.SYNC_COMMANDS_ON_STARTUP:
            print("Skipping slash command sync (SYNC_COMMANDS_ON_STARTUP=0).")
            return

        if config.GUILD_ID:
            guild = discord.Object(id=config.GUILD_ID)
            synced_guild = await self.tree.sync(guild=guild)
            print(f"Synced {len(synced_guild)} guild command(s).")
            # Avoid also syncing global commands here; it increases rate-limit risk and isn't needed
            # when you intentionally scope commands to a guild for fast iteration.
        else:
            synced = await self.tree.sync()
            print(f"Synced {len(synced)} global command(s). (May take time to appear)")


bot = RematchHQBot()

_setup_kwargs: dict = {}
if config.GUILD_ID:
    _setup_kwargs["guild"] = discord.Object(id=config.GUILD_ID)


@bot.command(name="sync")
@commands.guild_only()
async def sync_prefix(ctx: commands.Context):
    """
    Admin-only: manually sync slash commands.

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

    if not getattr(ctx.author, "guild_permissions", None) or not ctx.author.guild_permissions.administrator:
        await ctx.reply("Admins only.", mention_author=False)
        return

    try:
        if config.GUILD_ID:
            guild = discord.Object(id=config.GUILD_ID)
            synced = await bot.tree.sync(guild=guild)
            await ctx.reply(f"Synced {len(synced)} guild command(s).", mention_author=False)
        else:
            synced = await bot.tree.sync()
            await ctx.reply(f"Synced {len(synced)} global command(s).", mention_author=False)
    except discord.DiscordException as e:
        await ctx.reply(f"Sync failed: {e!r}", mention_author=False)


@bot.tree.command(name="setup", description="Post the Rematch HQ setup panel", **_setup_kwargs)
async def setup(interaction: discord.Interaction):
    if not interaction.guild or not interaction.channel:
        await interaction.response.send_message("Run this in the server.", ephemeral=True)
        return

    if not config.is_allowed_setup_channel(guild_id=interaction.guild.id, channel_id=interaction.channel.id):
        server = config.server_for_guild_id(interaction.guild.id)
        required = server.setup_channel_id if server else None
        if required is not None:
            await interaction.response.send_message(f"Use this in <#{required}>.", ephemeral=True)
            return

    if not interaction.user.guild_permissions.administrator:
        await interaction.response.send_message("Admins only.", ephemeral=True)
        return

    embed = discord.Embed(
        title="Rematch HQ Setup",
        description=(
            "🏆 **Tournament Results:** Post the results of a tournament.\n"
            "📅 **Tournament Today:** Post today's tournaments.\n"
            "📊 **Leaderboard:** Post the current leaderboard (top 48).\n"
            "👑 **Rosters:** Post the current rosters (top 8).\n"
            "🗑️ **Purge Scrims:** Delete all posts in the scrims forum."
        ),
        color=0xbe629b,
    )
    await interaction.response.send_message(embed=embed, view=SetupView())


@bot.tree.command(name="setup_part", description="Post the Rematch HQ setup-part panel", **_setup_kwargs)
async def setup_part(interaction: discord.Interaction):
    if not interaction.guild or not interaction.channel:
        await interaction.response.send_message("Run this in the server.", ephemeral=True)
        return

    if not config.is_allowed_setup_channel(guild_id=interaction.guild.id, channel_id=interaction.channel.id):
        server = config.server_for_guild_id(interaction.guild.id)
        required = server.setup_channel_id if server else None
        if required is not None:
            await interaction.response.send_message(f"Use this in <#{required}>.", ephemeral=True)
            return

    if not interaction.user.guild_permissions.administrator:
        await interaction.response.send_message("Admins only.", ephemeral=True)
        return

    embed = discord.Embed(
        title="PART Setup",
        description="""🏆 **Tournament Info:** Create a tournament info embed.\n
                    🥇 **Hall of Fame:** Create a hall of fame embed.\n
                    💰 **Sponsors:** Create a sponsors embed.\n
        """,
        color=0xbe629b,
    )
    await interaction.response.send_message(embed=embed, view=SetupPartView())


@bot.tree.command(name="setup_academy", description="Post the Academy registration panel", **_setup_kwargs)
async def setup_academy(interaction: discord.Interaction):
    if not interaction.guild or not interaction.channel:
        await interaction.response.send_message("Run this in the server.", ephemeral=True)
        return

    if not config.is_allowed_setup_channel(guild_id=interaction.guild.id, channel_id=interaction.channel.id):
        server = config.server_for_guild_id(interaction.guild.id)
        required = server.setup_channel_id if server else None
        if required is not None:
            await interaction.response.send_message(f"Use this in <#{required}>.", ephemeral=True)
            return

    # Keep panel posting admin-only to avoid spam; buttons are available to everyone.
    if not interaction.user.guild_permissions.administrator:
        await interaction.response.send_message("Admins only.", ephemeral=True)
        return

    embed = discord.Embed(
        title="Academy Registration",
        description="Use the buttons below to register or unregister for the Academy.",
        color=0xbe629b,
    )
    await interaction.response.send_message(embed=embed, view=AcademySetupView())


def run():
    bot.run(config.TOKEN)

