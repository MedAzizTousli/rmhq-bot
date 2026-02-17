import re
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import discord
import httpx
import yaml

from . import config
from .academy import ROLES, TEAMS_YAML_PATH, create_teams_from_file, register_player, unregister_player
from .team_emojis import emoji_for, emoji_for_org, emoji_name_for_team
from .team_icons import find_team_icon
from .tournament_icons import find_icon
from .notion_api import NotionClient
from .todays_tournaments import (
    cet_day,
    discord_timestamp,
    detect_props,
    extract_tournament,
    notion_query_payload_for_today_cups,
    today_cet,
)


_TS_RE = re.compile(r"<t:(\d+)(?::[tTdDfFR])?>")
_CET = ZoneInfo("Europe/Paris")
_USER_MENTION_RE = re.compile(r"<@!?(\d+)>")
_FLAG_ALIAS_RE = re.compile(r"^:flag_([a-z]{2}):$", re.IGNORECASE)

_REPO_ROOT = Path(__file__).resolve().parents[1]
_LEADERBOARD_CSV = _REPO_ROOT / "leaderboard" / "output" / "leaderboard_aggregated.csv"
_ROSTERS_YAML = _REPO_ROOT / "leaderboard" / "output" / "rosters.yaml"

_RULEBOOK_URL = "https://www.notion.so/Rulebook-2cd037d9654180bdba21ea03e737d8d8?source=copy_link"

def _find_guild_emoji_by_name(guild: discord.Guild, name: str) -> str:
    want = (name or "").strip().lower()
    if not want:
        return ""
    for e in guild.emojis:
        if (e.name or "").lower() == want:
            return str(e)
    return ""


def _pick_tournament_types(server: config.ServerConfig, *, require_key: str | None = None) -> list[str]:
    """
    Return available tournament type codes (e.g. ["PRT", "ART"]).
    If require_key is set, only return types present in that mapping.
    Falls back to ["PRT", "ART"] if nothing is configured.
    """
    preferred = ["PRT", "ART"]
    mapping = None
    if require_key == "tournament_info_channel_id":
        mapping = server.tournament_info_channel_id
    elif require_key == "hall_of_fame_channel_id":
        mapping = server.hall_of_fame_channel_id

    if mapping:
        available = {k.strip().upper() for k in mapping.keys() if str(k).strip()}
        kinds = [k for k in preferred if k in available]
        return kinds or preferred
    return preferred


async def _ensure_team_emoji(guild: discord.Guild, team_name: str) -> str:
    """
    Return the team's custom emoji string if available.
    If missing, best-effort upload it from /icons/teams (requires Manage Emojis permission).
    """
    team = " ".join((team_name or "").strip().split())
    if not team:
        return ""

    existing = emoji_for(team, guild)
    if existing:
        return existing

    icon_path = find_team_icon(team)
    if not icon_path:
        return ""

    emoji_name = emoji_name_for_team(team)[:32]
    if not emoji_name:
        return ""

    try:
        img = icon_path.read_bytes()
        # Discord custom emoji upload limit is small (~256KB). If too large, skip creation.
        if not (0 < len(img) <= 256 * 1024):
            return ""
        created = await guild.create_custom_emoji(
            name=emoji_name,
            image=img,
            reason="Auto-added for sponsor/Hall of Fame post",
        )
        return str(created)
    except (OSError, discord.Forbidden, discord.HTTPException):
        return ""


def _parse_sponsor_line(line: str) -> tuple[str, str, str] | tuple[None, str]:
    """
    Parse either:
      1) "Team name | Country | DiscordId"
      2) (legacy) "<amount> — <team name> <country> <discord id/mention>"

    Returns (team, flag, mention) or (None, error).
    """
    s = (line or "").strip()
    if not s:
        return None, "Empty line."

    # Preferred format: Team | Country | ID
    if "|" in s:
        parts = [p.strip() for p in s.split("|")]
        if len(parts) != 3:
            return None, f"Expected 3 parts separated by `|` in: `{s}`"
        team, country_raw, uid_raw = parts
        if not team or not country_raw or not uid_raw:
            return None, f"Missing team/country/id in: `{s}`"

        uid = _extract_user_id(uid_raw)
        if not uid:
            return None, f"Couldn't read a Discord user id from: `{uid_raw}`"
        mention = f"<@{uid}>"

        flag = _country_to_flag(country_raw)
        if not flag:
            return None, f"Couldn't read country/flag `{country_raw}` in: `{s}`"

        return team, flag, mention

    # Prefer em dash separator.
    if "—" in s:
        amount_raw, rest = (part.strip() for part in s.split("—", 1))
    elif "-" in s:
        amount_raw, rest = (part.strip() for part in s.split("-", 1))
    else:
        return None, f"Missing separator '—' in: `{s}`"

    if not amount_raw or not rest:
        return None, f"Invalid sponsor line: `{s}`"

    uid = _extract_user_id(rest)
    if not uid:
        return None, f"Missing Discord id/mention in: `{s}`"

    mention = f"<@{uid}>"

    # Remove uid/mention from rest to parse team + country.
    no_user = _USER_MENTION_RE.sub("", rest).strip()
    no_user = no_user.replace(uid, "", 1).strip()
    if not no_user:
        return None, f"Missing team/country in: `{s}`"

    parts = no_user.split()
    if len(parts) < 2:
        return None, f"Expected: '<team> <country> <id>' in: `{s}`"

    country_raw = parts[-1]
    team = " ".join(parts[:-1]).strip()
    if not team:
        return None, f"Missing team name in: `{s}`"

    flag = _country_to_flag(country_raw)
    if not flag:
        return None, f"Couldn't read country/flag `{country_raw}` in: `{s}`"

    return team, flag, mention


def _format_leaderboard_embed(rows: list[dict[str, str]]) -> discord.Embed:
    """
    Build an embed with 3 columns:
      Placement | Team | Points
    Placement supports tie ranges like "1-2" based on equal Points.
    """
    # Keep low so the Team field stays under Discord's 1024-char limit (48 lines).
    max_team = 18

    def to_points_int(s: str) -> int:
        try:
            # Formula points can be decimals; display + tie by rounded integer.
            return int(round(float((s or "").strip() or "0")))
        except ValueError:
            return 0

    # Sort by Points desc, then Team asc for stable display.
    sorted_rows = sorted(
        rows,
        key=lambda r: (-to_points_int(r.get("Points", "")), (r.get("Team") or "").casefold()),
    )

    # Compute placement labels with ties (same Points => same placement range)
    points_list = [to_points_int(r.get("Points", "")) for r in sorted_rows]
    placement_labels = [""] * len(sorted_rows)
    i = 0
    while i < len(sorted_rows):
        j = i
        while j + 1 < len(sorted_rows) and points_list[j + 1] == points_list[i]:
            j += 1
        label = f"{i+1}-{j+1}" if j > i else f"{i+1}"
        for k in range(i, j + 1):
            placement_labels[k] = label
        i = j + 1

    placement_vals: list[str] = []
    team_vals: list[str] = []
    points_vals: list[str] = []

    for idx, r in enumerate(sorted_rows):
        team = " ".join((r.get("Team") or "").split())
        if len(team) > max_team:
            team = team[: max_team - 1] + "…"

        # Replace 1/2/3 with medal emojis in the Placement column (but keep tie ranges like "1-2").
        pl = placement_labels[idx]
        if pl == "1":
            pl = "🥇"
        elif pl == "2":
            pl = "🥈"
        elif pl == "3":
            pl = "🥉"
        placement_vals.append(pl)
        pts_str = str(to_points_int(r.get("Points", "")))

        # Make top 3 teams + points bold (Discord markdown).
        if idx < 3:
            team_vals.append(f"**{team or '-'}**")
            points_vals.append(f"**{pts_str}**")
        else:
            team_vals.append(team or "-")
            points_vals.append(pts_str)

    e = discord.Embed(title="Leaderboard (02/02 -> 16/02)", color=0x36E3bA)
    e.add_field(name="Placement", value="\n".join(placement_vals) or "-", inline=True)
    e.add_field(name="Team", value="\n".join(team_vals) or "-", inline=True)
    e.add_field(name="Points", value="\n".join(points_vals) or "-", inline=True)
    e.set_footer(text="Best viewed on desktop.")
    return e


def _to_discord_timestamp(raw: str) -> str | None:
    s = raw.strip()
    if not s:
        return None

    m = _TS_RE.fullmatch(s)
    if m:
        return f"<t:{int(m.group(1))}:F>"

    if s.isdigit():
        return f"<t:{int(s)}:F>"

    # Accept "YYYY-MM-DD" or "YYYY-MM-DD HH:MM" (assumed CET/CEST)
    s2 = s.replace("/", "-")
    for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M"):
        try:
            dt = datetime.strptime(s2, fmt).replace(tzinfo=_CET)
            return f"<t:{int(dt.astimezone(timezone.utc).timestamp())}:F>"
        except ValueError:
            pass

    return s


def _split_entry_prize(raw: str) -> tuple[str, str] | None:
    # Expect: "Entry / Prize" (also accepts "|" as separator)
    s = raw.strip()
    if not s:
        return None

    if "|" in s:
        a, b = (part.strip() for part in s.split("|", 1))
    elif "/" in s:
        a, b = (part.strip() for part in s.split("/", 1))
    else:
        return None

    if not a or not b:
        return None
    return a, b


def _split_entry_prize_and_time(raw: str) -> tuple[str, str, str] | None:
    """
    Discord Modals support max 5 TextInputs, so we combine:
      Entry fee | Prize pool | Date & time

    Accepted separators: "|" or "/"
    Examples:
      "€10 | €200 | 2026-02-11 19:00"
      "€10 / €200 / <t:1739300400>"
    """
    s = raw.strip()
    if not s:
        return None

    sep = "|" if "|" in s else ("/" if "/" in s else None)
    if not sep:
        return None

    parts = [p.strip() for p in s.split(sep)]
    if len(parts) < 3:
        return None

    entry = parts[0]
    prize = parts[1]
    when_raw = sep.join(parts[2:]).strip()  # allow separators in the timestamp string
    if not entry or not prize or not when_raw:
        return None

    return entry, prize, when_raw


def _split_org_and_name(raw: str) -> tuple[str, str] | None:
    # Expect: "ORG | Tournament name"
    s = raw.strip()
    if "|" not in s:
        return None
    org, name = (part.strip() for part in s.split("|", 1))
    if not org or not name:
        return None
    return org, name


def _flag_from_iso2(code: str) -> str | None:
    c = (code or "").strip().upper()
    if len(c) != 2 or not c.isalpha():
        return None
    a = ord(c[0]) - ord("A")
    b = ord(c[1]) - ord("A")
    if not (0 <= a <= 25 and 0 <= b <= 25):
        return None
    return chr(0x1F1E6 + a) + chr(0x1F1E6 + b)


def _extract_user_id(raw: str) -> str | None:
    s = (raw or "").strip()
    if not s:
        return None

    m = _USER_MENTION_RE.search(s)
    if m:
        return m.group(1)

    # Accept raw numeric IDs too.
    if s.isdigit():
        return s

    m2 = re.search(r"(\d{15,20})", s)
    if m2:
        return m2.group(1)

    return None


def _country_to_flag(raw: str) -> str | None:
    s = (raw or "").strip()
    if not s:
        return None

    # If they paste the actual flag emoji, keep it.
    # (Flags are two "regional indicator" codepoints; we just accept common 2-char sequences.)
    if len(s) <= 4 and any("\U0001F1E6" <= ch <= "\U0001F1FF" for ch in s):
        return s

    # Support :flag_fr: style.
    m = _FLAG_ALIAS_RE.match(s)
    if m:
        return _flag_from_iso2(m.group(1))

    # Support ISO-2 codes like FR, GB, US, etc.
    if len(s) == 2 and s.isalpha():
        return _flag_from_iso2(s)

    # Support a few common country names.
    name = " ".join(s.casefold().split())
    common = {
        "france": "FR",
        "french": "FR",
        "germany": "DE",
        "deutschland": "DE",
        "serbia": "RS",
        "spain": "ES",
        "hungary": "HU",
        "italy": "IT",
        "portugal": "PT",
        "netherlands": "NL",
        "holland": "NL",
        "belgium": "BE",
        "switzerland": "CH",
        "austria": "AT",
        "sweden": "SE",
        "norway": "NO",
        "denmark": "DK",
        "finland": "FI",
        "poland": "PL",
        "czech republic": "CZ",
        "czechia": "CZ",
        "romania": "RO",
        "bulgaria": "BG",
        "greece": "GR",
        "turkey": "TR",
        "ukraine": "UA",
        "russia": "RU",
        "united kingdom": "GB",
        "uk": "GB",
        "england": "GB",
        "scotland": "GB",
        "wales": "GB",
        "ireland": "IE",
        "united states": "US",
        "usa": "US",
        "canada": "CA",
        "mexico": "MX",
        "brazil": "BR",
        "argentina": "AR",
        "chile": "CL",
        "colombia": "CO",
        "peru": "PE",
        "japan": "JP",
        "china": "CN",
        "south korea": "KR",
        "korea": "KR",
        "india": "IN",
        "australia": "AU",
        "new zealand": "NZ",
        "saudi arabia": "SA",
        "morocco": "MA",
        "tunisia": "TN",
        "algeria": "DZ",
        "egypt": "EG",
        "south africa": "ZA",
    }
    if name in common:
        return _flag_from_iso2(common[name])

    return None


async def _ensure_team_role(
    guild: discord.Guild,
    *,
    role_name: str,
    team_name: str,
    role_color: int | None,
) -> discord.Role | None:
    """
    Ensure a hoisted + mentionable role exists for the team.
    Colors the role using `role_color` from `rosters.yaml` (best-effort).
    """
    desired = " ".join((role_name or "").strip().split())
    legacy = " ".join((team_name or "").strip().split())
    if not desired or not legacy:
        return None

    # Prefer the rank-prefixed role name.
    role = discord.utils.get(guild.roles, name=desired)
    if role is not None:
        # Keep it fast: only create roles if missing.
        return role

    # Back-compat: if an old role exists with just the team name, reuse it and rename (best-effort).
    role = discord.utils.get(guild.roles, name=legacy)
    if role is not None and role.name != desired:
        try:
            await role.edit(name=desired, reason="Auto-renamed team role for rosters")
        except (discord.Forbidden, discord.HTTPException):
            # If we can't rename, we'll just use the legacy role.
            pass
        return role

    colour = discord.Colour(int(role_color)) if role_color is not None else None

    try:
        role = await guild.create_role(
            name=desired,
            colour=colour or discord.Colour.default(),
            hoist=True,  # displayed separately on the right
            mentionable=True,
            reason="Auto-created team role for rosters",
        )
    except discord.Forbidden:
        return None
    except discord.HTTPException:
        return None

    return role


def _parse_winning_roster(raw: str) -> tuple[list[str], str | None]:
    """
    Input: one player per line:
      <discord id or mention> <country>

    Country accepted as:
      - 🇫🇷 (flag emoji)
      - :flag_fr:
      - FR (ISO-2)
      - France (common names only)

    Output lines: "🇫🇷 <@123...>"
    Returns (lines, error_message)
    """
    lines_in = (raw or "").splitlines()
    out: list[str] = []
    for ln in lines_in:
        line = ln.strip()
        if not line:
            continue

        # Grab user id first, then treat the rest as country text.
        uid = _extract_user_id(line)
        if not uid:
            return [], f"Couldn't read a Discord user id from: `{line}`"

        # Remove the mention/id chunk to get country.
        rest = _USER_MENTION_RE.sub("", line).strip()
        rest = rest.replace(uid, "", 1).strip()
        if not rest:
            return [], f"Missing country for: `<@{uid}>` (line: `{line}`)"

        flag = _country_to_flag(rest)
        if not flag:
            return [], (
                f"Couldn't read a country/flag from: `{rest}`.\n"
                "Use `FR`, `:flag_fr:`, or `🇫🇷` (or a common country name like `France`)."
            )

        out.append(f"{flag} <@{uid}>")

    if not out:
        return [], "Winning roster is required (at least 1 player line)."

    return out, None


def _parse_roster(raw: str) -> tuple[list[str], str | None]:
    """
    Input: one player per line, in either order:
      <country> <discord id or mention>
      <discord id or mention> <country>

    Country accepted as:
      - 🇫🇷 (flag emoji)
      - :flag_fr:
      - FR (ISO-2)
      - common country names (limited list)

    Output lines: "🇫🇷 <@123...>"
    Returns (lines, error_message)
    """
    lines_in = (raw or "").splitlines()
    out: list[str] = []
    for ln in lines_in:
        line = ln.strip()
        if not line:
            continue

        uid = _extract_user_id(line)
        if not uid:
            return [], f"Couldn't read a Discord user id from: `{line}`"

        # Remove the mention/id chunk to get country.
        rest = _USER_MENTION_RE.sub("", line).strip()
        rest = rest.replace(uid, "", 1).strip()
        if not rest:
            return [], f"Missing country for: `<@{uid}>` (line: `{line}`)"

        flag = _country_to_flag(rest)
        if not flag:
            return [], (
                f"Couldn't read a country/flag from: `{rest}`.\n"
                "Use `FR`, `:flag_fr:`, or `🇫🇷` (or a common country name like `France`)."
            )

        out.append(f"{flag} <@{uid}>")

    if not out:
        return [], "Roster is required (at least 1 player line)."

    return out, None


class TournamentResultsModal(discord.ui.Modal, title="Tournament Results"):
    tournament_name = discord.ui.TextInput(
        label="Tournament (ORG | Name)",
        placeholder="e.g. MRC | Rematch Weekly #12",
        required=True,
        max_length=80,
    )
    tournament_url = discord.ui.TextInput(
        label="Tournament URL",
        placeholder="https://...",
        required=True,
        max_length=200,
    )
    entry_and_prize = discord.ui.TextInput(
        label="Entry | Prize | Date & time",
        placeholder="e.g. €10 | €200 | 2026-02-11 19:00",
        required=True,
        max_length=120,
    )
    standings = discord.ui.TextInput(
        label="Standings (top 4)",
        placeholder="Team A\nTeam B\nTeam C\nTeam D",
        style=discord.TextStyle.paragraph,
        required=True,
        max_length=400,
    )
    winning_roster = discord.ui.TextInput(
        label="Winning roster",
        placeholder="One per line: <@id> FR  (or :flag_fr: / 🇫🇷)",
        style=discord.TextStyle.paragraph,
        required=True,
        max_length=400,
    )

    async def on_submit(self, interaction: discord.Interaction):
        org_and_name = _split_org_and_name(self.tournament_name.value or "")
        if not org_and_name:
            await interaction.response.send_message(
                "Tournament format: `MRC | Rematch Weekly #12`",
                ephemeral=True,
            )
            return
        t_org, t_name = org_and_name
        t_url = (self.tournament_url.value or "").strip()
        entry_prize_time = _split_entry_prize_and_time(self.entry_and_prize.value or "")
        if not entry_prize_time:
            await interaction.response.send_message(
                "Entry/Prize/Date format: `€10 | €200 | 2026-02-11 19:00`",
                ephemeral=True,
            )
            return
        t_entry, t_prize, when_raw = entry_prize_time
        t_when = _to_discord_timestamp(when_raw)

        raw_lines = (self.standings.value or "").splitlines()
        teams = [line.strip() for line in raw_lines if line.strip()][:4]
        medals = ["1.", "2.", "3.", "4."]

        roster_lines, roster_err = _parse_winning_roster(self.winning_roster.value or "")
        if roster_err:
            await interaction.response.send_message(
                "Winning roster format (one per line):\n"
                "`<@123456789012345678> FR`\n"
                "`123456789012345678 :flag_fr:`\n"
                "`<@123456789012345678> 🇫🇷`\n\n"
                f"{roster_err}",
                ephemeral=True,
            )
            return

        lines: list[str] = []
        for i, name in enumerate(teams):
            medal = medals[i]
            e = emoji_for(name, interaction.guild)
            lines.append(f"{medal} {e + ' ' if e else ''}{name}")

        embed = discord.Embed(title=t_name, color=0x36E3bA)
        embed.add_field(name="Tournament", value=f"[URL]({t_url})", inline=True)
        embed.add_field(name="Entry fee", value=t_entry, inline=True)
        embed.add_field(name="Prize pool", value=t_prize, inline=True)
        embed.add_field(name="Date & time", value=t_when, inline=False)
        embed.add_field(name="Standings", value="\n".join(lines) or "-", inline=False)
        embed.add_field(name="Winning roster", value="\n".join(roster_lines), inline=False)

        icon_path = find_icon(t_org)
        icon_file = None
        if icon_path:
            filename = icon_path.name
            icon_file = discord.File(icon_path, filename=filename)
            embed.set_thumbnail(url=f"attachment://{filename}")

        if not interaction.guild:
            await interaction.response.send_message("Run this in the server.", ephemeral=True)
            return

        server = config.server_for_guild_id(interaction.guild.id)
        results_channel_id = server.results_tournaments_channel_id if server else None
        if not results_channel_id:
            await interaction.response.send_message(
                "This server is missing `RESULTS_TOURNAMENTS_CHANNEL_ID` in `config.yaml`.",
                ephemeral=True,
            )
            return

        channel = interaction.guild.get_channel(results_channel_id)
        if channel is None:
            try:
                channel = await interaction.guild.fetch_channel(results_channel_id)
            except discord.DiscordException:
                channel = None

        if channel is None or not hasattr(channel, "send"):
            await interaction.response.send_message(
                "Couldn't find the results channel. Check `RESULTS_TOURNAMENTS_CHANNEL_ID` in `config.yaml`.",
                ephemeral=True,
            )
            return

        try:
            kwargs = dict(
                embed=embed,
                allowed_mentions=discord.AllowedMentions(everyone=False, roles=True, users=True),
            )
            ping_id = server.tournaments_ping_id if server else None
            if ping_id:
                role = interaction.guild.get_role(ping_id)
                ping = f"<@&{ping_id}>" if role else f"<@{ping_id}>"
                kwargs["content"] = ping
            if icon_file:
                kwargs["file"] = icon_file
            msg = await channel.send(**kwargs)

            # React with winner + organizer emojis (best-effort).
            winner_team = teams[0] if teams else ""
            winner_emoji = emoji_for(winner_team, interaction.guild)
            org_emoji = emoji_for_org(t_org, interaction.guild)
            for r in (winner_emoji, org_emoji):
                if not r:
                    continue
                try:
                    await msg.add_reaction(r)
                except discord.DiscordException:
                    pass
        except discord.Forbidden:
            await interaction.response.send_message(
                "I don't have permission to post in the results channel.",
                ephemeral=True,
            )
            return

        await interaction.response.send_message(
            f"Posted in <#{results_channel_id}>.",
            ephemeral=True,
        )

    async def on_error(self, interaction: discord.Interaction, error: Exception) -> None:
        print("TournamentResultsModal error:", repr(error))
        msg = "Something went wrong while creating the results embed."
        if interaction.response.is_done():
            await interaction.followup.send(msg, ephemeral=True)
        else:
            await interaction.response.send_message(msg, ephemeral=True)


class TournamentInfoModal(discord.ui.Modal):
    tournament_name = discord.ui.TextInput(
        label="Tournament name",
        placeholder="e.g. PRT #9 — Rematch Weekly Cup",
        required=True,
        max_length=100,
    )
    battlefy_url = discord.ui.TextInput(
        label="Battlefy URL",
        placeholder="https://battlefy.com/...",
        required=True,
        max_length=200,
    )
    date_time = discord.ui.TextInput(
        label="Date & time",
        placeholder="e.g. 2026-02-11 19:00  (CET)  or  <t:1739300400>",
        required=True,
        max_length=80,
    )

    def __init__(self, *, tournament_type: str):
        self.tournament_type = (tournament_type or "").strip().upper()
        super().__init__(title=f"{self.tournament_type} Tournament Info")

    async def on_submit(self, interaction: discord.Interaction):
        t_name = " ".join((self.tournament_name.value or "").strip().split())
        t_url = (self.battlefy_url.value or "").strip()
        when = _to_discord_timestamp(self.date_time.value or "")

        if not interaction.guild:
            await interaction.response.send_message("Run this in the server.", ephemeral=True)
            return

        server = config.server_for_guild_id(interaction.guild.id)
        if server is None:
            await interaction.response.send_message(
                "This server is not configured in `config.yaml` (missing matching `SERVER_ID`).",
                ephemeral=True,
            )
            return

        ttype = self.tournament_type or "PRT"
        # Destination channel
        info_channel_id = (server.tournament_info_channel_id or {}).get(ttype)
        if not info_channel_id:
            await interaction.response.send_message(
                f"This server is missing `TOURNAMENT_INFO_CHANNEL_ID.{ttype}` in `config.yaml`.",
                ephemeral=True,
            )
            return

        # Embed color
        color = (server.embed_color or {}).get(ttype, 0x36E3bA)
        # Prize pool
        prize_pool = (server.prize_pool or {}).get(ttype, 50.0)

        embed = discord.Embed(title=t_name or "Tournament", color=int(color))
        # Row 1 (inline): Battlefy | Rules | Fees & Rewards
        embed.add_field(name="Battlefy", value=f"[URL]({t_url})" if t_url else "-", inline=True)
        embed.add_field(name="Rules", value=f"[URL]({_RULEBOOK_URL})", inline=True)
        embed.add_field(
            name="Fees & Rewards",
            value=f"__Entry Fee__: 0€\n__Prize Pool__: {prize_pool:g}€",
            inline=True,
        )
        # Rows below (stacked)
        embed.add_field(
            name="Date & time",
            value=(f"{when}\nRegistration closes 10 minutes before tournament start").strip(),
            inline=False,
        )
        embed.add_field(
            name="Format",
            value=(
                "Double Elimination Bracket\n"
                "__Winners Bracket__: BO3\n"
                "__Losers Bracket__: BO1\n"
                "__Grand Final__: BO5 with a 1-game advantage for the Winners Bracket team"
            ),
            inline=False,
        )
        embed.add_field(
            name="Match Settings",
            value=(
                "```"
                "Match duration: 6 min\n"
                "Overtime max duration: Infinite\n"
                "Score to reach: 0\n"
                "Mercy rule goal difference: 4\n"
                "Enable goal sweeper: No"
                "```"
            ),
            inline=False,
        )

        icon_path = find_icon(ttype)
        icon_file = None
        if icon_path:
            filename = icon_path.name
            icon_file = discord.File(icon_path, filename=filename)
            embed.set_thumbnail(url=f"attachment://{filename}")

        channel = interaction.guild.get_channel(info_channel_id)
        if channel is None:
            try:
                channel = await interaction.guild.fetch_channel(info_channel_id)
            except discord.DiscordException:
                channel = None

        if channel is None or not hasattr(channel, "send"):
            await interaction.response.send_message(
                f"Couldn't find the tournament-info channel. Check `TOURNAMENT_INFO_CHANNEL_ID.{ttype}` in `config.yaml`.",
                ephemeral=True,
            )
            return

        try:
            kwargs = dict(
                embed=embed,
                allowed_mentions=discord.AllowedMentions(everyone=False, roles=False, users=False),
            )
            if icon_file:
                kwargs["file"] = icon_file
            msg = await channel.send(**kwargs)

            # React with a tournament-type emoji (best-effort).
            # ART previously used "lART" which won't match a normal ":ART:" emoji name.
            candidates: list[str] = []
            if ttype in {"PRT", "ART"}:
                candidates.append(ttype)
            if ttype == "ART":
                candidates.append("lART")  # backward-compat if a server actually named it this way

            for emoji_name in candidates:
                e = _find_guild_emoji_by_name(interaction.guild, emoji_name)
                if not e:
                    continue
                try:
                    await msg.add_reaction(e)
                except discord.DiscordException:
                    pass
                break
        except discord.Forbidden:
            await interaction.response.send_message(
                "I don't have permission to post in the tournaments channel.",
                ephemeral=True,
            )
            return

        await interaction.response.send_message(
            f"Posted in <#{channel.id}>.",
            ephemeral=True,
        )

    async def on_error(self, interaction: discord.Interaction, error: Exception) -> None:
        print("TournamentInfoModal error:", repr(error))
        msg = "Something went wrong while creating the tournament info embed."
        try:
            if interaction.response.is_done():
                await interaction.followup.send(msg, ephemeral=True)
            else:
                await interaction.response.send_message(msg, ephemeral=True)
        except discord.DiscordException:
            pass


class TournamentInfoTypeSelect(discord.ui.Select):
    def __init__(self, *, options: list[discord.SelectOption]):
        super().__init__(
            placeholder="Select tournament type…",
            min_values=1,
            max_values=1,
            options=options,
        )

    async def callback(self, interaction: discord.Interaction):
        ttype = (self.values[0] if self.values else "").strip().upper()
        await interaction.response.send_modal(TournamentInfoModal(tournament_type=ttype))


class TournamentInfoTypeView(discord.ui.View):
    def __init__(self, *, options: list[discord.SelectOption]):
        super().__init__(timeout=120)
        self.add_item(TournamentInfoTypeSelect(options=options))


class HallOfFameModal(discord.ui.Modal):
    edition_number = discord.ui.TextInput(
        label="Edition number",
        placeholder="e.g. 9",
        required=True,
        max_length=10,
    )
    team_name = discord.ui.TextInput(
        label="Team name",
        placeholder="e.g. OVERDOZEE",
        required=True,
        max_length=60,
    )
    bracket_url = discord.ui.TextInput(
        label="Bracket URL",
        placeholder="https://battlefy.com/...",
        required=True,
        max_length=200,
    )
    roster = discord.ui.TextInput(
        label="Roster (one per line: country + discord id)",
        placeholder="FR 123456789012345678\nMA <@123456789012345678>\n:flag_es: 123456789012345678",
        style=discord.TextStyle.paragraph,
        required=True,
        max_length=600,
    )

    def __init__(self, *, tournament_type: str):
        self.tournament_type = (tournament_type or "").strip().upper()
        super().__init__(title=f"{self.tournament_type} Hall of Fame")

    async def on_submit(self, interaction: discord.Interaction):
        if not interaction.guild:
            await interaction.response.send_message("Run this in the server.", ephemeral=True)
            return

        server = config.server_for_guild_id(interaction.guild.id)
        if server is None:
            await interaction.response.send_message(
                "This server is not configured in `config.yaml` (missing matching `SERVER_ID`).",
                ephemeral=True,
            )
            return

        ttype = self.tournament_type or "PRT"
        hof_channel_id = (server.hall_of_fame_channel_id or {}).get(ttype)
        if not hof_channel_id:
            await interaction.response.send_message(
                f"This server is missing `HALL_OF_FAME_CHANNEL_ID.{ttype}` in `config.yaml`.",
                ephemeral=True,
            )
            return

        # Validate edition number
        ed_raw = (self.edition_number.value or "").strip()
        m = re.search(r"\d+", ed_raw)
        if not m:
            await interaction.response.send_message("Edition number must contain a number (e.g. `9`).", ephemeral=True)
            return
        edition = int(m.group(0))

        team = " ".join((self.team_name.value or "").strip().split())
        url = (self.bracket_url.value or "").strip()

        roster_lines, roster_err = _parse_roster(self.roster.value or "")
        if roster_err:
            await interaction.response.send_message(
                "Roster format (one per line):\n"
                "`FR 123456789012345678`\n"
                "`:flag_fr: <@123456789012345678>`\n"
                "`🇫🇷 123456789012345678`\n\n"
                f"{roster_err}",
                ephemeral=True,
            )
            return

        color = (server.embed_color or {}).get(ttype, 0x36E3bA)

        # Attach TEAM icon as the main embed image (not thumbnail).
        team_icon_path = find_team_icon(team)
        team_icon_file = None

        # Try to use (or best-effort create) the custom emoji.
        team_emoji = await _ensure_team_emoji(interaction.guild, team)

        title = f"{ttype} #{edition} Champions — {team}{(' ' + team_emoji) if team_emoji else ''}"
        embed = discord.Embed(title=title, color=int(color))
        embed.add_field(name="Bracket", value=f"[Battlefy]({url})" if url else "-", inline=False)
        embed.add_field(name="Roster", value="\n".join(roster_lines) or "-", inline=False)

        if team_icon_path:
            filename = team_icon_path.name
            team_icon_file = discord.File(team_icon_path, filename=filename)
            embed.set_image(url=f"attachment://{filename}")

        channel = interaction.guild.get_channel(hof_channel_id)
        if channel is None:
            try:
                channel = await interaction.guild.fetch_channel(hof_channel_id)
            except discord.DiscordException:
                channel = None

        if channel is None or not hasattr(channel, "send"):
            await interaction.response.send_message(
                f"Couldn't find the Hall of Fame channel. Check `HALL_OF_FAME_CHANNEL_ID.{ttype}` in `config.yaml`.",
                ephemeral=True,
            )
            return

        try:
            kwargs = dict(
                embed=embed,
                allowed_mentions=discord.AllowedMentions(everyone=False, roles=False, users=True),
            )
            if team_icon_file:
                kwargs["file"] = team_icon_file
            msg = await channel.send(**kwargs)

            # React with the team emoji (best-effort).
            if team_emoji:
                try:
                    await msg.add_reaction(team_emoji)
                except discord.DiscordException:
                    pass
        except discord.Forbidden:
            await interaction.response.send_message(
                "I don't have permission to post in the Hall of Fame channel.",
                ephemeral=True,
            )
            return

        await interaction.response.send_message(f"Posted in <#{channel.id}>.", ephemeral=True)

    async def on_error(self, interaction: discord.Interaction, error: Exception) -> None:
        print("HallOfFameModal error:", repr(error))
        msg = "Something went wrong while creating the Hall of Fame embed."
        try:
            if interaction.response.is_done():
                await interaction.followup.send(msg, ephemeral=True)
            else:
                await interaction.response.send_message(msg, ephemeral=True)
        except discord.DiscordException:
            pass


class HallOfFameTypeSelect(discord.ui.Select):
    def __init__(self, *, options: list[discord.SelectOption]):
        super().__init__(
            placeholder="Select tournament type…",
            min_values=1,
            max_values=1,
            options=options,
        )

    async def callback(self, interaction: discord.Interaction):
        ttype = (self.values[0] if self.values else "").strip().upper()
        await interaction.response.send_modal(HallOfFameModal(tournament_type=ttype))


class HallOfFameTypeView(discord.ui.View):
    def __init__(self, *, options: list[discord.SelectOption]):
        super().__init__(timeout=120)
        self.add_item(HallOfFameTypeSelect(options=options))


class SponsorsModal(discord.ui.Modal):
    edition_number = discord.ui.TextInput(
        label="Edition number",
        placeholder="e.g. 8",
        required=True,
        max_length=10,
    )
    section_name = discord.ui.TextInput(
        label="Section name",
        placeholder="e.g. TRIAL",
        required=True,
        max_length=50,
    )
    sponsors = discord.ui.TextInput(
        label="Sponsors (one per line)",
        placeholder="Orion Esports | Morocco | 263329265594925057",
        style=discord.TextStyle.paragraph,
        required=True,
        max_length=1200,
    )

    def __init__(self):
        super().__init__(title="PRT Sponsors")

    async def on_submit(self, interaction: discord.Interaction):
        if not interaction.guild:
            await interaction.response.send_message("Run this in the server.", ephemeral=True)
            return

        server = config.server_for_guild_id(interaction.guild.id)
        if server is None:
            await interaction.response.send_message(
                "This server is not configured in `config.yaml` (missing matching `SERVER_ID`).",
                ephemeral=True,
            )
            return

        ttype = "PRT"
        channel_id = (server.sponsors_channel_id or {}).get(ttype)
        if not channel_id:
            await interaction.response.send_message(
                "This server is missing `SPONSORS_CHANNEL_ID.PRT` in `config.yaml`.",
                ephemeral=True,
            )
            return

        # Parse edition
        ed_raw = (self.edition_number.value or "").strip()
        m = re.search(r"\d+", ed_raw)
        if not m:
            await interaction.response.send_message("Edition number must contain a number (e.g. `8`).", ephemeral=True)
            return
        edition = int(m.group(0))

        # Parse sponsor lines
        lines_in = (self.sponsors.value or "").splitlines()
        out_lines: list[str] = []
        for ln in lines_in:
            if not ln.strip():
                continue
            parsed = _parse_sponsor_line(ln)
            if parsed[0] is None:
                await interaction.response.send_message(f"Sponsor line error: {parsed[1]}", ephemeral=True)
                return
            team, flag, mention = parsed  # type: ignore[misc]

            team_emoji = await _ensure_team_emoji(interaction.guild, team)
            out_lines.append(f"10€ — {team_emoji + ' ' if team_emoji else ''}{flag} {mention}")

        if not out_lines:
            await interaction.response.send_message("Sponsors list is required (at least 1 line).", ephemeral=True)
            return

        section = " ".join((self.section_name.value or "").strip().split())
        if not section:
            section = "Sponsors"

        color = (server.embed_color or {}).get(ttype, 0x36E3bA)
        embed = discord.Embed(title=f"PRT #{edition} Sponsors", color=int(color))
        embed.add_field(name=section, value="\n".join(out_lines), inline=False)
        embed.set_footer(text="Huge thanks for the support!")

        channel = interaction.guild.get_channel(channel_id)
        if channel is None:
            try:
                channel = await interaction.guild.fetch_channel(channel_id)
            except discord.DiscordException:
                channel = None

        if channel is None or not hasattr(channel, "send"):
            await interaction.response.send_message(
                "Couldn't find the sponsors channel. Check `SPONSORS_CHANNEL_ID.PRT` in `config.yaml`.",
                ephemeral=True,
            )
            return

        try:
            msg = await channel.send(
                embed=embed,
                allowed_mentions=discord.AllowedMentions(everyone=False, roles=False, users=True),
            )
            # React with :heart_hands: (🫶). Best-effort.
            try:
                await msg.add_reaction("🫶")
            except discord.DiscordException:
                pass
        except discord.Forbidden:
            await interaction.response.send_message(
                "I don't have permission to post in the sponsors channel.",
                ephemeral=True,
            )
            return

        await interaction.response.send_message(f"Posted in <#{channel.id}>.", ephemeral=True)

    async def on_error(self, interaction: discord.Interaction, error: Exception) -> None:
        print("SponsorsModal error:", repr(error))
        msg = "Something went wrong while creating the sponsors embed."
        try:
            if interaction.response.is_done():
                await interaction.followup.send(msg, ephemeral=True)
            else:
                await interaction.response.send_message(msg, ephemeral=True)
        except discord.DiscordException:
            pass


class SetupView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(
        label="🏆 Tournament Results",
        style=discord.ButtonStyle.primary,
        custom_id="rematchhq:tournament_results",
    )
    async def tournament_results(self, interaction: discord.Interaction, _: discord.ui.Button):
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

        await interaction.response.send_modal(TournamentResultsModal())

    @discord.ui.button(
        label="📅 Tournament Today",
        style=discord.ButtonStyle.primary,
        custom_id="rematchhq:tournament_today",
    )
    async def tournament_today(self, interaction: discord.Interaction, _: discord.ui.Button):
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

        if not config.NOTION_TOKEN or not config.NOTION_DATABASE_ID:
            await interaction.response.send_message(
                "Missing `NOTION_TOKEN` or `NOTION_DATABASE_ID` in `.env`.",
                ephemeral=True,
            )
            return

        await interaction.response.defer(ephemeral=True, thinking=True)

        print("Notion: querying today's tournaments...")
        try:
            client = NotionClient(config.NOTION_TOKEN)
            db = await client.retrieve_database(config.NOTION_DATABASE_ID)
            props = detect_props(db)
            payload = notion_query_payload_for_today_cups(props)
            pages = await client.query_database(config.NOTION_DATABASE_ID, payload)
        except httpx.ReadTimeout:
            print("Notion: ReadTimeout while querying database.")
            await interaction.followup.send(
                "Notion timed out while fetching tournaments. Try again in a bit.",
                ephemeral=True,
            )
            return
        except httpx.HTTPStatusError as e:
            print("Notion: HTTP error:", e.response.status_code, e.response.text[:500])
            await interaction.followup.send(
                f"Notion API error ({e.response.status_code}). Check `NOTION_TOKEN` and `NOTION_DATABASE_ID`.",
                ephemeral=True,
            )
            return
        except Exception as e:
            print("Notion: unexpected error:", repr(e))
            await interaction.followup.send("Notion error. Check terminal logs.", ephemeral=True)
            return

        tournaments = [t for p in pages if (t := extract_tournament(p, props))]
        tday = today_cet()
        tournaments_today = [t for t in tournaments if cet_day(t.starts_at) == tday]
        print(f"Notion: {len(tournaments_today)} tournament(s) today")

        if not tournaments_today:
            await interaction.followup.send("No tournaments found for today.", ephemeral=True)
            return

        items: list[tuple[discord.Embed, str, str | None, str | None]] = []
        for t in tournaments_today[:25]:
            entry = f"{t.entry_fee_eur:g}€" if isinstance(t.entry_fee_eur, (int, float)) else "-"
            prize = f"{t.prize_pool_eur:g}€" if isinstance(t.prize_pool_eur, (int, float)) else "-"

            website = f"[URL]({t.website_url})" if t.website_url else "-"
            dsc = f"[URL]({t.discord_url})" if t.discord_url else "-"

            org = (t.organization or "").strip()
            org_emoji = emoji_for_org(org, interaction.guild)
            title = f"{org_emoji} {t.title}".strip() if org_emoji else t.title

            e = discord.Embed(title=title, color=0x36E3bA)
            e.add_field(name="Time", value=discord_timestamp(t.starts_at), inline=True)
            e.add_field(name="Entry fee", value=entry, inline=True)
            e.add_field(name="Prize pool", value=prize, inline=True)
            e.add_field(name="Website", value=website, inline=True)
            e.add_field(name="Discord", value=dsc, inline=True)

            icon_path = find_icon(org)
            icon_filename = icon_path.name if icon_path else None
            if icon_filename:
                e.set_thumbnail(url=f"attachment://{icon_filename}")
            items.append((e, org, str(icon_path) if icon_path else None, icon_filename))

        server = config.server_for_guild_id(interaction.guild.id)
        upcoming_channel_id = server.upcoming_tournaments_channel_id if server else None
        if not upcoming_channel_id:
            await interaction.followup.send(
                "This server is missing `UPCOMING_TOURNAMENTS_CHANNEL_ID` in `config.yaml`.",
                ephemeral=True,
            )
            return

        channel = interaction.guild.get_channel(upcoming_channel_id)
        if channel is None:
            try:
                channel = await interaction.guild.fetch_channel(upcoming_channel_id)
            except discord.DiscordException:
                channel = None

        if channel is None or not hasattr(channel, "send"):
            await interaction.followup.send(
                "Couldn't find tournaments channel. Check `UPCOMING_TOURNAMENTS_CHANNEL_ID` in `config.yaml`.",
                ephemeral=True,
            )
            return

        try:
            ping_id = server.tournaments_ping_id if server else None
            ping = None
            if ping_id:
                role = interaction.guild.get_role(ping_id)
                ping = f"<@&{ping_id}>" if role else f"<@{ping_id}>"
            for i in range(0, len(items), 10):
                chunk = items[i : i + 10]
                embeds = [e for (e, _, __, ___) in chunk]

                files: list[discord.File] = []
                seen: set[str] = set()
                for _, __org, path, filename in chunk:
                    if not path or not filename or filename in seen:
                        continue
                    seen.add(filename)
                    files.append(discord.File(path, filename=filename))

                msg = await channel.send(
                    content=ping if (ping and i == 0) else None,
                    embeds=embeds,
                    files=files,
                    allowed_mentions=discord.AllowedMentions(
                        everyone=False, roles=True, users=True
                    ),
                )

                # React with tournament (org) emoji(s). Best-effort.
                org_emojis = []
                for __e, org, __p, __f in chunk:
                    em = emoji_for_org(org, interaction.guild)
                    if em and em not in org_emojis:
                        org_emojis.append(em)
                for em in org_emojis[:5]:
                    try:
                        await msg.add_reaction(em)
                    except discord.DiscordException:
                        pass
        except discord.Forbidden:
            await interaction.followup.send(
                "I don't have permission to post in the tournaments channel.",
                ephemeral=True,
            )
            return

        await interaction.followup.send(
            f"Posted {len(items)} tournaments in <#{channel.id}>.",
            ephemeral=True,
        )

    @discord.ui.button(
        label="📊 Leaderboard",
        style=discord.ButtonStyle.green,
        custom_id="rematchhq:leaderboard",
    )
    async def leaderboard(self, interaction: discord.Interaction, _: discord.ui.Button):
        if not interaction.guild or not interaction.channel:
            await interaction.response.send_message("Run this in the server.", ephemeral=True)
            return

        # Always acknowledge quickly to avoid "Unknown interaction" timeouts.
        # We'll use followups for all user-facing responses after this.
        try:
            await interaction.response.defer(ephemeral=True, thinking=True)
        except discord.NotFound:
            # Interaction token already expired/invalid; nothing we can do.
            return

        if not config.is_allowed_setup_channel(guild_id=interaction.guild.id, channel_id=interaction.channel.id):
            server = config.server_for_guild_id(interaction.guild.id)
            required = server.setup_channel_id if server else None
            if required is not None:
                await interaction.followup.send(f"Use this in <#{required}>.", ephemeral=True)
                return

        if not interaction.user.guild_permissions.administrator:
            await interaction.followup.send("Admins only.", ephemeral=True)
            return

        if not _LEADERBOARD_CSV.exists():
            await interaction.followup.send(
                "Couldn't find `csv_points/leaderboard.csv`.\n"
                "Generate it first by running: `python leaderboard.py`",
                ephemeral=True,
            )
            return

        # Read leaderboard.csv and post the top 48 teams.
        import csv

        with _LEADERBOARD_CSV.open("r", newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            if reader.fieldnames is None:
                await interaction.followup.send("Leaderboard CSV has no header row.", ephemeral=True)
                return

            required = {"Rank", "Team", "Points"}
            missing = [c for c in required if c not in set(reader.fieldnames)]
            if missing:
                await interaction.followup.send(
                    f"Leaderboard CSV missing columns: {', '.join(missing)}",
                    ephemeral=True,
                )
                return

            rows = list(reader)

        # Take top 48 by Points (highest first).
        def _points_key(r: dict[str, str]) -> int:
            try:
                return int(round(float((r.get("Points") or "").strip() or "0")))
            except ValueError:
                return 0

        top = sorted(rows, key=lambda r: (-_points_key(r), (r.get("Team") or "").casefold()))[:48]
        embed = _format_leaderboard_embed(top)

        server = config.server_for_guild_id(interaction.guild.id)
        leaderboard_channel_id = server.leaderboard_channel_id if server else None
        if not leaderboard_channel_id:
            await interaction.followup.send(
                "This server is missing `LEADERBOARD_CHANNEL_ID` in `config.yaml`.",
                ephemeral=True,
            )
            return

        channel = interaction.guild.get_channel(leaderboard_channel_id)
        if channel is None:
            try:
                channel = await interaction.guild.fetch_channel(leaderboard_channel_id)
            except discord.DiscordException:
                channel = None

        if channel is None or not hasattr(channel, "send"):
            await interaction.followup.send(
                "Couldn't find the leaderboard channel. Check `LEADERBOARD_CHANNEL_ID` in `config.yaml`.",
                ephemeral=True,
            )
            return

        try:
            ping_id = server.tournaments_ping_id if server else None
            ping = None
            if ping_id:
                role = interaction.guild.get_role(ping_id)
                ping = f"<@&{ping_id}>" if role else f"<@{ping_id}>"
            await channel.send(
                content=ping,
                embed=embed,
                allowed_mentions=discord.AllowedMentions(everyone=False, roles=True, users=True),
            )
        except discord.Forbidden:
            await interaction.followup.send(
                "I don't have permission to post in the rosters channel.",
                ephemeral=True,
            )
            return

        await interaction.followup.send(
            f"Posted leaderboard in <#{channel.id}>.",
            ephemeral=True,
        )

    @discord.ui.button(
        label="👑 Rosters",
        style=discord.ButtonStyle.green,
        custom_id="rematchhq:rosters_embeds",
    )
    async def rosters_embeds(self, interaction: discord.Interaction, _: discord.ui.Button):
        if not interaction.guild or not interaction.channel:
            await interaction.response.send_message("Run this in the server.", ephemeral=True)
            return

        try:
            await interaction.response.defer(ephemeral=True, thinking=True)
        except discord.NotFound:
            return

        if not config.is_allowed_setup_channel(guild_id=interaction.guild.id, channel_id=interaction.channel.id):
            server = config.server_for_guild_id(interaction.guild.id)
            required = server.setup_channel_id if server else None
            if required is not None:
                await interaction.followup.send(f"Use this in <#{required}>.", ephemeral=True)
                return

        if not interaction.user.guild_permissions.administrator:
            await interaction.followup.send("Admins only.", ephemeral=True)
            return

        server = config.server_for_guild_id(interaction.guild.id)
        rosters_channel_id = server.rosters_channel_id if server else None
        if not rosters_channel_id:
            await interaction.followup.send(
                "This server is missing `ROSTERS_CHANNEL_ID` in `config.yaml`.",
                ephemeral=True,
            )
            return

        if not _ROSTERS_YAML.exists():
            await interaction.followup.send(
                "Couldn't find `leaderboard/output/rosters.yaml`.",
                ephemeral=True,
            )
            return

        # Load rosters.yaml as: {team_name: {color: 0x..., roster: [{Country: id}, ...]}}
        with _ROSTERS_YAML.open("r", encoding="utf-8") as f:
            raw = yaml.safe_load(f) or {}
        if not isinstance(raw, dict) or not raw:
            await interaction.followup.send("`leaderboard/output/rosters.yaml` is empty or invalid.", ephemeral=True)
            return

        # Create/ensure roles and assign roster members, then post embeds.
        # Discord allows max 10 embeds + 10 attachments per message; we do 8 teams.
        embeds: list[discord.Embed] = []
        files: list[discord.File] = []
        reactions: list[str] = []
        added = 0
        assigned = 0
        already_had = 0
        missing_members = 0
        role_failures = 0
        member_cache: dict[int, discord.Member] = {}
        roles_created = 0
        roles_renamed = 0
        roles_existing = 0

        for idx, (team_name, team_block) in enumerate(raw.items(), start=1):
            if added >= 8:
                break
            if not isinstance(team_name, str) or not team_name.strip():
                continue
            if not isinstance(team_block, dict):
                continue

            role_color_raw = team_block.get("color")
            role_color = None
            if role_color_raw is not None:
                try:
                    # Accept ints or strings like "0xFEF154".
                    role_color = int(str(role_color_raw).strip(), 0)
                except ValueError:
                    role_color = None

            players = team_block.get("roster")
            if not isinstance(players, list):
                continue

            icon_path = find_team_icon(team_name)
            desired_role_name = f"#{idx} — {team_name}"
            before_names = {r.name for r in interaction.guild.roles}
            role = await _ensure_team_role(
                interaction.guild,
                role_name=desired_role_name,
                team_name=team_name,
                role_color=role_color,
            )
            if role is None:
                role_failures += 1
            else:
                # Track whether we created/renamed/existed (best-effort).
                if role.name not in before_names:
                    roles_created += 1
                elif role.name == desired_role_name and team_name in before_names and desired_role_name not in before_names:
                    roles_renamed += 1
                else:
                    roles_existing += 1

            parsed_lines: list[str] = []
            for item in players:
                if not isinstance(item, dict) or len(item) != 1:
                    continue
                country, uid = next(iter(item.items()))
                if not isinstance(country, str):
                    continue
                try:
                    uid_i = int(uid)
                except (TypeError, ValueError):
                    continue
                flag = _country_to_flag(country) or country.strip()
                parsed_lines.append(f"{flag} <@{uid_i}>")

                # Assign role to member (best-effort).
                if role is not None:
                    try:
                        member = member_cache.get(uid_i) or interaction.guild.get_member(uid_i)
                        if member is None:
                            member = await interaction.guild.fetch_member(uid_i)
                        member_cache[uid_i] = member
                        if role in getattr(member, "roles", []):
                            already_had += 1
                        else:
                            await member.add_roles(
                                role,
                                reason=f"Auto-assigned from rosters.yaml by {interaction.user} ({interaction.user.id})",
                            )
                            assigned += 1
                    except discord.NotFound:
                        missing_members += 1
                    except discord.Forbidden:
                        role_failures += 1
                    except discord.HTTPException:
                        role_failures += 1

            if not parsed_lines:
                continue

            team_emoji = emoji_for(team_name, interaction.guild)
            role_tag = role.mention if role is not None else team_name
            bits: list[str] = []
            if team_emoji:
                bits.append(team_emoji)
                # Collect reactions in roster order (best-effort).
                if team_emoji not in reactions:
                    reactions.append(team_emoji)
            bits.append(role_tag)
            team_heading = "### " + " ".join(bits)
            e = discord.Embed(
                title=None,
                color=0x36E3bA,
                description=team_heading + "\n" + "\n".join(parsed_lines),
            )

            # Attach the team icon as-is (no resizing/padding).
            if icon_path:
                try:
                    attach_name = f"{idx}_{icon_path.name}"
                    files.append(discord.File(icon_path, filename=attach_name))
                    e.set_image(url=f"attachment://{attach_name}")
                except (OSError, discord.DiscordException):
                    pass

            embeds.append(e)
            added += 1

        if added == 0:
            await interaction.followup.send(
                "No valid rosters found in `leaderboard/output/rosters.yaml`.",
                ephemeral=True,
            )
            return

        channel = interaction.guild.get_channel(rosters_channel_id)
        if channel is None:
            try:
                channel = await interaction.guild.fetch_channel(rosters_channel_id)
            except discord.DiscordException:
                channel = None

        if channel is None or not hasattr(channel, "send"):
            await interaction.followup.send(
                "Couldn't find the rosters channel. Check `ROSTERS_CHANNEL_ID` in `config.yaml`.",
                ephemeral=True,
            )
            return

        try:
            ping_id = server.tournaments_ping_id if server else None
            ping = None
            if ping_id:
                ping_role = interaction.guild.get_role(ping_id)
                ping = f"<@&{ping_id}>" if ping_role else f"<@{ping_id}>"
            msg = await channel.send(
                content=ping,
                embeds=embeds,
                files=files,
                allowed_mentions=discord.AllowedMentions(everyone=False, roles=True, users=True),
            )

            # React with team emojis (best-effort), in the same order as the roster.
            for em in reactions[:20]:
                try:
                    await msg.add_reaction(em)
                except discord.DiscordException:
                    pass
        except discord.Forbidden:
            await interaction.followup.send(
                "I don't have permission to post in the rosters channel.",
                ephemeral=True,
            )
            return

        # Summary to the admin.
        await interaction.followup.send(
            (
                f"Posted rosters in <#{channel.id}>.\n"
                f"Roles: **{roles_created}** created, **{roles_renamed}** renamed, **{roles_existing}** existing.\n"
                f"Assignments: **{assigned}** added, **{already_had}** already had, **{missing_members}** missing.\n"
                f"Failures (permissions/API): **{role_failures}**."
            ),
            ephemeral=True,
        )

    @discord.ui.button(
        label="🗑️ Purge Scrims",
        style=discord.ButtonStyle.danger,
        custom_id="rematchhq:purge_scrims",
    )
    async def purge_scrims(self, interaction: discord.Interaction, _: discord.ui.Button):
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

        server = config.server_for_guild_id(interaction.guild.id)
        forum_id = server.scrim_forum_channel_id if server else None
        exclude_uid = server.scrim_forum_user_id_exclude if server else None
        if not forum_id:
            await interaction.response.send_message(
                "Missing `SCRIM_FORUM_CHANNEL_ID` in `config.yaml` for this server.",
                ephemeral=True,
            )
            return

        await interaction.response.send_message(
            "You're about to delete **all posts** in the configured scrims forum.\n\n"
            "Press **Confirm purge** to proceed.",
            ephemeral=True,
            view=ForumPurgeConfirmView(
                requester_id=interaction.user.id,
                forum_channel_id=int(forum_id),
                exclude_user_id=(int(exclude_uid) if exclude_uid else None),
            ),
        )

    async def on_error(self, interaction: discord.Interaction, error: Exception, item) -> None:
        print("SetupView error:", repr(error))
        msg = "Something went wrong handling that button."
        try:
            if interaction.response.is_done():
                await interaction.followup.send(msg, ephemeral=True)
            else:
                await interaction.response.send_message(msg, ephemeral=True)
        except discord.DiscordException:
            # If the interaction already expired/was acknowledged, we can't respond.
            pass


async def _iter_archived_threads_best_effort(
    forum: discord.abc.GuildChannel,
    *,
    private: bool,
) -> list[discord.Thread]:
    """
    Best-effort archived thread fetch across discord.py versions.
    Returns a list (may be empty) and never raises.
    """
    threads: list[discord.Thread] = []

    archived = getattr(forum, "archived_threads", None)
    if not archived:
        return threads

    # discord.py signatures vary slightly across versions (private/joined flags, limit support).
    # We'll try a few compatible call shapes.
    call_variants = [
        lambda: archived(private=private, limit=None),
        lambda: archived(private=private),
        lambda: archived(limit=None),
        lambda: archived(),
    ]

    it = None
    for make in call_variants:
        try:
            it = make()
            break
        except TypeError:
            it = None
            continue
        except discord.DiscordException:
            return threads

    if it is None:
        return threads

    try:
        async for t in it:
            # If we couldn't pass private=..., filter here when possible.
            if private and hasattr(t, "is_private") and callable(getattr(t, "is_private")):
                try:
                    if not t.is_private():
                        continue
                except Exception:
                    pass
            threads.append(t)
    except discord.DiscordException:
        return threads

    return threads


async def _purge_forum_posts(
    interaction: discord.Interaction,
    forum: discord.ForumChannel,
    *,
    exclude_user_id: int | None,
) -> None:
    await interaction.response.defer(ephemeral=True, thinking=True)

    if not interaction.guild:
        await interaction.followup.send("Run this in the server.", ephemeral=True)
        return

    # Collect active threads (guild-wide API, then filter by forum parent)
    all_active: list[discord.Thread] = []
    try:
        all_active = list(await interaction.guild.active_threads())
    except discord.DiscordException:
        all_active = []

    candidates: dict[int, discord.Thread] = {t.id: t for t in all_active if getattr(t, "parent_id", None) == forum.id}

    # Add archived threads (public + private best-effort).
    for t in await _iter_archived_threads_best_effort(forum, private=False):
        if getattr(t, "parent_id", None) == forum.id:
            candidates.setdefault(t.id, t)
    for t in await _iter_archived_threads_best_effort(forum, private=True):
        if getattr(t, "parent_id", None) == forum.id:
            candidates.setdefault(t.id, t)

    threads = list(candidates.values())
    if not threads:
        await interaction.followup.send(f"No posts found in {forum.mention}.", ephemeral=True)
        return

    skipped = 0
    if exclude_user_id:
        filtered: list[discord.Thread] = []
        for t in threads:
            if getattr(t, "owner_id", None) == int(exclude_user_id):
                skipped += 1
                continue
            filtered.append(t)
        threads = filtered

    if not threads:
        await interaction.followup.send(
            f"Found posts in {forum.mention} but skipped **{skipped}** post(s) due to the exclude user id.",
            ephemeral=True,
        )
        return

    ok = 0
    failed = 0
    last_err: str | None = None

    for t in threads:
        try:
            await t.delete(reason=f"/setup purge requested by {interaction.user} ({interaction.user.id})")
            ok += 1
        except discord.Forbidden:
            failed += 1
            last_err = "Missing permissions to delete some threads (need Manage Threads / Manage Channels)."
        except discord.HTTPException as e:
            failed += 1
            last_err = f"HTTP error while deleting: {getattr(e, 'text', None) or repr(e)}"

    msg = f"Purged **{ok}** post(s) in {forum.mention}."
    if skipped:
        msg += f" Skipped: **{skipped}**."
    if failed:
        msg += f" Failed: **{failed}**."
    if last_err:
        msg += f"\n\nNote: {last_err}"
    await interaction.followup.send(msg, ephemeral=True)


class ForumPurgeConfirmView(discord.ui.View):
    def __init__(self, *, requester_id: int, forum_channel_id: int, exclude_user_id: int | None):
        super().__init__(timeout=180)
        self.requester_id = requester_id
        self.forum_channel_id = int(forum_channel_id)
        self.exclude_user_id = int(exclude_user_id) if exclude_user_id else None

    @discord.ui.button(label="Confirm purge", style=discord.ButtonStyle.danger)
    async def confirm(self, interaction: discord.Interaction, _: discord.ui.Button):
        if interaction.user.id != self.requester_id:
            await interaction.response.send_message("Only the admin who opened this can confirm.", ephemeral=True)
            return

        if not interaction.guild:
            await interaction.response.send_message("Run this in the server.", ephemeral=True)
            return

        if not interaction.user.guild_permissions.administrator:
            await interaction.response.send_message("Admins only.", ephemeral=True)
            return

        channel = interaction.guild.get_channel(self.forum_channel_id)
        if channel is None:
            try:
                channel = await interaction.guild.fetch_channel(self.forum_channel_id)
            except discord.DiscordException:
                channel = None

        if not isinstance(channel, discord.ForumChannel):
            await interaction.response.send_message("Couldn't find that forum channel.", ephemeral=True)
            return

        await _purge_forum_posts(interaction, channel, exclude_user_id=self.exclude_user_id)

    @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
    async def cancel(self, interaction: discord.Interaction, _: discord.ui.Button):
        if interaction.user.id != self.requester_id:
            await interaction.response.send_message("Only the admin who opened this can cancel.", ephemeral=True)
            return
        await interaction.response.send_message("Cancelled.", ephemeral=True)


class SetupPartView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(
        label="🏆 Tournament Info",
        style=discord.ButtonStyle.primary,
        custom_id="rematchhq:tournament_info",
    )
    async def tournament_info(self, interaction: discord.Interaction, _: discord.ui.Button):
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

        server = config.server_for_guild_id(interaction.guild.id)
        if server is None:
            await interaction.response.send_message(
                "This server is not configured in `config.yaml` (missing matching `SERVER_ID`).",
                ephemeral=True,
            )
            return

        kinds = _pick_tournament_types(server, require_key="tournament_info_channel_id")

        options = [
            discord.SelectOption(
                label=k,
                value=k,
                description=f"Create a {k} tournament info embed",
            )
            for k in kinds
        ]
        await interaction.response.send_message(
            "Select which tournament you want to create the info embed for.",
            ephemeral=True,
            view=TournamentInfoTypeView(options=options),
        )

    @discord.ui.button(
        label="🥇 Hall of Fame",
        style=discord.ButtonStyle.red,
        custom_id="rematchhq:hall_of_fame",
    )
    async def hall_of_fame(self, interaction: discord.Interaction, _: discord.ui.Button):
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

        server = config.server_for_guild_id(interaction.guild.id)
        if server is None:
            await interaction.response.send_message(
                "This server is not configured in `config.yaml` (missing matching `SERVER_ID`).",
                ephemeral=True,
            )
            return

        kinds = _pick_tournament_types(server, require_key="hall_of_fame_channel_id")
        options = [
            discord.SelectOption(
                label=k,
                value=k,
                description=f"Post {k} champions to Hall of Fame",
            )
            for k in kinds
        ]
        await interaction.response.send_message(
            "Select which tournament you want to post Hall of Fame for.",
            ephemeral=True,
            view=HallOfFameTypeView(options=options),
        )

    @discord.ui.button(
        label="💰 Sponsors",
        style=discord.ButtonStyle.green,
        custom_id="rematchhq:sponsors_prt",
    )
    async def sponsors_prt(self, interaction: discord.Interaction, _: discord.ui.Button):
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

        await interaction.response.send_modal(SponsorsModal())


class AcademyRoleSelect(discord.ui.Select):
    def __init__(self):
        options = [
            discord.SelectOption(label=r, value=r, description=f"Register as {r}") for r in ROLES
        ]
        super().__init__(
            placeholder="Select your role…",
            min_values=1,
            max_values=len(ROLES),
            options=options,
        )

    async def callback(self, interaction: discord.Interaction):
        roles = [v.strip() for v in (self.values or []) if v.strip()]
        username = getattr(interaction.user, "name", "") or ""
        try:
            registered = await register_player(username=username, roles=roles, default_tier=3)
        except ValueError:
            await interaction.response.edit_message(
                content="Invalid role selection. Please try again.",
                view=None,
            )
            return

        parts = [f"**{r}** (tier **{t}**)" for r, t in registered.items()]
        await interaction.response.edit_message(
            content=f"Registered **{username}** as " + ", ".join(parts) + ".",
            view=None,
        )


class AcademyRoleView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=120)
        self.add_item(AcademyRoleSelect())


class AcademySetupView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(
        label="Register",
        style=discord.ButtonStyle.primary,
        custom_id="rematchhq:academy_register",
    )
    async def register(self, interaction: discord.Interaction, _: discord.ui.Button):
        await interaction.response.send_message(
            "What’s your role?",
            ephemeral=True,
            view=AcademyRoleView(),
        )

    @discord.ui.button(
        label="Unregister",
        style=discord.ButtonStyle.danger,
        custom_id="rematchhq:academy_unregister",
    )
    async def unregister(self, interaction: discord.Interaction, _: discord.ui.Button):
        username = getattr(interaction.user, "name", "") or ""
        existed = await unregister_player(username=username)
        msg = "You’ve been unregistered." if existed else "You weren’t registered."
        await interaction.response.send_message(msg, ephemeral=True)

    @discord.ui.button(
        label="Create teams",
        style=discord.ButtonStyle.secondary,
        custom_id="rematchhq:academy_create_teams",
    )
    async def create_teams(self, interaction: discord.Interaction, _: discord.ui.Button):
        if not interaction.guild:
            await interaction.response.send_message("Run this in the server.", ephemeral=True)
            return

        if not interaction.user.guild_permissions.administrator:
            await interaction.response.send_message("Admins only.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True, thinking=True)
        teams = await create_teams_from_file()

        if not teams:
            await interaction.followup.send(
                "Couldn't create any complete teams (need 1 player for each role, and 5 distinct players).",
                ephemeral=True,
            )
            return

        await interaction.followup.send(
            f"Generated **{len(teams)}** academy team(s) into `{TEAMS_YAML_PATH.name}`.",
            ephemeral=True,
        )

        # Preview ALL teams (chunked to stay under Discord's 2000-char limit).
        blocks: list[str] = []
        cur = ""
        for i, team in enumerate(teams, start=1):
            section_lines = [f"academy team {i}:"]
            for role in ROLES:
                u, t = team.get(role, ("-", 0))
                section_lines.append(f"  {role}: {u} ({t})")
            section = "\n".join(section_lines) + "\n"

            # Keep some headroom for code fences.
            if len(cur) + len(section) > 1800 and cur.strip():
                blocks.append(cur.rstrip())
                cur = ""
            cur += section
        if cur.strip():
            blocks.append(cur.rstrip())

        for b in blocks:
            await interaction.followup.send("```" + b + "```", ephemeral=True)

    async def on_error(self, interaction: discord.Interaction, error: Exception, item) -> None:
        print("AcademySetupView error:", repr(error))
        msg = "Something went wrong handling that action."
        try:
            if interaction.response.is_done():
                await interaction.followup.send(msg, ephemeral=True)
            else:
                await interaction.response.send_message(msg, ephemeral=True)
        except discord.DiscordException:
            pass
