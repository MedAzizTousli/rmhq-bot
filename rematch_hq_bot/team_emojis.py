import re

import discord


def _emoji_name_from_team(team_name: str) -> str:
    # Custom emoji names are typically [a-zA-Z0-9_]. We map spaces -> underscore and strip others.
    s = re.sub(r"\s+", "_", team_name.strip())
    s = re.sub(r"[^0-9A-Za-z_]", "", s)
    return s


def emoji_name_for_team(team_name: str) -> str:
    """
    Public helper for mapping a team name to the expected custom-emoji name.
    Example: "Orion Esports" -> "Orion_Esports"
    """
    return _emoji_name_from_team(team_name)


def _find_custom_emoji(guild: discord.Guild, raw_name: str) -> discord.Emoji | None:
    want = raw_name.strip()
    if not want:
        return None
    want_l = want.lower()
    for e in guild.emojis:
        if e.name.lower() == want_l:
            return e
    return None


def emoji_for(team_name: str, guild: discord.Guild | None) -> str:
    name = team_name.strip()
    if guild:
        emoji_name = _emoji_name_from_team(name)
        if emoji_name:
            e = _find_custom_emoji(guild, emoji_name) or _find_custom_emoji(guild, emoji_name.lower())
            if e:
                return str(e)
    return ""


def emoji_for_org(org_code: str, guild: discord.Guild | None) -> str:
    if not guild:
        return ""
    raw = org_code.strip()
    if not raw:
        return ""
    candidates: list[str] = []
    under = re.sub(r"[^0-9A-Za-z_]", "", raw.replace(" ", "_"))
    if under:
        candidates.append(under)
    compact = re.sub(r"[^0-9A-Za-z_]", "", raw)
    if compact and compact not in candidates:
        candidates.append(compact)
    parts = [p for p in re.split(r"[\s_-]+", raw) if p]
    if len(parts) >= 2:
        tail = re.sub(r"[^0-9A-Za-z_]", "", parts[-1])
        if tail and tail not in candidates:
            candidates.append(tail)
    for code in candidates:
        e = _find_custom_emoji(guild, code) or _find_custom_emoji(guild, code.lower())
        if e:
            return str(e)
    return ""

