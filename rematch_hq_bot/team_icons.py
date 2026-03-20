from __future__ import annotations

import re
import unicodedata
from urllib.parse import quote


_TEAM_ICONS_BASE_URL = "https://fymociohyudqxnfflkxy.supabase.co/storage/v1/object/public/teams/"


def _key(team_name: str) -> str:
    """
    Normalize a team name to match both:
    - custom emoji naming in Discord
    - Supabase icon file naming (e.g. 100x35_esports.png)
    """
    s = unicodedata.normalize("NFKD", (team_name or "").strip())
    s = s.encode("ascii", "ignore").decode("ascii")
    s = re.sub(r"[\s-]+", "_", s)
    s = re.sub(r"[^0-9A-Za-z_]", "", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s.lower()


def find_team_icon(team_name: str) -> str | None:
    key = _key(team_name)
    if not key:
        return None
    return _TEAM_ICONS_BASE_URL + quote(f"{key}.png")

