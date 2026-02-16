from __future__ import annotations

import re
from pathlib import Path


def _key(team_name: str) -> str:
    """
    Normalize a team name to match both:
    - custom emoji naming in Discord (spaces -> underscore, strip others)
    - icon file naming in /icons/teams (e.g. Orion_Esports.png)
    """
    s = re.sub(r"\s+", "_", (team_name or "").strip())
    s = re.sub(r"[^0-9A-Za-z_]", "", s)
    return s


def find_team_icon(team_name: str) -> Path | None:
    key = _key(team_name)
    if not key:
        return None

    root = Path(__file__).resolve().parents[1]
    icons_dir = root / "icons" / "teams"
    for ext in (".png", ".webp", ".jpg", ".jpeg"):
        p = icons_dir / f"{key}{ext}"
        if p.exists():
            return p
    return None

