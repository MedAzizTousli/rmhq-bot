from __future__ import annotations

import re
from pathlib import Path


def _key(raw: str) -> str:
    # "MRC", "mrc", "MRC.png" -> "MRC"
    s = raw.strip()
    s = re.sub(r"\.(png|jpg|jpeg|webp)$", "", s, flags=re.IGNORECASE)
    s = re.sub(r"[^0-9A-Za-z_-]", "", s)
    return s.upper()


def find_icon(org_code: str) -> Path | None:
    code = _key(org_code)
    if not code:
        return None

    root = Path(__file__).resolve().parents[1]
    icons_dir = root / "tournament_icons"
    for ext in (".png", ".webp", ".jpg", ".jpeg"):
        p = icons_dir / f"{code}{ext}"
        if p.exists():
            return p
    return None

