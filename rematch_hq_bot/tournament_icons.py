from __future__ import annotations

import re
import unicodedata
from urllib.parse import quote


_TOURNAMENT_ICONS_BASE_URL = "https://fymociohyudqxnfflkxy.supabase.co/storage/v1/object/public/tournaments/"


def _key(raw: str) -> str:
    # "MRC", "mrc", "MRC.png" -> "MRC"
    s = unicodedata.normalize("NFKD", raw.strip())
    s = s.encode("ascii", "ignore").decode("ascii")
    s = re.sub(r"\.(png|jpg|jpeg|webp)$", "", s, flags=re.IGNORECASE)
    s = re.sub(r"[^0-9A-Za-z_-]", "", s)
    return s.upper()


def find_icon(org_code: str) -> str | None:
    code = _key(org_code)
    if not code:
        return None
    return _TOURNAMENT_ICONS_BASE_URL + quote(f"{code}.png")

