from __future__ import annotations

import asyncio
import re
import unicodedata
from collections.abc import Iterable
from urllib.parse import quote

import httpx


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


async def team_icon_url_exists(client: httpx.AsyncClient, url: str, *, timeout: float = 8.0) -> bool:
    """True if the object URL responds with OK. On network errors, True (avoid false warnings)."""
    try:
        r = await client.head(url, follow_redirects=True, timeout=timeout)
        if r.status_code == 200:
            return True
        if r.status_code == 405:
            r = await client.get(url, follow_redirects=True, timeout=timeout)
            return r.status_code == 200
        return False
    except httpx.RequestError:
        return True


async def unreachable_team_icon_urls(
    team_names: Iterable[str],
    client: httpx.AsyncClient,
    *,
    timeout: float = 8.0,
    max_concurrent: int = 8,
) -> frozenset[str]:
    """
    URLs that find_team_icon would use but that are missing or non-OK on storage (e.g. no PNG in Supabase).
    """
    unique_urls: set[str] = set()
    for name in team_names:
        u = find_team_icon((name or "").strip())
        if u:
            unique_urls.add(u)

    if not unique_urls:
        return frozenset()

    sem = asyncio.Semaphore(max_concurrent)

    async def probe(url: str) -> tuple[str, bool]:
        async with sem:
            ok = await team_icon_url_exists(client, url, timeout=timeout)
            return (url, ok)

    results = await asyncio.gather(*(probe(u) for u in unique_urls), return_exceptions=True)
    bad: set[str] = set()
    for r in results:
        if isinstance(r, BaseException):
            continue
        url, ok = r
        if not ok:
            bad.add(url)
    return frozenset(bad)

