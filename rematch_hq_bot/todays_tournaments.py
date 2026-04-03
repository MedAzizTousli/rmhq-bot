from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from collections.abc import Collection
from typing import Any, Sequence
from zoneinfo import ZoneInfo

from .tournament_icons import find_icon


_CET = ZoneInfo("Europe/Paris")


@dataclass(frozen=True)
class Tournament:
    title: str
    organization: str
    starts_at: datetime
    # True when Notion's date start was YYYY-MM-DD only (no time in the API payload).
    start_is_date_only: bool
    format: str
    entry_fee_eur: int | float | None
    prize_pool_eur: int | float | None
    website_url: str
    discord_url: str


@dataclass(frozen=True)
class NotionProps:
    title: str
    organization: str
    type: str
    starts_at: str
    format: str
    entry_fee: str
    prize_pool: str
    website_url: str
    discord_url: str


def _get_prop(page: dict[str, Any], name: str) -> dict[str, Any] | None:
    return (page.get("properties") or {}).get(name)


def _title(page: dict[str, Any], name: str) -> str:
    p = _get_prop(page, name) or {}
    items = p.get("title") or []
    return "".join((it.get("plain_text") or "") for it in items).strip()


def _select(page: dict[str, Any], name: str) -> str:
    p = _get_prop(page, name) or {}
    sel = p.get("select") or {}
    return (sel.get("name") or "").strip()


def _number(page: dict[str, Any], name: str) -> int | float | None:
    p = _get_prop(page, name) or {}
    return p.get("number")


def _url(page: dict[str, Any], name: str) -> str:
    p = _get_prop(page, name) or {}
    return (p.get("url") or "").strip()


def _notion_starts_at(page: dict[str, Any], name: str) -> tuple[datetime | None, bool]:
    """Parse Notion date start; second value is True if the API value was date-only (no time)."""
    p = _get_prop(page, name) or {}
    d = p.get("date") or {}
    start = d.get("start")
    if not start:
        return None, False
    raw = str(start).strip()
    # Notion sends "2026-04-01" without a clock; with time it includes "T…".
    start_is_date_only = "T" not in raw
    s = raw.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return None, False
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt, start_is_date_only


def extract_tournament(page: dict[str, Any], props: NotionProps) -> Tournament | None:
    title = _title(page, props.title)
    if not title:
        return None

    org = _select(page, props.organization)
    t_type = _select(page, props.type)
    fmt = _select(page, props.format)
    starts_at, start_is_date_only = _notion_starts_at(page, props.starts_at)

    if t_type != "Cup":
        return None
    if not starts_at:
        return None

    website = _url(page, props.website_url)
    discord_url = _url(page, props.discord_url)

    return Tournament(
        title=title,
        organization=org,
        starts_at=starts_at,
        start_is_date_only=start_is_date_only,
        format=fmt,
        entry_fee_eur=_number(page, props.entry_fee),
        prize_pool_eur=_number(page, props.prize_pool),
        website_url=website,
        discord_url=discord_url,
    )


def notion_incomplete_fields(
    t: Tournament,
    *,
    unreachable_icon_urls: Collection[str] | None = None,
) -> list[str]:
    """Human-readable labels for Notion properties that are empty on this row."""
    missing: list[str] = []
    org_stripped = (t.organization or "").strip()
    icon_url = find_icon(org_stripped) if org_stripped else None
    if not org_stripped:
        missing.append("organization")
    elif icon_url is None:
        # Org is set but does not map to a tournaments/ORG.png key (e.g. non-ASCII-only name).
        missing.append("organization logo")
    elif unreachable_icon_urls is not None and icon_url in unreachable_icon_urls:
        missing.append("organization logo (missing Supabase image)")
    if t.start_is_date_only:
        missing.append("Date & Time (date only — add a start time in Notion)")
    if not (t.format or "").strip():
        missing.append("format")
    if t.entry_fee_eur is None:
        missing.append("entry fee")
    if t.prize_pool_eur is None:
        missing.append("prize pool")
    if not (t.website_url or "").strip():
        missing.append("website URL")
    if not (t.discord_url or "").strip():
        missing.append("Discord URL")
    return missing


def notion_incomplete_data_warning(
    tournaments: Sequence[Tournament],
    *,
    max_items: int = 12,
    title_max: int = 72,
    unreachable_icon_urls: Collection[str] | None = None,
) -> str | None:
    """Ephemeral warning text when any tournament row is missing expected Notion fields."""
    detail_lines: list[str] = []
    for t in tournaments:
        missing = notion_incomplete_fields(t, unreachable_icon_urls=unreachable_icon_urls)
        if not missing:
            continue
        title = " ".join((t.title or "").split())
        if len(title) > title_max:
            title = title[: max(0, title_max - 1)].rstrip() + "…"
        detail_lines.append(f"• **{title}**: {', '.join(missing)}")
    if not detail_lines:
        return None
    shown = detail_lines[:max_items]
    tail = ""
    if len(detail_lines) > max_items:
        tail = f"\n… and {len(detail_lines) - max_items} more."
    return (
        "⚠️ **Notion data is not complete** — some fields are empty. Update Notion, then run this again:\n"
        + "\n".join(shown)
        + tail
    )


def notion_query_payload_for_today_cups(
    props: NotionProps,
    now: datetime | None = None,
) -> dict[str, Any]:
    now = now or datetime.now(tz=_CET)
    start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=1)

    return {
        "filter": {
            "and": [
                {"property": props.type, "select": {"equals": "Cup"}},
                {
                    "property": props.starts_at,
                    "date": {"on_or_after": start.isoformat(), "before": end.isoformat()},
                },
            ]
        },
        "sorts": [{"property": props.starts_at, "direction": "ascending"}],
        "page_size": 100,
    }


def discord_timestamp(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return f"<t:{int(dt.astimezone(timezone.utc).timestamp())}:t>"


def cet_day(dt: datetime) -> datetime.date:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(_CET).date()


def today_cet(now: datetime | None = None) -> datetime.date:
    now = now or datetime.now(tz=_CET)
    return now.date()


def detect_props(db: dict[str, Any]) -> NotionProps:
    props = db.get("properties") or {}

    def pick(prop_type: str, *needles: str) -> str | None:
        best: str | None = None
        for name, meta in props.items():
            if meta.get("type") != prop_type:
                continue
            n = name.lower()
            if all(x in n for x in needles):
                return name
            if best is None and any(x in n for x in needles):
                best = name
        return best

    title = next((n for n, m in props.items() if m.get("type") == "title"), None) or "Name"

    return NotionProps(
        title=title,
        organization=pick("select", "org") or "Organization",
        type=pick("select", "type") or "Type",
        starts_at=pick("date", "date") or pick("date", "time") or "Date & Time",
        format=pick("select", "format") or "Format",
        entry_fee=pick("number", "entry") or "Entry Fee",
        prize_pool=pick("number", "prize") or "Prize Pool",
        website_url=pick("url", "web") or "Website URL",
        discord_url=pick("url", "discord") or "Discord URL",
    )
