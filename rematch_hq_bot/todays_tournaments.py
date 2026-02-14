from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo


_CET = ZoneInfo("Europe/Paris")


@dataclass(frozen=True)
class Tournament:
    title: str
    organization: str
    starts_at: datetime
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


def _date(page: dict[str, Any], name: str) -> datetime | None:
    p = _get_prop(page, name) or {}
    d = p.get("date") or {}
    start = d.get("start")
    if not start:
        return None
    # Handles "Z" and offsets.
    s = str(start).replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def extract_tournament(page: dict[str, Any], props: NotionProps) -> Tournament | None:
    title = _title(page, props.title)
    if not title:
        return None

    org = _select(page, props.organization)
    t_type = _select(page, props.type)
    fmt = _select(page, props.format)
    starts_at = _date(page, props.starts_at)

    if t_type != "Cup":
        return None
    if fmt != "5v5":
        return None
    if not starts_at:
        return None

    website = _url(page, props.website_url)
    discord_url = _url(page, props.discord_url)

    return Tournament(
        title=title,
        organization=org,
        starts_at=starts_at,
        format=fmt,
        entry_fee_eur=_number(page, props.entry_fee),
        prize_pool_eur=_number(page, props.prize_pool),
        website_url=website,
        discord_url=discord_url,
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
                {"property": props.format, "select": {"equals": "5v5"}},
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
