import asyncio
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import discord

from . import emergency_subs


DEFAULT_DURATION = timedelta(days=7)
DEFAULT_PRIZE = "10€ Gift Card (Your Choice)"
DEFAULT_PROVIDER = "Rematch HQ"

_schema_ready = False
_schema_lock = asyncio.Lock()
_DURATION_RE = re.compile(r"^\s*(\d+)\s*([a-zA-Z]+)\s*$")


@dataclass(frozen=True)
class Giveaway:
    id: int
    message_id: str | None
    channel_id: str
    guild_id: str
    prize: str
    provider: str
    winners_count: int
    ends_at: datetime
    created_by: str
    ended: bool


class GiveawayInputError(ValueError):
    pass


def _utcnow_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def parse_duration(raw: str) -> timedelta:
    value = " ".join((raw or "").strip().lower().split())
    if not value:
        return DEFAULT_DURATION

    match = _DURATION_RE.fullmatch(value)
    if not match:
        raise GiveawayInputError("Use a duration like `10 minutes`, `2h`, `7 days`, or `1 week`.")

    amount = int(match.group(1))
    unit = match.group(2)
    if amount <= 0:
        raise GiveawayInputError("Duration must be a positive number.")

    if unit in {"m", "min", "mins", "minute", "minutes"}:
        return timedelta(minutes=amount)
    if unit in {"h", "hr", "hrs", "hour", "hours"}:
        return timedelta(hours=amount)
    if unit in {"d", "day", "days"}:
        return timedelta(days=amount)
    if unit in {"w", "week", "weeks"}:
        return timedelta(weeks=amount)

    raise GiveawayInputError("Use minutes, hours, days, or weeks for the duration.")


def parse_winners_count(raw: str) -> int:
    value = (raw or "").strip()
    if not value:
        return 1
    try:
        winners_count = int(value)
    except ValueError as e:
        raise GiveawayInputError("Number of winners must be a positive number.") from e
    if winners_count <= 0:
        raise GiveawayInputError("Number of winners must be a positive number.")
    return winners_count


def clean_prize(raw: str) -> str:
    return " ".join((raw or "").split())[:250] or DEFAULT_PRIZE


def clean_provider(raw: str) -> str:
    return " ".join((raw or "").split())[:120] or DEFAULT_PROVIDER


def discord_timestamp(dt: datetime) -> int:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


def giveaway_embed(
    giveaway: Giveaway,
    *,
    entries_count: int,
    winners_text: str | None = None,
) -> discord.Embed:
    ended = bool(winners_text is not None or giveaway.ended)
    ts = discord_timestamp(giveaway.ends_at)
    embed = discord.Embed(title=f"🎁 {giveaway.prize}", color=0x2F3136 if ended else 0xbe629b)
    embed.set_author(name=f"Provided by {giveaway.provider}")
    embed.add_field(
        name="Ends",
        value=f"Ended <t:{ts}:F>" if ended else f"<t:{ts}:R>",
        inline=True,
    )
    embed.add_field(
        name="Winners",
        value=winners_text if winners_text is not None else str(giveaway.winners_count),
        inline=True,
    )
    embed.add_field(name="Entries", value=str(entries_count), inline=False)
    embed.set_footer(text="Giveaway ended" if ended else "React to enter the giveaway!")
    return embed


def _row_to_giveaway(row) -> Giveaway:
    return Giveaway(
        id=int(row["id"]),
        message_id=str(row["message_id"]) if row["message_id"] is not None else None,
        channel_id=str(row["channel_id"]),
        guild_id=str(row["guild_id"]),
        prize=str(row["prize"]),
        provider=str(row["provider"]),
        winners_count=int(row["winners_count"]),
        ends_at=row["ends_at"],
        created_by=str(row["created_by"]),
        ended=bool(row["ended"]),
    )


async def ensure_schema() -> None:
    global _schema_ready
    if _schema_ready:
        return

    async with _schema_lock:
        if _schema_ready:
            return

        pool = await emergency_subs.get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS giveaways (
                    id SERIAL PRIMARY KEY,
                    message_id TEXT UNIQUE,
                    channel_id TEXT NOT NULL,
                    guild_id TEXT NOT NULL,
                    prize TEXT NOT NULL,
                    provider TEXT NOT NULL,
                    winners_count INT NOT NULL DEFAULT 1,
                    ends_at TIMESTAMP NOT NULL,
                    created_by TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT NOW(),
                    ended BOOLEAN DEFAULT FALSE,
                    ended_at TIMESTAMP
                );
                """
            )
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS giveaway_entries (
                    giveaway_id INT REFERENCES giveaways(id) ON DELETE CASCADE,
                    user_id TEXT NOT NULL,
                    entered_at TIMESTAMP DEFAULT NOW(),
                    PRIMARY KEY (giveaway_id, user_id)
                );
                """
            )
        _schema_ready = True


async def create_giveaway(
    *,
    channel_id: int,
    guild_id: int,
    prize: str,
    provider: str,
    winners_count: int,
    ends_at: datetime,
    created_by: int,
) -> Giveaway:
    await ensure_schema()
    pool = await emergency_subs.get_pool()
    row = await pool.fetchrow(
        """
        INSERT INTO giveaways (channel_id, guild_id, prize, provider, winners_count, ends_at, created_by)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        RETURNING *
        """,
        str(channel_id),
        str(guild_id),
        prize,
        provider,
        winners_count,
        ends_at,
        str(created_by),
    )
    return _row_to_giveaway(row)


async def set_message_id(giveaway_id: int, message_id: int) -> Giveaway:
    await ensure_schema()
    pool = await emergency_subs.get_pool()
    row = await pool.fetchrow(
        "UPDATE giveaways SET message_id = $2 WHERE id = $1 RETURNING *",
        giveaway_id,
        str(message_id),
    )
    return _row_to_giveaway(row)


async def get_giveaway(giveaway_id: int) -> Giveaway | None:
    await ensure_schema()
    pool = await emergency_subs.get_pool()
    row = await pool.fetchrow("SELECT * FROM giveaways WHERE id = $1", giveaway_id)
    return _row_to_giveaway(row) if row is not None else None


async def add_entry(giveaway_id: int, user_id: int) -> bool:
    await ensure_schema()
    pool = await emergency_subs.get_pool()
    row = await pool.fetchrow(
        """
        INSERT INTO giveaway_entries (giveaway_id, user_id)
        VALUES ($1, $2)
        ON CONFLICT (giveaway_id, user_id) DO NOTHING
        RETURNING giveaway_id
        """,
        giveaway_id,
        str(user_id),
    )
    return row is not None


async def entry_count(giveaway_id: int) -> int:
    await ensure_schema()
    pool = await emergency_subs.get_pool()
    return int(await pool.fetchval("SELECT COUNT(*) FROM giveaway_entries WHERE giveaway_id = $1", giveaway_id))


async def entries(giveaway_id: int) -> list[str]:
    await ensure_schema()
    pool = await emergency_subs.get_pool()
    rows = await pool.fetch(
        "SELECT user_id FROM giveaway_entries WHERE giveaway_id = $1 ORDER BY entered_at, user_id",
        giveaway_id,
    )
    return [str(row["user_id"]) for row in rows]


async def active_giveaways() -> list[Giveaway]:
    await ensure_schema()
    pool = await emergency_subs.get_pool()
    rows = await pool.fetch("SELECT * FROM giveaways WHERE ended = FALSE ORDER BY ends_at, id")
    return [_row_to_giveaway(row) for row in rows]


async def due_giveaways() -> list[Giveaway]:
    await ensure_schema()
    pool = await emergency_subs.get_pool()
    rows = await pool.fetch(
        "SELECT * FROM giveaways WHERE ended = FALSE AND ends_at <= $1 ORDER BY ends_at, id",
        _utcnow_naive(),
    )
    return [_row_to_giveaway(row) for row in rows]


async def mark_ended(giveaway_id: int) -> None:
    await ensure_schema()
    pool = await emergency_subs.get_pool()
    await pool.execute(
        "UPDATE giveaways SET ended = TRUE, ended_at = NOW() WHERE id = $1",
        giveaway_id,
    )
