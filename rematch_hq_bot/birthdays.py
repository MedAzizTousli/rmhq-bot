import asyncio
import re
from dataclasses import dataclass
from datetime import datetime

from . import emergency_subs


MONTH_NAMES = {
    1: "January",
    2: "February",
    3: "March",
    4: "April",
    5: "May",
    6: "June",
    7: "July",
    8: "August",
    9: "September",
    10: "October",
    11: "November",
    12: "December",
}

_MONTH_ALIASES = {
    "jan": 1,
    "january": 1,
    "feb": 2,
    "february": 2,
    "mar": 3,
    "march": 3,
    "apr": 4,
    "april": 4,
    "may": 5,
    "jun": 6,
    "june": 6,
    "jul": 7,
    "july": 7,
    "aug": 8,
    "august": 8,
    "sep": 9,
    "sept": 9,
    "september": 9,
    "oct": 10,
    "october": 10,
    "nov": 11,
    "november": 11,
    "dec": 12,
    "december": 12,
}

_DAYS_IN_MONTH = {
    1: 31,
    2: 29,
    3: 31,
    4: 30,
    5: 31,
    6: 30,
    7: 31,
    8: 31,
    9: 30,
    10: 31,
    11: 30,
    12: 31,
}

_schema_ready = False
_schema_lock = asyncio.Lock()


@dataclass(frozen=True)
class Birthday:
    user_id: str
    day: int
    month: int


@dataclass(frozen=True)
class BirthdayRoleAssignment:
    guild_id: str
    user_id: str
    role_id: str


class BirthdayParseError(ValueError):
    pass


def _parse_month(value: str) -> int | None:
    clean = re.sub(r"[^a-z]", "", value.lower())
    return _MONTH_ALIASES.get(clean)


def _validate_day_month(day: int, month: int) -> tuple[int, int]:
    if month < 1 or month > 12:
        raise BirthdayParseError("Month must be between 1 and 12.")
    max_day = _DAYS_IN_MONTH[month]
    if day < 1 or day > max_day:
        raise BirthdayParseError(f"{MONTH_NAMES[month]} only has {max_day} days.")
    return day, month


def parse_birthday_input(raw: str) -> tuple[int, int]:
    value = " ".join((raw or "").strip().replace(",", " ").replace(".", " ").split())
    if not value:
        raise BirthdayParseError("Please enter your birthday.")

    numeric = re.fullmatch(r"(\d{1,2})\s*/\s*(\d{1,2})", value)
    if numeric:
        return _validate_day_month(int(numeric.group(1)), int(numeric.group(2)))

    parts = value.split()
    if len(parts) != 2:
        raise BirthdayParseError("Use a day and month only, without a year.")

    first_month = _parse_month(parts[0])
    second_month = _parse_month(parts[1])

    if first_month is not None and parts[1].isdigit():
        return _validate_day_month(int(parts[1]), first_month)
    if second_month is not None and parts[0].isdigit():
        return _validate_day_month(int(parts[0]), second_month)

    raise BirthdayParseError("Could not understand that birthday format.")


def format_birthday(day: int, month: int) -> str:
    return f"{day:02d} {MONTH_NAMES[month]}"


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
                CREATE TABLE IF NOT EXISTS birthdays (
                    user_id TEXT PRIMARY KEY,
                    day INT NOT NULL,
                    month INT NOT NULL,
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW()
                );
                """
            )
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS birthday_announcements (
                    date TEXT PRIMARY KEY,
                    sent_at TIMESTAMP DEFAULT NOW()
                );
                """
            )
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS birthday_role_assignments (
                    guild_id TEXT NOT NULL,
                    user_id TEXT NOT NULL,
                    role_id TEXT NOT NULL,
                    remove_at TIMESTAMPTZ NOT NULL,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    PRIMARY KEY (guild_id, user_id, role_id)
                );
                """
            )
        _schema_ready = True


async def save_birthday(user_id: str, day: int, month: int) -> None:
    day, month = _validate_day_month(day, month)
    await ensure_schema()
    pool = await emergency_subs.get_pool()
    await pool.execute(
        """
        INSERT INTO birthdays (user_id, day, month, updated_at)
        VALUES ($1, $2, $3, NOW())
        ON CONFLICT (user_id) DO UPDATE
        SET day = EXCLUDED.day,
            month = EXCLUDED.month,
            updated_at = NOW()
        """,
        str(user_id),
        day,
        month,
    )


async def birthdays_for(day: int, month: int) -> list[Birthday]:
    day, month = _validate_day_month(day, month)
    await ensure_schema()
    pool = await emergency_subs.get_pool()
    rows = await pool.fetch(
        """
        SELECT user_id, day, month
        FROM birthdays
        WHERE day = $1 AND month = $2
        ORDER BY user_id
        """,
        day,
        month,
    )
    return [Birthday(user_id=str(row["user_id"]), day=int(row["day"]), month=int(row["month"])) for row in rows]


async def all_birthdays() -> list[Birthday]:
    await ensure_schema()
    pool = await emergency_subs.get_pool()
    rows = await pool.fetch(
        """
        SELECT user_id, day, month
        FROM birthdays
        ORDER BY month, day, user_id
        """
    )
    return [Birthday(user_id=str(row["user_id"]), day=int(row["day"]), month=int(row["month"])) for row in rows]


async def delete_birthday(user_id: str) -> bool:
    await ensure_schema()
    pool = await emergency_subs.get_pool()
    result = await pool.execute("DELETE FROM birthdays WHERE user_id = $1", str(user_id))
    return result.endswith(" 1")


async def delete_birthdays(user_ids: list[str]) -> int:
    if not user_ids:
        return 0
    await ensure_schema()
    pool = await emergency_subs.get_pool()
    result = await pool.execute("DELETE FROM birthdays WHERE user_id = ANY($1::text[])", [str(user_id) for user_id in user_ids])
    try:
        return int(result.rsplit(" ", 1)[-1])
    except (IndexError, ValueError):
        return 0


async def claim_announcement_date(date_text: str) -> bool:
    await ensure_schema()
    pool = await emergency_subs.get_pool()
    row = await pool.fetchrow(
        """
        INSERT INTO birthday_announcements (date)
        VALUES ($1)
        ON CONFLICT (date) DO NOTHING
        RETURNING date
        """,
        date_text,
    )
    return row is not None


async def release_announcement_date(date_text: str) -> None:
    await ensure_schema()
    pool = await emergency_subs.get_pool()
    await pool.execute("DELETE FROM birthday_announcements WHERE date = $1", date_text)


async def record_role_assignment(guild_id: int, user_id: int, role_id: int, remove_at: datetime) -> None:
    await ensure_schema()
    pool = await emergency_subs.get_pool()
    await pool.execute(
        """
        INSERT INTO birthday_role_assignments (guild_id, user_id, role_id, remove_at)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (guild_id, user_id, role_id) DO UPDATE
        SET remove_at = EXCLUDED.remove_at,
            created_at = NOW()
        """,
        str(guild_id),
        str(user_id),
        str(role_id),
        remove_at,
    )


async def due_role_assignments() -> list[BirthdayRoleAssignment]:
    await ensure_schema()
    pool = await emergency_subs.get_pool()
    rows = await pool.fetch(
        """
        SELECT guild_id, user_id, role_id
        FROM birthday_role_assignments
        WHERE remove_at <= NOW()
        ORDER BY remove_at, guild_id, user_id
        """
    )
    return [
        BirthdayRoleAssignment(
            guild_id=str(row["guild_id"]),
            user_id=str(row["user_id"]),
            role_id=str(row["role_id"]),
        )
        for row in rows
    ]


async def delete_role_assignment(guild_id: str, user_id: str, role_id: str) -> None:
    await ensure_schema()
    pool = await emergency_subs.get_pool()
    await pool.execute(
        """
        DELETE FROM birthday_role_assignments
        WHERE guild_id = $1 AND user_id = $2 AND role_id = $3
        """,
        str(guild_id),
        str(user_id),
        str(role_id),
    )


async def clear_role_assignments_for_guild_role(guild_id: int, role_id: int) -> None:
    await ensure_schema()
    pool = await emergency_subs.get_pool()
    await pool.execute(
        """
        DELETE FROM birthday_role_assignments
        WHERE guild_id = $1 AND role_id = $2
        """,
        str(guild_id),
        str(role_id),
    )
