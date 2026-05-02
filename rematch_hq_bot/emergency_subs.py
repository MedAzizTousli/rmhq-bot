import asyncio
import os
from collections.abc import Iterable
from datetime import datetime, timedelta
from urllib.parse import quote, urlparse
from zoneinfo import ZoneInfo

try:
    import asyncpg
except ImportError as e:
    raise SystemExit(
        "Missing dependency: asyncpg\n"
        "Install it with: pip install -r requirements.txt"
    ) from e


ROLES = (
    "goalkeeper",
    "last_man",
    "second_defender",
    "second_striker",
    "main_striker",
)

_TABLES = {"emergency_subs", "emergency_requests"}
_REQUEST_TABLE = "emergency_requests"
_SUPABASE_DIRECT_HOST = "db.onxbmvcalbgdcyxxmwrz.supabase.co"
_SUPABASE_POOLER_HOST = "aws-0-eu-west-1.pooler.supabase.com"
_SUPABASE_POOLER_PORT = 5432
_SUPABASE_POOLER_USER = "postgres.onxbmvcalbgdcyxxmwrz"
_SUPABASE_DATABASE = "postgres"


_ALLOWED_SUPABASE_HOSTS = {_SUPABASE_DIRECT_HOST, _SUPABASE_POOLER_HOST}
_pool: asyncpg.Pool | None = None
_schema_ready = False
_schema_lock = asyncio.Lock()
_CEST = ZoneInfo("Europe/Paris")


def _expires_at_end_of_today_cest() -> datetime:
    now = datetime.now(_CEST)
    return datetime(now.year, now.month, now.day, tzinfo=_CEST) + timedelta(days=1)


def _with_sslmode_require(url: str) -> str:
    if "sslmode=" in url:
        return url
    separator = "&" if "?" in url else "?"
    return f"{url}{separator}sslmode=require"


def _database_url() -> str:
    url = os.getenv("DATABASE_URL", "").strip()
    password = os.getenv("SUPABASE_PASSWORD", "").strip()
    escaped_password = quote(password, safe="") if password else ""

    if escaped_password:
        return _with_sslmode_require(
            f"postgresql://{_SUPABASE_POOLER_USER}:{escaped_password}"
            f"@{_SUPABASE_POOLER_HOST}:{_SUPABASE_POOLER_PORT}/{_SUPABASE_DATABASE}"
        )

    if url:
        if "[YOUR-PASSWORD]" in url or "[SUPABASE_PASSWORD]" in url:
            raise RuntimeError("`DATABASE_URL` uses a password placeholder, but `SUPABASE_PASSWORD` is missing.")
        parsed = urlparse(url)
        if parsed.hostname not in _ALLOWED_SUPABASE_HOSTS:
            raise RuntimeError(
                "`DATABASE_URL` is malformed. Expected Supabase host "
                f"`{_SUPABASE_POOLER_HOST}` or `{_SUPABASE_DIRECT_HOST}`, got `{parsed.hostname or 'none'}`."
            )
        return _with_sslmode_require(url)

    raise RuntimeError("Missing `DATABASE_URL` or `SUPABASE_PASSWORD` env var.")


def database_diagnostics() -> dict[str, str | bool | int | None]:
    """Return connection diagnostics that are safe to print in logs."""
    raw_url = os.getenv("DATABASE_URL", "").strip()
    raw_password = os.getenv("SUPABASE_PASSWORD", "").strip()
    diagnostics: dict[str, str | bool | int | None] = {
        "DATABASE_URL_set": bool(raw_url),
        "SUPABASE_PASSWORD_set": bool(raw_password),
        "source": "SUPABASE_PASSWORD" if raw_password else "DATABASE_URL" if raw_url else "none",
        "DATABASE_URL_ignored": bool(raw_url and raw_password),
        "expected_hosts": ", ".join(sorted(_ALLOWED_SUPABASE_HOSTS)),
        "default_host": _SUPABASE_POOLER_HOST,
    }

    try:
        parsed = urlparse(_database_url())
    except Exception as e:
        diagnostics["url_error_type"] = type(e).__name__
        diagnostics["url_error"] = str(e)
        if raw_url:
            raw_parsed = urlparse(raw_url)
            diagnostics["raw_DATABASE_URL_host"] = raw_parsed.hostname
        return diagnostics

    diagnostics.update(
        {
            "scheme": parsed.scheme,
            "host": parsed.hostname,
            "port": parsed.port,
            "database": (parsed.path or "").lstrip("/"),
            "username": parsed.username,
            "password_present_in_url": parsed.password is not None,
            "password_contains_raw_question_mark": "?" in (parsed.password or ""),
            "sslmode": "require" if "sslmode=require" in parsed.query else parsed.query,
        }
    )
    return diagnostics


def _validate_role(role: str) -> str:
    clean = (role or "").strip().lower()
    if clean not in ROLES:
        raise ValueError(f"Invalid emergency role: {role!r}")
    return clean


def _validate_roles(roles: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for role in roles:
        clean = _validate_role(role)
        if clean in seen:
            continue
        seen.add(clean)
        out.append(clean)
    return out


def _validate_table(table: str) -> str:
    if table not in _TABLES:
        raise ValueError(f"Invalid emergency table: {table!r}")
    return table


async def _get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(
            dsn=_database_url(),
            min_size=1,
            max_size=5,
            command_timeout=10,
            timeout=10,
        )
    return _pool


async def get_pool() -> asyncpg.Pool:
    return await _get_pool()


async def ensure_schema() -> None:
    global _schema_ready
    if _schema_ready:
        return

    async with _schema_lock:
        if _schema_ready:
            return

        pool = await _get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS emergency_subs (
                    user_id TEXT NOT NULL,
                    role TEXT NOT NULL,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    last_update TIMESTAMPTZ DEFAULT NOW(),
                    expires_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (user_id, role)
                );
                """
            )
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS emergency_requests (
                    user_id TEXT NOT NULL,
                    role TEXT NOT NULL,
                    team_name TEXT NOT NULL DEFAULT 'Unknown Team',
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    last_update TIMESTAMPTZ DEFAULT NOW(),
                    expires_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (user_id, role)
                );
                """
            )
            for table in _TABLES:
                await conn.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW()")
                await conn.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS last_update TIMESTAMPTZ DEFAULT NOW()")
                await conn.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS expires_at TIMESTAMPTZ")
                await conn.execute(f"UPDATE {table} SET created_at = COALESCE(created_at, NOW())")
                await conn.execute(f"UPDATE {table} SET last_update = COALESCE(last_update, NOW())")
                await conn.execute(
                    f"UPDATE {table} SET expires_at = COALESCE(expires_at, NOW())"
                )
                await conn.execute(f"ALTER TABLE {table} ALTER COLUMN created_at SET DEFAULT NOW()")
                await conn.execute(f"ALTER TABLE {table} ALTER COLUMN last_update SET DEFAULT NOW()")
                await conn.execute(f"ALTER TABLE {table} ALTER COLUMN expires_at SET NOT NULL")

            await conn.execute(
                "ALTER TABLE emergency_requests ADD COLUMN IF NOT EXISTS team_name TEXT"
            )
            await conn.execute(
                "UPDATE emergency_requests SET team_name = 'Unknown Team' WHERE team_name IS NULL OR btrim(team_name) = ''"
            )
            await conn.execute("ALTER TABLE emergency_requests ALTER COLUMN team_name SET NOT NULL")
            await conn.execute("ALTER TABLE emergency_requests ALTER COLUMN team_name SET DEFAULT 'Unknown Team'")
        _schema_ready = True


async def deleteExpiredRows() -> None:
    await ensure_schema()
    pool = await _get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            for table in _TABLES:
                await conn.execute(f"DELETE FROM {table} WHERE expires_at <= NOW()")


async def _get_user_roles(table: str, user_id: str) -> list[str]:
    table = _validate_table(table)
    await deleteExpiredRows()
    await ensure_schema()
    pool = await _get_pool()
    rows = await pool.fetch(
        f"SELECT role FROM {table} WHERE user_id = $1 AND expires_at > NOW() ORDER BY role",
        str(user_id),
    )
    return [str(row["role"]) for row in rows]


async def _set_user_roles(table: str, user_id: str, roles: Iterable[str], *, team_name: str | None = None) -> None:
    table = _validate_table(table)
    clean_roles = _validate_roles(roles)
    await deleteExpiredRows()
    pool = await _get_pool()
    expires_at = _expires_at_end_of_today_cest()
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(f"DELETE FROM {table} WHERE user_id = $1", str(user_id))
            if clean_roles:
                if table == _REQUEST_TABLE:
                    clean_team_name = " ".join((team_name or "").split())[:50] or "Unknown Team"
                    await conn.executemany(
                        """
                        INSERT INTO emergency_requests (user_id, role, team_name, last_update, expires_at)
                        VALUES ($1, $2, $3, NOW(), $4)
                        """,
                        [(str(user_id), role, clean_team_name, expires_at) for role in clean_roles],
                    )
                else:
                    await conn.executemany(
                        """
                        INSERT INTO emergency_subs (user_id, role, last_update, expires_at)
                        VALUES ($1, $2, NOW(), $3)
                        """,
                        [(str(user_id), role, expires_at) for role in clean_roles],
                    )


async def getUserSubRoles(userId: str) -> list[str]:
    return await _get_user_roles("emergency_subs", userId)


async def setUserSubRoles(userId: str, roles: Iterable[str]) -> None:
    await _set_user_roles("emergency_subs", userId, roles)


async def getUserRequestRoles(userId: str) -> list[str]:
    return await _get_user_roles("emergency_requests", userId)


async def setUserRequestRoles(userId: str, roles: Iterable[str]) -> None:
    await _set_user_roles("emergency_requests", userId, roles)


async def setUserRequestRolesForTeam(userId: str, roles: Iterable[str], teamName: str) -> None:
    await _set_user_roles("emergency_requests", userId, roles, team_name=teamName)


async def clearUserSubRoles(userId: str) -> None:
    await deleteExpiredRows()
    pool = await _get_pool()
    await pool.execute("DELETE FROM emergency_subs WHERE user_id = $1", str(userId))


async def clearUserRequestRoles(userId: str) -> None:
    await deleteExpiredRows()
    pool = await _get_pool()
    await pool.execute("DELETE FROM emergency_requests WHERE user_id = $1", str(userId))


async def clearAllEmergencyRows() -> None:
    await ensure_schema()
    pool = await _get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("DELETE FROM emergency_subs")
            await conn.execute("DELETE FROM emergency_requests")


async def getUsersByRole(table: str, role: str) -> list[dict[str, str]]:
    table = _validate_table(table)
    role = _validate_role(role)
    await deleteExpiredRows()
    pool = await _get_pool()
    if table == _REQUEST_TABLE:
        rows = await pool.fetch(
            """
            SELECT user_id, team_name
            FROM emergency_requests
            WHERE role = $1 AND expires_at > NOW()
            ORDER BY last_update DESC, user_id
            """,
            role,
        )
        return [{"user_id": str(row["user_id"]), "team_name": str(row["team_name"])} for row in rows]

    rows = await pool.fetch(
        """
        SELECT user_id
        FROM emergency_subs
        WHERE role = $1 AND expires_at > NOW()
        ORDER BY last_update DESC, user_id
        """,
        role,
    )
    return [{"user_id": str(row["user_id"])} for row in rows]
