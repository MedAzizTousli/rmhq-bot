import os

from dotenv import load_dotenv

load_dotenv()

try:
    import yaml
except ImportError as e:
    raise SystemExit(
        "Missing dependency: PyYAML\n"
        "Install it with: pip install -r requirements.txt"
    ) from e

from dataclasses import dataclass
from pathlib import Path


def _get_required(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise SystemExit(f"Missing {name} env var.")
    return value


TOKEN = _get_required("DISCORD_TOKEN")

GUILD_ID_RAW = (os.getenv("DISCORD_GUILD_ID") or os.getenv("GUILD_ID") or "").strip()


def _parse_guild_ids(raw: str) -> tuple[int, ...]:
    if not raw:
        return ()

    guild_ids: list[int] = []
    for part in raw.split(","):
        value = part.strip()
        if not value:
            continue
        try:
            guild_ids.append(int(value))
        except ValueError as e:
            raise SystemExit(
                "`DISCORD_GUILD_ID` / `GUILD_ID` must contain numeric Discord server IDs, "
                "optionally comma-separated."
            ) from e
    return tuple(dict.fromkeys(guild_ids))


GUILD_IDS = _parse_guild_ids(GUILD_ID_RAW)
GUILD_ID = GUILD_IDS[0] if GUILD_IDS else None

SYNC_COMMANDS_ON_STARTUP = os.getenv("SYNC_COMMANDS_ON_STARTUP", "1").strip().lower() not in {
    "0",
    "false",
    "no",
}

# Notion (optional, required for "Today's tournaments" / "Earnings" buttons)
NOTION_TOKEN = os.getenv("NOTION_TOKEN", "").strip()
NOTION_DATABASE_ID = os.getenv("NOTION_DATABASE_ID", "").strip()
PRIZE_POOL_NOTION_DATABASE_ID = os.getenv("PRIZE_POOL_NOTION_DATABASE_ID", "").strip()


@dataclass(frozen=True)
class ServerConfig:
    name: str
    server_id: int

    # Optional: restrict /setup + buttons to a single channel in that server.
    setup_channel_id: int | None = None

    # Channels used by features (optional depending on server).
    test_channel_id: int | None = None
    results_tournaments_channel_id: int | None = None
    upcoming_tournaments_channel_id: int | None = None
    leaderboard_channel_id: int | dict[str, int] | None = None
    rosters_channel_id: int | None = None
    earnings_channel_id: int | None = None
    supreme_earner_role_id: int | None = None
    top_earner_role_id: int | None = None
    scrim_forum_channel_id: int | None = None
    scrim_forum_user_id_exclude: int | None = None

    # Mention target posted alongside results / tournaments (role preferred, else user).
    tournaments_ping_id: int | None = None

    # Compliments feature configuration.
    compliments_channel_id: int | None = None
    compliments_ping_id: int | None = None

    # GG channel (message counts for "Class of the Month").
    gg_channel_id: int | None = None
    emergency_pings_channel_id: int | None = None
    birthdays_channel_id: int | None = None
    birthdays_role_id: int | None = None
    giveaways_channel_id: int | None = None
    giveaways_role_id: int | None = None

    # Minimum role ID - team roles will be positioned above this role.
    minimum_role_id: int | None = None

    # Setup-part settings (per tournament type like "PRT", "ART").
    tournament_info_channel_id: dict[str, int] | None = None
    hall_of_fame_channel_id: int | dict[str, int] | None = None
    sponsors_channel_id: dict[str, int] | None = None
    embed_color: dict[str, int] | None = None
    prize_pool: dict[str, float] | None = None


_REPO_ROOT = Path(__file__).resolve().parents[1]
_CONFIG_YAML = _REPO_ROOT / "config.yaml"


def _as_int(v) -> int | None:
    if v is None:
        return None
    if isinstance(v, int):
        return v
    s = str(v).strip()
    if not s:
        return None
    try:
        # Accept "0x..." hex strings too.
        return int(s, 0)
    except ValueError:
        return None


def _parse_map_int(v) -> dict[str, int] | None:
    if v is None:
        return None
    if not isinstance(v, dict):
        return None
    out: dict[str, int] = {}
    for k, raw_val in v.items():
        key = str(k).strip().upper()
        val = _as_int(raw_val)
        if key and val is not None:
            out[key] = val
    return out or None


def _parse_int_or_map_int(v) -> int | dict[str, int] | None:
    scalar = _as_int(v)
    if scalar is not None:
        return scalar
    return _parse_map_int(v)


def _parse_map_float(v) -> dict[str, float] | None:
    if v is None:
        return None
    if not isinstance(v, dict):
        return None
    out: dict[str, float] = {}
    for k, raw_val in v.items():
        key = str(k).strip().upper()
        if not key:
            continue
        try:
            if raw_val is None:
                continue
            out[key] = float(raw_val)
        except (TypeError, ValueError):
            continue
    return out or None


def _load_servers() -> dict[int, ServerConfig]:
    if not _CONFIG_YAML.exists():
        raise SystemExit(f"Missing config file: {_CONFIG_YAML}")

    with _CONFIG_YAML.open("r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}
    if not isinstance(raw, dict):
        raise SystemExit("Invalid config.yaml: expected a mapping at top-level.")

    servers: dict[int, ServerConfig] = {}
    for name, block in raw.items():
        if not isinstance(block, dict):
            continue

        server_id_field = block.get("SERVER_ID")

        # Allow either a single SERVER_ID or a mapping like
        # SERVER_ID: { PRT: 123, ART: 123, FRT: 456 }
        server_ids: list[int] = []
        if isinstance(server_id_field, dict):
            for _, raw_val in server_id_field.items():
                sid = _as_int(raw_val)
                if sid:
                    server_ids.append(sid)
        else:
            sid = _as_int(server_id_field)
            if sid:
                server_ids.append(sid)

        if not server_ids:
            continue

        for server_id in server_ids:
            cfg = ServerConfig(
                name=str(name),
                server_id=server_id,
                setup_channel_id=_as_int(block.get("SETUP_CHANNEL_ID")),
                test_channel_id=_as_int(block.get("TEST_CHANNEL_ID")),
                results_tournaments_channel_id=_as_int(block.get("RESULTS_TOURNAMENTS_CHANNEL_ID")),
                upcoming_tournaments_channel_id=_as_int(block.get("UPCOMING_TOURNAMENTS_CHANNEL_ID")),
                leaderboard_channel_id=_parse_int_or_map_int(block.get("LEADERBOARD_CHANNEL_ID")),
                rosters_channel_id=_as_int(block.get("ROSTERS_CHANNEL_ID")),
                earnings_channel_id=_as_int(block.get("EARNINGS_CHANNEL_ID")),
                supreme_earner_role_id=_as_int(block.get("SUPREME_EARNER_ROLE_ID")),
                top_earner_role_id=_as_int(block.get("TOP_EARNER_ROLE_ID")),
                scrim_forum_channel_id=_as_int(block.get("SCRIM_FORUM_CHANNEL_ID")),
                scrim_forum_user_id_exclude=_as_int(block.get("SCRIM_FORUM_USER_ID_EXCLUDE")),
                tournaments_ping_id=_as_int(block.get("TOURNAMENTS_PING_ID")),
                compliments_channel_id=_as_int(block.get("COMPLIMENTS_CHANNEL_ID")),
                compliments_ping_id=_as_int(block.get("COMPLIMENTS_PING_ID")),
                gg_channel_id=_as_int(block.get("GG_CHANNEL_ID")),
                emergency_pings_channel_id=_as_int(block.get("EMERGENCY_PINGS_CHANNEL_ID")),
                birthdays_channel_id=_as_int(block.get("BIRTHDAYS_CHANNEL_ID")),
                birthdays_role_id=_as_int(block.get("BIRTHDAYS_ROLE_ID")),
                giveaways_channel_id=_as_int(block.get("GIVEAWAYS_CHANNEL_ID")),
                giveaways_role_id=_as_int(block.get("GIVEAWAYS_ROLE_ID")),
                minimum_role_id=_as_int(block.get("MINIMUM_ROLE_ID")),
                tournament_info_channel_id=_parse_map_int(block.get("TOURNAMENT_INFO_CHANNEL_ID")),
                hall_of_fame_channel_id=_parse_int_or_map_int(block.get("HALL_OF_FAME_CHANNEL_ID")),
                sponsors_channel_id=_parse_map_int(block.get("SPONSORS_CHANNEL_ID")),
                embed_color=_parse_map_int(block.get("EMBED_COLOR")),
                prize_pool=_parse_map_float(block.get("PRIZE_POOL")),
            )
            servers[server_id] = cfg

    if not servers:
        raise SystemExit("No servers found in config.yaml (missing SERVER_ID blocks).")

    return servers


def server_for_guild_id(guild_id: int) -> ServerConfig | None:
    return SERVERS_BY_ID.get(int(guild_id))


def is_allowed_setup_channel(*, guild_id: int, channel_id: int) -> bool:
    cfg = server_for_guild_id(guild_id)
    # If server not configured, allow anywhere (but features may fail later).
    if cfg is None or cfg.setup_channel_id is None:
        return True
    return int(channel_id) == int(cfg.setup_channel_id)


SERVERS_BY_ID: dict[int, ServerConfig] = _load_servers()
BIRTHDAYS_CHANNEL_ID = _as_int(os.getenv("BIRTHDAYS_CHANNEL_ID")) or next(
    (cfg.birthdays_channel_id for cfg in SERVERS_BY_ID.values() if cfg.birthdays_channel_id is not None),
    None,
)
BIRTHDAYS_ROLE_ID = _as_int(os.getenv("BIRTHDAYS_ROLE_ID")) or next(
    (cfg.birthdays_role_id for cfg in SERVERS_BY_ID.values() if cfg.birthdays_role_id is not None),
    None,
)
GIVEAWAYS_CHANNEL_ID = _as_int(os.getenv("GIVEAWAYS_CHANNEL_ID")) or next(
    (cfg.giveaways_channel_id for cfg in SERVERS_BY_ID.values() if cfg.giveaways_channel_id is not None),
    None,
)
GIVEAWAYS_ROLE_ID = _as_int(os.getenv("GIVEAWAYS_ROLE_ID")) or next(
    (cfg.giveaways_role_id for cfg in SERVERS_BY_ID.values() if cfg.giveaways_role_id is not None),
    None,
)


def _load_emergency_subs_roles() -> dict[str, int]:
    with _CONFIG_YAML.open("r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}
    if not isinstance(raw, dict):
        return {}
    roles = raw.get("EMERGENCY_SUBS_ROLES")
    if not isinstance(roles, dict):
        return {}
    out: dict[str, int] = {}
    for key, value in roles.items():
        role_id = _as_int(value)
        if role_id is not None:
            out[str(key).strip().lower()] = role_id
    return out


EMERGENCY_SUBS_ROLES: dict[str, int] = _load_emergency_subs_roles()
