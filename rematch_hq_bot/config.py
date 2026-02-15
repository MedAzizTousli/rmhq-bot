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

GUILD_ID_RAW = os.getenv("DISCORD_GUILD_ID", "").strip()
GUILD_ID = int(GUILD_ID_RAW) if GUILD_ID_RAW else None

SYNC_COMMANDS_ON_STARTUP = os.getenv("SYNC_COMMANDS_ON_STARTUP", "1").strip().lower() not in {
    "0",
    "false",
    "no",
}

# Notion (optional, required for "Today's tournaments" button)
NOTION_TOKEN = os.getenv("NOTION_TOKEN", "").strip()
NOTION_DATABASE_ID = os.getenv("NOTION_DATABASE_ID", "").strip()

# Scrims forum (optional; used by /setup purge button)
SCRIM_FORUM_CHANNEL_ID_RAW = os.getenv("SCRIM_FORUM_CHANNEL_ID", "").strip()
try:
    SCRIM_FORUM_CHANNEL_ID = int(SCRIM_FORUM_CHANNEL_ID_RAW) if SCRIM_FORUM_CHANNEL_ID_RAW else None
except ValueError:
    raise SystemExit("Invalid SCRIM_FORUM_CHANNEL_ID env var (expected an integer channel id).") from None

# Optional: when purging scrims forum, exclude posts (threads) created by this user id.
SCRIM_FORUM_USER_ID_EXCLUDE_RAW = os.getenv("SCRIM_FORUM_USER_ID_EXCLUDE", "").strip()
try:
    SCRIM_FORUM_USER_ID_EXCLUDE = (
        int(SCRIM_FORUM_USER_ID_EXCLUDE_RAW) if SCRIM_FORUM_USER_ID_EXCLUDE_RAW else None
    )
except ValueError:
    raise SystemExit(
        "Invalid SCRIM_FORUM_USER_ID_EXCLUDE env var (expected an integer user id)."
    ) from None


@dataclass(frozen=True)
class ServerConfig:
    name: str
    server_id: int

    # Optional: restrict /setup + buttons to a single channel in that server.
    setup_channel_id: int | None = None

    # Channels used by features (optional depending on server).
    results_tournaments_channel_id: int | None = None
    upcoming_tournaments_channel_id: int | None = None
    leaderboard_channel_id: int | None = None

    # Mention target posted alongside results / tournaments (role preferred, else user).
    tournaments_ping_id: int | None = None

    # Setup-part settings (per tournament type like "PRT", "ART").
    tournament_info_channel_id: dict[str, int] | None = None
    hall_of_fame_channel_id: dict[str, int] | None = None
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
        server_id = _as_int(block.get("SERVER_ID"))
        if not server_id:
            continue

        cfg = ServerConfig(
            name=str(name),
            server_id=server_id,
            setup_channel_id=_as_int(block.get("SETUP_CHANNEL_ID")),
            results_tournaments_channel_id=_as_int(block.get("RESULTS_TOURNAMENTS_CHANNEL_ID")),
            upcoming_tournaments_channel_id=_as_int(block.get("UPCOMING_TOURNAMENTS_CHANNEL_ID")),
            leaderboard_channel_id=_as_int(block.get("LEADERBOARD_CHANNEL_ID")),
            tournaments_ping_id=_as_int(block.get("TOURNAMENTS_PING_ID")),
            tournament_info_channel_id=_parse_map_int(block.get("TOURNAMENT_INFO_CHANNEL_ID")),
            hall_of_fame_channel_id=_parse_map_int(block.get("HALL_OF_FAME_CHANNEL_ID")),
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
