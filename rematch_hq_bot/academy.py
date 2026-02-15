from __future__ import annotations

import asyncio
import heapq
from pathlib import Path

try:
    import yaml
except ImportError as e:
    raise SystemExit(
        "Missing dependency: PyYAML\n"
        "Install it with: pip install -r requirements.txt"
    ) from e


_REPO_ROOT = Path(__file__).resolve().parents[1]
ACADEMY_YAML_PATH = _REPO_ROOT / "academy/players.yaml"
TEAMS_YAML_PATH = _REPO_ROOT / "academy/teams.yaml"

# Serialize YAML edits to avoid races between multiple interactions.
_ACADEMY_LOCK = asyncio.Lock()


ROLES: tuple[str, ...] = (
    "Goalkeeper",
    "Main Defender",
    "Second Defender",
    "Second Striker",
    "Main Striker",
)


def _normalize_username(username: str) -> str:
    return " ".join((username or "").strip().split())


def _role_key(raw: object) -> str | None:
    s = " ".join(str(raw or "").strip().split())
    if not s:
        return None
    want = s.casefold()
    for r in ROLES:
        if r.casefold() == want:
            return r
    return None


def load_academy(path: Path = ACADEMY_YAML_PATH) -> dict[str, dict[str, int]]:
    """
    Load academy registrations.

    New storage format (role-first):

      Goalkeeper:
        - Alice: 3
        - Bob: 2
      Main Striker:
        - Carol: 3

    We also accept a mapping form for convenience:

      Goalkeeper:
        Alice: 3
        Bob: 2

    And we auto-read the legacy per-user YAML stream (one document per user) and
    convert it into the role-first in-memory structure.
    """
    out: dict[str, dict[str, int]] = {r: {} for r in ROLES}
    if not path.exists():
        return out

    with path.open("r", encoding="utf-8") as f:
        docs = list(yaml.safe_load_all(f))

    if not docs:
        return out

    # New format is a single top-level mapping where keys are roles.
    doc0 = docs[0]
    is_new_style = (
        len(docs) == 1
        and isinstance(doc0, dict)
        and any(_role_key(k) for k in doc0.keys())
    )

    if is_new_style:
        top = doc0  # type: ignore[assignment]
        for role_raw, entries in top.items():  # type: ignore[union-attr]
            role = _role_key(role_raw)
            if role is None:
                continue

            # Accept either:
            # - list of {username: tier} single-item dicts
            # - mapping {username: tier}
            role_map: dict[str, int] = {}
            if isinstance(entries, dict):
                items = list(entries.items())
            elif isinstance(entries, list):
                items = []
                for it in entries:
                    if isinstance(it, dict):
                        items.extend(list(it.items()))
            else:
                items = []

            for username_raw, tier_raw in items:
                username = _normalize_username(str(username_raw))
                if not username:
                    continue
                try:
                    tier = int(tier_raw)
                except (TypeError, ValueError):
                    tier = 3
                role_map[username] = tier

            out[role] = role_map

        return out

    # Legacy stream format: each document contains {username: {role: ..., tier: ...}}
    for doc in docs:
        if not isinstance(doc, dict):
            continue
        for username_raw, info in doc.items():
            username = _normalize_username(str(username_raw))
            if not username or not isinstance(info, dict):
                continue
            role = _role_key(info.get("role"))
            if role is None:
                continue
            tier_raw = info.get("tier", 3)
            try:
                tier = int(tier_raw)
            except (TypeError, ValueError):
                tier = 3
            out.setdefault(role, {})[username] = tier

    return out


def save_academy(academy: dict[str, dict[str, int]], path: Path = ACADEMY_YAML_PATH) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    normalized: dict[str, dict[str, int]] = {r: {} for r in ROLES}
    for role_raw, users in (academy or {}).items():
        role = _role_key(role_raw)
        if role is None or not isinstance(users, dict):
            continue
        for username_raw, tier_raw in users.items():
            username = _normalize_username(str(username_raw))
            if not username:
                continue
            try:
                tier = int(tier_raw)
            except (TypeError, ValueError):
                tier = 3
            normalized[role][username] = tier

    if all(not normalized[r] for r in ROLES):
        # If empty, remove file (best-effort).
        try:
            path.unlink(missing_ok=True)
        except OSError:
            with path.open("w", encoding="utf-8", newline="\n") as f:
                f.write("{}\n")
        return

    # Write as: role -> list[{username: tier}, ...]
    top: dict[str, list[dict[str, int]]] = {}
    for role in ROLES:
        users = normalized.get(role) or {}
        entries: list[dict[str, int]] = []
        for username in sorted(users.keys(), key=lambda s: s.casefold()):
            entries.append({username: int(users[username])})
        top[role] = entries

    text = yaml.safe_dump(top, sort_keys=False, default_flow_style=False) or ""
    with path.open("w", encoding="utf-8", newline="\n") as f:
        f.write(text.rstrip() + "\n")


async def register_player(
    *,
    username: str,
    roles: list[str] | tuple[str, ...] | set[str],
    default_tier: int = 3,
) -> dict[str, int]:
    u = _normalize_username(username)
    if not u:
        raise ValueError("username is required")

    chosen: list[str] = []
    for raw in roles or []:
        r = _role_key(raw)
        if r is None:
            raise ValueError("invalid role")
        if r not in chosen:
            chosen.append(r)
    if not chosen:
        raise ValueError("roles is required")

    async with _ACADEMY_LOCK:
        academy = load_academy()
        out: dict[str, int] = {}
        for r in chosen:
            current_tier = academy.get(r, {}).get(u)
            tier = int(current_tier) if current_tier is not None else int(default_tier)
            academy.setdefault(r, {})[u] = tier
            out[r] = tier
        save_academy(academy)
        return out


async def unregister_player(*, username: str) -> bool:
    u = _normalize_username(username)
    if not u:
        return False

    async with _ACADEMY_LOCK:
        academy = load_academy()
        removed = False
        for r in ROLES:
            users = academy.get(r)
            if not users:
                continue
            if u in users:
                users.pop(u, None)
                removed = True
        if removed:
            save_academy(academy)
        return removed


def _best_team_assignment(
    academy: dict[str, dict[str, int]],
    *,
    banned_users: set[str],
) -> dict[str, tuple[str, int]] | None:
    """
    Pick 1 user per role (5 roles), all distinct usernames, minimizing sum(tier).
    (Tier 1 is best; higher numbers are worse.)
    Returns: {role: (username, tier)} or None if impossible.
    """
    candidates: dict[str, list[tuple[str, int]]] = {}
    for role in ROLES:
        role_users = academy.get(role) or {}
        items: list[tuple[str, int]] = []
        for u, t in role_users.items():
            username = _normalize_username(u)
            if not username or username in banned_users:
                continue
            try:
                tier = int(t)
            except (TypeError, ValueError):
                tier = 3
            items.append((username, tier))
        # Prefer lower tiers first (tier 1 is best), then deterministic by username.
        items.sort(key=lambda x: (x[1], x[0].casefold()))
        candidates[role] = items

    if any(len(candidates[r]) == 0 for r in ROLES):
        return None

    role_order = sorted(ROLES, key=lambda r: len(candidates[r]))
    best_score = 10**18  # we minimize
    best: dict[str, tuple[str, int]] | None = None

    def lower_bound(i: int, used: set[str], score: int) -> int:
        lb = score
        for j in range(i, len(role_order)):
            r = role_order[j]
            best_t = None
            for u, t in candidates[r]:
                if u not in used:
                    best_t = t
                    break
            if best_t is None:
                return -1
            lb += best_t
        return lb

    def dfs(i: int, used: set[str], score: int, cur: dict[str, tuple[str, int]]) -> None:
        nonlocal best_score, best
        if i >= len(role_order):
            if score < best_score:
                best_score = score
                best = dict(cur)
            return

        lb = lower_bound(i, used, score)
        if lb < 0:
            return
        if lb >= best_score:
            return

        role = role_order[i]
        for u, t in candidates[role]:
            if u in used:
                continue
            used.add(u)
            cur[role] = (u, t)
            dfs(i + 1, used, score + t, cur)
            cur.pop(role, None)
            used.remove(u)

    dfs(0, set(), 0, {})
    return best


def generate_teams(academy: dict[str, dict[str, int]]) -> list[dict[str, tuple[str, int]]]:
    """
    Generate as many complete teams as possible (max team count first).

    Constraints:
    - Each team has 5 distinct players: one per role.
    - A player can only be used once across all generated teams.

    Optimization:
    - We treat lower tier numbers as stronger (tier 1 is best).
    - After maximizing team count, we minimize total tier across all selected role assignments.

    Implementation:
    - Solve a min-cost flow assignment for k teams:
        source -> roles (cap k)
        roles -> players (cap 1, cost = tier)
        players -> sink (cap 1)
      Then build teams by sorting per-role picks and zipping.
    """
    # Normalize academy -> role -> user -> tier(int)
    normalized: dict[str, dict[str, int]] = {r: {} for r in ROLES}
    for role in ROLES:
        users = academy.get(role) or {}
        for u, t in users.items():
            username = _normalize_username(u)
            if not username:
                continue
            try:
                tier = int(t)
            except (TypeError, ValueError):
                tier = 3
            normalized[role][username] = tier

    # Quick bounds for max possible teams.
    all_users: set[str] = set()
    for role in ROLES:
        all_users |= set(normalized[role].keys())
    if not all_users:
        return []
    max_by_total = len(all_users) // len(ROLES)
    max_by_role = min(len(normalized[r]) for r in ROLES)
    upper_k = min(max_by_total, max_by_role)
    if upper_k <= 0:
        return []

    # ---------- Min-cost max-flow (successive shortest augmenting path) ----------
    class _Edge:
        __slots__ = ("to", "rev", "cap", "cost")

        def __init__(self, to: int, rev: int, cap: int, cost: int):
            self.to = to
            self.rev = rev
            self.cap = cap
            self.cost = cost

    def _add_edge(g: list[list[_Edge]], fr: int, to: int, cap: int, cost: int) -> None:
        g[fr].append(_Edge(to, len(g[to]), cap, cost))
        g[to].append(_Edge(fr, len(g[fr]) - 1, 0, -cost))

    def _min_cost_flow(
        k: int,
    ) -> tuple[bool, dict[str, list[tuple[str, int]]]]:
        # Build nodes: source, 5 roles, N players, sink
        players = sorted(all_users, key=lambda s: s.casefold())
        role_idx = {r: 1 + i for i, r in enumerate(ROLES)}
        player_offset = 1 + len(ROLES)
        player_idx = {u: player_offset + i for i, u in enumerate(players)}
        sink = player_offset + len(players)
        n = sink + 1
        g: list[list[_Edge]] = [[] for _ in range(n)]

        source = 0
        # source -> roles
        for r in ROLES:
            _add_edge(g, source, role_idx[r], k, 0)
        # roles -> players (cost=tier)
        for r in ROLES:
            rnode = role_idx[r]
            for u, tier in normalized[r].items():
                _add_edge(g, rnode, player_idx[u], 1, int(tier))
        # players -> sink
        for u in players:
            _add_edge(g, player_idx[u], sink, 1, 0)

        need = len(ROLES) * k
        flow = 0
        potential = [0] * n
        dist = [0] * n
        prev_v = [0] * n
        prev_e = [0] * n

        while flow < need:
            INF = 10**18
            for i in range(n):
                dist[i] = INF
            dist[source] = 0
            pq: list[tuple[int, int]] = [(0, source)]
            while pq:
                d, v = heapq.heappop(pq)
                if d != dist[v]:
                    continue
                for ei, e in enumerate(g[v]):
                    if e.cap <= 0:
                        continue
                    nd = d + e.cost + potential[v] - potential[e.to]
                    if nd < dist[e.to]:
                        dist[e.to] = nd
                        prev_v[e.to] = v
                        prev_e[e.to] = ei
                        heapq.heappush(pq, (nd, e.to))

            if dist[sink] >= INF:
                break

            for v in range(n):
                if dist[v] < INF:
                    potential[v] += dist[v]

            # augment 1 unit (all capacities are integer, bottleneck is 1)
            add = need - flow
            v = sink
            while v != source:
                pv = prev_v[v]
                pe = prev_e[v]
                add = min(add, g[pv][pe].cap)
                v = pv

            v = sink
            while v != source:
                pv = prev_v[v]
                pe = prev_e[v]
                e = g[pv][pe]
                e.cap -= add
                g[v][e.rev].cap += add
                v = pv

            flow += add

        if flow != need:
            return False, {}

        # Extract assignments by looking at role->player edges that are "used"
        assigned: dict[str, list[tuple[str, int]]] = {r: [] for r in ROLES}
        for r in ROLES:
            rnode = role_idx[r]
            for e in g[rnode]:
                # forward edge role->player has cap either 0 (used) or 1 (unused)
                if e.to >= player_offset and e.to < sink and e.cost >= 0:
                    # If forward cap == 0, we sent 1 unit.
                    if e.cap == 0:
                        # Map node back to username
                        # Reverse lookup is fine (small), but keep it deterministic:
                        u = players[e.to - player_offset]
                        assigned[r].append((u, int(e.cost)))

        # Sanity: each role should have k picks.
        if any(len(assigned[r]) != k for r in ROLES):
            return False, {}

        return True, assigned

    # Find maximum feasible k.
    best_k = 0
    best_assigned: dict[str, list[tuple[str, int]]] = {}
    for k in range(1, upper_k + 1):
        ok, assigned = _min_cost_flow(k)
        if not ok:
            break
        best_k = k
        best_assigned = assigned

    if best_k <= 0:
        return []

    # Build teams: sort each role's list best-first, then zip by index.
    for r in ROLES:
        best_assigned[r].sort(key=lambda x: (x[1], x[0].casefold()))

    teams: list[dict[str, tuple[str, int]]] = []
    for i in range(best_k):
        team: dict[str, tuple[str, int]] = {}
        for r in ROLES:
            u, t = best_assigned[r][i]
            team[r] = (u, t)
        teams.append(team)

    return teams


def save_teams(
    teams: list[dict[str, tuple[str, int]]],
    path: Path = TEAMS_YAML_PATH,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    top: dict[str, dict[str, str]] = {}
    for idx, team in enumerate(teams, start=1):
        key = f"academy team {idx}"
        block: dict[str, str] = {}
        for role in ROLES:
            u, t = team.get(role, ("", 0))
            block[role] = f"{u} ({t})" if u else "-"
        top[key] = block

    text = yaml.safe_dump(top, sort_keys=False, default_flow_style=False) or ""
    with path.open("w", encoding="utf-8", newline="\n") as f:
        f.write(text.rstrip() + "\n")


async def create_teams_from_file(
    *,
    players_path: Path = ACADEMY_YAML_PATH,
    teams_path: Path = TEAMS_YAML_PATH,
) -> list[dict[str, tuple[str, int]]]:
    async with _ACADEMY_LOCK:
        academy = load_academy(players_path)
        teams = generate_teams(academy)
        save_teams(teams, teams_path)
        return teams

