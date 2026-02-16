#!/usr/bin/env python3
"""
Aggregate teams in an existing leaderboard CSV using a name mapping.

Input (default):  output/leaderboard.csv
Output (default): output/leaderboard_aggregated.csv

Expected columns: Rank, Team, Points

Edit TEAM_NAME_MAP below to merge aliases, e.g.:
  "VEX"  and  "VΞX"  -> "VEX"
"""

from __future__ import annotations

import argparse
import csv
from collections import Counter
from dataclasses import dataclass
from pathlib import Path


# Defaults are relative to this script's folder so it works from any CWD.
SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_OUTPUT_DIR = SCRIPT_DIR / "output"


# Map "alias team name" -> "canonical team name"
# Add as many aliases as you want here.
TEAM_NAME_MAP: dict[str, str] = {
    "VΞX": "VEX",
    "Sugar Pills": "OVERDOZEE",
    "ΞØN Esports": "EON Esports"
}


def normalize_name(name: str) -> str:
    # Trim + collapse whitespace + casefold for robust matching.
    s = " ".join((name or "").strip().split())
    return s.casefold()


def parse_float(value: str) -> float:
    value = (value or "").strip()
    if not value:
        return 0.0
    return float(value)


@dataclass
class TeamAgg:
    canonical_display: str
    total_points: float = 0.0
    aliases: Counter[str] = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        if self.aliases is None:
            self.aliases = Counter()


def build_normalized_map(raw_map: dict[str, str]) -> dict[str, str]:
    out: dict[str, str] = {}
    for alias, canonical in raw_map.items():
        alias_n = normalize_name(alias)
        canonical_clean = " ".join((canonical or "").strip().split())
        if not canonical_clean:
            continue
        out[alias_n] = canonical_clean
    return out


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser(description="Aggregate/merge teams in leaderboard.csv via a mapping.")
    ap.add_argument(
        "--input",
        default=str(DEFAULT_OUTPUT_DIR / "leaderboard.csv"),
        help=f"Input leaderboard CSV path (default: {DEFAULT_OUTPUT_DIR / 'leaderboard.csv'}).",
    )
    ap.add_argument(
        "--output",
        default=str(DEFAULT_OUTPUT_DIR / "leaderboard_aggregated.csv"),
        help=f"Output aggregated CSV path (default: {DEFAULT_OUTPUT_DIR / 'leaderboard_aggregated.csv'}).",
    )
    ap.add_argument(
        "--decimals",
        type=int,
        default=2,
        help="Decimals for output points (default: 2).",
    )
    ap.add_argument(
        "--add-aliases-column",
        action="store_true",
        help='Also write an "Aliases" column listing merged names.',
    )
    args = ap.parse_args(argv)

    input_path = Path(args.input)
    if not input_path.exists():
        raise SystemExit(f"Input not found: {input_path}")

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    name_map = build_normalized_map(TEAM_NAME_MAP)

    aggs: dict[str, TeamAgg] = {}
    with input_path.open("r", newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise SystemExit(f"{input_path}: missing header row")
        for required in ("Team", "Points"):
            if required not in reader.fieldnames:
                raise SystemExit(f"{input_path}: missing required column {required!r}. Found: {reader.fieldnames}")

        for row in reader:
            team_raw = (row.get("Team") or "").strip()
            if not team_raw:
                continue

            points = parse_float(row.get("Points", "0"))

            team_norm = normalize_name(team_raw)
            canonical = name_map.get(team_norm, " ".join(team_raw.split()))
            canonical_norm = normalize_name(canonical)

            agg = aggs.get(canonical_norm)
            if agg is None:
                agg = TeamAgg(canonical_display=canonical)
                aggs[canonical_norm] = agg

            agg.total_points += points
            agg.aliases[team_raw] += 1

    # Build rows with a rounded numeric points value for stable tie grouping.
    rows: list[dict[str, object]] = []
    for agg in aggs.values():
        points_num = round(agg.total_points, args.decimals)
        r: dict[str, str] = {
            "Team": agg.canonical_display,
            "Points": f"{points_num:.{args.decimals}f}",
        }
        if args.add_aliases_column:
            aliases = [name for name, _count in agg.aliases.most_common()]
            r["Aliases"] = ", ".join(aliases)
        rows.append({**r, "_points_num": points_num})

    rows.sort(key=lambda r: (-float(r["_points_num"]), str(r["Team"]).casefold()))

    # Assign Rank as a range for ties (e.g. "27–28").
    # All teams with equal (rounded) points share the same rank label.
    i = 0
    while i < len(rows):
        j = i + 1
        while j < len(rows) and rows[j]["_points_num"] == rows[i]["_points_num"]:
            j += 1
        start_rank = i + 1
        end_rank = j
        rank_label = f"{start_rank}\u2013{end_rank}" if start_rank != end_rank else str(start_rank)
        for k in range(i, j):
            rows[k]["Rank"] = rank_label
        i = j

    fieldnames = ["Rank", "Team", "Points"]
    if args.add_aliases_column:
        fieldnames.append("Aliases")

    with output_path.open("w", newline="", encoding="utf-8") as f_out:
        writer = csv.DictWriter(f_out, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            r.pop("_points_num", None)
        writer.writerows(rows)  # type: ignore[arg-type]

    print(f"Wrote: {output_path} ({len(rows)} teams)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(__import__("sys").argv[1:]))

