#!/usr/bin/env python3
"""
Aggregate all CSVs in csv_points/ into a leaderboard.

Input files (default): csv_points/*.csv
Expected columns: Rank, Team, formula

Output (default): output/leaderboard.csv
Sorted by: total_formula (desc), Team (asc)
"""

from __future__ import annotations

import argparse
import csv
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_INPUT_DIR = SCRIPT_DIR / "csv_points"
DEFAULT_OUTPUT_DIR = SCRIPT_DIR / "output"


def canonical_team_name(name: str) -> str:
    # Normalize for grouping: trim, collapse whitespace, casefold.
    s = " ".join((name or "").strip().split())
    return s.casefold()


def iter_csv_files(folder: Path, *, include_tests: bool) -> Iterable[Path]:
    for p in sorted(folder.glob("*.csv")):
        stem = p.stem.lower()
        if stem.startswith("leaderboard"):
            continue
        if (not include_tests) and (stem.endswith("_test") or stem.endswith("-test")):
            continue
        yield p


@dataclass
class TeamAgg:
    display_names: Counter[str]
    events_played: int
    total_formula: float


def parse_int(value: str) -> int:
    value = (value or "").strip()
    if not value:
        return 0
    return int(float(value))


def parse_float(value: str) -> float:
    value = (value or "").strip()
    if not value:
        return 0.0
    return float(value)


def tier_for_leaderboard_rank(leaderboard_rank: int) -> str:
    if 1 <= leaderboard_rank <= 8:
        return "tier 1"
    if 9 <= leaderboard_rank <= 24:
        return "tier 2"
    if 25 <= leaderboard_rank <= 48:
        return "tier 3"
    return ""


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser(description="Aggregate csv_points into a leaderboard CSV.")
    ap.add_argument("--input-dir", default=str(DEFAULT_INPUT_DIR), help="Folder containing per-event CSVs.")
    ap.add_argument(
        "--output",
        default="",
        help=f"Output CSV path (default: {DEFAULT_OUTPUT_DIR / 'leaderboard.csv'}).",
    )
    ap.add_argument("--include-tests", action="store_true", help="Include *_test.csv files.")
    ap.add_argument("--rank-column", default="Rank", help="Column name for per-event placement/rank.")
    ap.add_argument("--team-column", default="Team", help="Column name for team name.")
    ap.add_argument("--formula-column", default="formula", help="Column name for formula score.")
    ap.add_argument("--decimals", type=int, default=2, help="Decimals for output totals (default: 2).")
    args = ap.parse_args(argv)

    input_dir = Path(args.input_dir)
    if not input_dir.exists() or not input_dir.is_dir():
        raise SystemExit(f"Input directory not found: {input_dir}")

    output_path = Path(args.output) if args.output else (DEFAULT_OUTPUT_DIR / "leaderboard.csv")

    teams: dict[str, TeamAgg] = {}

    files = list(iter_csv_files(input_dir, include_tests=args.include_tests))
    if not files:
        raise SystemExit(f"No CSV files found in: {input_dir}")

    for file_path in files:
        with file_path.open("r", newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            if reader.fieldnames is None:
                continue
            for col in (args.rank_column, args.team_column, args.formula_column):
                if col not in reader.fieldnames:
                    raise SystemExit(f"{file_path}: missing required column {col!r}. Found: {reader.fieldnames}")

            for row in reader:
                team_raw = (row.get(args.team_column) or "").strip()
                if not team_raw:
                    continue
                key = canonical_team_name(team_raw)
                score = parse_float(row.get(args.formula_column, "0"))

                agg = teams.get(key)
                if agg is None:
                    agg = TeamAgg(
                        display_names=Counter(),
                        events_played=0,
                        total_formula=0.0,
                    )
                    teams[key] = agg

                agg.display_names[team_raw] += 1
                agg.events_played += 1
                agg.total_formula += score

    rows = []
    for agg in teams.values():
        team_name = agg.display_names.most_common(1)[0][0]
        rows.append(
            {
                "Team": team_name,
                # Keep column name "Points" for the Discord embed, but this is TOTAL FORMULA.
                "Points": f"{agg.total_formula:.{args.decimals}f}",
            }
        )

    rows.sort(
        key=lambda r: (
            -float(r["Points"]),
            r["Team"].casefold(),
        )
    )

    # Add Rank after sorting
    for i, r in enumerate(rows, start=1):
        r["Rank"] = str(i)

    fieldnames = [
        "Rank",
        "Team",
        "Points",
    ]

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", newline="", encoding="utf-8") as f_out:
        writer = csv.DictWriter(f_out, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Wrote: {output_path} ({len(rows)} teams, {len(files)} files)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(__import__('sys').argv[1:]))

