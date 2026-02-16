#!/usr/bin/env python3
"""
Add a points column to a tournament CSV.

Expected input format (example):
Rank,Team
1,Some Team
5,Another Team

Points system:
1st  -> 100
2nd  -> 80
3rd  -> 65
4th  -> 55
5-6  -> 45
7-8  -> 35
9-12 -> 25
13-16-> 18
17-24-> 12
25-32-> 8
33-48-> 4
49-64-> 1

Notes:
- This script can also write a `formula` column for downstream aggregation.
- By default, `formula` now equals the raw placement points (no prize-pool multiplier).
- To generate the old prize-pool-weighted formula scores, use `--formula-mode prize_pool`.
"""

from __future__ import annotations

import argparse
import csv
import math
import re
import sys
from pathlib import Path
from typing import Optional


_POINT_RANGES: list[tuple[int, int, int]] = [
    (1, 1, 100),
    (2, 2, 80),
    (3, 3, 65),
    (4, 4, 55),
    (5, 6, 45),
    (7, 8, 35),
    (9, 12, 25),
    (13, 16, 18),
    (17, 24, 12),
    (25, 32, 8),
    (33, 48, 4),
    (49, 64, 1),
]


def compute_points(rank: int) -> Optional[int]:
    for lo, hi, pts in _POINT_RANGES:
        if lo <= rank <= hi:
            return pts
    return None


_FIRST_INT_RE = re.compile(r"\d+")


def parse_rank(value: object) -> Optional[int]:
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    # Accept "5-6" by taking the first number.
    if "-" in s:
        s = s.split("-", 1)[0].strip()
    m = _FIRST_INT_RE.search(s)
    if not m:
        return None
    try:
        return int(m.group(0))
    except ValueError:
        return None


def default_output_path(input_path: Path) -> Path:
    return input_path.with_name(f"{input_path.stem}_points{input_path.suffix}")


_TOURNAMENT_PRIZE_POOLS: dict[str, float] = {
    "art": 0.0,
    "prt": 50.0,
    "mrc": 1250.0,
    "mrc_swiss": 70.0,
    "rr": 0.0,
    "nc": 0.0,
}


def infer_tournament_key_from_filename(path: Path) -> Optional[str]:
    """
    Infer a tournament key from a CSV filename.

    Examples:
      - art9.csv -> "art"
      - prt6.csv -> "prt"
      - mrc.csv -> "mrc"
      - mrc_swiss.csv -> "mrc_swiss"
      - rr.csv -> "rr"
      - nc3.csv -> "nc"
    """
    name = path.stem.lower()
    if name.startswith("mrc_swiss"):
        return "mrc_swiss"
    for prefix in ("art", "prt", "mrc", "rr", "nc"):
        if name.startswith(prefix):
            return prefix
    return None


def infer_prize_pool(path: Path) -> Optional[float]:
    key = infer_tournament_key_from_filename(path)
    if key is None:
        return None
    return _TOURNAMENT_PRIZE_POOLS.get(key)


def compute_formula(
    points: float,
    prize_pool: float,
    *,
    x0: float = 100.0,
    multiplier_round_decimals: int = 2,
) -> float:
    """
    Formula = 10*Points * (1+log10(X / X0)) with X0 = 100
    Formula = 10*Points*0.3 if prize pool = 0
    """
    if prize_pool == 0:
        multiplier = 0.1
    else:
        multiplier = 1.0 + math.log10(prize_pool / x0)

    # User request: round the multiplier BEFORE multiplying by (10 * Points).
    multiplier = round(multiplier, multiplier_round_decimals)
    return 10.0 * points * multiplier


def add_points_to_file(
    *,
    input_path: Path,
    output_path: Path,
    rank_column: str,
    points_column: str,
    formula_column: str,
    formula_mode: str,
    prize_pool: Optional[float],
    decimals: int,
) -> None:
    with input_path.open("r", newline="", encoding="utf-8-sig") as f_in:
        reader = csv.DictReader(f_in)
        if reader.fieldnames is None:
            raise ValueError("Input CSV has no header row.")

        if rank_column not in reader.fieldnames:
            cols = ", ".join(reader.fieldnames)
            raise ValueError(f"Rank column {rank_column!r} not found. Columns: {cols}")

        fieldnames = list(reader.fieldnames)
        if points_column not in fieldnames:
            fieldnames.append(points_column)
        if formula_column and formula_column not in fieldnames:
            fieldnames.append(formula_column)

        output_path.parent.mkdir(parents=True, exist_ok=True)
        with output_path.open("w", newline="", encoding="utf-8") as f_out:
            writer = csv.DictWriter(f_out, fieldnames=fieldnames)
            writer.writeheader()

            for row in reader:
                rank = parse_rank(row.get(rank_column))
                pts = compute_points(rank) if rank is not None else None
                row[points_column] = "" if pts is None else str(pts)

                if formula_column:
                    if pts is None:
                        row[formula_column] = ""
                    else:
                        if formula_mode == "prize_pool":
                            if prize_pool is None:
                                row[formula_column] = ""
                            else:
                                value = compute_formula(
                                    float(pts),
                                    float(prize_pool),
                                    multiplier_round_decimals=decimals,
                                )
                                row[formula_column] = f"{value:.{decimals}f}"
                        else:
                            # Default: raw/normal points (no multiplier).
                            row[formula_column] = str(pts)
                writer.writerow(row)


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="Add a points column to a CSV based on placement/rank.")
    parser.add_argument(
        "-i",
        "--input",
        help="Input CSV path or directory (default: csv/).",
        default="csv",
    )
    parser.add_argument(
        "-o",
        "--output",
        help=(
            "Output CSV path (single-file mode only). "
            "If omitted, writes to <input>_points.csv."
        ),
    )
    parser.add_argument(
        "--output-dir",
        default="csv_points",
        help="Output directory when processing a directory input (default: csv_points/).",
    )
    parser.add_argument(
        "--keep-name",
        action="store_true",
        help="When processing a directory, keep original filenames (default behavior).",
    )
    parser.add_argument(
        "--suffix",
        default="",
        help="Optional suffix to add to output filenames before .csv (directory mode).",
    )
    parser.add_argument("--rank-column", default="Rank", help="Column name containing the rank/placement.")
    parser.add_argument("--points-column", default="points", help="New column name to write points into.")
    parser.add_argument(
        "--formula-column",
        default="formula",
        help="Column name to write the score into (set to empty to disable).",
    )
    parser.add_argument(
        "--formula-mode",
        choices=("normal", "prize_pool"),
        default="prize_pool",
        help=(
            "How to populate --formula-column. "
            "'normal' writes raw points; "
            "'prize_pool' writes the prize-pool-weighted formula (legacy behavior)."
        ),
    )
    parser.add_argument(
        "--prize-pool",
        type=float,
        default=None,
        help="Override prize pool (X). If omitted, inferred from filename (art/prt/mrc/mrc_swiss/rr/nc).",
    )
    parser.add_argument(
        "--decimals",
        type=int,
        default=2,
        help="Number of decimals for the formula output (default: 2).",
    )
    args = parser.parse_args(argv)

    input_path = Path(args.input)
    output_dir = Path(args.output_dir)

    if not input_path.exists():
        print(f"ERROR: Input file not found: {input_path}", file=sys.stderr)
        return 2

    # Single-file mode
    if input_path.is_file():
        output_path = Path(args.output) if args.output else default_output_path(input_path)
        prize_pool = args.prize_pool if args.prize_pool is not None else infer_prize_pool(input_path)
        try:
            add_points_to_file(
                input_path=input_path,
                output_path=output_path,
                rank_column=args.rank_column,
                points_column=args.points_column,
                formula_column=args.formula_column,
                formula_mode=args.formula_mode,
                prize_pool=prize_pool,
                decimals=args.decimals,
            )
        except ValueError as e:
            print(f"ERROR: {input_path}: {e}", file=sys.stderr)
            return 2
        print(f"Wrote: {output_path}")
        return 0

    # Directory mode (process all CSVs in /csv by default)
    any_failed = False
    processed = 0
    output_dir.mkdir(parents=True, exist_ok=True)

    for csv_file in sorted(p for p in input_path.iterdir() if p.is_file() and p.suffix.lower() == ".csv"):
        out_name = csv_file.name
        if args.suffix:
            out_name = f"{csv_file.stem}{args.suffix}{csv_file.suffix}"
        out_path = output_dir / out_name
        prize_pool = args.prize_pool if args.prize_pool is not None else infer_prize_pool(csv_file)
        try:
            add_points_to_file(
                input_path=csv_file,
                output_path=out_path,
                rank_column=args.rank_column,
                points_column=args.points_column,
                formula_column=args.formula_column,
                formula_mode=args.formula_mode,
                prize_pool=prize_pool,
                decimals=args.decimals,
            )
            processed += 1
        except ValueError as e:
            any_failed = True
            print(f"WARNING: Skipping {csv_file}: {e}", file=sys.stderr)

    if processed == 0:
        print(f"WARNING: No CSV files found in: {input_path}", file=sys.stderr)
        return 1

    print(f"Processed {processed} file(s). Output folder: {output_dir}")
    return 1 if any_failed else 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))

