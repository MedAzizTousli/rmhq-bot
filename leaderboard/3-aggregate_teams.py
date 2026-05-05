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
    "KIN EU": "KIN",
    "Phantom's": "BENEATH REALITY",
    "Sugar Pills": "OVERDOZEE",
    "Evolv ESPORT": "EVOLV",
    "JBG - Jung Brutal Gutaussehend": "Jung-Brutal-Gutaussehend",
    "PackMiko": "Bloody V",
    "RXM Esports": "RXM",
    "K7auzPool": "KlauzPool7",
    "no name": "noName",
    "noMercy": "noName",
    "BENATH REALITY": "BENEATH REALITY",
    "PHANTOM S": "BENEATH REALITY",
    "Reborn Xi - real": "Reborn Xi",
    "FLY KITSUNE": "Guardian Owls",
    "Minus Tempø": "La Pasión",
    "IGP™ Ultima Team": "EOZ Ultima",
    "ANGELS": "chicken sellers",
    "vaffanculo ghastly": "chicken sellers",
    "UDG": "Blind Spot",
    "Monarchy": "Monarchy EC",
    "ELEPHANT": "FC ELEPHANT",
    "Zatrox Esports": "Devil Esport",
    "ᴬⁿᵃʳᶜʰʸ": "Anarchy",
    "MKT NOVA": "InFeriX Esports",
    "RΞFLEX FC": "REFLEX FC",
    "Golden Requiem": "HAKI",
    "SCATTER.": "Team MOIRAI",
    "Playing-Ducks": "Team MOIRAI",
    "PlayingDucksᴿᴹ": "Team MOIRAI",
    "MOIRAI": "Team MOIRAI",
    "Samba Cookers (FEEL THE AURA)": "Samba Cookers",
    "Samba Cooker": "Samba Cookers",
    "√ ᴠᴏʀᴛᴇx": "Vortex",
    "100X35": "Volare",
    "The Chicks": "Volare",
    "Trapani FC": "Volare",
    "Karasuno 烏": "Volare",
    "Karasuno": "Volare",
    "Pride Warriors": "Volare",
    "ΞØN Esports": "1Motive",
    "EON Esports": "1Motive",
    "ΩRIGIN": "ORIGIN",
    "Entropy": "Death Cloud Esports",
    "NoLimits eSport": "NoLimits eSports",
    "ᴘʀʌɢᴍʌ ᴄʟʌɴ": "PRAGMA CLAN",
    "Ωrigin Académie": "Origin Iris",
    "Ωrigin Iris": "Origin Iris",
    "Desert elders": "SOLID",
    "AS Livorno": "Mist",
    "It's showtime...": "ASTRA",
    "хорошие девочки": "ASTRA",
    "princess": "ASTRA",
    "ASTRA eSports": "ASTRA",
    "FLOW RISING": "EON Esports",
    "Origin Iris": "Origin Ascend",
    "Ωrigin Ascend": "Origin Ascend",
    "Right Click": "X5",
    "BOMBO": "X5",
    "Brussels Blue Elites": "Brussels JK",
    "Brussels JK Blue Elites": "Brussels JK",
    "Beneath Reality": "Kin",
    "PackMiko E-sports": "Bloody V",
    "Monaco": "Monaco x AY",
    "YOMI eSports": "黄泉 YOMI eSports",
    "Karma": "Beneath Reality",
    "Delusion Esports": "Death Cloud Esports",
    "Classic X": "Classic V",
    "Exotic Raccoons": "Anarchy Raccoons",
    "𝐁𝐄𝐍𝐄𝐀𝐓𝐇 𝐑𝐄𝐀𝐋𝐈𝐓𝐘": "Beneath Reality",
    "EON x FLR": "EON Esports",
    "EON": "EON Esports",
    "ΔX x MIST": "Mist",
    "VTD": "Virtual Dragons",
    "EOZ": "EOZ Ultima",
    "∆X x Madness": "Madness",
    "ΔX x Madness": "Madness",
    "RandomCats": "wildcats",
    "mini PEKKA-1": "mini PEKKA",
    "DX x MIST": "Mist",
    "Death Cloud Esport": "Death Cloud Esports",
    "Guardian Owls": "VOLT Guardian Owls",
    "Ice GG": "Ice Gaming Group",
    "K-ill S-treak Monkeys": "Kill-Streak Monkey",
    "La pasion": "La Pasión",
    "Origin": "Origin x DMD",
    "Skepsis gaming": "Skepsis Esports",
    "Maledict": "ACE",
    "Z5": "X5",
    "Blind Spot": "Zentrix Esports",
    "very secret team": "Drain Cats",
    "Ωrigin x DMD": "Origin x DMD",
    "ᴍᴏɴᴀᴄᴏ x Aᵧ LAST DANCE": "Monaco x AY",
    "DSQ": "DSQ Esports",
    "VEX": "DSQ Esports",
    "Abaraka Never": "After Hours",
    "After Hours": "Anarchy",
    "DeltaX Madness": "Madness",
    "DX x Madness": "Madness",
    "Drain Cats": "ASTRA CIS",
    "Saviors": "xanax"
}

DISBANDED_TEAMS: list[str] = [
    "Monarchy EC",
    "chicken sellers",
    "Str1ve Corp",
    "Team ANDREA",
    "TEAM ENVOY",
    "Devil Esport",
    "Samba Cookers",
    "JUSTICE",
    "ORION ESPORTS",
    "Senger X",
    "Majin Purple",
    "Dual Esports",
    "we need name",
    "SPARTA",
    "Origin Ascend",
    "NoLimits eSports",
    "Seraphim",
    "El Cid Campeonador",
    "Voidborn",
    "FC ELEPHANT",
    "VALNOX",
    "Aïd moubarak la team",
    "1UP",
    "Bloody V",
    "PASTEL DE NATA",
    "Better Call Saul",
    "Monaco x AY",
    "wildcats"
]

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


def build_normalized_set(names: list[str]) -> set[str]:
    return {normalize_name(name) for name in names if name and name.strip()}


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
    disbanded_teams = build_normalized_set(DISBANDED_TEAMS)

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
            if canonical_norm in disbanded_teams:
                continue

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

