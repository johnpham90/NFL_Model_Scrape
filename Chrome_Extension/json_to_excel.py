"""Convert a PFR Weekly Scraper JSON bundle to the Python scraper's Excel layout."""

from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path
from typing import Any

import pandas as pd

CATEGORIES = (
    "Game_Summary",
    "Team_Stats",
    "Drives",
    "Rushing",
    "Passing",
    "Receiving",
    "Defense",
    "Returns",
    "Kicking",
    "Player_Offense",
    "Player_Defense",
    "ExpectedPoints",
    "Starters",
    "Snap_Counts",
    "Drive_Details",
)

FIXED_COLUMNS = {
    "Starters": ["player", "playerid", "pos", "teamid", "hometeamid", "awayteamid", "season", "week"],
    "Snap_Counts": ["player", "playerid", "pos", "off_num", "off_pct", "def_num", "def_pct", "st_num", "st_pct", "teamid", "hometeamid", "awayteamid", "season", "week"],
    "Drive_Details": [
        "Date", "Season", "Week", "Away Team", "Home Team", "Game_Time", "Quarter", "Time", "Down", "ToGo", "Location", "Detail",
        "Play_Type", "Primary_Player", "Receiver", "Sack_By", "Run_Location", "Run_Gap", "Pass_Type", "Pass_Location", "Pass_Yards",
        "Field_Goal_Yards", "Yards", "Tackler", "Tackler2", "Defender", "Result", "Penalized_Player", "Penalty_Yards", "EPB", "EPA",
        "Penalty", "Penalty_Accepted",
    ],
}

METADATA_COLUMNS = {
    "date": "Date",
    "season": "Season",
    "week": "Week",
    "awayTeam": "Away Team",
    "homeTeam": "Home Team",
    "awayScore": "Away Score",
    "homeScore": "Home Score",
    "team": "team",
    "is_home": "is_home",
    "playerid": "playerid",
}


def load_bundle(input_path: Path) -> dict[str, Any]:
    with input_path.open("r", encoding="utf-8") as input_file:
        bundle = json.load(input_file)
    if not isinstance(bundle, dict):
        raise ValueError("The JSON root must be an object.")
    return bundle


def find_latest_download() -> Path:
    downloads_dir = Path.home() / "Downloads" / "PFR_Weekly_Scraper"
    candidates = list(downloads_dir.glob("*_PFR.json"))
    if not candidates:
        raise FileNotFoundError(
            f"No PFR JSON files found in {downloads_dir}. "
            "Run the Chrome extension first or provide an input_json path."
        )
    return max(candidates, key=lambda path: path.stat().st_mtime)


def rename_metadata(record: dict[str, Any]) -> dict[str, Any]:
    renamed = {}
    for key, value in record.items():
        if key == "boxScoreUrl":
            continue
        renamed[METADATA_COLUMNS.get(key, key)] = value
    return renamed


def normalize_game_summary(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized = []
    for record in records:
        flat_record = {key: value for key, value in record.items() if key != "gameInfo"}
        game_info = record.get("gameInfo")
        if isinstance(game_info, dict):
            flat_record.update(game_info)
        normalized.append(rename_metadata(flat_record))
    return normalized


def normalize_records(category: str, records: Any) -> list[dict[str, Any]]:
    if not isinstance(records, list):
        return []
    objects = [record for record in records if isinstance(record, dict)]
    if category == "Game_Summary":
        return normalize_game_summary(objects)
    normalized = [rename_metadata(record) for record in objects]
    if category in {"Starters", "Snap_Counts"}:
        for record in normalized:
            record["season"] = record.pop("Season", record.get("season"))
            record["week"] = record.pop("Week", record.get("week"))
    if category == "Drive_Details":
        normalized = [rename_metadata(record) for record in objects]
    if category == "ExpectedPoints":
        for record in normalized:
            if "Away Team" in record:
                record["Away_Team"] = record.pop("Away Team")
            if "Home Team" in record:
                record["Home_Team"] = record.pop("Home Team")
            record.pop("team_name", None)
    return normalized


def write_category(category: str, records: list[dict[str, Any]], output_dir: Path, season: int, week: int) -> Path | None:
    if not records:
        return None
    frame = pd.DataFrame(records).drop_duplicates(keep="first")
    fixed_columns = FIXED_COLUMNS.get(category)
    if fixed_columns:
        for column in fixed_columns:
            if column not in frame.columns:
                frame[column] = None
        frame = frame[fixed_columns]
    if category == "ExpectedPoints":
        filename = f"{season}_Week{week}_ExpectedPoints.xlsx"
    elif category == "Drives":
        filename = f"{season}_Week{week}_Drives_Stats.xlsx"
    elif category == "Starters":
        filename = f"{season}_Week{week}_Starters.xlsx"
    elif category == "Snap_Counts":
        filename = f"{season}_Week{week}_Snap_Counts.xlsx"
    elif category == "Drive_Details":
        filename = f"{season}_Week{week}_Drive_Details.xlsx"
    else:
        filename = f"{season}_Week{week}_{category}_Stats.xlsx"
    output_path = output_dir / filename
    frame.to_excel(output_path, index=False)
    return output_path


def convert(input_path: Path, output_root: Path) -> list[Path]:
    bundle = load_bundle(input_path)
    season = int(bundle["season"])
    week = int(bundle["week"])
    output_dir = output_root / str(season) / f"Week_{week}"
    output_dir.mkdir(parents=True, exist_ok=True)

    json_output_path = output_dir / f"{season}_Week{week}_PFR.json"
    if input_path.resolve() != json_output_path.resolve():
        shutil.copy2(input_path, json_output_path)
    print(f"Saved JSON: {json_output_path}")

    written = []
    for category in CATEGORIES:
        records = normalize_records(category, bundle.get(category, []))
        output_path = write_category(category, records, output_dir, season, week)
        if output_path:
            written.append(output_path)
            print(f"Saved {len(records)} rows: {output_path}")
        else:
            print(f"Skipped {category}: no records")
    return written


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "input_json",
        type=Path,
        nargs="?",
        help="Downloaded PFR JSON bundle; defaults to the newest Downloads file",
    )
    parser.add_argument("--output-root", type=Path, default=Path(r"C:\NFLStats\data"), help="Root output directory")
    args = parser.parse_args()
    input_path = args.input_json or find_latest_download()
    print(f"Using input JSON: {input_path}")
    convert(input_path, args.output_root)


if __name__ == "__main__":
    main()
