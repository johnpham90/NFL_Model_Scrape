"""Convert a PFR Weekly Scraper JSON bundle to the Python scraper's Excel layout."""

from __future__ import annotations

import argparse
import json
import shutil
import tempfile
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
    candidates = list(downloads_dir.glob("*_PFR.json")) + list(downloads_dir.glob("*_Roster.json"))
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


def convert_roster(bundle: dict[str, Any], output_root: Path) -> Path:
    season = int(bundle["season"])
    output_dir = output_root / str(season)
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "NFL_Rosters.xlsx"
    records = [record for record in bundle.get("Roster", []) if isinstance(record, dict) and record.get("playerid")]
    new_frame = pd.DataFrame(records).drop_duplicates(subset=["playerid"], keep="last")
    run_info = (bundle.get("Run_Info") or [{}])[0]
    failed_team_ids = {failure.get("teamId") for failure in run_info.get("failures", []) if failure.get("teamId")}

    old_frame = pd.DataFrame()
    if output_path.exists():
        try:
            old_frame = pd.read_excel(output_path, sheet_name="Current_Roster")
        except (ValueError, OSError):
            old_frame = pd.DataFrame()

    deltas = []
    old_by_id = {str(row["playerid"]): row for _, row in old_frame.iterrows()} if "playerid" in old_frame.columns else {}
    new_by_id = {str(row["playerid"]): row for _, row in new_frame.iterrows()}
    for player_id, row in new_by_id.items():
        if player_id not in old_by_id:
            deltas.append(("Added", player_id, None, row.get("teamid")))
        elif old_by_id[player_id].get("teamid") != row.get("teamid"):
            deltas.append(("Team_Change", player_id, old_by_id[player_id].get("teamid"), row.get("teamid")))
        else:
            changed_fields = [column for column in new_frame.columns if column != "scrapedAt" and old_by_id[player_id].get(column) != row.get(column)]
            if changed_fields:
                deltas.append(("Updated", player_id, row.get("teamid"), ",".join(changed_fields)))
    for player_id, row in old_by_id.items():
        if player_id not in new_by_id and row.get("teamid") not in failed_team_ids:
            deltas.append(("Removed", player_id, row.get("teamid"), None))

    if failed_team_ids and not old_frame.empty:
        preserved = old_frame[old_frame["teamid"].isin(failed_team_ids)]
        if not new_frame.empty and "playerid" in new_frame.columns:
            preserved = preserved[~preserved["playerid"].isin(new_frame["playerid"])]
        new_frame = pd.concat([new_frame, preserved], ignore_index=True).drop_duplicates(subset=["playerid"], keep="last")
    new_frame["season"] = season
    with tempfile.NamedTemporaryFile(prefix="NFL_Rosters_", suffix=".xlsx", dir=output_dir, delete=False) as temp_file:
        temp_path = Path(temp_file.name)
    try:
        with pd.ExcelWriter(temp_path, engine="openpyxl") as writer:
            new_frame.to_excel(writer, sheet_name="Current_Roster", index=False)
        temp_path.replace(output_path)
    finally:
        temp_path.unlink(missing_ok=True)
    print(f"Saved current roster: {output_path} ({len(new_frame)} players)")
    print(f"Roster deltas: {len(deltas)}")
    for change_type, player_id, old_value, new_value in deltas:
        print(f"  {change_type}: {player_id} {old_value or ''} -> {new_value or ''}".rstrip())
    if failed_team_ids:
        print(f"Preserved prior rows for failed teams: {', '.join(sorted(failed_team_ids))}")
    return output_path


def convert(input_path: Path, output_root: Path) -> list[Path]:
    bundle = load_bundle(input_path)
    if "Roster" in bundle:
        season = int(bundle["season"])
        output_dir = output_root / str(season)
        output_dir.mkdir(parents=True, exist_ok=True)
        json_output_path = output_dir / f"{season}_Roster.json"
        if input_path.resolve() != json_output_path.resolve():
            shutil.copy2(input_path, json_output_path)
        print(f"Saved JSON: {json_output_path}")
        return [convert_roster(bundle, output_root)]
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
