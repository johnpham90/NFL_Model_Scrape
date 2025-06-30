import os
import pandas as pd
import traceback
from pathlib import Path
import re

def parse_play_details_penalty_only(detail_text):
    """
    Simplified version that only extracts penalty information
    Use this if you don't want to re-run the full parsing logic
    """
    penalty_info = {
        'Penalty': None,
        'Penalty_Accepted': None,
        'Penalized_Player': None,
        'Penalty_Yards': None
    }
    
    if not detail_text:
        return penalty_info
    
    detail_text = ' '.join(detail_text.lower().split())
    player_pattern = r'(?:[A-Z][A-Za-z\'\.-]*\s*){1,4}'
    
    # Check for penalty
    if 'penalty' in detail_text:
        # Set penalty field
        penalty_info['Penalty'] = 'Penalty'
        
        # Determine if penalty was accepted or declined
        if 'decline' in detail_text:
            penalty_info['Penalty_Accepted'] = 'Declined'
        else:
            penalty_info['Penalty_Accepted'] = 'Accepted'
        
        # Extract penalized player - try multiple patterns
        penalty_player_patterns = [
            fr'penalty on\s+({player_pattern})',
            fr'penalty,?\s+({player_pattern})',
            fr'penalty:\s+({player_pattern})',
            fr'({player_pattern}),?\s+penalty'
        ]
        
        for pattern in penalty_player_patterns:
            penalty_match = re.search(pattern, detail_text, re.IGNORECASE)
            if penalty_match:
                penalty_info['Penalized_Player'] = penalty_match.group(1).strip().title()
                break
        
        # Extract penalty yards - try multiple patterns
        penalty_yard_patterns = [
            r'penalty[^,]*,\s*(\d+)\s*yards?',
            r'(\d+)\s*yard\s*penalty',
            r'penalty[^,]*(\d+)\s*yards?',
            r',\s*(\d+)\s*yards?,\s*(?:accepted|declined)'
        ]
        
        for pattern in penalty_yard_patterns:
            penalty_yards = re.search(pattern, detail_text, re.IGNORECASE)
            if penalty_yards:
                penalty_info['Penalty_Yards'] = int(penalty_yards.group(1))
                break
    
    return penalty_info

def backfill_excel_files(data_directory, seasons=None, weeks=None):
    """
    Backfill Excel files with penalty information
    Matches your structure: Year/Week_#/Year_Week#_Drive_Details.xlsx
    
    Args:
        data_directory: Path to your NFL data directory
        seasons: List of seasons to process (None for all)
        weeks: List of weeks to process (None for all)
    """
    data_path = Path(data_directory)
    processed_files = []
    error_files = []
    
    # Find all season directories (year folders)
    if seasons:
        season_dirs = [data_path / str(season) for season in seasons]
    else:
        season_dirs = [d for d in data_path.iterdir() if d.is_dir() and d.name.isdigit()]
    
    for season_dir in season_dirs:
        if not season_dir.exists():
            print(f"Season directory not found: {season_dir}")
            continue
            
        season = season_dir.name
        print(f"\nProcessing Season {season}...")
        
        # Find week directories (Week_1, Week_2, etc.)
        if weeks:
            week_dirs = [season_dir / f"Week_{week}" for week in weeks]
        else:
            week_dirs = [d for d in season_dir.iterdir() if d.is_dir() and d.name.startswith("Week_")]
        
        for week_dir in week_dirs:
            if not week_dir.exists():
                continue
                
            week_num = week_dir.name.replace("Week_", "")
            print(f"  Processing Week {week_num}...")
            
            # Look for the specific Drive_Details file: Year_Week#_Drive_Details.xlsx
            drive_details_file = week_dir / f"{season}_Week{week_num}_Drive_Details.xlsx"
            
            if drive_details_file.exists():
                try:
                    print(f"    Processing file: {drive_details_file.name}")
                    
                    # Read the Excel file
                    df = pd.read_excel(drive_details_file)
                    original_count = len(df)
                    
                    print(f"      Found {original_count} rows")
                    print(f"      Current columns: {list(df.columns)}")
                    
                    # Check if penalty columns already exist
                    has_penalty_cols = all(col in df.columns for col in ['Penalty', 'Penalty_Accepted'])
                    
                    if has_penalty_cols:
                        print(f"      Penalty columns already exist, updating values...")
                    else:
                        print(f"      Adding new penalty columns...")
                        # Add new columns
                        df['Penalty'] = None
                        df['Penalty_Accepted'] = None
                        # Update existing penalty columns if they exist but are missing data
                        if 'Penalized_Player' not in df.columns:
                            df['Penalized_Player'] = None
                        if 'Penalty_Yards' not in df.columns:
                            df['Penalty_Yards'] = None
                    
                    # Process each row that has a Detail field
                    updated_rows = 0
                    penalty_found_count = 0
                    
                    for idx, row in df.iterrows():
                        if pd.notna(row.get('Detail', '')):
                            detail_text = str(row['Detail'])
                            
                            # Parse penalty information
                            penalty_info = parse_play_details_penalty_only(detail_text)
                            
                            # Update the row with penalty information
                            updated_this_row = False
                            for field, value in penalty_info.items():
                                if value is not None:  # Only update if we found something
                                    df.at[idx, field] = value
                                    updated_this_row = True
                            
                            if penalty_info['Penalty'] == 'Penalty':
                                penalty_found_count += 1
                                if updated_this_row:
                                    updated_rows += 1
                    
                    # Reorder columns to match your expected format
                    expected_columns = [
                        'Date', 'Season', 'Week', 'Away Team', 'Home Team', 'Game_Time',
                        'Quarter', 'Time', 'Down', 'ToGo', 'Location', 'Detail',
                        'Play_Type', 'Primary_Player', 'Receiver', 'Sack_By',
                        'Run_Location', 'Run_Gap', 'Pass_Type', 'Pass_Location',
                        'Pass_Yards', 'Field_Goal_Yards', 'Yards', 'Tackler', 'Tackler2', 'Defender',
                        'Result', 'Penalized_Player', 'Penalty_Yards', 'Penalty', 'Penalty_Accepted', 'EPB', 'EPA'
                    ]
                    
                    # Ensure all expected columns exist
                    for col in expected_columns:
                        if col not in df.columns:
                            df[col] = None
                    
                    # Reorder to match expected format
                    available_columns = [col for col in expected_columns if col in df.columns]
                    df = df[available_columns]
                    
                    # Save the updated file
                    df.to_excel(drive_details_file, index=False)
                    processed_files.append(str(drive_details_file))
                    print(f"      Found {penalty_found_count} plays with penalties")
                    print(f"      Updated {updated_rows} rows with penalty information")
                    print(f"      Final columns: {list(df.columns)}")
                    
                except Exception as e:
                    error_files.append((str(drive_details_file), str(e)))
                    print(f"      ERROR processing {drive_details_file.name}: {e}")
                    traceback.print_exc()
            else:
                print(f"    Drive Details file not found: {drive_details_file.name}")
    
    # Summary
    print(f"\n=== BACKFILL SUMMARY ===")
    print(f"Successfully processed: {len(processed_files)} files")
    print(f"Errors: {len(error_files)} files")
    
    if error_files:
        print("\nFiles with errors:")
        for file_path, error in error_files:
            print(f"  {file_path}: {error}")
    
    return processed_files, error_files

def main():
    """
    Main function to run the backfill process
    """
    # Configuration - Update this to match your structure
    DATA_DIRECTORY = r"C:\NFLStats\data"  # Your actual path
    
    # Specify which seasons/weeks to process (None for all)
    SEASONS_TO_PROCESS = None  # Start with 2004, or None for all
    WEEKS_TO_PROCESS = None       # Start with Week 1, or None for all weeks
    
    print("Starting penalty field backfill process...")
    print(f"Data directory: {DATA_DIRECTORY}")
    print(f"Processing seasons: {SEASONS_TO_PROCESS if SEASONS_TO_PROCESS else 'ALL'}")
    print(f"Processing weeks: {WEEKS_TO_PROCESS if WEEKS_TO_PROCESS else 'ALL'}")
    
    # Backfill Excel files
    print("\n=== BACKFILLING DRIVE DETAILS FILES ===")
    if os.path.exists(DATA_DIRECTORY):
        processed_files, error_files = backfill_excel_files(
            DATA_DIRECTORY, 
            seasons=SEASONS_TO_PROCESS, 
            weeks=WEEKS_TO_PROCESS
        )
        
        if processed_files:
            print(f"\nSuccessfully processed files:")
            for file_path in processed_files:
                print(f"  ✓ {file_path}")
    else:
        print(f"Data directory not found: {DATA_DIRECTORY}")
    
    print("\nBackfill process completed!")
    print("\nTo process more data:")
    print("1. Update SEASONS_TO_PROCESS = None to process all seasons")
    print("2. Update WEEKS_TO_PROCESS = None to process all weeks")
    print("3. Or specify specific seasons/weeks as needed")

if __name__ == "__main__":
    main()