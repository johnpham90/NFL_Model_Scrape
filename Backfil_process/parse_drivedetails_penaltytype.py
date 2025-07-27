import os
import pandas as pd
import traceback
from pathlib import Path
import re

def parse_play_details(detail_text):
    """
    Parse play details into structured categories with improved pattern matching.
    This handles penalties while determining the actual underlying play type.
    """
    play_info = {
        'Play_Type': None,
        'Primary_Player': None,
        'Receiver': None,
        'Sack_By': None,
        'Run_Location': None,
        'Run_Gap': None,
        'Pass_Type': None,
        'Pass_Location': None,
        'Pass_Yards': None,
        'Yards': None,
        'Tackler': None,
        'Tackler2': None,
        'Defender': None,
        'Result': None,
        'Penalized_Player': None,
        'Penalty_Yards': None,
        'Penalty': None,
        'Penalty_Accepted': None,
        'Detail': detail_text
    }

    if not detail_text:
        return play_info

    # Clean and normalize the text
    detail_text_clean = ' '.join(detail_text.lower().split())
    player_pattern = r'(?:[a-z][a-za-z\'\.-]*\s*){1,4}'

    # Extract penalty information FIRST
    if 'penalty' in detail_text_clean:
        play_info['Penalty'] = 'Penalty'
        
        if 'decline' in detail_text_clean:
            play_info['Penalty_Accepted'] = 'Declined'
        else:
            play_info['Penalty_Accepted'] = 'Accepted'
        
        # Extract penalized player
        penalty_player_patterns = [
            fr'penalty on\s+({player_pattern})',
            fr'penalty,?\s+({player_pattern})',
            fr'penalty:\s+({player_pattern})',
            fr'({player_pattern}),?\s+penalty'
        ]
        
        for pattern in penalty_player_patterns:
            penalty_match = re.search(pattern, detail_text_clean, re.IGNORECASE)
            if penalty_match:
                play_info['Penalized_Player'] = penalty_match.group(1).strip().title()
                break
        
        # Extract penalty yards
        penalty_yard_patterns = [
            r'penalty[^,]*,\s*(\d+)\s*yards?',
            r'(\d+)\s*yard\s*penalty',
            r'penalty[^,]*(\d+)\s*yards?',
            r',\s*(\d+)\s*yards?,\s*(?:accepted|declined)'
        ]
        
        for pattern in penalty_yard_patterns:
            penalty_yards = re.search(pattern, detail_text_clean, re.IGNORECASE)
            if penalty_yards:
                play_info['Penalty_Yards'] = int(penalty_yards.group(1))
                break

    # Extract yards
    yards_patterns = [
        r'for (\-?\d+) yards?',
        r'(\-?\d+) yard (gain|loss)',
        r'loses (\-?\d+) yards?',
        r'gains (\d+) yards?',
        r'punts (\d+) yards',
        r'\s+(\d+)\s+yard\s+field\s+goal'
    ]
    for pattern in yards_patterns:
        yards_match = re.search(pattern, detail_text_clean)
        if yards_match:
            play_info['Yards'] = int(yards_match.group(1))
            break

    # Determine play type - the key part for fixing penalty plays
    if 'two point attempt:' in detail_text_clean:
        play_info['Play_Type'] = 'Two Point'
    elif 'aborted' in detail_text_clean:
        play_info['Play_Type'] = 'Aborted'
    elif any(x in detail_text_clean for x in [
        'left tackle for', 'right tackle for', 'left guard for', 'right guard for', 
        'left end for', 'right end for', 'up the middle for', 'runs for', 'rushed for',
        ' middle run', ' left run', ' right run', 'middle for', 'scrambles',
        ' left tackle', ' right tackle', ' left guard', ' right guard', ' left end', ' right end'
    ]):
        play_info['Play_Type'] = 'Run'
        
        # Set run location and gap from the detail text
        if 'left tackle' in detail_text_clean:
            play_info['Run_Location'] = 'Left'
            play_info['Run_Gap'] = 'Tackle'
        elif 'right tackle' in detail_text_clean:
            play_info['Run_Location'] = 'Right'
            play_info['Run_Gap'] = 'Tackle'
        elif 'left guard' in detail_text_clean:
            play_info['Run_Location'] = 'Left'
            play_info['Run_Gap'] = 'Guard'
        elif 'right guard' in detail_text_clean:
            play_info['Run_Location'] = 'Right'
            play_info['Run_Gap'] = 'Guard'
        elif 'left end' in detail_text_clean:
            play_info['Run_Location'] = 'Left'
            play_info['Run_Gap'] = 'End'
        elif 'right end' in detail_text_clean:
            play_info['Run_Location'] = 'Right'
            play_info['Run_Gap'] = 'End'
        elif 'up the middle' in detail_text_clean or ' middle' in detail_text_clean:
            play_info['Run_Location'] = 'Middle'
            play_info['Run_Gap'] = 'Middle'
        
        # Extract runner - improved patterns
        runner_patterns = [
            fr'^({player_pattern})\s+(?:left|right)\s+(?:tackle|guard|end)',  # "Corey Dillon right guard"
            fr'^({player_pattern})\s+up the middle',                          # "Player up the middle"
            fr'^({player_pattern})\s+(?:runs|rushed|scrambles)',              # "Player runs/rushed/scrambles"
            fr'^({player_pattern})\s+middle'                                  # "Player middle"
        ]
        for pattern in runner_patterns:
            runner_match = re.search(pattern, detail_text_clean, re.IGNORECASE)
            if runner_match:
                play_info['Primary_Player'] = runner_match.group(1).strip().title()
                break
                
        # Set run result
        if 'no gain' in detail_text_clean:
            play_info['Yards'] = 0
            play_info['Result'] = 'No Gain'
        elif play_info['Yards'] is not None:
            if play_info['Yards'] > 0:
                play_info['Result'] = 'Gain'
            elif play_info['Yards'] < 0:
                play_info['Result'] = 'Loss'
                
    elif ('pass complete' in detail_text_clean or 
          'pass incomplete' in detail_text_clean or 
          'sacked' in detail_text_clean or 
          ('pass' in detail_text_clean and any(x in detail_text_clean for x in ['to', 'intended for']))):
        play_info['Play_Type'] = 'Pass'
        
        # Extract quarterback
        qb_patterns = [
            fr'^({player_pattern})\s+pass',
            fr'^({player_pattern})\s+sacked'
        ]
        for pattern in qb_patterns:
            qb_match = re.search(pattern, detail_text_clean, re.IGNORECASE)
            if qb_match:
                play_info['Primary_Player'] = qb_match.group(1).strip().title()
                break
        
        # Set pass result
        if 'sacked' in detail_text_clean:
            play_info['Result'] = 'Sack'
        elif 'incomplete' in detail_text_clean:
            play_info['Result'] = 'Incomplete'
        elif 'complete' in detail_text_clean:
            play_info['Result'] = 'Complete'
            
    elif 'punts' in detail_text_clean:
        play_info['Play_Type'] = 'Punt'
        punter_match = re.search(fr'^({player_pattern})\s+punts', detail_text_clean, re.IGNORECASE)
        if punter_match:
            play_info['Primary_Player'] = punter_match.group(1).strip().title()
            play_info['Result'] = 'Punt'
            
    elif 'kicks off' in detail_text_clean:
        play_info['Play_Type'] = 'Kickoff'
        kicker_match = re.search(fr'^({player_pattern})\s+kicks', detail_text_clean, re.IGNORECASE)
        if kicker_match:
            play_info['Primary_Player'] = kicker_match.group(1).strip().title()
            play_info['Result'] = 'Kick Off'
            
    elif 'kicks extra point' in detail_text_clean:
        play_info['Play_Type'] = 'Extra Point'
        kicker_match = re.search(fr'^({player_pattern})\s+kicks', detail_text_clean, re.IGNORECASE)
        if kicker_match:
            play_info['Primary_Player'] = kicker_match.group(1).strip().title()
        if 'good' in detail_text_clean:
            play_info['Result'] = 'Kick Good'
        else:
            play_info['Result'] = 'Missed Kick'
            
    elif 'field goal' in detail_text_clean:
        play_info['Play_Type'] = 'Field Goal'
        kicker_match = re.search(fr'^({player_pattern})\s+', detail_text_clean, re.IGNORECASE)
        if kicker_match:
            play_info['Primary_Player'] = kicker_match.group(1).strip().title()
        if 'good' in detail_text_clean:
            play_info['Result'] = 'Kick Good'
        else:
            play_info['Result'] = 'Missed Kick'
            
    elif 'kneels' in detail_text_clean or 'kneel' in detail_text_clean:
        play_info['Play_Type'] = 'Other'
        kneel_match = re.search(fr'^({player_pattern})\s+(?:kneels|kneel)', detail_text_clean, re.IGNORECASE)
        if kneel_match:
            play_info['Primary_Player'] = kneel_match.group(1).strip().title()
        play_info['Result'] = 'Kneel'

    # Override result for touchdowns and fumbles
    if 'touchdown' in detail_text_clean:
        play_info['Result'] = 'Touchdown'
    elif 'fumble' in detail_text_clean and not play_info['Result']:
        play_info['Result'] = 'Fumble'

    # Handle standalone penalties (only if no other play type was determined)
    if play_info['Play_Type'] is None and play_info['Penalty'] == 'Penalty':
        play_info['Play_Type'] = 'Other'
        if play_info['Penalty_Accepted'] == 'Accepted':
            play_info['Result'] = 'Penalty Accepted'
        elif play_info['Penalty_Accepted'] == 'Declined':
            play_info['Result'] = 'Penalty Declined'

    return play_info

def read_seasons_weeks_from_csv(csv_file_path):
    """
    Read seasons and weeks from a CSV file
    """
    try:
        df = pd.read_csv(csv_file_path)
        
        # Check if required columns exist
        required_columns = ['season', 'week']
        missing_columns = [col for col in required_columns if col not in df.columns.str.lower()]
        
        if missing_columns:
            print(f"Missing required columns: {missing_columns}")
            print(f"Available columns: {list(df.columns)}")
            return []
        
        # Normalize column names to lowercase
        df.columns = df.columns.str.lower()
        
        # Get unique combinations of season and week
        season_week_pairs = df[['season', 'week']].drop_duplicates().values.tolist()
        
        print(f"Found {len(season_week_pairs)} unique season/week combinations in CSV file:")
        for season, week in season_week_pairs[:10]:  # Show first 10
            print(f"  Season {season}, Week {week}")
        if len(season_week_pairs) > 10:
            print(f"  ... and {len(season_week_pairs) - 10} more")
            
        return season_week_pairs
        
    except Exception as e:
        print(f"Error reading CSV file {csv_file_path}: {e}")
        return []

def backfill_penalty_excel_files_from_list(data_directory, season_week_list):
    """
    Backfill Excel files based on a list of season/week pairs
    """
    data_path = Path(data_directory)
    processed_files = []
    error_files = []
    
    print(f"\nProcessing {len(season_week_list)} season/week combinations...")
    
    for season, week in season_week_list:
        season = int(season)
        week = int(week)
        
        season_dir = data_path / str(season)
        week_dir = season_dir / f"Week_{week}"
        
        if not season_dir.exists():
            print(f"Season directory not found: {season_dir}")
            continue
            
        if not week_dir.exists():
            print(f"Week directory not found: {week_dir}")
            continue
            
        print(f"\nProcessing Season {season}, Week {week}...")
        
        # Look for the specific Drive_Details file
        drive_details_file = week_dir / f"{season}_Week{week}_Drive_Details.xlsx"
        
        if drive_details_file.exists():
            try:
                print(f"  Processing file: {drive_details_file.name}")
                
                # Read the Excel file
                df = pd.read_excel(drive_details_file)
                original_count = len(df)
                
                print(f"    Found {original_count} rows")
                
                # Find rows where Play_Type is "Penalty" OR where Detail contains penalty
                penalty_mask = (
                    (df['Play_Type'].str.contains('penalty', case=False, na=False)) |
                    (df['Detail'].str.contains('penalty', case=False, na=False))
                )
                penalty_rows = df[penalty_mask]
                
                if len(penalty_rows) == 0:
                    print(f"    No penalty plays found, skipping...")
                    continue
                
                print(f"    Found {len(penalty_rows)} plays with penalties to reparse")
                
                # Reparse each penalty play
                updated_rows = 0
                for idx, row in penalty_rows.iterrows():
                    detail_text = str(row['Detail'])
                    
                    # Parse the play details using the updated function
                    parsed_info = parse_play_details(detail_text)
                    
                    # Store original penalty information if it exists
                    original_penalty = row.get('Penalty', None)
                    original_penalty_accepted = row.get('Penalty_Accepted', None)
                    original_penalized_player = row.get('Penalized_Player', None)
                    original_penalty_yards = row.get('Penalty_Yards', None)
                    
                    # Update the DataFrame with the reparsed information
                    for field, value in parsed_info.items():
                        if field in df.columns and value is not None:
                            # Always update Play_Type if we found a real play type
                            if field == 'Play_Type' and value not in ['Other', None]:
                                df.at[idx, field] = value
                            # Update other fields normally
                            elif field not in ['Penalty', 'Penalty_Accepted', 'Penalized_Player', 'Penalty_Yards']:
                                df.at[idx, field] = value
                            # For penalty fields, only update if we don't have existing data
                            elif field == 'Penalty' and pd.isna(original_penalty):
                                df.at[idx, field] = value
                            elif field == 'Penalty_Accepted' and pd.isna(original_penalty_accepted):
                                df.at[idx, field] = value
                            elif field == 'Penalized_Player' and pd.isna(original_penalized_player):
                                df.at[idx, field] = value
                            elif field == 'Penalty_Yards' and pd.isna(original_penalty_yards):
                                df.at[idx, field] = value
                                
                    updated_rows += 1
                    
                    # Debug print for first few rows
                    if updated_rows <= 2:
                        print(f"      Updated row {idx}: Play_Type = {df.at[idx, 'Play_Type']}")
                
                # Ensure all expected columns exist
                expected_columns = [
                    'Date', 'Season', 'Week', 'Away Team', 'Home Team', 'Game_Time',
                    'Quarter', 'Time', 'Down', 'ToGo', 'Location', 'Detail',
                    'Play_Type', 'Primary_Player', 'Receiver', 'Sack_By',
                    'Run_Location', 'Run_Gap', 'Pass_Type', 'Pass_Location',
                    'Pass_Yards', 'Field_Goal_Yards', 'Yards', 'Tackler', 'Tackler2', 'Defender',
                    'Result', 'Penalized_Player', 'Penalty_Yards', 'Penalty', 'Penalty_Accepted', 'EPB', 'EPA'
                ]
                
                # Add missing columns
                for col in expected_columns:
                    if col not in df.columns:
                        df[col] = None
                
                # Reorder to match expected format
                available_columns = [col for col in expected_columns if col in df.columns]
                df = df[available_columns]
                
                # Save the updated file
                df.to_excel(drive_details_file, index=False)
                processed_files.append(str(drive_details_file))
                print(f"    Reparsed and updated {updated_rows} penalty plays")
                
                # Show summary of changes
                updated_play_types = df[penalty_mask]['Play_Type'].value_counts()
                print(f"    Updated Play_Type distribution: {dict(updated_play_types)}")
                
            except Exception as e:
                error_files.append((str(drive_details_file), str(e)))
                print(f"    ERROR processing {drive_details_file.name}: {e}")
                traceback.print_exc()
        else:
            print(f"  Drive Details file not found: {drive_details_file.name}")
    
    # Summary
    print(f"\n=== PENALTY REPARSE SUMMARY ===")
    print(f"Successfully processed: {len(processed_files)} files")
    print(f"Errors: {len(error_files)} files")
    
    if error_files:
        print("\nFiles with errors:")
        for file_path, error in error_files:
            print(f"  {file_path}: {error}")
    
    return processed_files, error_files

def main():
    """
    Main function to run the penalty reparse backfill process
    """
    # Configuration
    DATA_DIRECTORY = r"C:\NFLStats\data"  # Removed Windows\ from path
    CSV_FILE_PATH = r"C:\NFLStats\Backfil\DriveDetailBackfil\penalty_weeks.csv"  # Removed Windows\ from path
    
    print("Starting penalty reparse backfill process...")
    print(f"Data directory: {DATA_DIRECTORY}")
    print(f"Reading season/week combinations from: {CSV_FILE_PATH}")
    
    # Check if data directory exists
    if not os.path.exists(DATA_DIRECTORY):
        print(f"Data directory not found: {DATA_DIRECTORY}")
        return
    
    # Check if CSV file exists
    if not os.path.exists(CSV_FILE_PATH):
        print(f"CSV file not found: {CSV_FILE_PATH}")
        print("Please create a CSV file with 'season' and 'week' columns")
        return
    
    # Read season/week combinations from CSV
    season_week_list = read_seasons_weeks_from_csv(CSV_FILE_PATH)
    
    if not season_week_list:
        print("No valid season/week combinations found in CSV file")
        return
    
    # Backfill Excel files based on the list
    print(f"\n=== REPARSING PENALTY PLAYS IN DRIVE DETAILS FILES ===")
    processed_files, error_files = backfill_penalty_excel_files_from_list(
        DATA_DIRECTORY, 
        season_week_list
    )
    
    if processed_files:
        print(f"\nSuccessfully processed files:")
        for file_path in processed_files:
            print(f"  ✓ {file_path}")
    
    print("\nPenalty reparse backfill completed!")

if __name__ == "__main__":
    main()