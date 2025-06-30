import os
import requests
import pandas as pd
from bs4 import BeautifulSoup
from bs4.element import Comment
import time
import traceback
import re 
import random
import urllib3
urllib3.PoolManager(maxsize=10)

# Base URL and save pathpip
base_url = "https://www.pro-football-reference.com"
save_path = "C:\\NFLStats\\data"

def make_request_with_retry(url, headers, max_retries=3, timeout=30):
    """Make HTTP request with retry logic"""
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers, timeout=timeout)
            response.raise_for_status()
            return response
        except requests.exceptions.Timeout:
            if attempt == max_retries - 1:
                print(f"Failed to fetch {url} after {max_retries} attempts due to timeout")
                raise
            print(f"Timeout occurred. Retrying... (Attempt {attempt + 1} of {max_retries})")
            time.sleep(5 * (attempt + 1))
        except requests.exceptions.RequestException as e:
            if attempt == max_retries - 1:
                print(f"Failed to fetch {url} after {max_retries} attempts: {str(e)}")
                raise
            print(f"Error occurred: {str(e)}. Retrying... (Attempt {attempt + 1} of {max_retries})")
            time.sleep(5 * (attempt + 1))

def safe_float_convert(cell):
    """Safely convert cell text to float."""
    if cell is None:
        return None
    try:
        text = cell.text.strip()
        return float(text) if text.replace('.', '', 1).isdigit() else None
    except (ValueError, AttributeError):
        return None
    
def process_drive_details(table, away_team, home_team):
    """Process drive details from play by play table including EPB and EPA with full diagnostic logging"""
    drive_details = []
    print("Processing play-by-play table...")
    
    if table:
        current_quarter = '1'
        overtime_period = 0    # Track which overtime period we're in
        ot_lowest_time_seen = None  # Track overtime progression
        rows = table.find_all('tr')
        print(f"Found {len(rows)} rows in play-by-play table")
        
        for i, row in enumerate(rows):
            # Skip header rows or rows without data
            if not row.find_all(['td', 'th']) or 'thead' in row.get('class', []):
                continue

            # Check for "Overtime" header row
            overtime_header = row.find('td', {'data-stat': 'onecell'})
            if overtime_header and 'overtime' in overtime_header.text.lower():
                overtime_period += 1  # Increment each time we see an "Overtime" header
                if overtime_period == 1:
                    current_quarter = '5'  # First overtime
                    ot_lowest_time_seen = None  # Reset time tracking
                    print(f"Found 1st Overtime header, switching to quarter {current_quarter}")
                elif overtime_period == 2:
                    current_quarter = '6'  # Second overtime  
                    ot_lowest_time_seen = None  # Reset time tracking
                    print(f"Found 2nd Overtime header, switching to quarter {current_quarter}")
                continue

            # Handle quarter changes
            quarter_cell = row.find('th', {'data-stat': 'quarter'})
            if quarter_cell:
                quarter_text = quarter_cell.text.strip()
                if quarter_text.isdigit():
                    # Only update to regular quarters if we haven't entered OT yet
                    if overtime_period == 0:
                        current_quarter = quarter_text
                    print(f"Found quarter cell with text: {quarter_text}, current_quarter: {current_quarter}")
                elif quarter_text.upper() == 'OT':
                    # Handle OT quarter detection
                    if current_quarter not in ['5', '6']:
                        # First time seeing OT
                        current_quarter = '5'
                        overtime_period = 1
                        ot_lowest_time_seen = None
                        print(f"First OT detected, switching to quarter {current_quarter}")
                    else:
                        # Already in OT, check time to determine if this is double OT
                        time_cell = row.find('td', {'data-stat': 'qtr_time_remain'})
                        if time_cell:
                            time_text = time_cell.text.strip()
                            
                            try:
                                # Convert time to seconds for comparison
                                if ':' in time_text:
                                    time_parts = time_text.split(':')
                                    current_time_seconds = int(time_parts[0]) * 60 + int(time_parts[1])
                                    
                                    if current_quarter == '5':
                                        # Track the progression of time in first OT
                                        if ot_lowest_time_seen is None:
                                            ot_lowest_time_seen = current_time_seconds
                                        else:
                                            # If we see a time that's significantly higher than our lowest,
                                            # and we've progressed well into the first OT, it's double OT
                                            if (current_time_seconds >= 600 and  # 10:00 or higher
                                                ot_lowest_time_seen < 300):      # We've been down to 5:00 or lower
                                                current_quarter = '6'
                                                overtime_period = 2
                                                ot_lowest_time_seen = current_time_seconds  # Reset for 2nd OT
                                                print(f"Detected double overtime, switching to quarter {current_quarter}")
                                            else:
                                                # Update our lowest time seen
                                                ot_lowest_time_seen = min(ot_lowest_time_seen, current_time_seconds)
                                
                            except (ValueError, IndexError):
                                # If time parsing fails, stay in current quarter
                                print(f"Failed to parse time: {time_text}")

            
            # Get all relevant cells
            cells = {
                'time': row.find('td', {'data-stat': 'qtr_time_remain'}),
                'down': row.find('td', {'data-stat': 'down'}),
                'togo': row.find('td', {'data-stat': 'yds_to_go'}),
                'location': row.find('td', {'data-stat': 'location'}),
                'detail': row.find('td', {'data-stat': 'detail'}),
                'epb': row.find('td', {'data-stat': 'exp_pts_before'}),
                'epa': row.find('td', {'data-stat': 'exp_pts_after'})
            }
            
            # Only process rows with time and detail
            if cells['time'] and cells['detail']:
                # Get the time, handling potential links
                time_text = cells['time'].text.strip()
                if cells['time'].find('a'):
                    time_text = cells['time'].find('a').text.strip()

                # Log cell content before attempting conversion
                epb_raw = cells['epb'].text.strip() if cells['epb'] else 'None'
                epa_raw = cells['epa'].text.strip() if cells['epa'] else 'None'
                print(f"Row {i}: EPB raw content: '{epb_raw}'")
                print(f"Row {i}: EPA raw content: '{epa_raw}'")

                try:
                    # Use safe conversion for EPB and EPA values
                    epb_value = safe_float_convert(cells['epb'])
                    epa_value = safe_float_convert(cells['epa'])
                    
                    # Log converted values
                    print(f"Row {i}: EPB after conversion: {epb_value}")
                    print(f"Row {i}: EPA after conversion: {epa_value}")
                except Exception as e:
                    print(f"Row {i}: Error converting EPB/EPA values - {e}")
                    continue  # Skip this row if conversion fails
                
                # Create the basic detail dictionary
                detail = {
                    'Quarter': current_quarter,
                    'Time': time_text,
                    'Down': cells['down'].text.strip() if cells['down'] else None,
                    'ToGo': cells['togo'].text.strip() if cells['togo'] else None,
                    'Location': cells['location'].text.strip() if cells['location'] else None,
                    'Detail': cells['detail'].get_text(separator=' ').strip(),
                    'EPB': epb_value,
                    'EPA': epa_value
                }
                
                # Add parsed play information
                if detail['Detail']:
                    play_info = parse_play_details(detail['Detail'])
                    detail.update(play_info)
                    print(f"Updated detail: {detail}")
                
                drive_details.append(detail)
                print(f"Added play: Q{detail['Quarter']} - {detail['Time']} - {detail['Detail'][:50]}...")

                # Debug print for overtime plays
                if current_quarter in ['5', '6']:
                    print(f"OT Play: Q{detail['Quarter']} - {detail['Time']} - {detail['Detail'][:50]}...")
    
    print(f"Processed {len(drive_details)} plays")
    return drive_details

def extract_commented_html(soup, div_id):
    """Extract HTML from comments within a div"""
    div = soup.find('div', id=div_id)
    if not div:
        return None
        
    # Find comments in the div
    from bs4.element import Comment
    comments = div.find_all(string=lambda text: isinstance(text, Comment))
    
    # Parse all comments and combine them
    combined_html = ''
    for comment in comments:
        if 'table' in comment.lower():
            combined_html += str(comment)
    
    if combined_html:
        return BeautifulSoup(combined_html, 'html.parser')
            
    return None

def scrape_game_details(main_page_soup, box_score_soup, date_text, season, week, away_team, home_team):
    """Scrape game details including drive information"""
    print(f"\nProcessing game: {away_team} vs {home_team}")
    
    game_info = {
        'Date': date_text,
        'Season': season,
        'Week': week,
        'Away Team': away_team,
        'Home Team': home_team,
        'Game_Time': None
    }
    
    # Get game time
    scorebox_meta = box_score_soup.find('div', class_='scorebox_meta')
    if scorebox_meta:
        start_time_div = scorebox_meta.find(string=lambda text: 'Start Time' in text if text else False)
        if start_time_div:
            time_text = start_time_div.find_next(string=True)
            if time_text:
                cleaned_time = time_text.strip().replace(': ', '')
                game_info['Game_Time'] = cleaned_time
                print(f"Found game time: {cleaned_time}")
    
    # Find play-by-play data
    play_by_play_div = box_score_soup.find('div', id='div_pbp')
    pbp_table = None
    
    if play_by_play_div:
        # Try to find the table directly first
        pbp_table = play_by_play_div.find('table', {'id': 'pbp'})
        
        # If not found directly, look in comments
        if not pbp_table:
            comments = play_by_play_div.find_all(string=lambda text: isinstance(text, Comment))
            for comment in comments:
                if 'table' in comment.lower() and 'pbp' in comment.lower():
                    comment_soup = BeautifulSoup(comment, 'html.parser')
                    pbp_table = comment_soup.find('table', {'id': 'pbp'})
                    if pbp_table:
                        print("Found play-by-play table in comments")
                        break
    
    if not pbp_table:
        # Try extracting from commented HTML as backup
        comment_soup = extract_commented_html(box_score_soup, 'all_pbp')
        if comment_soup:
            pbp_table = comment_soup.find('table', {'id': 'pbp'})
            if pbp_table:
                print("Found play-by-play table in extracted comments")
    
    if pbp_table:
        print("Processing play-by-play table...")
        all_plays = []
        current_quarter = '1'
        overtime_period = 0  # Track which overtime period we're in
        ot_lowest_time_seen = None  # Track overtime progression
        rows = pbp_table.find_all('tr')
        print(f"Found {len(rows)} rows in play-by-play table")
        
        for row in rows:
            # Skip header rows
            if not row.find_all(['td', 'th']) or 'thead' in row.get('class', []):
                continue

            # Check for "Overtime" header row
            overtime_header = row.find('td', {'data-stat': 'onecell'})
            if overtime_header and 'overtime' in overtime_header.text.lower():
                overtime_period += 1  # Increment each time we see an "Overtime" header
                if overtime_period == 1:
                    current_quarter = '5'  # First overtime
                    ot_lowest_time_seen = None  # Reset time tracking
                    print(f"Found 1st Overtime header, switching to quarter {current_quarter}")
                elif overtime_period == 2:
                    current_quarter = '6'  # Second overtime  
                    ot_lowest_time_seen = None  # Reset time tracking
                    print(f"Found 2nd Overtime header, switching to quarter {current_quarter}")
                continue

            # Handle quarter changes
            quarter_cell = row.find('th', {'data-stat': 'quarter'})
            if quarter_cell:
                quarter_text = quarter_cell.text.strip()
                if quarter_text.isdigit():
                    # Only update to regular quarters if we haven't entered OT yet
                    if overtime_period == 0:
                        current_quarter = quarter_text
                    print(f"Found quarter cell with text: {quarter_text}, current_quarter: {current_quarter}")
                elif quarter_text.upper() == 'OT':
                    # Handle OT quarter detection
                    if current_quarter not in ['5', '6']:
                        # First time seeing OT
                        current_quarter = '5'
                        overtime_period = 1
                        ot_lowest_time_seen = None
                        print(f"First OT detected, switching to quarter {current_quarter}")
                    else:
                        # Already in OT, check time to determine if this is double OT
                        time_cell = row.find('td', {'data-stat': 'qtr_time_remain'})
                        if time_cell:
                            time_text = time_cell.text.strip()
                            
                            try:
                                # Convert time to seconds for comparison
                                if ':' in time_text:
                                    time_parts = time_text.split(':')
                                    current_time_seconds = int(time_parts[0]) * 60 + int(time_parts[1])
                                    
                                    if current_quarter == '5':
                                        # Track the progression of time in first OT
                                        if ot_lowest_time_seen is None:
                                            ot_lowest_time_seen = current_time_seconds
                                        else:
                                            # If we see a time that's significantly higher than our lowest,
                                            # and we've progressed well into the first OT, it's double OT
                                            if (current_time_seconds >= 600 and  # 10:00 or higher
                                                ot_lowest_time_seen < 300):      # We've been down to 5:00 or lower
                                                current_quarter = '6'
                                                overtime_period = 2
                                                ot_lowest_time_seen = current_time_seconds  # Reset for 2nd OT
                                                print(f"Detected double overtime, switching to quarter {current_quarter}")
                                            else:
                                                # Update our lowest time seen
                                                ot_lowest_time_seen = min(ot_lowest_time_seen, current_time_seconds)
                                
                            except (ValueError, IndexError):
                                # If time parsing fails, stay in current quarter
                                print(f"Failed to parse time: {time_text}")
            
            # Get play details
            detail_cell = row.find('td', {'data-stat': 'detail'})
            if detail_cell:
                detail_text = detail_cell.get_text(separator=' ').strip()
                
                if detail_text:
                    # Create base play info
                    play_info = {
                        'Quarter': current_quarter,
                        'Time': row.find('td', {'data-stat': 'qtr_time_remain'}).text.strip() if row.find('td', {'data-stat': 'qtr_time_remain'}) else None,
                        'Down': row.find('td', {'data-stat': 'down'}).text.strip() if row.find('td', {'data-stat': 'down'}) else None,
                        'ToGo': row.find('td', {'data-stat': 'yds_to_go'}).text.strip() if row.find('td', {'data-stat': 'yds_to_go'}) else None,
                        'Location': row.find('td', {'data-stat': 'location'}).text.strip() if row.find('td', {'data-stat': 'location'}) else None,
                        'Detail': detail_text
                    }
                    
                    # Get EPB and EPA
                    epb_cell = row.find('td', {'data-stat': 'exp_pts_before'})
                    epa_cell = row.find('td', {'data-stat': 'exp_pts_after'})
                    
                    try:
                        play_info['EPB'] = float(epb_cell.text.strip()) if epb_cell and epb_cell.text.strip() else None
                        play_info['EPA'] = float(epa_cell.text.strip()) if epa_cell and epa_cell.text.strip() else None
                    except (ValueError, AttributeError):
                        play_info['EPB'] = None
                        play_info['EPA'] = None
                    
                    # Parse play details
                    parsed_details = parse_play_details(detail_text)
                    play_info.update(parsed_details)
                    
                    # Add game info
                    play_info.update(game_info)
                    
                    all_plays.append(play_info)
                    
                    # Debug print for overtime plays
                    if current_quarter in ['5', '6']:
                        print(f"OT Play: Q{play_info['Quarter']} - {play_info['Time']} - {detail_text[:50]}...")
        
        if all_plays:
            # Create DataFrame
            drive_df = pd.DataFrame(all_plays)
            
            # Define required columns
            required_columns = [
                'Date', 'Season', 'Week', 'Away Team', 'Home Team', 'Game_Time',
                'Quarter', 'Time', 'Down', 'ToGo', 'Location', 'Detail',
                'Play_Type', 'Primary_Player', 'Receiver', 'Sack_By',
                'Run_Location', 'Run_Gap', 'Pass_Type', 'Pass_Location',
                'Pass_Yards', 'Field_Goal_Yards', 'Yards', 'Tackler', 'Tackler2', 'Defender',
                'Result', 'Penalized_Player', 'Penalty_Yards', 'Penalty', 'Penalty_Accepted', 'EPB', 'EPA'
            ]
            
            # Ensure all columns exist and are in correct order
            for col in required_columns:
                if col not in drive_df.columns:
                    drive_df[col] = None
            
            drive_df = drive_df[required_columns]
            
            print(f"Successfully processed {len(drive_df)} plays")
            
            # Debug: Print quarter distribution
            quarter_counts = drive_df['Quarter'].value_counts().sort_index()
            print(f"Quarter distribution: {dict(quarter_counts)}")
            
            return drive_df
        else:
            print("No plays found in table")
    else:
        print("Could not find play-by-play table")
    
    return pd.DataFrame()

def parse_play_details(detail_text):
    """Parse play details into structured categories with improved pattern matching."""
    print(f"Parsing detail: {detail_text}")
    
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
        'Penalty': None,  # New field - will be "Penalty" if there's a penalty, None if not
        'Penalty_Accepted': None,  # New field - "Accepted" or "Declined"
        'Detail': detail_text
    }

    if not detail_text:
        return play_info

    # Clean and normalize the text
    detail_text = ' '.join(detail_text.lower().split())

    # Player name pattern
    player_pattern = r'(?:[A-Z][A-Za-z\'\.-]*\s*){1,4}'

    # Extract yards first
    yards_patterns = [
        r'for (\-?\d+) yards?',
        r'(\-?\d+) yard (gain|loss)',
        r'loses (\-?\d+) yards?',
        r'gains (\d+) yards?',
        r'punts (\d+) yards',
        r'\s+(\d+)\s+yard\s+field\s+goal'
    ]
    for pattern in yards_patterns:
        yards_match = re.search(pattern, detail_text)
        if yards_match:
            yards = int(yards_match.group(1))
            play_info['Yards'] = yards
            break

    # Check for penalty FIRST - this will apply to any play type
    if 'penalty' in detail_text:
        # Set penalty field
        play_info['Penalty'] = 'Penalty'
        
        # Determine if penalty was accepted or declined
        if 'decline' in detail_text:
            play_info['Penalty_Accepted'] = 'Declined'
        else:
            play_info['Penalty_Accepted'] = 'Accepted'
        
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
                play_info['Penalized_Player'] = penalty_match.group(1).strip().title()
                break
        
        # Extract penalty yards - try multiple patterns for different penalty descriptions
        penalty_yard_patterns = [
            r'penalty[^,]*,\s*(\d+)\s*yards?',  # "penalty on Smith, 10 yards"
            r'(\d+)\s*yard\s*penalty',          # "10 yard penalty"
            r'penalty[^,]*(\d+)\s*yards?',      # "penalty 10 yards"
            r',\s*(\d+)\s*yards?,\s*(?:accepted|declined)'  # ", 10 yards, accepted"
        ]
        
        for pattern in penalty_yard_patterns:
            penalty_yards = re.search(pattern, detail_text, re.IGNORECASE)
            if penalty_yards:
                play_info['Penalty_Yards'] = int(penalty_yards.group(1))
                break

    # Continue with normal play type classification regardless of penalty
    # The penalty fields are already set above, now we need to determine the actual play type

    # Handles Two Points
    if 'two point attempt:' in detail_text:
        play_info['Play_Type'] = 'Two Point'

        # Extract primary player
        qb_match = re.search(r'two point attempt:\s+([A-Z][a-zA-Z\'\.-]+(?:\s+[A-Z][a-zA-Z\'\.-]+)*?)(?=\s+(?:pass|left|right|runs|rushed|up|rushes))', detail_text, re.IGNORECASE)
        if qb_match:
            play_info['Primary_Player'] = qb_match.group(1).strip().title()

        # Handle pass attempts
        if 'pass' in detail_text:
            receiver_match = re.search(fr'(?:complete to|incomplete intended for)\s+({player_pattern})', detail_text, re.IGNORECASE)
            if receiver_match:
                play_info['Receiver'] = receiver_match.group(1).strip().title()

            # Set pass location
            if ' left' in detail_text:
                play_info['Pass_Location'] = 'Left'
            elif ' right' in detail_text:
                play_info['Pass_Location'] = 'Right'
            elif ' middle' in detail_text:
                play_info['Pass_Location'] = 'Middle'

            # Set result
            if 'complete' in detail_text and 'conversion succeeds' in detail_text:
                play_info['Result'] = 'Two Point Converted'
            elif 'incomplete' in detail_text or 'conversion fails' in detail_text:
                play_info['Result'] = 'Two Point Failed'

        # Handle run attempts
        else:
            location_patterns = [
                (r'left end', ('Left', 'End')),
                (r'right end', ('Right', 'End')),
                (r'left tackle', ('Left', 'Tackle')),
                (r'right tackle', ('Right', 'Tackle')),
                (r'left guard', ('Left', 'Guard')),
                (r'right guard', ('Right', 'Guard')),
                (r'up the middle', ('Middle', 'Middle')),
                (r' middle', ('Middle', 'Middle')),
                (r' left(?!\w)', ('Left', None)),
                (r' right(?!\w)', ('Right', None))
            ]

            for pattern, (location, gap) in location_patterns:
                if re.search(pattern, detail_text):
                    play_info['Run_Location'] = location
                    if gap:
                        play_info['Run_Gap'] = gap
                    break

            # Set result
            if 'conversion succeeds' in detail_text:
                play_info['Result'] = 'Two Point Converted'
            elif 'conversion fails' in detail_text:
                play_info['Result'] = 'Two Point Failed'

    #Handles Aborted snaps
    elif 'aborted' in detail_text:
        play_info['Play_Type'] = 'Aborted'

        # Handles Primary Player
        name_text = detail_text.split('aborted snap')[0].strip()
        if name_text:
            play_info['Primary_Player'] = name_text.title()

        # Handle yards
        if 'no gain' in detail_text:
            play_info['Yards'] = 0
        else:
            yards_match = re.search(r'for\s+(-?\d+)\s+yards?', detail_text)
            if yards_match:
                play_info['Yards'] = int(yards_match.group(1))

        # Handles Results
        if 'recovered' in detail_text:
            play_info['Result'] = 'Recovered'
        else:
            play_info['Result'] = 'Aborted'

    # Handles Run Plays
    elif any(x in detail_text for x in [
        'left tackle for', 'right tackle for', 'left guard for', 'right guard for', 
        'left end for', 'right end for', 'up the middle for', 'runs for', 'rushed for',
        ' middle run', ' left run', ' right run', 'middle for', 'scrambles'
    ]):
        play_info['Play_Type'] = 'Run'
        
        # Check for scrambles first
        if 'scrambles' in detail_text:
            # Get everything before 'scrambles' and clean it
            name_text = detail_text.split('scrambles')[0].strip()
            if name_text:  # Make sure we have text
                play_info['Primary_Player'] = name_text.title()
        else:
            # Handle other run plays with existing patterns
            runner_patterns = [
                fr'^({player_pattern})\s+(?:left|right|up the middle|runs|rushed)',
                fr'^({player_pattern})\s+(?:tackle|guard|end)',
                fr'^({player_pattern})\s+middle',
                fr'^({player_pattern})\s+middle for'
            ]
            for pattern in runner_patterns:
                runner_match = re.search(pattern, detail_text, re.IGNORECASE)
                if runner_match:
                    play_info['Primary_Player'] = runner_match.group(1).strip().title()
                    break

        # Run location and gap
        location_patterns = [
            (r'left end', ('Left', 'End')),
            (r'right end', ('Right', 'End')),
            (r'left tackle', ('Left', 'Tackle')),
            (r'right tackle', ('Right', 'Tackle')),
            (r'left guard', ('Left', 'Guard')),
            (r'right guard', ('Right', 'Guard')),
            (r'up the middle', ('Middle', 'Middle')),
            (r' middle', ('Middle', 'Middle')),
            (r' left(?!\w)', ('Left', None)),
            (r' right(?!\w)', ('Right', None)),
            (r'middle for', ('Middle', 'Middle')),
        ]
        for pattern, (location, gap) in location_patterns:
            if re.search(pattern, detail_text):
                play_info['Run_Location'] = location
                play_info['Run_Gap'] = gap
                break

        # Set run result based on yards
        if 'no gain' in detail_text:
            play_info['Yards'] = 0
            play_info['Result'] = 'No Gain'
        # Then check yards for other cases
        elif play_info['Yards'] is not None:
            if play_info['Yards'] > 0:
                play_info['Result'] = 'Gain'
            elif play_info['Yards'] < 0:
                play_info['Result'] = 'Loss'

    # Handle passes
    elif ('Two Point Attempt:' not in detail_text and 
      ('pass complete' in detail_text or 
       'pass incomplete' in detail_text or 
       'sacked' in detail_text or 
       ('pass' in detail_text and any(x in detail_text for x in ['to', 'intended for']))
      )
    ):
        play_info['Play_Type'] = 'Pass'
        
        # Extract quarterback
        qb_patterns = [
            fr'^({player_pattern})\s+pass',
            fr'^Two Point Attempt:\s+({player_pattern})\s+pass',
            fr'^({player_pattern})\s+sacked',
            fr'^({player_pattern})\s+pass\s+incomplete',
            fr'^({player_pattern})\s+pass\s+complete'
        ]
        for pattern in qb_patterns:
            qb_match = re.search(pattern, detail_text, re.IGNORECASE)
            if qb_match:
                play_info['Primary_Player'] = qb_match.group(1).strip().title()
                break

        # Handle sacks
        if 'sacked' in detail_text:
            sacker_match = re.search(fr'sacked by\s+({player_pattern}?)\s*(?=(?:\s+for|\s+is|\s+\(|$|\.))', detail_text, re.IGNORECASE)
            if sacker_match:
                play_info['Sack_By'] = sacker_match.group(1).strip().title()
            play_info['Result'] = 'Sack'
        else:
            # Extract receiver if not a sack
            receiver_patterns = [
                fr'(?:intended for|complete to|incomplete to|to)\s+({player_pattern}?)\s*(?=(?:\s+for|\s+is|\s+\(|$|\.))',
            ]
            for pattern in receiver_patterns:
                receiver_match = re.search(pattern, detail_text, re.IGNORECASE)
                if receiver_match:
                    play_info['Receiver'] = receiver_match.group(1).strip().title()
                    break

            # Pass type and location
            if 'short' in detail_text:
                play_info['Pass_Type'] = 'Short'
            elif 'deep' in detail_text:
                play_info['Pass_Type'] = 'Deep'

            if ' left' in detail_text:
                play_info['Pass_Location'] = 'Left'
            elif ' right' in detail_text:
                play_info['Pass_Location'] = 'Right'
            elif ' middle' in detail_text:
                play_info['Pass_Location'] = 'Middle'

            # Pass result
            if 'incomplete' in detail_text:
                play_info['Result'] = 'Incomplete'
            elif 'intercepted' in detail_text:
                play_info['Result'] = 'Interception'
            elif 'complete' in detail_text:
                play_info['Result'] = 'Complete'
                if play_info['Yards'] is not None:
                    play_info['Pass_Yards'] = play_info['Yards']

    # Handle punts
    elif 'punts' in detail_text:
        play_info['Play_Type'] = 'Punt'
        punter_match = re.search(fr'^({player_pattern})\s+punts', detail_text, re.IGNORECASE)
        if punter_match:
            play_info['Primary_Player'] = punter_match.group(1).strip().title()
            play_info['Result'] = 'Punt'
        
        returner_match = re.search(fr'returned by\s+({player_pattern}?)\s*(?=(?:\s+for|\s+is|\s+\(|$|\.))', detail_text, re.IGNORECASE)
        if returner_match:
            play_info['Receiver'] = returner_match.group(1).strip().title()

    # Handle kickoffs
    elif 'kicks off' in detail_text:
        play_info['Play_Type'] = 'Kickoff'
        kicker_match = re.search(fr'^({player_pattern})\s+kicks', detail_text, re.IGNORECASE)
        if kicker_match:
            play_info['Primary_Player'] = kicker_match.group(1).strip().title()
            play_info['Result'] = 'Kick Off'

    # Handles Extra Points
    elif 'kicks extra point' in detail_text:
        play_info['Play_Type'] = 'Extra Point'  
        kicker_match = re.search(fr'^({player_pattern})\s+kicks', detail_text, re.IGNORECASE)
        if kicker_match:
            play_info['Primary_Player'] = kicker_match.group(1).strip().title() 
        if 'no' in detail_text or 'block' in detail_text:
            play_info['Result'] = 'Missed Kicked'
        elif 'extra point good' in detail_text or 'good' in detail_text:
            play_info['Result'] = 'Kick Good'

    # Handles Field Goals
    elif 'field goal' in detail_text:   
        play_info['Play_Type'] = 'Field Goal'  
        kicker_match = re.search(fr'^({player_pattern})\s+', detail_text, re.IGNORECASE)
        if kicker_match:
            play_info['Primary_Player'] = kicker_match.group(1).strip().title()
        if 'no' in detail_text or 'block' in detail_text:
            play_info['Result'] = 'Missed Kicked'
        elif 'field goal good' in detail_text or 'good' in detail_text:
            play_info['Result'] = 'Kick Good'

    # Handles Kneels and other Play Types
    elif 'kneels' in detail_text or 'kneel' in detail_text:
        play_info['Play_Type'] = 'Other'
        
        # Extract primary player for kneel plays
        kneel_match = re.search(r'^([A-Z][a-zA-Z\'\.-]+(?:\s+[A-Z][a-zA-Z\'\.-]+)*?)(?=\s+(?:kneels|kneel))', detail_text, re.IGNORECASE)
        if kneel_match:
            play_info['Primary_Player'] = kneel_match.group(1).strip().title()
            
        # Set result as Kneel
        play_info['Result'] = 'Kneel'

    # Extract tacklers (applies to any play type)
    tackle_match = re.search(r'tackle by\s+([^)]+)', detail_text)
    if tackle_match:
        tacklers = tackle_match.group(1).strip().split(' and ')
        play_info['Tackler'] = tacklers[0].strip().title()
        if len(tacklers) > 1:
            play_info['Tackler2'] = tacklers[1].strip().title()

    # Handles Defenders (applies to any play type)
    defender_match = re.search(r'defended by\s+([A-Z][a-zA-Z\'\.-]*(?:\s+[A-Z][a-zA-Z\'\.-]+)*)', detail_text, re.IGNORECASE)
    if defender_match:
        play_info['Defender'] = defender_match.group(1).strip().title()

    # Override result for touchdowns and fumbles (applies to any play type)
    if 'touchdown' in detail_text:
        play_info['Result'] = 'Touchdown'
    elif 'fumble' in detail_text and not play_info['Result']:
        play_info['Result'] = 'Fumble'

    # Handle standalone penalties (only if no other play type was determined)
    if play_info['Play_Type'] is None and play_info['Penalty'] == 'Penalty':
        play_info['Play_Type'] = 'Other'
        if play_info['Penalty_Accepted'] == 'Accepted':
            play_info['Result'] = 'Penalty Accepted'
        elif play_info['Penalty_Accepted'] == 'Declined':
            play_info['Result'] = 'Penalty Declined'

    print(f"Parsed play info: {play_info}")
    return play_info

def extract_player_name(text):
    """Extract the primary player name from the play text."""
    words = text.split()
    name_parts = []
    for word in words[:3]:  # Look at first 3 words
        if word[0].isupper():  # Names start with capital letters
            name_parts.append(word)
        else:
            break
    return ' '.join(name_parts) if name_parts else None

def clean_player_name(name):
    """Clean and standardize player names."""
    if name:
        # Remove extra spaces and standardize formatting
        name = ' '.join(name.split())
        # Ensure proper capitalization
        return name.strip().title()
    return None

# Helper function for the DataFrame cleanup
def cleanup_dataframe(df):
    """Clean up the DataFrame after parsing all plays."""
    # Ensure all columns exist
    required_columns = [
        'Play_Type', 'Primary_Player', 'Receiver', 'Sack_By', 
        'Run_Location', 'Run_Gap', 'Pass_Type', 'Pass_Location', 
        'Pass_Yards', 'Field_Goal_Yards', 'Yards', 'Tackler',
        'Tackler2', 'Defender', 'Result', 'Penalized_Player', 'Penalty_Yards', 'Penalty', 'Penalty_Accepted'
    ]
    
    for col in required_columns:
        if col not in df.columns:
            df[col] = None
            
    # Convert numeric columns
    numeric_cols = ['Pass_Yards', 'Field_Goal_Yards', 'Yards', 'Penalty_Yards']
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')
        
    return df


def make_request_with_retry(url, headers, max_retries=3, timeout=30):
    """Make HTTP request with retry logic"""
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers, timeout=timeout)
            response.raise_for_status()
            return response
        except requests.exceptions.Timeout:
            if attempt == max_retries - 1:
                print(f"Failed to fetch {url} after {max_retries} attempts due to timeout")
                raise
            print(f"Timeout occurred. Retrying... (Attempt {attempt + 1} of {max_retries})")
            time.sleep(5 * (attempt + 1))
        except requests.exceptions.RequestException as e:
            if attempt == max_retries - 1:
                print(f"Failed to fetch {url} after {max_retries} attempts: {str(e)}")
                raise
            print(f"Error occurred: {str(e)}. Retrying... (Attempt {attempt + 1} of {max_retries})")
            time.sleep(5 * (attempt + 1))

def scrape_nfl_data(season, week):
    week_url = f"{base_url}/years/{season}/week_{week}.htm"
    week_path = os.path.join(save_path, str(season), f"Week_{week}")
    os.makedirs(week_path, exist_ok=True)
    
    try:
        print(f"Starting scrape for Week {week}...")
        time.sleep(5)
        
        response = make_request_with_retry(week_url, {})
        main_page_soup = BeautifulSoup(response.text, 'lxml')
        
        all_drive_details = []
        
        games = main_page_soup.find_all('div', class_='game_summary')
        if not games:
            print(f"No games found for Season {season}, Week {week}")
            return
            
        for game_index, game in enumerate(games):
            try:
                print(f"Processing game {game_index + 1} of {len(games)}")
                
                # Improved error handling for game data extraction
                boxscore_link = game.find('td', class_='right gamelink')
                if not boxscore_link or not boxscore_link.find('a'):
                    print(f"No boxscore link found for game {game_index + 1}")
                    continue
                    
                full_boxscore_url = base_url + boxscore_link.find('a')['href']
                
                # Add more descriptive logging
                print(f"Fetching boxscore from: {full_boxscore_url}")
                
                box_score_response = make_request_with_retry(full_boxscore_url, {})
                box_score_soup = BeautifulSoup(box_score_response.text, 'lxml')
                
                team_rows = game.find_all('tr', class_=['winner', 'loser','draw'])
                if len(team_rows) < 2:
                    print(f"Invalid team data for game {game_index + 1}")
                    continue
                    
                away_team = team_rows[0].find('td')
                home_team = team_rows[1].find('td')
                
                if not away_team or not home_team:
                    print(f"Missing team data for game {game_index + 1}")
                    continue
                    
                away_team = away_team.text.strip()
                home_team = home_team.text.strip()
                
                date_row = game.find('tr', class_='date')
                if not date_row or not date_row.find('td'):
                    print(f"Missing date for game {game_index + 1}")
                    continue
                
                game_date = date_row.find('td').text.strip()
                
                # Add diagnostic print before processing details
                print(f"Processing details for {away_team} vs {home_team} on {game_date}")
                
                drive_df = scrape_game_details(game, box_score_soup, game_date, season, week, away_team, home_team)
                
                if not drive_df.empty:
                    all_drive_details.append(drive_df)
                    print(f"Successfully processed {len(drive_df)} plays")
                else:
                    print(f"No plays found for game {game_index + 1}")
                
                if game_index < len(games) - 1:
                    time.sleep(3)
                    
            except Exception as e:
                print(f"Error processing game {game_index + 1}: {str(e)}")
                traceback.print_exc()  # Add stack trace for better debugging
                continue
        
        if all_drive_details:
            try:
                print("Combining all game data...")
                combined_df = pd.concat(all_drive_details, ignore_index=True)
                
                # Print shape and columns for debugging
                print(f"Combined DataFrame shape: {combined_df.shape}")
                print("Columns found:", combined_df.columns.tolist())
                
                columns_order = [
                    'Date', 'Season', 'Week', 'Away Team', 'Home Team', 'Game_Time',
                    'Quarter', 'Time', 'Down', 'ToGo', 'Location', 'Detail',
                    'Play_Type', 'Primary_Player', 'Receiver', 'Sack_By', 
                    'Run_Location', 'Run_Gap', 'Pass_Type', 'Pass_Location', 
                    'Pass_Yards', 'Field_Goal_Yards', 'Yards', 'Tackler', 'Tackler2', 'Defender','Result',
                    'Penalized_Player', 'Penalty_Yards', 'Penalty', 'Penalty_Accepted', 'EPB', 'EPA'
                ]
                
                for col in columns_order:
                    if col not in combined_df.columns:
                        print(f"Adding missing column: {col}")
                        combined_df[col] = None
                
                combined_df = combined_df[columns_order]
                
                file_path = os.path.join(week_path, f"{season}_Week{week}_Drive_Details.xlsx")
                print(f"Saving data to {file_path}")
                combined_df.to_excel(file_path, index=False)
                print(f"Successfully saved {len(combined_df)} plays")
                
                # Print sample data for verification
                print("\nSample of saved data:")
                print(combined_df.head())
                
            except Exception as e:
                print(f"Error saving drive details: {e}")
                traceback.print_exc()
                
    except Exception as e:
        print(f"Failed to fetch week data for Season {season}, Week {week}: {str(e)}")
        traceback.print_exc()

# Additional helper function to clean the parsed data

def clean_parsed_data(df):
    """Clean and standardize the parsed data"""
    # Convert Tackler from list to string if necessary
    if 'Tackler' in df.columns:
        df['Tackler'] = df['Tackler'].apply(
            lambda x: ', '.join(x) if isinstance(x, list) else x
        )
    
    # Ensure numeric columns are properly typed
    numeric_columns = ['Yards', 'Pass_Yards', 'Penalty_Yards', 'EPB', 'EPA']
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    return df
def main():
    seasons = [2005]  # Modify as needed
    weeks = list(range(1,2))  # Scrape all regular season weeks
    
    for season in seasons:
        for week in weeks:
            print(f"\nScraping data for Season {season}, Week {week}")
            scrape_nfl_data(season, week)
            time.sleep(5)  # Add delay between weeks

if __name__ == "__main__":
    main()