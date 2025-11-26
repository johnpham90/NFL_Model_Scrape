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
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from Utilis.weeks_api import get_nfl_current_week, get_nfl_previous_week
from prefect import flow, task
from prefect.cache_policies import NO_CACHE

# Base URL and save path
base_url = "https://www.pro-football-reference.com"
save_path = "C:\\NFLStats\\data"

@task(cache_policy=NO_CACHE)
def get_current_and_previous_week():
    """Get current and previous NFL weeks"""
    current_week = get_nfl_current_week()
    previous_week = get_nfl_previous_week()
    return current_week, previous_week

@task(cache_policy=NO_CACHE)
def create_directories(season, week):
    """Create necessary directories for data storage"""
    week_path = os.path.join(save_path, str(season), f"Week_{week}")
    os.makedirs(week_path, exist_ok=True)
    return week_path

@task(cache_policy=NO_CACHE)
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

@task(cache_policy=NO_CACHE)
def safe_float_convert(cell):
    """Safely convert cell text to float."""
    if cell is None:
        return None
    try:
        text = cell.text.strip()
        return float(text) if text.replace('.', '', 1).isdigit() else None
    except (ValueError, AttributeError):
        return None

@task(cache_policy=NO_CACHE)
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

@task(cache_policy=NO_CACHE)
def fetch_week_page(season, week):
    """Fetch the main week page and extract game information"""
    week_url = f"{base_url}/years/{season}/week_{week}.htm"
    headers = {}
    
    print(f"Starting scrape for Week {week}...")
    time.sleep(5)
    
    response = make_request_with_retry(week_url, headers)
    soup = BeautifulSoup(response.text, 'lxml')
    
    games_info = []
    games = soup.find_all('div', class_='game_summary')
    
    if not games:
        print(f"No games found for Season {season}, Week {week}")
        return games_info
    
    for game_index, game in enumerate(games):
        try:
            print(f"Processing game {game_index + 1} of {len(games)}")
            
            # Improved error handling for game data extraction
            boxscore_link = game.find('td', class_='right gamelink')
            if not boxscore_link or not boxscore_link.find('a'):
                print(f"No boxscore link found for game {game_index + 1}")
                continue
                
            full_boxscore_url = base_url + boxscore_link.find('a')['href']
            
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
            
            games_info.append({
                'game_index': game_index,
                'away_team': away_team,
                'home_team': home_team,
                'game_date': game_date,
                'boxscore_url': full_boxscore_url
            })
            
        except Exception as e:
            print(f"Error processing game {game_index + 1}: {str(e)}")
            continue
    
    return games_info

@task(cache_policy=NO_CACHE)
def scrape_single_game_data(game_info, season, week):
    """Scrape play-by-play data for a single game"""
    game_index = game_info['game_index']
    away_team = game_info['away_team']
    home_team = game_info['home_team']
    game_date = game_info['game_date']
    boxscore_url = game_info['boxscore_url']
    
    def parse_play_details(detail_text):
        """Parse play details into structured categories - moved back inside for performance"""
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

                return play_info
            
        # Handles Penalty Plays
        elif 'penalty' in detail_text:
            play_info['Play_Type'] = 'Penalty'
            penalty_match = re.search(fr'penalty on\s+({player_pattern})', detail_text, re.IGNORECASE)
            if penalty_match:
                play_info['Penalized_Player'] = penalty_match.group(1).strip().title()
            penalty_yards = re.search(r'(\d+) yards', detail_text)
            if penalty_yards:
                play_info['Penalty_Yards'] = int(penalty_yards.group(1))

            # Sets the Penalty Results
            if 'accepted' in detail_text:
                play_info['Result'] = 'Penalty Accepted'
            elif 'declined' in detail_text:
                play_info['Result'] = 'Penalty Declined'
                return play_info
            
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
                play_info['Result'] ='Aborted'
            

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

         # Extract tacklers
        tackle_match = re.search(r'tackle by\s+([^)]+)', detail_text)
        if tackle_match:
            tacklers = tackle_match.group(1).strip().split(' and ')
            play_info['Tackler'] = tacklers[0].strip().title()
            if len(tacklers) > 1:
                play_info['Tackler2'] = tacklers[1].strip().title()

        # Handles Defenders
        defender_match = re.search(r'defended by\s+([A-Z][a-zA-Z\'\.-]*(?:\s+[A-Z][a-zA-Z\'\.-]+)*)', detail_text, re.IGNORECASE)
        if defender_match:
            play_info['Defender'] = defender_match.group(1).strip().title()


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
                
                return play_info

        # Override result for touchdowns and fumbles
        if 'touchdown' in detail_text:
            play_info['Result'] = 'Touchdown'
        elif 'fumble' in detail_text and not play_info['Result']:
            play_info['Result'] = 'Fumble'

        print(f"Parsed play info: {play_info}")
        return play_info
    
    print(f"Add diagnostic print before processing details")
    print(f"Processing details for {away_team} vs {home_team} on {game_date}")
    print(f"Fetching boxscore from: {boxscore_url}")
    
    try:
        headers = {}
        box_score_response = make_request_with_retry(boxscore_url, headers)
        box_score_soup = BeautifulSoup(box_score_response.text, 'lxml')
        
        # Get game info
        game_info_data = {
            'Date': game_date,
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
                    game_info_data['Game_Time'] = cleaned_time
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
                        play_info.update(game_info_data)
                        
                        all_plays.append(play_info)
                        
                        # Debug print for overtime plays
                        if current_quarter in ['5', '6']:
                            print(f"OT Play: Q{play_info['Quarter']} - {play_info['Time']} - {detail_text[:50]}...")
            
            if all_plays:
                # Create DataFrame
                drive_df = pd.DataFrame(all_plays)
                
                # Print shape and columns for debugging
                print(f"Game DataFrame shape: {drive_df.shape}")
                
                # Define required columns
                required_columns = [
                    'Date', 'Season', 'Week', 'Away Team', 'Home Team', 'Game_Time',
                    'Quarter', 'Time', 'Down', 'ToGo', 'Location', 'Detail',
                    'Play_Type', 'Primary_Player', 'Receiver', 'Sack_By',
                    'Run_Location', 'Run_Gap', 'Pass_Type', 'Pass_Location',
                    'Pass_Yards', 'Field_Goal_Yards', 'Yards', 'Tackler', 'Tackler2', 'Defender',
                    'Result', 'Penalized_Player', 'Penalty_Yards', 'EPB', 'EPA'
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
        
    except Exception as e:
        print(f"Error processing game {game_index + 1}: {str(e)}")
        traceback.print_exc()
        return pd.DataFrame()

@task(cache_policy=NO_CACHE)
def clean_parsed_data(df):
    """Clean and standardize the parsed data"""
    if df.empty:
        return df
        
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

@task(cache_policy=NO_CACHE)
def save_combined_data(all_game_dataframes, season, week, week_path):
    """Save all combined play-by-play data to Excel file"""
    
    if not all_game_dataframes:
        print(f"No data collected for Season {season}, Week {week}")
        return
    
    # Filter out empty DataFrames
    valid_dataframes = [df for df in all_game_dataframes if not df.empty]
    
    if not valid_dataframes:
        print(f"No valid data collected for Season {season}, Week {week}")
        return
    
    try:
        print("Combining all game data...")
        combined_df = pd.concat(valid_dataframes, ignore_index=True)
        
        # Clean the data
        combined_df = clean_parsed_data(combined_df)
        
        # Print shape and columns for debugging
        print(f"Combined DataFrame shape: {combined_df.shape}")
        print("Columns found:", combined_df.columns.tolist())
        
        columns_order = [
            'Date', 'Season', 'Week', 'Away Team', 'Home Team', 'Game_Time',
            'Quarter', 'Time', 'Down', 'ToGo', 'Location', 'Detail',
            'Play_Type', 'Primary_Player', 'Receiver', 'Sack_By', 
            'Run_Location', 'Run_Gap', 'Pass_Type', 'Pass_Location', 
            'Pass_Yards', 'Field_Goal_Yards', 'Yards', 'Tackler', 'Tackler2', 'Defender','Result',
            'Penalized_Player', 'Penalty_Yards', 'EPB', 'EPA'
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

@flow
def scrape_play_by_play_flow(season, week):
    """Flow to scrape play-by-play data for a specific season and week"""
    
    # Create directories
    week_path = create_directories(season, week)
    
    # Fetch games information
    games_info = fetch_week_page(season, week)
    
    if not games_info:
        print(f"No games found for Season {season}, Week {week}")
        return
    
    # Process each game
    all_game_dataframes = []
    for game_info in games_info:
        game_df = scrape_single_game_data(game_info, season, week)
        all_game_dataframes.append(game_df)
        
        # Add delay between games if not the last game
        if game_info != games_info[-1]:
            time.sleep(3)
    
    # Save combined data
    save_combined_data(all_game_dataframes, season, week, week_path)

@flow
def main():
    """Main flow to coordinate the entire scraping process"""
    
    # Get current and previous week
   
    previous_week = get_nfl_previous_week()
    
    # Define seasons and weeks
    seasons = [2025]  # Modify as needed
    weeks = [previous_week]  # Use previous week
    
    for season in seasons:
        for week in weeks:
            print(f"\nScraping play-by-play data for Season {season}, Week {week}")
            scrape_play_by_play_flow(season, week)
            time.sleep(5)  # Add delay between weeks

if __name__ == "__main__":
    main()
