import os
import sys
import requests
import pandas as pd
from bs4 import BeautifulSoup
from bs4.element import Comment
import time
from datetime import datetime, timedelta
from time import sleep
from collections.abc import Iterable 
import re
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from Utilis.current_week_api import get_nfl_current_week

# Base URL and save path
base_url = "https://www.pro-football-reference.com"
save_path = "C:\\NFLStats\\data"

# Team mapping for Expected Points
TEAM_MAPPING = {
    "Lions": "DET", "Packers": "GNB", "Dolphins": "MIA", "Jets": "NYJ",
    "Falcons": "ATL", "Vikings": "MIN", "Saints": "NOR", "Giants": "NYG",
    "Jaguars": "JAX", "Titans": "TEN", "Panthers": "CAR", "Eagles": "PHI",
    "Browns": "CLE", "Steelers": "PIT", "Raiders": "LVR", "Buccaneers": "TAM",
    "Cardinals": "ARI", "Seahawks": "SEA", "Bills": "BUF", "Rams": "LAR",
    "Bears": "CHI", "49ers": "SFO", "Chiefs": "KAN", "Chargers": "LAC",
    "Bengals": "CIN", "Cowboys": "DAL", "Colts": "IND", "Ravens": "BAL",
    "Texans": "HOU", "Broncos": "DEN", "Commanders": "WAS", "Patriots": "NWE"
}

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

def find_commented_table(div, table_id):
    """Find table in comments"""
    if not div:
        return None
        
    # First try direct search
    table = div.find('table', {'id': table_id})
    if table:
        return table
        
    # Then look in comments
    comments = div.find_all(string=lambda text: isinstance(text, Comment))
    for comment in comments:
        try:
            comment_soup = BeautifulSoup(comment, 'lxml')
            table = comment_soup.find('table', {'id': table_id})
            if table:
                return table
        except Exception as e:
            print(f"Error parsing comment: {e}")
            break
    
    return None

def process_expected_points_table(table):
    """Process the Expected Points Summary table with team mapping"""
    rows = []
    if not table:
        return rows

    tbody = table.find('tbody')
    if tbody:
        for row in tbody.find_all('tr'):
            row_data = {}
            for cell in row.find_all(['th', 'td']):
                col_name = cell.get('data-stat', '').strip()
                if col_name:
                    value = cell.text.strip()
                    # Map team names to team IDs
                    if col_name == "team_name":
                        value = TEAM_MAPPING.get(value, value)
                    row_data[col_name] = value
            if row_data:
                rows.append(row_data)
    return rows

def process_drive_stats(table, team, is_home):
    """Process drive stats table for a team"""
    data_rows = []
    
    if table:
        tbody = table.find('tbody')
        if tbody:
            for row in tbody.find_all('tr'):
                row_data = {
                    'team': team,
                    'is_home': is_home
                }
                
                # Get drive number
                drive_num = row.find('th', {'data-stat': 'drive_num'})
                if drive_num:
                    row_data['drive_num'] = drive_num.text.strip()
                
                # Get all other stats
                for cell in row.find_all('td'):
                    stat = cell.get('data-stat', '')
                    if stat:
                        # Handle play count tip which contains play breakdown
                        if stat == 'play_count_tip':
                            tooltip = cell.find('span', class_='tooltip')
                            if tooltip:
                                row_data['plays_breakdown'] = tooltip.text.strip()
                            row_data['total_plays'] = cell.text.strip().split()[0]
                        else:
                            row_data[stat] = cell.text.strip()
                
                if row_data:
                    data_rows.append(row_data)
    
    return data_rows

def process_team_stats(table, away_team, home_team):
    """Process team stats"""
    data_rows = []
    
    if table:
        # Initialize dictionaries for both teams
        away_stats = {'team': away_team}
        home_stats = {'team': home_team}
        
        rows = table.find_all('tr')
        
        for row in rows:
            # Skip header rows
            if row.find('th', {'scope': 'col'}):
                continue
                
            stat_header = row.find('th', {'data-stat': 'stat'})
            vis_stat = row.find('td', {'data-stat': 'vis_stat'})
            home_stat = row.find('td', {'data-stat': 'home_stat'})
            
            if stat_header and vis_stat and home_stat:
                # Create column name from stat category
                column_name = stat_header.text.strip().replace(' ', '_').replace('-', '_')
                
                # Add stats to respective team dictionaries
                away_stats[column_name] = vis_stat.text.strip()
                home_stats[column_name] = home_stat.text.strip()
        
        # Create list with both team rows
        if away_stats and home_stats:
            data_rows = [away_stats, home_stats]
    
    return data_rows

def scrape_game_summary(main_page_soup, box_score_soup, date_text, season, week, away_team, home_team):
    game_info = {
        'Date': date_text,
        'Season': season,
        'Week': week,
        'Away Team': away_team,
        'Home Team': home_team,
        'Away Score': None,
        'Home Score': None,
        'Game_Time': None,
        'Won_Toss': None,
        'Roof': None,
        'Surface': None,
        'Duration': None,
        'Attendance': None,
        'Vegas_Line': None,
        'Over_Under': None
    }
    
    # Get game time
    scorebox_meta = box_score_soup.find('div', class_='scorebox_meta')
    if scorebox_meta:
        start_time_div = scorebox_meta.find(string=lambda text: 'Start Time' in text if text else False)
        if start_time_div:
            time_text = start_time_div.find_next(string=True)
            if time_text:
                # Remove the leading colon and any spaces
                cleaned_time = time_text.lstrip(': ')
                game_info['Game_Time'] = cleaned_time
                print(f"Found game time: {cleaned_time}")
    
    # Get scores from main page
    loser_row = main_page_soup.find('tr', class_='loser')
    winner_row = main_page_soup.find('tr', class_='winner')
    draw_rows = main_page_soup.find_all('tr', class_='draw')
    print(f"\nChecking for draw game between {away_team} and {home_team}")
    print(f"Number of draw rows found: {len(draw_rows)}")
    
    if loser_row and winner_row:
        loser_score_cell = loser_row.find('td', class_='right')
        winner_score_cell = winner_row.find('td', class_='right')
        
        if loser_score_cell and winner_score_cell:
            loser_score = loser_score_cell.text.strip()
            winner_score = winner_score_cell.text.strip()
            
            loser_score = int(loser_score) if loser_score.isdigit() else None
            winner_score = int(winner_score) if winner_score.isdigit() else None
            
            if loser_row.find('td').text.strip() == away_team:
                game_info['Away Score'] = loser_score
                game_info['Home Score'] = winner_score
            else:
                game_info['Away Score'] = winner_score
                game_info['Home Score'] = loser_score

    elif draw_rows and len(draw_rows) == 2:
        # Get scores from the draw rows
        away_score = draw_rows[0].find('td', class_='right').text.strip()
        home_score = draw_rows[1].find('td', class_='right').text.strip()
        print(f"Draw game scores - Away: {away_score}, Home: {home_score}")
        
        game_info['Away Score'] = int(away_score) if away_score.isdigit() else None
        game_info['Home Score'] = int(home_score) if home_score.isdigit() else None

    # Look for game info in comments
    div = box_score_soup.find('div', id='all_game_info')
    if div:
        comments = div.find_all(string=lambda text: isinstance(text, Comment))
        
        for comment in comments:
            if 'game_info' in comment.lower():
                comment_soup = BeautifulSoup(comment, 'html.parser')
                table = comment_soup.find('table', id='game_info')
                
                if table:
                    for row in table.find_all('tr'):
                        header_cell = row.find('th', {'data-stat': 'info'})
                        value_cell = row.find('td', {'data-stat': 'stat'})
                        
                        if header_cell and value_cell:
                            header_text = header_cell.text.strip()
                            value_text = value_cell.text.strip()
                            
                            if "Won Toss" in header_text:
                                game_info['Won_Toss'] = value_text
                            elif "Roof" in header_text:
                                game_info['Roof'] = value_text
                            elif "Surface" in header_text:
                                game_info['Surface'] = value_text
                            elif "Duration" in header_text:
                                game_info['Duration'] = value_text
                            elif "Attendance" in header_text:
                                game_info['Attendance'] = value_text
                            elif "Vegas Line" in header_text:
                                game_info['Vegas_Line'] = value_text
                            elif "Over/Under" in header_text:
                                game_info['Over_Under'] = value_text.split()[0] if value_text else None
    
    print("\nFinal game info collected:")
    for key, value in game_info.items():
        print(f"{key}: {value}")
    
    return pd.DataFrame([game_info])
def extract_player_id(player_cell):
    """
    Return the PFR player id like 'JackLa00' from a player <th> cell.
    Prefers the data-append-csv attribute, falls back to parsing the <a href>.
    """
    if not player_cell:
        return None
    pid = player_cell.get('data-append-csv')
    if pid:
        return pid
    a = player_cell.find('a')
    if a and a.get('href'):
        m = re.search(r'/players/[A-Z]/([A-Za-z0-9]+)\.htm', a['href'])
        if m:
            return m.group(1)
    return None

def scrape_box_score(url, season, week, date_text, away_team, home_team, data_storage):
    time.sleep(2)
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    try:
        response = make_request_with_retry(url, headers)
        soup = BeautifulSoup(response.text, 'html.parser')
    except Exception as e:
        print(f"Failed to fetch box score for {away_team} vs {home_team}: {str(e)}")
        return

    # Scrape Expected Points Summary
    print(f"Scraping Expected Points for {away_team} vs {home_team}")
    div_expected = soup.find('div', id='all_expected_points')
    expected_table = find_commented_table(div_expected, 'expected_points')
    if expected_table:
        expected_data = process_expected_points_table(expected_table)
        if expected_data:
            for row in expected_data:
                row['Season'] = season
                row['Week'] = week
                row['Date'] = date_text
                row['Away_Team'] = away_team
                row['Home_Team'] = home_team
            
            expected_df = pd.DataFrame(expected_data)
            if "team_name" in expected_df.columns:
                expected_df.rename(columns={"team_name": "teamid"}, inplace=True)
            data_storage['ExpectedPoints'].append(expected_df)
            print(f"Successfully scraped Expected Points data")

    sections = {
        'Team_Stats': {
            'wrapper_id': 'div_team_stats',
            'table_id': 'team_stats',
            'stats_to_extract': ['stat', 'vis_stat', 'home_stat']
        },
        'Drives': {
            'wrapper_id': 'div_vis_drives',  # Away team
            'home_wrapper_id': 'div_home_drives',  # Home team
            'table_id': 'vis_drives',
            'home_table_id': 'home_drives',
            'stats_to_extract': [
                'drive_num', 'quarter', 'time_start', 'start_at',
                'play_count_tip', 'time_total', 'net_yds', 'end_event'
            ]
        },
        'Rushing': {
            'wrapper_id': 'div_rushing_advanced',
            'table_id': 'rushing_advanced',
            'stats_to_extract': ['player', 'team', 'rush_att', 'rush_yds', 'rush_td']
        },
        'Passing': {
            'wrapper_id': 'div_passing_advanced',
            'table_id': 'passing_advanced',
            'stats_to_extract': ['player', 'team', 'pass_cmp', 'pass_att', 'pass_yds', 'pass_td', 'pass_int']
        },
        'Receiving': {
            'wrapper_id': 'div_receiving_advanced',
            'table_id': 'receiving_advanced',
            'stats_to_extract': ['player', 'team', 'targets', 'rec', 'rec_yds', 'rec_td']
        },
        'Defense': {
            'wrapper_id': 'div_defense_advanced',
            'table_id': 'defense_advanced',
            'stats_to_extract': ['player', 'team', 'tackles_combined', 'tackles_solo', 'sacks']
        },
        'Returns': {
            'wrapper_id': 'div_returns',
            'table_id': 'returns',
            'stats_to_extract': ['player', 'team', 'kick_ret', 'kick_ret_yds', 'punt_ret', 'punt_ret_yds']
        },
        'Kicking': {
            'wrapper_id': 'div_kicking',
            'table_id': 'kicking',
            'stats_to_extract': ['player', 'team', 'fgm', 'fga', 'xpm', 'xpa']
        },
        'Player_Offense': { 
            'wrapper_id': 'div_player_offense',
            'table_id': 'player_offense',
            'stats_to_extract': ['player', 'team', 'pass_cmp', 'pass_att', 'pass_yds', 'pass_td', 'rush_att', 'rush_yds', 'rush_td']
        },
        'Player_Defense': {
        'wrapper_id': 'div_player_defense',
        'table_id': 'player_defense',
        'stats_to_extract': ['player', 'team', 'def_int', 'def_int_yds', 'def_int_td', 'def_int_long',
                            'pass_defended', 'sacks', 'tackles_combined', 'tackles_solo', 
                            'tackles_assists', 'qb_hits', 'fumbles_forced', 'fumbles_rec']
        }
    }

    if 'Player_Offense' not in data_storage:
        data_storage['Player_Offense'] = []

    def process_player_offense(table):
        """Process player offense table"""
        data_rows = []
        tbody = table.find('tbody')

        if tbody:
            for row in tbody.find_all('tr', class_=lambda x: x != 'thead'):
                row_data = {}

                player_cell = row.find('th', {'data-stat': 'player'})
                if player_cell:
                    player_name = player_cell.text.strip()
                    if player_name:
                        row_data['player'] = player_name
                        row_data['playerid'] = extract_player_id(player_cell)
                    else:
                        continue  # no usable player name
                else:
                    continue  # no player cell

                for cell in row.find_all('td'):
                    stat = cell.get('data-stat', '')
                    if stat:
                        row_data[stat] = cell.text.strip()

                if row_data:
                    data_rows.append(row_data)

        return data_rows

    # Add processing for Player_Offense section
    div = soup.find('div', id='all_player_offense')
    if div:
        table = find_commented_table(div, 'player_offense')
        if table:
            data_rows = process_player_offense(table)
            if data_rows:
                stats_df = pd.DataFrame(data_rows)
                stats_df['Date'] = date_text
                stats_df['Season'] = season
                stats_df['Week'] = week
                stats_df['Away Team'] = away_team
                stats_df['Home Team'] = home_team
                data_storage['Player_Offense'].append(stats_df)
                print("Successfully scraped Player Offense data")

    # Define header texts to exclude
    header_texts = {'Kick Returns', 'Punt Returns', 'Scoring', 'Punting'}

    for stat_type, config in sections.items():
        print(f"\nProcessing {stat_type}...")
        
        if stat_type == 'Team_Stats':
            div = soup.find('div', id='all_team_stats')
            if div:
                table = find_commented_table(div, 'team_stats')
                if table:
                    data_rows = process_team_stats(table, away_team, home_team)
                    if data_rows:
                        stats_df = pd.DataFrame(data_rows)
                        stats_df['Date'] = date_text
                        stats_df['Season'] = season
                        stats_df['Week'] = week
                        stats_df['Away Team'] = away_team
                        stats_df['Home Team'] = home_team
                        data_storage[stat_type].append(stats_df)
                        print(f"Successfully scraped Team Stats data")
            continue

        elif stat_type == 'Drives':
            drives_data = []
            # Process away team drives
            away_div = soup.find('div', id='all_vis_drives')
            if away_div:
                comments = away_div.find_all(string=lambda text: isinstance(text, Comment))
                for comment in comments:
                    if 'vis_drives' in comment.lower():
                        comment_soup = BeautifulSoup(comment, 'html.parser')
                        away_table = comment_soup.find('table', id='vis_drives')
                        if away_table:
                            away_data = process_drive_stats(away_table, away_team, False)
                            if away_data:
                                away_df = pd.DataFrame(away_data)
                                away_df['Date'] = date_text
                                away_df['Season'] = season
                                away_df['Week'] = week
                                away_df['Away Team'] = away_team
                                away_df['Home Team'] = home_team
                                drives_data.append(away_df)
                                print(f"Successfully scraped {away_team} drives data")
            
            # Process home team drives
            home_div = soup.find('div', id='all_home_drives')
            if home_div:
                comments = home_div.find_all(string=lambda text: isinstance(text, Comment))
                for comment in comments:
                    if 'home_drives' in comment.lower():
                        comment_soup = BeautifulSoup(comment, 'html.parser')
                        home_table = comment_soup.find('table', id='home_drives')
                        if home_table:
                            home_data = process_drive_stats(home_table, home_team, True)
                            if home_data:
                                home_df = pd.DataFrame(home_data)
                                home_df['Date'] = date_text
                                home_df['Season'] = season
                                home_df['Week'] = week
                                home_df['Away Team'] = away_team
                                home_df['Home Team'] = home_team
                                drives_data.append(home_df)
                                print(f"Successfully scraped {home_team} drives data")
            
            if drives_data:
                data_storage[stat_type].extend(drives_data)
            
            continue

        # Process other stats
        div = soup.find('div', id=f"all_{config['table_id']}")
        if not div:
            print(f"No div found for {stat_type}")
            continue
            
        table = find_commented_table(div, config['table_id'])
        
        if table:
            try:
                data_rows = []
                tbody = table.find('tbody')
                
                if tbody:
                    for row in tbody.find_all('tr', class_=lambda x: x != 'thead'):
                        # Skip header rows
                        row_text = row.get_text(strip=True)
                        if any(header in row_text for header in header_texts):
                            continue

                        # Check if row has actual data
                        has_data = False
                        for cell in row.find_all(['td', 'th']):
                            if cell.text.strip() and cell.text.strip() not in header_texts:
                                has_data = True
                                break
                        
                        if not has_data:
                            continue

                        row_data = {}
                        
                        # Get player name + id
                        player_cell = row.find('th', {'data-stat': 'player'})
                        if player_cell:
                            player_name = player_cell.text.strip()
                            if player_name and player_name not in header_texts:
                                row_data['player'] = player_name
                                row_data['playerid'] = extract_player_id(player_cell)
                            else:
                                continue
                        else:
                            continue  # no player cell; skip
                        
                        # Get all other stats
                        for cell in row.find_all(['td', 'th']):
                            stat = cell.get('data-stat', '')
                            if stat and stat != 'player':
                                value = cell.text.strip()
                                if value and value not in header_texts:
                                    row_data[stat] = value
                        
                        if row_data.get('player') and len(row_data) > 1:
                            data_rows.append(row_data)
                
                if data_rows:
                    stats_df = pd.DataFrame(data_rows)
                    
                    # Add metadata
                    stats_df['Date'] = date_text
                    stats_df['Season'] = season
                    stats_df['Week'] = week
                    stats_df['Away Team'] = away_team
                    stats_df['Home Team'] = home_team
                    
                    data_storage[stat_type].append(stats_df)
                    print(f"Successfully scraped {stat_type} data with {len(data_rows)} rows")
                else:
                    print(f"No valid data rows found for {stat_type}")
                    
            except Exception as e:
                print(f"Error processing {stat_type} table: {e}")
                import traceback
                traceback.print_exc()
        else:
            print(f"No table found for {stat_type}")

        time.sleep(1)

def scrape_nfl_data(season, week):
    week_url = f"{base_url}/years/{season}/week_{week}.htm"
    week_path = os.path.join(save_path, str(season), f"Week_{week}")
    os.makedirs(week_path, exist_ok=True)
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    try:
        response = make_request_with_retry(week_url, headers)
        main_page_soup = BeautifulSoup(response.text, 'html.parser')
    except Exception as e:
        print(f"Failed to fetch week data for Season {season}, Week {week}: {str(e)}")
        return
    
    # Initialize data storage with all stat types including Expected Points
    data_storage = {
        'Game_Summary': [],
        'Team_Stats': [],
        'Drives': [],
        'Rushing': [],
        'Passing': [],
        'Receiving': [],
        'Defense': [],
        'Returns': [],
        'Kicking': [],
        'Player_Offense': [],
        'Player_Defense': [],
        'ExpectedPoints': []  # Added Expected Points
    }
    
    games = main_page_soup.find_all('div', class_='game_summary')
    if not games:
        print(f"No games found for Season {season}, Week {week}")
        return
    
    for game in games:
        team_rows = game.find_all('tr', class_=['winner', 'loser', 'draw'])
        if len(team_rows) < 2:
            continue
            
        away_team = team_rows[0].find('td').text.strip()
        home_team = team_rows[1].find('td').text.strip()
        
        # Get the actual date from the game summary
        date_row = game.find('tr', class_='date')
        game_date = None
        if date_row:
            date_cell = date_row.find('td')
            if date_cell:
                game_date = date_cell.text.strip()
        
        if not game_date:
            print(f"Warning: Could not find date for {away_team} vs {home_team}")
            continue
            
        boxscore_link = game.find('td', class_='right gamelink').find('a', href=lambda x: x and '/boxscores/' in x)
        
        if not boxscore_link:
            print(f"No boxscore link found for {away_team} vs {home_team}")
            continue
            
        full_boxscore_url = base_url + boxscore_link['href']
        print(f"Found boxscore link for {away_team} vs {home_team}: {full_boxscore_url}")
        
        try:
            box_score_response = make_request_with_retry(full_boxscore_url, headers)
            box_score_soup = BeautifulSoup(box_score_response.text, 'html.parser')
            
            # Get game summary including scores and game info
            game_summary_df = scrape_game_summary(game, box_score_soup, game_date, season, week, away_team, home_team)
            data_storage['Game_Summary'].append(game_summary_df)
            
            # Get all other stats including Expected Points
            scrape_box_score(full_boxscore_url, season, week, game_date, away_team, home_team, data_storage)
            
            print(f"Successfully processed {away_team} vs {home_team}")
            time.sleep(3)
            
        except Exception as e:
            print(f"Error processing box score for {away_team} vs {home_team}: {str(e)}")
            continue
    
    # Save data
    for stat_type, data_frames in data_storage.items():
        print(f"\nProcessing {stat_type} for saving...")
        print(f"Found {len(data_frames)} data frames")
        
        try:
            if data_frames:
                combined_df = pd.concat(data_frames, ignore_index=True)
                
                # Handle special naming cases to match your existing files
                if stat_type == "ExpectedPoints":
                    file_name = f"{season}_Week{week}_ExpectedPoints.xlsx"
                elif stat_type == "Drives":
                    file_name = f"{season}_Week{week}_Drives_Stats.xlsx"
                else:
                    file_name = f"{season}_Week{week}_{stat_type}_Stats.xlsx"
                
                file_path = os.path.join(week_path, file_name)
                combined_df.to_excel(file_path, index=False)
                print(f"Successfully saved {stat_type} stats to {file_path}")
                print(f"Shape of saved DataFrame: {combined_df.shape}")
            else:
                print(f"No data collected for {stat_type}")
        except Exception as e:
            print(f"Error saving {stat_type} stats: {e}")
            import traceback
            traceback.print_exc()

def ensure_iterable(value):
    """
    Ensures the input value is iterable. If not, wraps it in a list.
    Args:
        value: The input value to check.
    Returns:
        An iterable (list or range).
    """
    if isinstance(value, Iterable) and not isinstance(value, str):
        return value
    return [value]  

def main():
    # Define Week 1 start date
    week1_start_date = '2024-09-05'

    # Define seasons (single or multiple years)
    seasons = 2025 # Single season or range of seasons
    seasons = ensure_iterable(seasons)

    # Get the current NFL week
    #current_week = get_current_nfl_week(week1_start_date)
    #print(f"Current NFL Week: {current_week}")

    # Define weeks (single week or range of weeks)
    #weeks = current_week  # Set to current week
    current_week = get_nfl_current_week()

    weeks = [1]
    #weeks = ensure_iterable(weeks)  # Ensure weeks is iterable

    # Loop through seasons and weeks
    for season in seasons:
        for week in weeks:
            print(f"\nScraping data for Season {season}, Week {week}")
            scrape_nfl_data(season, week)
            time.sleep(3)  # Add delay between weeks

if __name__ == "__main__":
    main()