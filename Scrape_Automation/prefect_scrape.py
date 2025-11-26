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
from Utilis.weeks_api import get_nfl_current_week, get_nfl_previous_week
from prefect import flow, task
from prefect.cache_policies import NO_CACHE

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

@task(cache_policy=NO_CACHE)
def setup_and_get_games(season, week):
    """Setup directories and fetch all games data in one task"""
    
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
    
    def extract_game_summary_data(game_soup):
        """Extract game summary data from soup and return serializable data"""
        summary_data = {}
        
        # Extract score information
        loser_row = game_soup.find('tr', class_='loser')
        winner_row = game_soup.find('tr', class_='winner')
        draw_rows = game_soup.find_all('tr', class_='draw')
        
        if loser_row and winner_row:
            loser_score_cell = loser_row.find('td', class_='right')
            winner_score_cell = winner_row.find('td', class_='right')
            
            if loser_score_cell and winner_score_cell:
                summary_data['loser_score'] = loser_score_cell.text.strip()
                summary_data['winner_score'] = winner_score_cell.text.strip()
                summary_data['loser_team'] = loser_row.find('td').text.strip()
                summary_data['winner_team'] = winner_row.find('td').text.strip()
        
        elif draw_rows and len(draw_rows) == 2:
            summary_data['away_score'] = draw_rows[0].find('td', class_='right').text.strip()
            summary_data['home_score'] = draw_rows[1].find('td', class_='right').text.strip()
            summary_data['is_draw'] = True
        
        return summary_data
    
    # Get week info and create directories
    current_week = get_nfl_current_week()
    previous_week = get_nfl_previous_week()
    week_path = os.path.join(save_path, str(season), f"Week_{week}")
    os.makedirs(week_path, exist_ok=True)
    
    # Fetch week page
    week_url = f"{base_url}/years/{season}/week_{week}.htm"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    response = make_request_with_retry(week_url, headers)
    soup = BeautifulSoup(response.text, 'html.parser')
    
    games_info = []
    games = soup.find_all('div', class_='game_summary')
    
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
        
        # Extract game summary data directly here
        game_summary_data = extract_game_summary_data(game)
        
        games_info.append({
            'away_team': away_team,
            'home_team': home_team,
            'game_date': game_date,
            'boxscore_url': full_boxscore_url,
            'game_summary_data': game_summary_data
        })
    
    return current_week, previous_week, week_path, games_info

@task(cache_policy=NO_CACHE)
def process_all_games(games_info, season, week):
    """Process all games data in one large task"""
    
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

    def extract_player_id(player_cell):
        """Return the PFR player id like 'JackLa00' from a player <th> cell."""
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

    def create_game_summary_from_data(game_summary_data, box_score_soup, date_text, season, week, away_team, home_team):
        """Create game summary from extracted data"""
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
        
        # Process scores from the extracted data
        if game_summary_data.get('is_draw'):
            game_info['Away Score'] = int(game_summary_data['away_score']) if game_summary_data['away_score'].isdigit() else None
            game_info['Home Score'] = int(game_summary_data['home_score']) if game_summary_data['home_score'].isdigit() else None
        elif 'loser_score' in game_summary_data and 'winner_score' in game_summary_data:
            loser_score = int(game_summary_data['loser_score']) if game_summary_data['loser_score'].isdigit() else None
            winner_score = int(game_summary_data['winner_score']) if game_summary_data['winner_score'].isdigit() else None
            
            if game_summary_data['loser_team'] == away_team:
                game_info['Away Score'] = loser_score
                game_info['Home Score'] = winner_score
            else:
                game_info['Away Score'] = winner_score
                game_info['Home Score'] = loser_score
        
        # Get game time from box score soup
        scorebox_meta = box_score_soup.find('div', class_='scorebox_meta')
        if scorebox_meta:
            start_time_div = scorebox_meta.find(string=lambda text: 'Start Time' in text if text else False)
            if start_time_div:
                time_text = start_time_div.find_next(string=True)
                if time_text:
                    cleaned_time = time_text.lstrip(': ')
                    game_info['Game_Time'] = cleaned_time
        
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
        
        return pd.DataFrame([game_info])

    # Initialize combined data storage
    all_games_data = []
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    # Process each game
    for game_info in games_info:
        away_team = game_info['away_team']
        home_team = game_info['home_team']
        game_date = game_info['game_date']
        boxscore_url = game_info['boxscore_url']
        game_summary_data = game_info['game_summary_data']
        
        # Initialize data storage for this game
        game_data = {
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
            'ExpectedPoints': []
        }
        
        try:
            # Fetch box score page
            box_score_response = make_request_with_retry(boxscore_url, headers)
            box_score_soup = BeautifulSoup(box_score_response.text, 'html.parser')
            
            # Get game summary using the extracted data
            game_summary_df = create_game_summary_from_data(game_summary_data, box_score_soup, game_date, season, week, away_team, home_team)
            game_data['Game_Summary'].append(game_summary_df)
            
            # Scrape Expected Points Summary
            print(f"Scraping Expected Points for {away_team} vs {home_team}")
            div_expected = box_score_soup.find('div', id='all_expected_points')
            expected_table = find_commented_table(div_expected, 'expected_points')
            if expected_table:
                expected_data = process_expected_points_table(expected_table)
                if expected_data:
                    for row in expected_data:
                        row['Season'] = season
                        row['Week'] = week
                        row['Date'] = game_date
                        row['Away_Team'] = away_team
                        row['Home_Team'] = home_team
                    
                    expected_df = pd.DataFrame(expected_data)
                    if "team_name" in expected_df.columns:
                        expected_df.rename(columns={"team_name": "teamid"}, inplace=True)
                    game_data['ExpectedPoints'].append(expected_df)
                    print(f"Successfully scraped Expected Points data")

            # Process all other sections - consolidated logic
            sections = {
                'Team_Stats': {
                    'wrapper_id': 'div_team_stats',
                    'table_id': 'team_stats',
                    'stats_to_extract': ['stat', 'vis_stat', 'home_stat']
                },
                'Drives': {
                    'wrapper_id': 'div_vis_drives',
                    'home_wrapper_id': 'div_home_drives',
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

            # Define header texts to exclude
            header_texts = {'Kick Returns', 'Punt Returns', 'Scoring', 'Punting'}

            for stat_type, config in sections.items():
                print(f"\nProcessing {stat_type}...")
                
                if stat_type == 'Team_Stats':
                    div = box_score_soup.find('div', id='all_team_stats')
                    if div:
                        table = find_commented_table(div, 'team_stats')
                        if table:
                            data_rows = process_team_stats(table, away_team, home_team)
                            if data_rows:
                                stats_df = pd.DataFrame(data_rows)
                                stats_df['Date'] = game_date
                                stats_df['Season'] = season
                                stats_df['Week'] = week
                                stats_df['Away Team'] = away_team
                                stats_df['Home Team'] = home_team
                                game_data[stat_type].append(stats_df)
                                print(f"Successfully scraped Team Stats data")
                    continue

                elif stat_type == 'Drives':
                    drives_data = []
                    # Process away team drives
                    away_div = box_score_soup.find('div', id='all_vis_drives')
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
                                        away_df['Date'] = game_date
                                        away_df['Season'] = season
                                        away_df['Week'] = week
                                        away_df['Away Team'] = away_team
                                        away_df['Home Team'] = home_team
                                        drives_data.append(away_df)
                                        print(f"Successfully scraped {away_team} drives data")
                    
                    # Process home team drives
                    home_div = box_score_soup.find('div', id='all_home_drives')
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
                                        home_df['Date'] = game_date
                                        home_df['Season'] = season
                                        home_df['Week'] = week
                                        home_df['Away Team'] = away_team
                                        home_df['Home Team'] = home_team
                                        drives_data.append(home_df)
                                        print(f"Successfully scraped {home_team} drives data")
                    
                    if drives_data:
                        game_data[stat_type].extend(drives_data)
                    
                    continue

                # Process other stats
                div = box_score_soup.find('div', id=f"all_{config['table_id']}")
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
                            stats_df['Date'] = game_date
                            stats_df['Season'] = season
                            stats_df['Week'] = week
                            stats_df['Away Team'] = away_team
                            stats_df['Home Team'] = home_team
                            
                            game_data[stat_type].append(stats_df)
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
            
            print(f"Successfully processed {away_team} vs {home_team}")
            time.sleep(3)
            
            all_games_data.append(game_data)
            
        except Exception as e:
            print(f"Error processing box score for {away_team} vs {home_team}: {str(e)}")
            all_games_data.append(None)
            continue
    
    return all_games_data

@task(cache_policy=NO_CACHE)
def save_all_data(all_games_data, season, week, week_path):
    """Save all combined data to files - consolidated task"""
    
    # Combine data from all games
    combined_storage = {
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
        'ExpectedPoints': []
    }
    
    # Combine data from all games
    for game_data in all_games_data:
        if game_data:  # Skip None results
            for stat_type in combined_storage.keys():
                combined_storage[stat_type].extend(game_data.get(stat_type, []))
    
    # Save data
    for stat_type, data_frames in combined_storage.items():
        print(f"\nProcessing {stat_type} for saving...")
        print(f"Found {len(data_frames)} data frames")
        
        try:
            if data_frames:
                combined_df = pd.concat(data_frames, ignore_index=True)
                combined_df = combined_df.drop_duplicates(keep='first')
                
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

@flow
def main():
    """Main flow - optimized with minimal task overhead"""
    
    # Define seasons and weeks - using local logic to minimize tasks
    seasons = [2025]  # Single season or range of seasons
    current_week = get_nfl_current_week()
    previous_week = get_nfl_previous_week()
    weeks = [previous_week]  # Use previous week

    # Loop through seasons and weeks
    for season in seasons:
        for week in weeks:
            print(f"\nScraping data for Season {season}, Week {week}")
            
            # Step 1: Setup and get all games data (1 task)
            current_week, previous_week, week_path, games_info = setup_and_get_games(season, week)
            
            if not games_info:
                print(f"No games found for Season {season}, Week {week}")
                continue
            
            # Step 2: Process all games (1 massive task)
            all_games_data = process_all_games(games_info, season, week)
            
            # Step 3: Save all data (1 task)
            save_all_data(all_games_data, season, week, week_path)
            
            time.sleep(3)  # Add delay between weeks

if __name__ == "__main__":
    main()