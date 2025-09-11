import os
import requests
import pandas as pd
from bs4 import BeautifulSoup
from bs4.element import Comment
import time
from datetime import datetime, timedelta
from time import sleep
from collections.abc import Iterable 
import re
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from Utilis.current_week_api import get_nfl_current_week

# Base URL and save path
base_url = "https://www.pro-football-reference.com"
save_path = "C:\\NFLStats\\data"

# Team mapping - comprehensive mapping with multiple variations
TEAM_MAPPING = {
    'KC': 'KAN', 'KANSAS CITY': 'KAN', 'CHIEFS': 'KAN', 'KCCHIEFS': 'KAN', 'KANSAS CITY CHIEFS': 'KAN',
    'LAC': 'LAC', 'LOS ANGELES CHARGERS': 'LAC', 'CHARGERS': 'LAC', 'LACCHARGERS': 'LAC',
    'PHI': 'PHI', 'PHILADELPHIA': 'PHI', 'EAGLES': 'PHI', 'PHIEAGLES': 'PHI', 'PHILADELPHIA EAGLES': 'PHI',
    'DAL': 'DAL', 'DALLAS': 'DAL', 'COWBOYS': 'DAL', 'DALCOWBOYS': 'DAL', 'DALLAS COWBOYS': 'DAL',
    'CLE': 'CLE', 'CLEVELAND': 'CLE', 'BROWNS': 'CLE', 'CLEBROWNS': 'CLE', 'CLEVELAND BROWNS': 'CLE',
    'CIN': 'CIN', 'CINCINNATI': 'CIN', 'BENGALS': 'CIN', 'CINBENGALS': 'CIN', 'CINCINNATI BENGALS': 'CIN',
    'NE': 'NWE', 'NEW ENGLAND': 'NWE', 'PATRIOTS': 'NWE', 'NEPATRIOTS': 'NWE', 'NEW ENGLAND PATRIOTS': 'NWE',
    'LV': 'LVR', 'LAS VEGAS': 'LVR', 'RAIDERS': 'LVR', 'LVRAIDERS': 'LVR', 'LAS VEGAS RAIDERS': 'LVR',
    'WAS': 'WAS', 'WASHINGTON': 'WAS', 'COMMANDERS': 'WAS', 'WASCOMMANDERS': 'WAS', 'WASHINGTON COMMANDERS': 'WAS',
    'NYG': 'NYG', 'NEW YORK GIANTS': 'NYG', 'GIANTS': 'NYG', 'NYGGIANTS': 'NYG',
    'NYJ': 'NYJ', 'NEW YORK JETS': 'NYJ', 'JETS': 'NYJ', 'NYJJETS': 'NYJ',
    'PIT': 'PIT', 'PITTSBURGH': 'PIT', 'STEELERS': 'PIT', 'PITSTEELERS': 'PIT', 'PITTSBURGH STEELERS': 'PIT',
    'IND': 'IND', 'INDIANAPOLIS': 'IND', 'COLTS': 'IND', 'INDCOLTS': 'IND', 'INDIANAPOLIS COLTS': 'IND',
    'MIA': 'MIA', 'MIAMI': 'MIA', 'DOLPHINS': 'MIA', 'MIADOLPHINS': 'MIA', 'MIAMI DOLPHINS': 'MIA',
    'JAX': 'JAX', 'JACKSONVILLE': 'JAX', 'JAGUARS': 'JAX', 'JAXJAGUARS': 'JAX', 'JACKSONVILLE JAGUARS': 'JAX',
    'CAR': 'CAR', 'CAROLINA': 'CAR', 'PANTHERS': 'CAR', 'CARPANTHERS': 'CAR', 'CAROLINA PANTHERS': 'CAR',
    'NO': 'NOR', 'NEW ORLEANS': 'NOR', 'SAINTS': 'NOR', 'NOSAINTS': 'NOR', 'NEW ORLEANS SAINTS': 'NOR',
    'ARI': 'ARI', 'ARIZONA': 'ARI', 'CARDINALS': 'ARI', 'ARICARDINALS': 'ARI', 'ARIZONA CARDINALS': 'ARI',
    'ATL': 'ATL', 'ATLANTA': 'ATL', 'FALCONS': 'ATL', 'ATLFALCONS': 'ATL', 'ATLANTA FALCONS': 'ATL',
    'TB': 'TAM', 'TAMPA BAY': 'TAM', 'BUCCANEERS': 'TAM', 'TBBUCCANEERS': 'TAM', 'TAMPA BAY BUCCANEERS': 'TAM',
    'DEN': 'DEN', 'DENVER': 'DEN', 'BRONCOS': 'DEN', 'DENBRONCOS': 'DEN', 'DENVER BRONCOS': 'DEN',
    'TEN': 'TEN', 'TENNESSEE': 'TEN', 'TITANS': 'TEN', 'TENTITANS': 'TEN', 'TENNESSEE TITANS': 'TEN',
    'SEA': 'SEA', 'SEATTLE': 'SEA', 'SEAHAWKS': 'SEA', 'SEASEAHAWKS': 'SEA', 'SEATTLE SEAHAWKS': 'SEA',
    'SF': 'SFO', 'SAN FRANCISCO': 'SFO', '49ERS': 'SFO', 'SF49ERS': 'SFO', 'SAN FRANCISCO 49ERS': 'SFO',
    'LAR': 'LAR', 'LOS ANGELES RAMS': 'LAR', 'RAMS': 'LAR', 'LARRAMS': 'LAR',
    'HOU': 'HOU', 'HOUSTON': 'HOU', 'TEXANS': 'HOU', 'HOUTEXANS': 'HOU', 'HOUSTON TEXANS': 'HOU',
    'BUF': 'BUF', 'BUFFALO': 'BUF', 'BILLS': 'BUF', 'BUFBILLS': 'BUF', 'BUFFALO BILLS': 'BUF',
    'BAL': 'BAL', 'BALTIMORE': 'BAL', 'RAVENS': 'BAL', 'BALRAVENS': 'BAL', 'BALTIMORE RAVENS': 'BAL',
    'CHI': 'CHI', 'CHICAGO': 'CHI', 'BEARS': 'CHI', 'CHIBEARS': 'CHI', 'CHICAGO BEARS': 'CHI',
    'MIN': 'MIN', 'MINNESOTA': 'MIN', 'VIKINGS': 'MIN', 'MINVIKINGS': 'MIN', 'MINNESOTA VIKINGS': 'MIN',
    'GB': 'GNB', 'GREEN BAY': 'GNB', 'PACKERS': 'GNB', 'GBPACKERS': 'GNB', 'GREEN BAY PACKERS': 'GNB',
    'DET': 'DET', 'DETROIT': 'DET', 'LIONS': 'DET', 'DETLIONS': 'DET', 'DETROIT LIONS': 'DET',

    # Historical teams with unique IDs
    'OAKLAND': 'OAK', 'OAKLAND RAIDERS': 'OAK', 'OAKRAIDERS': 'OAK',
    'ST. LOUIS': 'STL', 'SAINT LOUIS': 'STL', 'ST LOUIS': 'STL', 'ST. LOUIS RAMS': 'STL', 'SAINT LOUIS RAMS': 'STL', 'ST LOUIS RAMS': 'STL', 'STLRAMS': 'STL',
    'SAN DIEGO': 'SDG', 'SAN DIEGO CHARGERS': 'SDG', 'SDGCHARGERS': 'SDG',
    # Historical Washington team names
    'FOOTBALL TEAM': 'WAS', 'WASHINGTON FOOTBALL TEAM': 'WAS', 'WASFOOTBALL TEAM': 'WAS', 'WFT': 'WAS',
    'REDSKINS': 'WAS', 'WASHINGTON REDSKINS': 'WAS', 'WASREDSKINS': 'WAS'
}

def extract_player_id(href):
    """Extract player ID from href URL"""
    if not href:
        return None
    
    # Pattern to match player URLs like /players/M/MahoPa00.htm
    match = re.search(r'/players/[A-Z]/([^/]+)\.htm', href)
    if match:
        return match.group(1)  # Returns the player ID part (e.g., "MahoPa00")
    return None

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
            continue
    
    return None

def process_starters_table(table, teamid, hometeamid, awayteamid, season, week):
    """Process starters table for a team"""
    data_rows = []
    
    if not table:
        return data_rows

    tbody = table.find('tbody')
    if tbody:
        for row in tbody.find_all('tr'):
            # Skip header rows or empty rows
            if row.find('th', {'scope': 'col'}) or not row.find('th', {'data-stat': 'player'}):
                continue
                
            row_data = {}
            
            # Get player name and ID from th element
            player_cell = row.find('th', {'data-stat': 'player'})
            if player_cell:
                player_name = player_cell.text.strip()
                if player_name:
                    row_data['player'] = player_name
                    
                    # Extract player ID from the link
                    player_link = player_cell.find('a')
                    if player_link and player_link.get('href'):
                        player_id = extract_player_id(player_link.get('href'))
                        row_data['playerid'] = player_id
                    else:
                        row_data['playerid'] = None
                else:
                    continue
            else:
                continue
            
            # Get position from td element
            pos_cell = row.find('td', {'data-stat': 'pos'})
            if pos_cell:
                row_data['pos'] = pos_cell.text.strip()
            
            # Add team and game context
            row_data['teamid'] = teamid
            row_data['hometeamid'] = hometeamid
            row_data['awayteamid'] = awayteamid
            row_data['season'] = season
            row_data['week'] = week
            
            if row_data and len(row_data) >= 3:  # Ensure we have meaningful data
                data_rows.append(row_data)
    
    return data_rows

def process_snap_counts_table(table, teamid, hometeamid, awayteamid, season, week):
    """Process snap counts table for a team"""
    data_rows = []
    
    if not table:
        return data_rows

    # Column mapping from HTML data-stat to our column names
    column_mapping = {
        'player': 'player',
        'pos': 'pos',
        'offense': 'off_num',
        'off_pct': 'off_pct',
        'defense': 'def_num',
        'def_pct': 'def_pct',
        'special_teams': 'st_num',
        'st_pct': 'st_pct'
    }

    tbody = table.find('tbody')
    if tbody:
        for row in tbody.find_all('tr'):
            # Skip header rows or empty rows
            if row.find('th', {'scope': 'col'}) or not row.find('th', {'data-stat': 'player'}):
                continue
                
            row_data = {}
            
            # Get player name and ID from th element
            player_cell = row.find('th', {'data-stat': 'player'})
            if player_cell:
                player_name = player_cell.text.strip()
                if player_name:
                    row_data['player'] = player_name
                    
                    # Extract player ID from the link
                    player_link = player_cell.find('a')
                    if player_link and player_link.get('href'):
                        player_id = extract_player_id(player_link.get('href'))
                        row_data['playerid'] = player_id
                    else:
                        row_data['playerid'] = None
                else:
                    continue
            else:
                continue
            
            # Get all other stats from td elements
            for cell in row.find_all('td'):
                stat = cell.get('data-stat', '')
                if stat in column_mapping:
                    value = cell.text.strip()
                    mapped_column = column_mapping[stat]
                    row_data[mapped_column] = value
            
            # Add team and game context
            row_data['teamid'] = teamid
            row_data['hometeamid'] = hometeamid
            row_data['awayteamid'] = awayteamid
            row_data['season'] = season
            row_data['week'] = week
            
            if row_data and len(row_data) >= 5:  # Ensure we have meaningful data
                data_rows.append(row_data)
    
    return data_rows

def scrape_starters_and_snap_counts(url, season, week, date_text, away_team, home_team):
    """Scrape both starters and snap counts data from a single game"""
    time.sleep(2)
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    try:
        response = make_request_with_retry(url, headers)
        soup = BeautifulSoup(response.text, 'html.parser')
    except Exception as e:
        print(f"Failed to fetch box score for {away_team} vs {home_team}: {str(e)}")
        return [], []

    # Convert team names to 3-char codes (try uppercase first for better matching)
    awayteamid = TEAM_MAPPING.get(away_team.upper()) or TEAM_MAPPING.get(away_team)
    hometeamid = TEAM_MAPPING.get(home_team.upper()) or TEAM_MAPPING.get(home_team)
    
    if awayteamid is None:
        print(f"Error: No mapping found for away team '{away_team}' - skipping game")
        return [], []
    if hometeamid is None:
        print(f"Error: No mapping found for home team '{home_team}' - skipping game")
        return [], []
    
    starters_data = []
    snap_counts_data = []
    
    print(f"Scraping Starters and Snap Counts for {away_team} ({awayteamid}) vs {home_team} ({hometeamid})")
    
    # ========== SCRAPE STARTERS ==========
    
    # Away Team Starters
    print(f"Processing {away_team} starters...")
    away_starters_div = soup.find('div', id='all_vis_starters')
    if away_starters_div:
        away_starters_table = find_commented_table(away_starters_div, 'vis_starters')
        if away_starters_table:
            away_starters = process_starters_table(
                away_starters_table, awayteamid, hometeamid, awayteamid, season, week
            )
            if away_starters:
                starters_data.extend(away_starters)
                print(f"Successfully scraped {len(away_starters)} away team starters")
        else:
            print(f"No away team starters table found")
    else:
        print(f"No away team starters div found")
    
    # Home Team Starters
    print(f"Processing {home_team} starters...")
    home_starters_div = soup.find('div', id='all_home_starters')
    if home_starters_div:
        home_starters_table = find_commented_table(home_starters_div, 'home_starters')
        if home_starters_table:
            home_starters = process_starters_table(
                home_starters_table, hometeamid, hometeamid, awayteamid, season, week
            )
            if home_starters:
                starters_data.extend(home_starters)
                print(f"Successfully scraped {len(home_starters)} home team starters")
        else:
            print(f"No home team starters table found")
    else:
        print(f"No home team starters div found")
    
    # ========== SCRAPE SNAP COUNTS ==========
    
    # Away Team Snap Counts
    print(f"Processing {away_team} snap counts...")
    away_snap_div = soup.find('div', id='all_vis_snap_counts')
    if away_snap_div:
        away_snap_table = find_commented_table(away_snap_div, 'vis_snap_counts')
        if away_snap_table:
            away_snaps = process_snap_counts_table(
                away_snap_table, awayteamid, hometeamid, awayteamid, season, week
            )
            if away_snaps:
                snap_counts_data.extend(away_snaps)
                print(f"Successfully scraped {len(away_snaps)} away team snap count records")
        else:
            print(f"No away team snap counts table found")
    else:
        print(f"No away team snap counts div found")
    
    # Home Team Snap Counts
    print(f"Processing {home_team} snap counts...")
    home_snap_div = soup.find('div', id='all_home_snap_counts')
    if home_snap_div:
        home_snap_table = find_commented_table(home_snap_div, 'home_snap_counts')
        if home_snap_table:
            home_snaps = process_snap_counts_table(
                home_snap_table, hometeamid, hometeamid, awayteamid, season, week
            )
            if home_snaps:
                snap_counts_data.extend(home_snaps)
                print(f"Successfully scraped {len(home_snaps)} home team snap count records")
        else:
            print(f"No home team snap counts table found")
    else:
        print(f"No home team snap counts div found")
    
    return starters_data, snap_counts_data

def scrape_nfl_starters_and_snap_counts(season, week):
    """Main function to scrape starters and snap counts for all games in a given week"""
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
    
    # Initialize data storage
    all_starters_data = []
    all_snap_counts_data = []
    
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
        print(f"\n{'='*60}")
        print(f"Processing: {away_team} vs {home_team}")
        print(f"URL: {full_boxscore_url}")
        print(f"{'='*60}")
        
        try:
            # Scrape both starters and snap counts for this game
            game_starters, game_snaps = scrape_starters_and_snap_counts(
                full_boxscore_url, season, week, game_date, away_team, home_team
            )
            
            if game_starters:
                all_starters_data.extend(game_starters)
                print(f"✓ Collected {len(game_starters)} starter records")
            else:
                print(f"✗ No starters data found")
                
            if game_snaps:
                all_snap_counts_data.extend(game_snaps)
                print(f"✓ Collected {len(game_snaps)} snap count records")
            else:
                print(f"✗ No snap counts data found")
            
            time.sleep(3)  # Be respectful to the server
            
        except Exception as e:
            print(f"Error processing {away_team} vs {home_team}: {str(e)}")
            continue
    
    # ========== SAVE STARTERS DATA ==========
    if all_starters_data:
        try:
            starters_df = pd.DataFrame(all_starters_data)
            # Reorder columns for better readability (now includes playerid)
            column_order = ['player', 'playerid', 'pos', 'teamid', 'hometeamid', 'awayteamid', 'season', 'week']
            starters_df = starters_df[column_order]
            
            starters_file_name = f"{season}_Week{week}_Starters.xlsx"
            starters_file_path = os.path.join(week_path, starters_file_name)
            starters_df.to_excel(starters_file_path, index=False)
            print(f"\n✓ Successfully saved starters data to {starters_file_path}")
            print(f"  Shape: {starters_df.shape}")
            print(f"  Columns: {list(starters_df.columns)}")
        except Exception as e:
            print(f"✗ Error saving starters data: {e}")
            import traceback
            traceback.print_exc()
    else:
        print(f"\n✗ No starters data collected for Season {season}, Week {week}")
    
    # ========== SAVE SNAP COUNTS DATA ==========
    if all_snap_counts_data:
        try:
            snap_counts_df = pd.DataFrame(all_snap_counts_data)
            # Reorder columns for better readability (now includes playerid)
            column_order = ['player', 'playerid', 'pos', 'off_num', 'off_pct', 'def_num', 'def_pct', 
                          'st_num', 'st_pct', 'teamid', 'hometeamid', 'awayteamid', 'season', 'week']
            # Only include columns that exist in the dataframe
            existing_columns = [col for col in column_order if col in snap_counts_df.columns]
            snap_counts_df = snap_counts_df[existing_columns]
            
            snap_counts_file_name = f"{season}_Week{week}_Snap_Counts.xlsx"
            snap_counts_file_path = os.path.join(week_path, snap_counts_file_name)
            snap_counts_df.to_excel(snap_counts_file_path, index=False)
            print(f"\n✓ Successfully saved snap counts data to {snap_counts_file_path}")
            print(f"  Shape: {snap_counts_df.shape}")
            print(f"  Columns: {list(snap_counts_df.columns)}")
        except Exception as e:
            print(f"✗ Error saving snap counts data: {e}")
            import traceback
            traceback.print_exc()
    else:
        print(f"\n✗ No snap counts data collected for Season {season}, Week {week}")

def ensure_iterable(value):
    """
    Ensures the input value is iterable. If not, wraps it in a list.
    """
    if isinstance(value, Iterable) and not isinstance(value, str):
        return value
    return [value]

def main():
    """Main execution function"""
    # Define seasons (single or multiple years)
    seasons = 2025  # Single season or range of seasons
    seasons = ensure_iterable(seasons)

    # Define weeks (single week or range of weeks)
    current_week = get_nfl_current_week()
    weeks = [1]  # Adjust as needed
    weeks = ensure_iterable(weeks)

    # Loop through seasons and weeks
    for season in seasons:
        for week in weeks:
            print(f"\n{'='*80}")
            print(f"SCRAPING STARTERS AND SNAP COUNTS - Season {season}, Week {week}")
            print(f"{'='*80}")
            scrape_nfl_starters_and_snap_counts(season, week)
            time.sleep(3)  # Add delay between weeks

if __name__ == "__main__":
    main()