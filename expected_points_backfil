import os
import requests
import pandas as pd
from bs4 import BeautifulSoup, Comment
import time

# Base URL and save path
base_url = "https://www.pro-football-reference.com"
save_path = "C:\\NFLStats\\data"
TEAM_MAPPING = {
    "Lions": "DET",
    "Packers": "GNB",
    "Dolphins":	"MIA",
    "Jets":	"NYJ",
    "Falcons": "ATL",
    "Vikings": "MIN",
    "Saints": "NOR",
    "Giants": "NYG",
    "Jaguars": "JAX",
    "Titans": "TEN",
    "Panthers": "CAR",
    "Eagles": "PHI",
    "Browns": "CLE",
    "Steelers": "PIT",
    "Raiders": "LVR",
    "Buccaneers": "TAM",
    "Cardinals": "ARI",
    "Seahawks": "SEA",
    "Bills": "BUF",
    "Rams":	"LAR",
    "Bears":"CHI",
    "49ers": "SFO",
    "Chiefs": "KAN",
    "Chargers": "LAC",
    "Bengals": "CIN",
    "Cowboys": "DAL",
    "Colts": "IND",
    "Ravens": "BAL",
    "Texans": "HOU",
    "Broncos": "DEN",
    "Commanders": "WAS",
    "Patriots": "NWE"

}

# Proxy configuration
PROXY_CONFIG = {
    'http': 'http://92.113.81.29:42004',
    'https': 'http://92.113.81.29:42004'
}

# Optional: Add proxy authentication if needed
PROXY_AUTH = {
    'username': 'tYxdcHIlsPM8L7p',
    'password': '8QRXXZjylbDY1Xl'
}

def create_proxy_session():
    """Create a requests session with proxy configuration"""
    session = requests.Session()
    
    # Create proxy URL with authentication embedded
    proxy_with_auth = f'http://{PROXY_AUTH["username"]}:{PROXY_AUTH["password"]}@92.113.81.29:42004'
    session.proxies = {
        'http': proxy_with_auth,
        'https': proxy_with_auth
    }
    
    # Add additional headers that some proxies require
    session.headers.update({
        'Connection': 'keep-alive',
        'Proxy-Connection': 'keep-alive'
    })
    
    return session

def make_request_with_retry(url, headers, max_retries=3, timeout=30):
    """Make HTTP request with retry logic using proxy"""
    session = create_proxy_session()
    
    # Add the user headers to the session
    session.headers.update(headers)
    
    for attempt in range(max_retries):
        try:
            response = session.get(url, timeout=timeout)
            response.raise_for_status()
            return response
        except requests.exceptions.ProxyError as e:
            print(f"Proxy error on attempt {attempt + 1}: {e}")
            if attempt < max_retries - 1:
                time.sleep(5 * (attempt + 1))
            continue
        except requests.exceptions.RequestException as e:
            print(f"Request error on attempt {attempt + 1}: {e}")
            if attempt < max_retries - 1:
                time.sleep(5 * (attempt + 1))
            continue
    raise Exception(f"Failed to fetch {url} after {max_retries} retries")

def find_commented_table(div, table_id):
    """Find a table inside HTML comments"""
    if not div:
        print("Div not found or empty")
        return None

    comments = div.find_all(string=lambda text: isinstance(text, Comment))
    for comment in comments:
        try:
            # Use 'lxml' parser to avoid deep recursion issues
            comment_soup = BeautifulSoup(comment, 'lxml')
            table = comment_soup.find('table', {'id': table_id})
            if table:
                return table
        except Exception as e:
            print(f"Error parsing comment: {e}")
            break  # Safeguard to stop infinite recursion
    return None

def process_expected_points_table(table):
    """Process the Expected Points Summary table with team mapping"""
    rows = []
    if not table:
        return rows

    tbody = table.find('tbody')
    for row in tbody.find_all('tr'):
        row_data = {}
        for cell in row.find_all(['th', 'td']):
            col_name = cell.get('data-stat', '').strip()
            if col_name:
                value = cell.text.strip()
                # Map team names to team IDs
                if col_name == "team_name":  # Replace with the actual column name for team
                    value = TEAM_MAPPING.get(value, value)  # Default to original value if no match
                row_data[col_name] = value
        if row_data:
            rows.append(row_data)
    return rows

def process_game_summary(game):
    """Extract game summary data including teams, date, and box score URL"""
    teams = game.find_all('tr', class_=['winner', 'loser', 'draw'])
    if len(teams) < 2:
        return None, None, None, None, None

    away_team = teams[0].find('td').text.strip()
    home_team = teams[1].find('td').text.strip()

    date_row = game.find('tr', class_='date')
    game_date = None
    if date_row:
        date_cell = date_row.find('td')
        if date_cell:
            game_date = date_cell.text.strip()

    boxscore_link = game.find('a', href=lambda x: x and '/boxscores/' in x)
    if not boxscore_link:
        return None, None, None, None, None

    boxscore_url = base_url + boxscore_link['href']

    # Get the Game Time
    game_time_row = game.find('tr', class_='time')
    game_time = None
    if game_time_row:
        game_time_cell = game_time_row.find('td')
        if game_time_cell:
            game_time = game_time_cell.text.strip()

    return away_team, home_team, game_date, boxscore_url, game_time

def scrape_expected_points_summary(season, week):
    """
    Scrape Expected Points Summary for a given season and week.
    Adds a delay between scraping each game to avoid rate limiting.

    Args:
    - season (int): The season year.
    - week (int): The week number.
    """
    week_url = f"{base_url}/years/{season}/week_{week}.htm"
    save_dir = os.path.join(save_path, str(season), f"Week_{week}")
    os.makedirs(save_dir, exist_ok=True)

    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        response = make_request_with_retry(week_url, headers)
        soup = BeautifulSoup(response.text, 'html.parser')
    except Exception as e:
        print(f"Failed to fetch week {week}: {e}")
        return

    games = soup.find_all('div', class_='game_summary')
    all_data = []

    for game in games:
        away_team, home_team, game_date, boxscore_url, game_time = process_game_summary(game)
        if not (away_team and home_team and boxscore_url):
            print("Missing game details, skipping...")
            continue

        print(f"Fetching Expected Points for {away_team} vs {home_team} ({game_date}, {game_time}): {boxscore_url}")

        try:
            response = make_request_with_retry(boxscore_url, headers)
            box_soup = BeautifulSoup(response.text, 'html.parser')
            div = box_soup.find('div', id='all_expected_points')
            table = find_commented_table(div, 'expected_points')
            if table:
                game_data = process_expected_points_table(table)
                for row in game_data:
                    row['Season'] = season
                    row['Week'] = week
                    row['Date'] = game_date
                    row['Game_Time'] = game_time
                    row['Away_Team'] = away_team
                    row['Home_Team'] = home_team
                all_data.extend(game_data)
        except Exception as e:
            print(f"Error processing {boxscore_url}: {e}")

        # Add a delay between games to avoid rate limiting
        time.sleep(2)

    if all_data:
        df = pd.DataFrame(all_data)
        if "team_name" in df.columns:  # Replace with the actual column name
            df.rename(columns={"team_name": "teamid"}, inplace=True)
        file_path = os.path.join(save_dir, f"{season}_Week{week}_ExpectedPoints.xlsx")
        df.to_excel(file_path, index=False)
        print(f"Saved Expected Points Summary to {file_path}")


def main():
    seasons = [2024]  # List of seasons to scrape
    for season in seasons:
        weeks = list(range(19, 20))  # Range of weeks to scrape
        for week in weeks:
            print(f"\nScraping Season {season}, Week {week} Expected Points Summary")
            scrape_expected_points_summary(season, week)
            time.sleep(1)

if __name__ == "__main__":
    main()