import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
import time
from typing import Dict, List, Optional

# Team mapping data
TEAM_MAPPING = [
    {"teamname": "Arizona Cardinals", "teamid": "ARI"},
    {"teamname": "Atlanta Falcons", "teamid": "ATL"},
    {"teamname": "Baltimore Ravens", "teamid": "BAL"},
    {"teamname": "Buffalo Bills", "teamid": "BUF"},
    {"teamname": "Carolina Panthers", "teamid": "CAR"},
    {"teamname": "Chicago Bears", "teamid": "CHI"},
    {"teamname": "Cincinnati Bengals", "teamid": "CIN"},
    {"teamname": "Cleveland Browns", "teamid": "CLE"},
    {"teamname": "Dallas Cowboys", "teamid": "DAL"},
    {"teamname": "Denver Broncos", "teamid": "DEN"},
    {"teamname": "Detroit Lions", "teamid": "DET"},
    {"teamname": "Houston Texans", "teamid": "HOU"},
    {"teamname": "Jacksonville Jaguars", "teamid": "JAX"},
    {"teamname": "Los Angeles Rams", "teamid": "LAR"},
    {"teamname": "Miami Dolphins", "teamid": "MIA"},
    {"teamname": "Minnesota Vikings", "teamid": "MIN"},
    {"teamname": "New York Giants", "teamid": "NYG"},
    {"teamname": "New York Jets", "teamid": "NYJ"},
    {"teamname": "Philadelphia Eagles", "teamid": "PHI"},
    {"teamname": "Pittsburgh Steelers", "teamid": "PIT"},
    {"teamname": "Seattle Seahawks", "teamid": "SEA"},
    {"teamname": "Tennessee Titans", "teamid": "TEN"},
    {"teamname": "Washington Commanders", "teamid": "WAS"},
    {"teamname": "Indianapolis Colts", "teamid": "IND"},
    {"teamname": "Green Bay Packers", "teamid": "GNB"},
    {"teamname": "Tampa Bay Buccaneers", "teamid": "TAM"},
    {"teamname": "New England Patriots", "teamid": "NWE"},
    {"teamname": "Kansas City Chiefs", "teamid": "KAN"},
    {"teamname": "New Orleans Saints", "teamid": "NOR"},
    {"teamname": "San Francisco 49ers", "teamid": "SFO"},
    {"teamname": "Las Vegas Raiders", "teamid": "LVR"},
    {"teamname": "Oakland Raiders", "teamid": "OAK"},
    {"teamname": "Los Angeles Chargers", "teamid": "LAC"},
    {"teamname": "San Diego Chargers", "teamid": "SDG"},
    {"teamname": "St. Louis Rams", "teamid": "STL"}
]

class NFLScheduleScraper:
    def __init__(self):
        self.team_lookup = self._build_team_lookup()
        self.season = 2025
        self.url = f"https://www.pro-football-reference.com/years/{self.season}/games.htm"
        
        # Headers to avoid being blocked
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
    
    def _build_team_lookup(self) -> Dict[str, str]:
        """Build lookup dictionary from team name to team ID"""
        lookup = {}
        for team in TEAM_MAPPING:
            lookup[team["teamname"]] = team["teamid"]
        return lookup
    
    def _get_team_id(self, team_name: str) -> str:
        """Convert team name to team ID, with error handling"""
        if team_name in self.team_lookup:
            return self.team_lookup[team_name]
        
        # If exact match fails, print all available teams and the problematic one
        print(f"❌ ERROR: Team '{team_name}' not found in mapping!")
        print("Available teams in mapping:")
        for name in sorted(self.team_lookup.keys()):
            print(f"  - {name}")
        print(f"\nProblematic team from website: '{team_name}'")
        raise ValueError(f"Team name '{team_name}' not found in team mapping. Please add it to TEAM_MAPPING.")
    
    def _is_valid_game(self, week: str) -> bool:
        """Check if this is a preseason game (Pre0-Pre3)"""
        if week.startswith('Pre'):
            return True
        if week.startswith('Wild') or week.startswith('Division') or week.startswith('Conf') or week.startswith('Super'):
            return False
        
        # Also include regular season when available (Week 1-18)
        try:
            week_num = int(week)
            return 1 <= week_num <= 18
        except ValueError:
            return False
    
    def scrape_schedule(self) -> List[Dict]:
        """Scrape the NFL schedule from Pro Football Reference"""
        print(f"🏈 Scraping {self.season} NFL preseason schedule from Pro Football Reference...")
        
        try:
            response = requests.get(self.url, headers=self.headers)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Find the games table
            games_table = soup.find('table', {'id': 'games'})
            if not games_table:
                raise ValueError("Could not find games table on the page")
            
            print(f"🔍 Found games table: {games_table is not None}")
            
            games = []
            
            # Find all game rows
            tbody = games_table.find('tbody')
            if not tbody:
                print("❌ Could not find tbody in games table")
                return games
                
            rows = tbody.find_all('tr')
            print(f"🔍 Found {len(rows)} total rows in tbody")
            
            for i, row in enumerate(rows):
                print(f"🔍 Processing row {i}")
                
                # Skip header rows and empty rows
                if row.get('class') and 'thead' in row.get('class'):
                    print(f"  ↳ Skipping header row")
                    continue
                
                cells = row.find_all(['td', 'th'])
                print(f"  ↳ Found {len(cells)} cells")
                
                if len(cells) < 6:  # Need at least 6 columns
                    print(f"  ↳ Skipping row - not enough cells ({len(cells)} < 6)")
                    continue
                
                try:
                    # Extract data using data-stat attributes
                    week_cell = row.find('td', {'data-stat': 'week_num'})
                    day_cell = row.find('td', {'data-stat': 'game_day_of_week'})
                    date_cell = row.find('td', {'data-stat': 'boxscore_word'})
                    visitor_cell = row.find('td', {'data-stat': 'visitor_team'})
                    home_cell = row.find('td', {'data-stat': 'home_team'})
                    time_cell = row.find('td', {'data-stat': 'gametime'})
                    
                    print(f"  ↳ Cells found - Week: {week_cell is not None}, Day: {day_cell is not None}, Date: {date_cell is not None}")
                    print(f"  ↳ Cells found - Visitor: {visitor_cell is not None}, Home: {home_cell is not None}, Time: {time_cell is not None}")
                    
                    # Skip if any required cells are missing
                    if not all([week_cell, day_cell, date_cell, visitor_cell, home_cell, time_cell]):
                        print(f"  ↳ Skipping row - missing required cells")
                        continue
                    
                    week = week_cell.get_text(strip=True)
                    print(f"  ↳ Week found: '{week}'")
                    
                    # Filter to preseason and regular season (when available)
                    if not self._is_valid_game(week):
                        print(f"  ↳ Skipping week '{week}' - not a valid game")
                        continue
                    
                    print(f"  ↳ Processing valid game for week '{week}'")
                    
                    day = day_cell.get_text(strip=True)
                    date = date_cell.get_text(strip=True)
                    
                    # Get team names from the links
                    visitor_link = visitor_cell.find('a')
                    home_link = home_cell.find('a')
                    
                    if visitor_link:
                        visitor_name = visitor_link.get_text(strip=True)
                    else:
                        visitor_name = visitor_cell.get_text(strip=True)
                        
                    if home_link:
                        home_name = home_link.get_text(strip=True)
                    else:
                        home_name = home_cell.get_text(strip=True)
                    
                    # Debug output to see what team names we're getting
                    print(f"🔍 Debug - Visitor: '{visitor_name}', Home: '{home_name}'")
                    
                    time_str = time_cell.get_text(strip=True)
                    
                    # Convert team names to IDs
                    away_team_id = self._get_team_id(visitor_name)
                    home_team_id = self._get_team_id(home_name)
                    
                    # Add year to date
                    date_with_year = f"{date}, {self.season}"
                    
                    game_data = {
                        'week': week,
                        'day': day,
                        'date': date_with_year,
                        'awayteam': away_team_id,
                        'hometeam': home_team_id,
                        'time': time_str
                    }
                    
                    games.append(game_data)
                    print(f"✅ Week {week}: {away_team_id} @ {home_team_id} - {date_with_year} {time_str}")
                    
                except Exception as e:
                    print(f"⚠️  Error processing row: {e}")
                    continue
            
            print(f"\n🎯 Successfully scraped {len(games)} preseason games")
            return games
            
        except requests.RequestException as e:
            print(f"❌ Error fetching data: {e}")
            raise
        except Exception as e:
            print(f"❌ Error parsing data: {e}")
            raise
    
    def save_to_csv(self, games: List[Dict], filename: Optional[str] = None):
        """Save games data to CSV file"""
        if not games:
            print("❌ No games to save")
            return

        if filename is None:
            filename = f"nfl_preseason_schedule_{self.season}.csv"
        
        df = pd.DataFrame(games)
        df.to_csv(filename, index=False)
        print(f"💾 Saved {len(games)} games to {filename}")
        
        # Display first few rows
        print("\n📊 Preview of scraped data:")
        print(df.head(10).to_string(index=False))

def main():
    """Main execution function"""
    scraper = NFLScheduleScraper()
    
    try:
        # Add a small delay to be respectful
        time.sleep(1)
        
        # Scrape the schedule
        games = scraper.scrape_schedule()
        
        # Save to CSV
        scraper.save_to_csv(games)
        
    except Exception as e:
        print(f"❌ Scraping failed: {e}")

if __name__ == "__main__":
    main()