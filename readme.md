# NFL Stats Scraper Documentation

## Overview
This Python script scrapes NFL game statistics from Pro Football Reference. It collects detailed game data including:
- Game summaries
- Team statistics
- Player statistics (rushing, passing, receiving, defense, returns, kicking)

## Prerequisites
```bash
pip install requests beautifulsoup4 pandas
```

## Project Structure
```
nfl_scraper/
├── nfl_boxscore_scraper.py  # Main script
└── data/                    # Output directory
    └── [SEASON]/
        └── Week_[NUMBER]/
            ├── [SEASON]_Week[NUMBER]_Game_Summary_Stats.xlsx
            ├── [SEASON]_Week[NUMBER]_Team_Stats.xlsx
            ├── [SEASON]_Week[NUMBER]_Rushing_Stats.xlsx
            ├── [SEASON]_Week[NUMBER]_Passing_Stats.xlsx
            └── ... (other stat files)
```

## How It Works

### 1. Data Collection Process
The script follows this sequence:
1. Fetches the weekly schedule page
2. Extracts game links and basic info
3. Visits each game's box score page
4. Scrapes detailed statistics
5. Saves data to Excel files

### 2. Key Components

#### Main Functions

##### `scrape_nfl_data(season, week)`
- Entry point for scraping
- Parameters:
  - season: NFL season year (e.g., 2023)
  - week: Week number (1-18)
- Manages the overall scraping process
- Initializes data storage for all stat types

##### `scrape_game_summary()`
- Collects game metadata:
  - Date, teams, scores
  - Game conditions (roof, surface)
  - Game info (attendance, Vegas line)
- Handles data from commented HTML sections

##### `scrape_box_score()`
- Processes detailed game statistics
- Handles multiple stat types:
  ```python
  sections = {
      'Team_Stats': {...},
      'Rushing': {...},
      'Passing': {...},
      'Receiving': {...},
      'Defense': {...},
      'Returns': {...},
      'Kicking': {...}
  }
  ```

#### Data Extraction Methods

##### HTML Comment Handling
Pro Football Reference hides data in HTML comments. The script:
1. Locates comment sections
2. Extracts table HTML
3. Parses with BeautifulSoup
```python
def extract_commented_html(soup, div_id):
    div = soup.find('div', id=div_id)
    comments = div.find_all(string=lambda text: isinstance(text, Comment))
    for comment in comments:
        if 'table' in comment.lower():
            return BeautifulSoup(comment, 'html.parser')
```

##### Team Stats Processing
- Transforms row-based stats into columns
- Creates consistent team-based records
- Handles special formatting for team statistics

### 3. Output Data Structure

#### Game Summary Excel
Contains one row per game with columns:
- Date
- Season/Week
- Teams (Away/Home)
- Scores
- Game conditions
- Vegas lines

#### Team Stats Excel
Contains team-level statistics:
- One row per team
- Statistics as columns
- Includes all game metrics (first downs, yards, etc.)

#### Player Stats Excel Files
Each stat type (rushing, passing, etc.) includes:
- Player name
- Team
- Game-specific stats
- Metadata (date, season, week)

### 4. Error Handling
The script includes:
- Request error handling
- Data validation
- Missing data management
- Rate limiting (delays between requests)

### 5. Customization
Key variables that can be modified:
```python
base_url = "https://www.pro-football-reference.com"
save_path = "C:\\NFLStats\\data"  # Change to your preferred path
```

Season and week range in main():
```python
seasons = [2023]  # Add more seasons as needed
weeks = list(range(1, 18))  # Adjust week range
```

### 6. Data Cleaning
The script handles:
- Header row removal
- Consistent date formatting
- Team name standardization
- Empty value handling

### 7. Rate Limiting
Built-in delays to respect the website:
```python
time.sleep(2)  # Between box score requests
time.sleep(1)  # Between stat type processing
time.sleep(3)  # Between games
time.sleep(5)  # Between weeks
```

## Usage
```python
python nfl_boxscore_scraper.py
```

## Output Examples

### Game Summary Format
```
Date       | Season | Week | Away Team | Away Score | Home Team | Home Score | ...
Sep 7,2023 | 2023   | 1    | DET      | 21         | KC        | 20         | ...
```

### Team Stats Format
```
team    | First_Downs | Rush_Yds_TDs | Total_Yards | ...
DET     | 19         | 34-118-1     | 368         | ...
KC      | 17         | 23-90-0      | 316         | ...
```

### Player Stats Format
```
player      | team | stat1 | stat2 | Date       | Season | Week | ...
J.Goff      | DET  | 253   | 1     | Sep 7,2023 | 2023   | 1    | ...
P.Mahomes   | KC   | 226   | 2     | Sep 7,2023 | 2023   | 1    | ...
```

## Best Practices
1. Run during off-peak hours
2. Maintain rate limiting
3. Regular backups of data
4. Verify data consistency
5. Monitor for website changes

## Limitations
- Dependent on website structure
- Rate limited for politeness
- Historical data may vary in format
- Some games may have missing data

## Troubleshooting
Common issues and solutions:
1. Missing data: Check HTML structure changes
2. Rate limiting: Adjust sleep timers
3. Connection errors: Verify network/URL
4. Parse errors: Update BeautifulSoup selectors

## Future Enhancements
Potential improvements:
1. Async request handling
2. Database integration
3. API implementation
4. Additional statistics
5. Historical data support
