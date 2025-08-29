import requests
from typing import Optional

def get_nfl_current_week(api_key: str = "69c2e9d1866745c6a493fb2c255c51bf") -> Optional[int]:
    """
    Gets the current NFL week number
    
    Args:
        api_key: Your SportsData.io API key
        
    Returns:
        Integer representing current week number, or None if request fails
    """
    try:
        url = f"https://api.sportsdata.io/v3/nfl/scores/json/CurrentWeek?key={api_key}"
        response = requests.get(url)
        response.raise_for_status()
        
        return response.json()
    
    except requests.exceptions.RequestException as e:
        print(f"Error fetching current week: {e}")
        return None

# Example usage
if __name__ == "__main__":
    current_week = get_nfl_current_week()
    print(f"Current NFL Week: {current_week}")