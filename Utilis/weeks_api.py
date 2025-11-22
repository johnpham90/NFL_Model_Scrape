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

def get_nfl_previous_week(api_key: str = "69c2e9d1866745c6a493fb2c255c51bf") -> Optional[int]:
    """
    Gets the previous NFL week number
    
    Args:
        api_key: Your SportsData.io API key
        
    Returns:
        Integer representing previous week number, or None if request fails
    """
    current_week = get_nfl_current_week(api_key)
    
    if current_week is not None:
        previous_week = current_week - 1
        # Handle edge case where current week is 1 (would give 0)
        return max(previous_week, 1)
    
    return None

# Example usage
if __name__ == "__main__":
    current_week = get_nfl_current_week()
    previous_week = get_nfl_previous_week()
    
    print(f"Current NFL Week: {current_week}")
    print(f"Previous NFL Week: {previous_week}")