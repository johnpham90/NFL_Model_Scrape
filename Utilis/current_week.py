from datetime import datetime, timedelta

def get_current_nfl_week(week1_start_date: str) -> int:
    """
    Detects the current NFL week based on the start date of Week 1.
    
    Args:
        week1_start_date (str): The start date of Week 1 in 'YYYY-MM-DD' format.
    
    Returns:
        int: The current NFL week.
    """
    # Convert Week 1 start date to a datetime object
    week1_date = datetime.strptime(week1_start_date, "%Y-%m-%d")
    
    # Get today's date
    today = datetime.today()

    # Calculate the number of days since Week 1
    days_since_week1 = (today - week1_date).days

    # Calculate the current week (each week is 7 days)
    # Add +1 to include partial weeks as the next week
    current_week = ((days_since_week1 // 7) + 1)

    # NFL regular season typically has 18 weeks
    # Post-season weeks start after Week 18
    if current_week < 1:
        return 1  # Before Week 1
    elif current_week > 18:
        return current_week  # Handle post-season weeks correctly
    else:
        return current_week

# Example usage
week1_start = "2024-09-05"  # Example Week 1 start date
current_week = get_current_nfl_week(week1_start)
print(f"The current NFL week is: {current_week}")
