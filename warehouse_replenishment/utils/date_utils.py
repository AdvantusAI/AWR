# warehouse_replenishment/utils/date_utils.py
from datetime import date, datetime, timedelta
from typing import Tuple, List, Dict, Optional, Union
import calendar

def get_current_period(periodicity: int) -> Tuple[int, int]:
    """Get the current period number and year based on periodicity.
    
    Args:
        periodicity: Periodicity (12=monthly, 13=4-weekly, 52=weekly)
        
    Returns:
        Tuple with period number and year
    """
    today = date.today()
    year = today.year
    
    if periodicity == 12:  # Monthly
        return (today.month, year)
    
    elif periodicity == 13:  # 4-weekly
        # Calculate which 4-week period we're in
        # Each period is 28 days (4 weeks)
        day_of_year = today.timetuple().tm_yday
        period = ((day_of_year - 1) // 28) + 1
        
        # Handle period rollover to next year
        if period > 13:
            period = 1
            year += 1
            
        return (period, year)
    
    elif periodicity == 52:  # Weekly
        # ISO week number
        week = today.isocalendar()[1]
        
        # Handle year edge cases
        if week > 52:
            week = 1
            year += 1
        
        return (week, year)
    
    else:
        raise ValueError(f"Invalid periodicity: {periodicity}")

def get_previous_period(period: int, year: int, periodicity: int) -> Tuple[int, int]:
    """Get the previous period number and year.
    
    Args:
        period: Current period number
        year: Current year
        periodicity: Periodicity (12=monthly, 13=4-weekly, 52=weekly)
        
    Returns:
        Tuple with previous period number and year
    """
    if periodicity == 12:  # Monthly
        if period == 1:
            return (12, year - 1)
        else:
            return (period - 1, year)
    
    elif periodicity == 13:  # 4-weekly
        if period == 1:
            return (13, year - 1)
        else:
            return (period - 1, year)
    
    elif periodicity == 52:  # Weekly
        if period == 1:
            # Last week of previous year (typically 52, but can be 53)
            prev_year = year - 1
            prev_period = 52  # Default to 52
            
            # Check if previous year had 53 weeks
            jan1 = date(prev_year, 1, 1)
            dec31 = date(prev_year, 12, 31)
            
            if dec31.isocalendar()[1] == 53:
                prev_period = 53
                
            return (prev_period, prev_year)
        else:
            return (period - 1, year)
    
    else:
        raise ValueError(f"Invalid periodicity: {periodicity}")

def get_next_period(period: int, year: int, periodicity: int) -> Tuple[int, int]:
    """Get the next period number and year.
    
    Args:
        period: Current period number
        year: Current year
        periodicity: Periodicity (12=monthly, 13=4-weekly, 52=weekly)
        
    Returns:
        Tuple with next period number and year
    """
    if periodicity == 12:  # Monthly
        if period == 12:
            return (1, year + 1)
        else:
            return (period + 1, year)
    
    elif periodicity == 13:  # 4-weekly
        if period == 13:
            return (1, year + 1)
        else:
            return (period + 1, year)
    
    elif periodicity == 52:  # Weekly
        # Check if current year has 53 weeks
        dec31 = date(year, 12, 31)
        has_week_53 = dec31.isocalendar()[1] == 53
        
        if (period == 52 and not has_week_53) or period == 53:
            return (1, year + 1)
        else:
            return (period + 1, year)
    
    else:
        raise ValueError(f"Invalid periodicity: {periodicity}")

def get_period_dates(period: int, year: int, periodicity: int) -> Tuple[date, date]:
    """Get start and end dates for a given period.
    
    Args:
        period: Period number
        year: Year
        periodicity: Periodicity (12=monthly, 13=4-weekly, 52=weekly)
        
    Returns:
        Tuple with start date and end date
    """
    if periodicity == 12:  # Monthly
        start_date = date(year, period, 1)
        
        # Last day of month
        if period == 12:
            end_date = date(year, 12, 31)
        else:
            next_month = period + 1
            end_date = date(year, next_month, 1) - timedelta(days=1)
            
        return (start_date, end_date)
    
    elif periodicity == 13:  # 4-weekly
        # Each period is 28 days (4 weeks)
        # First period starts on January 1
        jan1 = date(year, 1, 1)
        
        # Calculate start and end dates
        start_date = jan1 + timedelta(days=(period - 1) * 28)
        end_date = start_date + timedelta(days=27)
        
        return (start_date, end_date)
    
    elif periodicity == 52:  # Weekly
        # Find the first day of the given ISO week
        jan1 = date(year, 1, 1)
        
        # Get the ISO week number of January 1
        jan1_week = jan1.isocalendar()[1]
        jan1_weekday = jan1.isocalendar()[2]  # 1=Monday, 7=Sunday
        
        # Calculate start of first week
        if jan1_week == 1:
            # Jan 1 is in week 1
            start_of_week1 = jan1 - timedelta(days=jan1_weekday - 1)
        else:
            # Jan 1 is in the last week of the previous year
            # First week starts on the first Monday of the year
            start_of_week1 = jan1 + timedelta(days=8 - jan1_weekday)
            
        # Calculate start and end of the specified week
        start_date = start_of_week1 + timedelta(days=(period - 1) * 7)
        end_date = start_date + timedelta(days=6)
        
        return (start_date, end_date)
    
    else:
        raise ValueError(f"Invalid periodicity: {periodicity}")

def get_period_for_date(target_date: date, periodicity: int) -> Tuple[int, int]:
    """Get the period number and year for a given date.
    
    Args:
        target_date: Target date
        periodicity: Periodicity (12=monthly, 13=4-weekly, 52=weekly)
        
    Returns:
        Tuple with period number and year
    """
    year = target_date.year
    
    if periodicity == 12:  # Monthly
        return (target_date.month, year)
    
    elif periodicity == 13:  # 4-weekly
        # Calculate which 4-week period the date falls in
        day_of_year = target_date.timetuple().tm_yday
        period = ((day_of_year - 1) // 28) + 1
        
        # Handle period rollover to next year
        if period > 13:
            period = 1
            year += 1
            
        return (period, year)
    
    elif periodicity == 52:  # Weekly
        # ISO week number
        week = target_date.isocalendar()[1]
        week_year = target_date.isocalendar()[0]  # ISO year may differ from calendar year
        
        return (week, week_year)
    
    else:
        raise ValueError(f"Invalid periodicity: {periodicity}")

def is_period_end_day(target_date: date, periodicity: int) -> bool:
    """Check if the given date is the last day of a period.
    
    Args:
        target_date: Target date
        periodicity: Periodicity (12=monthly, 13=4-weekly, 52=weekly)
        
    Returns:
        True if the date is the last day of a period
    """
    if periodicity == 12:  # Monthly
        # Last day of month
        last_day = calendar.monthrange(target_date.year, target_date.month)[1]
        return target_date.day == last_day
    
    elif periodicity == 13:  # 4-weekly
        # Each period is 28 days (4 weeks)
        jan1 = date(target_date.year, 1, 1)
        day_of_year = target_date.timetuple().tm_yday
        
        # Calculate day within the 4-weekly period (0-27)
        day_in_period = (day_of_year - 1) % 28
        
        # Last day of period is day 27 (zero-based)
        return day_in_period == 27
    
    elif periodicity == 52:  # Weekly
        # Last day of ISO week (Sunday)
        return target_date.weekday() == 6  # 0=Monday, 6=Sunday
    
    else:
        raise ValueError(f"Invalid periodicity: {periodicity}")

def add_days(start_date: date, days: int) -> date:
    """Add days to a date.
    
    Args:
        start_date: Start date
        days: Number of days to add
        
    Returns:
        New date
    """
    return start_date + timedelta(days=days)

def days_between(start_date: date, end_date: date) -> int:
    """Calculate days between two dates.
    
    Args:
        start_date: Start date
        end_date: End date
        
    Returns:
        Number of days
    """
    delta = end_date - start_date
    return delta.days

def convert_to_date(date_string: str, format_string: str = "%Y-%m-%d") -> date:
    """Convert string to date.
    
    Args:
        date_string: Date string
        format_string: Format string
        
    Returns:
        Date object
    """
    return datetime.strptime(date_string, format_string).date()

def get_days_in_month(year: int, month: int) -> int:
    """Get number of days in a month.
    
    Args:
        year: Year
        month: Month
        
    Returns:
        Number of days
    """
    return calendar.monthrange(year, month)[1]

def get_weekday(target_date: date) -> int:
    """Get weekday number (1=Monday, 7=Sunday).
    
    Args:
        target_date: Target date
        
    Returns:
        Weekday number
    """
    return target_date.weekday() + 1  # Convert from 0-6 to 1-7

def get_next_weekday(from_date: date, weekday: int) -> date:
    """Get the next occurrence of a weekday.
    
    Args:
        from_date: Start date
        weekday: Target weekday (1=Monday, 7=Sunday)
        
    Returns:
        Date of next occurrence
    """
    # Convert from 1-7 to 0-6
    target_weekday = weekday - 1
    
    # Calculate days to add
    days_ahead = target_weekday - from_date.weekday()
    if days_ahead <= 0:  # Target weekday already passed this week
        days_ahead += 7
        
    return from_date + timedelta(days=days_ahead)

def get_day_of_month(year: int, month: int, day: int) -> date:
    """Get a specific day of a month, handling edge cases.
    
    Args:
        year: Year
        month: Month
        day: Day
        
    Returns:
        Date object
    """
    # Handle day exceeding month length
    max_day = get_days_in_month(year, month)
    actual_day = min(day, max_day)
    
    return date(year, month, actual_day)

def get_next_month_day(from_date: date, day: int) -> date:
    """Get the specified day in the next month.
    
    Args:
        from_date: Start date
        day: Target day of month
        
    Returns:
        Date in next month
    """
    # Get next month
    if from_date.month == 12:
        next_month = 1
        next_year = from_date.year + 1
    else:
        next_month = from_date.month + 1
        next_year = from_date.year
        
    return get_day_of_month(next_year, next_month, day)

def get_period_type(periodicity: int) -> str:
    """Get the period type based on periodicity.
    
    Args:
        periodicity: Periodicity (12=monthly, 13=4-weekly, 52=weekly)
        
    Returns:
        Period type string ('monthly', '4-weekly', 'weekly', or 'unknown')
    """
    if periodicity == 12:
        return 'monthly'
    elif periodicity == 13:
        return '4-weekly'
    elif periodicity == 52:
        return 'weekly'
    else:
        return 'unknown'