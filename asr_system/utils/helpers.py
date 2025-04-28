"""
Helper functions for the ASR system.
"""
import datetime
import math
import numpy as np
from sqlalchemy import and_

def get_current_period(periodicity=13):
    """
    Get the current period number and year based on periodicity.
    
    Args:
        periodicity (int): Forecasting periodicity (13 for 4-weekly, 52 for weekly)
    
    Returns:
        tuple: (period_year, period_number)
    """
    today = datetime.date.today()
    year = today.year
    
    if periodicity == 13:
        # 4-weekly periods (13 periods per year)
        # We need to determine which 4-week period we're in
        day_of_year = today.timetuple().tm_yday
        period = math.ceil(day_of_year / 28)
        if period > 13:
            # If we've gone past 13 periods, we're in the next year
            period = 1
            year += 1
    else:
        # Weekly periods (52 periods per year)
        # ISO week number
        week = today.isocalendar()[1]
        period = week
        
        # Adjust for year boundary if needed
        if today.month == 1 and week > 50:
            # We're in early January but still in the last ISO week of the previous year
            year -= 1
        elif today.month == 12 and week == 1:
            # We're in late December but in the first ISO week of the next year
            year += 1
    
    return year, period

def calculate_mad(actuals, forecasts):
    """
    Calculate Mean Absolute Deviation for a series of actuals and forecasts.
    
    Args:
        actuals (list): List of actual values
        forecasts (list): List of forecast values
    
    Returns:
        float: Mean Absolute Deviation
    """
    if not actuals or not forecasts:
        return 0.0
    
    if len(actuals) != len(forecasts):
        raise ValueError("Actuals and forecasts must have the same length")
    
    deviations = [abs(a - f) for a, f in zip(actuals, forecasts)]
    return sum(deviations) / len(deviations)

def calculate_madp(actuals, forecasts):
    """
    Calculate Mean Absolute Deviation Percentage for a series of actuals and forecasts.
    
    Args:
        actuals (list): List of actual values
        forecasts (list): List of forecast values
    
    Returns:
        float: Mean Absolute Deviation Percentage
    """
    if not actuals or not forecasts:
        return 0.0
    
    if len(actuals) != len(forecasts):
        raise ValueError("Actuals and forecasts must have the same length")
    
    # Avoid division by zero
    valid_pairs = [(a, f) for a, f in zip(actuals, forecasts) if f != 0]
    
    if not valid_pairs:
        return 0.0
    
    madp_values = [abs(a - f) / f * 100 for a, f in valid_pairs]
    return sum(madp_values) / len(madp_values)

def calculate_tracking_signal(actuals, forecasts):
    """
    Calculate the Tracking Signal for a series of actuals and forecasts.
    
    Args:
        actuals (list): List of actual values
        forecasts (list): List of forecast values
    
    Returns:
        float: Tracking Signal (-1 to 1, with 0 being no trend)
    """
    if not actuals or not forecasts:
        return 0.0
    
    if len(actuals) != len(forecasts):
        raise ValueError("Actuals and forecasts must have the same length")
    
    # Calculate errors and absolute errors
    errors = [a - f for a, f in zip(actuals, forecasts)]
    abs_errors = [abs(e) for e in errors]
    
    # Avoid division by zero
    mad = sum(abs_errors) / len(abs_errors) if abs_errors else 0
    if mad == 0:
        return 0.0
    
    # Tracking signal is the sum of errors divided by MAD
    return sum(errors) / (mad * len(errors))

def round_to_buying_multiple(quantity, buying_multiple):
    """
    Round a quantity up to the nearest buying multiple.
    
    Args:
        quantity (float): Quantity to round
        buying_multiple (int): Buying multiple
    
    Returns:
        float: Quantity rounded up to the nearest buying multiple
    """
    if buying_multiple <= 0:
        return quantity
    
    return math.ceil(quantity / buying_multiple) * buying_multiple

def calculate_available_balance(on_hand, on_order, back_order, reserved, quantity_held):
    """
    Calculate the available balance for a SKU.
    
    Args:
        on_hand (float): On-hand quantity
        on_order (float): On-order quantity
        back_order (float): Back-ordered quantity
        reserved (float): Reserved quantity
        quantity_held (float): Quantity held
    
    Returns:
        float: Available balance
    """
    return on_hand + on_order - back_order - reserved - quantity_held

def get_seasonal_index(profile, period_number):
    """
    Get the seasonal index for a period from a seasonal profile.
    
    Args:
        profile: Seasonal profile object
        period_number (int): Period number (1-13)
    
    Returns:
        float: Seasonal index
    """
    if not profile:
        return 1.0
    
    # Map period number to profile attribute
    attr_name = f"p{period_number}_index"
    
    # Return the index or 1.0 if not found
    return getattr(profile, attr_name, 1.0)

def filter_skus_by_buyer(session, sku_model, buyer_id=None):
    """
    Filter SKUs by buyer ID.
    
    Args:
        session: SQLAlchemy session
        sku_model: SKU model class
        buyer_id (str): Buyer ID to filter by (None for all)
    
    Returns:
        SQLAlchemy query object
    """
    query = session.query(sku_model)
    
    if buyer_id:
        # Join to source to filter by buyer ID
        query = query.join(sku_model.source).filter(source_model.buyer_id == buyer_id)
    
    return query

def is_due_order(order_cycle, last_order_date):
    """
    Determine if an order is due based on order cycle and last order date.
    
    Args:
        order_cycle (int): Order cycle in days
        last_order_date (datetime): Last order date
    
    Returns:
        bool: True if order is due, False otherwise
    """
    if not last_order_date:
        return True
    
    today = datetime.datetime.now().date()
    days_since_last_order = (today - last_order_date.date()).days
    
    return days_since_last_order >= order_cycle

def get_period_range(years=3, periodicity=13):
    """
    Get a range of periods for history lookback.
    
    Args:
        years (int): Number of years to look back
        periodicity (int): Forecasting periodicity (13 for 4-weekly, 52 for weekly)
    
    Returns:
        list: List of (year, period) tuples
    """
    current_year, current_period = get_current_period(periodicity)
    
    periods = []
    for year_offset in range(years):
        year = current_year - year_offset
        
        if year_offset == 0:
            # Current year - include only periods up to current period
            for period in range(1, current_period + 1):
                periods.append((year, period))
        else:
            # Previous years - include all periods
            for period in range(1, periodicity + 1):
                periods.append((year, period))
    
    return periods