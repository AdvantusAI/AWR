"""
MADP (Mean Absolute Deviation Percentage) calculation module for the ASR system.

This module provides functionality to calculate MADP which measures the 
deviation between forecast and actual demand. MADP is a critical factor
in safety stock calculations and SKU classification.
"""
import numpy as np
import math
from datetime import datetime, timedelta
from sqlalchemy import and_, func
from typing import List, Dict, Tuple, Optional, Union


def calculate_madp(actuals: List[float], forecasts: List[float]) -> float:
    """
    Calculate Mean Absolute Deviation Percentage for a series of actuals and forecasts.
    
    MADP describes how much a SKU's demand deviates from its forecast. The higher
    the MADP, the more a SKU's demand deviates from the forecast, and the more
    safety stock is required to maintain the Service Level Goal.
    
    Args:
        actuals (list): List of actual demand values
        forecasts (list): List of forecast values
    
    Returns:
        float: Mean Absolute Deviation Percentage
    """
    if not actuals or not forecasts:
        return 0.0
    
    if len(actuals) != len(forecasts):
        raise ValueError("Actuals and forecasts must have the same length")
    
    # Avoid division by zero - only consider pairs where forecast is not zero
    valid_pairs = [(a, f) for a, f in zip(actuals, forecasts) if f != 0]
    
    if not valid_pairs:
        return 0.0
    
    # Calculate the MADP for each period as |actual - forecast| / forecast * 100
    madp_values = [abs(a - f) / f * 100 for a, f in valid_pairs]
    
    # Return the average MADP
    return sum(madp_values) / len(madp_values)


def calculate_madp_from_history(session, sku_id: str, store_id: str, 
                               periods_to_analyze: int = 12,
                               periodicity: int = 13) -> float:
    """
    Calculate MADP from a SKU's demand history in the database.
    
    Args:
        session: SQLAlchemy session
        sku_id (str): The SKU ID
        store_id (str): The Store ID
        periods_to_analyze (int): Number of periods to include in the analysis
        periodicity (int): Forecasting periodicity (13 for 4-weekly, 52 for weekly)
    
    Returns:
        float: Calculated MADP value
    """
    from models.sku import DemandHistory, ForecastData
    
    # Get current period information
    current_year, current_period = get_current_period(periodicity)
    
    # Generate a list of periods to analyze
    periods = get_periods_to_analyze(current_year, current_period, 
                                    periods_to_analyze, periodicity)
    
    # Query demand history for these periods
    history_records = session.query(DemandHistory).filter(
        and_(
            DemandHistory.sku_id == sku_id,
            DemandHistory.store_id == store_id,
            DemandHistory.ignore_history == False,
            (DemandHistory.period_year, DemandHistory.period_number).in_(periods)
        )
    ).all()
    
    # Get forecast data for these periods
    forecast_records = {}
    for record in session.query(ForecastData).filter(
        and_(
            ForecastData.sku_id == sku_id,
            ForecastData.store_id == store_id
        )
    ).all():
        if periodicity == 13:
            forecast_records[(record.period_year, record.period_number)] = record.period_forecast
        else:
            forecast_records[(record.period_year, record.period_number)] = record.weekly_forecast
    
    # Extract actual and forecast values
    actuals = []
    forecasts = []
    
    for record in history_records:
        # Get the actual demand (total_demand field)
        actuals.append(record.total_demand)
        
        # Get the corresponding forecast
        period_key = (record.period_year, record.period_number)
        if period_key in forecast_records:
            forecasts.append(forecast_records[period_key])
        else:
            # If no forecast found, we can't include this period
            actuals.pop()
    
    # Calculate MADP
    if not actuals or not forecasts:
        return 0.0
    
    return calculate_madp(actuals, forecasts)


def get_current_period(periodicity: int = 13) -> Tuple[int, int]:
    """
    Get the current period number and year based on periodicity.
    
    Args:
        periodicity (int): Forecasting periodicity (13 for 4-weekly, 52 for weekly)
    
    Returns:
        tuple: (period_year, period_number)
    """
    today = datetime.now().date()
    year = today.year
    
    if periodicity == 13:
        # 4-weekly periods (13 periods per year)
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
            # Early January but last ISO week of previous year
            year -= 1
        elif today.month == 12 and week == 1:
            # Late December but first ISO week of next year
            year += 1
    
    return year, period


def get_periods_to_analyze(current_year: int, current_period: int, 
                         periods_to_analyze: int, periodicity: int) -> List[Tuple[int, int]]:
    """
    Generate a list of periods to analyze for MADP calculation.
    
    Args:
        current_year (int): Current year
        current_period (int): Current period
        periods_to_analyze (int): Number of periods to include
        periodicity (int): Forecasting periodicity (13 for 4-weekly, 52 for weekly)
    
    Returns:
        list: List of (year, period) tuples
    """
    periods = []
    year = current_year
    period = current_period
    
    # Start with the previous period (since we're analyzing history)
    period -= 1
    if period < 1:
        period = periodicity
        year -= 1
    
    # Generate periods list going backward in time
    for _ in range(periods_to_analyze):
        periods.append((year, period))
        
        period -= 1
        if period < 1:
            period = periodicity
            year -= 1
    
    return periods


def update_madp_for_sku(session, sku_id: str, store_id: str) -> float:
    """
    Calculate MADP for a SKU and update it in the database.
    
    Args:
        session: SQLAlchemy session
        sku_id (str): The SKU ID
        store_id (str): The Store ID
    
    Returns:
        float: The updated MADP value
    """
    from models.sku import SKU, ForecastData
    
    # Get the SKU
    sku = session.query(SKU).filter(
        and_(
            SKU.sku_id == sku_id,
            SKU.store_id == store_id
        )
    ).first()
    
    if not sku:
        raise ValueError(f"SKU {sku_id} not found for store {store_id}")
    
    # Get the forecast data
    forecast_data = session.query(ForecastData).filter(
        and_(
            ForecastData.sku_id == sku_id,
            ForecastData.store_id == store_id
        )
    ).first()
    
    if not forecast_data:
        # Create new forecast data if it doesn't exist
        forecast_data = ForecastData(
            sku_id=sku_id,
            store_id=store_id,
            sku_store_id=sku.id
        )
        session.add(forecast_data)
    
    # Calculate the new MADP
    periodicity = 13  # Default to 4-weekly
    if hasattr(sku, 'forecast_periodicity'):
        periodicity = sku.forecast_periodicity
    
    madp_value = calculate_madp_from_history(
        session, sku_id, store_id, periodicity=periodicity
    )
    
    # Update the MADP
    forecast_data.madp = madp_value
    session.commit()
    
    return madp_value


def determine_system_class_from_madp(madp: float, 
                                    lumpy_demand_limit: float = 50.0,
                                    slow_demand_limit: float = 10.0) -> str:
    """
    Determine the System Class code based on MADP value.
    
    Args:
        madp (float): MADP value
        lumpy_demand_limit (float): Threshold for Lumpy classification
        slow_demand_limit (float): Threshold for Slow classification
    
    Returns:
        str: System Class code ('L' for Lumpy, 'S' for Slow, 'R' for Regular)
    """
    # Note: This is a simplified version. In a real system, Slow would be
    # based on forecast value, not MADP. We're including it for illustration.
    
    if madp >= lumpy_demand_limit:
        return 'L'  # Lumpy
    elif madp <= slow_demand_limit:
        return 'S'  # Slow
    else:
        return 'R'  # Regular


def batch_update_madp(session, buyer_id: Optional[str] = None) -> Dict[str, int]:
    """
    Update MADP values for multiple SKUs in a batch process.
    
    Args:
        session: SQLAlchemy session
        buyer_id (str, optional): Filter by Buyer ID
    
    Returns:
        dict: Statistics about the update
    """
    from models.sku import SKU
    
    # Query for SKUs
    query = session.query(SKU)
    
    # Filter by buyer ID if provided
    if buyer_id:
        query = query.filter(SKU.source.has(buyer_id=buyer_id))
    
    # Get all active SKUs (R-Regular or W-Watch)
    skus = query.filter(SKU.buyer_class.in_(['R', 'W'])).all()
    
    stats = {
        'total': len(skus),
        'updated': 0,
        'errors': 0
    }
    
    # Update MADP for each SKU
    for sku in skus:
        try:
            update_madp_for_sku(session, sku.sku_id, sku.store_id)
            stats['updated'] += 1
        except Exception as e:
            print(f"Error updating MADP for SKU {sku.sku_id}: {e}")
            stats['errors'] += 1
    
    return stats