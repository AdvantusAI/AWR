"""
Helper functions for the ASR system.
"""
import datetime
import math
import numpy as np
from sqlalchemy import and_
from typing import List

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
        day_of_year = today.timetuple().tm_yday
        # Each period is 28 days, starting from day 1
        period = ((day_of_year - 1) // 28) + 1
        if period > 13:
            # If we've gone past 13 periods, we're in the next year
            period = 1
            year += 1
    else:
        # Weekly periods (52 periods per year)
        # ISO week number
        period = today.isocalendar()[1]
        
        # Adjust for year boundary if needed
        if today.month == 1 and period > 50:
            # We're in early January but still in the last ISO week of the previous year
            year -= 1
        elif today.month == 12 and period == 1:
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

def calculate_tracking_signal(actuals: List[float], forecasts: List[float]) -> float:
    """
    Calculate the Tracking Signal for a series of actuals and forecasts.
    
    The tracking signal measures if a SKU is trending up or down compared to the forecast.
    It is used in the reforecasting calculation at period-end.
    
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
    
    # Tracking signal is the sum of errors divided by MAD * number of periods
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

def collect_demand_history(session, sku_id, store_id, periods=12, periodicity=13):
    """
    Collect demand history for a SKU/store over a specified number of periods.
    
    Args:
        session: SQLAlchemy session
        sku_id (str): SKU ID
        store_id (str): Store ID
        periods (int): Number of periods to collect
        periodicity (int): Forecasting periodicity (13 for 4-weekly, 52 for weekly)
    
    Returns:
        tuple: (actuals, forecasts, periods_list) where periods_list is [(year, period),...]
    """
    # Get current period
    current_year, current_period = get_current_period(periodicity)
    
    # Generate periods list
    periods_list = []
    year, period = current_year, current_period
    
    # Start with the previous period
    period -= 1
    if period < 1:
        period = periodicity
        year -= 1
    
    for _ in range(periods):
        periods_list.append((year, period))
        period -= 1
        if period < 1:
            period = periodicity
            year -= 1
    
    # Get demand history for these periods
    history_records = {}
    for record in session.query(DemandHistory).filter(
        and_(
            DemandHistory.sku_id == sku_id,
            DemandHistory.store_id == store_id,
            DemandHistory.ignore_history == False,
            (DemandHistory.period_year, DemandHistory.period_number).in_(periods_list)
        )
    ).all():
        history_records[(record.period_year, record.period_number)] = record
    
    # Get forecast data for these periods
    forecast_data = session.query(ForecastData).filter(
        and_(
            ForecastData.sku_id == sku_id,
            ForecastData.store_id == store_id
        )
    ).first()
    
    # Extract actual and forecast values
    actuals = []
    forecasts = []
    
    for year, period in periods_list:
        if (year, period) in history_records:
            record = history_records[(year, period)]
            actuals.append(record.total_demand)
            
            # Use the appropriate forecast value based on periodicity
            if forecast_data:
                if periodicity == 13:
                    forecasts.append(forecast_data.period_forecast)
                else:
                    forecasts.append(forecast_data.weekly_forecast)
            else:
                # If no forecast found, we can't include this period
                actuals.pop()
    
    return actuals, forecasts, periods_list

def e3_regular_avs_reforecast(session, sku_id, store_id, periodicity=13):
    """
    Perform E3 Regular AVS reforecasting for a SKU.
    
    The E3 Regular AVS method uses tracking signal as a weight factor to
    blend the most recent demand with the existing forecast.
    
    Formula: [Track × Most Recent Demand] + [(1-Track) × Old Forecast] = New Forecast
    
    Args:
        session: SQLAlchemy session
        sku_id (str): SKU ID
        store_id (str): Store ID
        periodicity (int): Forecasting periodicity (13 for 4-weekly, 52 for weekly)
    
    Returns:
        dict: Results of reforecasting
    """
    # Get the SKU and its forecast data
    sku = session.query(SKU).filter(
        and_(
            SKU.sku_id == sku_id,
            SKU.store_id == store_id
        )
    ).first()
    
    if not sku:
        raise ValueError(f"SKU {sku_id} not found for store {store_id}")
    
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
    
    # Check if forecast is frozen
    if hasattr(sku, 'freeze_forecast_until') and sku.freeze_forecast_until:
        if sku.freeze_forecast_until > datetime.date.today():
            # Skip reforecasting if the forecast is frozen
            return {
                'sku_id': sku_id,
                'store_id': store_id,
                'reforecast': False,
                'reason': 'frozen',
                'current_forecast': {
                    'weekly': forecast_data.weekly_forecast,
                    'period': forecast_data.period_forecast,
                    'quarterly': forecast_data.quarterly_forecast,
                    'yearly': forecast_data.yearly_forecast
                }
            }
    
    # Collect demand history and calculate MADP and Tracking Signal
    actuals, forecasts, periods_list = collect_demand_history(
        session, sku_id, store_id, periodicity=periodicity
    )
    
    if not actuals or not forecasts:
        return {
            'sku_id': sku_id,
            'store_id': store_id,
            'reforecast': False,
            'reason': 'no_history',
            'current_forecast': {
                'weekly': forecast_data.weekly_forecast,
                'period': forecast_data.period_forecast,
                'quarterly': forecast_data.quarterly_forecast,
                'yearly': forecast_data.yearly_forecast
            }
        }
    
    # Calculate MADP
    madp = calculate_madp(actuals, forecasts)
    
    # Calculate Tracking Signal (trend indicator)
    track = calculate_tracking_signal(actuals, forecasts)
    
    # Store the old forecast for reference
    old_forecast = {
        'weekly': forecast_data.weekly_forecast,
        'period': forecast_data.period_forecast,
        'quarterly': forecast_data.quarterly_forecast,
        'yearly': forecast_data.yearly_forecast
    }
    
    # Get most recent demand (first in the list since we collected going backward)
    most_recent_demand = actuals[0] if actuals else 0
    
    # Get the current forecast value based on periodicity
    if periodicity == 13:
        current_forecast = forecast_data.period_forecast
    else:
        current_forecast = forecast_data.weekly_forecast
    
    # If current forecast is None, initialize it
    if current_forecast is None:
        current_forecast = 0
    
    # Calculate track percentage as a weight (between 0 and 1)
    # Using abs because track signal can be negative, but we want positive weight
    track_weight = min(abs(track), 0.5)  # Cap weight at 0.5 (50%)
    
    # Apply the E3 Regular AVS formula for reforecasting
    # [Track × Most Recent Demand] + [(1-Track) × Old Forecast] = New Forecast
    new_forecast = (track_weight * most_recent_demand) + ((1 - track_weight) * current_forecast)
    
    # Update the forecast data
    if periodicity == 13:
        forecast_data.period_forecast = new_forecast
        # Update weekly, quarterly, and yearly forecasts based on period forecast
        forecast_data.weekly_forecast = new_forecast / 4  # 4 weeks in a period
        forecast_data.quarterly_forecast = new_forecast * 3  # 3 periods in a quarter
        forecast_data.yearly_forecast = new_forecast * 13  # 13 periods in a year
    else:
        forecast_data.weekly_forecast = new_forecast
        # Update period, quarterly, and yearly forecasts based on weekly forecast
        forecast_data.period_forecast = new_forecast * 4  # 4 weeks in a period
        forecast_data.quarterly_forecast = new_forecast * 13  # 13 weeks in a quarter
        forecast_data.yearly_forecast = new_forecast * 52  # 52 weeks in a year
    
    # Update MADP and track
    forecast_data.madp = madp
    forecast_data.track = track
    
    # Update last forecast date
    forecast_data.last_forecast_date = datetime.datetime.now()
    
    # Save changes
    session.commit()
    
    return {
        'sku_id': sku_id,
        'store_id': store_id,
        'reforecast': True,
        'old_forecast': old_forecast,
        'new_forecast': {
            'weekly': forecast_data.weekly_forecast,
            'period': forecast_data.period_forecast,
            'quarterly': forecast_data.quarterly_forecast,
            'yearly': forecast_data.yearly_forecast
        },
        'madp': madp,
        'track': track,
        'most_recent_demand': most_recent_demand,
        'track_weight': track_weight
    }

def e3_enhanced_avs_reforecast(session, sku_id, store_id, periodicity=13):
    """
    Perform E3 Enhanced AVS reforecasting for a SKU.
    
    The E3 Enhanced AVS method is designed for slow-moving or intermittent selling SKUs.
    It only updates the forecast when demand occurs and accounts for the time since 
    the last demand occurrence.
    
    Args:
        session: SQLAlchemy session
        sku_id (str): SKU ID
        store_id (str): Store ID
        periodicity (int): Forecasting periodicity (13 for 4-weekly, 52 for weekly)
    
    Returns:
        dict: Results of reforecasting
    """
    # Get the SKU and its forecast data
    sku = session.query(SKU).filter(
        and_(
            SKU.sku_id == sku_id,
            SKU.store_id == store_id
        )
    ).first()
    
    if not sku:
        raise ValueError(f"SKU {sku_id} not found for store {store_id}")
    
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
    
    # Check if forecast is frozen
    if hasattr(sku, 'freeze_forecast_until') and sku.freeze_forecast_until:
        if sku.freeze_forecast_until > datetime.date.today():
            # Skip reforecasting if the forecast is frozen
            return {
                'sku_id': sku_id,
                'store_id': store_id,
                'reforecast': False,
                'reason': 'frozen',
                'current_forecast': {
                    'weekly': forecast_data.weekly_forecast,
                    'period': forecast_data.period_forecast,
                    'quarterly': forecast_data.quarterly_forecast,
                    'yearly': forecast_data.yearly_forecast
                }
            }
    
    # Collect demand history and calculate MADP and Tracking Signal
    actuals, forecasts, periods_list = collect_demand_history(
        session, sku_id, store_id, periodicity=periodicity
    )
    
    if not actuals or not forecasts:
        return {
            'sku_id': sku_id,
            'store_id': store_id,
            'reforecast': False,
            'reason': 'no_history',
            'current_forecast': {
                'weekly': forecast_data.weekly_forecast,
                'period': forecast_data.period_forecast,
                'quarterly': forecast_data.quarterly_forecast,
                'yearly': forecast_data.yearly_forecast
            }
        }
    
    # Store the old forecast for reference
    old_forecast = {
        'weekly': forecast_data.weekly_forecast,
        'period': forecast_data.period_forecast,
        'quarterly': forecast_data.quarterly_forecast,
        'yearly': forecast_data.yearly_forecast
    }
    
    # Get most recent demand (first in the list since we collected going backward)
    most_recent_demand = actuals[0] if actuals else 0
    zero_demand_periods = 0
    
    # Count consecutive zero demand periods
    for demand in actuals:
        if demand == 0:
            zero_demand_periods += 1
        else:
            break
    
    # Get the forecasting demand limit from company properties or default
    # This is the minimum demand threshold for triggering reforecasting
    forecasting_demand_limit = 0
    if hasattr(sku, 'forecasting_demand_limit'):
        forecasting_demand_limit = sku.forecasting_demand_limit
    
    # Get update frequency impact control from company properties or default
    update_frequency_impact_control = 2  # Default value
    # In a real implementation, this would be fetched from company properties
    
    # Check if reforecasting is needed
    need_reforecast = False
    
    if most_recent_demand > forecasting_demand_limit:
        # Reforecast if there's significant demand in the most recent period
        need_reforecast = True
    elif zero_demand_periods > 0:
        # Check if we need to force a reforecast due to extended zero demand
        # Calculate expected zero demand periods
        expected_zero_periods = 1  # This would be calculated based on average demand pattern
        
        # Calculate force reforecast threshold
        force_reforecast_threshold = expected_zero_periods * update_frequency_impact_control
        
        if zero_demand_periods >= force_reforecast_threshold:
            need_reforecast = True
    
    if not need_reforecast:
        return {
            'sku_id': sku_id,
            'store_id': store_id,
            'reforecast': False,
            'reason': 'no_demand_trigger',
            'current_forecast': {
                'weekly': forecast_data.weekly_forecast,
                'period': forecast_data.period_forecast,
                'quarterly': forecast_data.quarterly_forecast,
                'yearly': forecast_data.yearly_forecast
            },
            'zero_demand_periods': zero_demand_periods
        }
    
    # Get the current forecast value based on periodicity
    if periodicity == 13:
        current_forecast = forecast_data.period_forecast
    else:
        current_forecast = forecast_data.weekly_forecast
    
    # If current forecast is None, initialize it
    if current_forecast is None:
        current_forecast = 0
    
    # Calculate MADP
    madp = calculate_madp(actuals, forecasts)
    
    # Calculate Tracking Signal (trend indicator)
    track = calculate_tracking_signal(actuals, forecasts)
    
    # Calculate track percentage as a weight (between 0 and 1)
    # Using abs because track signal can be negative, but we want positive weight
    track_weight = min(abs(track), 0.5)  # Cap weight at 0.5 (50%)
    
    # For E3 Enhanced AVS, adjust the weight based on time since last demand
    if zero_demand_periods > 0:
        # Adjust weight to account for time since last non-zero demand
        adjusted_weight = track_weight / (zero_demand_periods + 1)
    else:
        adjusted_weight = track_weight
    
    # Apply the E3 Enhanced AVS formula for reforecasting
    # [Adjusted_Weight × Most Recent Demand] + [(1-Adjusted_Weight) × Old Forecast] = New Forecast
    new_forecast = (adjusted_weight * most_recent_demand) + ((1 - adjusted_weight) * current_forecast)
    
    # Update the forecast data
    if periodicity == 13:
        forecast_data.period_forecast = new_forecast
        # Update weekly, quarterly, and yearly forecasts based on period forecast
        forecast_data.weekly_forecast = new_forecast / 4  # 4 weeks in a period
        forecast_data.quarterly_forecast = new_forecast * 3  # 3 periods in a quarter
        forecast_data.yearly_forecast = new_forecast * 13  # 13 periods in a year
    else:
        forecast_data.weekly_forecast = new_forecast
        # Update period, quarterly, and yearly forecasts based on weekly forecast
        forecast_data.period_forecast = new_forecast * 4  # 4 weeks in a period
        forecast_data.quarterly_forecast = new_forecast * 13  # 13 weeks in a quarter
        forecast_data.yearly_forecast = new_forecast * 52  # 52 weeks in a year
    
    # Update MADP and track
    forecast_data.madp = madp
    forecast_data.track = track
    
    # Update last forecast date
    forecast_data.last_forecast_date = datetime.datetime.now()
    
    # Save changes
    session.commit()
    
    return {
        'sku_id': sku_id,
        'store_id': store_id,
        'reforecast': True,
        'old_forecast': old_forecast,
        'new_forecast': {
            'weekly': forecast_data.weekly_forecast,
            'period': forecast_data.period_forecast,
            'quarterly': forecast_data.quarterly_forecast,
            'yearly': forecast_data.yearly_forecast
        },
        'madp': madp,
        'track': track,
        'most_recent_demand': most_recent_demand,
        'adjusted_weight': adjusted_weight,
        'zero_demand_periods': zero_demand_periods
    }

def apply_seasonal_profile(session, sku_id, store_id):
    """
    Apply a seasonal profile to a SKU's forecast.
    
    Args:
        session: SQLAlchemy session
        sku_id (str): SKU ID
        store_id (str): Store ID
    
    Returns:
        dict: Results of applying the seasonal profile
    """
    # Get the SKU
    sku = session.query(SKU).filter(
        and_(
            SKU.sku_id == sku_id,
            SKU.store_id == store_id
        )
    ).first()
    
    if not sku:
        return None
        
    # Get current period
    current_period, _ = get_current_period()
    
    # Get seasonal index for current period
    seasonal_index = get_seasonal_index(sku.demand_profile_id, current_period)
    
    # Apply seasonal index to forecasts
    deseasonalized_forecast = {
        'weekly': sku.demand_weekly / seasonal_index if seasonal_index else sku.demand_weekly,
        'period': sku.demand_4weekly / seasonal_index if seasonal_index else sku.demand_4weekly,
        'quarterly': sku.demand_quarterly / seasonal_index if seasonal_index else sku.demand_quarterly,
        'yearly': sku.demand_yearly / seasonal_index if seasonal_index else sku.demand_yearly
    }
    
    return {
        'sku_id': sku_id,
        'store_id': store_id,
        'profile_applied': True,
        'profile_id': sku.demand_profile_id,
        'seasonal_index': seasonal_index,
        'current_period': current_period,
        'deseasonalized_forecast': deseasonalized_forecast
    }