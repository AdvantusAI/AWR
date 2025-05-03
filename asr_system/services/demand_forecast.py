"""
Demand forecasting services for the ASR system.

This module implements the algorithms and functions for demand forecasting, 
including E3 Regular AVS, E3 Enhanced AVS, and seasonal adjustments.
"""
import logging
import math
import numpy as np
from datetime import datetime
from sqlalchemy import and_, func

from models.sku import SKU, ForecastData, DemandHistory, SeasonalProfile
from utils.helpers import get_current_period, calculate_madp, calculate_tracking_signal, get_seasonal_index
from utils.db import get_session
from config.settings import ASR_CONFIG

logger = logging.getLogger(__name__)


class ForecastingMethod:
    """Enumeration of available forecasting methods"""
    E3_REGULAR_AVS = "E3 Regular AVS"
    E3_ENHANCED_AVS = "E3 Enhanced AVS"
    DEMAND_IMPORT = "Demand Import"
    ALTERNATE = "Alternate"
    
def check_forecast_exceptions(sku, forecast_data, demand_history, stats, session):
    """
    Check for forecast exceptions based on recent demand vs forecast.
    
    Args:
        sku: SKU object
        forecast_data: ForecastData object
        demand_history: DemandHistory object
        stats: Statistics dictionary to update
        session: SQLAlchemy session
    """
    # Placeholder for company properties values - in real implementation, get from database
    demand_filter_high = 5.0
    demand_filter_low = 3.0
    tracking_signal_limit = 0.55
    
    # Get the old forecast and actual demand
    old_forecast = forecast_data.period_forecast
    actual_demand = demand_history.total_demand
    
    # Calculate MADP in units (forecast error)
    if old_forecast > 0:
        madp_units = forecast_data.madp * old_forecast / 100
    else:
        madp_units = 0
    
    # Check for Demand Filter High exception
    demand_high_threshold = old_forecast + (madp_units * demand_filter_high)
    if actual_demand > demand_high_threshold:
        logger.info(f"Demand Filter High exception for SKU {sku.sku_id}: Actual={actual_demand}, Threshold={demand_high_threshold}")
        stats['demand_filter_high'] += 1
        # In real implementation, create exception record
    
    # Check for Demand Filter Low exception
    demand_low_threshold = old_forecast - (madp_units * demand_filter_low)
    if actual_demand < demand_low_threshold and demand_low_threshold > 0:
        logger.info(f"Demand Filter Low exception for SKU {sku.sku_id}: Actual={actual_demand}, Threshold={demand_low_threshold}")
        stats['demand_filter_low'] += 1
        # In real implementation, create exception record
    
    # Check for Tracking Signal exceptions
    if forecast_data.track > tracking_signal_limit:
        # Determine if trending up or down
        track_direction = "high" if forecast_data.track > 0 else "low"
        logger.info(f"Tracking Signal {track_direction} exception for SKU {sku.sku_id}: Track={forecast_data.track}")
        
        if track_direction == "high":
            stats['tracking_signal_high'] += 1
        else:
            stats['tracking_signal_low'] += 1
        # In real implementation, create exception record

def get_historical_demand(sku, periods, session):
    """
    Get historical demand data for a SKU.
    
    Args:
        sku: SKU object
        periods: Number of periods to retrieve
        session: SQLAlchemy session
    
    Returns:
        list: DemandHistory objects
    """
    current_year, current_period = get_current_period(
        periodicity=sku.forecasting_periodicity
    )
    
    # Build a list of (year, period) tuples for the query
    period_list = []
    for i in range(periods):
        if current_period - i > 0:
            period_list.append((current_year, current_period - i))
        else:
            # Go to previous year
            periods_in_year = 13 if sku.forecasting_periodicity == 13 else 52
            period_list.append((current_year - 1, periods_in_year + current_period - i))
    
    # Query for demand history records
    history_data = session.query(DemandHistory).filter(
        DemandHistory.sku_id == sku.id,
        or_(*[
            and_(
                DemandHistory.period_year == year,
                DemandHistory.period_number == period
            ) for year, period in period_list
        ])
    ).order_by(
        DemandHistory.period_year.desc(),
        DemandHistory.period_number.desc()
    ).all()
    
    return history_data

def count_consecutive_zero_demand_periods(sku, session):
    """
    Count the number of consecutive periods with zero or below-limit demand.
    
    Args:
        sku: SKU object
        session: SQLAlchemy session
    
    Returns:
        int: Number of consecutive zero demand periods
    """
    current_year, current_period = get_current_period(
        periodicity=sku.forecasting_periodicity
    )
    
    # Get the forecasting demand limit
    forecast_demand_limit = sku.forecasting_demand_limit or 0
    
    count = 0
    period = current_period
    year = current_year
    
    while True:
        # Get previous period
        if period > 1:
            period -= 1
        else:
            period = 13 if sku.forecasting_periodicity == 13 else 52
            year -= 1
        
        # Get demand history for this period
        history = session.query(DemandHistory).filter(
            DemandHistory.sku_id == sku.id,
            DemandHistory.period_year == year,
            DemandHistory.period_number == period
        ).first()
        
        if not history or history.total_demand < forecast_demand_limit:
            count += 1
        else:
            break
        
        # Limit how far back we check to avoid infinite loops
        if count >= 24:  # Maximum 2 years worth of periods
            break
    
    return count

class ForecastingPeriodicity:
    """Enumeration of forecasting periodicity options"""
    WEEKLY = 52
    FOUR_WEEKLY = 13
     
def get_forecasting_method(sku):
    """
    Determine the forecasting method to use for a SKU.
    
    Args:
        sku: SKU object
    
    Returns:
        str: Forecasting method to use
    """
    # Default to E3 Regular AVS if not specified
    if not sku.forecast_method or sku.forecast_method == "E3 Regular AVS":
        return ForecastingMethod.E3_REGULAR_AVS
    elif sku.forecast_method == "E3 Enhanced AVS":
        return ForecastingMethod.E3_ENHANCED_AVS
    elif sku.forecast_method == "Demand Import":
        return ForecastingMethod.DEMAND_IMPORT
    elif sku.forecast_method == "Alternate":
        return ForecastingMethod.ALTERNATE
    else:
        return ForecastingMethod.E3_REGULAR_AVS
    

def update_e3_regular_avs_forecast(sku, forecast_data, demand_history, session):
    """
    Update forecast using E3 Regular AVS method.
    This method uses the tracking signal to weight recent demand history.
    
    Args:
        sku: SKU object
        forecast_data: ForecastData object
        demand_history: DemandHistory object
        session: SQLAlchemy session
    """
    # Get the current forecast and track
    current_forecast = forecast_data.period_forecast or 0
    track = forecast_data.track or 0.10  # Default track if not available
    
    # Get the most recent demand
    recent_demand = demand_history.total_demand
    
    # Calculate the new forecast using the tracking signal
    # Formula: [Track * Most recent demand] + [(1 - Track) * Old forecast]
    new_forecast = (track * recent_demand) + ((1 - track) * current_forecast)
    
    # Update forecast data
    forecast_data.period_forecast = new_forecast
    
    # Calculate weekly and other forecasts based on periodicity
    if sku.forecasting_periodicity == ForecastingPeriodicity.WEEKLY:
        forecast_data.weekly_forecast = new_forecast
        forecast_data.quarterly_forecast = new_forecast * 13  # 13 weeks in a quarter
        forecast_data.yearly_forecast = new_forecast * 52     # 52 weeks in a year
    else:  # 4-weekly
        forecast_data.weekly_forecast = new_forecast / 4      # 4 weeks in a period
        forecast_data.quarterly_forecast = new_forecast * 3   # 3 periods in a quarter
        forecast_data.yearly_forecast = new_forecast * 13     # 13 periods in a year
    
    # Update MADP and track based on recent history
    update_forecast_metrics(sku, forecast_data, session)
    
    # Update the last forecast date
    forecast_data.last_forecast_date = datetime.datetime.now()
    
    logger.info(f"Updated forecast for SKU {sku.sku_id}: {current_forecast} -> {new_forecast}")

def calculate_expected_zero_periods(forecast):
    """
    Calculate the expected number of periods with zero demand based on forecast.
    
    Args:
        forecast: Current forecast value
    
    Returns:
        float: Expected number of zero demand periods
    """
    # Simple model: As forecast decreases, expected zero periods increases
    # This is a simplified approximation - in a real system, this would use
    # statistical probability distributions based on forecast
    if forecast <= 0:
        return 10  # High number of expected zero periods
    elif forecast < 1:
        return 6
    elif forecast < 5:
        return 3
    elif forecast < 10:
        return 2
    else:
        return 1
def calculate_time_since_last_demand(sku, session):
    """
    Calculate the time (in periods) since the last demand occurrence.
    
    Args:
        sku: SKU object
        session: SQLAlchemy session
    
    Returns:
        int: Number of periods since last demand
    """
    current_year, current_period = get_current_period(
        periodicity=sku.forecasting_periodicity
    )
    
    # Get the forecasting demand limit
    forecast_demand_limit = sku.forecasting_demand_limit or 0
    
    # Find the most recent period with demand above the limit
    histories = session.query(DemandHistory).filter(
        DemandHistory.sku_id == sku.id,
        DemandHistory.total_demand >= forecast_demand_limit
    ).order_by(
        DemandHistory.period_year.desc(),
        DemandHistory.period_number.desc()
    ).first()
    
    if not histories:
        return 12  # Default to 12 periods if no history with demand
    
    last_year = histories.period_year
    last_period = histories.period_number
    
    # Calculate periods difference
    if last_year == current_year:
        return current_period - last_period
    else:
        periods_in_year = 13 if sku.forecasting_periodicity == 13 else 52
        return current_period + (periods_in_year - last_period) + ((current_year - last_year - 1) * periods_in_year)

def adjust_forecast_for_time_gap(current_forecast, time_gap):
    """
    Adjust the forecast based on time since last demand.
    
    Args:
        current_forecast: Current forecast value
        time_gap: Time since last demand in periods
    
    Returns:
        float: Adjusted forecast
    """
    # Simple model: Decrease forecast based on time gap
    # The longer the time gap, the more we reduce the forecast
    if time_gap <= 1:
        return current_forecast
    
    # Exponential decay model
    decay_factor = 0.8  # Adjust based on business needs
    adjustment = decay_factor ** (time_gap - 1)
    
    return current_forecast * adjustment

      
def update_e3_enhanced_avs_forecast(sku, forecast_data, demand_history, session):
    """
    Update forecast using E3 Enhanced AVS method.
    This method is optimized for slow-moving or intermittent selling SKUs.
    
    Args:
        sku: SKU object
        forecast_data: ForecastData object
        demand_history: DemandHistory object
        session: SQLAlchemy session
    """
    # Get the current forecast and track
    current_forecast = forecast_data.period_forecast or 0
    track = forecast_data.track or 0.10  # Default track if not available
    
    # Get the most recent demand and forecasting demand limit
    recent_demand = demand_history.total_demand
    forecast_demand_limit = sku.forecasting_demand_limit or 0
    
    # Check if demand is below the forecasting demand limit
    if recent_demand < forecast_demand_limit:
        # Treat as zero demand for Enhanced AVS logic
        logger.debug(f"SKU {sku.sku_id} demand {recent_demand} below limit {forecast_demand_limit}")
        
        # Get the forecast update frequency impact control from company properties
        # This would normally come from Company Properties table
        update_frequency_impact = 2  # Default value, should be retrieved from Company Properties
        
        # Get history records to determine periods with zero demand
        zero_demand_periods = count_consecutive_zero_demand_periods(sku, session)
        
        # Calculate expected zero demand periods based on current forecast
        expected_zero_periods = calculate_expected_zero_periods(current_forecast)
        
        # Check if force reforecast is needed
        force_reforecast_threshold = expected_zero_periods * update_frequency_impact
        
        if zero_demand_periods >= force_reforecast_threshold:
            # Force a reforecast to account for the extended period without demand
            logger.info(f"Forcing reforecast for SKU {sku.sku_id} after {zero_demand_periods} periods of zero/low demand")
            
            # Calculate time since last demand occurrence
            time_since_last_demand = calculate_time_since_last_demand(sku, session)
            
            # Adjust forecast based on time since last demand
            new_forecast = adjust_forecast_for_time_gap(current_forecast, time_since_last_demand)
        else:
            # No need to update forecast as we're within expected zero demand periods
            new_forecast = current_forecast
    else:
        # Standard calculation when demand is above the limit
        new_forecast = (track * recent_demand) + ((1 - track) * current_forecast)
    
    # Update forecast data
    forecast_data.period_forecast = new_forecast
    
    # Calculate weekly and other forecasts based on periodicity
    if sku.forecasting_periodicity == ForecastingPeriodicity.WEEKLY:
        forecast_data.weekly_forecast = new_forecast
        forecast_data.quarterly_forecast = new_forecast * 13
        forecast_data.yearly_forecast = new_forecast * 52
    else:  # 4-weekly
        forecast_data.weekly_forecast = new_forecast / 4
        forecast_data.quarterly_forecast = new_forecast * 3
        forecast_data.yearly_forecast = new_forecast * 13
    
    # Update MADP and track
    update_forecast_metrics(sku, forecast_data, session)
    
    # Update the last forecast date
    forecast_data.last_forecast_date = datetime.datetime.now()
    
    logger.info(f"Updated Enhanced AVS forecast for SKU {sku.sku_id}: {current_forecast} -> {new_forecast}")

def update_alternate_forecast(sku, forecast_data, demand_history, session):
    """
    Update forecast using Alternate forecasting method.
    This advanced method is used for SKUs with special forecasting needs.
    
    Args:
        sku: SKU object
        forecast_data: ForecastData object
        demand_history: DemandHistory object
        session: SQLAlchemy session
    """
    # Alternate forecasting method:
    # 1. Disables MADP (displayed as 99.9)
    # 2. Increases Safety Stock to handle spikes
    # 3. Calculates the necessary IOP and OUTL to achieve the service goal
    
    # For Alternate forecasting, we'll use a different approach to calculate the forecast
    # Get the last 12 months of demand history
    history_data = get_historical_demand(sku, 12, session)
    
    if not history_data:
        logger.warning(f"No historical data available for SKU {sku.sku_id} with Alternate forecasting")
        return
    
    # Calculate total demand and average
    total_demand = sum([h.total_demand for h in history_data])
    avg_demand = total_demand / len(history_data)
    
    # Set the forecast
    forecast_data.period_forecast = avg_demand
    
    # Calculate weekly and other forecasts based on periodicity
    if sku.forecasting_periodicity == ForecastingPeriodicity.WEEKLY:
        forecast_data.weekly_forecast = avg_demand
        forecast_data.quarterly_forecast = avg_demand * 13
        forecast_data.yearly_forecast = avg_demand * 52
    else:  # 4-weekly
        forecast_data.weekly_forecast = avg_demand / 4
        forecast_data.quarterly_forecast = avg_demand * 3
        forecast_data.yearly_forecast = avg_demand * 13
    
    # For Alternate forecasting, set MADP to 99.9
    forecast_data.madp = 99.9
    
    # Update the last forecast date
    forecast_data.last_forecast_date = datetime.datetime.now()
    
    logger.info(f"Updated Alternate forecast for SKU {sku.sku_id} to {avg_demand}")

def update_forecast_metrics(sku, forecast_data, session):
    """
    Update MADP and tracking signal for a SKU based on historical demand.
    
    Args:
        sku: SKU object
        forecast_data: ForecastData object
        session: SQLAlchemy session
    """
    # Get historical demand and forecasts for the SKU
    history_periods = 12  # Use up to 12 periods for calculation
    
    # Get demand history records
    history_data = get_historical_demand(sku, history_periods, session)
    
    if not history_data or len(history_data) < 2:
        logger.debug(f"Insufficient history for SKU {sku.sku_id} to calculate metrics")
        return
    
    # Extract actuals and forecasts for MADP and track calculation
    actuals = [h.total_demand for h in history_data]
    
    # We need historical forecasts that correspond to each period
    # In a real implementation, these would be retrieved from a historical forecasts table
    # For now, we'll use a simplified approach using the current forecast
    forecasts = [forecast_data.period_forecast] * len(actuals)
    
    # Calculate MADP
    madp = calculate_madp(actuals, forecasts)
    forecast_data.madp = madp
    
    # Calculate tracking signal
    track = calculate_tracking_signal(actuals, forecasts)
    forecast_data.track = abs(track)  # Use absolute value for forecast weighting
    
    logger.debug(f"Updated metrics for SKU {sku.sku_id}: MADP={madp}, Track={track}")

   

    """
    Run the period-end forecasting process for all SKUs.
    
    Args:
        session: SQLAlchemy session object
    
    Returns:
        dict: Statistics about the forecasting process
    """
    try:
        logger.info("Starting period-end forecasting process")
        
        # Get all active SKUs (Regular and Watch)
        active_skus = session.query(SKU).filter(
            SKU.buyer_class.in_(['R', 'W'])
        ).all()
        
        stats = {
            'skus_processed': 0,
            'forecasts_updated': 0,
            'errors': 0,
            'demand_filter_high': 0,
            'demand_filter_low': 0,
            'tracking_signal_high': 0,
            'tracking_signal_low': 0
        }
        
        current_year, current_period = get_current_period()
        
        # Process each SKU
        for sku in active_skus:
            try:
                # Skip SKUs with frozen forecasts
                if sku.freeze_forecast_until and sku.freeze_forecast_until > datetime.datetime.now():
                    logger.debug(f"Skipping SKU {sku.sku_id} due to frozen forecast")
                    continue
                
                # Determine the appropriate forecasting method
                forecast_method = get_forecasting_method(sku)
                
                # Get demand history for the previous period
                prev_period = current_period - 1 if current_period > 1 else (13 if sku.forecasting_periodicity == 13 else 52)
                prev_year = current_year if current_period > 1 else current_year - 1
                
                demand_history = session.query(DemandHistory).filter(
                    DemandHistory.sku_id == sku.id,
                    DemandHistory.period_year == prev_year,
                    DemandHistory.period_number == prev_period
                ).first()
                
                # Get current forecast data
                forecast_data = session.query(ForecastData).filter(
                    ForecastData.sku_id == sku.id
                ).first()
                
                if not forecast_data:
                    # Create new forecast data if it doesn't exist
                    forecast_data = ForecastData(
                        sku_id=sku.sku_id,
                        store_id=sku.store_id,
                        sku_store_id=sku.id
                    )
                    session.add(forecast_data)
                
                if demand_history and forecast_data:
                    # Update the forecast based on the method
                    if forecast_method == ForecastingMethod.E3_REGULAR_AVS:
                        update_e3_regular_avs_forecast(sku, forecast_data, demand_history, session)
                    elif forecast_method == ForecastingMethod.E3_ENHANCED_AVS:
                        update_e3_enhanced_avs_forecast(sku, forecast_data, demand_history, session)
                    elif forecast_method == ForecastingMethod.ALTERNATE:
                        update_alternate_forecast(sku, forecast_data, demand_history, session)
                    
                    stats['forecasts_updated'] += 1
                    
                    # Check for exceptions
                    check_forecast_exceptions(sku, forecast_data, demand_history, stats, session)
                
                stats['skus_processed'] += 1
                
            except Exception as e:
                logger.error(f"Error processing SKU {sku.sku_id}: {e}")
                stats['errors'] += 1
        
        session.commit()
        logger.info(f"Period-end forecasting completed: {stats}")
        return stats
        
    except Exception as e:
        logger.error(f"Error in period-end forecasting: {e}")
        session.rollback()
        return {'error': str(e)}
    
    
def get_demand_history(session, sku_id, store_id, years=3):
    """
    Get demand history for a SKU.
    
    Args:
        session: SQLAlchemy session
        sku_id (str): SKU ID
        store_id (str): Store ID
        years (int): Number of years of history to retrieve
    
    Returns:
        dict: Dictionary with period data as keys and demand as values
    """
    try:
        # Get SKU
        sku = session.query(SKU).filter(
            and_(SKU.sku_id == sku_id, SKU.store_id == store_id)
        ).first()
        
        if not sku:
            logger.error(f"SKU {sku_id} not found in store {store_id}")
            return {}
        
        # Get current period
        forecast_data = session.query(ForecastData).filter(
            and_(ForecastData.sku_id == sku_id, ForecastData.store_id == store_id)
        ).first()
        
        # Determine periodicity
        periodicity = getattr(sku, 'forecasting_periodicity', 13)  # Default to 13 (4-weekly)
        
        # Get current period
        current_year, current_period = get_current_period(periodicity)
        
        # Build array of periods to retrieve
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
        
        # Get history for periods
        history_data = {}
        
        for year, period in periods:
            history = session.query(DemandHistory).filter(
                and_(
                    DemandHistory.sku_id == sku_id,
                    DemandHistory.store_id == store_id,
                    DemandHistory.period_year == year,
                    DemandHistory.period_number == period
                )
            ).first()
            
            if history and not history.ignore_history:
                history_data[(year, period)] = history.total_demand
        
        return history_data
    
    except Exception as e:
        logger.error(f"Error getting demand history: {e}")
        return {}

def calculate_e3_regular_avs_forecast(session, sku_id, store_id):
    """
    Calculate a new forecast using the E3 Regular AVS method.
    
    Args:
        session: SQLAlchemy session
        sku_id (str): SKU ID
        store_id (str): Store ID
    
    Returns:
        dict: Dictionary with forecast results
    """
    try:
        # Get demand history
        history_data = get_demand_history(session, sku_id, store_id)
        
        if not history_data:
            logger.warning(f"No demand history found for SKU {sku_id} in store {store_id}")
            return None
        
        # Get current forecast data
        forecast_data = session.query(ForecastData).filter(
            and_(ForecastData.sku_id == sku_id, ForecastData.store_id == store_id)
        ).first()
        
        if not forecast_data:
            logger.warning(f"No forecast data found for SKU {sku_id} in store {store_id}")
            return None
        
        # Get current period
        periodicity = getattr(forecast_data, 'forecasting_periodicity', 13)  # Default to 13 (4-weekly)
        current_year, current_period = get_current_period(periodicity)
        
        # Get previous period
        prev_period = current_period - 1
        prev_year = current_year
        if prev_period < 1:
            prev_period = periodicity
            prev_year = current_year - 1
        
        # Get previous period's demand
        prev_demand = history_data.get((prev_year, prev_period), 0.0)
        
        # Get seasonal profile
        sku = session.query(SKU).filter(
            and_(SKU.sku_id == sku_id, SKU.store_id == store_id)
        ).first()
        
        seasonal_profile = None
        if sku and sku.demand_profile_id:
            seasonal_profile = session.query(SeasonalProfile).filter(
                SeasonalProfile.profile_id == sku.demand_profile_id
            ).first()
        
        # Get seasonal index for previous period
        if seasonal_profile:
            seasonal_index = get_seasonal_index(seasonal_profile, prev_period)
            # Adjust demand for seasonality
            if seasonal_index > 0:
                prev_demand = prev_demand / seasonal_index
        
        # Get track (trend percentage)
        track = forecast_data.track or 0.0
        
        # Calculate new forecast using E3 Regular AVS formula
        old_forecast = forecast_data.period_forecast or 0.0
        
        # Formula: New Forecast = (Track * Newest Demand) + ((1 - Track) * Old Forecast)
        new_forecast = (track * prev_demand) + ((1 - track) * old_forecast)
        
        # Update weekly and other period forecasts
        if periodicity == 13:  # 4-weekly
            weekly_forecast = new_forecast / 4.0
            quarterly_forecast = new_forecast * 3.0
            yearly_forecast = new_forecast * 13.0
        else:  # Weekly
            weekly_forecast = new_forecast
            quarterly_forecast = new_forecast * 13.0
            yearly_forecast = new_forecast * 52.0
        
        # Calculate MADP and Track
        history_values = list(history_data.values())
        forecast_values = [forecast_data.period_forecast] * len(history_values)
        
        # Update forecast values with new forecast for future comparison
        forecast_values[0] = new_forecast
        
        madp = calculate_madp(history_values, forecast_values)
        track = calculate_tracking_signal(history_values, forecast_values)
        
        # Return results
        return {
            'period_forecast': new_forecast,
            'weekly_forecast': weekly_forecast,
            'quarterly_forecast': quarterly_forecast,
            'yearly_forecast': yearly_forecast,
            'madp': madp,
            'track': track
        }
    
    except Exception as e:
        logger.error(f"Error calculating E3 Regular AVS forecast: {e}")
        return None

def calculate_e3_enhanced_avs_forecast(session, sku_id, store_id, forecast_demand_limit=None):
    """
    Calculate a new forecast using the E3 Enhanced AVS method for slow-moving or intermittent items.
    
    The Enhanced AVS method is specifically designed for items with intermittent demand
    patterns where many periods might have zero or very low demand interspersed with
    periods of activity. The algorithm accounts for the time since last demand occurrence
    to produce more accurate forecasts.
    
    Args:
        session: SQLAlchemy session
        sku_id (str): SKU ID
        store_id (str): Store ID
        forecast_demand_limit (float): Optional override for the forecasting demand limit
    
    Returns:
        dict: Dictionary with forecast results or None if insufficient data
    """
    try:
        # Get demand history
        history_data = get_demand_history(session, sku_id, store_id)
        
        if not history_data:
            logger.warning(f"No demand history found for SKU {sku_id} in store {store_id}")
            return None
        
        # Get current forecast data
        forecast_data = session.query(ForecastData).filter(
            and_(ForecastData.sku_id == sku_id, ForecastData.store_id == store_id)
        ).first()
        
        if not forecast_data:
            logger.warning(f"No forecast data found for SKU {sku_id} in store {store_id}")
            return None
        
        # Get the SKU
        sku = session.query(SKU).filter(
            and_(SKU.sku_id == sku_id, SKU.store_id == store_id)
        ).first()
        
        if not sku:
            logger.warning(f"SKU {sku_id} not found in store {store_id}")
            return None
        
        # Get the forecasting demand limit - either from parameter, SKU, source, or company default
        if forecast_demand_limit is None:
            forecast_demand_limit = getattr(sku, 'forecasting_demand_limit', None)
            
            if forecast_demand_limit is None and sku.source:
                forecast_demand_limit = getattr(sku.source, 'forecasting_demand_limit', None)
            
            if forecast_demand_limit is None:
                forecast_demand_limit = ASR_CONFIG.get('forecasting_demand_limit', 0.0)
        
        # Get current period
        periodicity = getattr(forecast_data, 'forecasting_periodicity', 13)  # Default to 13 (4-weekly)
        current_year, current_period = get_current_period(periodicity)
        
        # Get previous period
        prev_period = current_period - 1
        prev_year = current_year
        if prev_period < 1:
            prev_period = periodicity
            prev_year = current_year - 1
        
        # Get previous period's demand
        prev_demand = history_data.get((prev_year, prev_period), 0.0)
        
        # Check if demand exceeds the limit
        if prev_demand <= forecast_demand_limit:
            # For E3 Enhanced, unlike regular E3, we need to check when the last
            # significant demand occurred to correctly adjust the track value
            
            # Initialize tracking variables
            last_significant_demand = None
            last_significant_period = None
            last_significant_year = None
            
            # Find the most recent period with demand > limit by sorting history
            sorted_periods = sorted(history_data.keys(), reverse=True)
            
            for year, period in sorted_periods:
                demand = history_data.get((year, period), 0.0)
                if demand > forecast_demand_limit:
                    last_significant_demand = demand
                    last_significant_period = period
                    last_significant_year = year
                    break
            
            # If no significant demand found in history, don't update the forecast
            if last_significant_demand is None:
                return {
                    'period_forecast': forecast_data.period_forecast,
                    'weekly_forecast': forecast_data.weekly_forecast,
                    'quarterly_forecast': forecast_data.quarterly_forecast,
                    'yearly_forecast': forecast_data.yearly_forecast,
                    'madp': forecast_data.madp,
                    'track': forecast_data.track
                }
            
            # Calculate periods since last significant demand
            if last_significant_year == prev_year:
                periods_since_last_demand = prev_period - last_significant_period
            else:
                periods_since_last_demand = prev_period + (periodicity * (prev_year - last_significant_year)) - last_significant_period
            
            # Don't allow zero or negative periods
            periods_since_last_demand = max(1, periods_since_last_demand)
            
            # Get the update frequency impact control from Company Control Factors
            update_frequency_impact = ASR_CONFIG.get('update_frequency_impact', 0.95)
            
            # Adjust track for slow-moving items:
            # As periods without demand increase, the impact of the last demand decreases
            adjusted_track = forecast_data.track * (update_frequency_impact ** periods_since_last_demand)
            
            # Create forecast response with unchanged values as we didn't update
            return {
                'period_forecast': forecast_data.period_forecast,
                'weekly_forecast': forecast_data.weekly_forecast,
                'quarterly_forecast': forecast_data.quarterly_forecast,
                'yearly_forecast': forecast_data.yearly_forecast,
                'madp': forecast_data.madp,
                'track': adjusted_track  # Only track is adjusted
            }
        
        # Process demand that exceeds the limit - normal reforecast occurs
        
        # Get seasonal profile
        seasonal_profile = None
        if sku.demand_profile_id:
            seasonal_profile = session.query(SeasonalProfile).filter(
                SeasonalProfile.profile_id == sku.demand_profile_id
            ).first()
        
        # Get seasonal index for previous period
        if seasonal_profile:
            from utils.helpers import get_seasonal_index
            seasonal_index = get_seasonal_index(seasonal_profile, prev_period)
            # Adjust demand for seasonality
            if seasonal_index > 0:
                prev_demand = prev_demand / seasonal_index
        
        # Calculate periods since last significant demand
        periods_since_last_demand = 1  # Default to 1 period
        
        # Find the most recent period with demand > limit
        for i, (year, period) in enumerate(sorted_periods[1:], 1):  # Skip current period (index 0)
            demand = history_data.get((year, period), 0.0)
            if demand > forecast_demand_limit:
                # Calculate periods since last demand
                if year == prev_year:
                    periods_since_last_demand = prev_period - period
                else:
                    periods_since_last_demand = prev_period + (periodicity * (prev_year - year)) - period
                break
        
        # Get track (trend percentage)
        track = forecast_data.track or 0.2  # Default track if none exists
        
        # Adjust track based on time since last demand
        # This is a key differentiator of Enhanced E3 AVS - track decreases 
        # exponentially with time since last significant demand
        update_frequency_impact = ASR_CONFIG.get('update_frequency_impact', 0.95)
        adjusted_track = track * (update_frequency_impact ** (periods_since_last_demand - 1))
        
        # Cap the adjusted track to prevent it from getting too small
        min_track = 0.01  # Minimum track value
        adjusted_track = max(min_track, adjusted_track)
        
        # Calculate new forecast using E3 Enhanced AVS formula
        old_forecast = forecast_data.period_forecast or 0.0
        
        # Formula with adjustment for time between demands
        new_forecast = (adjusted_track * prev_demand) + ((1 - adjusted_track) * old_forecast)
        
        # Update weekly and other period forecasts
        if periodicity == 13:  # 4-weekly
            weekly_forecast = new_forecast / 4.0
            quarterly_forecast = new_forecast * 3.0
            yearly_forecast = new_forecast * 13.0
        else:  # Weekly
            weekly_forecast = new_forecast
            quarterly_forecast = new_forecast * 13.0
            yearly_forecast = new_forecast * 52.0
        
        # Calculate MADP and Track
        history_values = list(history_data.values())
        forecast_values = [forecast_data.period_forecast] * len(history_values)
        
        # Update forecast values with new forecast for future comparison
        forecast_values[0] = new_forecast
        
        # Calculate updated MADP and track
        from utils.helpers import calculate_madp, calculate_tracking_signal
        madp = calculate_madp(history_values, forecast_values)
        track = calculate_tracking_signal(history_values, forecast_values)
        
        # Return results
        return {
            'period_forecast': new_forecast,
            'weekly_forecast': weekly_forecast,
            'quarterly_forecast': quarterly_forecast,
            'yearly_forecast': yearly_forecast,
            'madp': madp,
            'track': track,
            'periods_since_demand': periods_since_last_demand
        }
    
    except Exception as e:
        logger.error(f"Error calculating E3 Enhanced AVS forecast: {e}")
        return None

def calculate_initial_forecast(session, sku_id, store_id, starting_forecast=None):
    """
    Calculate an initial forecast for a new SKU.
    
    Args:
        session: SQLAlchemy session
        sku_id (str): SKU ID
        store_id (str): Store ID
        starting_forecast (float): Optional starting forecast value
    
    Returns:
        dict: Dictionary with forecast results
    """
    try:
        # Get SKU
        sku = session.query(SKU).filter(
            and_(SKU.sku_id == sku_id, SKU.store_id == store_id)
        ).first()
        
        if not sku:
            logger.error(f"SKU {sku_id} not found in store {store_id}")
            return None
        
        # Get forecast periodicity
        periodicity = getattr(sku, 'forecasting_periodicity', 13)  # Default to 13 (4-weekly)
        
        # If starting forecast is provided, use it
        if starting_forecast is not None:
            period_forecast = starting_forecast
        else:
            # No starting forecast provided, try to estimate from similar SKUs
            similar_skus = session.query(SKU).filter(
                and_(
                    SKU.source_id == sku.source_id,
                    SKU.store_id == store_id,
                    SKU.id != sku.id,
                    SKU.system_class != 'U',  # Not uninitialized
                    SKU.buyer_class.in_(['R', 'W'])  # Regular or Watch
                )
            ).all()
            
            if similar_skus:
                # Get forecast data for similar SKUs
                sku_ids = [s.id for s in similar_skus]
                forecast_data = session.query(ForecastData).filter(
                    ForecastData.sku_id.in_(sku_ids)
                ).all()
                
                if forecast_data:
                    # Calculate average forecast
                    avg_forecast = sum(f.period_forecast for f in forecast_data) / len(forecast_data)
                    period_forecast = avg_forecast
                else:
                    # No forecast data for similar SKUs, use default
                    period_forecast = 1.0
            else:
                # No similar SKUs, use default
                period_forecast = 1.0
        
        # Calculate weekly and other period forecasts
        if periodicity == 13:  # 4-weekly
            weekly_forecast = period_forecast / 4.0
            quarterly_forecast = period_forecast * 3.0
            yearly_forecast = period_forecast * 13.0
        else:  # Weekly
            weekly_forecast = period_forecast
            quarterly_forecast = period_forecast * 13.0
            yearly_forecast = period_forecast * 52.0
        
        # For new SKUs, set default MADP and track
        madp = 30.0  # Default MADP
        track = 0.2   # Default track
        
        # Return results
        return {
            'period_forecast': period_forecast,
            'weekly_forecast': weekly_forecast,
            'quarterly_forecast': quarterly_forecast,
            'yearly_forecast': yearly_forecast,
            'madp': madp,
            'track': track
        }
    
    except Exception as e:
        logger.error(f"Error calculating initial forecast: {e}")
        return None

def update_forecast_data(session, sku_id, store_id, forecast_results, manual=False):
    """
    Update forecast data for a SKU.
    
    Args:
        session: SQLAlchemy session
        sku_id (str): SKU ID
        store_id (str): Store ID
        forecast_results (dict): Dictionary with forecast results
        manual (bool): True if manual forecast update
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Get forecast data
        forecast_data = session.query(ForecastData).filter(
            and_(ForecastData.sku_id == sku_id, ForecastData.store_id == store_id)
        ).first()
        
        if not forecast_data:
            # Create new forecast data
            forecast_data = ForecastData(
                sku_id=sku_id,
                store_id=store_id
            )
            session.add(forecast_data)
        
        # Update forecast data
        forecast_data.period_forecast = forecast_results['period_forecast']
        forecast_data.weekly_forecast = forecast_results['weekly_forecast']
        forecast_data.quarterly_forecast = forecast_results['quarterly_forecast']
        forecast_data.yearly_forecast = forecast_results['yearly_forecast']
        forecast_data.madp = forecast_results['madp']
        forecast_data.track = forecast_results['track']
        forecast_data.last_forecast_date = datetime.now()
        
        if manual:
            forecast_data.last_manual_forecast_date = datetime.now()
        
        # Update system class for SKU
        sku = session.query(SKU).filter(
            and_(SKU.sku_id == sku_id, SKU.store_id == store_id)
        ).first()
        
        if sku:
            # Update system class based on forecast values
            if sku.system_class == 'U':  # Uninitialized
                sku.system_class = 'N'  # New
            elif sku.system_class == 'N':  # Keep as New until 6 months old
                # Only change if SKU is more than 6 months old
                if sku.created_at and (datetime.now() - sku.created_at).days > 180:
                    # Check MADP for Lumpy
                    madp_high_threshold = ASR_CONFIG.get('madp_high_threshold', 50)
                    if forecast_results['madp'] > madp_high_threshold:
                        sku.system_class = 'L'  # Lumpy
                    else:
                        # Check for slow mover
                        slow_mover_limit = ASR_CONFIG.get('slow_mover_limit', 5)
                        if forecast_results['yearly_forecast'] < slow_mover_limit:
                            sku.system_class = 'S'  # Slow
                        else:
                            sku.system_class = 'R'  # Regular
            elif sku.system_class != 'A':  # Don't change Alternate
                # Check MADP for Lumpy
                madp_high_threshold = ASR_CONFIG.get('madp_high_threshold', 50)
                if forecast_results['madp'] > madp_high_threshold:
                    sku.system_class = 'L'  # Lumpy
                else:
                    # Check for slow mover
                    slow_mover_limit = ASR_CONFIG.get('slow_mover_limit', 5)
                    if forecast_results['yearly_forecast'] < slow_mover_limit:
                        sku.system_class = 'S'  # Slow
                    else:
                        sku.system_class = 'R'  # Regular
        
        # Commit changes
        session.commit()
        return True
    
    except Exception as e:
        logger.error(f"Error updating forecast data: {e}")
        session.rollback()
        return False


    """
    Run period-end forecasting for all SKUs.
    
    Args:
        session: SQLAlchemy session
    
    Returns:
        dict: Statistics about the forecasting run
    """
    try:
        # Get active SKUs
        skus = session.query(SKU).filter(
            SKU.buyer_class.in_(['R', 'W'])
        ).all()
        
        # Statistics
        stats = {
            'total_skus': len(skus),
            'e3_regular_avs': 0,
            'e3_enhanced_avs': 0,
            'demand_import': 0,
            'e3_alternate': 0,
            'frozen_forecast': 0,
            'no_update': 0,
            'errors': 0
        }
        
        # Process each SKU
        for sku in skus:
            try:
                # Check for frozen forecast
                if hasattr(sku, 'freeze_forecast_until') and sku.freeze_forecast_until:
                    if sku.freeze_forecast_until > datetime.now():
                        # Skip this SKU if forecast is frozen
                        stats['frozen_forecast'] += 1
                        continue
                
                # Determine forecast method
                forecast_method = getattr(sku, 'forecast_method', 'E3 Regular AVS')
                
                if forecast_method == 'E3 Regular AVS':
                    # Calculate forecast using E3 Regular AVS
                    forecast_results = calculate_e3_regular_avs_forecast(session, sku.sku_id, sku.store_id)
                    if forecast_results:
                        update_forecast_data(session, sku.sku_id, sku.store_id, forecast_results)
                        stats['e3_regular_avs'] += 1
                    else:
                        stats['no_update'] += 1
                
                elif forecast_method == 'E3 Enhanced AVS':
                    # Calculate forecast using E3 Enhanced AVS
                    forecast_results = calculate_e3_enhanced_avs_forecast(session, sku.sku_id, sku.store_id)
                    if forecast_results:
                        update_forecast_data(session, sku.sku_id, sku.store_id, forecast_results)
                        stats['e3_enhanced_avs'] += 1
                    else:
                        stats['no_update'] += 1
                
                elif forecast_method == 'Demand Import':
                    # Skip SKUs with imported demand
                    stats['demand_import'] += 1
                
                elif forecast_method == 'E3 Alternate':
                    # Skip SKUs with alternate forecast method
                    stats['e3_alternate'] += 1
                
                else:
                    # Unknown forecast method
                    stats['no_update'] += 1
            
            except Exception as e:
                logger.error(f"Error forecasting SKU {sku.sku_id}: {e}")
                stats['errors'] += 1
        
        return stats
    
    except Exception as e:
        logger.error(f"Error running period-end forecasting: {e}")
        return {'error': str(e)}

def simulate_seasonal_profile(session, sku_id, store_id, ignore_years=None):
    """
    Simulate a seasonal profile for a SKU.
    
    Args:
        session: SQLAlchemy session
        sku_id (str): SKU ID
        store_id (str): Store ID
        ignore_years (list): List of years to ignore in calculation
    
    Returns:
        dict: Dictionary with seasonal indices
    """
    try:
        # Get demand history
        history_data = get_demand_history(session, sku_id, store_id)
        
        if not history_data:
            logger.warning(f"No demand history found for SKU {sku_id} in store {store_id}")
            return None
        
        # Determine periodicity
        sku = session.query(SKU).filter(
            and_(SKU.sku_id == sku_id, SKU.store_id == store_id)
        ).first()
        
        periodicity = getattr(sku, 'forecasting_periodicity', 13)  # Default to 13 (4-weekly)
        
        # Filter out ignored years
        if ignore_years:
            history_data = {k: v for k, v in history_data.items() if k[0] not in ignore_years}
        
        # Group by period
        period_data = {}
        for (year, period), demand in history_data.items():
            if period not in period_data:
                period_data[period] = []
            period_data[period].append(demand)
        
        # Calculate average demand per period
        period_averages = {}
        for period, demands in period_data.items():
            if demands:
                period_averages[period] = sum(demands) / len(demands)
        
        if not period_averages:
            logger.warning(f"No valid period averages found for SKU {sku_id} in store {store_id}")
            return None
        
        # Calculate average demand across all periods
        all_demands = [d for demands in period_data.values() for d in demands]
        overall_average = sum(all_demands) / len(all_demands) if all_demands else 0.0
        
        if overall_average == 0.0:
            logger.warning(f"Overall average demand is zero for SKU {sku_id} in store {store_id}")
            return None
        
        # Calculate seasonal indices
        seasonal_indices = {}
        for period in range(1, periodicity + 1):
            if period in period_averages and period_averages[period] > 0:
                seasonal_indices[period] = period_averages[period] / overall_average
            else:
                seasonal_indices[period] = 1.0  # Default to 1.0 if no data
        
        return seasonal_indices
    
    except Exception as e:
        logger.error(f"Error simulating seasonal profile: {e}")
        return None

def create_seasonal_profile(session, profile_id, seasonal_indices, description=None):
    """
    Create a seasonal profile.
    
    Args:
        session: SQLAlchemy session
        profile_id (str): Profile ID
        seasonal_indices (dict): Dictionary with seasonal indices
        description (str): Optional profile description
    
    Returns:
        SeasonalProfile: Created profile object
    """
    try:
        # Check if profile already exists
        profile = session.query(SeasonalProfile).filter(
            SeasonalProfile.profile_id == profile_id
        ).first()
        
        if not profile:
            # Create new profile
            profile = SeasonalProfile(
                profile_id=profile_id,
                description=description or f"Profile {profile_id}"
            )
            session.add(profile)
        
        # Update profile indices
        for period, index in seasonal_indices.items():
            # Set field based on period number
            setattr(profile, f"p{period}_index", index)
        
        # Commit changes
        session.commit()
        return profile
    
    except Exception as e:
        logger.error(f"Error creating seasonal profile: {e}")
        session.rollback()
        return None


    """
    Apply a seasonal profile to a SKU.
    
    Args:
        session: SQLAlchemy session
        sku_id (str): SKU ID
        store_id (str): Store ID
        profile_id (str): Profile ID
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Get SKU
        sku = session.query(SKU).filter(
            and_(SKU.sku_id == sku_id, SKU.store_id == store_id)
        ).first()
        
        if not sku:
            logger.error(f"SKU {sku_id} not found in store {store_id}")
            return False
        
        # Check if profile exists
        profile = session.query(SeasonalProfile).filter(
            SeasonalProfile.profile_id == profile_id
        ).first()
        
        if not profile:
            logger.error(f"Profile {profile_id} not found")
            return False
        
        # Apply profile to SKU
        sku.demand_profile_id = profile_id
        
        # Reforecast with seasonal profile
        # Get forecast data
        forecast_data = session.query(ForecastData).filter(
            and_(ForecastData.sku_id == sku_id, ForecastData.store_id == store_id)
        ).first()
        
        if forecast_data:
            # Get current period
            periodicity = getattr(sku, 'forecasting_periodicity', 13)  # Default to 13 (4-weekly)
            current_year, current_period = get_current_period(periodicity)
            
            # Get seasonal index for current period
            seasonal_index = get_seasonal_index(profile, current_period)
            
            # Adjust forecast for seasonality
            if seasonal_index != 0:
                new_period_forecast = forecast_data.period_forecast / seasonal_index
                
                # Update forecast data
                forecast_data.period_forecast = new_period_forecast
                
                # Update other forecast periods
                if periodicity == 13:  # 4-weekly
                    forecast_data.weekly_forecast = new_period_forecast / 4.0
                    forecast_data.quarterly_forecast = new_period_forecast * 3.0
                    forecast_data.yearly_forecast = new_period_forecast * 13.0
                else:  # Weekly
                    forecast_data.weekly_forecast = new_period_forecast
                    forecast_data.quarterly_forecast = new_period_forecast * 13.0
                    forecast_data.yearly_forecast = new_period_forecast * 52.0
        
        # Commit changes
        session.commit()
        return True
    
    except Exception as e:
        logger.error(f"Error applying seasonal profile: {e}")
        session.rollback()
        return False

def get_seasonal_index_for_period(profile_id, period_number):
    """
    Get seasonal index for a specific period.
    
    Args:
        profile_id (str): Profile ID
        period_number (int): Period number
    
    Returns:
        float: Seasonal index
    """
    session = get_session()
    try:
        # Get profile
        profile = session.query(SeasonalProfile).filter(
            SeasonalProfile.profile_id == profile_id
        ).first()
        
        if not profile:
            logger.error(f"Profile {profile_id} not found")
            return 1.0  # Default to 1.0 if profile not found
        
        # Get index for period
        return get_seasonal_index(profile, period_number)
    
    except Exception as e:
        logger.error(f"Error getting seasonal index: {e}")
        return 1.0  # Default to 1.0 on error
    
    finally:
        session.close()
        
def analyze_intermittent_demand_pattern(session, sku_id, store_id, years=3):
    """
    Analyze a SKU's demand pattern to determine if it's intermittent.
    
    Args:
        session: SQLAlchemy session
        sku_id (str): SKU ID
        store_id (str): Store ID
        years (int): Number of years of history to analyze
    
    Returns:
        dict: Analysis results including intermittence metrics
    """
    try:
        # Get demand history
        history_data = get_demand_history(session, sku_id, store_id, years)
        
        if not history_data:
            return {
                'is_intermittent': False,
                'reason': 'insufficient_history',
                'zero_demand_periods': 0,
                'total_periods': 0,
                'zero_demand_percentage': 0.0,
                'coefficient_of_variation': 0.0,
                'average_demand_interval': 0.0
            }
        
        # Count periods with zero demand
        zero_demand_periods = sum(1 for demand in history_data.values() if demand == 0)
        total_periods = len(history_data)
        
        # Calculate zero demand percentage
        zero_demand_percentage = (zero_demand_periods / total_periods) * 100 if total_periods > 0 else 0
        
        # Calculate demand values
        demand_values = list(history_data.values())
        
        # Calculate coefficient of variation (CV)
        mean_demand = sum(demand_values) / len(demand_values) if demand_values else 0
        
        if mean_demand > 0:
            variance = sum((d - mean_demand) ** 2 for d in demand_values) / len(demand_values)
            std_dev = math.sqrt(variance)
            coefficient_of_variation = std_dev / mean_demand
        else:
            coefficient_of_variation = 0.0
        
        # Calculate average demand interval
        if zero_demand_periods < total_periods:  # Avoid division by zero
            average_demand_interval = total_periods / (total_periods - zero_demand_periods)
        else:
            average_demand_interval = float('inf')  # All periods have zero demand
        
        # Determine if the pattern is intermittent
        # Standard classification criteria:
        # - High zero_demand_percentage (>30%)
        # - High coefficient_of_variation (>1.0)
        # - High average_demand_interval (>1.3)
        
        is_intermittent = (
            zero_demand_percentage >= 30.0 or 
            coefficient_of_variation >= 1.0 or 
            average_demand_interval >= 1.3
        )
        
        return {
            'is_intermittent': is_intermittent,
            'zero_demand_periods': zero_demand_periods,
            'total_periods': total_periods,
            'zero_demand_percentage': zero_demand_percentage,
            'coefficient_of_variation': coefficient_of_variation,
            'average_demand_interval': average_demand_interval,
            'mean_demand': mean_demand
        }
    
    except Exception as e:
        logger.error(f"Error analyzing intermittent demand pattern: {e}")
        return {
            'is_intermittent': False,
            'error': str(e)
        }

def recommend_forecast_method(session, sku_id, store_id):
    """
    Recommend the appropriate forecasting method for a SKU based on its demand pattern.
    
    Args:
        session: SQLAlchemy session
        sku_id (str): SKU ID
        store_id (str): Store ID
    
    Returns:
        dict: Recommendation including recommended method and analysis
    """
    try:
        # Analyze the demand pattern
        analysis = analyze_intermittent_demand_pattern(session, sku_id, store_id)
        
        # Get the current forecast method
        sku = session.query(SKU).filter(
            and_(SKU.sku_id == sku_id, SKU.store_id == store_id)
        ).first()
        
        if not sku:
            return {
                'recommended_method': 'unknown',
                'current_method': 'unknown',
                'reason': 'sku_not_found'
            }
        
        # Get current method
        current_method = getattr(sku, 'forecast_method', 'E3 Regular AVS')
        
        # Determine recommended method based on analysis
        recommended_method = current_method  # Default to keeping current method
        reason = 'no_change_needed'
        
        # Check if item has a seasonal profile
        has_seasonal_profile = sku.demand_profile_id is not None
        
        # Use enhanced AVS for intermittent patterns
        if analysis['is_intermittent']:
            recommended_method = 'E3 Enhanced AVS'
            reason = 'intermittent_demand'
        
        # If very slow moving (very low mean demand), recommend Enhanced AVS
        elif analysis.get('mean_demand', 0) < ASR_CONFIG.get('slow_mover_limit', 5):
            recommended_method = 'E3 Enhanced AVS'
            reason = 'slow_moving'
        
        # If item has a seasonal profile, check if there's enough consistent history
        elif has_seasonal_profile and analysis.get('zero_demand_percentage', 0) > 15:
            # May need to reconsider seasonal profile if many zero periods
            reason = 'seasonal_with_gaps'
        
        # If regular AVS is appropriate
        elif not analysis['is_intermittent'] and analysis.get('coefficient_of_variation', 0) < 0.5:
            recommended_method = 'E3 Regular AVS'
            reason = 'stable_demand'
        
        return {
            'recommended_method': recommended_method,
            'current_method': current_method,
            'reason': reason,
            'analysis': analysis,
            'has_seasonal_profile': has_seasonal_profile
        }
    
    except Exception as e:
        logger.error(f"Error recommending forecast method: {e}")
        return {
            'recommended_method': 'E3 Regular AVS',  # Default to regular
            'current_method': 'unknown',
            'reason': 'error',
            'error': str(e)
        }

def apply_recommended_forecast_methods(session, buyer_id=None, store_id=None, source_id=None, 
                                       auto_apply=False, threshold=0.9):
    """
    Analyze and recommend forecast methods for a group of SKUs.
    
    Args:
        session: SQLAlchemy session
        buyer_id (str): Filter by buyer ID
        store_id (str): Filter by store ID
        source_id (str): Filter by source ID
        auto_apply (bool): Whether to automatically apply recommendations
        threshold (float): Confidence threshold for auto-application (0.0-1.0)
    
    Returns:
        dict: Results of the analysis
    """
    try:
        # Build query for SKUs
        query = session.query(SKU)
        
        # Apply filters
        if buyer_id:
            query = query.join(SKU.source).filter(Source.buyer_id == buyer_id)
        
        if store_id:
            query = query.filter(SKU.store_id == store_id)
        
        if source_id:
            query = query.join(SKU.source).filter(Source.source_id == source_id)
        
        # Get active SKUs
        skus = query.filter(SKU.buyer_class.in_(['R', 'W'])).all()
        
        # Results
        results = {
            'total_skus': len(skus),
            'analyzed': 0,
            'recommended_changes': 0,
            'auto_applied': 0,
            'errors': 0,
            'recommendations': []
        }
        
        # Process each SKU
        for sku in skus:
            try:
                # Get recommendation
                recommendation = recommend_forecast_method(session, sku.sku_id, sku.store_id)
                results['analyzed'] += 1
                
                # Check if recommendation differs from current
                if recommendation['recommended_method'] != recommendation['current_method']:
                    results['recommended_changes'] += 1
                    
                    # Determine confidence - this is a simplified example
                    # In a real implementation, this would be based on multiple factors
                    confidence = 0.0
                    
                    if recommendation['reason'] == 'intermittent_demand':
                        # Calculate confidence based on intermittence metrics
                        analysis = recommendation['analysis']
                        
                        if analysis['zero_demand_percentage'] > 50:
                            confidence = 0.9
                        elif analysis['coefficient_of_variation'] > 1.5:
                            confidence = 0.85
                        elif analysis['average_demand_interval'] > 2.0:
                            confidence = 0.8
                        else:
                            confidence = 0.7
                    
                    elif recommendation['reason'] == 'slow_moving':
                        confidence = 0.85
                    
                    elif recommendation['reason'] == 'stable_demand':
                        confidence = 0.75
                    
                    # Add to recommendations list
                    recommendation_record = {
                        'sku_id': sku.sku_id,
                        'store_id': sku.store_id,
                        'current_method': recommendation['current_method'],
                        'recommended_method': recommendation['recommended_method'],
                        'reason': recommendation['reason'],
                        'confidence': confidence,
                        'applied': False
                    }
                    
                    # Auto-apply if enabled and confidence is high enough
                    if auto_apply and confidence >= threshold:
                        try:
                            # Update forecast method
                            sku.forecast_method = recommendation['recommended_method']
                            recommendation_record['applied'] = True
                            results['auto_applied'] += 1
                        except Exception as e:
                            logger.error(f"Error applying forecast method: {e}")
                            recommendation_record['error'] = str(e)
                    
                    results['recommendations'].append(recommendation_record)
            
            except Exception as e:
                logger.error(f"Error analyzing SKU {sku.sku_id}: {e}")
                results['errors'] += 1
        
        # Commit changes if auto-apply is enabled
        if auto_apply and results['auto_applied'] > 0:
            session.commit()
        
        return results
    
    except Exception as e:
        logger.error(f"Error applying recommended forecast methods: {e}")
        if auto_apply:
            session.rollback()
        return {'error': str(e)}

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
    
    if not sku or not sku.demand_profile_id:
        return {
            'sku_id': sku_id,
            'store_id': store_id,
            'profile_applied': False,
            'reason': 'no_profile'
        }
    
    # Get the seasonal profile
    profile = session.query(SeasonalProfile).filter(
        SeasonalProfile.profile_id == sku.demand_profile_id
    ).first()
    
    if not profile:
        return {
            'sku_id': sku_id,
            'store_id': store_id,
            'profile_applied': False,
            'reason': 'profile_not_found'
        }
    
    # Get the forecast data
    forecast_data = session.query(ForecastData).filter(
        and_(
            ForecastData.sku_id == sku_id,
            ForecastData.store_id == store_id
        )
    ).first()
    
    if not forecast_data:
        return {
            'sku_id': sku_id,
            'store_id': store_id,
            'profile_applied': False,
            'reason': 'no_forecast_data'
        }
    
    # Get current period
    current_year, current_period = get_current_period(
        periodicity=13 if hasattr(sku, 'forecast_periodicity') and sku.forecast_periodicity == 13 else 52
    )
    
    # Get the seasonal index for the current period
    index_attr = f"p{current_period}_index"
    if hasattr(profile, index_attr):
        seasonal_index = getattr(profile, index_attr)
    else:
        seasonal_index = 1.0  # Default if no index found
    
    # Store the old forecast for reference
    old_forecast = {
        'weekly': forecast_data.weekly_forecast,
        'period': forecast_data.period_forecast,
        'quarterly': forecast_data.quarterly_forecast,
        'yearly': forecast_data.yearly_forecast
    }
    
    # Apply the seasonal index to the forecast
    if sku.forecast_periodicity == 13:
        # For 4-weekly forecasting
        deseasonalized_forecast = forecast_data.period_forecast / seasonal_index
        forecast_data.period_forecast = deseasonalized_forecast * seasonal_index
        forecast_data.weekly_forecast = forecast_data.period_forecast / 4
    else:
        # For weekly forecasting
        deseasonalized_forecast = forecast_data.weekly_forecast / seasonal_index
        forecast_data.weekly_forecast = deseasonalized_forecast * seasonal_index
        forecast_data.period_forecast = forecast_data.weekly_forecast * 4
    
    # Update quarterly and yearly forecasts
    if sku.forecast_periodicity == 13:
        forecast_data.quarterly_forecast = forecast_data.period_forecast * 3
        forecast_data.yearly_forecast = forecast_data.period_forecast * 13
    else:
        forecast_data.quarterly_forecast = forecast_data.weekly_forecast * 13
        forecast_data.yearly_forecast = forecast_data.weekly_forecast * 52
    
    # Save changes
    session.commit()
    
    return {
        'sku_id': sku_id,
        'store_id': store_id,
        'profile_applied': True,
        'profile_id': sku.demand_profile_id,
        'seasonal_index': seasonal_index,
        'current_period': current_period,
        'deseasonalized_forecast': deseasonalized_forecast,
        'old_forecast': old_forecast,
        'new_forecast': {
            'weekly': forecast_data.weekly_forecast,
            'period': forecast_data.period_forecast,
            'quarterly': forecast_data.quarterly_forecast,
            'yearly': forecast_data.yearly_forecast
        }
    }
    
    """
    Apply seasonal profile adjustments to the forecast.
    
    Args:
        sku: SKU object
        forecast_data: ForecastData object
        session: SQLAlchemy session
    """
    # Check if SKU has a seasonal profile
    if not sku.demand_profile_id:
        return
    
    # Get the seasonal profile
    profile = session.query(SeasonalProfile).filter(
        SeasonalProfile.profile_id == sku.demand_profile_id
    ).first()
    
    if not profile:
        logger.warning(f"Seasonal profile {sku.demand_profile_id} not found for SKU {sku.sku_id}")
        return
    
    # Get current period
    _, current_period = get_current_period(
        periodicity=sku.forecasting_periodicity
    )
    
    # Get the seasonal index for the current period
    seasonal_index = get_seasonal_index(profile, current_period)
    
    # Adjust forecast with seasonal index
    # The base forecast is the deseasonalized forecast
    base_forecast = forecast_data.period_forecast
    seasonal_forecast = base_forecast * seasonal_index
    
    # Update the forecast with seasonal adjustment
    forecast_data.period_forecast = seasonal_forecast
    
    # Adjust weekly forecast as well
    if sku.forecasting_periodicity == ForecastingPeriodicity.WEEKLY:
        forecast_data.weekly_forecast = seasonal_forecast
    else:
        forecast_data.weekly_forecast = seasonal_forecast / 4
    
    logger.debug(f"Applied seasonal index {seasonal_index} to SKU {sku.sku_id}: {base_forecast} -> {seasonal_forecast}")

def get_seasonal_index(profile, period_number):
    """
    Get the seasonal index for a period from a seasonal profile.
    
    Args:
        profile: Seasonal profile object
        period_number: Period number (1-13)
    
    Returns:
        float: Seasonal index
    """
    # Map period number to profile attribute
    attr_name = f"p{period_number}_index"
    
    # Return the index or 1.0 if not found
    return getattr(profile, attr_name, 1.0)

def manually_update_forecast(sku, new_forecast, session):
    """
    Manually update the forecast for a SKU.
    
    Args:
        sku: SKU object
        new_forecast: New forecast value
        session: SQLAlchemy session
    
    Returns:
        ForecastData: Updated forecast data
    """
    # Get current forecast data
    forecast_data = session.query(ForecastData).filter(
        ForecastData.sku_id == sku.id
    ).first()
    
    if not forecast_data:
        # Create new forecast data if it doesn't exist
        forecast_data = ForecastData(
            sku_id=sku.sku_id,
            store_id=sku.store_id,
            sku_store_id=sku.id
        )
        session.add(forecast_data)
    
    # Update the period forecast
    forecast_data.period_forecast = new_forecast
    
    # Calculate weekly and other forecasts based on periodicity
    if sku.forecasting_periodicity == ForecastingPeriodicity.WEEKLY:
        forecast_data.weekly_forecast = new_forecast
        forecast_data.quarterly_forecast = new_forecast * 13
        forecast_data.yearly_forecast = new_forecast * 52
    else:  # 4-weekly
        forecast_data.weekly_forecast = new_forecast / 4
        forecast_data.quarterly_forecast = new_forecast * 3
        forecast_data.yearly_forecast = new_forecast * 13
    
    # Update the last manual forecast date
    forecast_data.last_manual_forecast_date = datetime.datetime.now()
    
    session.commit()
    
    logger.info(f"Manually updated forecast for SKU {sku.sku_id} to {new_forecast}")
    
    return forecast_data

def freeze_forecast(sku, freeze_until_date, session):
    """
    Freeze a SKU's forecast until a specific date.
    
    Args:
        sku: SKU object
        freeze_until_date: Date until which to freeze the forecast
        session: SQLAlchemy session
    """
    # Update the freeze date
    sku.freeze_forecast_until = freeze_until_date
    
    session.commit()
    
    logger.info(f"Frozen forecast for SKU {sku.sku_id} until {freeze_until_date}")

def thaw_forecast(sku, session):
    """
    Thaw a SKU's forecast (remove freeze).
    
    Args:
        sku: SKU object
        session: SQLAlchemy session
    """
    # Remove the freeze date
    sku.freeze_forecast_until = None
    
    session.commit()
    
    logger.info(f"Thawed forecast for SKU {sku.sku_id}")

def initialize_new_sku(sku, initial_forecast, session):
    """
    Initialize a new SKU with a starting forecast.
    
    Args:
        sku: SKU object
        initial_forecast: Initial forecast value
        session: SQLAlchemy session
    """
    # Update the SKU's system class from Uninitialized to New
    sku.system_class = 'N'
    
    # Create forecast data
    forecast_data = ForecastData(
        sku_id=sku.sku_id,
        store_id=sku.store_id,
        sku_store_id=sku.id,
        period_forecast=initial_forecast,
        weekly_forecast=initial_forecast / 4 if sku.forecasting_periodicity == 13 else initial_forecast,
        quarterly_forecast=initial_forecast * 3 if sku.forecasting_periodicity == 13 else initial_forecast * 13,
        yearly_forecast=initial_forecast * 13 if sku.forecasting_periodicity == 13 else initial_forecast * 52,
        last_forecast_date=datetime.datetime.now(),
        last_manual_forecast_date=datetime.datetime.now(),
        madp=30.0,  # Default MADP for new SKUs
        track=0.10   # Default track for new SKUs
    )
    
    session.add(forecast_data)
    session.commit()
    
    logger.info(f"Initialized new SKU {sku.sku_id} with forecast {initial_forecast}")

def create_seasonal_profile(sku, composite_data, profile_name, session):
    """
    Create a seasonal profile for a SKU.
    
    Args:
        sku: SKU object
        composite_data: Composite data for the seasonal profile (list of values)
        profile_name: Name for the profile
        session: SQLAlchemy session
    
    Returns:
        SeasonalProfile: Created seasonal profile
    """
    # Calculate indices based on composite data
    avg_value = sum(composite_data) / len(composite_data)
    indices = [val / avg_value for val in composite_data]
    
    # Create profile
    profile = SeasonalProfile(
        profile_id=profile_name,
        description=f"Profile for {sku.sku_id}"
    )
    
    # Set indices based on periodicity
    if sku.forecasting_periodicity == ForecastingPeriodicity.FOUR_WEEKLY:
        # 13 periods
        for i in range(min(13, len(indices))):
            setattr(profile, f"p{i+1}_index", indices[i])
    else:
        # 52 weeks - we'll just use the first 13 for this example
        # In a full implementation, all 52 would be set
        for i in range(min(13, len(indices))):
            setattr(profile, f"p{i+1}_index", indices[i])
    
    session.add(profile)
    session.commit()
    
    # Assign profile to SKU
    sku.demand_profile_id = profile.profile_id
    session.commit()
    
    logger.info(f"Created seasonal profile {profile.profile_id} for SKU {sku.sku_id}")
    
    return profile

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

def detect_demand_pattern(session, sku_id, store_id):
    """
    Detect the demand pattern for a SKU.
    
    This function analyzes the SKU's demand history and classifies
    it as Low MADP, High MADP, Trending, or potentially Seasonal.
    
    Args:
        session: SQLAlchemy session
        sku_id (str): SKU ID
        store_id (str): Store ID
    
    Returns:
        dict: Detected pattern information
    """
    # Get the SKU and its forecast data
    sku = session.query(SKU).filter(
        and_(
            SKU.sku_id == sku_id,
            SKU.store_id == store_id
        )
    ).first()
    
    if not sku:
        return {
            'sku_id': sku_id,
            'store_id': store_id,
            'pattern_detected': False,
            'reason': 'sku_not_found'
        }
    
    forecast_data = session.query(ForecastData).filter(
        and_(
            ForecastData.sku_id == sku_id,
            ForecastData.store_id == store_id
        )
    ).first()
    
    if not forecast_data:
        return {
            'sku_id': sku_id,
            'store_id': store_id,
            'pattern_detected': False,
            'reason': 'no_forecast_data'
        }
    
    # Collect demand history
    periodicity = 13 if hasattr(sku, 'forecast_periodicity') and sku.forecast_periodicity == 13 else 52
    actuals, forecasts, periods_list = collect_demand_history(
        session, sku_id, store_id, periods=24, periodicity=periodicity
    )  # Use more periods for better pattern detection
    
    if not actuals or len(actuals) < 12:
        return {
            'sku_id': sku_id,
            'store_id': store_id,
            'pattern_detected': False,
            'reason': 'insufficient_history'
        }
    
    # Calculate MADP and Track
    madp = calculate_madp(actuals, forecasts)
    track = calculate_tracking_signal(actuals, forecasts)
    
    # Detect seasonal pattern
    # Simplistic approach: compare same periods in consecutive years
    seasonal_pattern = False
    seasonal_confidence = 0.0
    
    if len(actuals) >= 24 and periodicity == 13:
        # For 4-weekly periodicity, compare same periods across years
        first_year = actuals[12:24]  # Periods 1-12 from the previous year
        second_year = actuals[0:12]  # Periods 1-12 from the current year
        
        # Calculate correlation between years
        if sum(first_year) > 0 and sum(second_year) > 0:
            # Normalize the data
            avg_first = sum(first_year) / len(first_year)
            avg_second = sum(second_year) / len(second_year)
            
            norm_first = [x / avg_first for x in first_year]
            norm_second = [x / avg_second for x in second_year]
            
            # Calculate correlation
            correlation = sum((a - 1) * (b - 1) for a, b in zip(norm_first, norm_second)) / len(norm_first)
            
            seasonal_confidence = abs(correlation)
            seasonal_pattern = seasonal_confidence > 0.5  # Arbitrary threshold
    
    # Classify the pattern
    pattern = {
        'sku_id': sku_id,
        'store_id': store_id,
        'pattern_detected': True,
        'madp': madp,
        'track': track,
        'seasonal_pattern': seasonal_pattern,
        'seasonal_confidence': seasonal_confidence
    }
    
    # Determine the primary pattern
    if abs(track) > 0.5:
        pattern['primary_pattern'] = 'Trending'
        pattern['trend_direction'] = 'Up' if track > 0 else 'Down'
    elif madp > 50:
        pattern['primary_pattern'] = 'High MADP'
    elif seasonal_pattern:
        pattern['primary_pattern'] = 'Seasonal'
    else:
        pattern['primary_pattern'] = 'Low MADP'
    
    # Check if a seasonal profile would be beneficial
    if seasonal_pattern and not sku.demand_profile_id:
        pattern['seasonal_profile_recommended'] = True
    
    return pattern

def run_period_end_forecasting(session, buyer_id=None, store_id=None):
    """
    Run the period-end forecasting process for all SKUs or a subset of SKUs.
    
    This function performs the following steps:
    1. Identify SKUs to reforecast
    2. Apply the appropriate forecasting method based on SKU settings
    3. Apply seasonal profiles if available
    4. Update forecast data in the database
    
    Args:
        session: SQLAlchemy session
        buyer_id (str, optional): Filter by Buyer ID
        store_id (str, optional): Filter by Store ID
    
    Returns:
        dict: Statistics about the forecasting run
    """
    stats = {
        'total_skus': 0,
        'reforecast_regular': 0,
        'reforecast_enhanced': 0,
        'seasonal_profiles_applied': 0,
        'skipped_frozen': 0,
        'skipped_no_history': 0,
        'errors': 0
    }
    
    # Query for SKUs
    query = session.query(SKU)
    
    # Apply filters
    if buyer_id:
        query = query.filter(SKU.source.has(buyer_id=buyer_id))
    
    if store_id:
        query = query.filter(SKU.store_id == store_id)
    
    # Get all active SKUs (R-Regular or W-Watch)
    skus = query.filter(SKU.buyer_class.in_(['R', 'W'])).all()
    
    stats['total_skus'] = len(skus)
    
    # Process each SKU
    for sku in skus:
        try:
            # Determine which forecasting method to use
            forecast_method = 'regular'
            if hasattr(sku, 'forecast_method'):
                if sku.forecast_method == 'E3 Enhanced AVS':
                    forecast_method = 'enhanced'
            
            # Perform reforecasting
            if forecast_method == 'enhanced':
                result = e3_enhanced_avs_reforecast(
                    session, sku.sku_id, sku.store_id, 
                    periodicity=sku.forecast_periodicity if hasattr(sku, 'forecast_periodicity') else 13
                )
                if result['reforecast']:
                    stats['reforecast_enhanced'] += 1
            else:
                result = e3_regular_avs_reforecast(
                    session, sku.sku_id, sku.store_id,
                    periodicity=sku.forecast_periodicity if hasattr(sku, 'forecast_periodicity') else 13
                )
                if result['reforecast']:
                    stats['reforecast_regular'] += 1
            
            # Log reason for skipping reforecast
            if not result.get('reforecast', False):
                reason = result.get('reason', 'unknown')
                if reason == 'frozen':
                    stats['skipped_frozen'] += 1
                elif reason == 'no_history':
                    stats['skipped_no_history'] += 1
            
            # Apply seasonal profile if it exists
            if hasattr(sku, 'demand_profile_id') and sku.demand_profile_id:
                profile_result = apply_seasonal_profile(session, sku.sku_id, sku.store_id)
                if profile_result.get('profile_applied', False):
                    stats['seasonal_profiles_applied'] += 1
            
        except Exception as e:
            logger.error(f"Error reforecasting SKU {sku.sku_id}: {e}")
            stats['errors'] += 1
    
    logger.info(f"Period-end forecasting completed: {stats}")
    return stats