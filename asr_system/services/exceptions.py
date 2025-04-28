"""
Exception handling services for the ASR system.

This module implements the logic for identifying and processing history
exceptions, such as demand filter high/low, tracking signal high/low,
and service level checks.
"""
import logging
import math
from sqlalchemy import and_, or_

from models.sku import SKU, ForecastData, DemandHistory
from models.history import HistoryException, ExceptionType, ArchivedException
from utils.helpers import get_current_period
from utils.db import get_session
from config.settings import ASR_CONFIG, EXCEPTION_THRESHOLDS

logger = logging.getLogger(__name__)

def check_demand_filter_high(forecast_data, actual_demand):
    """
    Check if demand exceeds the forecast by more than the threshold.
    
    Args:
        forecast_data: ForecastData object
        actual_demand (float): Actual demand
    
    Returns:
        bool: True if exception should be raised, False otherwise
    """
    if not forecast_data or not forecast_data.period_forecast:
        return False
    
    # Get threshold
    threshold = EXCEPTION_THRESHOLDS.get('demand_filter_high', 3.0)
    
    # Calculate deviation
    if forecast_data.madp:
        madp_decimal = forecast_data.madp / 100.0
        expected_deviation = forecast_data.period_forecast * madp_decimal
        
        if expected_deviation == 0:
            return actual_demand > (forecast_data.period_forecast * 2)
        
        # Calculate standard deviations above forecast
        deviations = (actual_demand - forecast_data.period_forecast) / expected_deviation
        
        return deviations > threshold
    else:
        # If no MADP, use simple percentage
        return actual_demand > (forecast_data.period_forecast * (1 + threshold))

def check_demand_filter_low(forecast_data, actual_demand):
    """
    Check if demand is less than the forecast by more than the threshold.
    
    Args:
        forecast_data: ForecastData object
        actual_demand (float): Actual demand
    
    Returns:
        bool: True if exception should be raised, False otherwise
    """
    if not forecast_data or not forecast_data.period_forecast:
        return False
    
    # Get threshold
    threshold = EXCEPTION_THRESHOLDS.get('demand_filter_low', 3.0)
    
    # Calculate deviation
    if forecast_data.madp:
        madp_decimal = forecast_data.madp / 100.0
        expected_deviation = forecast_data.period_forecast * madp_decimal
        
        if expected_deviation == 0:
            return actual_demand < (forecast_data.period_forecast * 0.5)
        
        # Calculate standard deviations below forecast
        deviations = (forecast_data.period_forecast - actual_demand) / expected_deviation
        
        return deviations > threshold
    else:
        # If no MADP, use simple percentage
        return actual_demand < (forecast_data.period_forecast * (1 - threshold))

def check_tracking_signal_high(forecast_data):
    """
    Check if tracking signal exceeds the threshold in the positive direction.
    
    Args:
        forecast_data: ForecastData object
    
    Returns:
        bool: True if exception should be raised, False otherwise
    """
    if not forecast_data:
        return False
    
    # Get threshold
    threshold = EXCEPTION_THRESHOLDS.get('tracking_signal_limit', 0.55)
    
    # Check tracking signal (converted to positive)
    return forecast_data.track and forecast_data.track > threshold

def check_tracking_signal_low(forecast_data):
    """
    Check if tracking signal exceeds the threshold in the negative direction.
    
    Args:
        forecast_data: ForecastData object
    
    Returns:
        bool: True if exception should be raised, False otherwise
    """
    if not forecast_data:
        return False
    
    # Get threshold
    threshold = EXCEPTION_THRESHOLDS.get('tracking_signal_limit', 0.55)
    
    # Check tracking signal (converted to negative)
    return forecast_data.track and forecast_data.track < -threshold

def check_service_level(sku):
    """
    Check if attained service level is below the goal.
    
    Args:
        sku: SKU object
    
    Returns:
        bool: True if exception should be raised, False otherwise
    """
    if not sku or not sku.service_level_goal or not sku.attained_service_level:
        return False
    
    # Check if attained service level is significantly below goal
    return sku.attained_service_level < (sku.service_level_goal * 0.95)

def check_infinity(forecast_data, actual_demand):
    """
    Check for "infinity" exception (demand but no forecast).
    
    Args:
        forecast_data: ForecastData object
        actual_demand (float): Actual demand
    
    Returns:
        bool: True if exception should be raised, False otherwise
    """
    if not forecast_data:
        return False
    
    # Check if there's demand but no forecast
    return actual_demand > 0 and (not forecast_data.period_forecast or forecast_data.period_forecast == 0)

def create_history_exception(session, sku, exception_type, period_year, period_number):
    """
    Create a history exception record.
    
    Args:
        session: SQLAlchemy session
        sku: SKU object
        exception_type: ExceptionType enum value
        period_year (int): Year of the period
        period_number (int): Period number
    
    Returns:
        HistoryException: Created exception object
    """
    # Get forecast data
    forecast_data = session.query(ForecastData).filter(
        and_(ForecastData.sku_id == sku.sku_id, ForecastData.store_id == sku.store_id)
    ).first()
    
    # Get history data
    history = session.query(DemandHistory).filter(
        and_(
            DemandHistory.sku_id == sku.sku_id,
            DemandHistory.store_id == sku.store_id,
            DemandHistory.period_year == period_year,
            DemandHistory.period_number == period_number
        )
    ).first()
    
    # Create exception
    exception = HistoryException(
        sku_id=sku.id,
        period_year=period_year,
        period_number=period_number,
        exception_type=exception_type,
        old_forecast=forecast_data.period_forecast if forecast_data else None,
        new_forecast=forecast_data.period_forecast if forecast_data else None,
        actual_demand=history.total_demand if history else None,
        old_madp=forecast_data.madp if forecast_data else None,
        new_ma