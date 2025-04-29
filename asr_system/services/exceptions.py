"""
Exception handling services for the ASR system.

This module implements the logic for identifying and processing history
exceptions, such as demand filter high/low, tracking signal high/low,
and service level checks.
"""
import logging
import math
from sqlalchemy import and_, or_
from datetime import datetime

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
        new_madp=forecast_data.madp if forecast_data else None,
        old_track=forecast_data.track if forecast_data else None,
        new_track=forecast_data.track if forecast_data else None,
        service_level_goal=sku.service_level_goal,
        attained_service_level=sku.attained_service_level,
        is_reviewed=False
    )
    
    session.add(exception)
    return exception

def delete_history_exception(session, exception_id, action_taken=None, reviewer=None):
    """
    Delete a history exception and optionally archive it.
    
    Args:
        session: SQLAlchemy session
        exception_id (int): Exception ID
        action_taken (str): Description of action taken
        reviewer (str): ID of user who reviewed the exception
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Get the exception
        exception = session.query(HistoryException).filter(HistoryException.id == exception_id).first()
        
        if not exception:
            logger.error(f"Exception {exception_id} not found")
            return False
        
        # Archive exception if needed
        if action_taken:
            archive_exception(session, exception, action_taken, reviewer)
        
        # Delete the exception
        session.delete(exception)
        session.commit()
        
        return True
    
    except Exception as e:
        logger.error(f"Error deleting exception: {e}")
        session.rollback()
        return False

def archive_exception(session, exception, action_taken, reviewer=None):
    """
    Archive a history exception.
    
    Args:
        session: SQLAlchemy session
        exception: HistoryException object
        action_taken (str): Description of action taken
        reviewer (str): ID of user who reviewed the exception
    
    Returns:
        ArchivedException: Archived exception object
    """
    # Create archived exception
    archived = ArchivedException(
        sku_id=exception.sku_id,
        period_year=exception.period_year,
        period_number=exception.period_number,
        exception_type=exception.exception_type,
        old_forecast=exception.old_forecast,
        new_forecast=exception.new_forecast,
        actual_demand=exception.actual_demand,
        old_madp=exception.old_madp,
        new_madp=exception.new_madp,
        old_track=exception.old_track,
        new_track=exception.new_track,
        service_level_goal=exception.service_level_goal,
        attained_service_level=exception.attained_service_level,
        action_taken=action_taken,
        resolved_by=reviewer,
        resolved_at=datetime.now()
    )
    
    session.add(archived)
    return archived

def process_history_exceptions(session, period_year=None, period_number=None):
    """
    Process history exceptions for the current or specified period.
    
    Args:
        session: SQLAlchemy session
        period_year (int): Year of the period (None for current)
        period_number (int): Period number (None for current)
    
    Returns:
        dict: Statistics about the processing
    """
    try:
        # Get current period if not specified
        if not period_year or not period_number:
            period_year, period_number = get_current_period()
        
        # Get previous period
        prev_period_year, prev_period_number = get_previous_period(period_year, period_number)
        
        # Clear existing exceptions for the previous period
        session.query(HistoryException).filter(
            and_(
                HistoryException.period_year == prev_period_year,
                HistoryException.period_number == prev_period_number
            )
        ).delete()
        
        # Get all active SKUs
        skus = session.query(SKU).filter(
            SKU.buyer_class.in_(['R', 'W'])
        ).all()
        
        # Statistics
        stats = {
            'total_skus': len(skus),
            'demand_filter_high': 0,
            'demand_filter_low': 0,
            'tracking_signal_high': 0,
            'tracking_signal_low': 0,
            'service_level_check': 0,
            'infinity_check': 0,
            'watch_sku': 0,
            'seasonal_sku': 0,
            'new_sku': 0,
            'manual_sku': 0,
            'discontinued_sku': 0
        }
        
        # Process each SKU
        for sku in skus:
            # Get forecast data
            forecast_data = session.query(ForecastData).filter(
                and_(ForecastData.sku_id == sku.sku_id, ForecastData.store_id == sku.store_id)
            ).first()
            
            # Get history data for previous period
            history = session.query(DemandHistory).filter(
                and_(
                    DemandHistory.sku_id == sku.sku_id,
                    DemandHistory.store_id == sku.store_id,
                    DemandHistory.period_year == prev_period_year,
                    DemandHistory.period_number == prev_period_number
                )
            ).first()
            
            # Skip if no history
            if not history:
                continue
            
            # Check for frozen forecast
            if hasattr(sku, 'freeze_forecast_until') and sku.freeze_forecast_until:
                if sku.freeze_forecast_until > datetime.now():
                    # Skip this SKU if forecast is frozen
                    continue
            
            # Check for exceptions
            actual_demand = history.total_demand if history else 0.0
            
            # Check for demand filter high
            if forecast_data and check_demand_filter_high(forecast_data, actual_demand):
                create_history_exception(
                    session, sku, ExceptionType.DEMAND_FILTER_HIGH, 
                    prev_period_year, prev_period_number
                )
                stats['demand_filter_high'] += 1
            
            # Check for demand filter low
            if forecast_data and check_demand_filter_low(forecast_data, actual_demand):
                create_history_exception(
                    session, sku, ExceptionType.DEMAND_FILTER_LOW, 
                    prev_period_year, prev_period_number
                )
                stats['demand_filter_low'] += 1
            
            # Check for tracking signal high
            if forecast_data and check_tracking_signal_high(forecast_data):
                create_history_exception(
                    session, sku, ExceptionType.TRACKING_SIGNAL_HIGH, 
                    prev_period_year, prev_period_number
                )
                stats['tracking_signal_high'] += 1
            
            # Check for tracking signal low
            if forecast_data and check_tracking_signal_low(forecast_data):
                create_history_exception(
                    session, sku, ExceptionType.TRACKING_SIGNAL_LOW, 
                    prev_period_year, prev_period_number
                )
                stats['tracking_signal_low'] += 1
            
            # Check for service level issues
            if check_service_level(sku):
                create_history_exception(
                    session, sku, ExceptionType.SERVICE_LEVEL_CHECK, 
                    prev_period_year, prev_period_number
                )
                stats['service_level_check'] += 1
            
            # Check for infinity
            if forecast_data and check_infinity(forecast_data, actual_demand):
                create_history_exception(
                    session, sku, ExceptionType.INFINITY_CHECK, 
                    prev_period_year, prev_period_number
                )
                stats['infinity_check'] += 1
            
            # Add SKUs based on classifications
            if sku.buyer_class == 'W':
                create_history_exception(
                    session, sku, ExceptionType.WATCH_SKU, 
                    prev_period_year, prev_period_number
                )
                stats['watch_sku'] += 1
            
            if sku.demand_profile_id:
                create_history_exception(
                    session, sku, ExceptionType.SEASONAL_SKU, 
                    prev_period_year, prev_period_number
                )
                stats['seasonal_sku'] += 1
            
            if sku.system_class == 'N':
                create_history_exception(
                    session, sku, ExceptionType.NEW_SKU, 
                    prev_period_year, prev_period_number
                )
                stats['new_sku'] += 1
            
            if sku.buyer_class == 'M':
                create_history_exception(
                    session, sku, ExceptionType.MANUAL_SKU, 
                    prev_period_year, prev_period_number
                )
                stats['manual_sku'] += 1
            
            if sku.buyer_class == 'D':
                create_history_exception(
                    session, sku, ExceptionType.DISCONTINUED_SKU, 
                    prev_period_year, prev_period_number
                )
                stats['discontinued_sku'] += 1
        
        # Commit changes
        session.commit()
        
        return stats
    
    except Exception as e:
        logger.error(f"Error processing history exceptions: {e}")
        session.rollback()
        return {'error': str(e)}

def get_previous_period(period_year, period_number, periodicity=13):
    """
    Get the previous period based on current period.
    
    Args:
        period_year (int): Current period year
        period_number (int): Current period number
        periodicity (int): Periodicity (13 for 4-weekly, 52 for weekly)
    
    Returns:
        tuple: (prev_year, prev_period)
    """
    if period_number > 1:
        # Previous period in same year
        return period_year, period_number - 1
    else:
        # Previous period in previous year
        return period_year - 1, periodicity

def get_history_exceptions_by_type(session, exception_type, buyer_id=None):
    """
    Get history exceptions of a specific type.
    
    Args:
        session: SQLAlchemy session
        exception_type: ExceptionType enum value
        buyer_id (str): Buyer ID to filter by (None for all)
    
    Returns:
        list: List of exception objects
    """
    query = session.query(HistoryException).filter(
        HistoryException.exception_type == exception_type
    )
    
    if buyer_id:
        # Join to sku to filter by buyer ID
        query = query.join(SKU).join(SKU.source).filter(Source.buyer_id == buyer_id)
    
    return query.all()

def mass_delete_exceptions(session, filter_criteria):
    """
    Mass delete exceptions based on filter criteria.
    
    Args:
        session: SQLAlchemy session
        filter_criteria (dict): Dictionary of filter criteria
    
    Returns:
        int: Number of exceptions deleted
    """
    try:
        # Build query
        query = session.query(HistoryException)
        
        # Apply filters
        if 'sku_id' in filter_criteria:
            query = query.filter(HistoryException.sku_id == filter_criteria['sku_id'])
        
        if 'store_id' in filter_criteria:
            # Join to SKU to filter by store_id
            query = query.join(SKU).filter(SKU.store_id == filter_criteria['store_id'])
        
        if 'exception_type' in filter_criteria:
            query = query.filter(HistoryException.exception_type == filter_criteria['exception_type'])
        
        if 'period_year' in filter_criteria:
            query = query.filter(HistoryException.period_year == filter_criteria['period_year'])
        
        if 'period_number' in filter_criteria:
            query = query.filter(HistoryException.period_number == filter_criteria['period_number'])
        
        # Count and delete
        count = query.count()
        query.delete(synchronize_session=False)
        
        # Commit changes
        session.commit()
        
        return count
    
    except Exception as e:
        logger.error(f"Error mass deleting exceptions: {e}")
        session.rollback()
        return 0