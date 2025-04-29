"""
Lead Time Forecasting services for the ASR system.

This module implements the algorithms and functions for lead time forecasting
based on actual receipt data. It handles lead time trend detection, variance
calculation, and seasonal adjustments to lead times.

Lead time forecasting is one of the four key components of replenishment,
determining when to place orders to maintain service levels.
"""
import logging
import math
import numpy as np
from datetime import datetime, timedelta
from sqlalchemy import and_, func, desc

from models.sku import SKU
from models.source import Source
from models.order import Order, OrderLine
from utils.db import get_session
from config.settings import ASR_CONFIG

logger = logging.getLogger(__name__)

def calculate_actual_lead_time(order_date, receipt_date):
    """
    Calculate the actual lead time between order and receipt dates.
    
    Args:
        order_date (datetime): Date when order was placed
        receipt_date (datetime): Date when goods were received
    
    Returns:
        int: Actual lead time in days
    """
    if not order_date or not receipt_date:
        return None
    
    # Calculate the difference in days
    difference = receipt_date - order_date
    
    # Return the number of days
    return difference.days

def get_receipt_history(session, sku_id=None, store_id=None, source_id=None, months=6):
    """
    Get receipt history for calculating lead time forecasts.
    
    Args:
        session: SQLAlchemy session
        sku_id (str): Filter by SKU ID (optional)
        store_id (str): Filter by store ID (optional)
        source_id (str): Filter by source ID (optional)
        months (int): Number of months of history to retrieve
    
    Returns:
        list: List of receipt objects (orders with receipt dates)
    """
    try:
        # Define start date for history
        start_date = datetime.now() - timedelta(days=30 * months)
        
        # Build query for completed orders
        query = session.query(Order).filter(
            Order.receipt_date.isnot(None),
            Order.receipt_date >= start_date,
            Order.order_date.isnot(None)
        )
        
        # Apply filters
        if store_id:
            query = query.filter(Order.store_id == store_id)
            
        if source_id:
            # Get the source object first
            source = session.query(Source).filter(Source.source_id == source_id).first()
            if source:
                query = query.filter(Order.source_id == source.id)
        
        # Get orders
        orders = query.all()
        
        # If SKU_ID is provided, filter to only include order lines for that SKU
        if sku_id and orders:
            filtered_orders = []
            
            for order in orders:
                # Check if the order has order lines for the specified SKU
                sku_lines = session.query(OrderLine).join(SKU).filter(
                    OrderLine.order_id == order.id,
                    SKU.sku_id == sku_id
                ).all()
                
                if sku_lines:
                    filtered_orders.append(order)
            
            return filtered_orders
        
        return orders
    
    except Exception as e:
        logger.error(f"Error getting receipt history: {e}")
        return []

def filter_special_receipts(receipts, exclude_expedited=True, exclude_delayed=True):
    """
    Filter out special receipts (expedited or delayed) from lead time calculation.
    
    Args:
        receipts (list): List of receipt objects
        exclude_expedited (bool): Whether to exclude expedited receipts
        exclude_delayed (bool): Whether to exclude delayed receipts
    
    Returns:
        list: Filtered list of receipt objects
    """
    filtered_receipts = []
    
    for receipt in receipts:
        # Skip receipts with missing dates
        if not receipt.order_date or not receipt.receipt_date:
            continue
        
        # Calculate actual lead time
        actual_lead_time = calculate_actual_lead_time(receipt.order_date, receipt.receipt_date)
        
        # Skip if lead time couldn't be calculated
        if actual_lead_time is None:
            continue
        
        # Check if order was expedited based on actual vs. expected lead time
        if hasattr(receipt, 'expected_delivery_date') and receipt.expected_delivery_date:
            expected_lead_time = (receipt.expected_delivery_date - receipt.order_date).days
            
            # If actual is significantly less than expected, it may have been expedited
            if exclude_expedited and actual_lead_time < 0.7 * expected_lead_time:
                continue
            
            # If actual is significantly more than expected, it may have been delayed
            if exclude_delayed and actual_lead_time > 1.5 * expected_lead_time:
                continue
        
        # Include in filtered list if it passes checks
        filtered_receipts.append(receipt)
    
    return filtered_receipts

def calculate_lead_time_stats(receipts):
    """
    Calculate lead time statistics from a list of receipts.
    
    Args:
        receipts (list): List of receipt objects
    
    Returns:
        dict: Dictionary with lead time statistics
    """
    if not receipts:
        return None
    
    # Calculate actual lead times
    lead_times = []
    
    for receipt in receipts:
        lead_time = calculate_actual_lead_time(receipt.order_date, receipt.receipt_date)
        if lead_time is not None and lead_time > 0:  # Ensure positive lead time
            lead_times.append(lead_time)
    
    if not lead_times:
        return None
    
    # Calculate statistics
    mean_lead_time = sum(lead_times) / len(lead_times)
    
    # Calculate variance
    variance = sum((lt - mean_lead_time) ** 2 for lt in lead_times) / len(lead_times)
    
    # Calculate standard deviation
    std_dev = math.sqrt(variance)
    
    # Calculate variance as percentage of mean
    variance_pct = (std_dev / mean_lead_time) * 100 if mean_lead_time > 0 else 0
    
    # Find min and max
    min_lead_time = min(lead_times)
    max_lead_time = max(lead_times)
    
    # Calculate median
    sorted_lead_times = sorted(lead_times)
    mid = len(sorted_lead_times) // 2
    
    if len(sorted_lead_times) % 2 == 0:
        median_lead_time = (sorted_lead_times[mid - 1] + sorted_lead_times[mid]) / 2
    else:
        median_lead_time = sorted_lead_times[mid]
    
    # Calculate trend (average of last 3 minus average of first 3)
    # Sort by chronological order for trend calculation
    if len(lead_times) >= 6:
        # Assuming receipts are already ordered by date
        recent_avg = sum(lead_times[-3:]) / 3
        older_avg = sum(lead_times[:3]) / 3
        trend = recent_avg - older_avg
    else:
        trend = 0
    
    return {
        'mean': mean_lead_time,
        'median': median_lead_time,
        'min': min_lead_time,
        'max': max_lead_time,
        'std_dev': std_dev,
        'variance': variance,
        'variance_pct': variance_pct,
        'trend': trend,
        'count': len(lead_times)
    }

def detect_lead_time_trend(lead_time_stats, trend_threshold=0.1):
    """
    Detect trend in lead time data.
    
    Args:
        lead_time_stats (dict): Lead time statistics
        trend_threshold (float): Threshold for trend detection as fraction of mean
    
    Returns:
        dict: Trend information
    """
    if not lead_time_stats:
        return {'has_trend': False, 'trend_value': 0, 'trend_pct': 0}
    
    trend = lead_time_stats['trend']
    mean = lead_time_stats['mean']
    
    # Calculate trend as percentage of mean
    trend_pct = (trend / mean) * 100 if mean > 0 else 0
    
    # Check if trend exceeds threshold
    threshold_value = mean * trend_threshold
    has_trend = abs(trend) > threshold_value
    
    return {
        'has_trend': has_trend,
        'trend_value': trend,
        'trend_pct': trend_pct,
        'direction': 'increasing' if trend > 0 else 'decreasing' if trend < 0 else 'stable'
    }

def detect_lead_time_seasonality(session, source_id, months=24):
    """
    Detect seasonality in lead time data.
    
    Args:
        session: SQLAlchemy session
        source_id (str): Source ID
        months (int): Number of months of history to analyze
    
    Returns:
        dict: Seasonality information
    """
    try:
        # Get receipt history
        receipts = get_receipt_history(session, source_id=source_id, months=months)
        
        # Filter receipts
        filtered_receipts = filter_special_receipts(receipts)
        
        if not filtered_receipts:
            return {'has_seasonality': False}
        
        # Group by month
        monthly_lead_times = {}
        
        for receipt in filtered_receipts:
            lead_time = calculate_actual_lead_time(receipt.order_date, receipt.receipt_date)
            if lead_time is not None:
                month = receipt.receipt_date.month
                if month not in monthly_lead_times:
                    monthly_lead_times[month] = []
                monthly_lead_times[month].append(lead_time)
        
        # Calculate monthly averages
        monthly_averages = {}
        overall_average = []
        
        for month, lead_times in monthly_lead_times.items():
            if lead_times:
                monthly_averages[month] = sum(lead_times) / len(lead_times)
                overall_average.extend(lead_times)
        
        if not monthly_averages:
            return {'has_seasonality': False}
        
        # Calculate overall average
        overall_avg = sum(overall_average) / len(overall_average)
        
        # Calculate monthly indices
        monthly_indices = {}
        for month, avg in monthly_averages.items():
            monthly_indices[month] = avg / overall_avg if overall_avg > 0 else 1.0
        
        # Check for seasonality
        # A simple method: if max index - min index > 0.2, consider it seasonal
        if monthly_indices:
            max_index = max(monthly_indices.values())
            min_index = min(monthly_indices.values())
            seasonal_range = max_index - min_index
            
            has_seasonality = seasonal_range > 0.2
            
            return {
                'has_seasonality': has_seasonality,
                'monthly_indices': monthly_indices,
                'seasonal_range': seasonal_range,
                'max_month': max(monthly_indices, key=monthly_indices.get),
                'min_month': min(monthly_indices, key=monthly_indices.get)
            }
        else:
            return {'has_seasonality': False}
    
    except Exception as e:
        logger.error(f"Error detecting lead time seasonality: {e}")
        return {'has_seasonality': False, 'error': str(e)}

def forecast_lead_time(session, sku_id, store_id, apply_trend=True, exclude_special=True):
    """
    Forecast lead time for a specific SKU.
    
    Args:
        session: SQLAlchemy session
        sku_id (str): SKU ID
        store_id (str): Store ID
        apply_trend (bool): Whether to apply trend to forecast
        exclude_special (bool): Whether to exclude special receipts
    
    Returns:
        dict: Lead time forecast results
    """
    try:
        # Get the SKU
        sku = session.query(SKU).filter(
            and_(SKU.sku_id == sku_id, SKU.store_id == store_id)
        ).first()
        
        if not sku:
            logger.error(f"SKU {sku_id} not found in store {store_id}")
            return None
        
        # Get source
        source = sku.source
        
        if not source:
            logger.error(f"Source not found for SKU {sku_id}")
            return None
        
        # Get receipt history
        receipts = get_receipt_history(session, sku_id=sku_id, store_id=store_id)
        
        # Filter receipts if needed
        if exclude_special:
            receipts = filter_special_receipts(receipts)
        
        # Calculate lead time statistics
        lt_stats = calculate_lead_time_stats(receipts)
        
        if not lt_stats:
            # Not enough data, use source lead time
            source_lt = source.lead_time_forecast or source.lead_time_quoted or ASR_CONFIG.get('default_lead_time', 7)
            source_lt_variance = source.lead_time_variance or ASR_CONFIG.get('default_lead_time_variance', 10)  # Default 10%
            
            return {
                'sku_id': sku_id,
                'store_id': store_id,
                'lead_time_forecast': source_lt,
                'lead_time_variance': source_lt_variance,
                'data_source': 'source_default',
                'receipt_count': 0
            }
        
        # Detect trend
        trend_info = detect_lead_time_trend(lt_stats)
        
        # Base forecast on median (more robust than mean)
        lead_time_forecast = lt_stats['median']
        
        # Apply trend if significant and requested
        if apply_trend and trend_info['has_trend']:
            # Apply half of the detected trend to forecast future trend
            lead_time_forecast += (trend_info['trend_value'] / 2)
        
        # Ensure lead time is not negative
        lead_time_forecast = max(1, lead_time_forecast)
        
        # Calculate variance as percentage
        lead_time_variance = lt_stats['variance_pct']
        
        # Ensure minimum variance
        min_variance = 5.0  # Minimum 5% variance
        lead_time_variance = max(min_variance, lead_time_variance)
        
        return {
            'sku_id': sku_id,
            'store_id': store_id,
            'lead_time_forecast': round(lead_time_forecast),
            'lead_time_variance': round(lead_time_variance, 1),
            'data_source': 'receipt_history',
            'receipt_count': lt_stats['count'],
            'has_trend': trend_info['has_trend'],
            'trend_direction': trend_info['direction'],
            'trend_value': trend_info['trend_value']
        }
    
    except Exception as e:
        logger.error(f"Error forecasting lead time: {e}")
        return None

def forecast_source_lead_time(session, source_id, store_id=None, apply_trend=True, exclude_special=True):
    """
    Forecast lead time at the source level.
    
    Args:
        session: SQLAlchemy session
        source_id (str): Source ID
        store_id (str): Store ID (optional)
        apply_trend (bool): Whether to apply trend to forecast
        exclude_special (bool): Whether to exclude special receipts
    
    Returns:
        dict: Source lead time forecast results
    """
    try:
        # Get the source
        source = session.query(Source).filter(Source.source_id == source_id).first()
        
        if not source:
            logger.error(f"Source {source_id} not found")
            return None
        
        # Get receipt history
        receipts = get_receipt_history(session, source_id=source_id, store_id=store_id)
        
        # Filter receipts if needed
        if exclude_special:
            receipts = filter_special_receipts(receipts)
        
        # Calculate lead time statistics
        lt_stats = calculate_lead_time_stats(receipts)
        
        if not lt_stats:
            # Not enough data, use existing source lead time
            source_lt = source.lead_time_forecast or source.lead_time_quoted or ASR_CONFIG.get('default_lead_time', 7)
            source_lt_variance = source.lead_time_variance or ASR_CONFIG.get('default_lead_time_variance', 10)  # Default 10%
            
            return {
                'source_id': source_id,
                'store_id': store_id,
                'lead_time_forecast': source_lt,
                'lead_time_variance': source_lt_variance,
                'data_source': 'existing_source_values',
                'receipt_count': 0
            }
        
        # Detect trend
        trend_info = detect_lead_time_trend(lt_stats)
        
        # Base forecast on median (more robust than mean)
        lead_time_forecast = lt_stats['median']
        
        # Apply trend if significant and requested
        if apply_trend and trend_info['has_trend']:
            # Apply half of the detected trend to forecast future trend
            lead_time_forecast += (trend_info['trend_value'] / 2)
        
        # Ensure lead time is not negative
        lead_time_forecast = max(1, lead_time_forecast)
        
        # Calculate variance as percentage
        lead_time_variance = lt_stats['variance_pct']
        
        # Ensure minimum variance
        min_variance = 5.0  # Minimum 5% variance
        lead_time_variance = max(min_variance, lead_time_variance)
        
        # Check for seasonality
        seasonality_info = detect_lead_time_seasonality(session, source_id)
        
        return {
            'source_id': source_id,
            'store_id': store_id,
            'lead_time_forecast': round(lead_time_forecast),
            'lead_time_variance': round(lead_time_variance, 1),
            'data_source': 'receipt_history',
            'receipt_count': lt_stats['count'],
            'has_trend': trend_info['has_trend'],
            'trend_direction': trend_info['direction'],
            'trend_value': trend_info['trend_value'],
            'has_seasonality': seasonality_info.get('has_seasonality', False)
        }
    
    except Exception as e:
        logger.error(f"Error forecasting source lead time: {e}")
        return None

def apply_lead_time_forecast(session, sku_id, store_id, lead_time_forecast, lead_time_variance):
    """
    Apply lead time forecast to a SKU.
    
    Args:
        session: SQLAlchemy session
        sku_id (str): SKU ID
        store_id (str): Store ID
        lead_time_forecast (float): Lead time forecast in days
        lead_time_variance (float): Lead time variance as percentage
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Get the SKU
        sku = session.query(SKU).filter(
            and_(SKU.sku_id == sku_id, SKU.store_id == store_id)
        ).first()
        
        if not sku:
            logger.error(f"SKU {sku_id} not found in store {store_id}")
            return False
        
        # Update lead time forecasts
        sku.lead_time_forecast = lead_time_forecast
        sku.lead_time_variance = lead_time_variance
        
        # Commit changes
        session.commit()
        
        return True
    
    except Exception as e:
        logger.error(f"Error applying lead time forecast: {e}")
        session.rollback()
        return False

def apply_source_lead_time_forecast(session, source_id, lead_time_forecast, lead_time_variance):
    """
    Apply lead time forecast to a source.
    
    Args:
        session: SQLAlchemy session
        source_id (str): Source ID
        lead_time_forecast (float): Lead time forecast in days
        lead_time_variance (float): Lead time variance as percentage
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Get the source
        source = session.query(Source).filter(Source.source_id == source_id).first()
        
        if not source:
            logger.error(f"Source {source_id} not found")
            return False
        
        # Update lead time forecasts
        source.lead_time_forecast = lead_time_forecast
        source.lead_time_variance = lead_time_variance
        
        # Commit changes
        session.commit()
        
        return True
    
    except Exception as e:
        logger.error(f"Error applying source lead time forecast: {e}")
        session.rollback()
        return False

def run_lead_time_forecasting(session, update_source_level=True, update_sku_level=True):
    """
    Run lead time forecasting for all sources and SKUs.
    
    Args:
        session: SQLAlchemy session
        update_source_level (bool): Whether to update source-level lead times
        update_sku_level (bool): Whether to update SKU-level lead times
    
    Returns:
        dict: Statistics about the forecasting run
    """
    try:
        # Statistics
        stats = {
            'source_updates': 0,
            'sku_updates': 0,
            'sources_processed': 0,
            'skus_processed': 0,
            'errors': 0
        }
        
        # First update source-level lead times if requested
        if update_source_level:
            # Get all sources
            sources = session.query(Source).all()
            
            stats['sources_processed'] = len(sources)
            
            for source in sources:
                try:
                    # Forecast lead time for source
                    forecast_result = forecast_source_lead_time(session, source.source_id)
                    
                    if forecast_result and forecast_result['data_source'] != 'existing_source_values':
                        # Apply forecast
                        success = apply_source_lead_time_forecast(
                            session, 
                            source.source_id,
                            forecast_result['lead_time_forecast'],
                            forecast_result['lead_time_variance']
                        )
                        
                        if success:
                            stats['source_updates'] += 1
                
                except Exception as e:
                    logger.error(f"Error forecasting lead time for source {source.source_id}: {e}")
                    stats['errors'] += 1
        
        # Then update SKU-level lead times if requested
        if update_sku_level:
            # Get active SKUs
            skus = session.query(SKU).filter(
                SKU.buyer_class.in_(['R', 'W'])
            ).all()
            
            stats['skus_processed'] = len(skus)
            
            for sku in skus:
                try:
                    # Forecast lead time for SKU
                    forecast_result = forecast_lead_time(session, sku.sku_id, sku.store_id)
                    
                    if forecast_result and forecast_result['data_source'] != 'source_default':
                        # Apply forecast
                        success = apply_lead_time_forecast(
                            session, 
                            sku.sku_id,
                            sku.store_id,
                            forecast_result['lead_time_forecast'],
                            forecast_result['lead_time_variance']
                        )
                        
                        if success:
                            stats['sku_updates'] += 1
                
                except Exception as e:
                    logger.error(f"Error forecasting lead time for SKU {sku.sku_id}: {e}")
                    stats['errors'] += 1
        
        return stats
    
    except Exception as e:
        logger.error(f"Error running lead time forecasting: {e}")
        return {'error': str(e)}

def seasonalize_lead_times(session, source_id):
    """
    Adjust lead times for seasonal variations.
    
    Args:
        session: SQLAlchemy session
        source_id (str): Source ID
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Check for seasonality in lead times
        seasonality_info = detect_lead_time_seasonality(session, source_id)
        
        if not seasonality_info or not seasonality_info.get('has_seasonality', False):
            logger.info(f"No seasonality detected for source {source_id}")
            return False
        
        # Get current month
        current_month = datetime.now().month
        
        # Get seasonal index for current month
        monthly_indices = seasonality_info.get('monthly_indices', {})
        current_index = monthly_indices.get(current_month, 1.0)
        
        # Get the source
        source = session.query(Source).filter(Source.source_id == source_id).first()
        
        if not source:
            logger.error(f"Source {source_id} not found")
            return False
        
        # Get base lead time forecast (without seasonal adjustment)
        forecast_result = forecast_source_lead_time(session, source_id)
        
        if not forecast_result:
            logger.error(f"Could not forecast lead time for source {source_id}")
            return False
        
        base_lead_time = forecast_result['lead_time_forecast']
        
        # Apply seasonal index
        seasonal_lead_time = base_lead_time * current_index
        
        # Update source lead time
        source.lead_time_forecast = round(seasonal_lead_time)
        
        # Update SKUs that use source lead time
        skus = session.query(SKU).filter(
            and_(
                SKU.source_id == source.id,
                SKU.buyer_class.in_(['R', 'W'])
            )
        ).all()
        
        for sku in skus:
            # Only update SKUs that don't have their own lead time forecast
            if sku.lead_time_forecast is None:
                sku.lead_time_forecast = round(seasonal_lead_time)
        
        # Commit changes
        session.commit()
        
        return True
    
    except Exception as e:
        logger.error(f"Error seasonalizing lead times: {e}")
        session.rollback()
        return False

def exclude_expedited_order(session, sku_id, store_id, order_date):
    """
    Mark an order as expedited to exclude from normal lead time calculations.
    
    Args:
        session: SQLAlchemy session
        sku_id (str): SKU ID
        store_id (str): Store ID
        order_date (datetime): Order date
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Get the SKU
        sku = session.query(SKU).filter(
            and_(SKU.sku_id == sku_id, SKU.store_id == store_id)
        ).first()
        
        if not sku:
            logger.error(f"SKU {sku_id} not found in store {store_id}")
            return False
        
        # Find order with this order date
        # Use a small window around the order date to account for time differences
        order_start = order_date - timedelta(hours=12)
        order_end = order_date + timedelta(hours=12)
        
        order = session.query(Order).filter(
            Order.store_id == store_id,
            Order.source_id == sku.source_id,
            Order.order_date.between(order_start, order_end)
        ).first()
        
        if not order:
            logger.error(f"Order not found for SKU {sku_id} on {order_date}")
            return False
        
        # Mark order as expedited
        order.is_expedited = True
        
        # Commit changes
        session.commit()
        
        return True
    
    except Exception as e:
        logger.error(f"Error excluding expedited order: {e}")
        session.rollback()
        return False

def exclude_delayed_order(session, sku_id, store_id, order_date):
    """
    Mark an order as delayed to exclude from normal lead time calculations.
    
    Args:
        session: SQLAlchemy session
        sku_id (str): SKU ID
        store_id (str): Store ID
        order_date (datetime): Order date
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Get the SKU
        sku = session.query(SKU).filter(
            and_(SKU.sku_id == sku_id, SKU.store_id == store_id)
        ).first()
        
        if not sku:
            logger.error(f"SKU {sku_id} not found in store {store_id}")
            return False
        
        # Find order with this order date
        # Use a small window around the order date to account for time differences
        order_start = order_date - timedelta(hours=12)
        order_end = order_date + timedelta(hours=12)
        
        order = session.query(Order).filter(
            Order.store_id == store_id,
            Order.source_id == sku.source_id,
            Order.order_date.between(order_start, order_end)
        ).first()
        
        if not order:
            logger.error(f"Order not found for SKU {sku_id} on {order_date}")
            return False
        
        # Mark order as delayed
        order.is_delayed = True
        
        # Commit changes
        session.commit()
        
        return True
    
    except Exception as e:
        logger.error(f"Error excluding delayed order: {e}")
        session.rollback()
        return False