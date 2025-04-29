"""
Safety Stock calculation services for the ASR system.

This module implements the algorithms and functions needed to calculate 
safety stock levels based on service level goals, mean absolute deviation 
percentage (MADP), lead time, lead time variance, and order cycle.

Safety stock is a key component of replenishment, providing protection 
against unexpected fluctuations in demand and/or supply.
"""
import logging
import math
import numpy as np
from scipy import stats
from sqlalchemy import and_

from models.sku import SKU, ForecastData
from models.source import Source
from utils.db import get_session
from utils.helpers import get_seasonal_index
from config.settings import ASR_CONFIG

logger = logging.getLogger(__name__)

def calculate_service_factor(service_level_goal):
    """
    Convert service level goal percentage to a statistical service factor (z-score).
    
    Args:
        service_level_goal (float): Service level goal as a percentage (0-100)
    
    Returns:
        float: Service factor (z-score) corresponding to the service level goal
    """
    try:
        # Convert percentage to decimal
        service_level_decimal = service_level_goal / 100.0
        
        # Ensure service level is within valid range
        service_level_decimal = max(0.5, min(0.9999, service_level_decimal))
        
        # Calculate z-score using inverse normal distribution
        # This gives the number of standard deviations needed to achieve the service level
        service_factor = stats.norm.ppf(service_level_decimal)
        
        return service_factor
    
    except Exception as e:
        logger.error(f"Error calculating service factor: {e}")
        # Default to a reasonable service factor for 95% service level
        return 1.65

def calculate_safety_stock(daily_demand, madp, service_factor, lead_time, lead_time_variance, order_cycle):
    """
    Calculate safety stock based on demand, MADP, service level, lead time, and variance.
    
    Args:
        daily_demand (float): Daily demand forecast
        madp (float): Mean Absolute Deviation Percentage
        service_factor (float): Service factor (z-score)
        lead_time (float): Lead time forecast in days
        lead_time_variance (float): Lead time variance as a percentage
        order_cycle (float): Order cycle in days
    
    Returns:
        float: Safety stock in units
    """
    try:
        # Convert MADP to decimal
        madp_decimal = madp / 100.0
        
        # Convert lead time variance to decimal
        lead_time_variance_decimal = lead_time_variance / 100.0
        
        # Calculate Mean Absolute Deviation (MAD) in units
        mad = daily_demand * madp_decimal
        
        # Calculate standard deviation (sigma) from MAD
        # The relationship between MAD and sigma is approximately sigma = MAD * 1.25
        sigma = mad * 1.25
        
        # Calculate the effective replenishment period (lead time + order cycle/2)
        effective_period = lead_time + (order_cycle / 2)
        
        # Calculate the standard deviation of demand during lead time
        # This accounts for both demand variability and lead time variability
        demand_sigma_lt = sigma * math.sqrt(effective_period)
        lead_time_sigma = daily_demand * lead_time * lead_time_variance_decimal
        
        # Total variability is the square root of the sum of squares of individual variabilities
        total_sigma = math.sqrt(demand_sigma_lt**2 + lead_time_sigma**2)
        
        # Calculate safety stock = service factor * total standard deviation
        safety_stock = service_factor * total_sigma
        
        return max(0, safety_stock)
    
    except Exception as e:
        logger.error(f"Error calculating safety stock: {e}")
        return 0

def apply_seasonal_adjustments(safety_stock, sku, period_number=None):
    """
    Apply seasonal adjustments to safety stock.
    
    Args:
        safety_stock (float): Base safety stock calculation
        sku: SKU object
        period_number (int): Current period number (None for current period)
    
    Returns:
        float: Seasonally adjusted safety stock
    """
    try:
        # Check if SKU has a seasonal profile
        if not hasattr(sku, 'demand_profile_id') or not sku.demand_profile_id:
            return safety_stock
        
        # Get the session
        session = get_session()
        
        try:
            # Get the seasonal profile
            from models.sku import SeasonalProfile
            profile = session.query(SeasonalProfile).filter(
                SeasonalProfile.profile_id == sku.demand_profile_id
            ).first()
            
            if not profile:
                return safety_stock
            
            # If period_number not provided, get current period
            if period_number is None:
                # Determine periodicity
                periodicity = getattr(sku, 'forecasting_periodicity', 13)  # Default to 13 (4-weekly)
                from utils.helpers import get_current_period
                _, period_number = get_current_period(periodicity)
            
            # Get seasonal index for the period
            seasonal_index = get_seasonal_index(profile, period_number)
            
            # Apply seasonal index to safety stock
            # For high-season periods (index > 1), increase safety stock
            # For low-season periods (index < 1), decrease safety stock
            adjusted_safety_stock = safety_stock * seasonal_index
            
            return adjusted_safety_stock
        
        finally:
            session.close()
    
    except Exception as e:
        logger.error(f"Error applying seasonal adjustments: {e}")
        return safety_stock

def calculate_item_order_point(session, sku_id, store_id):
    """
    Calculate Item Order Point (IOP) based on safety stock and lead time.
    IOP = Safety Stock + Lead Time Demand
    
    Args:
        session: SQLAlchemy session
        sku_id (str): SKU ID
        store_id (str): Store ID
    
    Returns:
        dict: Dictionary with IOP in days and units
    """
    try:
        # Get the SKU
        sku = session.query(SKU).filter(
            and_(SKU.sku_id == sku_id, SKU.store_id == store_id)
        ).first()
        
        if not sku:
            logger.error(f"SKU {sku_id} not found in store {store_id}")
            return {'days': 0, 'units': 0}
        
        # Get forecast data
        forecast_data = session.query(ForecastData).filter(
            and_(ForecastData.sku_id == sku_id, ForecastData.store_id == store_id)
        ).first()
        
        if not forecast_data:
            logger.error(f"Forecast data not found for SKU {sku_id}")
            return {'days': 0, 'units': 0}
        
        # Get lead time
        lead_time = sku.lead_time_forecast or 0
        
        # Get lead time variance
        lead_time_variance = sku.lead_time_variance or 0
        
        # Get service level goal
        service_level_goal = sku.service_level_goal or ASR_CONFIG.get('default_service_level', 95)
        
        # Get forecast
        daily_demand = forecast_data.weekly_forecast / 7 if forecast_data.weekly_forecast else 0
        
        # Get MADP
        madp = forecast_data.madp or 0
        
        # Get source
        source = sku.source
        
        # Get order cycle
        order_cycle = source.order_cycle if source else 0
        
        # Check for manual safety stock
        if hasattr(sku, 'manual_safety_stock') and hasattr(sku, 'safety_stock_type'):
            if sku.safety_stock_type == 2:  # Always use manual
                safety_stock_units = sku.manual_safety_stock
                safety_stock_days = safety_stock_units / daily_demand if daily_demand > 0 else 0
            elif sku.safety_stock_type == 1:  # Use lesser of manual and calculated
                # Calculate safety stock
                service_factor = calculate_service_factor(service_level_goal)
                calculated_ss = calculate_safety_stock(
                    daily_demand, madp, service_factor, lead_time, lead_time_variance, order_cycle
                )
                
                # Apply seasonal adjustments
                calculated_ss = apply_seasonal_adjustments(calculated_ss, sku)
                
                # Use lesser of manual and calculated
                safety_stock_units = min(sku.manual_safety_stock, calculated_ss)
                safety_stock_days = safety_stock_units / daily_demand if daily_demand > 0 else 0
            else:
                # Calculate normal safety stock
                service_factor = calculate_service_factor(service_level_goal)
                safety_stock_units = calculate_safety_stock(
                    daily_demand, madp, service_factor, lead_time, lead_time_variance, order_cycle
                )
                
                # Apply seasonal adjustments
                safety_stock_units = apply_seasonal_adjustments(safety_stock_units, sku)
                
                safety_stock_days = safety_stock_units / daily_demand if daily_demand > 0 else 0
        else:
            # Calculate normal safety stock
            service_factor = calculate_service_factor(service_level_goal)
            safety_stock_units = calculate_safety_stock(
                daily_demand, madp, service_factor, lead_time, lead_time_variance, order_cycle
            )
            
            # Apply seasonal adjustments
            safety_stock_units = apply_seasonal_adjustments(safety_stock_units, sku)
            
            safety_stock_days = safety_stock_units / daily_demand if daily_demand > 0 else 0
        
        # Check for presentation stock
        if hasattr(sku, 'min_presentation_stock') and sku.min_presentation_stock:
            if sku.min_presentation_stock > safety_stock_units:
                safety_stock_units = sku.min_presentation_stock
                safety_stock_days = safety_stock_units / daily_demand if daily_demand > 0 else 0
        
        # Calculate lead time demand
        lead_time_demand_units = lead_time * daily_demand
        lead_time_demand_days = lead_time
        
        # Calculate IOP
        iop_units = safety_stock_units + lead_time_demand_units
        iop_days = safety_stock_days + lead_time_demand_days
        
        return {
            'days': iop_days,
            'units': iop_units,
            'safety_stock_days': safety_stock_days,
            'safety_stock_units': safety_stock_units,
            'lead_time_days': lead_time_demand_days,
            'lead_time_units': lead_time_demand_units
        }
    
    except Exception as e:
        logger.error(f"Error calculating item order point: {e}")
        return {'days': 0, 'units': 0}

def calculate_vendor_order_point(session, sku_id, store_id):
    """
    Calculate Vendor Order Point (VOP) based on IOP and vendor order cycle.
    VOP = Safety Stock + Lead Time Demand + Vendor Order Cycle Demand
    
    Args:
        session: SQLAlchemy session
        sku_id (str): SKU ID
        store_id (str): Store ID
    
    Returns:
        dict: Dictionary with VOP in days and units
    """
    try:
        # Calculate IOP
        iop = calculate_item_order_point(session, sku_id, store_id)
        
        # Get the SKU
        sku = session.query(SKU).filter(
            and_(SKU.sku_id == sku_id, SKU.store_id == store_id)
        ).first()
        
        if not sku:
            logger.error(f"SKU {sku_id} not found in store {store_id}")
            return iop  # Return IOP as fallback
        
        # Get forecast data
        forecast_data = session.query(ForecastData).filter(
            and_(ForecastData.sku_id == sku_id, ForecastData.store_id == store_id)
        ).first()
        
        if not forecast_data:
            logger.error(f"Forecast data not found for SKU {sku_id}")
            return iop  # Return IOP as fallback
        
        # Get source
        source = sku.source
        
        if not source:
            logger.error(f"Source not found for SKU {sku_id}")
            return iop  # Return IOP as fallback
        
        # Get vendor order cycle
        vendor_cycle = source.order_cycle or 0
        
        # Get daily demand
        daily_demand = forecast_data.weekly_forecast / 7 if forecast_data.weekly_forecast else 0
        
        # Calculate vendor cycle demand
        vendor_cycle_demand_units = vendor_cycle * daily_demand
        vendor_cycle_demand_days = vendor_cycle
        
        # Calculate VOP
        vop_units = iop['units'] + vendor_cycle_demand_units
        vop_days = iop['days'] + vendor_cycle_demand_days
        
        return {
            'days': vop_days,
            'units': vop_units,
            'iop_days': iop['days'],
            'iop_units': iop['units'],
            'vendor_cycle_days': vendor_cycle_demand_days,
            'vendor_cycle_units': vendor_cycle_demand_units
        }
    
    except Exception as e:
        logger.error(f"Error calculating vendor order point: {e}")
        return {'days': 0, 'units': 0}

def recalculate_all_safety_stocks(session, buyer_id=None, store_id=None, source_id=None):
    """
    Recalculate safety stocks for all or filtered SKUs.
    
    Args:
        session: SQLAlchemy session
        buyer_id (str): Filter by buyer ID
        store_id (str): Filter by store ID
        source_id (str): Filter by source ID
    
    Returns:
        dict: Statistics about the recalculation
    """
    try:
        # Build query
        query = session.query(SKU)
        
        # Apply filters
        if buyer_id:
            query = query.join(SKU.source).filter(Source.buyer_id == buyer_id)
        
        if store_id:
            query = query.filter(SKU.store_id == store_id)
        
        if source_id:
            query = query.join(SKU.source).filter(Source.source_id == source_id)
        
        # Filter to active SKUs
        query = query.filter(SKU.buyer_class.in_(['R', 'W']))
        
        # Get SKUs
        skus = query.all()
        
        # Statistics
        stats = {
            'total_skus': len(skus),
            'recalculated': 0,
            'errors': 0
        }
        
        # Process each SKU
        for sku in skus:
            try:
                # Calculate IOP
                iop = calculate_item_order_point(session, sku.sku_id, sku.store_id)
                
                # Update SKU with calculated safety stock
                if hasattr(sku, 'safety_stock_calculated'):
                    sku.safety_stock_calculated = iop['safety_stock_units']
                
                stats['recalculated'] += 1
            
            except Exception as e:
                logger.error(f"Error recalculating safety stock for SKU {sku.sku_id}: {e}")
                stats['errors'] += 1
        
        # Commit changes
        session.commit()
        
        return stats
    
    except Exception as e:
        logger.error(f"Error recalculating all safety stocks: {e}")
        session.rollback()
        return {'error': str(e)}

def optimize_safety_stock(session, sku_id, store_id, target_service_level=None, target_inventory_value=None):
    """
    Optimize safety stock to balance service level and inventory cost.
    Either target_service_level or target_inventory_value must be provided.
    
    Args:
        session: SQLAlchemy session
        sku_id (str): SKU ID
        store_id (str): Store ID
        target_service_level (float): Target service level percentage
        target_inventory_value (float): Target safety stock inventory value
    
    Returns:
        dict: Optimized safety stock settings
    """
    try:
        # Get the SKU
        sku = session.query(SKU).filter(
            and_(SKU.sku_id == sku_id, SKU.store_id == store_id)
        ).first()
        
        if not sku:
            logger.error(f"SKU {sku_id} not found in store {store_id}")
            return None
        
        # Get forecast data
        forecast_data = session.query(ForecastData).filter(
            and_(ForecastData.sku_id == sku_id, ForecastData.store_id == store_id)
        ).first()
        
        if not forecast_data:
            logger.error(f"Forecast data not found for SKU {sku_id}")
            return None
        
        # Get source
        source = sku.source
        
        if not source:
            logger.error(f"Source not found for SKU {sku_id}")
            return None
        
        # Get daily demand
        daily_demand = forecast_data.weekly_forecast / 7 if forecast_data.weekly_forecast else 0
        
        # If targeting service level, find optimal safety stock
        if target_service_level is not None:
            # Calculate service factor
            service_factor = calculate_service_factor(target_service_level)
            
            # Get other needed parameters
            madp = forecast_data.madp or 0
            lead_time = sku.lead_time_forecast or 0
            lead_time_variance = sku.lead_time_variance or 0
            order_cycle = source.order_cycle or 0
            
            # Calculate optimal safety stock
            safety_stock = calculate_safety_stock(
                daily_demand, madp, service_factor, lead_time, lead_time_variance, order_cycle
            )
            
            # Apply seasonal adjustments
            safety_stock = apply_seasonal_adjustments(safety_stock, sku)
            
            # Calculate safety stock value
            safety_stock_value = safety_stock * sku.purchase_price
            
            return {
                'sku_id': sku_id,
                'store_id': store_id,
                'target_service_level': target_service_level,
                'optimized_safety_stock': safety_stock,
                'safety_stock_value': safety_stock_value,
                'safety_stock_days': safety_stock / daily_demand if daily_demand > 0 else 0
            }
        
        # If targeting inventory value, find optimal service level
        elif target_inventory_value is not None:
            # Binary search to find optimal service level
            min_service_level = 50.0  # Minimum acceptable service level
            max_service_level = 99.9  # Maximum service level
            tolerance = 0.01  # Tolerance for inventory value
            
            current_service_level = (min_service_level + max_service_level) / 2
            
            for _ in range(20):  # Maximum iterations
                # Calculate service factor
                service_factor = calculate_service_factor(current_service_level)
                
                # Get other needed parameters
                madp = forecast_data.madp or 0
                lead_time = sku.lead_time_forecast or 0
                lead_time_variance = sku.lead_time_variance or 0
                order_cycle = source.order_cycle or 0
                
                # Calculate safety stock
                safety_stock = calculate_safety_stock(
                    daily_demand, madp, service_factor, lead_time, lead_time_variance, order_cycle
                )
                
                # Apply seasonal adjustments
                safety_stock = apply_seasonal_adjustments(safety_stock, sku)
                
                # Calculate safety stock value
                safety_stock_value = safety_stock * sku.purchase_price
                
                # Check if we're close enough to target
                if abs(safety_stock_value - target_inventory_value) < tolerance:
                    break
                
                # Adjust service level
                if safety_stock_value > target_inventory_value:
                    max_service_level = current_service_level
                else:
                    min_service_level = current_service_level
                
                current_service_level = (min_service_level + max_service_level) / 2
            
            return {
                'sku_id': sku_id,
                'store_id': store_id,
                'target_inventory_value': target_inventory_value,
                'optimized_service_level': current_service_level,
                'optimized_safety_stock': safety_stock,
                'safety_stock_value': safety_stock_value,
                'safety_stock_days': safety_stock / daily_demand if daily_demand > 0 else 0
            }
        
        else:
            logger.error("Either target_service_level or target_inventory_value must be provided")
            return None
    
    except Exception as e:
        logger.error(f"Error optimizing safety stock: {e}")
        return None