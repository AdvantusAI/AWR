"""
Due Order service for the ASR system.

This module implements the logic for identifying orders that must be placed today
to maintain service levels, considering item depletion rates, order points,
and service level goals.
"""
import logging
import datetime
from sqlalchemy import and_, func, desc
from typing import Dict, List, Tuple, Optional, Any

from models.sku import SKU, StockStatus, ForecastData
from models.source import Source, SourceBracket
from models.order import Order, OrderLine, OrderStatus, OrderCategory
from utils.helpers import calculate_available_balance
from utils.db import get_session
from config.settings import ASR_CONFIG

logger = logging.getLogger(__name__)

def calculate_items_at_risk_percentage(session, source_id: int, store_id: str) -> float:
    """
    Calculate the percentage of SKUs in a source that are at or below their order points.
    
    Args:
        session: SQLAlchemy session
        source_id (int): Source ID
        store_id (str): Store ID
    
    Returns:
        float: Percentage of SKUs at risk (0-100)
    """
    try:
        # Get active SKUs for this source
        active_skus = session.query(SKU).filter(
            and_(
                SKU.source_id == source_id,
                SKU.store_id == store_id,
                SKU.buyer_class.in_(['R', 'W'])
            )
        ).all()
        
        if not active_skus:
            logger.warning(f"No active SKUs found for source {source_id} in store {store_id}")
            return 0.0
        
        # Count SKUs at or below order point
        skus_at_risk = 0
        
        for sku in active_skus:
            # Get stock status
            if not sku.stock_status:
                continue
            
            # Calculate available balance
            available_balance = calculate_available_balance(
                sku.stock_status.on_hand,
                sku.stock_status.on_order,
                sku.stock_status.customer_back_order,
                sku.stock_status.reserved,
                sku.stock_status.quantity_held
            )
            
            # Calculate VOP (Vendor Order Point)
            from services.safety_stock import calculate_vendor_order_point
            vop = calculate_vendor_order_point(session, sku.sku_id, store_id)
            
            # Check if balance is at or below VOP
            if available_balance <= vop['units']:
                skus_at_risk += 1
        
        # Calculate percentage
        total_skus = len(active_skus)
        percent_at_risk = (skus_at_risk / total_skus) * 100.0 if total_skus > 0 else 0.0
        
        return percent_at_risk
    
    except Exception as e:
        logger.error(f"Error calculating items at risk percentage: {e}")
        return 0.0

def calculate_projected_service_impact(session, source_id: int, store_id: str, delay_days: int = 1) -> Dict[str, Any]:
    """
    Calculate the projected impact on service levels if an order is delayed by a given number of days.
    
    Args:
        session: SQLAlchemy session
        source_id (int): Source ID
        store_id (str): Store ID
        delay_days (int): Number of days to delay the order
    
    Returns:
        dict: Service impact assessment
    """
    try:
        # Get active SKUs for this source
        active_skus = session.query(SKU).filter(
            and_(
                SKU.source_id == source_id,
                SKU.store_id == store_id,
                SKU.buyer_class.in_(['R', 'W'])
            )
        ).all()
        
        if not active_skus:
            logger.warning(f"No active SKUs found for source {source_id} in store {store_id}")
            return {
                'service_impact': 0.0,
                'skus_affected': 0,
                'high_service_skus_affected': 0,
                'total_skus': 0
            }
        
        # Initialize counters
        skus_affected = 0
        high_service_skus_affected = 0
        service_impact_sum = 0.0
        
        # Get prime limit for identifying high-service SKUs
        op_prime_limit = ASR_CONFIG.get('order_point_prime_limit', 95)
        
        for sku in active_skus:
            # Get forecast data
            forecast_data = session.query(ForecastData).filter(
                and_(ForecastData.sku_id == sku.sku_id, ForecastData.store_id == sku.store_id)
            ).first()
            
            if not forecast_data or not sku.stock_status:
                continue
            
            # Calculate daily forecast
            daily_forecast = forecast_data.weekly_forecast / 7.0 if forecast_data.weekly_forecast else 0
            
            if daily_forecast <= 0:
                continue
            
            # Calculate available balance
            available_balance = calculate_available_balance(
                sku.stock_status.on_hand,
                sku.stock_status.on_order,
                sku.stock_status.customer_back_order,
                sku.stock_status.reserved,
                sku.stock_status.quantity_held
            )
            
            # Calculate days of supply
            days_of_supply = available_balance / daily_forecast if daily_forecast > 0 else float('inf')
            
            # Calculate Safety Stock and Item Order Point
            from services.safety_stock import calculate_item_order_point
            iop = calculate_item_order_point(session, sku.sku_id, store_id)
            safety_stock_days = iop.get('safety_stock_days', 0)
            
            # Calculate lead time
            lead_time_days = iop.get('lead_time_days', 0)
            
            # Check if delaying the order would put the SKU at risk
            # The SKU is at risk if the delay would cause the balance to fall below safety stock
            if days_of_supply <= (lead_time_days + delay_days):
                skus_affected += 1
                
                # Calculate projected service impact
                if days_of_supply <= safety_stock_days:
                    # If delay causes depletion into safety stock, calculate impact
                    service_level_goal = sku.service_level_goal or 95
                    days_into_safety = max(0, safety_stock_days - days_of_supply + delay_days)
                    
                    # Impact is proportional to days into safety stock
                    # Higher service level goals are affected more severely
                    impact_factor = days_into_safety / safety_stock_days if safety_stock_days > 0 else 1.0
                    service_impact = service_level_goal * impact_factor * 0.01
                    service_impact_sum += service_impact
                    
                    # Check if this is a high-service SKU
                    if service_level_goal >= op_prime_limit:
                        high_service_skus_affected += 1
        
        total_skus = len(active_skus)
        
        # Average impact across all SKUs
        avg_service_impact = service_impact_sum / total_skus if total_skus > 0 else 0.0
        
        return {
            'service_impact': avg_service_impact,
            'skus_affected': skus_affected,
            'high_service_skus_affected': high_service_skus_affected,
            'total_skus': total_skus,
            'percent_affected': (skus_affected / total_skus) * 100.0 if total_skus > 0 else 0.0
        }
    
    except Exception as e:
        logger.error(f"Error calculating projected service impact: {e}")
        return {
            'service_impact': 0.0,
            'skus_affected': 0,
            'high_service_skus_affected': 0,
            'total_skus': 0,
            'error': str(e)
        }

def is_service_due_order(session, source_id: int, store_id: str) -> Tuple[bool, Dict[str, Any]]:
    """
    Determine if an order is due based on service level requirements.
    
    This function analyzes the SKUs in a source to determine if an order
    must be placed today to maintain service level goals.
    
    Args:
        session: SQLAlchemy session
        source_id (int): Source ID
        store_id (str): Store ID
    
    Returns:
        tuple: (is_due, details) where is_due is a boolean and details is a dict
    """
    try:
        # Get source
        source = session.query(Source).filter(Source.id == source_id).first()
        
        if not source:
            logger.error(f"Source ID {source_id} not found")
            return False, {'reason': 'source_not_found'}
        
        # Check if order is fixed frequency
        if is_fixed_frequency_due(source):
            return True, {'reason': 'fixed_frequency'}
        
        # Check if order is due based on next_order_date
        if source.next_order_date:
            today = datetime.datetime.now().date()
            if today >= source.next_order_date.date():
                return True, {'reason': 'scheduled_date'}
        
        # Check percentage of SKUs at risk
        percent_at_risk = calculate_items_at_risk_percentage(session, source_id, store_id)
        
        # Get threshold from config
        at_risk_threshold = ASR_CONFIG.get('at_risk_threshold', 20.0)
        
        if percent_at_risk >= at_risk_threshold:
            return True, {
                'reason': 'at_risk_percentage',
                'percent_at_risk': percent_at_risk,
                'threshold': at_risk_threshold
            }
        
        # Check projected service impact if order is delayed by 1 day
        service_impact = calculate_projected_service_impact(session, source_id, store_id, 1)
        
        # Get service impact threshold from config
        impact_threshold = ASR_CONFIG.get('service_impact_threshold', 0.05)
        
        if service_impact['service_impact'] > impact_threshold:
            return True, {
                'reason': 'service_impact',
                'service_impact': service_impact,
                'threshold': impact_threshold
            }
        
        # Check high service SKUs specifically
        if service_impact['high_service_skus_affected'] > 0:
            high_service_threshold = ASR_CONFIG.get('high_service_threshold', 1)
            
            if service_impact['high_service_skus_affected'] >= high_service_threshold:
                return True, {
                    'reason': 'high_service_skus',
                    'high_service_skus_affected': service_impact['high_service_skus_affected'],
                    'threshold': high_service_threshold
                }
        
        # Check if order meets current bracket minimum and should be ordered
        if source.order_when_minimum_met and source.current_bracket > 0:
            # Calculate total SOQ value
            total_value = calculate_order_value(session, source_id, store_id)
            
            # Get current bracket minimum
            bracket = next((b for b in source.brackets if b.bracket_number == source.current_bracket), None)
            
            if bracket and total_value >= bracket.minimum:
                return True, {
                    'reason': 'bracket_minimum_met',
                    'order_value': total_value,
                    'bracket_minimum': bracket.minimum
                }
        
        # If we get here, the order is not due
        return False, {
            'reason': 'not_due',
            'percent_at_risk': percent_at_risk,
            'service_impact': service_impact['service_impact']
        }
    
    except Exception as e:
        logger.error(f"Error determining if order is service due: {e}")
        return False, {'reason': 'error', 'error': str(e)}

def is_fixed_frequency_due(source: Source) -> bool:
    """
    Determine if an order is due because of fixed frequency ordering.
    
    Args:
        source: Source object
    
    Returns:
        bool: True if order is due based on fixed frequency, False otherwise
    """
    # Check if order is on a fixed schedule
    if source.order_days_in_week:
        # Fixed days of week
        today = datetime.datetime.now().weekday() + 1  # 1=Monday, 7=Sunday
        if str(today) in source.order_days_in_week:
            # Check week requirement if any
            if source.order_week == 0:  # Every week
                return True
            elif source.order_week == 1:  # Odd weeks
                week_number = datetime.datetime.now().isocalendar()[1]
                return week_number % 2 == 1
            elif source.order_week == 2:  # Even weeks
                week_number = datetime.datetime.now().isocalendar()[1]
                return week_number % 2 == 0
    
    # Check for specific day of month
    if source.order_day_in_month:
        today = datetime.datetime.now().day
        return today == source.order_day_in_month
    
    return False

def calculate_order_value(session, source_id: int, store_id: str) -> float:
    """
    Calculate the total value of a potential order based on SOQs.
    
    Args:
        session: SQLAlchemy session
        source_id (int): Source ID
        store_id (str): Store ID
    
    Returns:
        float: Total order value
    """
    total_value = 0.0
    
    # Get active SKUs
    skus = session.query(SKU).filter(
        and_(
            SKU.source_id == source_id,
            SKU.store_id == store_id,
            SKU.buyer_class.in_(['R', 'W'])
        )
    ).all()
    
    for sku in skus:
        # Calculate SOQ for this SKU
        from services.replenishment import calculate_suggested_order_quantity
        soq = calculate_suggested_order_quantity(session, sku.sku_id, store_id)
        
        # Add to total
        total_value += soq['units'] * sku.purchase_price
    
    return total_value

def identify_due_orders(session, buyer_id: Optional[str] = None, store_id: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Identify all orders that are due today based on service requirements.
    
    Args:
        session: SQLAlchemy session
        buyer_id (str): Filter by buyer ID (optional)
        store_id (str): Filter by store ID (optional)
    
    Returns:
        list: List of due orders with details
    """
    try:
        # Build query for sources
        query = session.query(Source)
        
        # Apply filters
        if buyer_id:
            query = query.filter(Source.buyer_id == buyer_id)
        
        # Get all sources
        sources = query.all()
        
        # Determine which stores to process
        stores = [store_id] if store_id else ['STORE1']  # Example - in a real system, query all stores
        
        # Results
        due_orders = []
        
        # Process each source for each store
        for source in sources:
            try:
                for store in stores:
                    # Check if order is due
                    is_due, details = is_service_due_order(session, source.id, store)
                    
                    if is_due:
                        # Get existing order or create new one
                        order = session.query(Order).filter(
                            and_(
                                Order.source_id == source.id,
                                Order.store_id == store,
                                Order.status.in_([OrderStatus.PLANNED, OrderStatus.DUE])
                            )
                        ).first()
                        
                        if not order:
                            # Create new order
                            order = Order(
                                source_id=source.id,
                                store_id=store,
                                status=OrderStatus.DUE,
                                category=OrderCategory.DUE,
                                order_date=datetime.datetime.now()
                            )
                            session.add(order)
                            session.flush()  # Get ID without committing
                        else:
                            # Update existing order
                            order.status = OrderStatus.DUE
                            order.category = OrderCategory.DUE
                        
                        # Add to results
                        due_orders.append({
                            'order_id': order.id,
                            'source_id': source.source_id,
                            'source_name': source.name,
                            'store_id': store,
                            'reason': details['reason'],
                            'details': details
                        })
            
            except Exception as e:
                logger.error(f"Error processing source {source.source_id}: {e}")
                continue
        
        # No need to commit changes - that will be handled by the calling function
        
        return due_orders
    
    except Exception as e:
        logger.error(f"Error identifying due orders: {e}")
        return []

def get_order_delay(session, source_id: int, store_id: str) -> int:
    """
    Calculate the approximate number of days until an order becomes due.
    
    Args:
        session: SQLAlchemy session
        source_id (int): Source ID
        store_id (str): Store ID
    
    Returns:
        int: Approximate days until order is due
    """
    try:
        # Get source
        source = session.query(Source).filter(Source.id == source_id).first()
        
        if not source:
            logger.error(f"Source ID {source_id} not found")
            return 0
        
        # Check if order is already due
        is_due, _ = is_service_due_order(session, source_id, store_id)
        if is_due:
            return 0
        
        # Check if order is on a fixed schedule
        if source.order_days_in_week or source.order_day_in_month:
            return calculate_days_to_fixed_order(source)
        
        # Check next order date
        if source.next_order_date:
            today = datetime.datetime.now().date()
            days_to_next = (source.next_order_date.date() - today).days
            if days_to_next > 0:
                return days_to_next
        
        # Calculate based on depletion rates
        # Get percent at risk and project forward
        current_percent = calculate_items_at_risk_percentage(session, source_id, store_id)
        threshold = ASR_CONFIG.get('at_risk_threshold', 20.0)
        
        if current_percent >= threshold:
            return 0
        
        # Calculate depletion rate
        # This is a simplification - in reality, you would examine each SKU individually
        active_skus = session.query(SKU).filter(
            and_(
                SKU.source_id == source_id,
                SKU.store_id == store_id,
                SKU.buyer_class.in_(['R', 'W'])
            )
        ).all()
        
        if not active_skus:
            return source.order_cycle or 30  # Default if no data
        
        # Calculate average daily depletion rate for percent at risk
        # Simple model: assume linear depletion toward threshold
        avg_depletion_rate = 0.0
        for sku in active_skus:
            forecast_data = session.query(ForecastData).filter(
                and_(ForecastData.sku_id == sku.sku_id, ForecastData.store_id == sku.store_id)
            ).first()
            
            if not forecast_data or not sku.stock_status:
                continue
                
            # Calculate daily forecast and depletion
            daily_forecast = forecast_data.weekly_forecast / 7.0 if forecast_data.weekly_forecast else 0
            
            if daily_forecast > 0:
                avg_depletion_rate += 1.0 / len(active_skus)
        
        # If we can't calculate a rate, return the order cycle
        if avg_depletion_rate <= 0:
            return source.order_cycle or 30
        
        # Estimate days until threshold is reached
        days_to_threshold = (threshold - current_percent) / avg_depletion_rate
        
        # Round and ensure minimum 1 day if not already due
        return max(1, round(days_to_threshold))
    
    except Exception as e:
        logger.error(f"Error calculating order delay: {e}")
        return 30  # Default fallback

def calculate_days_to_fixed_order(source: Source) -> int:
    """
    Calculate days until the next fixed-frequency order is due.
    
    Args:
        source: Source object
    
    Returns:
        int: Days until next fixed order
    """
    today = datetime.datetime.now()
    current_day = today.weekday() + 1  # 1=Monday, 7=Sunday
    current_date = today.day
    
    # Check for day of week ordering
    if source.order_days_in_week:
        order_days = [int(day) for day in source.order_days_in_week if day.isdigit()]
        
        if order_days:
            # Check week requirement
            week_matches = True
            if source.order_week > 0:
                current_week = today.isocalendar()[1]
                week_matches = (current_week % 2 == 1 and source.order_week == 1) or \
                              (current_week % 2 == 0 and source.order_week == 2)
            
            if week_matches:
                # Find next ordering day
                days_ahead = [(day - current_day) % 7 for day in order_days]
                days_ahead = [days for days in days_ahead if days > 0]
                
                if days_ahead:
                    return min(days_ahead)
                elif source.order_week == 0:  # Every week
                    return 7  # Next week, same day
                else:
                    return 7  # Next appropriate week
            else:
                # Need to wait for next appropriate week
                return 7
    
    # Check for day of month ordering
    if source.order_day_in_month:
        if current_date < source.order_day_in_month:
            # Later this month
            return source.order_day_in_month - current_date
        else:
            # Next month
            # Calculate days remaining in this month plus days until order day next month
            import calendar
            days_in_month = calendar.monthrange(today.year, today.month)[1]
            return (days_in_month - current_date) + source.order_day_in_month
    
    # Fallback to order cycle
    return source.order_cycle or 30
"""