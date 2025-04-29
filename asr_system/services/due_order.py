"""
Due Order services for the ASR system.

This module implements the algorithms and functions for due order determination, 
prioritization, and highlighting critical orders that need immediate attention.
"""
import logging
import datetime
from typing import List, Dict, Any, Tuple, Optional
from sqlalchemy import and_, or_, func, desc
from sqlalchemy.orm import Session

from models.sku import SKU, StockStatus, ForecastData
from models.source import Source
from models.order import Order, OrderLine, OrderStatus, OrderCategory
from utils.helpers import calculate_available_balance
from utils.db import get_session
from config.settings import ASR_CONFIG

logger = logging.getLogger(__name__)

def calculate_urgency_score(session: Session, order_id: int) -> float:
    """
    Calculate an urgency score for a due order based on multiple factors.
    Higher scores indicate higher urgency.
    
    Factors considered:
    - Number of SKUs at or below order point
    - Number of high-service level SKUs at risk
    - Average service level attainment vs goal
    - Risk of stockout
    - Order value
    
    Args:
        session: SQLAlchemy session
        order_id: Order ID
    
    Returns:
        float: Urgency score (higher is more urgent)
    """
    try:
        # Get the order
        order = session.query(Order).filter(Order.id == order_id).first()
        
        if not order:
            logger.error(f"Order {order_id} not found")
            return 0.0
        
        # Initialize urgency components
        urgency_components = {
            'order_point_ratio': 0.0,  # % of items at/below order point
            'high_service_risk': 0.0,   # % of high service items at risk
            'service_gap': 0.0,         # gap between attained and goal service
            'stockout_risk': 0.0,       # average days until stockout
            'order_value': 0.0          # normalized order value
        }
        
        # Get order lines and associated SKUs
        order_lines = session.query(OrderLine).filter(OrderLine.order_id == order_id).all()
        
        if not order_lines:
            return 0.0
        
        # Get the OP Prime Limit % from company settings
        op_prime_limit = ASR_CONFIG.get('order_point_prime_limit', 95)
        
        # Count items
        total_items = len(order_lines)
        order_point_items = 0
        high_service_items = 0
        high_service_at_risk = 0
        service_gap_sum = 0.0
        days_to_stockout_sum = 0.0
        
        for line in order_lines:
            # Get the SKU
            sku = session.query(SKU).filter(SKU.id == line.sku_id).first()
            if not sku:
                continue
                
            # Get stock status
            stock_status = sku.stock_status
            if not stock_status:
                continue
                
            # Calculate available balance
            available_balance = calculate_available_balance(
                stock_status.on_hand,
                stock_status.on_order,
                stock_status.customer_back_order,
                stock_status.reserved,
                stock_status.quantity_held
            )
            
            # Get forecast data
            forecast_data = session.query(ForecastData).filter(
                and_(ForecastData.sku_id == sku.sku_id, ForecastData.store_id == sku.store_id)
            ).first()
            
            if not forecast_data or forecast_data.weekly_forecast <= 0:
                continue
                
            # Calculate Item Order Point (IOP = Safety Stock + Lead Time)
            safety_stock_days = line.item_delay if line.item_delay is not None else 0
            lead_time_days = sku.lead_time_forecast if sku.lead_time_forecast else 0
            item_order_point = safety_stock_days + lead_time_days
            
            # Calculate days of supply remaining
            daily_forecast = forecast_data.weekly_forecast / 7
            days_of_supply = available_balance / daily_forecast if daily_forecast > 0 else float('inf')
            
            # Check if at or below order point
            if available_balance <= item_order_point:
                order_point_items += 1
            
            # Check if high service SKU
            is_high_service = sku.service_level_goal and sku.service_level_goal >= op_prime_limit
            if is_high_service:
                high_service_items += 1
                
                # Check if high service item at risk
                if available_balance <= item_order_point:
                    high_service_at_risk += 1
            
            # Calculate service gap
            if sku.service_level_goal and sku.attained_service_level:
                service_gap = max(0, sku.service_level_goal - sku.attained_service_level)
                service_gap_sum += service_gap
            
            # Calculate days to stockout
            days_to_stockout_sum += min(days_of_supply, 30)  # Cap at 30 days
        
        # Calculate component values
        if total_items > 0:
            urgency_components['order_point_ratio'] = order_point_items / total_items
            urgency_components['service_gap'] = service_gap_sum / total_items
            urgency_components['stockout_risk'] = 1.0 - (days_to_stockout_sum / (total_items * 30))
        
        if high_service_items > 0:
            urgency_components['high_service_risk'] = high_service_at_risk / high_service_items
        
        # Normalize order value (assuming a maximum order value of $100,000)
        max_order_value = 100000.0
        order_value = order.final_adjust_amount if order.final_adjust_amount else 0.0
        urgency_components['order_value'] = min(order_value / max_order_value, 1.0)
        
        # Calculate weighted score
        # Weights can be adjusted based on business priorities
        weights = {
            'order_point_ratio': 0.25,
            'high_service_risk': 0.3,
            'service_gap': 0.15,
            'stockout_risk': 0.2,
            'order_value': 0.1
        }
        
        urgency_score = sum(urgency_components[key] * weights[key] for key in urgency_components)
        
        # Scale to 0-100 range for easier interpretation
        return urgency_score * 100
    
    except Exception as e:
        logger.error(f"Error calculating urgency score: {e}")
        return 0.0

def get_prioritized_due_orders(session: Session, buyer_id: Optional[str] = None, 
                              store_id: Optional[str] = None, limit: int = 50) -> List[Dict[str, Any]]:
    """
    Get due orders sorted by urgency score.
    
    Args:
        session: SQLAlchemy session
        buyer_id: Filter by buyer ID (optional)
        store_id: Filter by store ID (optional)
        limit: Maximum number of orders to return
    
    Returns:
        list: List of due orders with urgency scores and metrics
    """
    try:
        # Get due orders
        query = session.query(Order).filter(
            and_(
                Order.status == OrderStatus.DUE,
                Order.category == OrderCategory.DUE
            )
        )
        
        # Apply filters
        if buyer_id:
            query = query.join(Order.source).filter(Source.buyer_id == buyer_id)
        
        if store_id:
            query = query.filter(Order.store_id == store_id)
        
        # Get orders
        orders = query.all()
        
        # Calculate urgency scores and metrics
        results = []
        
        for order in orders:
            # Calculate urgency score
            urgency_score = calculate_urgency_score(session, order.id)
            
            # Get order metrics
            metrics = get_order_risk_metrics(session, order.id)
            
            # Check if critical
            is_critical = (
                urgency_score >= 75.0 or               # High urgency score
                metrics['days_to_stockout'] <= 1.0 or  # Imminent stockout
                metrics['high_service_at_risk'] >= 3   # Multiple high service SKUs at risk
            )
            
            results.append({
                'order_id': order.id,
                'source_id': order.source_id,
                'source_name': order.source.name if order.source else None,
                'store_id': order.store_id,
                'order_date': order.order_date,
                'expected_delivery_date': order.expected_delivery_date,
                'urgency_score': urgency_score,
                'is_critical': is_critical,
                'order_point_count': metrics['order_point_count'],
                'high_service_at_risk': metrics['high_service_at_risk'],
                'avg_days_to_stockout': metrics['days_to_stockout'],
                'service_level_gap': metrics['service_gap'],
                'order_value': order.final_adjust_amount
            })
        
        # Sort by urgency score (highest first)
        results.sort(key=lambda x: x['urgency_score'], reverse=True)
        
        # Apply limit
        if limit:
            results = results[:limit]
        
        return results
    
    except Exception as e:
        logger.error(f"Error getting prioritized due orders: {e}")
        return []

def get_order_risk_metrics(session: Session, order_id: int) -> Dict[str, Any]:
    """
    Calculate risk metrics for a due order.
    
    Args:
        session: SQLAlchemy session
        order_id: Order ID
    
    Returns:
        dict: Dictionary with risk metrics
    """
    try:
        # Initialize metrics
        metrics = {
            'order_point_count': 0,       # Number of SKUs at or below order point
            'high_service_at_risk': 0,    # Number of high service SKUs at risk
            'service_gap': 0.0,           # Average service level gap
            'days_to_stockout': float('inf')  # Average days to stockout (limited to 30)
        }
        
        # Get the order
        order = session.query(Order).filter(Order.id == order_id).first()
        
        if not order:
            return metrics
        
        # Get order lines
        order_lines = session.query(OrderLine).filter(OrderLine.order_id == order_id).all()
        
        if not order_lines:
            return metrics
        
        # Get the OP Prime Limit % from company settings
        op_prime_limit = ASR_CONFIG.get('order_point_prime_limit', 95)
        
        # Process each line
        total_items = len(order_lines)
        service_gap_sum = 0.0
        days_to_stockout_sum = 0.0
        min_days_to_stockout = float('inf')
        
        for line in order_lines:
            # Get the SKU
            sku = session.query(SKU).filter(SKU.id == line.sku_id).first()
            if not sku:
                continue
                
            # Get stock status
            stock_status = sku.stock_status
            if not stock_status:
                continue
                
            # Calculate available balance
            available_balance = calculate_available_balance(
                stock_status.on_hand,
                stock_status.on_order,
                stock_status.customer_back_order,
                stock_status.reserved,
                stock_status.quantity_held
            )
            
            # Get forecast data
            forecast_data = session.query(ForecastData).filter(
                and_(ForecastData.sku_id == sku.sku_id, ForecastData.store_id == sku.store_id)
            ).first()
            
            if not forecast_data:
                continue
                
            # Calculate Item Order Point (IOP = Safety Stock + Lead Time)
            safety_stock_days = line.item_delay if line.item_delay is not None else 0
            lead_time_days = sku.lead_time_forecast if sku.lead_time_forecast else 0
            item_order_point = safety_stock_days + lead_time_days
            
            # Calculate days of supply remaining
            daily_forecast = forecast_data.weekly_forecast / 7
            days_of_supply = available_balance / daily_forecast if daily_forecast > 0 and daily_forecast is not None else float('inf')
            
            # Check if at or below order point
            if available_balance <= item_order_point:
                metrics['order_point_count'] += 1
            
            # Check if high service SKU at risk
            is_high_service = sku.service_level_goal and sku.service_level_goal >= op_prime_limit
            if is_high_service and available_balance <= item_order_point:
                metrics['high_service_at_risk'] += 1
            
            # Calculate service gap
            if sku.service_level_goal and sku.attained_service_level:
                service_gap = max(0, sku.service_level_goal - sku.attained_service_level)
                service_gap_sum += service_gap
            
            # Calculate days to stockout
            capped_days = min(days_of_supply, 30.0)  # Cap at 30 days
            days_to_stockout_sum += capped_days
            min_days_to_stockout = min(min_days_to_stockout, capped_days)
        
        # Calculate average metrics
        if total_items > 0:
            metrics['service_gap'] = service_gap_sum / total_items
            metrics['days_to_stockout'] = days_to_stockout_sum / total_items
        
        # Also include the minimum days to stockout (most critical)
        metrics['min_days_to_stockout'] = min_days_to_stockout if min_days_to_stockout != float('inf') else 30.0
        
        return metrics
    
    except Exception as e:
        logger.error(f"Error calculating order risk metrics: {e}")
        return {
            'order_point_count': 0,
            'high_service_at_risk': 0,
            'service_gap': 0.0,
            'days_to_stockout': 30.0
        }

def identify_critical_due_orders(session: Session, buyer_id: Optional[str] = None, 
                               store_id: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Identify critical due orders that need immediate attention.
    
    Args:
        session: SQLAlchemy session
        buyer_id: Filter by buyer ID (optional)
        store_id: Filter by store ID (optional)
    
    Returns:
        list: List of critical due orders
    """
    try:
        # Get all prioritized due orders
        all_orders = get_prioritized_due_orders(session, buyer_id, store_id, limit=None)
        
        # Filter for critical orders
        critical_orders = [order for order in all_orders if order['is_critical']]
        
        return critical_orders
    
    except Exception as e:
        logger.error(f"Error identifying critical due orders: {e}")
        return []

def get_due_order_summary_metrics(session: Session, buyer_id: Optional[str] = None, 
                                store_id: Optional[str] = None) -> Dict[str, Any]:
    """
    Calculate summary metrics for all due orders.
    
    Args:
        session: SQLAlchemy session
        buyer_id: Filter by buyer ID (optional)
        store_id: Filter by store ID (optional)
    
    Returns:
        dict: Dictionary with summary metrics
    """
    try:
        # Get all prioritized due orders
        all_orders = get_prioritized_due_orders(session, buyer_id, store_id, limit=None)
        
        # Calculate summary metrics
        total_orders = len(all_orders)
        critical_orders = sum(1 for order in all_orders if order['is_critical'])
        total_value = sum(order['order_value'] for order in all_orders if order['order_value'])
        high_service_at_risk = sum(order['high_service_at_risk'] for order in all_orders)
        
        # Calculate orders by urgency level
        high_urgency = sum(1 for order in all_orders if order['urgency_score'] >= 75)
        medium_urgency = sum(1 for order in all_orders if 50 <= order['urgency_score'] < 75)
        low_urgency = sum(1 for order in all_orders if order['urgency_score'] < 50)
        
        # Calculate orders by days to stockout
        immediate_risk = sum(1 for order in all_orders if order['avg_days_to_stockout'] <= 1)
        short_term_risk = sum(1 for order in all_orders if 1 < order['avg_days_to_stockout'] <= 3)
        medium_term_risk = sum(1 for order in all_orders if 3 < order['avg_days_to_stockout'] <= 7)
        
        return {
            'total_due_orders': total_orders,
            'critical_orders': critical_orders,
            'total_order_value': total_value,
            'high_service_skus_at_risk': high_service_at_risk,
            'urgency_breakdown': {
                'high': high_urgency,
                'medium': medium_urgency,
                'low': low_urgency
            },
            'stockout_risk_breakdown': {
                'immediate': immediate_risk,
                'short_term': short_term_risk,
                'medium_term': medium_term_risk
            }
        }
    
    except Exception as e:
        logger.error(f"Error calculating due order summary metrics: {e}")
        return {
            'total_due_orders': 0,
            'critical_orders': 0,
            'total_order_value': 0,
            'high_service_skus_at_risk': 0,
            'urgency_breakdown': {'high': 0, 'medium': 0, 'low': 0},
            'stockout_risk_breakdown': {'immediate': 0, 'short_term': 0, 'medium_term': 0}
        }

def update_due_order_status(session: Session) -> Dict[str, Any]:
    """
    Update the status of orders to mark them as due based on various criteria.
    This function is typically called during nightly processing.
    
    Criteria for an order becoming due:
    1. Fixed order day has arrived
    2. Enough SKUs have depleted to their order points to place the source's 
       average service level at risk
    3. When minimum order value has been met (if configured)
    
    Args:
        session: SQLAlchemy session
    
    Returns:
        dict: Statistics about the update
    """
    try:
        # Statistics
        stats = {
            'orders_processed': 0,
            'orders_marked_due': 0,
            'orders_due_to_fixed_schedule': 0,
            'orders_due_to_service_risk': 0,
            'orders_due_to_minimum_met': 0,
            'errors': 0
        }
        
        # Get orders that are not due or accepted
        orders = session.query(Order).filter(
            and_(
                Order.status != OrderStatus.DUE,
                Order.status != OrderStatus.ACCEPTED,
                Order.status != OrderStatus.PURGED
            )
        ).all()
        
        stats['orders_processed'] = len(orders)
        
        # Process each order
        for order in orders:
            try:
                # Check fixed order schedule
                is_due_to_schedule = check_fixed_order_schedule(session, order)
                
                # Check service level risk
                is_due_to_service = check_service_level_risk(session, order)
                
                # Check minimum met
                is_due_to_minimum = check_minimum_met(session, order)
                
                # Mark as due if any criteria is met
                if is_due_to_schedule or is_due_to_service or is_due_to_minimum:
                    order.status = OrderStatus.DUE
                    order.category = OrderCategory.DUE
                    stats['orders_marked_due'] += 1
                    
                    # Update specific reason counts
                    if is_due_to_schedule:
                        stats['orders_due_to_fixed_schedule'] += 1
                    
                    if is_due_to_service:
                        stats['orders_due_to_service_risk'] += 1
                    
                    if is_due_to_minimum:
                        stats['orders_due_to_minimum_met'] += 1
            
            except Exception as e:
                logger.error(f"Error processing order {order.id}: {e}")
                stats['errors'] += 1
        
        # Commit changes
        session.commit()
        
        return stats
    
    except Exception as e:
        logger.error(f"Error updating due order status: {e}")
        session.rollback()
        return {'error': str(e)}

def check_fixed_order_schedule(session: Session, order: Order) -> bool:
    """
    Check if an order is due based on fixed order schedule.
    
    Args:
        session: SQLAlchemy session
        order: Order object
    
    Returns:
        bool: True if order is due based on schedule
    """
    try:
        # Get the source
        source = order.source
        
        if not source:
            return False
        
        today = datetime.datetime.now()
        
        # Check order days in week
        if source.order_days_in_week:
            # Convert to list of integers
            order_days = [int(day) for day in source.order_days_in_week if day.isdigit()]
            
            # Check if today is an order day
            today_day = today.weekday() + 1  # 1=Monday, 7=Sunday
            
            if today_day in order_days:
                # Check week requirement if any
                if source.order_week == 0:  # Every week
                    return True
                elif source.order_week == 1:  # Odd weeks
                    week_number = today.isocalendar()[1]
                    return week_number % 2 == 1
                elif source.order_week == 2:  # Even weeks
                    week_number = today.isocalendar()[1]
                    return week_number % 2 == 0
        
        # Check order day in month
        if source.order_day_in_month:
            today_day = today.day
            return today_day == source.order_day_in_month
        
        # Check next order date
        if source.next_order_date:
            return today.date() >= source.next_order_date.date()
        
        return False
    
    except Exception as e:
        logger.error(f"Error checking fixed order schedule: {e}")
        return False

def check_service_level_risk(session: Session, order: Order) -> bool:
    """
    Check if an order is due based on service level risk.
    
    Args:
        session: SQLAlchemy session
        order: Order object
    
    Returns:
        bool: True if order is due based on service level risk
    """
    try:
        # Get the source
        source = order.source
        
        if not source:
            return False
        
        # Get active SKUs
        skus = session.query(SKU).filter(
            and_(
                SKU.source_id == source.id,
                SKU.store_id == order.store_id,
                SKU.buyer_class.in_(['R', 'W'])
            )
        ).all()
        
        if not skus:
            return False
        
        # Count SKUs at risk
        skus_at_risk = 0
        total_skus = len(skus)
        
        for sku in skus:
            # Get stock status
            stock_status = sku.stock_status
            if not stock_status:
                continue
                
            # Calculate available balance
            available_balance = calculate_available_balance(
                stock_status.on_hand,
                stock_status.on_order,
                stock_status.customer_back_order,
                stock_status.reserved,
                stock_status.quantity_held
            )
            
            # Calculate Vendor Order Point (VOP)
            from services.safety_stock import calculate_vendor_order_point
            vop = calculate_vendor_order_point(session, sku.sku_id, sku.store_id)
            
            # Check if balance is at or below VOP
            if available_balance <= vop['units']:
                skus_at_risk += 1
        
        # Calculate percentage of SKUs at risk
        if total_skus == 0:
            return False
        
        percent_at_risk = (skus_at_risk / total_skus) * 100.0
        
        # Get threshold from company settings or use default
        threshold = ASR_CONFIG.get('due_order_risk_threshold', 20.0)  # Default: 20% of SKUs at risk
        
        return percent_at_risk >= threshold
    
    except Exception as e:
        logger.error(f"Error checking service level risk: {e}")
        return False

def check_minimum_met(session: Session, order: Order) -> bool:
    """
    Check if an order is due based on minimum order value being met.
    
    Args:
        session: SQLAlchemy session
        order: Order object
    
    Returns:
        bool: True if order is due based on minimum being met
    """
    try:
        # Get the source
        source = order.source
        
        if not source:
            return False
        
        # Check if order when minimum met is enabled
        if not source.order_when_minimum_met or source.current_bracket <= 0:
            return False
        
        # Get current bracket
        bracket = next((b for b in source.brackets if b.bracket_number == source.current_bracket), None)
        
        if not bracket or not bracket.minimum:
            return False
        
        # Check if order meets minimum
        if order.auto_adjust_amount >= bracket.minimum:
            return True
        
        return False
    
    except Exception as e:
        logger.error(f"Error checking minimum met: {e}")
        return False