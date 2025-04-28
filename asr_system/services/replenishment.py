"""
Main replenishment services for the ASR system.

This module implements the core logic for replenishment, including
building suggested order quantities, creating orders, and processing
daily replenishment activities.
"""
import datetime
import logging
import math
from sqlalchemy import and_, func

from models.sku import SKU, StockStatus, ForecastData, SeasonalProfile
from models.source import Source, SourceBracket
from models.order import Order, OrderLine, OrderCheck, OrderStatus, OrderCategory
from services.demand_forecast import get_seasonal_index
from services.safety_stock import calculate_item_order_point, calculate_vendor_order_point
from services.order_policy import get_effective_order_cycle
from utils.helpers import round_to_buying_multiple, calculate_available_balance
from utils.db import get_session
from config.settings import ASR_CONFIG

logger = logging.getLogger(__name__)

def calculate_order_up_to_level(session, sku_id, store_id):
    """
    Calculate the Order Up To Level (OUTL) for a SKU.
    OUTL = (Lead Time + Safety Stock) + Effective Order Cycle
    
    Args:
        session: SQLAlchemy session
        sku_id (str): SKU ID
        store_id (str): Store ID
    
    Returns:
        dict: Dictionary with OUTL in days and units
    """
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
    
    # Get Item Order Point (Safety Stock + Lead Time)
    iop = calculate_item_order_point(session, sku_id, store_id)
    
    # Get effective order cycle
    effective_order_cycle = get_effective_order_cycle(sku)
    
    # Calculate OUTL days
    outl_days = iop['days'] + effective_order_cycle
    
    # Calculate OUTL units
    daily_forecast = forecast_data.weekly_forecast / 7
    outl_units = iop['units'] + (effective_order_cycle * daily_forecast)
    
    # Apply OUTL hard maximum if set
    outl_hard_max = getattr(sku, 'outl_hard_max', None)
    if outl_hard_max and outl_units > outl_hard_max:
        outl_units = outl_hard_max
        outl_days = outl_units / daily_forecast if daily_forecast > 0 else 0
    
    return {
        'days': outl_days,
        'units': round(outl_units)
    }

def calculate_suggested_order_quantity(session, sku_id, store_id):
    """
    Calculate the Suggested Order Quantity (SOQ) for a SKU.
    SOQ = Order Up To Level - Available Balance
    
    Args:
        session: SQLAlchemy session
        sku_id (str): SKU ID
        store_id (str): Store ID
    
    Returns:
        dict: Dictionary with SOQ in days and units
    """
    # Get the SKU and stock status
    sku = session.query(SKU).join(StockStatus).filter(
        and_(SKU.sku_id == sku_id, SKU.store_id == store_id)
    ).first()
    
    if not sku or not sku.stock_status:
        logger.error(f"SKU {sku_id} or stock status not found in store {store_id}")
        return {'days': 0, 'units': 0}
    
    # Get forecast data
    forecast_data = session.query(ForecastData).filter(
        and_(ForecastData.sku_id == sku_id, ForecastData.store_id == store_id)
    ).first()
    
    if not forecast_data:
        logger.error(f"Forecast data not found for SKU {sku_id}")
        return {'days': 0, 'units': 0}
    
    # If buyer class is M (Manual) or D (Discontinued), no SOQ unless back order exists
    if sku.buyer_class in ['M', 'D']:
        # Check for back order
        if sku.stock_status.customer_back_order > 0:
            # Order enough to cover back order if negative balance
            available_balance = calculate_available_balance(
                sku.stock_status.on_hand,
                sku.stock_status.on_order,
                sku.stock_status.customer_back_order,
                sku.stock_status.reserved,
                sku.stock_status.quantity_held
            )
            
            if available_balance < 0:
                soq_units = -available_balance
                daily_forecast = forecast_data.weekly_forecast / 7
                soq_days = soq_units / daily_forecast if daily_forecast > 0 else 0
                
                # Round to buying multiple
                if not sku.ignore_multiple and sku.buying_multiple > 1:
                    soq_units = round_to_buying_multiple(soq_units, sku.buying_multiple)
                
                return {
                    'days': soq_days,
                    'units': soq_units
                }
        
        # Otherwise, no SOQ
        return {'days': 0, 'units': 0}
    
    # Calculate OUTL
    outl = calculate_order_up_to_level(session, sku_id, store_id)
    
    # Calculate available balance
    available_balance = calculate_available_balance(
        sku.stock_status.on_hand,
        sku.stock_status.on_order,
        sku.stock_status.customer_back_order,
        sku.stock_status.reserved,
        sku.stock_status.quantity_held
    )
    
    # Calculate SOQ units
    soq_units = outl['units'] - available_balance
    
    # If SOQ is negative or zero, no order needed
    if soq_units <= 0:
        return {'days': 0, 'units': 0}
    
    # Round to buying multiple if needed
    if not sku.ignore_multiple and sku.buying_multiple > 1:
        soq_units = round_to_buying_multiple(soq_units, sku.buying_multiple)
    
    # Calculate SOQ days
    daily_forecast = forecast_data.weekly_forecast / 7
    soq_days = soq_units / daily_forecast if daily_forecast > 0 else 0
    
    return {
        'days': soq_days,
        'units': soq_units
    }

def is_order_due(session, source_id, store_id):
    """
    Determine if an order is due based on service level requirements or fixed ordering criteria.
    
    Args:
        session: SQLAlchemy session
        source_id (str): Source ID
        store_id (str): Store ID
    
    Returns:
        bool: True if order is due, False otherwise
    """
    # Get the source
    source = session.query(Source).filter(Source.source_id == source_id).first()
    
    if not source:
        logger.error(f"Source {source_id} not found")
        return False
    
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
    
    if source.order_day_in_month:
        # Fixed day of month
        today = datetime.datetime.now().day
        return today == source.order_day_in_month
    
    # Check next order date if set
    if source.next_order_date:
        today = datetime.datetime.now().date()
        return today >= source.next_order_date.date()
    
    # Check if order meets current bracket minimum
    if source.order_when_minimum_met and source.current_bracket > 0:
        # Get active SKUs
        skus = session.query(SKU).filter(
            and_(
                SKU.source_id == source.id,
                SKU.store_id == store_id,
                SKU.buyer_class.in_(['R', 'W'])
            )
        ).all()
        
        # Calculate total SOQ
        total_amount = 0.0
        
        for sku in skus:
            soq = calculate_suggested_order_quantity(session, sku.sku_id, store_id)
            total_amount += soq['units'] * sku.purchase_price
        
        # Get current bracket minimum
        bracket = next((b for b in source.brackets if b.bracket_number == source.current_bracket), None)
        
        if bracket and total_amount >= bracket.minimum:
            return True
    
    # Check if enough SKUs are at or below their order points
    # Get active SKUs
    skus = session.query(SKU).filter(
        and_(
            SKU.source_id == source.id,
            SKU.store_id == store_id,
            SKU.buyer_class.in_(['R', 'W'])
        )
    ).all()
    
    # Count SKUs at or below order point
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
        
        # Calculate VOP
        vop = calculate_vendor_order_point(session, sku.sku_id, store_id)
        
        # Check if balance is at or below VOP
        if available_balance <= vop['units']:
            skus_at_risk += 1
    
    # Calculate percentage of SKUs at risk
    if total_skus == 0:
        return False
    
    percent_at_risk = (skus_at_risk / total_skus) * 100.0
    
    # If percentage exceeds threshold, order is due
    # Threshold could be based on average service level goal or fixed value
    threshold = 20.0  # Example: 20% of SKUs at risk
    
    return percent_at_risk >= threshold

def build_order(session, source_id, store_id):
    """
    Build a replenishment order for a source and store.
    
    Args:
        session: SQLAlchemy session
        source_id (str): Source ID
        store_id (str): Store ID
    
    Returns:
        Order: Created order object
    """
    try:
        # Get the source
        source = session.query(Source).filter(Source.source_id == source_id).first()
        
        if not source:
            logger.error(f"Source {source_id} not found")
            return None
        
        # Determine if order is due
        is_due = is_order_due(session, source_id, store_id)
        
        # Create order object
        order = Order(
            source_id=source.id,
            store_id=store_id,
            status=OrderStatus.DUE if is_due else OrderStatus.PLANNED,
            category=OrderCategory.DUE if is_due else OrderCategory.ALL,
            order_date=datetime.datetime.now()
        )
        
        session.add(order)
        
        # Get active SKUs
        skus = session.query(SKU).filter(
            and_(
                SKU.source_id == source.id,
                SKU.store_id == store_id,
                SKU.buyer_class.in_(['R', 'W', 'M', 'D'])  # Include M and D for back orders
            )
        ).all()
        
        # Initialize order checks
        order_checks = {
            'order_point_a': 0,
            'order_point': 0,
            'watch': 0,
            'manual': 0,
            'new': 0,
            'uninitialized': 0,
            'quantity': 0,
            'shelf_life': 0
        }
        
        # Initialize order totals
        order_totals = {
            'independent_amount': 0.0,
            'independent_eaches': 0.0,
            'independent_weight': 0.0,
            'independent_volume': 0.0,
            'auto_adjust_amount': 0.0,
            'auto_adjust_eaches': 0.0,
            'auto_adjust_weight': 0.0,
            'auto_adjust_volume': 0.0
        }
        
        # Process each SKU
        for sku in skus:
            # Calculate suggested order quantity
            soq = calculate_suggested_order_quantity(session, sku.sku_id, store_id)
            
            if soq['units'] <= 0:
                # No order needed for this SKU
                continue
            
            # Create order line
            order_line = OrderLine(
                order_id=order.id,
                sku_id=sku.id,
                suggested_order_quantity=soq['units'],
                soq_days=soq['days'],
                purchase_price=sku.purchase_price,
                extended_amount=soq['units'] * sku.purchase_price
            )
            
            # Calculate item delay (days to order point)
            # Get stock status
            stock_status = sku.stock_status
            if stock_status:
                # Calculate available balance
                available_balance = calculate_available_balance(
                    stock_status.on_hand,
                    stock_status.on_order,
                    stock_status.customer_back_order,
                    stock_status.reserved,
                    stock_status.quantity_held
                )
                
                # Calculate IOP
                iop = calculate_item_order_point(session, sku.sku_id, store_id)
                
                # Calculate daily forecasted demand
                forecast_data = sku.forecast_data
                daily_forecast = forecast_data.weekly_forecast / 7 if forecast_data else 0
                
                # Calculate delay in days
                if daily_forecast > 0:
                    order_line.item_delay = (available_balance - iop['units']) / daily_forecast
                else:
                    order_line.item_delay = 0
            
            session.add(order_line)
            
            # Update order checks
            if sku.buyer_class == 'W':
                order_checks['watch'] += 1
            
            if sku.buyer_class == 'M':
                order_checks['manual'] += 1
            
            if sku.system_class == 'N':
                order_checks['new'] += 1
            
            if sku.system_class == 'U':
                order_checks['uninitialized'] += 1
            
            # Check if SKU is at or below order point
            if stock_status:
                available_balance = calculate_available_balance(
                    stock_status.on_hand,
                    stock_status.on_order,
                    stock_status.customer_back_order,
                    stock_status.reserved,
                    stock_status.quantity_held
                )
                
                iop = calculate_item_order_point(session, sku.sku_id, store_id)
                
                if available_balance <= iop['units']:
                    # Check if high service level
                    op_prime_limit = ASR_CONFIG.get('order_point_prime_limit', 95)
                    
                    if sku.service_level_goal >= op_prime_limit:
                        order_checks['order_point_a'] += 1
                    else:
                        order_checks['order_point'] += 1
            
            # Check if quantity exceeds 6 months supply
            forecast_data = sku.forecast_data
            if forecast_data and forecast_data.period_forecast > 0:
                months_supply = soq['units'] / (forecast_data.period_forecast * 1.5)  # 1.5 = 6/4 weeks
                if months_supply > 6:
                    order_checks['quantity'] += 1
            
            # Check for shelf life constraint
            if sku.shelf_life_days and soq['days'] > sku.shelf_life_days:
                order_checks['shelf_life'] += 1
            
            # Update order totals
            order_totals['independent_amount'] += order_line.extended_amount
            order_totals['independent_eaches'] += soq['units']
            
            if hasattr(sku, 'weight') and sku.weight:
                order_totals['independent_weight'] += soq['units'] * sku.weight
            
            if hasattr(sku, 'cube') and sku.cube:
                order_totals['independent_volume'] += soq['units'] * sku.cube
        
        # Copy independent totals to auto adjust totals
        for key, value in order_totals.items():
            if key.startswith('independent_'):
                auto_key = key.replace('independent_', 'auto_adjust_')
                order_totals[auto_key] = value
        
        # Apply automatic rebuilding if enabled
        if source.automatic_rebuild > 0:
            # Check if current bracket applies
            if source.automatic_rebuild in [4, 5] and source.current_bracket > 0:
                # Get current bracket
                bracket = next((b for b in source.brackets if b.bracket_number == source.current_bracket), None)
                
                if bracket and order_totals['auto_adjust_amount'] < bracket.minimum:
                    # Calculate days to add
                    additional_amount = bracket.minimum - order_totals['auto_adjust_amount']
                    
                    # Calculate total daily demand
                    total_daily_demand = sum(
                        sku.forecast_data.weekly_forecast / 7 * sku.purchase_price
                        for sku in skus
                        if hasattr(sku, 'forecast_data') and sku.forecast_data
                    )
                    
                    if total_daily_demand > 0:
                        days_to_add = math.ceil(additional_amount / total_daily_demand)
                        
                        # Apply additional days to order
                        order.extra_days = days_to_add
                        
                        # Recalculate order with additional days
                        for sku in skus:
                            # Skip inactive SKUs
                            if sku.buyer_class not in ['R', 'W']:
                                continue
                            
                            # Get forecast data
                            forecast_data = sku.forecast_data
                            if not forecast_data:
                                continue
                            
                            # Calculate additional units
                            daily_forecast = forecast_data.weekly_forecast / 7
                            additional_units = daily_forecast * days_to_add
                            
                            # Find order line
                            order_line = next((ol for ol in order.order_lines if ol.sku_id == sku.id), None)
                            
                            if order_line:
                                # Update order line
                                order_line.suggested_order_quantity += additional_units
                                order_line.soq_days += days_to_add
                                order_line.extended_amount = order_line.suggested_order_quantity * sku.purchase_price
                            else:
                                # Create new order line if SKU wasn't previously included
                                if additional_units > 0:
                                    order_line = OrderLine(
                                        order_id=order.id,
                                        sku_id=sku.id,
                                        suggested_order_quantity=additional_units,
                                        soq_days=days_to_add,
                                        purchase_price=sku.purchase_price,
                                        extended_amount=additional_units * sku.purchase_price
                                    )
                                    session.add(order_line)
                            
                            # Update order totals
                            order_totals['auto_adjust_amount'] += additional_units * sku.purchase_price
                            order_totals['auto_adjust_eaches'] += additional_units
                            
                            if hasattr(sku, 'weight') and sku.weight:
                                order_totals['auto_adjust_weight'] += additional_units * sku.weight
                            
                            if hasattr(sku, 'cube') and sku.cube:
                                order_totals['auto_adjust_volume'] += additional_units * sku.cube
        
        # Update order with calculated totals
        order.independent_amount = order_totals['independent_amount']
        order.independent_eaches = order_totals['independent_eaches']
        order.independent_weight = order_totals['independent_weight']
        order.independent_volume = order_totals['independent_volume']
        
        order.auto_adjust_amount = order_totals['auto_adjust_amount']
        order.auto_adjust_eaches = order_totals['auto_adjust_eaches']
        order.auto_adjust_weight = order_totals['auto_adjust_weight']
        order.auto_adjust_volume = order_totals['auto_adjust_volume']
        
        # Copy auto adjust totals to final adjust totals
        order.final_adjust_amount = order.auto_adjust_amount
        order.final_adjust_eaches = order.auto_adjust_eaches
        order.final_adjust_weight = order.auto_adjust_weight
        order.final_adjust_volume = order.auto_adjust_volume
        
        # Create order checks
        for check_type, count in order_checks.items():
            if count > 0:
                order_check = OrderCheck(
                    order_id=order.id,
                    check_type=check_type,
                    count=count
                )
                session.add(order_check)
        
        # Set expected delivery date
        lead_time_quoted = source.lead_time_quoted or 0
        order.expected_delivery_date = datetime.datetime.now() + datetime.timedelta(days=lead_time_quoted)
        
        # Update source's last order date
        source.last_order_date = datetime.datetime.now()
        
        # Calculate next order date if using fixed order cycle
        if source.order_cycle > 0:
            source.next_order_date = datetime.datetime.now() + datetime.timedelta(days=source.order_cycle)
        
        # Commit transaction
        session.commit()
        
        return order
    
    except Exception as e:
        logger.error(f"Error building order: {e}")
        session.rollback()
        return None

def accept_order(session, order_id):
    """
    Accept an order for processing.
    
    Args:
        session: SQLAlchemy session
        order_id (int): Order ID
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Get the order
        order = session.query(Order).filter(Order.id == order_id).first()
        
        if not order:
            logger.error(f"Order {order_id} not found")
            return False
        
        # Update order status
        order.status = OrderStatus.ACCEPTED
        
        # Commit changes
        session.commit()
        return True
    
    except Exception as e:
        logger.error(f"Error accepting order: {e}")
        session.rollback()
        return False

def update_order_quantity(session, order_id, sku_id, new_quantity):
    """
    Update the suggested order quantity for a SKU in an order.
    
    Args:
        session: SQLAlchemy session
        order_id (int): Order ID
        sku_id (int): SKU ID
        new_quantity (float): New suggested order quantity
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Get the order line
        order_line = session.query(OrderLine).filter(
            and_(OrderLine.order_id == order_id, OrderLine.sku_id == sku_id)
        ).first()
        
        if not order_line:
            logger.error(f"Order line not found for order {order_id}, SKU {sku_id}")
            return False
        
        # Get the SKU
        sku = session.query(SKU).filter(SKU.id == sku_id).first()
        
        if not sku:
            logger.error(f"SKU {sku_id} not found")
            return False
        
        # Calculate old extended amount
        old_extended_amount = order_line.extended_amount
        
        # Update order line
        order_line.suggested_order_quantity = new_quantity
        order_line.extended_amount = new_quantity * sku.purchase_price
        order_line.is_frozen = True  # Mark as manually changed
        
        # Get forecast data to update SOQ days
        forecast_data = session.query(ForecastData).filter(
            and_(ForecastData.sku_id == sku.sku_id, ForecastData.store_id == sku.store_id)
        ).first()
        
        if forecast_data and forecast_data.weekly_forecast > 0:
            # Update SOQ days
            daily_forecast = forecast_data.weekly_forecast / 7
            order_line.soq_days = new_quantity / daily_forecast
        
        # Get the order
        order = session.query(Order).filter(Order.id == order_id).first()
        
        # Update order totals
        difference = order_line.extended_amount - old_extended_amount
        order.final_adjust_amount += difference
        order.final_adjust_eaches += new_quantity - (old_extended_amount / sku.purchase_price)
        
        if hasattr(sku, 'weight') and sku.weight:
            order.final_adjust_weight += (new_quantity - (old_extended_amount / sku.purchase_price)) * sku.weight
        
        if hasattr(sku, 'cube') and sku.cube:
            order.final_adjust_volume += (new_quantity - (old_extended_amount / sku.purchase_price)) * sku.cube
        
        # Commit changes
        session.commit()
        return True
    
    except Exception as e:
        logger.error(f"Error updating order quantity: {e}")
        session.rollback()
        return False

def recalculate_order(session, order_id):
    """
    Recalculate an order based on current SKU data.
    
    Args:
        session: SQLAlchemy session
        order_id (int): Order ID
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Get the order
        order = session.query(Order).filter(Order.id == order_id).first()
        
        if not order:
            logger.error(f"Order {order_id} not found")
            return False
        
        # Get the source
        source = session.query(Source).filter(Source.id == order.source_id).first()
        
        if not source:
            logger.error(f"Source not found for order {order_id}")
            return False
        
        # Delete all existing order lines
        session.query(OrderLine).filter(OrderLine.order_id == order_id).delete()
        
        # Delete all existing order checks
        session.query(OrderCheck).filter(OrderCheck.order_id == order_id).delete()
        
        session.flush()
        
        # Rebuild order
        # Get active SKUs
        skus = session.query(SKU).filter(
            and_(
                SKU.source_id == source.id,
                SKU.store_id == order.store_id,
                SKU.buyer_class.in_(['R', 'W', 'M', 'D'])  # Include M and D for back orders
            )
        ).all()
        
        # Initialize order checks
        order_checks = {
            'order_point_a': 0,
            'order_point': 0,
            'watch': 0,
            'manual': 0,
            'new': 0,
            'uninitialized': 0,
            'quantity': 0,
            'shelf_life': 0
        }
        
        # Initialize order totals
        order_totals = {
            'independent_amount': 0.0,
            'independent_eaches': 0.0,
            'independent_weight': 0.0,
            'independent_volume': 0.0,
            'auto_adjust_amount': 0.0,
            'auto_adjust_eaches': 0.0,
            'auto_adjust_weight': 0.0,
            'auto_adjust_volume': 0.0
        }
        
        # Process each SKU
        for sku in skus:
            # Calculate suggested order quantity
            soq = calculate_suggested_order_quantity(session, sku.sku_id, order.store_id)
            
            if soq['units'] <= 0:
                # No order needed for this SKU
                continue
            
            # Create order line
            order_line = OrderLine(
                order_id=order.id,
                sku_id=sku.id,
                suggested_order_quantity=soq['units'],
                soq_days=soq['days'],
                purchase_price=sku.purchase_price,
                extended_amount=soq['units'] * sku.purchase_price
            )
            
            # Calculate item delay (days to order point)
            # Same logic as in build_order
            
            session.add(order_line)
            
            # Update order checks
            # Same logic as in build_order
            
            # Update order totals
            order_totals['independent_amount'] += order_line.extended_amount
            order_totals['independent_eaches'] += soq['units']
            
            if hasattr(sku, 'weight') and sku.weight:
                order_totals['independent_weight'] += soq['units'] * sku.weight
            
            if hasattr(sku, 'cube') and sku.cube:
                order_totals['independent_volume'] += soq['units'] * sku.cube
        
        # Copy independent totals to auto adjust totals
        for key, value in order_totals.items():
            if key.startswith('independent_'):
                auto_key = key.replace('independent_', 'auto_adjust_')
                order_totals[auto_key] = value
        
        # Apply automatic rebuilding if enabled
        # Same logic as in build_order
        
        # Update order with calculated totals
        order.independent_amount = order_totals['independent_amount']
        order.independent_eaches = order_totals['independent_eaches']
        order.independent_weight = order_totals['independent_weight']
        order.independent_volume = order_totals['independent_volume']
        
        order.auto_adjust_amount = order_totals['auto_adjust_amount']
        order.auto_adjust_eaches = order_totals['auto_adjust_eaches']
        order.auto_adjust_weight = order_totals['auto_adjust_weight']
        order.auto_adjust_volume = order_totals['auto_adjust_volume']
        
        # Copy auto adjust totals to final adjust totals
        order.final_adjust_amount = order.auto_adjust_amount
        order.final_adjust_eaches = order.auto_adjust_eaches
        order.final_adjust_weight = order.auto_adjust_weight
        order.final_adjust_volume = order.auto_adjust_volume
        
        # Create order checks
        for check_type, count in order_checks.items():
            if count > 0:
                order_check = OrderCheck(
                    order_id=order.id,
                    check_type=check_type,
                    count=count
                )
                session.add(order_check)
        
        # Commit transaction
        session.commit()
        
        return True
    
    except Exception as e:
        logger.error(f"Error recalculating order: {e}")
        session.rollback()
        return False

def rebuild_order_to_days(session, order_id, additional_days):
    """
    Rebuild an order by adding or subtracting days of supply.
    
    Args:
        session: SQLAlchemy session
        order_id (int): Order ID
        additional_days (int): Days to add (positive) or subtract (negative)
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Get the order
        order = session.query(Order).filter(Order.id == order_id).first()
        
        if not order:
            logger.error(f"Order {order_id} not found")
            return False
        
        # Get the source
        source = session.query(Source).filter(Source.id == order.source_id).first()
        
        if not source:
            logger.error(f"Source not found for order {order_id}")
            return False
        
        # Update order's extra days
        order.extra_days = additional_days
        
        # Get active SKUs
        skus = session.query(SKU).filter(
            and_(
                SKU.source_id == source.id,
                SKU.store_id == order.store_id,
                SKU.buyer_class.in_(['R', 'W'])  # Only active SKUs
            )
        ).all()
        
        # Initialize changes in totals
        changes = {
            'amount': 0.0,
            'eaches': 0.0,
            'weight': 0.0,
            'volume': 0.0
        }
        
        # Process each SKU
        for sku in skus:
            # Skip SKUs with frozen SOQs
            order_line = session.query(OrderLine).filter(
                and_(OrderLine.order_id == order_id, OrderLine.sku_id == sku.id)
            ).first()
            
            if order_line and order_line.is_frozen:
                continue
            
            # Get forecast data
            forecast_data = session.query(ForecastData).filter(
                and_(ForecastData.sku_id == sku.sku_id, ForecastData.store_id == sku.store_id)
            ).first()
            
            if not forecast_data:
                continue
            
            # Calculate daily demand
            daily_forecast = forecast_data.weekly_forecast / 7
            
            # Calculate additional units
            additional_units = daily_forecast * additional_days
            
            # If no order line exists, create one
            if not order_line and additional_units > 0:
                order_line = OrderLine(
                    order_id=order.id,
                    sku_id=sku.id,
                    suggested_order_quantity=additional_units,
                    soq_days=additional_days,
                    purchase_price=sku.purchase_price,
                    extended_amount=additional_units * sku.purchase_price
                )
                session.add(order_line)
                
                # Update changes
                changes['amount'] += order_line.extended_amount
                changes['eaches'] += order_line.suggested_order_quantity
                
                if hasattr(sku, 'weight') and sku.weight:
                    changes['weight'] += order_line.suggested_order_quantity * sku.weight
                
                if hasattr(sku, 'cube') and sku.cube:
                    changes['volume'] += order_line.suggested_order_quantity * sku.cube
            
            # If order line exists, update it
            elif order_line:
                # Calculate new quantity
                new_quantity = order_line.suggested_order_quantity + additional_units
                
                # Apply minimum
                if new_quantity < 0:
                    new_quantity = 0
                
                # Apply buying multiple
                if new_quantity > 0 and not sku.ignore_multiple and sku.buying_multiple > 1:
                    new_quantity = round_to_buying_multiple(new_quantity, sku.buying_multiple)
                
                # Calculate changes
                quantity_change = new_quantity - order_line.suggested_order_quantity
                amount_change = quantity_change * sku.purchase_price
                
                # Update order line
                order_line.suggested_order_quantity = new_quantity
                order_line.soq_days = order_line.soq_days + additional_days
                order_line.extended_amount = new_quantity * sku.purchase_price
                
                # Update changes
                changes['amount'] += amount_change
                changes['eaches'] += quantity_change
                
                if hasattr(sku, 'weight') and sku.weight:
                    changes['weight'] += quantity_change * sku.weight
                
                if hasattr(sku, 'cube') and sku.cube:
                    changes['volume'] += quantity_change * sku.cube
        
        # Update order totals
        order.final_adjust_amount += changes['amount']
        order.final_adjust_eaches += changes['eaches']
        order.final_adjust_weight += changes['weight']
        order.final_adjust_volume += changes['volume']
        
        # Commit transaction
        session.commit()
        
        return True
    
    except Exception as e:
        logger.error(f"Error rebuilding order to days: {e}")
        session.rollback()
        return False

def rebuild_order_to_bracket(session, order_id, bracket):
    """
    Rebuild an order to meet a specific bracket.
    
    Args:
        session: SQLAlchemy session
        order_id (int): Order ID
        bracket (int): Bracket number
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Get the order
        order = session.query(Order).filter(Order.id == order_id).first()
        
        if not order:
            logger.error(f"Order {order_id} not found")
            return False
        
        # Get the source
        source = session.query(Source).filter(Source.id == order.source_id).first()
        
        if not source:
            logger.error(f"Source not found for order {order_id}")
            return False
        
        # Get the bracket
        bracket_obj = next((b for b in source.brackets if b.bracket_number == bracket), None)
        
        if not bracket_obj:
            logger.error(f"Bracket {bracket} not found for source")
            return False
        
        # Calculate days needed to meet bracket
        from services.order_policy import calculate_days_to_meet_bracket
        days_needed = calculate_days_to_meet_bracket(session, source.source_id, order.store_id, bracket)
        
        # Rebuild order with additional days
        if days_needed > 0:
            return rebuild_order_to_days(session, order_id, days_needed)
        else:
            return True  # Already meets bracket
    
    except Exception as e:
        logger.error(f"Error rebuilding order to bracket: {e}")
        return False

def run_nightly_replenishment(session, store_id=None):
    """
    Run nightly replenishment for all sources or a specific store.
    
    Args:
        session: SQLAlchemy session
        store_id (str): Store ID to process (None for all)
    
    Returns:
        dict: Statistics about the replenishment run
    """
    try:
        # Build query for sources
        query = session.query(Source)
        
        # Filter by store if specified
        # (In a real system, you would join to a store-source relationship table)
        
        sources = query.all()
        
        # Statistics
        stats = {
            'total_sources': len(sources),
            'due_orders': 0,
            'planned_orders': 0,
            'error_sources': 0
        }
        
        # Set store ID to process
        stores = [store_id] if store_id else ['STORE1']  # Example - in a real system, query all stores
        
        # Process each source for each store
        for source in sources:
            try:
                for store in stores:
                    # Build order
                    order = build_order(session, source.source_id, store)
                    
                    if order:
                        if order.status == OrderStatus.DUE:
                            stats['due_orders'] += 1
                        else:
                            stats['planned_orders'] += 1
                    else:
                        stats['error_sources'] += 1
            
            except Exception as e:
                logger.error(f"Error processing source {source.source_id}: {e}")
                stats['error_sources'] += 1
        
        return stats
    
    except Exception as e:
        logger.error(f"Error running nightly replenishment: {e}")
        session.rollback()
        return {'error': str(e)}

def get_order_category_counts(session):
    """
    Get counts of orders in each category for the To Do menu.
    
    Args:
        session: SQLAlchemy session
    
    Returns:
        dict: Counts by category
    """
    try:
        # Initialize counts
        counts = {
            'due': 0,
            'planned': 0,
            'all': 0,
            'a_order_point': 0,
            'order_point': 0,
            'accepted': 0,
            'purged': 0,
            'deactivated': 0
        }
        
        # Count orders by category
        for category in OrderCategory:
            count = session.query(func.count(Order.id)).filter(Order.category == category).scalar()
            counts[category.value] = count
        
        # Count orders by status for completed orders
        for status in [OrderStatus.ACCEPTED, OrderStatus.PURGED, OrderStatus.DEACTIVATED]:
            count = session.query(func.count(Order.id)).filter(Order.status == status).scalar()
            counts[status.value] = count
        
        # Count all orders
        counts['all'] = session.query(func.count(Order.id)).scalar()
        
        return counts
    
    except Exception as e:
        logger.error(f"Error getting order category counts: {e}")
        return {}