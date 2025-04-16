# warehouse_replenishment/services/order_service.py
from datetime import date, datetime, timedelta
from typing import List, Dict, Tuple, Optional, Union, Any
import logging
import math
import sys
import os
from pathlib import Path

from sqlalchemy import and_, func, or_
from sqlalchemy.orm import Session

# Add the parent directory to the path so we can import our modules
parent_dir = str(Path(__file__).parent.parent.parent)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)
    
    
from warehouse_replenishment.models import (
    Item, Order, OrderItem, Vendor, VendorBracket, Company,
    BuyerClassCode, VendorType
)
from warehouse_replenishment.core.order_policy import (
    analyze_order_policy, calculate_acquisition_cost,
    calculate_vendor_discount_impact
)
from warehouse_replenishment.utils.math_utils import round_to_multiple
from warehouse_replenishment.utils.date_utils import (
    get_next_weekday, add_days, get_next_month_day
)
from warehouse_replenishment.exceptions import OrderError

from warehouse_replenishment.logging_setup import logger
logger = logging.getLogger(__name__)

class OrderService:
    """Service for handling order-related operations."""
    
    def __init__(self, session: Session):
        """Initialize the order service.
        
        Args:
            session: Database session
        """
        self.session = session
        self._company_settings = None
    
    @property
    def company_settings(self) -> Dict:
        """Get company settings.
        
        Returns:
            Dictionary with company settings
        """
        if not self._company_settings:
            company = self.session.query(Company).first()
            if not company:
                raise OrderError("Company settings not found")
            
            self._company_settings = {
                'order_header_cost': company.order_header_cost,
                'order_line_cost': company.order_line_cost,
                'total_carrying_rate': company.total_carrying_rate,
                'forward_buy_maximum': company.forward_buy_maximum,
                'forward_buy_filter': company.forward_buy_filter
            }
        
        return self._company_settings
    
    def get_order(self, order_id: int) -> Optional[Order]:
        """Get an order by ID.
        
        Args:
            order_id: Order ID
            
        Returns:
            Order object or None if not found
        """
        return self.session.query(Order).get(order_id)
    
    def get_orders(
        self,
        warehouse_id: Optional[int] = None,
        vendor_id: Optional[int] = None,
        status: Optional[str] = None,
        is_due: Optional[bool] = None,
        from_date: Optional[date] = None,
        to_date: Optional[date] = None
    ) -> List[Order]:
        """Get orders matching criteria.
        
        Args:
            warehouse_id: Optional warehouse ID filter
            vendor_id: Optional vendor ID filter
            status: Optional order status filter
            is_due: Optional filter for due orders
            from_date: Optional start date filter
            to_date: Optional end date filter
            
        Returns:
            List of order objects
        """
        query = self.session.query(Order)
        
        if warehouse_id is not None:
            query = query.filter(Order.warehouse_id == warehouse_id)
            
        if vendor_id is not None:
            query = query.filter(Order.vendor_id == vendor_id)
            
        if status is not None:
            query = query.filter(Order.status == status)
            
        if is_due is not None:
            query = query.filter(Order.is_due == is_due)
            
        if from_date is not None:
            query = query.filter(Order.order_date >= from_date)
            
        if to_date is not None:
            query = query.filter(Order.order_date <= to_date)
            
        # Order by order date (most recent first)
        query = query.order_by(Order.order_date.desc())
        
        return query.all()
    
    def get_order_items(self, order_id: int) -> List[OrderItem]:
        """Get all items for an order.
        
        Args:
            order_id: Order ID
            
        Returns:
            List of order item objects
        """
        return self.session.query(OrderItem).filter(OrderItem.order_id == order_id).all()
    
    def create_order(
        self,
        vendor_id: int,
        warehouse_id: int,
        order_date: Optional[datetime] = None,
        status: str = 'OPEN',
        is_due: bool = False,
        is_order_point_a: bool = False,
        is_order_point: bool = False,
        order_delay: int = 0
    ) -> int:
        """Create a new order.
        
        Args:
            vendor_id: Vendor ID
            warehouse_id: Warehouse ID
            order_date: Order date (defaults to current date/time)
            status: Order status
            is_due: Whether the order is due
            is_order_point_a: Whether the order is an Order Point A
            is_order_point: Whether the order is an Order Point
            order_delay: Days until order will be due
            
        Returns:
            ID of the created order
        """
        # Check if vendor exists
        vendor = self.session.query(Vendor).get(vendor_id)
        if not vendor:
            raise OrderError(f"Vendor with ID {vendor_id} not found")
            
        # Use current date/time if not provided
        if order_date is None:
            order_date = datetime.now()
            
        # Calculate expected delivery date based on lead time
        lead_time = vendor.lead_time_forecast or 7  # Default to 7 days
        expected_delivery = add_days(order_date.date(), lead_time)
        
        # Create new order
        order = Order(
            vendor_id=vendor_id,
            warehouse_id=warehouse_id,
            order_date=order_date,
            status=status,
            is_due=is_due,
            is_order_point_a=is_order_point_a,
            is_order_point=is_order_point,
            order_delay=order_delay,
            expected_delivery_date=expected_delivery,
            current_bracket=1  # Start with the lowest bracket
        )
        
        self.session.add(order)
        
        try:
            self.session.commit()
            return order.id
        except Exception as e:
            self.session.rollback()
            raise OrderError(f"Failed to create order: {str(e)}")
    
    def add_item_to_order(
        self,
        order_id: int,
        item_id: int,
        soq_units: float,
        is_frozen: bool = False,
        is_order_point: bool = False,
        is_manual: bool = False,
        is_deal: bool = False,
        is_planned: bool = False,
        is_forward_buy: bool = False
    ) -> int:
        """Add an item to an order.
        
        Args:
            order_id: Order ID
            item_id: Item ID
            soq_units: Suggested Order Quantity in units
            is_frozen: Whether the SOQ is frozen
            is_order_point: Whether item is at order point
            is_manual: Whether item was manually added
            is_deal: Whether item is part of a deal
            is_planned: Whether item is part of a plan
            is_forward_buy: Whether item is a forward buy
            
        Returns:
            ID of the created order item
        """
        # Check if order exists
        order = self.get_order(order_id)
        if not order:
            raise OrderError(f"Order with ID {order_id} not found")
            
        # Check if item exists
        item = self.session.query(Item).get(item_id)
        if not item:
            raise OrderError(f"Item with ID {item_id} not found")
            
        # Check if item already exists in order
        existing_item = self.session.query(OrderItem).filter(
            OrderItem.order_id == order_id,
            OrderItem.item_id == item_id
        ).first()
        
        if existing_item:
            raise OrderError(f"Item with ID {item_id} already exists in order {order_id}")
            
        # Calculate SOQ in days
        daily_demand = item.demand_4weekly / 28  # Assuming 28 days in a 4-weekly period
        soq_days = round(soq_units / daily_demand, 1) if daily_demand > 0 else 0
        
        # Create order item
        order_item = OrderItem(
            order_id=order_id,
            item_id=item_id,
            soq_units=soq_units,
            soq_days=soq_days,
            is_frozen=is_frozen,
            is_order_point=is_order_point,
            is_manual=is_manual,
            is_deal=is_deal,
            is_planned=is_planned,
            is_forward_buy=is_forward_buy,
            item_order_point_units=item.item_order_point_units,
            balance_units=item.on_hand + item.on_order,
            order_up_to_level_units=item.order_up_to_level_units
        )
        
        self.session.add(order_item)
        
        # Update order totals
        self._update_order_totals(order)
        
        try:
            self.session.commit()
            return order_item.id
        except Exception as e:
            self.session.rollback()
            raise OrderError(f"Failed to add item to order: {str(e)}")
    
    def remove_item_from_order(
        self,
        order_id: int,
        item_id: int
    ) -> bool:
        """Remove an item from an order.
        
        Args:
            order_id: Order ID
            item_id: Item ID
            
        Returns:
            True if item was removed successfully
        """
        # Check if order exists
        order = self.get_order(order_id)
        if not order:
            raise OrderError(f"Order with ID {order_id} not found")
            
        # Check if order is accepted
        if order.status == 'ACCEPTED':
            raise OrderError("Cannot modify an accepted order")
            
        # Find the order item
        order_item = self.session.query(OrderItem).filter(
            OrderItem.order_id == order_id,
            OrderItem.item_id == item_id
        ).first()
        
        if not order_item:
            raise OrderError(f"Item with ID {item_id} not found in order {order_id}")
            
        # Remove the item
        self.session.delete(order_item)
        
        # Update order totals
        self._update_order_totals(order)
        
        try:
            self.session.commit()
            return True
        except Exception as e:
            self.session.rollback()
            raise OrderError(f"Failed to remove item from order: {str(e)}")
    
    def update_item_soq(
        self,
        order_id: int,
        item_id: int,
        soq_units: float,
        is_frozen: Optional[bool] = None
    ) -> bool:
        """Update Suggested Order Quantity for an item.
        
        Args:
            order_id: Order ID
            item_id: Item ID
            soq_units: New Suggested Order Quantity in units
            is_frozen: Whether the SOQ is frozen
            
        Returns:
            True if SOQ was updated successfully
        """
        # Check if order exists
        order = self.get_order(order_id)
        if not order:
            raise OrderError(f"Order with ID {order_id} not found")
            
        # Check if order is accepted
        if order.status == 'ACCEPTED':
            raise OrderError("Cannot modify an accepted order")
            
        # Find the order item
        order_item = self.session.query(OrderItem).filter(
            OrderItem.order_id == order_id,
            OrderItem.item_id == item_id
        ).first()
        
        if not order_item:
            raise OrderError(f"Item with ID {item_id} not found in order {order_id}")
            
        # Get the item
        item = self.session.query(Item).get(item_id)
        if not item:
            raise OrderError(f"Item with ID {item_id} not found")
            
        # Round to buying multiple if needed
        if item.buying_multiple > 1:
            soq_units = round_to_multiple(soq_units, item.buying_multiple)
            
        # Check if SOQ is less than minimum
        if soq_units < item.minimum_quantity:
            soq_units = item.minimum_quantity
            
        # Update SOQ
        order_item.soq_units = soq_units
        
        # Calculate SOQ in days
        daily_demand = item.demand_4weekly / 28  # Assuming 28 days in a 4-weekly period
        order_item.soq_days = round(soq_units / daily_demand, 1) if daily_demand > 0 else 0
        
        # Update is_frozen if provided
        if is_frozen is not None:
            order_item.is_frozen = is_frozen
            
        # Update order totals
        self._update_order_totals(order)
        
        try:
            self.session.commit()
            return True
        except Exception as e:
            self.session.rollback()
            raise OrderError(f"Failed to update SOQ: {str(e)}")
    
    def _update_order_totals(self, order: Order) -> None:
        """Update order totals based on items.
        
        Args:
            order: Order object
        """
        # Get all order items
        order_items = self.get_order_items(order.id)
        
        # Initialize totals
        independent_amount = 0.0
        independent_eaches = 0.0
        independent_weight = 0.0
        independent_volume = 0.0
        independent_dozens = 0.0
        independent_cases = 0.0
        
        # Calculate totals
        for order_item in order_items:
            # Get item
            item = self.session.query(Item).get(order_item.item_id)
            if not item:
                continue
                
            # Update amounts
            independent_amount += order_item.soq_units * item.purchase_price
            independent_eaches += order_item.soq_units
            independent_weight += order_item.soq_units * (item.weight_per_unit or 0)
            independent_volume += order_item.soq_units * (item.volume_per_unit or 0)
            
            # Calculate dozens
            if item.units_per_case == 12:
                independent_dozens += order_item.soq_units / 12
                
            # Calculate cases
            if item.units_per_case > 0:
                independent_cases += order_item.soq_units / item.units_per_case
        
        # Update independent totals
        order.independent_amount = independent_amount
        order.independent_eaches = independent_eaches
        order.independent_weight = independent_weight
        order.independent_volume = independent_volume
        order.independent_dozens = independent_dozens
        order.independent_cases = independent_cases
        
        # Copy to auto_adj and final_adj totals
        order.auto_adj_amount = independent_amount
        order.auto_adj_eaches = independent_eaches
        order.auto_adj_weight = independent_weight
        order.auto_adj_volume = independent_volume
        order.auto_adj_dozens = independent_dozens
        order.auto_adj_cases = independent_cases
        
        order.final_adj_amount = independent_amount
        order.final_adj_eaches = independent_eaches
        order.final_adj_weight = independent_weight
        order.final_adj_volume = independent_volume
        order.final_adj_dozens = independent_dozens
        order.final_adj_cases = independent_cases
        
        # Update current bracket
        self._update_current_bracket(order)
        
        # Update check counts
        self._update_check_counts(order)
    
    def _update_current_bracket(self, order: Order) -> None:
        """Update the current bracket for an order.
        
        Args:
            order: Order object
        """
        # Get vendor brackets
        brackets = self.session.query(VendorBracket).filter(
            VendorBracket.vendor_id == order.vendor_id
        ).order_by(VendorBracket.bracket_number).all()
        
        if not brackets:
            return
            
        # Find applicable bracket
        current_bracket = 1
        order_amount = order.independent_amount
        
        for bracket in brackets:
            # Check if order amount meets the minimum for this bracket
            if order_amount >= bracket.minimum:
                current_bracket = bracket.bracket_number
                
            # If we exceed the maximum, stop
            if bracket.maximum > 0 and order_amount > bracket.maximum:
                break
                
        # Update current bracket
        order.current_bracket = current_bracket
    
    def _update_check_counts(self, order: Order) -> None:
        """Update check counts for an order.
        
        Args:
            order: Order object
        """
        # Get all order items
        order_items = self.get_order_items(order.id)
        
        # Reset counts
        order.order_point_checks = 0
        order.planned_checks = 0
        order.forward_checks = 0
        order.deal_checks = 0
        order.shelf_life_checks = 0
        order.uninitialized_checks = 0
        order.watch_checks = 0
        
        # Count checks
        for order_item in order_items:
            # Get item
            item = self.session.query(Item).get(order_item.item_id)
            if not item:
                continue
                
            if order_item.is_order_point:
                order.order_point_checks += 1
                
            if order_item.is_planned:
                order.planned_checks += 1
                
            if order_item.is_forward_buy:
                order.forward_checks += 1
                
            if order_item.is_deal:
                order.deal_checks += 1
                
            # Shelf life checks
            if item.shelf_life_days > 0:
                order.shelf_life_checks += 1
                
            # Uninitialized checks
            if item.buyer_class == BuyerClassCode.UNINITIALIZED:
                order.uninitialized_checks += 1
                
            # Watch checks
            if item.buyer_class == BuyerClassCode.WATCH:
                order.watch_checks += 1
    
    def approve_order(
        self,
        order_id: int,
        approval_date: Optional[datetime] = None
    ) -> bool:
        """Approve an order.
        
        Args:
            order_id: Order ID
            approval_date: Approval date (defaults to current date/time)
            
        Returns:
            True if order was approved successfully
        """
        order = self.get_order(order_id)
        if not order:
            raise OrderError(f"Order with ID {order_id} not found")
            
        if order.status == 'ACCEPTED':
            raise OrderError("Order is already accepted")
            
        if approval_date is None:
            approval_date = datetime.now()
            
        # Update order
        order.status = 'ACCEPTED'
        order.approval_date = approval_date
        
        # Get all order items
        order_items = self.get_order_items(order.id)
        
        # Update item on_order quantities
        for order_item in order_items:
            item = self.session.query(Item).get(order_item.item_id)
            if not item:
                continue
                
            item.on_order += order_item.soq_units
        
        try:
            self.session.commit()
            return True
        except Exception as e:
            self.session.rollback()
            raise OrderError(f"Failed to approve order: {str(e)}")
    
    def receive_order(
        self,
        order_id: int,
        receipt_date: Optional[date] = None
    ) -> bool:
        """Mark an order as received.
        
        Args:
            order_id: Order ID
            receipt_date: Receipt date (defaults to current date)
            
        Returns:
            True if order was marked as received successfully
        """
        order = self.get_order(order_id)
        if not order:
            raise OrderError(f"Order with ID {order_id} not found")
            
        if order.status != 'ACCEPTED':
            raise OrderError("Only accepted orders can be received")
            
        if receipt_date is None:
            receipt_date = date.today()
            
        # Get all order items
        order_items = self.get_order_items(order.id)
        
        # Update item inventories
        for order_item in order_items:
            item = self.session.query(Item).get(order_item.item_id)
            if not item:
                continue
                
            # Transfer from on_order to on_hand
            item.on_order -= order_item.soq_units
            item.on_hand += order_item.soq_units
        
        # Update order
        order.status = 'RECEIVED'
        
        try:
            self.session.commit()
            return True
        except Exception as e:
            self.session.rollback()
            raise OrderError(f"Failed to receive order: {str(e)}")
    
    def purge_order(
        self,
        order_id: int
    ) -> bool:
        """Purge (delete) an order.
        
        Args:
            order_id: Order ID
            
        Returns:
            True if order was purged successfully
        """
        order = self.get_order(order_id)
        if not order:
            raise OrderError(f"Order with ID {order_id} not found")
            
        if order.status == 'ACCEPTED':
            # For accepted orders, we need to update item on_order quantities
            order_items = self.get_order_items(order.id)
            
            for order_item in order_items:
                item = self.session.query(Item).get(order_item.item_id)
                if not item:
                    continue
                    
                # Reduce on_order quantity
                item.on_order = max(0, item.on_order - order_item.soq_units)
            
            # Mark order as purged instead of deleting
            order.status = 'PURGED'
            
            try:
                self.session.commit()
                return True
            except Exception as e:
                self.session.rollback()
                raise OrderError(f"Failed to purge order: {str(e)}")
        else:
            # For open orders, we can simply delete
            try:
                # Delete order items first
                self.session.query(OrderItem).filter(
                    OrderItem.order_id == order_id
                ).delete(synchronize_session=False)
                
                # Delete the order
                self.session.delete(order)
                
                self.session.commit()
                return True
            except Exception as e:
                self.session.rollback()
                raise OrderError(f"Failed to purge order: {str(e)}")
    
    def purge_accepted_orders(
        self,
        age_days: int = 90
    ) -> Dict:
        """Purge old accepted orders.
        
        Args:
            age_days: Age threshold in days
            
        Returns:
            Dictionary with purge results
        """
        cutoff_date = date.today() - timedelta(days=age_days)
        
        # Get orders older than cutoff date
        old_orders = self.session.query(Order).filter(
            Order.status == 'ACCEPTED',
            Order.approval_date < cutoff_date
        ).all()
        
        results = {
            'total_orders': len(old_orders),
            'purged_orders': 0,
            'errors': 0
        }
        
        for order in old_orders:
            try:
                # Mark as purged
                order.status = 'PURGED'
                results['purged_orders'] += 1
            except Exception as e:
                logger.error(f"Error purging order {order.id}: {str(e)}")
                results['errors'] += 1
        
        try:
            self.session.commit()
        except Exception as e:
            self.session.rollback()
            logger.error(f"Error committing purged orders: {str(e)}")
            results['errors'] += 1
        
        return results
    
    def add_extra_days(
        self,
        order_id: int,
        extra_days: float
    ) -> bool:
        """Add extra days to an order.
        
        Args:
            order_id: Order ID
            extra_days: Extra days to add
            
        Returns:
            True if extra days were added successfully
        """
        order = self.get_order(order_id)
        if not order:
            raise OrderError(f"Order with ID {order_id} not found")
            
        if order.status == 'ACCEPTED':
            raise OrderError("Cannot modify an accepted order")
            
        # Update extra days
        order.extra_days = extra_days
        
        # Recalculate final adjusted values based on extra days
        # We increase the quantities proportionally to the extra days
        # For example, if extra_days = 10, we add 10/30 = 33% more
        factor = 1.0 + (extra_days / 30.0)
        
        order.final_adj_amount = order.auto_adj_amount * factor
        order.final_adj_eaches = order.auto_adj_eaches * factor
        order.final_adj_weight = order.auto_adj_weight * factor
        order.final_adj_volume = order.auto_adj_volume * factor
        order.final_adj_dozens = order.auto_adj_dozens * factor
        order.final_adj_cases = order.auto_adj_cases * factor
        
        try:
            self.session.commit()
            return True
        except Exception as e:
            self.session.rollback()
            raise OrderError(f"Failed to add extra days: {str(e)}")
    
    def balance_to_bracket(
        self,
        order_id: int,
        target_bracket: int
    ) -> Dict:
        """Balance an order to reach a target bracket.
        
        Args:
            order_id: Order ID
            target_bracket: Target bracket number
            
        Returns:
            Dictionary with balancing results
        """
        order = self.get_order(order_id)
        if not order:
            raise OrderError(f"Order with ID {order_id} not found")
            
        if order.status == 'ACCEPTED':
            raise OrderError("Cannot modify an accepted order")
            
        # Get vendor brackets
        brackets = self.session.query(VendorBracket).filter(
            VendorBracket.vendor_id == order.vendor_id
        ).order_by(VendorBracket.bracket_number).all()
        
        # Find target bracket
        target = None
        for bracket in brackets:
            if bracket.bracket_number == target_bracket:
                target = bracket
                break
                
        if not target:
            raise OrderError(f"Bracket {target_bracket} not found for vendor {order.vendor_id}")
            
        # Current order amount
        current_amount = order.independent_amount
        
        # Calculate amount needed to reach target
        amount_needed = max(0, target.minimum - current_amount)
        
        results = {
            'current_amount': current_amount,
            'target_amount': target.minimum,
            'amount_needed': amount_needed,
            'current_bracket': order.current_bracket,
            'target_bracket': target_bracket,
            'items_adjusted': 0,
            'success': False
        }
        
        if amount_needed <= 0:
            # Already at or above target
            results['success'] = True
            return results
            
        # Get items eligible for increasing
        # - Not frozen
        # - Below their order-up-to level
        eligible_items = self.session.query(OrderItem).join(
            Item, OrderItem.item_id == Item.id
        ).filter(
            OrderItem.order_id == order_id,
            OrderItem.is_frozen == False,
            Item.buyer_class.in_([BuyerClassCode.REGULAR, BuyerClassCode.WATCH])
        ).all()
        
        if not eligible_items:
            return results
            
        # Get items with their details
        item_details = []
        for order_item in eligible_items:
            item = self.session.query(Item).get(order_item.item_id)
            
            if not item:
                continue
                
            # Calculate room to grow
            max_units = item.order_up_to_level_units - (item.on_hand + item.on_order)
            headroom = max(0, max_units - order_item.soq_units)
            
            if headroom <= 0:
                continue
                
            item_details.append({
                'order_item': order_item,
                'item': item,
                'current_soq': order_item.soq_units,
                'headroom': headroom,
                'price': item.purchase_price,
                'daily_demand': item.demand_4weekly / 28,
                'value_ratio': item.demand_4weekly * item.purchase_price  # Value-volume ratio
            })
        
        if not item_details:
            return results
            
        # Sort by value-volume ratio (highest first)
        item_details.sort(key=lambda x: x['value_ratio'], reverse=True)
        
        # Distribute amount needed among items
        remaining_amount = amount_needed
        adjusted_items = 0
        
        for item_detail in item_details:
            if remaining_amount <= 0:
                break
                
            order_item = item_detail['order_item']
            item = item_detail['item']
            price = item_detail['price']
            headroom = item_detail['headroom']
            
            # Calculate how many units we can add to this item
            max_additional_units = min(
                headroom,
                remaining_amount / price
            )
            
            # Round to buying multiple
            if item.buying_multiple > 1:
                max_additional_units = round_to_multiple(max_additional_units, item.buying_multiple)
                
            if max_additional_units <= 0:
                continue
                
            # Update SOQ
            new_soq = order_item.soq_units + max_additional_units
            self.update_item_soq(order_id, item.id, new_soq)
            
            # Update remaining amount
            remaining_amount -= max_additional_units * price
            adjusted_items += 1
        
        # Check if we reached the target
        order = self.get_order(order_id)  # Refresh order
        results['current_amount'] = order.independent_amount
        results['amount_needed'] = max(0, target.minimum - order.independent_amount)
        results['current_bracket'] = order.current_bracket
        results['items_adjusted'] = adjusted_items
        results['success'] = order.current_bracket >= target_bracket
        
        return results
    
    def calculate_suggested_order_quantity(
        self,
        item_id: int,
        force_recalculation: bool = False
    ) -> Dict:
        """Calculate suggested order quantity for an item.
        
        Args:
            item_id: Item ID
            force_recalculation: Whether to force recalculation even if item doesn't need ordering
            
        Returns:
            Dictionary with SOQ calculation results
        """
        item = self.session.query(Item).get(item_id)
        if not item:
            raise OrderError(f"Item with ID {item_id} not found")
            
        # Get vendor for lead time
        vendor = self.session.query(Vendor).get(item.vendor_id)
        if not vendor:
            raise OrderError(f"Vendor with ID {item.vendor_id} not found")
            
        # Calculate current balance
        balance = item.on_hand + item.on_order
        
        # Calculate Item Order Point (IOP) and Order Up To Level (OUTL)
        iop = item.item_order_point_units
        outl = item.order_up_to_level_units
        
        # Initial SOQ calculation
        soq = max(0, outl - balance)
        
        # Round to buying multiple
        if item.buying_multiple > 1 and soq > 0:
            soq = round_to_multiple(soq, item.buying_multiple)
            
        # Check if SOQ is below minimum
        if soq > 0 and soq < item.minimum_quantity:
            soq = item.minimum_quantity
            
        results = {
            'item_id': item_id,
            'balance': balance,
            'iop': iop,
            'outl': outl,
            'soq_units': soq,
            'is_order_point': balance < iop,
            'is_orderable': force_recalculation or balance < iop,
            'buying_multiple': item.buying_multiple,
            'minimum_quantity': item.minimum_quantity
        }
        
        # Calculate SOQ in days if we have demand
        daily_demand = item.demand_4weekly / 28
        if daily_demand > 0:
            results['soq_days'] = round(soq / daily_demand, 1)
        else:
            results['soq_days'] = 0
            
        return results
    
    def generate_vendor_order(
        self,
        vendor_id: int,
        include_watch: bool = True,
        include_manual: bool = True,
        order_date: Optional[datetime] = None
    ) -> Dict:
        """Generate an order for a vendor.
        
        Args:
            vendor_id: Vendor ID
            include_watch: Whether to include Watch items
            include_manual: Whether to include Manual items
            order_date: Order date (defaults to current date/time)
            
        Returns:
            Dictionary with order generation results
        """
        vendor = self.session.query(Vendor).get(vendor_id)
        if not vendor:
            raise OrderError(f"Vendor with ID {vendor_id} not found")
            
        # Use current date/time if not provided
        if order_date is None:
            order_date = datetime.now()
            
        # Get all active items for this vendor
        query = self.session.query(Item).filter(
            Item.vendor_id == vendor_id,
            Item.buyer_class == BuyerClassCode.REGULAR
        )
        
        if include_watch:
            query = query.union(
                self.session.query(Item).filter(
                    Item.vendor_id == vendor_id,
                    Item.buyer_class == BuyerClassCode.WATCH
                )
            )
            
        if include_manual:
            query = query.union(
                self.session.query(Item).filter(
                    Item.vendor_id == vendor_id,
                    Item.buyer_class == BuyerClassCode.MANUAL
                )
            )
            
        items = query.all()
        
        # Check if there are any items
        if not items:
            return {
                'success': False,
                'message': 'No active items found for vendor',
                'vendor_id': vendor_id
            }
            
        # Calculate SOQ for each item
        order_point_items = []
        total_amount = 0.0
        
        for item in items:
            # Calculate SOQ
            soq_result = self.calculate_suggested_order_quantity(item.id)
            
            # Check if item is at order point
            if soq_result['is_order_point'] and soq_result['soq_units'] > 0:
                order_point_items.append({
                    'item': item,
                    'soq_units': soq_result['soq_units'],
                    'soq_days': soq_result['soq_days']
                })
                
                total_amount += soq_result['soq_units'] * item.purchase_price
        
        # Check if there are any items at order point
        if not order_point_items:
            return {
                'success': False,
                'message': 'No items at order point',
                'vendor_id': vendor_id
            }
            
        # Create the order
        order_id = self.create_order(
            vendor_id=vendor_id,
            warehouse_id=vendor.warehouse_id,
            order_date=order_date,
            status='OPEN',
            is_due=True,
            is_order_point=True
        )
        
        # Add items to the order
        for item_data in order_point_items:
            item = item_data['item']
            soq_units = item_data['soq_units']
            
            self.add_item_to_order(
                order_id=order_id,
                item_id=item.id,
                soq_units=soq_units,
                is_order_point=True
            )
        
        return {
            'success': True,
            'order_id': order_id,
            'vendor_id': vendor_id,
            'total_items': len(order_point_items),
            'total_amount': total_amount
        }
    
    def generate_orders(
        self,
        warehouse_id: Optional[int] = None,
        vendor_id: Optional[int] = None,
        include_watch: bool = True,
        include_manual: bool = True
    ) -> Dict:
        """Generate orders for all vendors or a specific vendor.
        
        Args:
            warehouse_id: Optional warehouse ID
            vendor_id: Optional vendor ID
            include_watch: Whether to include Watch items
            include_manual: Whether to include Manual items
            
        Returns:
            Dictionary with order generation results
        """
        # Build query for vendors
        query = self.session.query(Vendor)
        
        if warehouse_id is not None:
            query = query.filter(Vendor.warehouse_id == warehouse_id)
            
        if vendor_id is not None:
            query = query.filter(Vendor.id == vendor_id)
            
        # Only include regular vendors (not transfer, kitting, etc.)
        query = query.filter(Vendor.vendor_type == VendorType.REGULAR)
        
        # Filter out vendors with no active items
        query = query.filter(Vendor.active_items_count > 0)
        
        # Filter out deactivated vendors
        today = date.today()
        query = query.filter(
            (Vendor.deactivate_until.is_(None)) | 
            (Vendor.deactivate_until < today)
        )
        
        vendors = query.all()
        
        # Process results
        results = {
            'total_vendors': len(vendors),
            'generated_orders': 0,
            'total_items': 0,
            'errors': 0,
            'order_details': []
        }
        
        # Process each vendor
        for vendor in vendors:
            try:
                vendor_result = self.generate_vendor_order(
                    vendor_id=vendor.id,
                    include_watch=include_watch,
                    include_manual=include_manual
                )
                
                if vendor_result['success']:
                    results['generated_orders'] += 1
                    results['total_items'] += vendor_result.get('total_items', 0)
                    
                    results['order_details'].append({
                        'order_id': vendor_result['order_id'],
                        'vendor_id': vendor.id,
                        'vendor_name': vendor.name,
                        'total_items': vendor_result.get('total_items', 0)
                    })
            except Exception as e:
                logger.error(f"Error generating order for vendor {vendor.id}: {str(e)}")
                results['errors'] += 1
        
        return results
    
    def determine_next_order_date(
        self,
        vendor_id: int
    ) -> date:
        """Determine the next order date for a vendor.
        
        Args:
            vendor_id: Vendor ID
            
        Returns:
            Next order date
        """
        vendor = self.session.query(Vendor).get(vendor_id)
        if not vendor:
            raise OrderError(f"Vendor with ID {vendor_id} not found")
            
        today = date.today()
        
        # Check if vendor is deactivated
        if vendor.deactivate_until and vendor.deactivate_until > today:
            return vendor.deactivate_until
            
        # Check ordering patterns
        if vendor.order_days_in_week:
            # Order on specific days of the week
            days = [int(day) for day in vendor.order_days_in_week]
            
            # Check week pattern (0=every, 1=odd, 2=even)
            week_num = today.isocalendar()[1]
            
            if vendor.week == 1 and week_num % 2 == 0:
                # Odd weeks only, but we're in an even week
                # Move to next week
                next_monday = today + timedelta(days=(7 - today.weekday()))
                return get_next_weekday(next_monday, days[0])
                
            elif vendor.week == 2 and week_num % 2 == 1:
                # Even weeks only, but we're in an odd week
                # Move to next week
                next_monday = today + timedelta(days=(7 - today.weekday()))
                return get_next_weekday(next_monday, days[0])
                
            else:
                # Get next allowed day
                for day in days:
                    next_day = get_next_weekday(today, day)
                    if next_day > today:
                        return next_day
                
                # If we're already past all days this week, move to first day next week
                next_monday = today + timedelta(days=(7 - today.weekday()))
                return get_next_weekday(next_monday, days[0])
                
        elif vendor.order_day_in_month:
            # Order on specific day of the month
            return get_next_month_day(today, vendor.order_day_in_month)
            
        else:
            # Default to order cycle days from today
            cycle = vendor.order_cycle or 14  # Default to 14 days
            return today + timedelta(days=cycle)
    
    def analyze_vendor_orders(
        self,
        vendor_id: int,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None
    ) -> Dict:
        """Analyze orders for a vendor.
        
        Args:
            vendor_id: Vendor ID
            start_date: Optional start date for analysis
            end_date: Optional end date for analysis
            
        Returns:
            Dictionary with analysis results
        """
        vendor = self.session.query(Vendor).get(vendor_id)
        if not vendor:
            raise OrderError(f"Vendor with ID {vendor_id} not found")
            
        # Use default dates if not provided
        if end_date is None:
            end_date = date.today()
            
        if start_date is None:
            # Default to 90 days before end date
            start_date = end_date - timedelta(days=90)
            
        # Get all orders for this vendor in the date range
        orders = self.session.query(Order).filter(
            Order.vendor_id == vendor_id,
            Order.order_date >= start_date,
            Order.order_date <= end_date
        ).all()
        
        # Process results
        results = {
            'vendor_id': vendor_id,
            'vendor_name': vendor.name,
            'start_date': start_date,
            'end_date': end_date,
            'total_orders': len(orders),
            'accepted_orders': 0,
            'total_amount': 0.0,
            'average_order_amount': 0.0,
            'average_items_per_order': 0.0,
            'bracket_distribution': {}
        }
        
        if not orders:
            return results
            
        # Calculate metrics
        total_amount = 0.0
        total_items = 0
        bracket_counts = {}
        
        for order in orders:
            if order.status == 'ACCEPTED':
                results['accepted_orders'] += 1
                total_amount += order.final_adj_amount
                
                # Count items
                items = self.get_order_items(order.id)
                total_items += len(items)
                
                # Track bracket distribution
                bracket = order.current_bracket
                if bracket not in bracket_counts:
                    bracket_counts[bracket] = 0
                    
                bracket_counts[bracket] += 1
        
        # Calculate averages
        if results['accepted_orders'] > 0:
            results['total_amount'] = total_amount
            results['average_order_amount'] = total_amount / results['accepted_orders']
            results['average_items_per_order'] = total_items / results['accepted_orders']
            
        # Format bracket distribution
        for bracket, count in bracket_counts.items():
            results['bracket_distribution'][f"Bracket {bracket}"] = {
                'count': count,
                'percentage': round((count / results['accepted_orders']) * 100, 2) if results['accepted_orders'] > 0 else 0
            }
            
        return results
    
    def optimize_vendor_cycle(
        self,
        vendor_id: int
    ) -> Dict:
        """Optimize order cycle for a vendor using OPA.
        
        Args:
            vendor_id: Vendor ID
            
        Returns:
            Dictionary with optimization results
        """
        vendor = self.session.query(Vendor).get(vendor_id)
        if not vendor:
            raise OrderError(f"Vendor with ID {vendor_id} not found")
            
        # Get active items for this vendor
        items = self.session.query(Item).filter(
            Item.vendor_id == vendor_id,
            Item.buyer_class.in_([BuyerClassCode.REGULAR, BuyerClassCode.WATCH])
        ).all()
        
        if not items:
            return {
                'success': False,
                'message': 'No active items found for vendor',
                'vendor_id': vendor_id
            }
            
        # Calculate total annual demand value
        total_demand_value = sum(item.demand_yearly * item.purchase_price for item in items)
        
        # Get acquisition costs
        header_cost = vendor.header_cost or self.company_settings['order_header_cost']
        line_cost = vendor.line_cost or self.company_settings['order_line_cost']
        
        # Calculate acquisition cost
        acquisition_cost = calculate_acquisition_cost(
            header_cost, line_cost, len(items)
        )
        
        # Get carrying rate
        carrying_rate = self.company_settings['total_carrying_rate']
        
        # Calculate optimal cycle using OPA
        opa_result = analyze_order_policy(
            demand_forecast=total_demand_value,
            carrying_cost_rate=carrying_rate,
            acquisition_cost=acquisition_cost,
            min_order_quantity=0
        )
        
        # Calculate optimal cycle in days
        orders_per_year = opa_result['num_orders_per_year']
        optimal_cycle = round(365 / orders_per_year)
        
        # Standard cycles to evaluate
        standard_cycles = [7, 14, 21, 28, 42, 56, 84]
        
        # Find closest standard cycle
        closest_cycle = min(standard_cycles, key=lambda x: abs(x - optimal_cycle))
        
        # Current cycle
        current_cycle = vendor.order_cycle or 14  # Default to 14 days if not set
        
        # Calculate bracket impact
        brackets = self.session.query(VendorBracket).filter(
            VendorBracket.vendor_id == vendor_id
        ).order_by(VendorBracket.bracket_number).all()
        
        bracket_impact = {}
        if brackets:
            # Calculate average order size for each cycle
            for cycle in [current_cycle, closest_cycle]:
                orders_per_year = 365 / cycle
                avg_order_size = total_demand_value / orders_per_year
                
                # Determine which bracket this order size would reach
                bracket_num = 1
                discount_pct = 0
                
                for bracket in brackets:
                    if avg_order_size >= bracket.minimum:
                        bracket_num = bracket.bracket_number
                        discount_pct = bracket.discount
                
                bracket_impact[cycle] = {
                    'avg_order_size': avg_order_size,
                    'bracket': bracket_num,
                    'discount_pct': discount_pct
                }
        
        return {
            'success': True,
            'vendor_id': vendor_id,
            'current_cycle': current_cycle,
            'optimal_cycle': optimal_cycle,
            'recommended_cycle': closest_cycle,
            'total_demand_value': total_demand_value,
            'orders_per_year': opa_result['num_orders_per_year'],
            'acquisition_cost': acquisition_cost,
            'carrying_rate': carrying_rate,
            'bracket_impact': bracket_impact,
            'opa_details': opa_result
        }