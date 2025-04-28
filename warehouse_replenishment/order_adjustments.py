#!/usr/bin/env python
# order_adjustments.py - Advanced order adjustment functionality

import sys
import os
import logging
import argparse
import json
from datetime import datetime, date
from decimal import Decimal
from pathlib import Path
from typing import Dict, List, Optional, Union, Any, Tuple

# Add the parent directory to the path so we can import our modules
parent_dir = str(Path(__file__).parent.parent)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from warehouse_replenishment.config import config
from warehouse_replenishment.db import db, session_scope
from warehouse_replenishment.logging_setup import get_logger
from warehouse_replenishment.models import (
    Item, Order, OrderItem, Vendor, VendorBracket, BuyerClassCode, 
    Company, ForecastMethod, SafetyStockType
)
from warehouse_replenishment.services.order_service import OrderService
from warehouse_replenishment.services.item_service import ItemService
from warehouse_replenishment.services.vendor_service import VendorService
from warehouse_replenishment.services.forecast_service import ForecastService
from warehouse_replenishment.services.safety_stock_service import SafetyStockService
from warehouse_replenishment.exceptions import OrderError
from warehouse_replenishment.utils.math_utils import round_to_multiple


class AdvancedOrderAdjustments:
    """Advanced order adjustment functionality extending OrderService."""
    
    def __init__(self, session):
        """Initialize with database session.
        
        Args:
            session: SQLAlchemy database session
        """
        self.session = session
        self.order_service = OrderService(session)
        self.item_service = ItemService(session)
        self.vendor_service = VendorService(session)
        self.forecast_service = None  # Initialize on demand
        self.safety_stock_service = None  # Initialize on demand
        self.logger = get_logger("order_adjustments")
        
        # Get company settings
        self.company = session.query(Company).first()
        if not self.company:
            raise OrderError("Company settings not found")
            
    def _get_forecast_service(self):
        """Get or create forecast service."""
        if not self.forecast_service:
            self.forecast_service = ForecastService(self.session)
        return self.forecast_service
        
    def _get_safety_stock_service(self):
        """Get or create safety stock service."""
        if not self.safety_stock_service:
            self.safety_stock_service = SafetyStockService(self.session)
        return self.safety_stock_service
    
    def rebuild_order(
        self,
        order_id: int,
        preserve_manual_adjustments: bool = True,
        recalculate_order_points: bool = False,
        apply_forward_buy: bool = False,
        forward_buy_days: int = None,
        respect_frozen_items: bool = True,
        update_database: bool = True
    ) -> Dict:
        """Rebuild an order with latest inventory and forecast data.
        
        Args:
            order_id: Order ID
            preserve_manual_adjustments: Whether to preserve manual adjustments
            recalculate_order_points: Whether to recalculate order points
            apply_forward_buy: Whether to apply forward buy logic
            forward_buy_days: Days to use for forward buy (default: company setting)
            respect_frozen_items: Whether to respect frozen items
            update_database: Whether to update the database
            
        Returns:
            Dictionary with rebuild results
        """
        # Get the order
        order = self.order_service.get_order(order_id)
        if not order:
            raise OrderError(f"Order with ID {order_id} not found")
        
        # Check if order can be modified
        if order.status != 'OPEN':
            raise OrderError(f"Cannot rebuild order with status '{order.status}'")
        
        # Get the vendor
        vendor = self.session.query(Vendor).get(order.vendor_id)
        if not vendor:
            raise OrderError(f"Vendor with ID {order.vendor_id} not found")
        
        # Get order items
        order_items = self.order_service.get_order_items(order_id)
        if not order_items:
            return {
                "success": False,
                "message": "Order has no items to rebuild",
                "order_id": order_id
            }
        
        results = {
            "order_id": order_id,
            "vendor_id": order.vendor_id,
            "before": {
                "items_count": len(order_items),
                "total_amount": order.independent_amount,
                "total_eaches": order.independent_eaches,
                "bracket": order.current_bracket
            },
            "after": {},
            "items": {
                "unchanged": 0,
                "increased": 0,
                "decreased": 0,
                "added": 0,
                "removed": 0,
                "frozen": 0,
                "manual": 0,
                "forward_buy": 0
            },
            "details": [],
            "success": False,
            "message": ""
        }
        
        # Track items to be updated (item_id -> new SOQ)
        updates = {}
        
        # If recalculating order points, do it for all items
        if recalculate_order_points:
            self._recalculate_order_points_for_items(order_items)
        
        # Process each order item
        for order_item in order_items:
            # Get the item
            item = self.session.query(Item).get(order_item.item_id)
            if not item:
                results["details"].append({
                    "item_id": order_item.item_id,
                    "status": "ERROR",
                    "message": "Item not found"
                })
                continue
            
            # If item is frozen and we respect frozen items, skip it
            if respect_frozen_items and order_item.is_frozen:
                results["items"]["frozen"] += 1
                results["details"].append({
                    "item_id": item.item_id,
                    "status": "FROZEN",
                    "soq_original": order_item.soq_units,
                    "soq_new": order_item.soq_units,
                    "change": 0,
                    "reason": "Item is frozen"
                })
                # Keep original SOQ
                updates[item.id] = order_item.soq_units
                continue
            
            # If item is manually added and we preserve manual adjustments
            if preserve_manual_adjustments and order_item.is_manual:
                results["items"]["manual"] += 1
                results["details"].append({
                    "item_id": item.item_id,
                    "status": "MANUAL",
                    "soq_original": order_item.soq_units,
                    "soq_new": order_item.soq_units,
                    "change": 0,
                    "reason": "Manual item preserved"
                })
                # Keep original SOQ
                updates[item.id] = order_item.soq_units
                continue
            
            # Calculate new SOQ based on current inventory and order points
            soq_results = self.order_service.calculate_suggested_order_quantity(
                item_id=item.id,
                force_recalculation=True
            )
            
            # Calculate change
            original_soq = order_item.soq_units
            new_soq = soq_results["soq_units"]
            
            # Apply forward buy if requested and relevant
            forward_buy_applied = False
            if apply_forward_buy and new_soq > 0:
                fb_days = forward_buy_days if forward_buy_days is not None else self.company.forward_buy_maximum
                new_soq, forward_buy_applied = self._apply_forward_buy(item, new_soq, fb_days)
            
            # Determine status and update counts
            if original_soq == new_soq:
                status = "UNCHANGED"
                reason = "SOQ unchanged"
                results["items"]["unchanged"] += 1
            elif new_soq > original_soq:
                status = "INCREASED"
                reason = "Increased based on current inventory"
                results["items"]["increased"] += 1
            else:  # new_soq < original_soq
                status = "DECREASED"
                reason = "Decreased based on current inventory"
                results["items"]["decreased"] += 1
            
            # Track forward buy
            if forward_buy_applied:
                status = f"{status}_FORWARD_BUY"
                reason = f"{reason} with forward buy"
                results["items"]["forward_buy"] += 1
            
            # Add to details
            results["details"].append({
                "item_id": item.item_id,
                "status": status,
                "soq_original": original_soq,
                "soq_new": new_soq,
                "change": new_soq - original_soq,
                "is_order_point": soq_results["is_order_point"],
                "reason": reason
            })
            
            # Store the new SOQ for update
            updates[item.id] = new_soq
        
        # Check if there are any additions needed (items at order point but not in order)
        if apply_forward_buy or recalculate_order_points:
            additional_items = self._find_additional_items(order, updates.keys())
            
            for item_id, soq in additional_items.items():
                item = self.session.query(Item).get(item_id)
                
                results["items"]["added"] += 1
                results["details"].append({
                    "item_id": item.item_id,
                    "status": "ADDED",
                    "soq_original": 0,
                    "soq_new": soq,
                    "change": soq,
                    "is_order_point": True,
                    "reason": "Item now at order point"
                })
                
                updates[item_id] = soq
        
        # Apply updates to database if requested
        if update_database:
            self._apply_updates_to_order(order_id, updates)
            
            # Refresh order to get updated totals
            order = self.order_service.get_order(order_id)
            
            results["after"] = {
                "items_count": len(self.order_service.get_order_items(order_id)),
                "total_amount": order.independent_amount,
                "total_eaches": order.independent_eaches,
                "bracket": order.current_bracket
            }
        
        results["success"] = True
        results["message"] = f"Order rebuilt successfully with {len(updates)} item updates"
        
        return results
    
    def _recalculate_order_points_for_items(self, order_items: List[OrderItem]) -> None:
        """Recalculate order points for a list of order items.
        
        Args:
            order_items: List of order items
        """
        ss_service = self._get_safety_stock_service()
        
        for order_item in order_items:
            try:
                # Update safety stock and order points
                ss_service.update_safety_stock_for_item(
                    order_item.item_id,
                    update_sstf=True,
                    update_order_points=True
                )
            except Exception as e:
                self.logger.warning(f"Error recalculating order points for item {order_item.item_id}: {str(e)}")
    
    def _apply_forward_buy(
        self,
        item: Item,
        base_soq: float,
        forward_buy_days: int
    ) -> Tuple[float, bool]:
        """Apply forward buy logic to SOQ.
        
        Args:
            item: Item object
            base_soq: Base SOQ
            forward_buy_days: Maximum forward buy days
            
        Returns:
            Tuple with new SOQ and whether forward buy was applied
        """
        # Calculate daily demand
        daily_demand = item.demand_4weekly / 28  # Assuming 28 days in 4 weeks
        
        # If no demand, can't apply forward buy
        if daily_demand <= 0:
            return base_soq, False
        
        # Get vendor
        vendor = self.session.query(Vendor).get(item.vendor_id)
        if not vendor:
            return base_soq, False
        
        # Calculate current days of supply in the base SOQ
        days_of_supply = base_soq / daily_demand if daily_demand > 0 else 0
        
        # Check if forward buy would exceed max days
        max_days = min(forward_buy_days, self.company.forward_buy_maximum or 60)
        
        # If already exceeding, don't apply forward buy
        if days_of_supply >= max_days:
            return base_soq, False
        
        # Calculate forward buy quantity
        forward_buy_qty = (max_days - days_of_supply) * daily_demand
        
        # Apply minimum filter - only apply forward buy if it's significant
        min_filter = self.company.forward_buy_filter or 30
        if forward_buy_qty < (min_filter * daily_demand):
            return base_soq, False
        
        # Round to buying multiple if needed
        if item.buying_multiple > 1:
            forward_buy_qty = round_to_multiple(forward_buy_qty, item.buying_multiple)
        
        # Apply forward buy
        new_soq = base_soq + forward_buy_qty
        
        return new_soq, True
    
    def _find_additional_items(
        self,
        order: Order,
        existing_item_ids: List[int]
    ) -> Dict[int, float]:
        """Find additional items that should be in the order but aren't.
        
        Args:
            order: Order object
            existing_item_ids: List of existing item IDs in the order
            
        Returns:
            Dictionary mapping item IDs to SOQs
        """
        additional_items = {}
        
        # Get all items for this vendor
        vendor_items = self.vendor_service.get_vendor_items(order.vendor_id)
        
        # Filter to only active items not already in the order
        for item in vendor_items:
            if item.id not in existing_item_ids and item.buyer_class in [BuyerClassCode.REGULAR, BuyerClassCode.WATCH]:
                # Calculate SOQ
                soq_results = self.order_service.calculate_suggested_order_quantity(
                    item_id=item.id,
                    force_recalculation=False  # Only include if genuinely at order point
                )
                
                # Only add if orderable
                if soq_results["is_orderable"] and soq_results["soq_units"] > 0:
                    additional_items[item.id] = soq_results["soq_units"]
        
        return additional_items
    
    def _apply_updates_to_order(
        self,
        order_id: int,
        updates: Dict[int, float]
    ) -> None:
        """Apply updates to an order.
        
        Args:
            order_id: Order ID
            updates: Dictionary mapping item IDs to new SOQs
        """
        # Get the order
        order = self.order_service.get_order(order_id)
        if not order:
            raise OrderError(f"Order with ID {order_id} not found")
        
        # Get existing order items
        order_items = {oi.item_id: oi for oi in self.order_service.get_order_items(order_id)}
        
        # Process updates
        for item_id, new_soq in updates.items():
            if item_id in order_items:
                # Item exists - update SOQ
                if new_soq <= 0:
                    # Remove item if SOQ is zero or negative
                    try:
                        self.order_service.remove_item_from_order(order_id, item_id)
                    except Exception as e:
                        self.logger.error(f"Error removing item {item_id} from order {order_id}: {str(e)}")
                else:
                    # Update SOQ
                    try:
                        self.order_service.update_item_soq(order_id, item_id, new_soq)
                    except Exception as e:
                        self.logger.error(f"Error updating SOQ for item {item_id} in order {order_id}: {str(e)}")
            else:
                # Item doesn't exist - add it if SOQ is positive
                if new_soq > 0:
                    try:
                        # Get item to determine if it's at order point
                        item = self.session.query(Item).get(item_id)
                        is_order_point = item.on_hand + item.on_order < item.item_order_point_units
                        
                        self.order_service.add_item_to_order(
                            order_id=order_id,
                            item_id=item_id,
                            soq_units=new_soq,
                            is_order_point=is_order_point
                        )
                    except Exception as e:
                        self.logger.error(f"Error adding item {item_id} to order {order_id}: {str(e)}")
    
    def apply_percentage_adjustment(
        self,
        order_id: int,
        percentage: float,
        item_ids: List[int] = None,
        adjustment_type: str = 'ALL'  # ALL, NON_FROZEN, ORDER_POINT_ONLY
    ) -> Dict:
        """Apply a percentage adjustment to items in an order.
        
        Args:
            order_id: Order ID
            percentage: Percentage adjustment (e.g., 10.0 for 10% increase)
            item_ids: Optional list of specific item IDs to adjust
            adjustment_type: Type of adjustment to apply
            
        Returns:
            Dictionary with adjustment results
        """
        # Get the order
        order = self.order_service.get_order(order_id)
        if not order:
            raise OrderError(f"Order with ID {order_id} not found")
        
        # Check if order can be modified
        if order.status != 'OPEN':
            raise OrderError(f"Cannot adjust order with status '{order.status}'")
        
        # Get order items
        order_items = self.order_service.get_order_items(order_id)
        if not order_items:
            return {
                "success": False,
                "message": "Order has no items to adjust",
                "order_id": order_id
            }
        
        # Filter items based on parameters
        if item_ids:
            order_items = [oi for oi in order_items if oi.item_id in item_ids]
            
        if adjustment_type == 'NON_FROZEN':
            order_items = [oi for oi in order_items if not oi.is_frozen]
            
        elif adjustment_type == 'ORDER_POINT_ONLY':
            order_items = [oi for oi in order_items if oi.is_order_point]
        
        if not order_items:
            return {
                "success": False,
                "message": f"No matching items found for adjustment type: {adjustment_type}",
                "order_id": order_id
            }
        
        # Calculate adjustment multiplier
        multiplier = 1.0 + (percentage / 100.0)
        
        # Track results
        results = {
            "order_id": order_id,
            "percentage": percentage,
            "adjustment_type": adjustment_type,
            "items_adjusted": 0,
            "original_total": order.independent_amount,
            "new_total": 0,
            "details": [],
            "success": False
        }
        
        # Process each order item
        for order_item in order_items:
            # Get the item
            item = self.session.query(Item).get(order_item.item_id)
            if not item:
                continue
            
            # Calculate new SOQ
            original_soq = order_item.soq_units
            new_soq = original_soq * multiplier
            
            # Round to buying multiple if needed
            if item.buying_multiple > 1:
                new_soq = round_to_multiple(new_soq, item.buying_multiple)
            
            # Ensure minimum quantity
            if new_soq < item.minimum_quantity and new_soq > 0:
                new_soq = item.minimum_quantity
            
            # Update the SOQ
            try:
                self.order_service.update_item_soq(order_id, order_item.item_id, new_soq)
                results["items_adjusted"] += 1
                
                # Add to details
                results["details"].append({
                    "item_id": item.item_id,
                    "soq_original": original_soq,
                    "soq_new": new_soq,
                    "change": new_soq - original_soq,
                    "change_pct": ((new_soq / original_soq) - 1) * 100 if original_soq > 0 else 0
                })
                
            except Exception as e:
                self.logger.error(f"Error adjusting SOQ for item {item.item_id}: {str(e)}")
        
        # Get updated order totals
        updated_order = self.order_service.get_order(order_id)
        results["new_total"] = updated_order.independent_amount
        results["success"] = True
        results["message"] = f"Applied {percentage}% adjustment to {results['items_adjusted']} items"
        
        return results
    
    def optimize_order_to_bracket(
        self,
        order_id: int,
        target_bracket: int = None,
        strategy: str = 'BALANCED'  # BALANCED, MIN_INVENTORY, PREFERRED_ITEMS
    ) -> Dict:
        """Optimize an order to reach a specific bracket or the next available bracket.
        
        Args:
            order_id: Order ID
            target_bracket: Target bracket number (or next available if None)
            strategy: Optimization strategy
            
        Returns:
            Dictionary with optimization results
        """
        # Get the order
        order = self.order_service.get_order(order_id)
        if not order:
            raise OrderError(f"Order with ID {order_id} not found")
        
        # Check if order can be modified
        if order.status != 'OPEN':
            raise OrderError(f"Cannot optimize order with status '{order.status}'")
        
        # Get vendor brackets
        vendor_id = order.vendor_id
        brackets = self.session.query(VendorBracket).filter(
            VendorBracket.vendor_id == vendor_id
        ).order_by(VendorBracket.bracket_number).all()
        
        if not brackets:
            return {
                "success": False,
                "message": "No brackets defined for this vendor",
                "order_id": order_id
            }
        
        # Determine target bracket
        current_bracket = order.current_bracket
        current_amount = order.independent_amount
        
        # If no target specified, use next bracket
        if target_bracket is None:
            # Find next bracket
            for bracket in brackets:
                if bracket.bracket_number > current_bracket:
                    target_bracket = bracket.bracket_number
                    break
            
            # If no higher bracket found, we're already at the highest
            if target_bracket is None:
                return {
                    "success": True,
                    "message": "Already at highest bracket",
                    "order_id": order_id,
                    "current_bracket": current_bracket,
                    "target_bracket": current_bracket,
                    "current_amount": current_amount
                }
        
        # Find target bracket details
        target_bracket_obj = None
        for bracket in brackets:
            if bracket.bracket_number == target_bracket:
                target_bracket_obj = bracket
                break
        
        if not target_bracket_obj:
            return {
                "success": False,
                "message": f"Target bracket {target_bracket} not found",
                "order_id": order_id
            }
        
        # Calculate amount needed to reach target
        target_amount = target_bracket_obj.minimum
        amount_needed = max(0, target_amount - current_amount)
        
        if amount_needed <= 0:
            return {
                "success": True,
                "message": f"Already at or above target bracket {target_bracket}",
                "order_id": order_id,
                "current_bracket": current_bracket,
                "target_bracket": target_bracket,
                "current_amount": current_amount,
                "target_amount": target_amount
            }
        
        # Get items eligible for increasing
        order_items = self.order_service.get_order_items(order_id)
        if not order_items:
            return {
                "success": False,
                "message": "Order has no items to optimize",
                "order_id": order_id
            }
        
        # Prepare results
        results = {
            "order_id": order_id,
            "current_bracket": current_bracket,
            "target_bracket": target_bracket,
            "current_amount": current_amount,
            "target_amount": target_amount,
            "amount_needed": amount_needed,
            "strategy": strategy,
            "items_adjusted": 0,
            "original_total": current_amount,
            "new_total": current_amount,
            "details": [],
            "success": False
        }
        
        # Apply optimization strategy
        if strategy == 'BALANCED':
            success = self._optimize_balanced(order_id, order_items, amount_needed, results)
        elif strategy == 'MIN_INVENTORY':
            success = self._optimize_min_inventory(order_id, order_items, amount_needed, results)
        elif strategy == 'PREFERRED_ITEMS':
            success = self._optimize_preferred_items(order_id, order_items, amount_needed, results)
        else:
            return {
                "success": False,
                "message": f"Unknown optimization strategy: {strategy}",
                "order_id": order_id
            }
        
        # Get updated order totals
        updated_order = self.order_service.get_order(order_id)
        results["new_total"] = updated_order.independent_amount
        results["success"] = success
        
        if success:
            results["message"] = f"Successfully optimized order to reach bracket {target_bracket}"
        else:
            results["message"] = f"Unable to fully optimize order to reach bracket {target_bracket}"
        
        return results
    
    def _optimize_balanced(
        self,
        order_id: int,
        order_items: List[OrderItem],
        amount_needed: float,
        results: Dict
    ) -> bool:
        """Apply balanced optimization strategy to reach target amount.
        
        Args:
            order_id: Order ID
            order_items: List of order items
            amount_needed: Amount needed to reach target
            results: Results dictionary to update
            
        Returns:
            True if optimization was successful
        """
        # Get eligible items (not frozen)
        eligible_items = []
        
        for order_item in order_items:
            if order_item.is_frozen:
                continue
                
            item = self.session.query(Item).get(order_item.item_id)
            if not item:
                continue
                
            # Calculate room to grow
            current_balance = item.on_hand + item.on_order
            max_units = item.order_up_to_level_units - current_balance + order_item.soq_units
            headroom = max(0, max_units - order_item.soq_units)
            
            if headroom <= 0:
                continue
                
            eligible_items.append({
                "order_item": order_item,
                "item": item,
                "current_soq": order_item.soq_units,
                "headroom": headroom,
                "price": item.purchase_price,
                "daily_demand": item.demand_4weekly / 28
            })
        
        if not eligible_items:
            return False
        
        # Calculate total headroom value
        total_headroom_value = sum(item["headroom"] * item["price"] for item in eligible_items)
        
        # If not enough headroom, we can't reach the target
        if total_headroom_value < amount_needed:
            return False
        
        # Distribute amount needed proportionally based on headroom
        remaining_amount = amount_needed
        
        for item_detail in eligible_items:
            # Skip if no more amount needed
            if remaining_amount <= 0:
                break
                
            order_item = item_detail["order_item"]
            item = item_detail["item"]
            headroom = item_detail["headroom"]
            price = item_detail["price"]
            
            # Calculate fair share proportion
            proportion = (headroom * price) / total_headroom_value
            
            # Calculate additional units based on proportion
            additional_value = proportion * amount_needed
            additional_units = additional_value / price
            
            # Round to buying multiple
            if item.buying_multiple > 1:
                additional_units = round_to_multiple(additional_units, item.buying_multiple)
            
            # Calculate new SOQ
            new_soq = order_item.soq_units + additional_units
            
            # Update SOQ
            try:
                self.order_service.update_item_soq(order_id, item.id, new_soq)
                results["items_adjusted"] += 1
                
                # Add to details
                results["details"].append({
                    "item_id": item.item_id,
                    "soq_original": order_item.soq_units,
                    "soq_new": new_soq,
                    "change": new_soq - order_item.soq_units,
                    "change_pct": ((new_soq / order_item.soq_units) - 1) * 100 if order_item.soq_units > 0 else 0,
                    "additional_value": additional_units * price
                })
                
                # Update remaining amount
                remaining_amount -= (additional_units * price)
                
            except Exception as e:
                self.logger.error(f"Error optimizing SOQ for item {item.item_id}: {str(e)}")
        
        # Return success if we've reached the target amount
        return remaining_amount <= 0
    
    def _optimize_min_inventory(
        self,
        order_id: int,
        order_items: List[OrderItem],
        amount_needed: float,
        results: Dict
    ) -> bool:
        """Apply minimum inventory optimization strategy to reach target amount.
        
        Args:
            order_id: Order ID
            order_items: List of order items
            amount_needed: Amount needed to reach target
            results: Results dictionary to update
            
        Returns:
            True if optimization was successful
        """
        # Get eligible items (not frozen)
        eligible_items = []
        
        for order_item in order_items:
            if order_item.is_frozen:
                continue
                
            item = self.session.query(Item).get(order_item.item_id)
            if not item:
                continue
                
            # Calculate room to grow
            current_balance = item.on_hand + item.on_order
            max_units = item.order_up_to_level_units - current_balance + order_item.soq_units
            headroom = max(0, max_units - order_item.soq_units)
            
            if headroom <= 0:
                continue
                
            eligible_items.append({
                "order_item": order_item,
                "item": item,
                "current_soq": order_item.soq_units,
                "headroom": headroom,
                "price": item.purchase_price,
                "daily_demand": item.demand_4weekly / 28,
                "days_supply": (item.on_hand + item.on_order) / item.demand_4weekly * 28 if item.demand_4weekly > 0 else float('inf')
            })
        
        if not eligible_items:
            return False
        
        # Sort by days of supply (ascending) - prioritize items with low inventory
        eligible_items.sort(key=lambda x: x["days_supply"])
        
        # Distribute amount needed starting with lowest inventory items
        remaining_amount = amount_needed
        
        for item_detail in eligible_items:
            # Skip if no more amount needed
            if remaining_amount <= 0:
                break
                
            order_item = item_detail["order_item"]
            item = item_detail["item"]
            headroom = item_detail["headroom"]
            price = item_detail["price"]
            
            # Calculate additional units based on available amount
            max_additional_value = min(headroom * price, remaining_amount)
            additional_units = max_additional_value / price
            
            # Round to buying multiple
            if item.buying_multiple > 1:
                additional_units = round_to_multiple(additional_units, item.buying_multiple)
            
            # Calculate new SOQ
            new_soq = order_item.soq_units + additional_units
            
            # Update SOQ
            try:
                self.order_service.update_item_soq(order_id, item.id, new_soq)
                results["items_adjusted"] += 1
                
                # Add to details
                results["details"].append({
                    "item_id": item.item_id,
                    "soq_original": order_item.soq_units,
                    "soq_new": new_soq,
                    "change": new_soq - order_item.soq_units,
                    "change_pct": ((new_soq / order_item.soq_units) - 1) * 100 if order_item.soq_units > 0 else 0,
                    "additional_value": additional_units * price,
                    "days_supply": item_detail["days_supply"]
                })
                
                # Update remaining amount
                remaining_amount -= (additional_units * price)
                
            except Exception as e:
                self.logger.error(f"Error optimizing SOQ for item {item.item_id}: {str(e)}")
        
        # Return success if we've reached the target amount
        return remaining_amount <= 0
    
    def _optimize_preferred_items(
        self,
        order_id: int,
        order_items: List[OrderItem],
        amount_needed: float,
        results: Dict
    ) -> bool:
        """Apply preferred items optimization strategy to reach target amount.
        
        Args:
            order_id: Order ID
            order_items: List of order items
            amount_needed: Amount needed to reach target
            results: Results dictionary to update
            
        Returns:
            True if optimization was successful
        """
        # Get eligible items (not frozen)
        eligible_items = []
        
        for order_item in order_items:
            if order_item.is_frozen:
                continue
                
            item = self.session.query(Item).get(order_item.item_id)
            if not item:
                continue
                
            # Calculate room to grow
            current_balance = item.on_hand + item.on_order
            max_units = item.order_up_to_level_units - current_balance + order_item.soq_units
            headroom = max(0, max_units - order_item.soq_units)
            
            if headroom <= 0:
                continue
                
            # Calculate metrics for prioritization
            gross_margin = (item.sales_price - item.purchase_price) / item.purchase_price * 100 if item.purchase_price > 0 else 0
            turnover = item.demand_yearly / item.on_hand if item.on_hand > 0 else 0
                
            eligible_items.append({
                "order_item": order_item,
                "item": item,
                "current_soq": order_item.soq_units,
                "headroom": headroom,
                "price": item.purchase_price,
                "daily_demand": item.demand_4weekly / 28,
                "gross_margin": gross_margin,
                "turnover": turnover,
                "score": gross_margin * turnover  # Combined score for ranking
            })
        
        if not eligible_items:
            return False
        
        # Sort by score (descending) - prioritize high margin, high turnover items
        eligible_items.sort(key=lambda x: x["score"], reverse=True)
        
        # Distribute amount needed starting with highest scoring items
        remaining_amount = amount_needed
        
        for item_detail in eligible_items:
            # Skip if no more amount needed
            if remaining_amount <= 0:
                break
                
            order_item = item_detail["order_item"]
            item = item_detail["item"]
            headroom = item_detail["headroom"]
            price = item_detail["price"]
            
            # Calculate additional units based on available amount
            max_additional_value = min(headroom * price, remaining_amount)
            additional_units = max_additional_value / price
            
            # Round to buying multiple
            if item.buying_multiple > 1:
                additional_units = round_to_multiple(additional_units, item.buying_multiple)
            
            # Calculate new SOQ
            new_soq = order_item.soq_units + additional_units
            
            # Update SOQ
            try:
                self.order_service.update_item_soq(order_id, item.id, new_soq)
                results["items_adjusted"] += 1
                
                # Add to details
                results["details"].append({
                    "item_id": item.item_id,
                    "soq_original": order_item.soq_units,
                    "soq_new": new_soq,
                    "change": new_soq - order_item.soq_units,
                    "change_pct": ((new_soq / order_item.soq_units) - 1) * 100 if order_item.soq_units > 0 else 0,
                    "additional_value": additional_units * price,
                    "margin": item_detail["gross_margin"],
                    "turnover": item_detail["turnover"],
                    "score": item_detail["score"]
                })
                
                # Update remaining amount
                remaining_amount -= (additional_units * price)
                
            except Exception as e:
                self.logger.error(f"Error optimizing SOQ for item {item.item_id}: {str(e)}")
        
        # Return success if we've reached the target amount
        return remaining_amount <= 0
    
    def apply_forward_buy(
        self,
        order_id: int,
        forward_buy_days: int = None,
        item_ids: List[int] = None,
        min_days_filter: int = None
    ) -> Dict:
        """Apply forward buy to items in an order.
        
        Args:
            order_id: Order ID
            forward_buy_days: Days to use for forward buy (default: company setting)
            item_ids: Optional list of specific item IDs to adjust
            min_days_filter: Minimum days filter for forward buy (default: company setting)
            
        Returns:
            Dictionary with forward buy results
        """
        # Get the order
        order = self.order_service.get_order(order_id)
        if not order:
            raise OrderError(f"Order with ID {order_id} not found")
        
        # Check if order can be modified
        if order.status != 'OPEN':
            raise OrderError(f"Cannot apply forward buy to order with status '{order.status}'")
        
        # Get order items
        order_items = self.order_service.get_order_items(order_id)
        if not order_items:
            return {
                "success": False,
                "message": "Order has no items for forward buy",
                "order_id": order_id
            }
        
        # Set defaults from company settings if not provided
        if forward_buy_days is None:
            forward_buy_days = self.company.forward_buy_maximum or 60
            
        if min_days_filter is None:
            min_days_filter = self.company.forward_buy_filter or 30
            
        # Filter items if specific IDs provided
        if item_ids:
            order_items = [oi for oi in order_items if oi.item_id in item_ids]
            
        if not order_items:
            return {
                "success": False,
                "message": "No matching items found for forward buy",
                "order_id": order_id
            }
        
        # Prepare results
        results = {
            "order_id": order_id,
            "forward_buy_days": forward_buy_days,
            "min_days_filter": min_days_filter,
            "items_processed": 0,
            "items_forward_bought": 0,
            "original_total": order.independent_amount,
            "new_total": 0,
            "details": [],
            "success": False
        }
        
        # Process each order item
        for order_item in order_items:
            # Skip frozen items
            if order_item.is_frozen:
                continue
                
            # Get the item
            item = self.session.query(Item).get(order_item.item_id)
            if not item:
                continue
                
            results["items_processed"] += 1
            
            # Apply forward buy
            original_soq = order_item.soq_units
            new_soq, forward_buy_applied = self._apply_forward_buy(
                item, original_soq, forward_buy_days
            )
            
            if not forward_buy_applied:
                continue
                
            # Update the SOQ
            try:
                self.order_service.update_item_soq(order_id, order_item.item_id, new_soq)
                results["items_forward_bought"] += 1
                
                # Calculate days of supply
                daily_demand = item.demand_4weekly / 28
                original_days = original_soq / daily_demand if daily_demand > 0 else 0
                new_days = new_soq / daily_demand if daily_demand > 0 else 0
                
                # Add to details
                results["details"].append({
                    "item_id": item.item_id,
                    "soq_original": original_soq,
                    "soq_new": new_soq,
                    "change": new_soq - original_soq,
                    "change_pct": ((new_soq / original_soq) - 1) * 100 if original_soq > 0 else 0,
                    "additional_value": (new_soq - original_soq) * item.purchase_price,
                    "original_days": original_days,
                    "new_days": new_days
                })
                
            except Exception as e:
                self.logger.error(f"Error applying forward buy for item {item.item_id}: {str(e)}")
        
        # Get updated order totals
        updated_order = self.order_service.get_order(order_id)
        results["new_total"] = updated_order.independent_amount
        results["success"] = True
        results["message"] = f"Applied forward buy to {results['items_forward_bought']} of {results['items_processed']} items"
        
        return results


def setup_logging():
    """Setup logging for the script."""
    log_level = logging.INFO
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    log_dir = Path(parent_dir) / 'logs'
    log_dir.mkdir(exist_ok=True)
    
    logging.basicConfig(
        level=log_level,
        format=log_format,
        handlers=[
            logging.FileHandler(log_dir / f'order_adjustments_{datetime.now().strftime("%Y%m%d")}.log'),
            logging.StreamHandler()
        ]
    )
    
    return get_logger('order_adjustments')

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Advanced order adjustment functionality')
    
    subparsers = parser.add_subparsers(dest='command', help='Command')
    
    # Rebuild order command
    rebuild_parser = subparsers.add_parser('rebuild', help='Rebuild an order')
    rebuild_parser.add_argument('--order-id', type=int, required=True, help='Order ID')
    rebuild_parser.add_argument('--ignore-manual', action='store_true', help='Ignore manual adjustments')
    rebuild_parser.add_argument('--recalc-order-points', action='store_true', help='Recalculate order points')
    rebuild_parser.add_argument('--forward-buy', action='store_true', help='Apply forward buy')
    rebuild_parser.add_argument('--forward-buy-days', type=int, help='Forward buy days')
    rebuild_parser.add_argument('--ignore-frozen', action='store_true', help='Ignore frozen items')
    rebuild_parser.add_argument('--dry-run', action='store_true', help='Dry run (no database updates)')
    rebuild_parser.add_argument('--verbose', '-v', action='store_true', help='Verbose output')
    
    # Percentage adjustment command
    pct_parser = subparsers.add_parser('adjust-pct', help='Apply percentage adjustment')
    pct_parser.add_argument('--order-id', type=int, required=True, help='Order ID')
    pct_parser.add_argument('--percentage', type=float, required=True, help='Percentage adjustment')
    pct_parser.add_argument('--items', type=int, nargs='*', help='Specific item IDs')
    pct_parser.add_argument('--type', choices=['ALL', 'NON_FROZEN', 'ORDER_POINT_ONLY'], 
                           default='ALL', help='Adjustment type')
    pct_parser.add_argument('--verbose', '-v', action='store_true', help='Verbose output')
    
    # Bracket optimization command
    bracket_parser = subparsers.add_parser('optimize-bracket', help='Optimize order to bracket')
    bracket_parser.add_argument('--order-id', type=int, required=True, help='Order ID')
    bracket_parser.add_argument('--target-bracket', type=int, help='Target bracket number')
    bracket_parser.add_argument('--strategy', choices=['BALANCED', 'MIN_INVENTORY', 'PREFERRED_ITEMS'],
                              default='BALANCED', help='Optimization strategy')
    bracket_parser.add_argument('--verbose', '-v', action='store_true', help='Verbose output')
    
    # Forward buy command
    fb_parser = subparsers.add_parser('forward-buy', help='Apply forward buy')
    fb_parser.add_argument('--order-id', type=int, required=True, help='Order ID')
    fb_parser.add_argument('--days', type=int, help='Forward buy days')
    fb_parser.add_argument('--items', type=int, nargs='*', help='Specific item IDs')
    fb_parser.add_argument('--min-filter', type=int, help='Minimum days filter')
    fb_parser.add_argument('--verbose', '-v', action='store_true', help='Verbose output')
    
    return parser.parse_args()

def main():
    """Main entry point."""
    args = parse_args()
    logger = setup_logging()
    
    if not args.command:
        print("Please specify a command. Use -h for help.")
        sys.exit(1)
    
    logger.info(f"Starting order adjustments with command: {args.command}")
    
    try:
        # Initialize database
        db.initialize()
        
        with session_scope() as session:
            # Create advanced order adjustments service
            adj_service = AdvancedOrderAdjustments(session)
            
            # Process command
            if args.command == 'rebuild':
                result = adj_service.rebuild_order(
                    order_id=args.order_id,
                    preserve_manual_adjustments=not args.ignore_manual,
                    recalculate_order_points=args.recalc_order_points,
                    apply_forward_buy=args.forward_buy,
                    forward_buy_days=args.forward_buy_days,
                    respect_frozen_items=not args.ignore_frozen,
                    update_database=not args.dry_run
                )
                
                # Output results
                print(f"Order rebuild for order {args.order_id}: {'SUCCESS' if result['success'] else 'FAILED'}")
                print(f"Message: {result['message']}")
                
                if 'before' in result and 'after' in result:
                    print("\nBefore rebuild:")
                    print(f"  Items: {result['before']['items_count']}")
                    print(f"  Total: ${result['before']['total_amount']:.2f}")
                    print(f"  Bracket: {result['before']['bracket']}")
                    
                    if not args.dry_run:
                        print("\nAfter rebuild:")
                        print(f"  Items: {result['after']['items_count']}")
                        print(f"  Total: ${result['after']['total_amount']:.2f}")
                        print(f"  Bracket: {result['after']['bracket']}")
                
                if 'items' in result:
                    print("\nItem changes:")
                    print(f"  Unchanged: {result['items']['unchanged']}")
                    print(f"  Increased: {result['items']['increased']}")
                    print(f"  Decreased: {result['items']['decreased']}")
                    print(f"  Added: {result['items']['added']}")
                    print(f"  Removed: {result['items']['removed']}")
                    print(f"  Frozen: {result['items']['frozen']}")
                    print(f"  Manual: {result['items']['manual']}")
                    print(f"  Forward Buy: {result['items']['forward_buy']}")
                
                if args.verbose and 'details' in result:
                    print("\nItem details:")
                    for item in sorted(result['details'], key=lambda x: abs(x.get('change', 0)), reverse=True):
                        print(f"  {item['item_id']}: {item['soq_original']} → {item['soq_new']} ({item['change']:+.2f}) - {item['status']}")
            
            elif args.command == 'adjust-pct':
                result = adj_service.apply_percentage_adjustment(
                    order_id=args.order_id,
                    percentage=args.percentage,
                    item_ids=args.items,
                    adjustment_type=args.type
                )
                
                # Output results
                print(f"Percentage adjustment for order {args.order_id}: {'SUCCESS' if result['success'] else 'FAILED'}")
                print(f"Applied {args.percentage}% adjustment to {result['items_adjusted']} items")
                print(f"Original total: ${result['original_total']:.2f}")
                print(f"New total: ${result['new_total']:.2f}")
                print(f"Change: ${result['new_total'] - result['original_total']:+.2f}")
                
                if args.verbose and 'details' in result:
                    print("\nItem details:")
                    for item in sorted(result['details'], key=lambda x: abs(x.get('change', 0)), reverse=True):
                        print(f"  {item['item_id']}: {item['soq_original']} → {item['soq_new']} ({item['change']:+.2f}, {item['change_pct']:+.2f}%)")
            
            elif args.command == 'optimize-bracket':
                result = adj_service.optimize_order_to_bracket(
                    order_id=args.order_id,
                    target_bracket=args.target_bracket,
                    strategy=args.strategy
                )
                
                # Output results
                print(f"Bracket optimization for order {args.order_id}: {'SUCCESS' if result['success'] else 'FAILED'}")
                print(f"Message: {result['message']}")
                print(f"Current bracket: {result['current_bracket']}")
                print(f"Target bracket: {result['target_bracket']}")
                print(f"Strategy: {result['strategy']}")
                print(f"Original total: ${result['original_total']:.2f}")
                print(f"New total: ${result['new_total']:.2f}")
                print(f"Change: ${result['new_total'] - result['original_total']:+.2f}")
                print(f"Items adjusted: {result['items_adjusted']}")
                
                if args.verbose and 'details' in result:
                    print("\nItem details:")
                    for item in sorted(result['details'], key=lambda x: abs(x.get('change', 0)), reverse=True):
                        print(f"  {item['item_id']}: {item['soq_original']} → {item['soq_new']} ({item['change']:+.2f}, {item['change_pct']:+.2f}%)")
            
            elif args.command == 'forward-buy':
                result = adj_service.apply_forward_buy(
                    order_id=args.order_id,
                    forward_buy_days=args.days,
                    item_ids=args.items,
                    min_days_filter=args.min_filter
                )
                
                # Output results
                print(f"Forward buy for order {args.order_id}: {'SUCCESS' if result['success'] else 'FAILED'}")
                print(f"Message: {result['message']}")
                print(f"Items processed: {result['items_processed']}")
                print(f"Items forward bought: {result['items_forward_bought']}")
                print(f"Forward buy days: {result['forward_buy_days']}")
                print(f"Original total: ${result['original_total']:.2f}")
                print(f"New total: ${result['new_total']:.2f}")
                print(f"Change: ${result['new_total'] - result['original_total']:+.2f}")
                
                if args.verbose and 'details' in result:
                    print("\nItem details:")
                    for item in sorted(result['details'], key=lambda x: abs(x.get('change', 0)), reverse=True):
                        print(f"  {item['item_id']}: {item['soq_original']} → {item['soq_new']} ({item['change']:+.2f}, {item['original_days']:.1f} → {item['new_days']:.1f} days)")
        
        sys.exit(0)
    
    except Exception as e:
        logger.error(f"Error during order adjustments: {str(e)}", exc_info=True)
        print(f"Error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()