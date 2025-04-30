# warehouse_replenishment/services/safety_stock_service.py
from datetime import date, datetime, timedelta
from typing import List, Dict, Tuple, Optional, Union, Any
import logging
import sys
import os
from pathlib import Path

# Add the parent directory to the path so we can import our modules
parent_dir = str(Path(__file__).parent.parent.parent)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from sqlalchemy import and_, func, or_
from sqlalchemy.orm import Session


from warehouse_replenishment.models import (
    Item, Company, Vendor, Warehouse, SafetyStockType, BuyerClassCode
)
from warehouse_replenishment.core.safety_stock import (
    calculate_safety_stock, calculate_service_level,
    empirical_safety_stock_adjustment, calculate_safety_stock_units
)
from warehouse_replenishment.exceptions import SafetyStockError, ItemError

from warehouse_replenishment.logging_setup import logger
logger = logging.getLogger(__name__)

class SafetyStockService:
    """Service for managing safety stock calculations and adjustments."""
    
    def __init__(self, session: Session):
        """Initialize the safety stock service.
        
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
                raise SafetyStockError("Company settings not found")
            
            self._company_settings = {
                'service_level_goal': company.service_level_goal,
                'tracking_signal_limit': company.tracking_signal_limit,
                'forecast_demand_limit': company.forecast_demand_limit
            }
        
        return self._company_settings
        
    def calculate_safety_stock_for_item(
        self,
        item_id: int,
        service_level_override: Optional[float] = None
    ) -> Dict:
        """Calculate safety stock for a specific item.
        
        Args:
            item_id: Item ID
            service_level_override: Optional override for service level goal
            
        Returns:
            Dictionary with safety stock calculation results
        """
        
        item = self.session.query(Item).get(item_id)
        if not item:
            raise ItemError(f"Item with ID {item_id} not found")
        
        vendor = self.session.query(Vendor).get(item.vendor_id)
        if not vendor:
            raise ItemError(f"Vendor with ID {item.vendor_id} not found")
        
        # Determine service level to use
        if service_level_override is not None:
            service_level = service_level_override
        elif item.service_level_goal:
            service_level = item.service_level_goal
        elif vendor.service_level_goal:
            service_level = vendor.service_level_goal
        else:
            service_level = self.company_settings['service_level_goal']
        
        # Get effective order cycle
        effective_order_cycle = max(vendor.order_cycle or 0, item.item_cycle_days or 0)
        
        # Calculate safety stock in days
        safety_stock_days = calculate_safety_stock(
            service_level_goal=service_level,
            madp=item.madp,
            lead_time=item.lead_time_forecast,
            lead_time_variance=item.lead_time_variance,
            order_cycle=effective_order_cycle
        )
        
        
        
        daily_demand = item.demand_4weekly / 28  # Assuming 28 days in a 4-weekly period
        safety_stock_units = calculate_safety_stock_units(safety_stock_days, daily_demand)
        
        # If manual safety stock is set and safety stock type is not NEVER
        manual_ss_applied = False
        
        
        
        if item.manual_ss > 0 and item.ss_type != SafetyStockType.NEVER:
            if item.ss_type == SafetyStockType.ALWAYS:
                # Always use manual safety stock
                safety_stock_units = item.manual_ss
                # Recalculate days based on the manual units
                safety_stock_days = safety_stock_units / daily_demand if daily_demand > 0 else 0
                manual_ss_applied = True
            elif item.ss_type == SafetyStockType.LESSER_OF:                
                if item.manual_ss < safety_stock_units:
                    safety_stock_units = item.manual_ss
                    # Recalculate days based on the manual units
                    safety_stock_days = safety_stock_units / daily_demand if daily_demand > 0 else 0
                    manual_ss_applied = True
                    
        return {
            'item_id': item_id,
            'service_level': service_level,
            'lead_time': item.lead_time_forecast,
            'lead_time_variance': item.lead_time_variance,
            'madp': item.madp,
            'order_cycle': effective_order_cycle,
            'safety_stock_days': safety_stock_days,
            'safety_stock_units': safety_stock_units,
            'daily_demand': daily_demand,
            'manual_ss_applied': manual_ss_applied,
            'manual_ss': item.manual_ss if item.manual_ss > 0 else None,
            'manual_ss_type': item.ss_type.name if item.ss_type else None
        }
    
    def update_safety_stock_for_item(
        self,
        item_id: int,
        update_sstf: bool = True,
        update_order_points: bool = True,
        service_level_override: Optional[float] = None
    ) -> bool:
        """Update safety stock for a specific item.
        
        Args:
            item_id: Item ID
            update_sstf: Whether to update the Safety Stock Time Factor field
            update_order_points: Whether to update order points and levels
            service_level_override: Optional override for service level goal
            
        Returns:
            True if safety stock was updated successfully
        """
        item = self.session.query(Item).get(item_id)
       
        if not item:
            raise ItemError(f"Item with ID {item_id} not found")
        
        # Calculate safety stock
        ss_result = self.calculate_safety_stock_for_item(
            item_id, service_level_override
        )
        
        # Update SSTF if requested
        if update_sstf:
            item.sstf = float(ss_result['safety_stock_days'])
        
        # Update order points and levels if requested
        if update_order_points:
            vendor = self.session.query(Vendor).get(item.vendor_id)
            if not vendor:
                raise ItemError(f"Vendor with ID {item.vendor_id} not found")
            
            # Update item order point days and units
            item.item_order_point_days = float(ss_result['safety_stock_days'] + item.lead_time_forecast)
            item.item_order_point_units = float(item.item_order_point_days * ss_result['daily_demand'])
            
            # Update vendor order point days
            item.vendor_order_point_days = float(item.item_order_point_days + (vendor.order_cycle or 0))
            
            # Get effective order cycle
            effective_order_cycle = max(vendor.order_cycle or 0, item.item_cycle_days or 0)
            
            # Update order up to level
            item.order_up_to_level_days = float(item.item_order_point_days + effective_order_cycle)
            item.order_up_to_level_units = float(item.order_up_to_level_days * ss_result['daily_demand'])
        
        try:
            self.session.commit()
            return True
        except Exception as e:
            self.session.rollback()
            raise SafetyStockError(f"Failed to update safety stock: {str(e)}")
    
    def adjust_safety_stock_empirically(
        self,
        item_id: int,
        service_level_attained: float,
        max_adjustment_pct: float = 10.0
    ) -> Dict:
        """Adjust safety stock based on empirical service level performance.
        
        Args:
            item_id: Item ID
            service_level_attained: Service level attained as percentage
            max_adjustment_pct: Maximum adjustment percentage
            
        Returns:
            Dictionary with adjustment results
        """
        item = self.session.query(Item).get(item_id)
        if not item:
            raise ItemError(f"Item with ID {item_id} not found")
        
        # Get current safety stock
        current_ss_days = item.sstf
        
        # Determine service level goal
        vendor = self.session.query(Vendor).get(item.vendor_id)
        if not vendor:
            raise ItemError(f"Vendor with ID {item.vendor_id} not found")
        
        service_level_goal = (
            item.service_level_goal or 
            vendor.service_level_goal or 
            self.company_settings['service_level_goal']
        )
        
        # Calculate adjusted safety stock
        adjusted_ss_days = empirical_safety_stock_adjustment(
            current_safety_stock=current_ss_days,
            service_level_goal=service_level_goal,
            service_level_attained=service_level_attained,
            max_adjustment_pct=max_adjustment_pct
        )
        
        # Calculate change percentage
        if current_ss_days > 0:
            change_pct = ((adjusted_ss_days - current_ss_days) / current_ss_days) * 100
        else:
            change_pct = 100.0
        
        # Calculate safety stock in units
        daily_demand = item.demand_4weekly / 28  # Assuming 28 days in a 4-weekly period
        adjusted_ss_units = calculate_safety_stock_units(adjusted_ss_days, daily_demand)
        
        return {
            'item_id': item_id,
            'service_level_goal': service_level_goal,
            'service_level_attained': service_level_attained,
            'current_ss_days': current_ss_days,
            'adjusted_ss_days': adjusted_ss_days,
            'adjusted_ss_units': adjusted_ss_units,
            'change_pct': change_pct,
            'max_adjustment_pct': max_adjustment_pct
        }
    
    def apply_empirical_adjustment(
        self,
        item_id: int,
        service_level_attained: float,
        max_adjustment_pct: float = 10.0,
        update_order_points: bool = True
    ) -> bool:
        """Apply empirical safety stock adjustment to an item.
        
        Args:
            item_id: Item ID
            service_level_attained: Service level attained as percentage
            max_adjustment_pct: Maximum adjustment percentage
            update_order_points: Whether to update order points and levels
            
        Returns:
            True if adjustment was applied successfully
        """
        # Calculate adjustment
        adjustment = self.adjust_safety_stock_empirically(
            item_id, service_level_attained, max_adjustment_pct
        )
        
        # Update item with adjusted safety stock
        item = self.session.query(Item).get(item_id)
        if not item:
            raise ItemError(f"Item with ID {item_id} not found")
        
        # Update SSTF
        item.sstf = adjustment['adjusted_ss_days']
        
        # Update service level attained for tracking
        item.service_level_attained = service_level_attained
        
        # Update order points and levels if requested
        if update_order_points:
            vendor = self.session.query(Vendor).get(item.vendor_id)
            if not vendor:
                raise ItemError(f"Vendor with ID {item.vendor_id} not found")
            
            # Recalculate daily demand
            daily_demand = item.demand_4weekly / 28  # Assuming 28 days in a 4-weekly period
            
            # Update item order point days and units
            item.item_order_point_days = adjustment['adjusted_ss_days'] + item.lead_time_forecast
            item.item_order_point_units = item.item_order_point_days * daily_demand
            
            # Update vendor order point days
            item.vendor_order_point_days = item.item_order_point_days + vendor.order_cycle
            
            # Get effective order cycle
            effective_order_cycle = max(vendor.order_cycle or 0, item.item_cycle_days or 0)
            
            # Update order up to level
            item.order_up_to_level_days = item.item_order_point_days + effective_order_cycle
            item.order_up_to_level_units = item.order_up_to_level_days * daily_demand
        
        try:
            self.session.commit()
            return True
        except Exception as e:
            self.session.rollback()
            raise SafetyStockError(f"Failed to apply empirical adjustment: {str(e)}")
    
    def update_safety_stock_for_all_items(
        self,
        warehouse_id: Optional[int] = None,
        vendor_id: Optional[int] = None,
        update_order_points: bool = True
    ) -> Dict:
        """Update safety stock for all items matching criteria.
        
        Args:
            warehouse_id: Optional warehouse ID filter
            vendor_id: Optional vendor ID filter
            update_order_points: Whether to update order points and levels
            
        Returns:
            Dictionary with update results
        """
        # Build query to get items
        query = self.session.query(Item)
        
        if warehouse_id is not None:
            query = query.filter(Item.warehouse_id == warehouse_id)
            
        if vendor_id is not None:
            query = query.filter(Item.vendor_id == vendor_id)
        
        # Only include active items - use string values instead of enum
        query = query.filter(Item.buyer_class.in_(['R', 'W']))
        
        items = query.all()
        
        results = {
            'total_items': len(items),
            'updated_items': 0,
            'errors': 0,
            'error_items': []
        }
        
        # Process each item
        for item in items:
            try:
                success = self.update_safety_stock_for_item(
                    item.id, update_sstf=True, update_order_points=update_order_points
                )
                
                if success:
                    results['updated_items'] += 1
                
            except Exception as e:
                logger.error(f"Error updating safety stock for item {item.id}: {str(e)}")
                results['errors'] += 1
                results['error_items'].append({
                    'item_id': item.id,
                    'error': str(e)
                })
        
        return results
    
    def set_manual_safety_stock(
        self,
        item_id: int,
        manual_ss: float,
        ss_type: SafetyStockType,
        update_order_points: bool = True
    ) -> bool:
        """Set manual safety stock for an item.
        
        Args:
            item_id: Item ID
            manual_ss: Manual safety stock units
            ss_type: Safety stock type (NEVER, LESSER_OF, ALWAYS)
            update_order_points: Whether to update order points and levels
            
        Returns:
            True if manual safety stock was set successfully
        """
        item = self.session.query(Item).get(item_id)
        if not item:
            raise ItemError(f"Item with ID {item_id} not found")
        
        # Update manual safety stock fields
        item.manual_ss = manual_ss
        item.ss_type = ss_type
        
        # Update safety stock if needed
        if ss_type != SafetyStockType.NEVER and update_order_points:
            self.update_safety_stock_for_item(item_id, update_order_points=True)
        
        try:
            self.session.commit()
            return True
        except Exception as e:
            self.session.rollback()
            raise SafetyStockError(f"Failed to set manual safety stock: {str(e)}")
    
    def analyze_safety_stock_efficiency(
        self,
        item_id: int,
        simulate_service_levels: bool = True
    ) -> Dict:
        """Analyze efficiency of current safety stock settings.
        
        Args:
            item_id: Item ID
            simulate_service_levels: Whether to simulate different service levels
            
        Returns:
            Dictionary with analysis results
        """
        item = self.session.query(Item).get(item_id)
        if not item:
            raise ItemError(f"Item with ID {item_id} not found")
        
        # Calculate current safety stock
        current_ss = self.calculate_safety_stock_for_item(item_id)
        
        # Calculate inventory value
        ss_value = current_ss['safety_stock_units'] * item.purchase_price
        
        analysis = {
            'item_id': item_id,
            'current_settings': {
                'service_level': current_ss['service_level'],
                'safety_stock_days': current_ss['safety_stock_days'],
                'safety_stock_units': current_ss['safety_stock_units'],
                'safety_stock_value': ss_value
            },
            'simulated_levels': []
        }
        
        if simulate_service_levels:
            # Simulate different service levels
            service_levels = [90.0, 95.0, 97.0, 99.0, 99.5]
            
            for sl in service_levels:
                # Skip if it's the current service level
                if abs(sl - current_ss['service_level']) < 0.1:
                    continue
                    
                # Calculate safety stock at this service level
                sim_ss = self.calculate_safety_stock_for_item(item_id, service_level_override=sl)
                sim_value = sim_ss['safety_stock_units'] * item.purchase_price
                
                # Calculate change from current
                if current_ss['safety_stock_units'] > 0:
                    units_change_pct = ((sim_ss['safety_stock_units'] - current_ss['safety_stock_units']) / 
                                        current_ss['safety_stock_units']) * 100
                else:
                    units_change_pct = 100.0
                
                analysis['simulated_levels'].append({
                    'service_level': sl,
                    'safety_stock_days': sim_ss['safety_stock_days'],
                    'safety_stock_units': sim_ss['safety_stock_units'],
                    'safety_stock_value': sim_value,
                    'change_pct': units_change_pct
                })
        
        return analysis