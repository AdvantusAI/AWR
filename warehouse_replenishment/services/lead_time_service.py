# warehouse_replenishment/services/lead_time_service.py
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
    Item, Order, OrderItem, Vendor, Company, Warehouse
)
from warehouse_replenishment.core.lead_time import (
    forecast_lead_time, calculate_variance, detect_lead_time_anomalies,
    calculate_safety_stock_adjustment, predict_fill_in_lead_time,
    evaluate_lead_time_reliability
)
from warehouse_replenishment.exceptions import LeadTimeError
from warehouse_replenishment.logging_setup import get_logger

logger = get_logger(__name__)

class LeadTimeService:
    """Service for handling lead time forecasting and analysis operations."""
    
    def __init__(self, session: Session):
        """Initialize the lead time service.
        
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
                raise LeadTimeError("Company settings not found")
            
            self._company_settings = {
                'lead_time_forecast_control': company.lead_time_forecast_control,
                'lead_time_default': getattr(company, 'lead_time_default', 7)
            }
        
        return self._company_settings
    
    def update_lead_time_forecasts(
        self,
        warehouse_id: Optional[int] = None,
        vendor_id: Optional[int] = None,
        item_ids: List[int] = None
    ) -> Dict:
        """Update lead time forecasts for all applicable items.
        
        Args:
            warehouse_id: Optional warehouse ID to filter items
            vendor_id: Optional vendor ID to filter items
            item_ids: Optional list of specific item IDs to process
            
        Returns:
            Dictionary with processing results
        """
        # Check if lead time forecasting is enabled
        forecast_control = self.company_settings.get('lead_time_forecast_control', 0)
        if forecast_control == 0:
            return {
                'success': True,
                'message': 'Lead time forecasting is not enabled',
                'total_vendors': 0,
                'updated_vendors': 0,
                'total_items': 0,
                'updated_items': 0,
                'errors': 0
            }
        
        # Build query to get vendors
        vendor_query = self.session.query(Vendor)
        
        if warehouse_id is not None:
            vendor_query = vendor_query.filter(Vendor.warehouse_id == warehouse_id)
        
        if vendor_id is not None:
            vendor_query = vendor_query.filter(Vendor.id == vendor_id)
        
        # Only include active vendors
        vendor_query = vendor_query.filter(Vendor.active_items_count > 0)
        
        vendors = vendor_query.all()
        
        # Process results
        results = {
            'total_vendors': len(vendors),
            'updated_vendors': 0,
            'total_items': 0,
            'updated_items': 0,
            'errors': 0,
            'error_details': []
        }
        
        # Process each vendor
        for vendor in vendors:
            try:
                # Update vendor lead time
                vendor_result = self.update_vendor_lead_time(vendor.id)
                
                # Update results
                if vendor_result['success']:
                    results['updated_vendors'] += 1
                    results['total_items'] += vendor_result.get('total_items', 0)
                    results['updated_items'] += vendor_result.get('updated_items', 0)
                
                results['errors'] += vendor_result.get('errors', 0)
                
                # If specific items were provided, only update those items
                if item_ids:
                    # Filter items to those belonging to this vendor
                    vendor_items = self.session.query(Item).filter(
                        Item.vendor_id == vendor.id,
                        Item.id.in_(item_ids)
                    ).all()
                    
                    for item in vendor_items:
                        try:
                            # Update item lead time
                            self.update_item_lead_time(item.id)
                            results['updated_items'] += 1
                        except Exception as e:
                            logger.error(f"Error updating lead time for item {item.id}: {str(e)}")
                            results['errors'] += 1
                            results['error_details'].append({
                                'item_id': item.id,
                                'error': str(e)
                            })
                
            except Exception as e:
                logger.error(f"Error updating lead time for vendor {vendor.id}: {str(e)}")
                results['errors'] += 1
                results['error_details'].append({
                    'vendor_id': vendor.id,
                    'error': str(e)
                })
        
        # Mark results as successful if at least some vendors were updated
        results['success'] = results['updated_vendors'] > 0 or results['errors'] == 0
        
        return results
    
    def update_vendor_lead_time(self, vendor_id: int) -> Dict:
        """Update lead time forecast for a vendor based on historical orders.
        
        Args:
            vendor_id: Vendor ID
            
        Returns:
            Dictionary with update results
        """
        vendor = self.session.query(Vendor).get(vendor_id)
        if not vendor:
            raise LeadTimeError(f"Vendor with ID {vendor_id} not found")
        
        results = {
            'vendor_id': vendor_id,
            'success': False,
            'total_items': 0,
            'updated_items': 0,
            'errors': 0,
            'original_lead_time': vendor.lead_time_forecast,
            'new_lead_time': None,
            'original_variance': vendor.lead_time_variance,
            'new_variance': None
        }
        
        try:
            # Get historical lead times from completed orders
            historical_lead_times = self._get_vendor_historical_lead_times(vendor_id)
            
            # Get detailed order history
            order_history = self._get_vendor_order_history(vendor_id)
            
            if historical_lead_times:
                # Calculate new lead time forecast
                current_lead_time = vendor.lead_time_forecast or self.company_settings['lead_time_default']
                new_lead_time = forecast_lead_time(
                    historical_lead_times,
                    current_lead_time,
                    order_history
                )
                
                # Calculate lead time variance
                new_variance = calculate_variance(historical_lead_times)
                
                # Detect anomalies
                anomalies = detect_lead_time_anomalies(
                    historical_lead_times,
                    current_lead_time
                )
                
                # Update vendor lead time if there are no severe anomalies
                if not any(anomaly['type'] == 'OUTLIER' for anomaly in anomalies):
                    vendor.lead_time_forecast = new_lead_time
                    vendor.lead_time_variance = new_variance
                    vendor.lead_time_last_updated = datetime.now()
                    
                    # Store results
                    results['new_lead_time'] = new_lead_time
                    results['new_variance'] = new_variance
                    
                    # Also store anomalies in results
                    results['anomalies'] = anomalies
                    
                    # Now update all items for this vendor
                    item_update_results = self._update_all_items_for_vendor(vendor_id, new_lead_time, new_variance)
                    
                    # Update results
                    results.update(item_update_results)
                    results['success'] = True
                else:
                    # Severe anomaly detected - log but don't update
                    logger.warning(f"Lead time anomaly detected for vendor {vendor_id}, not updating lead time")
                    results['anomalies'] = anomalies
                    results['message'] = "Lead time anomaly detected, not updating"
            else:
                # No historical lead times available
                logger.info(f"No historical lead times available for vendor {vendor_id}")
                results['message'] = "No historical lead times available"
            
            self.session.commit()
            
        except Exception as e:
            self.session.rollback()
            logger.error(f"Error updating vendor lead time for vendor {vendor_id}: {str(e)}")
            results['error'] = str(e)
            results['errors'] += 1
        
        return results
    
    def update_item_lead_time(self, item_id: int) -> Dict:
        """Update lead time forecast for a specific item.
        
        Args:
            item_id: Item ID
            
        Returns:
            Dictionary with update results
        """
        item = self.session.query(Item).get(item_id)
        if not item:
            raise LeadTimeError(f"Item with ID {item_id} not found")
        
        results = {
            'item_id': item_id,
            'success': False,
            'original_lead_time': item.lead_time_forecast,
            'new_lead_time': None,
            'original_variance': item.lead_time_variance,
            'new_variance': None,
            'adjustment_made': False
        }
        
        try:
            # Get item-specific historical lead times
            item_historical_lead_times = self._get_item_historical_lead_times(item_id)
            
            # If item has enough history, calculate item-specific lead time
            if len(item_historical_lead_times) >= 3:  # Require at least 3 data points
                # Calculate item-specific lead time
                current_lead_time = item.lead_time_forecast or item.vendor.lead_time_forecast
                new_lead_time = forecast_lead_time(
                    item_historical_lead_times,
                    current_lead_time
                )
                
                # Calculate item-specific variance
                new_variance = calculate_variance(item_historical_lead_times)
                
                # Update item lead time
                item.lead_time_forecast = new_lead_time
                item.lead_time_variance = new_variance
                item.lead_time_maintained = True
                
                # Store results
                results['new_lead_time'] = new_lead_time
                results['new_variance'] = new_variance
                results['adjustment_made'] = True
                results['success'] = True
            else:
                # Use vendor lead time but check for potential adjustments
                vendor = self.session.query(Vendor).get(item.vendor_id)
                if vendor:
                    # Check if item needs adjustment from vendor average
                    adjustment_needed = False
                    
                    # If item is high value or critical, consider adjusting lead time
                    if hasattr(item, 'is_critical') and item.is_critical:
                        adjustment_needed = True
                    
                    # If item has special handling requirements
                    if hasattr(item, 'special_handling') and item.special_handling:
                        adjustment_needed = True
                    
                    if adjustment_needed:
                        # Apply a conservative adjustment (e.g., add a buffer)
                        vendor_lead_time = vendor.lead_time_forecast or self.company_settings['lead_time_default']
                        item.lead_time_forecast = int(vendor_lead_time * 1.1)  # 10% buffer
                        item.lead_time_variance = vendor.lead_time_variance or 10.0
                        item.lead_time_maintained = True
                        
                        results['new_lead_time'] = item.lead_time_forecast
                        results['new_variance'] = item.lead_time_variance
                        results['adjustment_made'] = True
                    else:
                        # Use vendor lead time unchanged
                        item.lead_time_forecast = vendor.lead_time_forecast
                        item.lead_time_variance = vendor.lead_time_variance
                        item.lead_time_maintained = False
                        
                        results['new_lead_time'] = vendor.lead_time_forecast
                        results['new_variance'] = vendor.lead_time_variance
                        results['adjustment_made'] = False
                        
                    results['success'] = True
                else:
                    # Vendor not found, use default
                    item.lead_time_forecast = self.company_settings['lead_time_default']
                    item.lead_time_variance = 10.0  # Default variance
                    item.lead_time_maintained = False
                    
                    results['new_lead_time'] = item.lead_time_forecast
                    results['new_variance'] = item.lead_time_variance
                    results['adjustment_made'] = False
                    results['success'] = True
            
            # Recalculate safety stock if lead time changed
            if results['original_lead_time'] != results['new_lead_time'] or results['original_variance'] != results['new_variance']:
                self._recalculate_safety_stock(item)
            
            self.session.commit()
            
        except Exception as e:
            self.session.rollback()
            logger.error(f"Error updating item lead time for item {item_id}: {str(e)}")
            results['error'] = str(e)
        
        return results
    
    def _get_vendor_historical_lead_times(self, vendor_id: int) -> List[float]:
        """Get historical lead times for a vendor.
        
        Args:
            vendor_id: Vendor ID
            
        Returns:
            List of historical lead times
        """
        # Query completed orders
        completed_orders = self.session.query(Order).filter(
            Order.vendor_id == vendor_id,
            Order.status == 'RECEIVED',
            Order.approval_date.isnot(None),
            Order.receipt_date.isnot(None)
        ).order_by(Order.receipt_date.desc()).limit(20).all()
        
        # Calculate actual lead times (receipt_date - approval_date)
        lead_times = []
        for order in completed_orders:
            if order.approval_date and order.receipt_date:
                lead_time = (order.receipt_date - order.approval_date.date()).days
                if lead_time > 0:  # Ensure positive lead time
                    lead_times.append(lead_time)
        
        return lead_times
    
    def _get_item_historical_lead_times(self, item_id: int) -> List[float]:
        """Get historical lead times for a specific item.
        
        Args:
            item_id: Item ID
            
        Returns:
            List of historical lead times
        """
        # Find order items for this item that were received
        order_items = self.session.query(OrderItem).filter(
            OrderItem.item_id == item_id
        ).join(Order).filter(
            Order.status == 'RECEIVED',
            Order.approval_date.isnot(None),
            Order.receipt_date.isnot(None)
        ).order_by(Order.receipt_date.desc()).all()
        
        # Calculate lead times
        lead_times = []
        for order_item in order_items:
            order = order_item.order
            if order.approval_date and order.receipt_date:
                lead_time = (order.receipt_date - order.approval_date.date()).days
                if lead_time > 0:  # Ensure positive lead time
                    lead_times.append(lead_time)
        
        return lead_times
    
    def _get_vendor_order_history(self, vendor_id: int) -> List[Dict]:
        """Get order history for a vendor.
        
        Args:
            vendor_id: Vendor ID
            
        Returns:
            List of order history dictionaries
        """
        # Query orders
        orders = self.session.query(Order).filter(
            Order.vendor_id == vendor_id,
            Order.status.in_(['ACCEPTED', 'RECEIVED'])
        ).order_by(Order.order_date.desc()).limit(50).all()
        
        # Format order history
        order_history = []
        for order in orders:
            order_dict = {
                'order_id': order.id,
                'order_date': order.order_date,
                'approval_date': order.approval_date,
                'receipt_date': order.receipt_date,
                'expected_delivery_date': order.expected_delivery_date,
                'status': order.status
            }
            
            if order.approval_date and order.receipt_date:
                order_dict['actual_lead_time'] = (order.receipt_date - order.approval_date.date()).days
            
            order_history.append(order_dict)
        
        return order_history
    
    def _update_all_items_for_vendor(
        self,
        vendor_id: int,
        new_lead_time: float,
        new_variance: float
    ) -> Dict:
        """Update lead time for all items belonging to a vendor.
        
        Args:
            vendor_id: Vendor ID
            new_lead_time: New lead time forecast
            new_variance: New lead time variance
            
        Returns:
            Dictionary with update results
        """
        # Get all active items for this vendor
        items = self.session.query(Item).filter(
            Item.vendor_id == vendor_id,
            Item.buyer_class.in_(['R', 'W'])  # Regular and Watch items (active)
        ).all()
        
        results = {
            'total_items': len(items),
            'updated_items': 0,
            'errors': 0
        }
        
        # Update each item
        for item in items:
            try:
                # Skip items with manually maintained lead times
                if item.lead_time_maintained:
                    continue
                
                # Update lead time to match vendor
                item.lead_time_forecast = new_lead_time
                item.lead_time_variance = new_variance
                
                # Recalculate safety stock
                self._recalculate_safety_stock(item)
                
                results['updated_items'] += 1
                
            except Exception as e:
                logger.error(f"Error updating lead time for item {item.id}: {str(e)}")
                results['errors'] += 1
        
        return results
    
    def _recalculate_safety_stock(self, item: Item) -> None:
        """Recalculate safety stock for an item after lead time changes.
        
        Args:
            item: Item to update
        """
        # Avoid circular imports
        try:
            # Only import here to avoid circular dependencies
            from warehouse_replenishment.services.safety_stock_service import SafetyStockService
            ss_service = SafetyStockService(self.session)
            ss_service.update_safety_stock_for_item(item.id, update_sstf=True, update_order_points=True)
        except (ImportError, AttributeError) as e:
            logger.info(f"SafetyStockService not available, using core functions directly: {str(e)}")
            
            # Use core functions directly
            from warehouse_replenishment.core.safety_stock import calculate_safety_stock, calculate_safety_stock_units
            
            # Get company settings
            company = self.session.query(Company).first()
            if not company:
                logger.warning("Company settings not found")
                return
            
            # Get service level
            service_level = item.service_level_goal
            if service_level is None:
                service_level = company.service_level_goal
            
            # Get vendor
            vendor = self.session.query(Vendor).get(item.vendor_id)
            
            # Get effective order cycle
            vendor_cycle = vendor.order_cycle if vendor and vendor.order_cycle is not None else 0
            item_cycle = item.item_cycle_days if item.item_cycle_days is not None else 0
            effective_order_cycle = max(vendor_cycle, item_cycle)
            
            # Calculate safety stock in days
            safety_stock_days = calculate_safety_stock(
                service_level_goal=service_level,
                madp=item.madp if item.madp is not None else 0.0,
                lead_time=item.lead_time_forecast if item.lead_time_forecast is not None else 0,
                lead_time_variance=item.lead_time_variance if item.lead_time_variance is not None else 0.0,
                order_cycle=effective_order_cycle
            )
            
            # Calculate safety stock in units
            daily_demand = item.demand_4weekly / 28 if item.demand_4weekly else 0.0
            safety_stock_units = calculate_safety_stock_units(safety_stock_days, daily_demand)
            
            # Update item fields
            item.sstf = safety_stock_days
            
            # Calculate order points
            item.item_order_point_days = safety_stock_days + item.lead_time_forecast
            item.item_order_point_units = item.item_order_point_days * daily_demand
            
            item.vendor_order_point_days = item.item_order_point_days + vendor_cycle
            
            item.order_up_to_level_days = item.item_order_point_days + effective_order_cycle
            item.order_up_to_level_units = item.order_up_to_level_days * daily_demand
    
    def analyze_vendor_lead_time_reliability(self, vendor_id: int) -> Dict:
        """Analyze lead time reliability for a vendor.
        
        Args:
            vendor_id: Vendor ID
            
        Returns:
            Dictionary with reliability analysis
        """
        vendor = self.session.query(Vendor).get(vendor_id)
        if not vendor:
            raise LeadTimeError(f"Vendor with ID {vendor_id} not found")
        
        # Get historical lead times
        historical_lead_times = self._get_vendor_historical_lead_times(vendor_id)
        
        if not historical_lead_times:
            return {
                'vendor_id': vendor_id,
                'status': 'INSUFFICIENT_DATA',
                'message': 'No historical lead times available for analysis'
            }
        
        # Get expected lead time
        expected_lead_time = vendor.lead_time_forecast or self.company_settings['lead_time_default']
        
        # Evaluate reliability
        reliability = evaluate_lead_time_reliability(expected_lead_time, historical_lead_times)
        
        # Add vendor info to results
        reliability['vendor_id'] = vendor_id
        reliability['vendor_name'] = vendor.name
        reliability['expected_lead_time'] = expected_lead_time
        
        return reliability
    
    def calculate_fill_in_lead_time(self, item_id: int) -> Dict:
        """Calculate fill-in lead time for an item.
        
        Args:
            item_id: Item ID
            
        Returns:
            Dictionary with fill-in lead time calculation
        """
        item = self.session.query(Item).get(item_id)
        if not item:
            raise LeadTimeError(f"Item with ID {item_id} not found")
        
        vendor = self.session.query(Vendor).get(item.vendor_id)
        if not vendor:
            raise LeadTimeError(f"Vendor with ID {item.vendor_id} not found")
        
        # Get historical lead times
        historical_lead_times = self._get_item_historical_lead_times(item_id)
        
        # Calculate fill-in lead time
        vendor_lead_time = vendor.lead_time_forecast or self.company_settings['lead_time_default']
        fill_in_lead_time = predict_fill_in_lead_time(
            vendor_lead_time,
            historical_lead_times
        )
        
        # Check if there's an alternate vendor
        alternate_vendor_lead_time = None
        # This would require additional data model support for alternate vendors
        
        return {
            'item_id': item_id,
            'vendor_id': vendor.id,
            'vendor_lead_time': vendor_lead_time,
            'fill_in_lead_time': fill_in_lead_time,
            'historical_lead_times_count': len(historical_lead_times)
        }