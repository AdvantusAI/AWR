# warehouse_replenishment/services/item_service.py
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
    Item, DemandHistory, Company, Vendor, SeasonalProfile, 
    SeasonalProfileIndex, HistoryException, Warehouse,
    BuyerClassCode, SystemClassCode, ForecastMethod, Inventory
)
from warehouse_replenishment.core.demand_forecast import (
    calculate_lost_sales as core_calculate_lost_sales, 
    adjust_history_value
)
from warehouse_replenishment.core.safety_stock import (
    calculate_safety_stock, calculate_safety_stock_units
)
from warehouse_replenishment.exceptions import ItemError
from warehouse_replenishment.utils.date_utils import (
    get_current_period, get_previous_period, get_period_dates
)

from warehouse_replenishment.logging_setup import logger
logger = logging.getLogger(__name__)


class ItemService:
    """Service for handling item operations."""
    
    def __init__(self, session_or_client):
        """Initialize the item service.
        
        Args:
            session_or_client: Database session or Supabase client
        """
        self.session = session_or_client
        self._company_settings = None
        self._is_supabase = not hasattr(session_or_client, 'query')
    
    @property
    def company_settings(self) -> Dict:
        """Get company settings.
        
        Returns:
            Dictionary with company settings
        """
        if not self._company_settings:
            if self._is_supabase:
                result = self.session.table('company').select('*').limit(1).execute()
                if not result.data:
                    raise ItemError("Company settings not found")
                company = result.data[0]
            else:
                company = self.session.query(Company).first()
                if not company:
                    raise ItemError("Company settings not found")
            
            self._company_settings = {
                'service_level_goal': company.get('service_level_goal', 95.0) if self._is_supabase else company.service_level_goal,
                'demand_from_days_out': company.get('demand_from_days_out', 1) if self._is_supabase else company.demand_from_days_out,
                'lead_time_forecast_control': company.get('lead_time_forecast_control', 1) if self._is_supabase else company.lead_time_forecast_control,
                'history_periodicity_default': company.get('history_periodicity_default', 13) if self._is_supabase else company.history_periodicity_default,
                'forecasting_periodicity_default': company.get('forecasting_periodicity_default', 13) if self._is_supabase else company.forecasting_periodicity_default,
                'slow_mover_limit': company.get('slow_mover_limit', 10.0) if self._is_supabase else company.slow_mover_limit,
                'lumpy_demand_limit': company.get('lumpy_demand_limit', 50.0) if self._is_supabase else company.lumpy_demand_limit
            }
        
        return self._company_settings
    
    def get_item(self, item_id: int) -> Optional[Item]:
        """Get an item by ID.
        
        Args:
            item_id: Item ID
            
        Returns:
            Item object or None if not found
        """
        return self.session.query(Item).get(item_id)
    
    def get_item_by_code(self, item_code: str, vendor_id: int, warehouse_id: int) -> Optional[Item]:
        """Get an item by code, vendor, and warehouse.
        
        Args:
            item_code: Item code
            vendor_id: Vendor ID
            warehouse_id: Warehouse ID
            
        Returns:
            Item object or None if not found
        """
        return self.session.query(Item).filter(
            Item.item_id == item_code,
            Item.vendor_id == vendor_id,
            Item.warehouse_id == warehouse_id
        ).first()
    
    def get_items(
        self,
        warehouse_id: Optional[int] = None,
        vendor_id: Optional[int] = None,
        buyer_id: Optional[str] = None,
        item_group: Optional[str] = None,
        buyer_class: Optional[List[str]] = None,
        system_class: Optional[List[str]] = None,
        active_only: bool = True
    ) -> List[Item]:
        """Get items matching criteria.
        
        Args:
            warehouse_id: Optional warehouse ID filter
            vendor_id: Optional vendor ID filter
            buyer_id: Optional buyer ID filter
            item_group: Optional item group filter
            buyer_class: Optional list of buyer classes
            system_class: Optional list of system classes (ignored if field doesn't exist)
            active_only: Whether to include only active items
            
        Returns:
            List of item objects
        """
        if self._is_supabase:
            query = self.session.table('item').select('*')
            
            if warehouse_id is not None:
                query = query.eq('warehouse_id', warehouse_id)
                
            if vendor_id is not None:
                query = query.eq('vendor_id', vendor_id)
                
            if buyer_id is not None:
                query = query.eq('buyer_id', buyer_id)
                
            if item_group is not None:
                query = query.like('item_group_codes', f'%{item_group}%')
                
            if buyer_class:
                query = query.in_('buyer_class', buyer_class)
            elif active_only:
                query = query.in_('buyer_class', ['R', 'W'])
                
            result = query.execute()
            items = []
            for item_data in result.data or []:
                try:
                    items.append(Item(**{k: v for k, v in item_data.items() if hasattr(Item, k)}))
                except TypeError:
                    continue
            return items
        else:
            query = self.session.query(Item)
            
            if warehouse_id is not None:
                query = query.filter(Item.warehouse_id == warehouse_id)
                
            if vendor_id is not None:
                query = query.filter(Item.vendor_id == vendor_id)
                
            if buyer_id is not None:
                query = query.filter(Item.buyer_id == buyer_id)
                
            if item_group is not None:
                query = query.filter(Item.item_group_codes.like(f'%{item_group}%'))
                
            if buyer_class:
                query = query.filter(Item.buyer_class.in_(buyer_class))
            elif active_only:
                query = query.filter(Item.buyer_class.in_(['R', 'W']))
                
            if system_class and hasattr(Item, 'system_class'):
                query = query.filter(Item.system_class.in_(system_class))
                
            return query.all()
    
    def create_item(
        self,
        item_id: str,
        description: str,
        vendor_id: int,
        warehouse_id: int,
        service_level_goal: Optional[float] = None,
        lead_time_forecast: Optional[int] = None,
        lead_time_variance: Optional[float] = None,
        buying_multiple: float = 1.0,
        minimum_quantity: float = 1.0,
        purchase_price: float = 0.0,
        sales_price: float = 0.0,
        buyer_id: Optional[str] = None,
        buyer_class: str = 'U'  # Uninitialized
    ) -> int:
        """Create a new item.
        
        Args:
            item_id: Item ID
            description: Item description
            vendor_id: Vendor ID
            warehouse_id: Warehouse ID
            service_level_goal: Optional service level goal
            lead_time_forecast: Optional lead time forecast
            lead_time_variance: Optional lead time variance
            buying_multiple: Buying multiple
            minimum_quantity: Minimum quantity
            purchase_price: Purchase price
            sales_price: Sales price
            buyer_id: Optional buyer ID
            buyer_class: Buyer class
            
        Returns:
            ID of the created item
        """
        # Check if item already exists
        existing_item = self.session.query(Item).filter(
            Item.item_id == item_id,
            Item.vendor_id == vendor_id,
            Item.warehouse_id == warehouse_id
        ).first()
        
        if existing_item:
            raise ItemError(f"Item with ID {item_id} already exists for vendor {vendor_id} in warehouse {warehouse_id}")
        
        # Get vendor
        vendor = self.session.query(Vendor).get(vendor_id)
        if not vendor:
            raise ItemError(f"Vendor with ID {vendor_id} not found")
        
        # Use defaults from vendor if not provided
        if service_level_goal is None:
            service_level_goal = vendor.service_level_goal
            
        if lead_time_forecast is None:
            lead_time_forecast = vendor.lead_time_forecast
            
        if lead_time_variance is None:
            lead_time_variance = vendor.lead_time_variance
            
        if buyer_id is None:
            buyer_id = vendor.buyer_id
        
        # Map string buyer class to enum if needed
        if isinstance(buyer_class, str):
            try:
                buyer_class = BuyerClassCode(buyer_class)
            except ValueError:
                logger.warning(f"Invalid buyer class string: {buyer_class}, using UNINITIALIZED")
                buyer_class = BuyerClassCode.UNINITIALIZED
        
        # Create new item
        item = Item(
            item_id=item_id,
            description=description,
            vendor_id=vendor_id,
            warehouse_id=warehouse_id,
            service_level_goal=service_level_goal,
            service_level_maintained=True,  # Manually set
            lead_time_forecast=lead_time_forecast,
            lead_time_variance=lead_time_variance,
            lead_time_maintained=True,  # Manually set
            buying_multiple=buying_multiple,
            minimum_quantity=minimum_quantity,
            purchase_price=purchase_price,
            sales_price=sales_price,
            buyer_id=buyer_id,
            buyer_class=buyer_class,
            on_hand=0.0,
            on_order=0.0,
            demand_weekly=0.0,
            demand_4weekly=0.0,
            demand_monthly=0.0,
            demand_quarterly=0.0,
            demand_yearly=0.0,
            madp=0.0,
            track=0.0
        )
        
        self.session.add(item)
        
        try:
            self.session.commit()
            
            # Update vendor active items count
            vendor.active_items_count = self.session.query(func.count(Item.id)).filter(
                Item.vendor_id == vendor_id,
                Item.buyer_class.in_([BuyerClassCode.REGULAR, BuyerClassCode.WATCH])
            ).scalar() or 0
            
            self.session.commit()
            
            return item.id
        except Exception as e:
            self.session.rollback()
            raise ItemError(f"Failed to create item: {str(e)}")
    
    def update_item(
        self,
        item_id: int,
        updates: Dict[str, Any]
    ) -> bool:
        """Update an item.
        
        Args:
            item_id: Item ID
            updates: Dictionary with fields to update
            
        Returns:
            True if item was updated successfully
        """
        item = self.get_item(item_id)
        if not item:
            raise ItemError(f"Item with ID {item_id} not found")
        
        # Update fields
        for field, value in updates.items():
            if hasattr(item, field):
                setattr(item, field, value)
            else:
                logger.warning(f"Field {field} does not exist on Item model")
        
        try:
            self.session.commit()
            return True
        except Exception as e:
            self.session.rollback()
            raise ItemError(f"Failed to update item: {str(e)}")
    
    def set_buyer_class(
        self,
        item_id: int,
        buyer_class: Union[str, BuyerClassCode]
    ) -> bool:
        """Set buyer class for an item.
        
        Args:
            item_id: Item ID
            buyer_class: Buyer class (string or enum)
            
        Returns:
            True if buyer class was set successfully
        """
        item = self.get_item(item_id)
        if not item:
            raise ItemError(f"Item with ID {item_id} not found")
        
        # Convert string to enum if needed
        if isinstance(buyer_class, str):
            try:
                buyer_class = BuyerClassCode(buyer_class)
            except ValueError:
                valid_classes = [bc.value for bc in BuyerClassCode]
                raise ItemError(f"Invalid buyer class: {buyer_class}. Valid values are {valid_classes}")
        
        # Update buyer class
        item.buyer_class = buyer_class
        
        try:
            self.session.commit()
            
            # Update vendor active items count if necessary
            active_classes = [BuyerClassCode.REGULAR, BuyerClassCode.WATCH]
            if buyer_class in active_classes or item.buyer_class in active_classes:
                vendor = self.session.query(Vendor).get(item.vendor_id)
                if vendor:
                    vendor.active_items_count = self.session.query(func.count(Item.id)).filter(
                        Item.vendor_id == item.vendor_id,
                        Item.buyer_class.in_(active_classes)
                    ).scalar() or 0
                    
                    self.session.commit()
            
            return True
        except Exception as e:
            self.session.rollback()
            raise ItemError(f"Failed to set buyer class: {str(e)}")
    
    def update_service_level_goal(
        self,
        item_id: int,
        service_level_goal: float
    ) -> bool:
        """Update service level goal for an item.
        
        Args:
            item_id: Item ID
            service_level_goal: Service level goal
            
        Returns:
            True if service level goal was updated successfully
        """
        item = self.get_item(item_id)
        if not item:
            raise ItemError(f"Item with ID {item_id} not found")
        
        # Validate service level goal
        if service_level_goal < 0 or service_level_goal > 100:
            raise ItemError(f"Invalid service level goal: {service_level_goal}. Must be between 0 and 100")
        
        # Update service level goal
        item.service_level_goal = service_level_goal
        item.service_level_maintained = True
        
        # Avoid circular imports
        ss_service = None
        try:
            # Only import here to avoid circular dependencies
            from .safety_stock_service import SafetyStockService
            ss_service = SafetyStockService(self.session)
        except (ImportError, AttributeError):
            logger.info("SafetyStockService not available, using internal method")
            ss_service = None
            
        if ss_service:
            try:
                ss_service.update_safety_stock_for_item(item_id, update_sstf=True, update_order_points=True)
            except Exception as e:
                logger.warning(f"Error updating safety stock: {str(e)}")
                # Fall back to internal method on error
                self._recalculate_safety_stock(item)
        else:
            # Fall back to internal method if safety stock service is not available
            self._recalculate_safety_stock(item)
        
        try:
            self.session.commit()
            return True
        except Exception as e:
            self.session.rollback()
            raise ItemError(f"Failed to update service level goal: {str(e)}")
    
    def update_lead_time(
        self,
        item_id: int,
        lead_time_forecast: int,
        lead_time_variance: float
    ) -> bool:
        """Update lead time for an item.
        
        Args:
            item_id: Item ID
            lead_time_forecast: Lead time forecast
            lead_time_variance: Lead time variance
            
        Returns:
            True if lead time was updated successfully
        """
        item = self.get_item(item_id)
        if not item:
            raise ItemError(f"Item with ID {item_id} not found")
        
        # Validate lead time
        if lead_time_forecast < 0:
            raise ItemError(f"Invalid lead time forecast: {lead_time_forecast}. Must be non-negative")
            
        if lead_time_variance < 0:
            raise ItemError(f"Invalid lead time variance: {lead_time_variance}. Must be non-negative")
        
        # Update lead time
        item.lead_time_forecast = lead_time_forecast
        item.lead_time_variance = lead_time_variance
        item.lead_time_maintained = True
        
        # Avoid circular imports
        ss_service = None
        try:
            # Only import here to avoid circular dependencies
            from .safety_stock_service import SafetyStockService
            ss_service = SafetyStockService(self.session)
        except (ImportError, AttributeError):
            logger.info("SafetyStockService not available, using internal method")
            ss_service = None
            
        if ss_service:
            try:
                ss_service.update_safety_stock_for_item(item_id, update_sstf=True, update_order_points=True)
            except Exception as e:
                logger.warning(f"Error updating safety stock: {str(e)}")
                # Fall back to internal method on error
                self._recalculate_safety_stock(item)
        else:
            # Fall back to internal method if safety stock service is not available
            self._recalculate_safety_stock(item)
        
        try:
            self.session.commit()
            return True
        except Exception as e:
            self.session.rollback()
            raise ItemError(f"Failed to update lead time: {str(e)}")
    
    def _recalculate_safety_stock(self, item: Item) -> None:
        """Recalculate safety stock for an item.
        
        Args:
            item: Item object
        """
        # Get vendor
        vendor = self.session.query(Vendor).get(item.vendor_id)
        if not vendor:
            logger.warning(f"Vendor with ID {item.vendor_id} not found")
            return
        
        # Get effective order cycle
        vendor_cycle = vendor.order_cycle if vendor.order_cycle is not None else 0
        item_cycle = item.item_cycle_days if item.item_cycle_days is not None else 0
        effective_order_cycle = max(vendor_cycle, item_cycle)
        
        # Calculate safety stock in days
        safety_stock_days = calculate_safety_stock(
            service_level_goal=item.service_level_goal or self.company_settings['service_level_goal'],
            madp=item.madp if item.madp is not None else 0.0,
            lead_time=item.lead_time_forecast if item.lead_time_forecast is not None else 0,
            lead_time_variance=item.lead_time_variance if item.lead_time_variance is not None else 0.0,
            order_cycle=effective_order_cycle
        )
        
        # Calculate safety stock in units (avoid division by zero)
        daily_demand = item.demand_4weekly / 28 if item.demand_4weekly else 0.0
        safety_stock_units = calculate_safety_stock_units(safety_stock_days, daily_demand)
        
        # Update item
        item.sstf = safety_stock_days
        
        # Calculate order points and levels
        lead_time = item.lead_time_forecast if item.lead_time_forecast is not None else 0
        item.item_order_point_days = safety_stock_days + lead_time
        item.item_order_point_units = item.item_order_point_days * daily_demand
        
        vendor_cycle = vendor.order_cycle if vendor.order_cycle is not None else 0
        item.vendor_order_point_days = item.item_order_point_days + vendor_cycle
        
        item.order_up_to_level_days = item.item_order_point_days + effective_order_cycle
        item.order_up_to_level_units = item.order_up_to_level_days * daily_demand
    
    def update_stock_status(
        self,
        item_id: int,
        on_hand: Optional[float] = None,
        on_order: Optional[float] = None,
        customer_back_order: Optional[float] = None,
        reserved: Optional[float] = None,
        held_until: Optional[date] = None,
        quantity_held: Optional[float] = None
    ) -> bool:
        """Update stock status for an item.
        
        Args:
            item_id: Item ID
            on_hand: On hand quantity
            on_order: On order quantity
            customer_back_order: Customer back order quantity
            reserved: Reserved quantity
            held_until: Date until which quantity is held
            quantity_held: Quantity held
            
        Returns:
            True if stock status was updated successfully
        """
        item = self.get_item(item_id)
        if not item:
            raise ItemError(f"Item with ID {item_id} not found")
        
        # Update fields if provided
        if on_hand is not None:
            item.on_hand = on_hand
            
        if on_order is not None:
            item.on_order = on_order
            
        if customer_back_order is not None:
            item.customer_back_order = customer_back_order
            
        if reserved is not None:
            item.reserved = reserved
            
        if held_until is not None:
            item.held_until = held_until
            
        if quantity_held is not None:
            item.quantity_held = quantity_held
        
        try:
            self.session.commit()
            return True
        except Exception as e:
            self.session.rollback()
            raise ItemError(f"Failed to update stock status: {str(e)}")
    
    def get_current_balance(self, item_id: int) -> float:
        """Get current available balance for an item.
        
        Args:
            item_id: Item ID
            
        Returns:
            Current available balance
        """
        item = self.get_item(item_id)
        if not item:
            raise ItemError(f"Item with ID {item_id} not found")
        
        # Calculate available balance
        balance = (item.on_hand or 0.0) + (item.on_order or 0.0)
        
        # Subtract customer back order
        if item.customer_back_order:
            balance -= item.customer_back_order
        
        # Subtract reserved quantity
        if item.reserved:
            balance -= item.reserved
        
        # Subtract held quantity if still within hold date
        if item.quantity_held and item.held_until and item.held_until >= date.today():
            balance -= item.quantity_held
        
        return balance
    
    def update_item_stock_status(
        self,
        warehouse_id: Optional[int] = None,
        vendor_id: Optional[int] = None,
        item_id: Optional[int] = None
    ) -> Dict[str, Any]:
        """Update stock status for all items matching criteria.
        
        Args:
            warehouse_id: Optional warehouse ID
            vendor_id: Optional vendor ID
            item_id: Optional item ID
            
        Returns:
            Dictionary with update results
        """
        # Build query for items
        query = self.session.query(Item)
        
        if warehouse_id is not None:
            query = query.filter(Item.warehouse_id == warehouse_id)
            
        if vendor_id is not None:
            query = query.filter(Item.vendor_id == vendor_id)
            
        if item_id is not None:
            query = query.filter(Item.id == item_id)
        
        # Get all items
        items = query.all()
        
        results = {
            'total_items': len(items),
            'updated_items': 0,
            'errors': 0,
            'details': []
        }
        
        # Process each item
        for item in items:
            try:
                # Fetch current inventory status
                inventory = self.session.query(
                    func.sum(Inventory.quantity).label('total_quantity'),
                    func.sum(Inventory.allocated_quantity).label('total_allocated'),
                    func.max(Inventory.last_receipt_date).label('last_receipt'),
                    func.max(Inventory.last_issue_date).label('last_issue')
                ).filter(
                    Inventory.item_id == item.id,
                    Inventory.warehouse_id == item.warehouse_id
                ).group_by(
                    Inventory.item_id,
                    Inventory.warehouse_id
                ).first()
                
                if inventory:
                    # Update item's stock status based on inventory
                    item.on_hand = inventory.total_quantity or 0.0
                    item.reserved = inventory.total_allocated or 0.0
                    
                    # Calculate available quantity
                    available = (inventory.total_quantity or 0.0) - (inventory.total_allocated or 0.0)
                    
                    # Update back orders if available quantity is negative
                    if available < 0:
                        item.customer_back_order = abs(available)
                    else:
                        item.customer_back_order = 0.0
                    
                    # Update last receipt/issue dates
                    if inventory.last_receipt:
                        item.last_receipt_date = inventory.last_receipt
                    if inventory.last_issue:
                        item.last_issue_date = inventory.last_issue
                    
                    # Recalculate safety stock based on new inventory levels
                    self._recalculate_safety_stock(item)
                    
                    results['details'].append({
                        'item_id': item.id,
                        'item_code': item.item_id,
                        'on_hand': item.on_hand,
                        'reserved': item.reserved,
                        'back_orders': item.customer_back_order,
                        'status': 'UPDATED'
                    })
                else:
                    # No inventory record found
                    results['details'].append({
                        'item_id': item.id,
                        'item_code': item.item_id,
                        'status': 'NO_INVENTORY_RECORD'
                    })
                
                results['updated_items'] += 1
                
            except Exception as e:
                logger.error(f"Error updating stock status for item {item.id}: {str(e)}")
                results['errors'] += 1
                results['details'].append({
                    'item_id': item.id,
                    'item_code': item.item_id,
                    'status': 'ERROR',
                    'error': str(e)
                })
        
        try:
            self.session.commit()
        except Exception as e:
            self.session.rollback()
            logger.error(f"Error committing stock status updates: {str(e)}")
            results['errors'] += 1
        
        return results
    
    def calculate_lost_sales(
        self,
        warehouse_id: Optional[int] = None,
        vendor_id: Optional[int] = None,
        item_id: Optional[int] = None
    ) -> Dict[str, Any]:
        """Calculate lost sales for items matching criteria.
        
        Args:
            warehouse_id: Optional warehouse ID
            vendor_id: Optional vendor ID
            item_id: Optional item ID
            
        Returns:
            Dictionary with calculation results
        """
        # Check if lost sales calculation is enabled
        if self.company_settings['demand_from_days_out'] == 0:
            return {
                'success': True,
                'message': 'Lost sales calculation is not enabled',
                'total_items': 0,
                'updated_items': 0,
                'errors': 0
            }
        
        # Build query for items
        query = self.session.query(Item)
        
        if warehouse_id is not None:
            query = query.filter(Item.warehouse_id == warehouse_id)
            
        if vendor_id is not None:
            query = query.filter(Item.vendor_id == vendor_id)
            
        if item_id is not None:
            query = query.filter(Item.id == item_id)
        
        # Only include active items
        query = query.filter(Item.buyer_class.in_(['R', 'W']))  # Use string values that match enum values
        
        # Get all items
        items = query.all()
        
        results = {
            'total_items': len(items),
            'updated_items': 0,
            'calculated_lost_sales': 0,
            'errors': 0
        }
        
        # Get current period - Fixed: Provide the periodicity parameter
        periodicity = self.company_settings['history_periodicity_default']
        current_period, current_year = get_current_period(periodicity)
        
        
        # Process each item
        for item in items:
            try:
                # Get history period
                history_period = self.session.query(DemandHistory).filter(
                    DemandHistory.item_id == item.id,
                    DemandHistory.period_number == current_period,
                    DemandHistory.period_year == current_year
                ).first()
                
                if not history_period:
                    # Create new history period if it doesn't exist
                    history_period = DemandHistory(
                        item_id=item.id,
                        period_number=current_period,
                        period_year=current_year,
                        shipped=0.0,
                        lost_sales=0.0,
                        promotional_demand=0.0,
                        total_demand=0.0,
                        out_of_stock_days=0
                    )
                    
                    self.session.add(history_period)
                
                # Check if there are any out of stock days
                if history_period.out_of_stock_days <= 0:
                    continue
                
                # Get daily forecast (avoid division by zero)
                daily_forecast = item.demand_4weekly / 28 if item.demand_4weekly else 0.0
                
                # Get seasonal indices if available
                seasonal_indices = None
                current_period_index = None
                
                if item.demand_profile:
                    # Get profile
                    profile = self.session.query(SeasonalProfile).filter(
                        SeasonalProfile.profile_id == item.demand_profile
                    ).first()
                    
                    if profile:
                        # Get indices
                        indices = self.session.query(SeasonalProfileIndex).filter(
                            SeasonalProfileIndex.profile_id == profile.profile_id
                        ).order_by(SeasonalProfileIndex.period_number).all()
                        
                        if indices:  # Check that we got indices before using them
                            seasonal_indices = [index.index_value for index in indices]
                            current_period_index = current_period - 1  # Convert to 0-based index
                
                # Calculate lost sales using the imported function (renamed to avoid confusion)
                lost_sales = core_calculate_lost_sales(
                    history_period.out_of_stock_days,
                    daily_forecast,
                    seasonal_indices,
                    current_period_index
                )
                
                # Round to 2 decimal places
                lost_sales = round(lost_sales, 2)
                
                # Update history period
                if lost_sales > 0:
                    history_period.lost_sales = lost_sales
                    history_period.total_demand = history_period.shipped + lost_sales - history_period.promotional_demand
                    results['calculated_lost_sales'] += lost_sales
                
                results['updated_items'] += 1
                
            except Exception as e:
                logger.error(f"Error calculating lost sales for item {item.id}: {str(e)}")
                results['errors'] += 1
        
        try:
            self.session.commit()
        except Exception as e:
            self.session.rollback()
            logger.error(f"Error committing lost sales calculations: {str(e)}")
            results['errors'] += 1
        
        return results
    
    def initialize_forecast(
        self,
        item_id: int,
        initial_forecast: float
    ) -> bool:
        """Initialize forecast for an uninitialized item.
        
        Args:
            item_id: Item ID
            initial_forecast: Initial forecast value
            
        Returns:
            True if forecast was initialized successfully
        """
        item = self.get_item(item_id)
        if not item:
            raise ItemError(f"Item with ID {item_id} not found")
        
        if item.buyer_class != BuyerClassCode.UNINITIALIZED:
            logger.warning(f"Item {item_id} is not uninitialized (buyer class: {item.buyer_class})")
        
        # Update forecast values
        item.demand_4weekly = initial_forecast
        item.demand_weekly = initial_forecast / 4
        item.demand_monthly = initial_forecast * (365/12) / (365/13)
        item.demand_quarterly = initial_forecast * 3
        item.demand_yearly = initial_forecast * 13
        
        # Set initial MADP and track
        item.madp = 20  # Default MADP for new items
        item.track = 0  # Default track for new items
        
        # Update system class
        if item.demand_yearly <= self.company_settings['slow_mover_limit']:
            item.system_class = SystemClassCode.SLOW
        else:
            item.system_class = SystemClassCode.NEW
        
        # Update buyer class if it's uninitialized
        if item.buyer_class == BuyerClassCode.UNINITIALIZED:
            item.buyer_class = BuyerClassCode.REGULAR
        
        # Set forecast date
        item.forecast_date = datetime.now()
        
        # Recalculate safety stock
        self._recalculate_safety_stock(item)
        
        try:
            self.session.commit()
            return True
        except Exception as e:
            self.session.rollback()
            raise ItemError(f"Failed to initialize forecast: {str(e)}")
    
    def get_uninitialized_items(
        self,
        warehouse_id: Optional[int] = None,
        vendor_id: Optional[int] = None
    ) -> List[Item]:
        """Get all uninitialized items.
        
        Args:
            warehouse_id: Optional warehouse ID
            vendor_id: Optional vendor ID
            
        Returns:
            List of uninitialized item objects
        """
        query = self.session.query(Item).filter(Item.buyer_class == BuyerClassCode.UNINITIALIZED)
        
        if warehouse_id is not None:
            query = query.filter(Item.warehouse_id == warehouse_id)
            
        if vendor_id is not None:
            query = query.filter(Item.vendor_id == vendor_id)
            
        return query.all()
    
    def get_out_of_stock_items(
        self,
        warehouse_id: Optional[int] = None,
        vendor_id: Optional[int] = None
    ) -> List[Item]:
        """Get all out of stock items.
        
        Args:
            warehouse_id: Optional warehouse ID
            vendor_id: Optional vendor ID
            
        Returns:
            List of out of stock item objects
        """
        query = self.session.query(Item).filter(
            Item.on_hand <= 0,
            Item.buyer_class.in_([BuyerClassCode.REGULAR, BuyerClassCode.WATCH])
        )
        
        if warehouse_id is not None:
            query = query.filter(Item.warehouse_id == warehouse_id)
            
        if vendor_id is not None:
            query = query.filter(Item.vendor_id == vendor_id)
            
        return query.all()
    
    def transfer_item_between_vendors(
        self,
        item_id: int,
        new_vendor_id: int,
        transfer_history: bool = True,
        transfer_stock_status: bool = True
    ) -> bool:
        """Transfer an item from one vendor to another.
        
        Args:
            item_id: Item ID
            new_vendor_id: New vendor ID
            transfer_history: Whether to transfer demand history
            transfer_stock_status: Whether to transfer stock status
            
        Returns:
            True if item was transferred successfully
        """
        item = self.get_item(item_id)
        if not item:
            raise ItemError(f"Item with ID {item_id} not found")
        
        # Get new vendor
        new_vendor = self.session.query(Vendor).get(new_vendor_id)
        if not new_vendor:
            raise ItemError(f"Vendor with ID {new_vendor_id} not found")
        
        # Check if item already exists for new vendor
        existing_item = self.session.query(Item).filter(
            Item.item_id == item.item_id,
            Item.vendor_id == new_vendor_id,
            Item.warehouse_id == item.warehouse_id
        ).first()
        
        if existing_item:
            raise ItemError(f"Item with ID {item.item_id} already exists for vendor {new_vendor_id} in warehouse {item.warehouse_id}")
        
        # Store old vendor ID
        old_vendor_id = item.vendor_id
        
        # Create new history records if requested
        if transfer_history:
            # Get history records to transfer
            history_records = self.session.query(DemandHistory).filter(
                DemandHistory.item_id == item_id
            ).all()
            
            # No need to create a separate list - we'll keep the original records
            # but will need to update item_id after vendor change
        
        # Store stock status if requested
        stock_status = None
        if transfer_stock_status:
            stock_status = {
                'on_hand': item.on_hand,
                'on_order': item.on_order,
                'customer_back_order': item.customer_back_order,
                'reserved': item.reserved,
                'held_until': item.held_until,
                'quantity_held': item.quantity_held
            }
        
        # Update vendor ID
        item.vendor_id = new_vendor_id
        
        # Apply new vendor settings if needed
        # For this example, we'll keep the existing item settings
        
        try:
            self.session.commit()
            
            # Update vendor active items count for both vendors
            old_vendor = self.session.query(Vendor).get(old_vendor_id)
            if old_vendor:
                old_vendor.active_items_count = self.session.query(func.count(Item.id)).filter(
                    Item.vendor_id == old_vendor_id,
                    Item.buyer_class.in_([BuyerClassCode.REGULAR, BuyerClassCode.WATCH])
                ).scalar() or 0
                
            new_vendor.active_items_count = self.session.query(func.count(Item.id)).filter(
                Item.vendor_id == new_vendor_id,
                Item.buyer_class.in_([BuyerClassCode.REGULAR, BuyerClassCode.WATCH])
            ).scalar() or 0
            
            self.session.commit()
            
            return True
        except Exception as e:
            self.session.rollback()
            raise ItemError(f"Failed to transfer item between vendors: {str(e)}")
    
    def apply_supersession(
        self,
        from_item_id: int,
        to_item_id: int,
        copy_history: bool = True,
        copy_demand_profile: bool = True
    ) -> bool:
        """Apply supersession from one item to another.
        
        Args:
            from_item_id: Source item ID
            to_item_id: Target item ID
            copy_history: Whether to copy demand history
            copy_demand_profile: Whether to copy demand profile
            
        Returns:
            True if supersession was applied successfully
        """
        from_item = self.get_item(from_item_id)
        if not from_item:
            raise ItemError(f"Source item with ID {from_item_id} not found")
        
        to_item = self.get_item(to_item_id)
        if not to_item:
            raise ItemError(f"Target item with ID {to_item_id} not found")
        
        # Set supersession relationships
        from_item.supersede_to_item_id = to_item.item_id
        to_item.supersede_from_item_id = from_item.item_id
        
        # Copy demand profile if needed
        if copy_demand_profile and from_item.demand_profile and not to_item.demand_profile:
            to_item.demand_profile = from_item.demand_profile
        
        # Copy history if needed
        if copy_history:
            # Get history records for source item
            history_records = self.session.query(DemandHistory).filter(
                DemandHistory.item_id == from_item_id
            ).all()
            
            # Copy history to target item
            for record in history_records:
                # Check if history already exists for target item
                existing_record = self.session.query(DemandHistory).filter(
                    DemandHistory.item_id == to_item_id,
                    DemandHistory.period_number == record.period_number,
                    DemandHistory.period_year == record.period_year
                ).first()
                
                if existing_record:
                    # Update existing record
                    existing_record.shipped += record.shipped
                    existing_record.lost_sales += record.lost_sales
                    existing_record.promotional_demand += record.promotional_demand
                    existing_record.total_demand = (
                        existing_record.shipped + 
                        existing_record.lost_sales - 
                        existing_record.promotional_demand
                    )
                    existing_record.is_adjusted = True
                else:
                    # Create new record
                    new_record = DemandHistory(
                        item_id=to_item_id,
                        period_number=record.period_number,
                        period_year=record.period_year,
                        shipped=record.shipped,
                        lost_sales=record.lost_sales,
                        promotional_demand=record.promotional_demand,
                        total_demand=record.total_demand,
                        is_ignored=record.is_ignored,
                        is_adjusted=record.is_adjusted,
                        out_of_stock_days=record.out_of_stock_days
                    )
                    self.session.add(new_record)
        
        # Set auxiliary balance on target item
        to_item.auxiliary_balance = from_item.on_hand
        
        # Change source item's buyer class if it's not already discontinued
        if from_item.buyer_class != BuyerClassCode.DISCONTINUED:
            from_item.buyer_class = BuyerClassCode.MANUAL
        
        try:
            self.session.commit()
            return True
        except Exception as e:
            self.session.rollback()
            raise ItemError(f"Failed to apply supersession: {str(e)}")
    
    def remove_supersession(
        self,
        item_id: int,
        remove_history: bool = False
    ) -> bool:
        """Remove supersession for an item.
        
        Args:
            item_id: Item ID
            remove_history: Whether to remove copied history
            
        Returns:
            True if supersession was removed successfully
        """
        item = self.get_item(item_id)
        if not item:
            raise ItemError(f"Item with ID {item_id} not found")
        
        # Find supersession relationships
        super_to = None
        super_from = None
        
        if item.supersede_to_item_id:
            super_to = self.session.query(Item).filter(
                Item.item_id == item.supersede_to_item_id,
                Item.warehouse_id == item.warehouse_id
            ).first()
        
        if item.supersede_from_item_id:
            super_from = self.session.query(Item).filter(
                Item.item_id == item.supersede_from_item_id,
                Item.warehouse_id == item.warehouse_id
            ).first()
        
        # Clear supersession references
        item.supersede_to_item_id = None
        item.supersede_from_item_id = None
        
        # Update related items
        if super_to:
            super_to.supersede_from_item_id = None
            super_to.auxiliary_balance = 0.0
        
        if super_from:
            super_from.supersede_to_item_id = None
        
        # Remove copied history if requested
        if remove_history and super_from and item.id != super_from.id:
            # The proper implementation would look at timestamps to identify copied history
            # For this implementation, we'll mark all history as adjusted
            history_records = self.session.query(DemandHistory).filter(
                DemandHistory.item_id == item.id
            ).all()
            
            for record in history_records:
                record.is_adjusted = True
        
        try:
            self.session.commit()
            return True
        except Exception as e:
            self.session.rollback()
            raise ItemError(f"Failed to remove supersession: {str(e)}")
    
    def get_item_demand_history(
        self,
        item_id: int,
        periods: int = None,
        include_ignored: bool = False
    ) -> List[Dict]:
        """Get demand history for an item.
        
        Args:
            item_id: Item ID
            periods: Number of periods to retrieve (most recent)
            include_ignored: Whether to include ignored periods
            
        Returns:
            List of demand history dictionaries
        """
        query = self.session.query(DemandHistory).filter(
            DemandHistory.item_id == item_id
        )
        
        if not include_ignored:
            query = query.filter(DemandHistory.is_ignored == False)
        
        # Order by period year and number in descending order
        query = query.order_by(
            DemandHistory.period_year.desc(),
            DemandHistory.period_number.desc()
        )
        
        if periods:
            query = query.limit(periods)
        
        results = query.all()
        
        # Convert to dictionaries
        history = [
            {
                'period_number': record.period_number,
                'period_year': record.period_year,
                'shipped': record.shipped,
                'lost_sales': record.lost_sales,
                'promotional_demand': record.promotional_demand,
                'total_demand': record.total_demand,
                'is_ignored': record.is_ignored,
                'is_adjusted': record.is_adjusted,
                'out_of_stock_days': record.out_of_stock_days
            }
            for record in results
        ]
        
        return history