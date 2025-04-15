# warehouse_replenishment/warehouse_replenishment/services/item_service.py
from datetime import date, datetime, timedelta
from typing import List, Dict, Tuple, Optional, Union, Any
import logging

from sqlalchemy import and_, func, or_
from sqlalchemy.orm import Session

from ..models import (
    Item, DemandHistory, Company, Vendor, SeasonalProfile, 
    SeasonalProfileIndex, HistoryException
)
from ..core.demand_forecast import (
    calculate_lost_sales, adjust_history_value
)
from ..core.safety_stock import (
    calculate_safety_stock, calculate_safety_stock_units
)
from ..exceptions import ItemError

logger = logging.getLogger(__name__)

class ItemService:
    """Service for handling item operations."""
    
    def __init__(self, session: Session):
        """Initialize the item service.
        
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
                raise ItemError("Company settings not found")
            
            self._company_settings = {
                'service_level_goal': company.service_level_goal,
                'demand_from_days_out': company.demand_from_days_out,
                'lead_time_forecast_control': company.lead_time_forecast_control,
                'history_periodicity_default': company.history_periodicity_default,
                'forecasting_periodicity_default': company.forecasting_periodicity_default,
                'slow_mover_limit': company.slow_mover_limit,
                'lumpy_demand_limit': company.lumpy_demand_limit
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
            system_class: Optional list of system classes
            active_only: Whether to include only active items
            
        Returns:
            List of item objects
        """
        query = self.session.query(Item)
        
        if warehouse_id is not None:
            query = query.filter(Item.warehouse_id == warehouse_id)
            
        if vendor_id is not None:
            query = query.filter(Item.vendor_id == vendor_id)
            
        if buyer_id is not None:
            query = query.filter(Item.buyer_id == buyer_id)
            
        if item_group is not None:
            # Search within item group codes using LIKE
            query = query.filter(Item.item_group_codes.like(f'%{item_group}%'))
            
        if buyer_class:
            query = query.filter(Item.buyer_class.in_(buyer_class))
        elif active_only:
            # Default to active items (Regular or Watch)
            query = query.filter(Item.buyer_class.in_(['R', 'W']))
            
        if system_class:
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
                Item.buyer_class.in_(['R', 'W'])
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
        buyer_class: str
    ) -> bool:
        """Set buyer class for an item.
        
        Args:
            item_id: Item ID
            buyer_class: Buyer class
            
        Returns:
            True if buyer class was set successfully
        """
        item = self.get_item(item_id)
        if not item:
            raise ItemError(f"Item with ID {item_id} not found")
        
        # Validate buyer class
        valid_classes = ['R', 'W', 'M', 'D', 'U']
        if buyer_class not in valid_classes:
            raise ItemError(f"Invalid buyer class: {buyer_class}. Valid values are {valid_classes}")
        
        # Update buyer class
        item.buyer_class = buyer_class
        
        try:
            self.session.commit()
            
            # Update vendor active items count if necessary
            if buyer_class in ['R', 'W'] or item.buyer_class in ['R', 'W']:
                vendor = self.session.query(Vendor).get(item.vendor_id)
                if vendor:
                    vendor.active_items_count = self.session.query(func.count(Item.id)).filter(
                        Item.vendor_id == item.vendor_id,
                        Item.buyer_class.in_(['R', 'W'])
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
        
        # Recalculate safety stock
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
        
        # Recalculate safety stock
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
        effective_order_cycle = max(vendor.order_cycle or 0, item.item_cycle_days or 0)
        
        # Calculate safety stock in days
        safety_stock_days = calculate_safety_stock(
            service_level_goal=item.service_level_goal,
            madp=item.madp,
            lead_time=item.lead_time_forecast,
            lead_time_variance=item.lead_time_variance,
            order_cycle=effective_order_cycle
        )
        
        # Calculate safety stock in units
        daily_demand = item.demand_4weekly / 28  # Assuming 28 days in a 4-weekly period
        safety_stock_units = calculate_safety_stock_units(safety_stock_days, daily_demand)
        
        # Update item
        item.sstf = safety_stock_days
        
        # Calculate order points and levels
        item.item_order_point_days = safety_stock_days + item.lead_time_forecast
        item.item_order_point_units = item.item_order_point_days * daily_demand
        
        item.vendor_order_point_days = item.item_order_point_days + vendor.order_cycle
        
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
            'errors': 0
        }
        
        # Process each item
        for item in items:
            try:
                # In a real implementation, this would update stock status from a host system
                # For this example, we'll just recalculate safety stock
                self._recalculate_safety_stock(item)
                results['updated_items'] += 1
                
            except Exception as e:
                logger.error(f"Error updating stock status for item {item.id}: {str(e)}")
                results['errors'] += 1
        
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
        query = query.filter(Item.buyer_class.in_(['R', 'W']))
        
        # Get all items
        items = query.all()
        
        results = {
            'total_items': len(items),
            'updated_items': 0,
            'calculated_lost_sales': 0,
            'errors': 0
        }
        
        # Get current period
        periodicity = self.company_settings['history_periodicity_default']
        current_period, current_year = self._get_current_period(periodicity)
        
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
                
                # Get daily forecast
                daily_forecast = item.demand_4weekly / 28  # Assuming 28 days in a 4-weekly period
                
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
                        
                        seasonal_indices = [index.index_value for index in indices]
                        current_period_index = current_period - 1  # Convert to 0-based index
                
                # Calculate lost sales
                lost_sales = calculate_lost_sales(
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
    
    def _get_current_period(self, periodicity: int) -> Tuple[int, int]:
        """Get the current period number and year.
        
        Args:
            periodicity: Periodicity (12=monthly, 13=4-weekly, 52=weekly)
            
        Returns:
            Tuple with period number and year
        """
        today = date.today()
        year = today.year
        
        if periodicity == 12:  # Monthly
            return (today.month, year)
        
        elif periodicity == 13:  # 4-weekly
            # Calculate which 4-week period we're in
            # Each period is 28 days (4 weeks)
            day_of_year = today.timetuple().tm_yday
            period = ((day_of_year - 1) // 28) + 1
            
            # Handle period rollover to next year
            if period > 13:
                period = 1
                year += 1
                
            return (period, year)
        
        elif periodicity == 52:  # Weekly
            # ISO week number
            week = today.isocalendar()[1]
            
            # Handle year edge cases
            if week > 52:
                week = 1
                year += 1
            
            return (week, year)
        
        else:
            raise ValueError(f"Invalid periodicity: {periodicity}")
    
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
        
        if item.buyer_class != 'U':
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
            item.system_class = 'S'  # Slow
        else:
            item.system_class = 'N'  # New
        
        # Update buyer class if it's uninitialized
        if item.buyer_class == 'U':
            item.buyer_class = 'R'  # Regular
        
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
        query = self.session.query(Item).filter(Item.buyer_class == 'U')
        
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
            Item.buyer_class.in_(['R', 'W'])
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
        quantity: Optional[float] = None
    ) -> bool:
        """Transfer an item between vendors.
        
        Args:
            item_id: Item ID
            new_vendor_id: New vendor ID
            quantity: Optional quantity to transfer
            
        Returns:
            True if item was transferred successfully
        """
        item = self.get_item(item_id)
        if not item:
            raise ItemError(f"Item with ID {item_id} not found")
        
        new_vendor = self.session.query(Vendor).get(new_vendor_id)
        if not new_vendor:
            raise ItemError(f"New vendor with ID {new_vendor_id} not found")
        
        # Check if transfer is between vendors in the same warehouse
        if item.warehouse_id != new_vendor.warehouse_id:
            raise ItemError(f"Cannot transfer item between different warehouses")
        
        # Check if quantity is provided
        if quantity is not None:
            # Create new item for new vendor if it doesn't exist
            new_item = self.session.query(Item).filter(
                Item.item_id == item.item_id,
                Item.vendor_id == new_vendor_id,
                Item.warehouse_id == item.warehouse_id
            ).first()
            
            if not new_item:
                # Create new item
                new_item_id = self.create_item(
                    item_id=item.item_id,
                    description=item.description,
                    vendor_id=new_vendor_id,
                    warehouse_id=item.warehouse_id,
                    service_level_goal=item.service_level_goal,
                    lead_time_forecast=item.lead_time_forecast,
                    lead_time_variance=item.lead_time_variance,
                    buying_multiple=item.buying_multiple,
                    minimum_quantity=item.minimum_quantity,
                    purchase_price=item.purchase_price,
                    sales_price=item.sales_price,
                    buyer_id=item.buyer_id,
                    buyer_class=item.buyer_class
                )
                
                new_item = self.get_item(new_item_id)
            
            # Transfer quantity
            if quantity > item.on_hand:
                raise ItemError(f"Cannot transfer {quantity} units, only {item.on_hand} available")
                
            item.on_hand -= quantity
            new_item.on_hand += quantity
            
        else:
            # Transfer entire item
            # Update vendor ID
            item.vendor_id = new_vendor_id
        
        try:
            self.session.commit