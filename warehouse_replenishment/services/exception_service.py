# warehouse_replenishment/services/exception_service.py
from datetime import date, datetime, timedelta
from typing import List, Dict, Tuple, Optional, Union, Any
import logging
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
    Item, Company, Warehouse, Vendor,
    HistoryException, ManagementException, ManagementExceptionItem,
    ArchivedHistoryException
)
from warehouse_replenishment.core.demand_forecast import (
    detect_demand_spike, detect_tracking_signal_exception
)
from warehouse_replenishment.exceptions import ForecastError, OrderError

from warehouse_replenishment.logging_setup import logger
logger = logging.getLogger(__name__)

class ExceptionService:
    """Service for handling exception-related operations."""
    
    def __init__(self, session: Session):
        """Initialize the exception service.
        
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
                raise Exception("Company settings not found")
            
            self._company_settings = {
                'demand_filter_high': company.demand_filter_high,
                'demand_filter_low': company.demand_filter_low,
                'tracking_signal_limit': company.tracking_signal_limit,
                'keep_archived_exceptions_days': company.keep_archived_exceptions_days
            }
        
        return self._company_settings
    
    def get_history_exception(self, exception_id: int) -> Optional[HistoryException]:
        """Get a history exception by ID.
        
        Args:
            exception_id: Exception ID
            
        Returns:
            History exception object or None if not found
        """
        return self.session.query(HistoryException).get(exception_id)
    
    def get_history_exceptions(
        self,
        item_id: Optional[int] = None,
        warehouse_id: Optional[str] = None,
        vendor_id: Optional[int] = None,
        exception_type: Optional[str] = None,
        is_resolved: Optional[bool] = None,
        from_date: Optional[datetime] = None,
        to_date: Optional[datetime] = None
    ) -> List[HistoryException]:
        """Get history exceptions matching criteria.
        
        Args:
            item_id: Optional item ID filter
            warehouse_id: Optional warehouse ID filter
            vendor_id: Optional vendor ID filter
            exception_type: Optional exception type filter
            is_resolved: Optional resolution status filter
            from_date: Optional start date filter
            to_date: Optional end date filter
            
        Returns:
            List of history exception objects
        """
        query = self.session.query(HistoryException)
        
        # Apply item filter
        if item_id is not None:
            query = query.filter(HistoryException.item_id == item_id)

        # Apply vendor and warehouse filters
        if vendor_id is not None or warehouse_id is not None:
            query = query.join(Item, HistoryException.item_id == Item.id)
            
            if vendor_id is not None:
                query = query.filter(Item.vendor_id == vendor_id)
                
            if warehouse_id is not None:
                query = query.filter(Item.warehouse_id == warehouse_id)
        
        # Apply exception type filter
        if exception_type is not None:
            query = query.filter(HistoryException.exception_type == exception_type)
        
        # Apply resolution status filter
        if is_resolved is not None:
            query = query.filter(HistoryException.is_resolved == is_resolved)
        
        # Apply date filters
        if from_date is not None:
            query = query.filter(HistoryException.creation_date >= from_date)
            
        if to_date is not None:
            query = query.filter(HistoryException.creation_date <= to_date)
        
        # Order by creation date (most recent first)
        query = query.order_by(HistoryException.creation_date.desc())
        
        return query.all()
    
    def create_history_exception(
        self,
        item_id: int,
        exception_type: str,
        period_number: int,
        period_year: int,
        forecast_value: Optional[float] = None,
        actual_value: Optional[float] = None,
        madp: Optional[float] = None,
        track: Optional[float] = None,
        notes: Optional[str] = None
    ) -> int:
        """Create a history exception.
        
        Args:
            item_id: Item ID
            exception_type: Exception type
            period_number: Period number
            period_year: Period year
            forecast_value: Optional forecast value
            actual_value: Optional actual value
            madp: Optional MADP value
            track: Optional track value
            notes: Optional notes
            
        Returns:
            ID of the created exception
        """
        # Check if item exists
        item = self.session.query(Item).get(item_id)
        if not item:
            raise ForecastError(f"Item with ID {item_id} not found")
        
        # Check if exception already exists
        existing_exception = self.session.query(HistoryException).filter(
            HistoryException.item_id == item_id,
            HistoryException.exception_type == exception_type,
            HistoryException.period_number == period_number,
            HistoryException.period_year == period_year,
            HistoryException.is_resolved == False
        ).first()
        
        if existing_exception:
            return existing_exception.id
        
        # Create new exception
        exception = HistoryException(
            item_id=item_id,
            exception_type=exception_type,
            creation_date=datetime.now(),
            period_number=period_number,
            period_year=period_year,
            forecast_value=forecast_value,
            actual_value=actual_value,
            madp=madp,
            track=track,
            notes=notes,
            is_resolved=False
        )
        
        self.session.add(exception)
        
        try:
            self.session.commit()
            return exception.id
        except Exception as e:
            self.session.rollback()
            raise ForecastError(f"Failed to create history exception: {str(e)}")
    
    def resolve_history_exception(
        self,
        exception_id: int,
        resolution_action: str,
        resolution_notes: Optional[str] = None
    ) -> bool:
        """Resolve a history exception.
        
        Args:
            exception_id: Exception ID
            resolution_action: Resolution action
            resolution_notes: Optional resolution notes
            
        Returns:
            True if exception was resolved successfully
        """
        exception = self.get_history_exception(exception_id)
        if not exception:
            raise ForecastError(f"Exception with ID {exception_id} not found")
        
        # Update exception
        exception.is_resolved = True
        exception.resolution_date = datetime.now()
        exception.resolution_action = resolution_action
        exception.resolution_notes = resolution_notes
        
        try:
            self.session.commit()
            return True
        except Exception as e:
            self.session.rollback()
            raise ForecastError(f"Failed to resolve exception: {str(e)}")
    
    def archive_history_exceptions(
        self,
        days_to_keep: Optional[int] = None
    ) -> Dict:
        """Archive resolved history exceptions.
        
        Args:
            days_to_keep: Optional number of days to keep resolved exceptions
            
        Returns:
            Dictionary with archiving results
        """
        if days_to_keep is None:
            days_to_keep = self.company_settings.get('keep_archived_exceptions_days', 90)
        
        # Calculate cutoff date
        cutoff_date = datetime.now() - timedelta(days=days_to_keep)
        
        # Get all resolved exceptions older than cutoff date
        exceptions = self.session.query(HistoryException).filter(
            HistoryException.is_resolved == True,
            HistoryException.resolution_date < cutoff_date
        ).all()
        
        results = {
            'total_exceptions': len(exceptions),
            'archived_exceptions': 0,
            'errors': 0
        }
        
        for exception in exceptions:
            try:
                # Create archive record
                archived_exception = ArchivedHistoryException(
                    item_id=exception.item_id,
                    exception_type=exception.exception_type,
                    creation_date=exception.creation_date,
                    resolution_date=exception.resolution_date,
                    period_number=exception.period_number,
                    period_year=exception.period_year,
                    before_forecast=exception.forecast_value,
                    before_madp=exception.madp,
                    before_track=exception.track,
                    after_forecast=exception.forecast_value,  # Updated values would come from item
                    after_madp=exception.madp,
                    after_track=exception.track,
                    resolution_action=exception.resolution_action,
                    resolution_notes=exception.resolution_notes
                )
                
                self.session.add(archived_exception)
                
                # Delete original exception
                self.session.delete(exception)
                
                results['archived_exceptions'] += 1
            except Exception as e:
                logger.error(f"Error archiving exception {exception.id}: {str(e)}")
                results['errors'] += 1
        
        try:
            self.session.commit()
        except Exception as e:
            self.session.rollback()
            logger.error(f"Error committing archived exceptions: {str(e)}")
            results['errors'] += 1
        
        return results
    
    def get_archived_exceptions(
        self,
        item_id: Optional[int] = None,
        exception_type: Optional[str] = None,
        from_date: Optional[datetime] = None,
        to_date: Optional[datetime] = None
    ) -> List[ArchivedHistoryException]:
        """Get archived history exceptions.
        
        Args:
            item_id: Optional item ID filter
            exception_type: Optional exception type filter
            from_date: Optional start date filter
            to_date: Optional end date filter
            
        Returns:
            List of archived history exception objects
        """
        query = self.session.query(ArchivedHistoryException)
        
        # Apply filters
        if item_id is not None:
            query = query.filter(ArchivedHistoryException.item_id == item_id)
            
        if exception_type is not None:
            query = query.filter(ArchivedHistoryException.exception_type == exception_type)
            
        if from_date is not None:
            query = query.filter(ArchivedHistoryException.resolution_date >= from_date)
            
        if to_date is not None:
            query = query.filter(ArchivedHistoryException.resolution_date <= to_date)
        
        # Order by resolution date (most recent first)
        query = query.order_by(ArchivedHistoryException.resolution_date.desc())
        
        return query.all()
    
    def get_management_exception(self, exception_id: int) -> Optional[ManagementException]:
        """Get a management exception by ID.
        
        Args:
            exception_id: Exception ID
            
        Returns:
            Management exception object or None if not found
        """
        return self.session.query(ManagementException).get(exception_id)
    
    def get_management_exceptions(
        self,
        warehouse_id: Optional[str] = None,
        exception_type: Optional[str] = None,
        is_enabled: Optional[bool] = None
    ) -> List[ManagementException]:
        """Get management exceptions matching criteria.
        
        Args:
            warehouse_id: Optional warehouse ID filter
            exception_type: Optional exception type filter
            is_enabled: Optional enabled status filter
            
        Returns:
            List of management exception objects
        """
        query = self.session.query(ManagementException)
        
        # Apply filters
        if warehouse_id is not None:
            query = query.filter(ManagementException.warehouse_id == warehouse_id)
            
        if exception_type is not None:
            query = query.filter(ManagementException.exception_type == exception_type)
            
        if is_enabled is not None:
            query = query.filter(ManagementException.is_enabled == is_enabled)
        
        return query.all()
    
    def create_management_exception(
        self,
        warehouse_id: int,
        exception_type: str,
        parameter_x: Optional[float] = None,
        parameter_y: Optional[float] = None,
        is_enabled: bool = True
    ) -> int:
        """Create a management exception.
        
        Args:
            warehouse_id: Warehouse ID
            exception_type: Exception type
            parameter_x: Optional X parameter
            parameter_y: Optional Y parameter
            is_enabled: Whether the exception is enabled
            
        Returns:
            ID of the created exception
        """
        # Check if warehouse exists
        warehouse = self.session.query(Warehouse).get(warehouse_id)
        if not warehouse:
            raise Exception(f"Warehouse with ID {warehouse_id} not found")
        
        # Check if exception already exists
        existing_exception = self.session.query(ManagementException).filter(
            ManagementException.warehouse_id == warehouse_id,
            ManagementException.exception_type == exception_type
        ).first()
        
        if existing_exception:
            # Update existing exception
            existing_exception.parameter_x = parameter_x
            existing_exception.parameter_y = parameter_y
            existing_exception.is_enabled = is_enabled
            
            try:
                self.session.commit()
                return existing_exception.id
            except Exception as e:
                self.session.rollback()
                raise Exception(f"Failed to update management exception: {str(e)}")
        
        # Create new exception
        exception = ManagementException(
            warehouse_id=warehouse_id,
            exception_type=exception_type,
            parameter_x=parameter_x,
            parameter_y=parameter_y,
            is_enabled=is_enabled
        )
        
        self.session.add(exception)
        
        try:
            self.session.commit()
            return exception.id
        except Exception as e:
            self.session.rollback()
            raise Exception(f"Failed to create management exception: {str(e)}")
    
    def add_item_to_management_exception(
        self,
        exception_id: int,
        item_id: int,
        value_x: Optional[float] = None,
        value_y: Optional[float] = None,
        notes: Optional[str] = None
    ) -> int:
        """Add an item to a management exception.
        
        Args:
            exception_id: Exception ID
            item_id: Item ID
            value_x: Optional X value
            value_y: Optional Y value
            notes: Optional notes
            
        Returns:
            ID of the created management exception item
        """
        # Check if exception exists
        exception = self.get_management_exception(exception_id)
        if not exception:
            raise Exception(f"Management exception with ID {exception_id} not found")
        
        # Check if item exists
        item = self.session.query(Item).get(item_id)
        if not item:
            raise Exception(f"Item with ID {item_id} not found")
        
        # Check if item is already in the exception
        existing_item = self.session.query(ManagementExceptionItem).filter(
            ManagementExceptionItem.exception_id == exception_id,
            ManagementExceptionItem.item_id == item_id
        ).first()
        
        if existing_item:
            return existing_item.id
        
        # Create management exception item
        exception_item = ManagementExceptionItem(
            exception_id=exception_id,
            item_id=item_id,
            creation_date=datetime.now(),
            value_x=value_x,
            value_y=value_y,
            notes=notes,
            is_resolved=False
        )
        
        self.session.add(exception_item)
        
        try:
            self.session.commit()
            return exception_item.id
        except Exception as e:
            self.session.rollback()
            raise Exception(f"Failed to add item to management exception: {str(e)}")
    
    def resolve_management_exception_item(
        self,
        exception_item_id: int,
        resolution_action: str,
        resolution_notes: Optional[str] = None
    ) -> bool:
        """Resolve a management exception item.
        
        Args:
            exception_item_id: Exception item ID
            resolution_action: Resolution action
            resolution_notes: Optional resolution notes
            
        Returns:
            True if exception item was resolved successfully
        """
        exception_item = self.session.query(ManagementExceptionItem).get(exception_item_id)
        if not exception_item:
            raise Exception(f"Management exception item with ID {exception_item_id} not found")
        
        # Update exception item
        exception_item.is_resolved = True
        exception_item.resolution_date = datetime.now()
        exception_item.resolution_action = resolution_action
        exception_item.resolution_notes = resolution_notes
        
        try:
            self.session.commit()
            return True
        except Exception as e:
            self.session.rollback()
            raise Exception(f"Failed to resolve management exception item: {str(e)}")
    
    def detect_inventory_exceptions(
        self,
        warehouse_id: Optional[str] = None,
        vendor_id: Optional[int] = None,
        item_id: Optional[int] = None
    ) -> Dict:
        """Detect inventory exceptions.
        
        Args:
            warehouse_id: Optional warehouse ID
            vendor_id: Optional vendor ID
            item_id: Optional item ID
            
        Returns:
            Dictionary with detection results
        """
        # Build query to get items
        query = self.session.query(Item)
        
        # Apply filters
        if warehouse_id is not None:
            query = query.filter(Item.warehouse_id == warehouse_id)
            
        if vendor_id is not None:
            query = query.filter(Item.vendor_id == vendor_id)
            
        if item_id is not None:
            query = query.filter(Item.id == item_id)
        
        # Only include active items
        query = query.filter(Item.buyer_class.in_(['R', 'W']))
        
        items = query.all()
        
        results = {
            'total_items': len(items),
            'out_of_stock': 0,
            'low_stock': 0,
            'over_stock': 0,
            'approaching_shelf_life': 0,
            'errors': 0
        }
        
        # Process each item
        for item in items:
            try:
                # Check for out of stock
                if item.on_hand <= 0:
                    self._create_inventory_exception(item.id, 'OUT_OF_STOCK')
                    results['out_of_stock'] += 1
                    continue
                
                # Calculate available inventory in days
                daily_demand = item.demand_4weekly / 28  # Assuming 28 days in a 4-weekly period
                
                if daily_demand > 0:
                    inventory_days = item.on_hand / daily_demand
                    
                    # Check for low stock
                    if inventory_days < item.lead_time_forecast:
                        self._create_inventory_exception(item.id, 'LOW_STOCK')
                        results['low_stock'] += 1
                    
                    # Check for over stock
                    if inventory_days > (item.order_up_to_level_days * 1.5):
                        self._create_inventory_exception(item.id, 'OVER_STOCK')
                        results['over_stock'] += 1
                
                # Check for approaching shelf life
                if item.shelf_life_days > 0:
                    # Note: In a real implementation, we would need to track inventory age
                    # For this example, we'll skip this check
                    pass
                
            except Exception as e:
                logger.error(f"Error detecting inventory exceptions for item {item.id}: {str(e)}")
                results['errors'] += 1
        
        return results
    
    def _create_inventory_exception(
        self,
        item_id: int,
        exception_type: str
    ) -> int:
        """Create a management exception item for inventory exceptions.
        
        Args:
            item_id: Item ID
            exception_type: Exception type
            
        Returns:
            ID of the created management exception item
        """
        # Get the item
        item = self.session.query(Item).get(item_id)
        if not item:
            raise Exception(f"Item with ID {item_id} not found")
        
        # Get or create management exception
        exception = self.session.query(ManagementException).filter(
            ManagementException.warehouse_id == item.warehouse_id,
            ManagementException.exception_type == exception_type
        ).first()
        
        if not exception:
            # Create new exception
            exception_id = self.create_management_exception(
                warehouse_id=item.warehouse_id,
                exception_type=exception_type
            )
            exception = self.get_management_exception(exception_id)
        
        # Check if item is already in the exception and not resolved
        existing_item = self.session.query(ManagementExceptionItem).filter(
            ManagementExceptionItem.exception_id == exception.id,
            ManagementExceptionItem.item_id == item_id,
            ManagementExceptionItem.is_resolved == False
        ).first()
        
        if existing_item:
            return existing_item.id
            
        # Create management exception item
        if exception_type == 'OUT_OF_STOCK':
            notes = f"Item is out of stock. On hand: {item.on_hand}, On order: {item.on_order}"
        elif exception_type == 'LOW_STOCK':
            daily_demand = item.demand_4weekly / 28
            inventory_days = item.on_hand / daily_demand if daily_demand > 0 else 0
            notes = f"Low stock level. Inventory days: {inventory_days:.1f}, Lead time: {item.lead_time_forecast}"
        elif exception_type == 'OVER_STOCK':
            daily_demand = item.demand_4weekly / 28
            inventory_days = item.on_hand / daily_demand if daily_demand > 0 else 0
            notes = f"Over stock level. Inventory days: {inventory_days:.1f}, OUTL days: {item.order_up_to_level_days}"
        else:
            notes = f"Inventory exception: {exception_type}"
            
        # Add item to management exception
        return self.add_item_to_management_exception(
            exception_id=exception.id,
            item_id=item_id,
            value_x=item.on_hand,
            value_y=item.on_order,
            notes=notes
        )
    
    def detect_demand_pattern_exceptions(
        self,
        warehouse_id: Optional[str] = None,
        vendor_id: Optional[int] = None,
        item_id: Optional[int] = None
    ) -> Dict:
        """Detect demand pattern exceptions.
        
        Args:
            warehouse_id: Optional warehouse ID
            vendor_id: Optional vendor ID
            item_id: Optional item ID
            
        Returns:
            Dictionary with detection results
        """
        # Build query to get items
        query = self.session.query(Item)
        
        # Apply filters
        if warehouse_id is not None:
            query = query.filter(Item.warehouse_id == warehouse_id)
            
        if vendor_id is not None:
            query = query.filter(Item.vendor_id == vendor_id)
            
        if item_id is not None:
            query = query.filter(Item.id == item_id)
        
        # Only include active items
        query = query.filter(Item.buyer_class.in_(['R', 'W']))
        
        items = query.all()
        
        results = {
            'total_items': len(items),
            'lumpy_demand': 0,
            'high_madp': 0,
            'high_track': 0,
            'errors': 0
        }
        
        # Process each item
        for item in items:
            try:
                # Check for lumpy demand
                if item.madp >= self.company_settings.get('lumpy_demand_limit', 50.0):
                    self._create_demand_pattern_exception(item.id, 'LUMPY_DEMAND')
                    results['lumpy_demand'] += 1
                
                # Check for high MADP
                if item.madp >= 60.0:  # Example threshold
                    self._create_demand_pattern_exception(item.id, 'HIGH_MADP')
                    results['high_madp'] += 1
                
                # Check for high tracking signal
                if item.track >= self.company_settings.get('tracking_signal_limit', 55.0):
                    self._create_demand_pattern_exception(item.id, 'HIGH_TRACK')
                    results['high_track'] += 1
                
            except Exception as e:
                logger.error(f"Error detecting demand pattern exceptions for item {item.id}: {str(e)}")
                results['errors'] += 1
        
        return results
    
    def _create_demand_pattern_exception(
        self,
        item_id: int,
        exception_type: str
    ) -> int:
        """Create a management exception item for demand pattern exceptions.
        
        Args:
            item_id: Item ID
            exception_type: Exception type
            
        Returns:
            ID of the created management exception item
        """
        # Get the item
        item = self.session.query(Item).get(item_id)
        if not item:
            raise Exception(f"Item with ID {item_id} not found")
        
        # Get or create management exception
        exception = self.session.query(ManagementException).filter(
            ManagementException.warehouse_id == item.warehouse_id,
            ManagementException.exception_type == exception_type
        ).first()
        
        if not exception:
            # Create new exception
            exception_id = self.create_management_exception(
                warehouse_id=item.warehouse_id,
                exception_type=exception_type
            )
            exception = self.get_management_exception(exception_id)
        
        # Check if item is already in the exception and not resolved
        existing_item = self.session.query(ManagementExceptionItem).filter(
            ManagementExceptionItem.exception_id == exception.id,
            ManagementExceptionItem.item_id == item_id,
            ManagementExceptionItem.is_resolved == False
        ).first()
        
        if existing_item:
            return existing_item.id
            
        # Create management exception item
        if exception_type == 'LUMPY_DEMAND':
            notes = f"Lumpy demand pattern. MADP: {item.madp:.1f}%, Demand: {item.demand_4weekly:.2f}"
        elif exception_type == 'HIGH_MADP':
            notes = f"High MADP. Value: {item.madp:.1f}%, Threshold: 60.0%"
        elif exception_type == 'HIGH_TRACK':
            notes = f"High tracking signal. Value: {item.track:.1f}%, Threshold: {self.company_settings.get('tracking_signal_limit', 55.0)}%"
        else:
            notes = f"Demand pattern exception: {exception_type}"
            
        # Add item to management exception
        return self.add_item_to_management_exception(
            exception_id=exception.id,
            item_id=item_id,
            value_x=item.madp if exception_type in ['LUMPY_DEMAND', 'HIGH_MADP'] else item.track,
            value_y=item.demand_4weekly,
            notes=notes
        )