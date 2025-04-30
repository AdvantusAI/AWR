# Modified version of warehouse_replenishment/services/history_manager.py
# Fix for the duplicate key violation in demand_history table

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

from sqlalchemy import and_, func
from sqlalchemy.orm import Session

from warehouse_replenishment.models import (
    Item, DemandHistory, Company, SeasonalProfile, SeasonalProfileIndex, 
    ArchivedHistoryException
)
from warehouse_replenishment.core.demand_forecast import (
    calculate_lost_sales, adjust_history_value
)
from warehouse_replenishment.utils.date_utils import (
    get_current_period, get_previous_period, get_period_dates,
    get_period_for_date, is_period_end_day
)
from warehouse_replenishment.exceptions import ForecastError

from warehouse_replenishment.logging_setup import logger
logger = logging.getLogger(__name__)

class HistoryManager:
    """Service for managing demand history."""
    
    def __init__(self, session: Session):
        """Initialize the history manager.
        
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
                raise ForecastError("Company settings not found")
            
            self._company_settings = {
                'demand_from_days_out': company.demand_from_days_out,
                'history_periodicity_default': company.history_periodicity_default,
                'forecasting_periodicity_default': company.forecasting_periodicity_default,
                'keep_archived_exceptions_days': company.keep_archived_exceptions_days
            }
        
        return self._company_settings
    
    def create_history_period(
        self,
        item_id: int,
        period_number: int,
        period_year: int,
        shipped: float = 0.0,
        lost_sales: float = 0.0,
        promotional_demand: float = 0.0,
        out_of_stock_days: int = 0
    ) -> int:
        """Create a new history period for an item.
        
        Args:
            item_id: Item ID
            period_number: Period number
            period_year: Period year
            shipped: Shipped quantity
            lost_sales: Lost sales quantity
            promotional_demand: Promotional demand quantity
            out_of_stock_days: Number of out of stock days
            
        Returns:
            ID of the created history period
        """
        # Check if period already exists
        existing_period = self.session.query(DemandHistory).filter(
            DemandHistory.item_id == item_id,
            DemandHistory.period_number == period_number,
            DemandHistory.period_year == period_year
        ).first()
        
        if existing_period:
            raise ForecastError(f"History period already exists for item {item_id}, period {period_number}/{period_year}")
        
        # Calculate total demand
        total_demand = shipped + lost_sales - promotional_demand
        
        # Create new period - FIXED: Don't set the ID, let the database generate it
        history_period = DemandHistory(
            item_id=item_id,
            period_number=period_number,
            period_year=period_year,
            shipped=shipped,
            lost_sales=lost_sales,
            promotional_demand=promotional_demand,
            total_demand=total_demand,
            out_of_stock_days=out_of_stock_days,
            is_ignored=False,
            is_adjusted=False
        )
        
        self.session.add(history_period)
        
        try:
            self.session.commit()
            return history_period.id
        except Exception as e:
            self.session.rollback()
            raise ForecastError(f"Failed to create history period: {str(e)}")
    
    def process_daily_history_update(
        self,
        item_id: int,
        shipped: float,
        out_of_stock: bool = False
    ) -> bool:
        """Process daily history update for an item.
        
        Args:
            item_id: Item ID
            shipped: Shipped quantity
            out_of_stock: Whether the item is out of stock
            
        Returns:
            True if history was updated successfully
        """
        item = self.session.query(Item).get(item_id)
        if not item:
            raise ForecastError(f"Item with ID {item_id} not found")
        
        # Get periodicity
        periodicity = item.history_periodicity or self.company_settings['history_periodicity_default']
        
        # Get current period
        current_period, current_year = get_current_period(periodicity)
        
        # Get or create history period
        history_period = self.session.query(DemandHistory).filter(
            DemandHistory.item_id == item_id,
            DemandHistory.period_number == current_period,
            DemandHistory.period_year == current_year
        ).first()
        
        if not history_period:
            # Create new period - using the method that now has the fix
            self.create_history_period(
                item_id, current_period, current_year, 
                shipped=shipped, 
                out_of_stock_days=1 if out_of_stock else 0
            )
            return True
        
        # Update existing period
        new_shipped = history_period.shipped + shipped
        new_out_of_stock_days = history_period.out_of_stock_days + (1 if out_of_stock else 0)
        
        return self.update_history_period(
            item_id, current_period, current_year,
            shipped=new_shipped,
            out_of_stock_days=new_out_of_stock_days
        )
    
    # Rest of the class remains unchanged...
    # Include other methods from the original file
    
    def get_history_value_multiple(
        self,
        original_value: float,
        multiple: float
    ) -> float:
        """Get a history value adjusted by a multiple.
        
        Args:
            original_value: Original value
            multiple: Multiple to apply
            
        Returns:
            Adjusted value
        """
        return original_value * multiple
    
    def apply_history_multiple(
        self,
        item_id: int,
        multiple: float
    ) -> Dict:
        """Apply a multiple to all history values for an item.
        
        Args:
            item_id: Item ID
            multiple: Multiple to apply
            
        Returns:
            Dictionary with processing results
        """
        # Get all history periods for the item
        history_periods = self.session.query(DemandHistory).filter(
            DemandHistory.item_id == item_id
        ).all()
        
        results = {
            'total_periods': len(history_periods),
            'updated_periods': 0,
            'errors': 0
        }
        
        for period in history_periods:
            try:
                # Apply multiple to shipped, lost sales, and promotional demand
                new_shipped = period.shipped * multiple
                new_lost_sales = period.lost_sales * multiple
                new_promotional_demand = period.promotional_demand * multiple
                
                # Update period
                success = self.update_history_period(
                    item_id, period.period_number, period.period_year,
                    shipped=new_shipped,
                    lost_sales=new_lost_sales,
                    promotional_demand=new_promotional_demand
                )
                
                if success:
                    results['updated_periods'] += 1
                
            except Exception as e:
                logger.error(f"Error applying history multiple to period {period.period_number}/{period.period_year}: {str(e)}")
                results['errors'] += 1
        
        return results
    
    def copy_history_between_items(
        self,
        source_item_id: int,
        target_item_id: int,
        apply_multiple: float = 1.0,
        include_ignored: bool = False
    ) -> Dict:
        """Copy history from one item to another.
        
        Args:
            source_item_id: Source item ID
            target_item_id: Target item ID
            apply_multiple: Multiple to apply to history values
            include_ignored: Whether to include ignored periods
            
        Returns:
            Dictionary with processing results
        """
        # Check if source item exists
        source_item = self.session.query(Item).get(source_item_id)
        if not source_item:
            raise ForecastError(f"Source item with ID {source_item_id} not found")
        
        # Check if target item exists
        target_item = self.session.query(Item).get(target_item_id)
        if not target_item:
            raise ForecastError(f"Target item with ID {target_item_id} not found")
        
        # Get source history periods
        query = self.session.query(DemandHistory).filter(
            DemandHistory.item_id == source_item_id
        )
        
        if not include_ignored:
            query = query.filter(DemandHistory.is_ignored == False)
        
        source_periods = query.all()
        
        results = {
            'total_periods': len(source_periods),
            'copied_periods': 0,
            'updated_periods': 0,
            'errors': 0
        }
        
        for source_period in source_periods:
            try:
                # Check if target period already exists
                target_period = self.session.query(DemandHistory).filter(
                    DemandHistory.item_id == target_item_id,
                    DemandHistory.period_number == source_period.period_number,
                    DemandHistory.period_year == source_period.period_year
                ).first()
                
                # Apply multiple to values
                new_shipped = source_period.shipped * apply_multiple
                new_lost_sales = source_period.lost_sales * apply_multiple
                new_promotional_demand = source_period.promotional_demand * apply_multiple
                
                if target_period:
                    # Update existing period
                    success = self.update_history_period(
                        target_item_id, source_period.period_number, source_period.period_year,
                        shipped=new_shipped,
                        lost_sales=new_lost_sales,
                        promotional_demand=new_promotional_demand,
                        out_of_stock_days=source_period.out_of_stock_days,
                        is_ignored=source_period.is_ignored
                    )
                    
                    if success:
                        results['updated_periods'] += 1
                    
                else:
                    # Create new period
                    self.create_history_period(
                        target_item_id, source_period.period_number, source_period.period_year,
                        shipped=new_shipped,
                        lost_sales=new_lost_sales,
                        promotional_demand=new_promotional_demand,
                        out_of_stock_days=source_period.out_of_stock_days
                    )
                    
                    results['copied_periods'] += 1
            
            except Exception as e:
                logger.error(f"Error copying history period {source_period.period_number}/{source_period.period_year}: {str(e)}")
                results['errors'] += 1
        
        return results
    
    def purge_old_history(
        self,
        keep_periods: int = None,
        cutoff_date: date = None
    ) -> Dict:
        """Purge old history periods.
        
        Args:
            keep_periods: Number of periods to keep
            cutoff_date: Cutoff date for purging
            
        Returns:
            Dictionary with purge results
        """
        if keep_periods is None:
            keep_periods = 52  # Default to keeping 1 year (52 weeks)
        
        # Calculate cutoff period and year if not provided
        if cutoff_date is None:
            # Default to 1 year ago
            cutoff_date = date.today() - timedelta(days=365)
        
        # Get current period
        current_period, current_year = get_current_period(
            self.company_settings['history_periodicity_default']
        )
        
        results = {
            'total_periods': 0,
            'purged_periods': 0,
            'errors': 0
        }
        
        # Get count of all history periods
        total_periods = self.session.query(func.count(DemandHistory.id)).scalar()
        results['total_periods'] = total_periods or 0
        
        try:
            # Delete history periods older than cutoff date
            query = self.session.query(DemandHistory)
            
            # Delete based on period and year
            # This is approximate but works for most cases
            deleted = query.filter(
                (DemandHistory.period_year < cutoff_date.year) | 
                ((DemandHistory.period_year == cutoff_date.year) & 
                 (DemandHistory.period_number < cutoff_date.month))
            ).delete(synchronize_session=False)
            
            self.session.commit()
            
            results['purged_periods'] = deleted
            
        except Exception as e:
            self.session.rollback()
            logger.error(f"Error purging old history: {str(e)}")
            results['errors'] += 1
        
        return results
    
    def archive_resolved_exceptions(
        self,
        days_to_keep: int = None
    ) -> Dict:
        """Archive resolved history exceptions.
        
        Args:
            days_to_keep: Number of days to keep resolved exceptions
            
        Returns:
            Dictionary with archive results
        """
        if days_to_keep is None:
            days_to_keep = self.company_settings['keep_archived_exceptions_days']
        
        # Calculate cutoff date
        cutoff_date = datetime.now() - timedelta(days=days_to_keep)
        
        results = {
            'total_exceptions': 0,
            'archived_exceptions': 0,
            'errors': 0
        }
        
        # Get count of all resolved exceptions
        from warehouse_replenishment.models import HistoryException
        total_exceptions = self.session.query(func.count(HistoryException.id)).filter(
            HistoryException.is_resolved == True,
            HistoryException.resolution_date < cutoff_date
        ).scalar()
        
        results['total_exceptions'] = total_exceptions or 0
        
        try:
            # Get all resolved exceptions older than cutoff date
            exceptions = self.session.query(HistoryException).filter(
                HistoryException.is_resolved == True,
                HistoryException.resolution_date < cutoff_date
            ).all()
            
            # Archive each exception
            for exception in exceptions:
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
                    after_forecast=exception.forecast_value,  # Updated values would be here
                    after_madp=exception.madp,
                    after_track=exception.track,
                    resolution_action=exception.resolution_action,
                    resolution_notes=exception.resolution_notes
                )
                
                self.session.add(archived_exception)
                
                # Delete original exception
                self.session.delete(exception)
                
                results['archived_exceptions'] += 1
            
            self.session.commit()
            
        except Exception as e:
            self.session.rollback()
            logger.error(f"Error archiving resolved exceptions: {str(e)}")
            results['errors'] += 1
        
        return results