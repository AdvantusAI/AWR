# warehouse_replenishment/warehouse_replenishment/services/forecast_service.py
from datetime import date, datetime
from typing import List, Dict, Tuple, Optional, Union, Any
import logging

from sqlalchemy import and_, func
from sqlalchemy.orm import Session

from ..models import (
    Item, DemandHistory, Company, SeasonalProfile, 
    SeasonalProfileIndex, HistoryException
)
from ..core.demand_forecast import (
    calculate_initial_forecast, calculate_madp_from_history,
    calculate_track_from_history, calculate_regular_avs_forecast,
    calculate_enhanced_avs_forecast, apply_seasonality_to_forecast,
    calculate_composite_line, generate_seasonal_indices,
    detect_demand_spike, detect_tracking_signal_exception,
    adjust_history_value, filter_history, calculate_lost_sales,
    calculate_expected_zero_periods
)
from ..utils.date_utils import (
    get_current_period, get_previous_period, get_period_dates,
    get_period_for_date, is_period_end_day
)
from ..exceptions import ForecastError

logger = logging.getLogger(__name__)

class ForecastService:
    """Service for handling demand forecasting operations."""
    
    def __init__(self, session: Session):
        """Initialize the forecast service.
        
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
                'basic_alpha_factor': company.basic_alpha_factor,
                'demand_from_days_out': company.demand_from_days_out,
                'lumpy_demand_limit': company.lumpy_demand_limit, 
                'slow_mover_limit': company.slow_mover_limit,
                'demand_filter_high': company.demand_filter_high,
                'demand_filter_low': company.demand_filter_low,
                'tracking_signal_limit': company.tracking_signal_limit,
                'op_prime_limit_pct': company.op_prime_limit_pct,
                'forecast_demand_limit': company.forecast_demand_limit,
                'update_frequency_impact_control': company.update_frequency_impact_control,
                'history_periodicity_default': company.history_periodicity_default,
                'forecasting_periodicity_default': company.forecasting_periodicity_default
            }
        
        return self._company_settings
    
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
    
    def get_item_demand_history_by_year(
        self, 
        item_id: int,
        max_years: int = 4,
        include_ignored: bool = False
    ) -> Dict[int, List[float]]:
        """Get demand history for an item organized by year.
        
        Args:
            item_id: Item ID
            max_years: Maximum number of years to retrieve
            include_ignored: Whether to include ignored periods
            
        Returns:
            Dictionary mapping years to lists of demand values
        """
        item = self.session.query(Item).get(item_id)
        if not item:
            raise ForecastError(f"Item with ID {item_id} not found")
        
        # Get item periodicity
        periodicity = item.history_periodicity or self.company_settings['history_periodicity_default']
        
        # Get all history
        history = self.get_item_demand_history(item_id, include_ignored=include_ignored)
        
        # Organize by year
        history_by_year = {}
        
        for period in history:
            year = period['period_year']
            
            if year not in history_by_year:
                # Initialize with zeros
                history_by_year[year] = [0] * periodicity
            
            period_number = period['period_number']
            # Adjust to 0-based index
            if 1 <= period_number <= periodicity:
                history_by_year[year][period_number - 1] = period['total_demand']
        
        # Sort years and limit to max_years
        sorted_years = sorted(history_by_year.keys(), reverse=True)[:max_years]
        
        return {year: history_by_year[year] for year in sorted_years}
    
    def get_item_forecast_values(self, item_id: int) -> Dict:
        """Get forecast values for an item.
        
        Args:
            item_id: Item ID
            
        Returns:
            Dictionary with forecast values
        """
        item = self.session.query(Item).get(item_id)
        if not item:
            raise ForecastError(f"Item with ID {item_id} not found")
        
        return {
            'demand_weekly': item.demand_weekly,
            'demand_4weekly': item.demand_4weekly,
            'demand_monthly': item.demand_monthly,
            'demand_quarterly': item.demand_quarterly,
            'demand_yearly': item.demand_yearly,
            'madp': item.madp,
            'track': item.track,
            'freeze_until_date': item.freeze_until_date,
            'buyer_class': item.buyer_class.value if item.buyer_class else None,
            'system_class': item.system_class.value if item.system_class else None,
            'forecast_method': item.forecast_method.value if item.forecast_method else None,
            'forecast_date': item.forecast_date
        }
    
    def get_seasonal_profile(self, profile_id: str) -> List[float]:
        """Get seasonal profile indices.
        
        Args:
            profile_id: Profile ID
            
        Returns:
            List of seasonal indices
        """
        profile = self.session.query(SeasonalProfile).filter(
            SeasonalProfile.profile_id == profile_id
        ).first()
        
        if not profile:
            return []
        
        # Get indices ordered by period number
        indices = self.session.query(SeasonalProfileIndex).filter(
            SeasonalProfileIndex.profile_id == profile_id
        ).order_by(SeasonalProfileIndex.period_number).all()
        
        return [index.index_value for index in indices]
    
    def calculate_item_composite_line(
        self,
        item_id: int,
        max_years: int = 4,
        recent_weight: float = 0.5
    ) -> List[float]:
        """Calculate composite line for an item.
        
        Args:
            item_id: Item ID
            max_years: Maximum number of years to consider
            recent_weight: Weight for the most recent year
            
        Returns:
            List of composite line values
        """
        history_by_year = self.get_item_demand_history_by_year(
            item_id, max_years, include_ignored=False
        )
        
        return calculate_composite_line(history_by_year, max_years, recent_weight)
    
    def create_seasonal_profile(
        self,
        profile_id: str,
        description: str,
        periodicity: int,
        indices: List[float]
    ) -> bool:
        """Create a new seasonal profile.
        
        Args:
            profile_id: Profile ID
            description: Profile description
            periodicity: Profile periodicity
            indices: List of seasonal indices
            
        Returns:
            True if profile was created successfully
        """
        # Check if profile already exists
        existing_profile = self.session.query(SeasonalProfile).filter(
            SeasonalProfile.profile_id == profile_id
        ).first()
        
        if existing_profile:
            raise ForecastError(f"Profile with ID {profile_id} already exists")
        
        # Create new profile
        profile = SeasonalProfile(
            profile_id=profile_id,
            description=description,
            periodicity=periodicity
        )
        
        self.session.add(profile)
        
        # Create indices
        for i, index_value in enumerate(indices, 1):
            index = SeasonalProfileIndex(
                profile_id=profile_id,
                period_number=i,
                index_value=index_value
            )
            self.session.add(index)
        
        try:
            self.session.commit()
            return True
        except Exception as e:
            self.session.rollback()
            raise ForecastError(f"Failed to create seasonal profile: {str(e)}")
    
    def assign_profile_to_item(
        self,
        item_id: int,
        profile_id: str,
        update_forecast: bool = True
    ) -> bool:
        """Assign a seasonal profile to an item.
        
        Args:
            item_id: Item ID
            profile_id: Profile ID
            update_forecast: Whether to update the forecast immediately
            
        Returns:
            True if profile was assigned successfully
        """
        item = self.session.query(Item).get(item_id)
        if not item:
            raise ForecastError(f"Item with ID {item_id} not found")
        
        # Check if profile exists
        profile = self.session.query(SeasonalProfile).filter(
            SeasonalProfile.profile_id == profile_id
        ).first()
        
        if not profile:
            raise ForecastError(f"Profile with ID {profile_id} not found")
        
        # Update item
        item.demand_profile = profile_id
        
        try:
            self.session.commit()
            
            # Update forecast if requested
            if update_forecast:
                self.update_seasonal_forecast(item_id)
            
            return True
        except Exception as e:
            self.session.rollback()
            raise ForecastError(f"Failed to assign seasonal profile: {str(e)}")
    
    def update_seasonal_forecast(self, item_id: int) -> bool:
        """Update forecast based on seasonality.
        
        Args:
            item_id: Item ID
            
        Returns:
            True if forecast was updated successfully
        """
        item = self.session.query(Item).get(item_id)
        if not item:
            raise ForecastError(f"Item with ID {item_id} not found")
        
        # Get profile
        if not item.demand_profile:
            return False
        
        seasonal_indices = self.get_seasonal_profile(item.demand_profile)
        if not seasonal_indices:
            return False
        
        # Get current period
        periodicity = item.forecasting_periodicity or self.company_settings['forecasting_periodicity_default']
        current_period, _ = get_current_period(periodicity)
        
        # Apply seasonality to forecast
        base_forecast = item.demand_4weekly
        seasonal_forecast = apply_seasonality_to_forecast(
            base_forecast, seasonal_indices, current_period
        )
        
        # Update forecasts
        item.demand_4weekly = seasonal_forecast
        item.demand_weekly = seasonal_forecast / 4
        item.demand_monthly = seasonal_forecast * (365/12) / (365/13)
        item.demand_quarterly = seasonal_forecast * 3
        item.demand_yearly = seasonal_forecast * 13
        
        try:
            self.session.commit()
            return True
        except Exception as e:
            self.session.rollback()
            raise ForecastError(f"Failed to update seasonal forecast: {str(e)}")
    
    def initialize_item_forecast(
        self,
        item_id: int,
        initial_forecast: float = None
    ) -> bool:
        """Initialize forecast for a new item.
        
        Args:
            item_id: Item ID
            initial_forecast: Optional initial forecast value
            
        Returns:
            True if forecast was initialized successfully
        """
        item = self.session.query(Item).get(item_id)
        if not item:
            raise ForecastError(f"Item with ID {item_id} not found")
        
        if item.system_class != 'UNINITIALIZED':
            return False
        
        # Set initial forecast
        if initial_forecast is None:
            # Calculate from history if available
            history = self.get_item_demand_history(item_id)
            if history:
                history_values = [period['total_demand'] for period in history]
                initial_forecast = calculate_initial_forecast(history_values)
            else:
                initial_forecast = 0
        
        # Update forecasts
        item.demand_4weekly = initial_forecast
        item.demand_weekly = initial_forecast / 4
        item.demand_monthly = initial_forecast * (365/12) / (365/13)
        item.demand_quarterly = initial_forecast * 3
        item.demand_yearly = initial_forecast * 13
        
        # Update system class
        item.system_class = 'NEW'
        
        # Set forecast date
        item.forecast_date = datetime.now()
        
        try:
            self.session.commit()
            return True
        except Exception as e:
            self.session.rollback()
            raise ForecastError(f"Failed to initialize forecast: {str(e)}")
    
    def manually_update_forecast(
        self,
        item_id: int,
        new_forecast: float,
        freeze_until_date: date = None
    ) -> bool:
        """Manually update forecast for an item.
        
        Args:
            item_id: Item ID
            new_forecast: New forecast value
            freeze_until_date: Optional date until which to freeze the forecast
            
        Returns:
            True if forecast was updated successfully
        """
        item = self.session.query(Item).get(item_id)
        if not item:
            raise ForecastError(f"Item with ID {item_id} not found")
        
        # Update forecasts
        item.demand_4weekly = new_forecast
        item.demand_weekly = new_forecast / 4
        item.demand_monthly = new_forecast * (365/12) / (365/13)
        item.demand_quarterly = new_forecast * 3
        item.demand_yearly = new_forecast * 13
        
        # Set freeze until date if provided
        if freeze_until_date:
            item.freeze_until_date = freeze_until_date
        
        # Set forecast date
        item.forecast_date = datetime.now()
        
        try:
            self.session.commit()
            return True
        except Exception as e:
            self.session.rollback()
            raise ForecastError(f"Failed to update forecast: {str(e)}")
    
    def adjust_history(
        self,
        item_id: int,
        period_number: int,
        period_year: int,
        shipped: float = None,
        lost_sales: float = None,
        promotional_demand: float = None,
        ignore: bool = None
    ) -> bool:
        """Adjust history for an item.
        
        Args:
            item_id: Item ID
            period_number: Period number
            period_year: Period year
            shipped: New shipped value
            lost_sales: New lost sales value
            promotional_demand: New promotional demand value
            ignore: Whether to ignore the period
            
        Returns:
            True if history was adjusted successfully
        """
        history_record = self.session.query(DemandHistory).filter(
            DemandHistory.item_id == item_id,
            DemandHistory.period_number == period_number,
            DemandHistory.period_year == period_year
        ).first()
        
        if not history_record:
            raise ForecastError(f"History record not found for item {item_id}, period {period_number}/{period_year}")
        
        # Update values if provided
        if shipped is not None:
            history_record.shipped = shipped
        
        if lost_sales is not None:
            history_record.lost_sales = lost_sales
        
        if promotional_demand is not None:
            history_record.promotional_demand = promotional_demand
        
        if ignore is not None:
            history_record.is_ignored = ignore
        
        # Calculate new total demand
        if any(param is not None for param in [shipped, lost_sales, promotional_demand]):
            history_record.total_demand = (
                history_record.shipped + 
                history_record.lost_sales - 
                history_record.promotional_demand
            )
            history_record.is_adjusted = True
        
        try:
            self.session.commit()
            return True
        except Exception as e:
            self.session.rollback()
            raise ForecastError(f"Failed to adjust history: {str(e)}")
    
    def reforecast_item(self, item_id: int) -> bool:
        """Reforecast an item.
        
        Args:
            item_id: Item ID
            
        Returns:
            True if item was reforecasted successfully
        """
        item = self.session.query(Item).get(item_id)
        if not item:
            raise ForecastError(f"Item with ID {item_id} not found")
        
        # Check if forecast is frozen
        if item.freeze_until_date and item.freeze_until_date >= date.today():
            return False
        
        # Get history
        history = self.get_item_demand_history(item_id)
        if not history:
            return False
        
        # Get periodicity
        periodicity = item.forecasting_periodicity or self.company_settings['forecasting_periodicity_default']
        
        # Get latest demand
        latest_demand = history[0]['total_demand'] if history else 0
        
        # Get history values for MADP and Track calculation
        history_values = [period['total_demand'] for period in history]
        
        # Calculate MADP and Track
        current_forecast = item.demand_4weekly
        madp = calculate_madp_from_history(current_forecast, history_values)
        track = calculate_track_from_history(current_forecast, history_values)
        
        # Update MADP and Track
        item.madp = madp
        item.track = track
        
        # Determine forecast method
        forecast_method = item.forecast_method or 'E3_REGULAR_AVS'
        
        # Calculate new forecast
        if forecast_method == 'E3_ENHANCED_AVS':
            # Get Enhanced AVS specific parameters
            periods_with_zero_demand = getattr(item, 'periods_with_zero_demand', 0)
            expected_zero_periods = calculate_expected_zero_periods(current_forecast, madp)
            update_frequency_impact = self.company_settings['update_frequency_impact_control']
            forecast_demand_limit = getattr(
                item, 'forecasting_demand_limit', 
                self.company_settings['forecast_demand_limit']
            )
            
            new_forecast, was_forced = calculate_enhanced_avs_forecast(
                current_forecast,
                latest_demand,
                track,
                periods_with_zero_demand,
                expected_zero_periods,
                update_frequency_impact,
                forecast_demand_limit,
                self.company_settings['basic_alpha_factor']
            )
            
            # Update periods_with_zero_demand field
            if latest_demand == 0:
                item.periods_with_zero_demand = periods_with_zero_demand + 1
            else:
                item.periods_with_zero_demand = 0
                
        else:  # Default to Regular AVS
            new_forecast = calculate_regular_avs_forecast(
                current_forecast,
                latest_demand,
                track,
                self.company_settings['basic_alpha_factor']
            )
        
        # Apply seasonality if applicable
        if item.demand_profile:
            seasonal_indices = self.get_seasonal_profile(item.demand_profile)
            if seasonal_indices:
                current_period, _ = get_current_period(periodicity)
                new_forecast = apply_seasonality_to_forecast(
                    new_forecast,
                    seasonal_indices,
                    current_period
                )
        
        # Update forecasts
        item.demand_4weekly = new_forecast
        item.demand_weekly = new_forecast / 4
        item.demand_monthly = new_forecast * (365/12) / (365/13)
        item.demand_quarterly = new_forecast * 3
        item.demand_yearly = new_forecast * 13
        
        # Set forecast date
        item.forecast_date = datetime.now()
        
        # Update system class based on madp and annual forecast
        annual_forecast = item.demand_yearly
        
        if annual_forecast <= self.company_settings['slow_mover_limit']:
            item.system_class = 'SLOW'
        elif madp >= self.company_settings['lumpy_demand_limit']:
            item.system_class = 'LUMPY'
        else:
            item.system_class = 'REGULAR'
        
        try:
            self.session.commit()
            return True
        except Exception as e:
            self.session.rollback()
            raise ForecastError(f"Failed to reforecast item: {str(e)}")
    
    def process_period_end_reforecasting(
        self,
        warehouse_id: int = None,
        vendor_id: int = None,
        item_ids: List[int] = None
    ) -> Dict:
        """Process period-end reforecasting for all applicable items.
        
        Args:
            warehouse_id: Optional warehouse ID to filter items
            vendor_id: Optional vendor ID to filter items
            item_ids: Optional list of specific item IDs to process
            
        Returns:
            Dictionary with processing results
        """
        # Build query to get items
        query = self.session.query(Item)
        
        # Apply filters
        if warehouse_id:
            query = query.filter(Item.warehouse_id == warehouse_id)
        
        if vendor_id:
            query = query.filter(Item.vendor_id == vendor_id)
        
        if item_ids:
            query = query.filter(Item.id.in_(item_ids))
        
        # Only include active items (Regular or Watch)
        query = query.filter(Item.buyer_class.in_(['REGULAR', 'WATCH']))
        
        # Exclude items with frozen forecasts
        query = query.filter(
            (Item.freeze_until_date.is_(None)) | 
            (Item.freeze_until_date < func.current_date())
        )
        
        items = query.all()
        
        # Process results
        results = {
            'total_items': len(items),
            'processed': 0,
            'errors': 0,
            'error_items': []
        }
        
        # Process each item
        for item in items:
            try:
                # Call reforecast_item
                success = self.reforecast_item(item.id)
                
                if success:
                    results['processed'] += 1
            except Exception as e:
                logger.error(f"Error reforecasting item {item.id}: {str(e)}")
                results['errors'] += 1
                results['error_items'].append({
                    'item_id': item.id,
                    'error': str(e)
                })
        
        return results
    
    def detect_history_exceptions(
        self,
        warehouse_id: int = None,
        vendor_id: int = None,
        item_ids: List[int] = None
    ) -> Dict:
        """Detect history exceptions.
        
        Args:
            warehouse_id: Optional warehouse ID to filter items
            vendor_id: Optional vendor ID to filter items
            item_ids: Optional list of specific item IDs to process
            
        Returns:
            Dictionary with exception detection results
        """
        # Build query to get items
        query = self.session.query(Item)
        
        # Apply filters
        if warehouse_id:
            query = query.filter(Item.warehouse_id == warehouse_id)
        
        if vendor_id:
            query = query.filter(Item.vendor_id == vendor_id)
        
        if item_ids:
            query = query.filter(Item.id.in_(item_ids))
        
        # Only include active items (Regular or Watch)
        query = query.filter(Item.buyer_class.in_(['REGULAR', 'WATCH']))
        
        # Exclude items with frozen forecasts
        query = query.filter(
            (Item.freeze_until_date.is_(None)) | 
            (Item.freeze_until_date < func.current_date())
        )
        
        items = query.all()
        
        # Get latest period
        current_period, current_year = get_current_period(
            self.company_settings['forecasting_periodicity_default']
        )
        previous_period, previous_year = get_previous_period(
            current_period, current_year, 
            self.company_settings['forecasting_periodicity_default']
        )
        
        # Process results
        results = {
            'total_items': len(items),
            'demand_filter_high': 0,
            'demand_filter_low': 0,
            'tracking_signal_high': 0,
            'tracking_signal_low': 0,
            'service_level_check': 0,
            'infinity_check': 0,
            'errors': 0,
            'error_items': []
        }
        
        # Process each item
        for item in items:
            try:
                # Get latest history
                history = self.get_item_demand_history(item.id, periods=1)
                if not history:
                    continue
                
                latest_history = history[0]
                
                # Demand filter checks
                demand_exception = detect_demand_spike(
                    item.demand_4weekly,
                    latest_history['total_demand'],
                    item.madp,
                    self.company_settings['demand_filter_high'],
                    self.company_settings['demand_filter_low']
                )
                
                # Tracking signal checks
                tracking_exception = detect_tracking_signal_exception(
                    item.track,
                    self.company_settings['tracking_signal_limit']
                )
                
                # Create exceptions
                if demand_exception == 'HIGH':
                    self._create_history_exception(
                        item.id, 'DEMAND_FILTER_HIGH', 
                        latest_history['period_number'], 
                        latest_history['period_year'],
                        forecast_value=item.demand_4weekly,
                        actual_value=latest_history['total_demand'],
                        madp=item.madp,
                        track=item.track
                    )
                    results['demand_filter_high'] += 1
                    
                elif demand_exception == 'LOW':
                    self._create_history_exception(
                        item.id, 'DEMAND_FILTER_LOW', 
                        latest_history['period_number'], 
                        latest_history['period_year'],
                        forecast_value=item.demand_4weekly,
                        actual_value=latest_history['total_demand'],
                        madp=item.madp,
                        track=item.track
                    )
                    results['demand_filter_low'] += 1
                
                if tracking_exception == 'HIGH':
                    self._create_history_exception(
                        item.id, 'TRACKING_SIGNAL_HIGH', 
                        latest_history['period_number'], 
                        latest_history['period_year'],
                        forecast_value=item.demand_4weekly,
                        actual_value=latest_history['total_demand'],
                        madp=item.madp,
                        track=item.track
                    )
                    results['tracking_signal_high'] += 1
                    
                elif tracking_exception == 'LOW':
                    self._create_history_exception(
                        item.id, 'TRACKING_SIGNAL_LOW', 
                        latest_history['period_number'], 
                        latest_history['period_year'],
                        forecast_value=item.demand_4weekly,
                        actual_value=latest_history['total_demand'],
                        madp=item.madp,
                        track=item.track
                    )
                    results['tracking_signal_low'] += 1
                
                # Service level checks
                if item.service_level_attained < item.service_level_goal:
                    self._create_history_exception(
                        item.id, 'SERVICE_LEVEL_CHECK', 
                        latest_history['period_number'], 
                        latest_history['period_year'],
                        forecast_value=item.demand_4weekly,
                        actual_value=latest_history['total_demand'],
                        madp=item.madp,
                        track=item.track
                    )
                    results['service_level_check'] += 1
                
                # Infinity checks - for items with zero forecast but demand > 0
                if item.demand_4weekly == 0 and latest_history['total_demand'] > 0:
                    self._create_history_exception(
                        item.id, 'INFINITY_CHECK', 
                        latest_history['period_number'], 
                        latest_history['period_year'],
                        forecast_value=item.demand_4weekly,
                        actual_value=latest_history['total_demand'],
                        madp=item.madp,
                        track=item.track
                    )
                    results['infinity_check'] += 1
                
            except Exception as e:
                logger.error(f"Error detecting exceptions for item {item.id}: {str(e)}")
                results['errors'] += 1
                results['error_items'].append({
                    'item_id': item.id,
                    'error': str(e)
                })
        
        return results
    
    def _create_history_exception(
        self,
        item_id: int,
        exception_type: str,
        period_number: int,
        period_year: int,
        forecast_value: float = None,
        actual_value: float = None,
        madp: float = None,
        track: float = None,
        notes: str = None
    ):
        """Create a history exception record.
        
        Args:
            item_id: Item ID
            exception_type: Exception type
            period_number: Period number
            period_year: Period year
            forecast_value: Forecast value
            actual_value: Actual value
            madp: MADP value
            track: Track value
            notes: Optional notes
        """
        # Check if exception already exists
        existing_exception = self.session.query(HistoryException).filter(
            HistoryException.item_id == item_id,
            HistoryException.exception_type == exception_type,
            HistoryException.period_number == period_number,
            HistoryException.period_year == period_year
        ).first()
        
        if existing_exception:
            return
        
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
        except Exception as e:
            self.session.rollback()
            logger.error(f"Failed to create history exception: {str(e)}")
    
    def get_history_exceptions(
        self,
        warehouse_id: int = None,
        vendor_id: int = None,
        item_id: int = None,
        exception_type: str = None,
        resolved: bool = None
    ) -> List[Dict]:
        """Get history exceptions.
        
        Args:
            warehouse_id: Optional warehouse ID to filter exceptions
            vendor_id: Optional vendor ID to filter exceptions
            item_id: Optional item ID to filter exceptions
            exception_type: Optional exception type to filter
            resolved: Optional resolved status to filter
            
        Returns:
            List of exception dictionaries
        """
        query = self.session.query(HistoryException)
        
        # Join with Item to filter by warehouse and vendor
        if warehouse_id or vendor_id:
            query = query.join(Item, HistoryException.item_id == Item.id)
            
            if warehouse_id:
                query = query.filter(Item.warehouse_id == warehouse_id)
                
            if vendor_id:
                query = query.filter(Item.vendor_id == vendor_id)
        
        # Filter by item ID
        if item_id:
            query = query.filter(HistoryException.item_id == item_id)
        
        # Filter by exception type
        if exception_type:
            query = query.filter(HistoryException.exception_type == exception_type)
        
        # Filter by resolved status
        if resolved is not None:
            query = query.filter(HistoryException.is_resolved == resolved)
        
        # Order by creation date (most recent first)
        query = query.order_by(HistoryException.creation_date.desc())
        
        exceptions = query.all()
        
        # Convert to dictionaries
        result = []
        for exception in exceptions:
            result.append({
                'id': exception.id,
                'item_id': exception.item_id,
                'exception_type': exception.exception_type,
                'creation_date': exception.creation_date,
                'period_number': exception.period_number,
                'period_year': exception.period_year,
                'forecast_value': exception.forecast_value,
                'actual_value': exception.actual_value,
                'madp': exception.madp,
                'track': exception.track,
                'notes': exception.notes,
                'is_resolved': exception.is_resolved,
                'resolution_date': exception.resolution_date,
                'resolution_action': exception.resolution_action,
                'resolution_notes': exception.resolution_notes
            })
        
        return result
    
    def resolve_history_exception(
        self,
        exception_id: int,
        resolution_action: str,
        resolution_notes: str = None
    ) -> bool:
        """Resolve a history exception.
        
        Args:
            exception_id: Exception ID
            resolution_action: Resolution action
            resolution_notes: Optional resolution notes
            
        Returns:
            True if exception was resolved successfully
        """
        exception = self.session.query(HistoryException).get(exception_id)
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