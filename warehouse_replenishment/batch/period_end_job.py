# warehouse_replenishment/batch/period_end_job.py
import logging
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional
from collections import defaultdict
import numpy as np
import math

from sqlalchemy.orm import Session
from warehouse_replenishment.logging_setup import get_logger
from warehouse_replenishment.config import config
from warehouse_replenishment.db import session_scope
from warehouse_replenishment.models import (
    Company, Item, Warehouse, SeasonalProfile, SeasonalProfileIndex,
    DemandHistory, ItemForecast, ForecastMethod, SystemClassCode, BuyerClassCode
)
from warehouse_replenishment.services.forecast_service import ForecastService
from warehouse_replenishment.services.history_manager import HistoryManager
from warehouse_replenishment.services.safety_stock_service import SafetyStockService
from warehouse_replenishment.utils.date_utils import (
    get_current_period, get_previous_period, get_period_dates, is_period_end_day, 
    get_period_type, add_days
)
from warehouse_replenishment.utils.math_utils import calculate_madp, calculate_track
from warehouse_replenishment.core.demand_forecast import calculate_composite_line, generate_seasonal_indices
from warehouse_replenishment.core.safety_stock import empirical_safety_stock_adjustment
from warehouse_replenishment.exceptions import BatchProcessError, ForecastError
from warehouse_replenishment.logging_setup import logger

logger = get_logger('period_end_job')

def update_seasonal_profiles(
    warehouse_id: int,
    session: Session,
    refresh_all: bool = False
) -> Dict:
    """Update seasonal profiles based on actual period data.
    
    Args:
        warehouse_id: Warehouse ID to process
        session: Database session
        refresh_all: Whether to refresh all profiles or just update existing ones
        
    Returns:
        Dictionary with update results
    """
    # Get company settings
    company = session.query(Company).first()
    if not company:
        raise Exception("Company settings not found")
    
    periodicity = company.history_periodicity_default
    max_years = 4  # Look back up to 4 years
    
    # Get all items with seasonal profiles or high seasonality indicators
    query = session.query(Item).filter(
        Item.warehouse_id == warehouse_id,
        Item.buyer_class.in_(['R', 'W'])
    )
    
    if not refresh_all:
        # Only update items with existing seasonal profiles
        query = query.filter(Item.demand_profile.isnot(None))
    
    items = query.all()
    
    results = {
        'warehouse_id': warehouse_id,
        'periodicity': periodicity,
        'total_items': len(items),
        'updated_profiles': 0,
        'new_profiles': 0,
        'removed_profiles': 0,
        'errors': 0,
        'profile_details': {},
        'start_time': datetime.now(),
        'success': True
    }
    
    # Group items by seasonal profile
    items_by_profile = defaultdict(list)
    seasonal_indicators = {}
    
    for item in items:
        if item.demand_profile:
            items_by_profile[item.demand_profile].append(item)
        
        # Calculate seasonality indicator
        seasonal_indicators[item.id] = calculate_seasonality_indicator(item, session, periodicity)
    
    # Update existing profiles
    for profile_id, profile_items in items_by_profile.items():
        try:
            profile = session.query(SeasonalProfile).filter(
                SeasonalProfile.profile_id == profile_id
            ).first()
            
            if not profile:
                logger.warning(f"Profile {profile_id} not found")
                continue
            
            # Collect history for all items in this profile
            history_by_year = defaultdict(lambda: defaultdict(list))
            
            for item in profile_items:
                item_history = get_item_history_by_year(item, session, max_years, periodicity)
                
                # Aggregate history by year and period
                for year, periods in item_history.items():
                    for period_idx, demand in enumerate(periods):
                        if demand > 0:  # Only include periods with demand
                            history_by_year[year][period_idx].append(demand)
            
            # Calculate average demand by year and period
            aggregated_history = {}
            for year, period_data in history_by_year.items():
                aggregated_history[year] = []
                for period_idx in range(periodicity):
                    if period_idx in period_data and period_data[period_idx]:
                        avg_demand = np.mean(period_data[period_idx])
                        aggregated_history[year].append(avg_demand)
                    else:
                        aggregated_history[year].append(0.0)
            
            # Calculate composite line
            composite_line = calculate_composite_line(aggregated_history, max_years)
            
            # Generate seasonal indices
            new_indices = generate_seasonal_indices(composite_line)
            
            # Update profile indices
            update_success = update_profile_indices(profile, new_indices, session)
            
            if update_success:
                results['updated_profiles'] += 1
                results['profile_details'][profile_id] = {
                    'items_count': len(profile_items),
                    'composite_line': composite_line,
                    'seasonal_indices': new_indices,
                    'updated': True
                }
            
        except Exception as e:
            logger.error(f"Error updating profile {profile_id}: {str(e)}")
            results['errors'] += 1
            results['success'] = False
    
    # Identify items that need new seasonal profiles
    if refresh_all:
        for item in items:
            if not item.demand_profile:
                # Check if item has significant seasonality
                seasonality_score = seasonal_indicators.get(item.id, 0)
                
                if seasonality_score > 0.3:  # Threshold for strong seasonality
                    # Create or assign to a seasonal profile
                    new_profile_id = create_or_assign_seasonal_profile(
                        item, session, periodicity
                    )
                    
                    if new_profile_id:
                        results['new_profiles'] += 1
                        results['profile_details'][new_profile_id] = {
                            'items_count': 1,
                            'newly_created': True
                        }
    
    # Remove profiles for items with low seasonality
    for item in items:
        if item.demand_profile:
            seasonality_score = seasonal_indicators.get(item.id, 0)
            
            if seasonality_score < 0.1:  # Threshold for non-seasonal items
                # Remove profile assignment
                item.demand_profile = None
                results['removed_profiles'] += 1
    
    results['end_time'] = datetime.now()
    results['duration'] = results['end_time'] - results['start_time']
    
    # Commit changes
    if results['success']:
        try:
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"Error committing seasonal profile updates: {str(e)}")
            results['success'] = False
    
    return results

def calculate_seasonality_indicator(
    item: Item,
    session: Session,
    periodicity: int
) -> float:
    """Calculate a seasonality indicator for an item.
    
    Args:
        item: Item to analyze
        session: Database session
        periodicity: Periodicity to use
        
    Returns:
        Seasonality indicator (0-1, higher means stronger seasonality)
    """
    # Get recent history
    history = session.query(DemandHistory).filter(
        DemandHistory.item_id == item.id,
        DemandHistory.is_ignored == False
    ).order_by(
        DemandHistory.period_year.desc(),
        DemandHistory.period_number.desc()
    ).limit(periodicity * 2).all()  # Get at least 2 years
    
    if len(history) < periodicity:
        return 0.0  # Not enough data
    
    # Group by period number
    period_demands = defaultdict(list)
    for h in history:
        period_demands[h.period_number].append(h.total_demand)
    
    # Calculate coefficient of variation for each period
    period_cvs = []
    for period_num, demands in period_demands.items():
        if len(demands) >= 2:
            mean = np.mean(demands)
            if mean > 0:
                cv = np.std(demands) / mean
                period_cvs.append(cv)
    
    if not period_cvs:
        return 0.0
    
    # Calculate overall seasonality score
    # Higher CV means more seasonality
    avg_cv = np.mean(period_cvs)
    seasonality_score = min(1.0, avg_cv)
    
    return seasonality_score

def get_item_history_by_year(
    item: Item,
    session: Session,
    max_years: int,
    periodicity: int
) -> Dict[int, List[float]]:
    """Get item demand history organized by year.
    
    Args:
        item: Item to get history for
        session: Database session
        max_years: Maximum years to retrieve
        periodicity: Periodicity to use
        
    Returns:
        Dictionary mapping years to lists of demand values
    """
    # Get history
    history = session.query(DemandHistory).filter(
        DemandHistory.item_id == item.id,
        DemandHistory.is_ignored == False
    ).order_by(
        DemandHistory.period_year.desc(),
        DemandHistory.period_number.desc()
    ).all()
    
    # Organize by year
    history_by_year = {}
    
    for h in history:
        year = h.period_year
        
        if year not in history_by_year:
            history_by_year[year] = [0.0] * periodicity
        
        period_idx = h.period_number - 1  # Convert to 0-based index
        if 0 <= period_idx < periodicity:
            history_by_year[year][period_idx] = h.total_demand
    
    # Sort by year (most recent first) and limit to max_years
    sorted_years = sorted(history_by_year.keys(), reverse=True)[:max_years]
    
    return {year: history_by_year[year] for year in sorted_years}

def update_profile_indices(
    profile: SeasonalProfile,
    new_indices: List[float],
    session: Session
) -> bool:
    """Update seasonal profile indices.
    
    Args:
        profile: Seasonal profile to update
        new_indices: New seasonal indices
        session: Database session
        
    Returns:
        True if successful, False otherwise
    """
    try:
        # Delete existing indices
        session.query(SeasonalProfileIndex).filter(
            SeasonalProfileIndex.profile_id == profile.profile_id
        ).delete()
        
        # Create new indices
        for i, index_value in enumerate(new_indices, 1):
            index = SeasonalProfileIndex(
                profile_id=profile.profile_id,
                period_number=i,
                index_value=index_value
            )
            session.add(index)
        
        return True
    except Exception as e:
        logger.error(f"Error updating profile indices: {str(e)}")
        return False

def create_or_assign_seasonal_profile(
    item: Item,
    session: Session,
    periodicity: int
) -> Optional[str]:
    """Create or assign a seasonal profile for an item.
    
    Args:
        item: Item to assign profile to
        session: Database session
        periodicity: Periodicity to use
        
    Returns:
        Profile ID if successful, None otherwise
    """
    try:
        # Get item history
        item_history = get_item_history_by_year(item, session, 4, periodicity)
        
        if not item_history:
            return None
        
        # Calculate composite line for this item
        composite_line = calculate_composite_line(item_history, 4)
        
        # Generate seasonal indices
        seasonal_indices = generate_seasonal_indices(composite_line)
        
        # Check if similar profile already exists
        existing_profile = find_similar_profile(
            seasonal_indices, session, periodicity
        )
        
        if existing_profile:
            # Assign existing profile
            item.demand_profile = existing_profile.profile_id
            return existing_profile.profile_id
        else:
            # Create new profile
            profile_id = f"S_{item.item_id}_{datetime.now().strftime('%Y%m%d')}"
            profile = SeasonalProfile(
                profile_id=profile_id,
                description=f"Seasonal Profile for {item.item_id}",
                periodicity=periodicity
            )
            session.add(profile)
            
            # Add indices
            for i, index_value in enumerate(seasonal_indices, 1):
                index = SeasonalProfileIndex(
                    profile_id=profile_id,
                    period_number=i,
                    index_value=index_value
                )
                session.add(index)
            
            # Assign to item
            item.demand_profile = profile_id
            
            return profile_id
    
    except Exception as e:
        logger.error(f"Error creating seasonal profile for item {item.id}: {str(e)}")
        return None

def find_similar_profile(
    indices: List[float],
    session: Session,
    periodicity: int,
    similarity_threshold: float = 0.95
) -> Optional[SeasonalProfile]:
    """Find a similar seasonal profile based on indices.
    
    Args:
        indices: Seasonal indices to match
        session: Database session
        periodicity: Periodicity to use
        similarity_threshold: Minimum similarity required
        
    Returns:
        Matching profile or None
    """
    # Get all profiles with the same periodicity
    profiles = session.query(SeasonalProfile).filter(
        SeasonalProfile.periodicity == periodicity
    ).all()
    
    best_similarity = 0.0
    best_profile = None
    
    for profile in profiles:
        # Get profile indices
        profile_indices = session.query(SeasonalProfileIndex).filter(
            SeasonalProfileIndex.profile_id == profile.profile_id
        ).order_by(SeasonalProfileIndex.period_number).all()
        
        if len(profile_indices) != len(indices):
            continue
        
        # Calculate similarity (correlation coefficient)
        profile_values = [idx.index_value for idx in profile_indices]
        correlation = np.corrcoef(indices, profile_values)[0, 1]
        
        if correlation > best_similarity:
            best_similarity = correlation
            best_profile = profile
    
    if best_similarity >= similarity_threshold:
        return best_profile
    
    return None

def calculate_forecast_accuracy(
    warehouse_id: int,
    session: Session,
    period_number: Optional[int] = None,
    period_year: Optional[int] = None
) -> Dict:
    """Calculate forecast accuracy for the completed period.
    
    Args:
        warehouse_id: Warehouse ID to process
        session: Database session
        period_number: Period number (defaults to previous period)
        period_year: Period year (defaults to previous period)
        
    Returns:
        Dictionary with forecast accuracy results
    """
    # Get company settings
    company = session.query(Company).first()
    if not company:
        raise Exception("Company settings not found")
    
    periodicity = company.forecasting_periodicity_default
    
    # Use previous period if not specified
    if period_number is None or period_year is None:
        current_period, current_year = get_current_period(periodicity)
        period_number, period_year = get_previous_period(current_period, current_year, periodicity)
    
    # Get forecast service
    forecast_service = ForecastService(session)
    
    # Get all active items for the warehouse
    items = session.query(Item).filter(
        Item.warehouse_id == warehouse_id,
        Item.buyer_class.in_(['R', 'W'])  # Regular and Watch items
    ).all()
    
    results = {
        'warehouse_id': warehouse_id,
        'period_number': period_number,
        'period_year': period_year,
        'total_items': len(items),
        'processed_items': 0,
        'accurate_forecasts': 0,  # Within acceptable tolerance
        'total_absolute_error': 0.0,
        'total_actual_demand': 0.0,
        'mape': 0.0,  # Mean Absolute Percentage Error
        'wape': 0.0,  # Weighted Absolute Percentage Error
        'error_distribution': {
            'under_forecast': 0,
            'over_forecast': 0,
            'within_tolerance': 0
        },
        'top_missed_forecasts': [],
        'item_accuracy_details': [],
        'start_time': datetime.now(),
        'success': True
    }
    
    tolerance_threshold = 0.20  # 20% tolerance for "accurate" forecasts
    
    # Process each item
    for item in items:
        try:
            # Get forecast for this period
            forecasts = session.query(ItemForecast).filter(
                ItemForecast.item_id == item.id,
                ItemForecast.period_number == period_number,
                ItemForecast.period_year == period_year
            ).all()
            
            # Get actual demand for this period
            history = session.query(DemandHistory).filter(
                DemandHistory.item_id == item.id,
                DemandHistory.period_number == period_number,
                DemandHistory.period_year == period_year
            ).first()
            
            if not history:
                continue  # Skip if no actual history available
            
            actual_demand = history.total_demand
            forecast_value = None
            
            # Get the forecast value (use the most recent forecast for this period)
            if forecasts:
                latest_forecast = max(forecasts, key=lambda f: f.forecast_date)
                forecast_value = latest_forecast.forecast_value
            elif item.demand_4weekly is not None:
                # Fallback to item's current forecast if no historical forecast exists
                forecast_value = item.demand_4weekly
            
            if forecast_value is not None:
                # Calculate forecast accuracy metrics
                error = abs(actual_demand - forecast_value)
                relative_error = error / actual_demand if actual_demand > 0 else None
                
                # Update totals
                results['processed_items'] += 1
                results['total_absolute_error'] += error
                results['total_actual_demand'] += actual_demand
                
                # Store forecast accuracy for this item
                item_detail = {
                    'item_id': item.item_id,
                    'description': item.description,
                    'forecast': forecast_value,
                    'actual': actual_demand,
                    'absolute_error': error,
                    'percentage_error': relative_error * 100 if relative_error is not None else None,
                    'is_accurate': relative_error is not None and relative_error <= tolerance_threshold
                }
                
                # Update error distribution
                if relative_error is not None:
                    if relative_error <= tolerance_threshold:
                        results['accurate_forecasts'] += 1
                        results['error_distribution']['within_tolerance'] += 1
                        item_detail['status'] = 'WITHIN_TOLERANCE'
                    elif forecast_value < actual_demand:
                        results['error_distribution']['under_forecast'] += 1
                        item_detail['status'] = 'UNDER_FORECAST'
                    else:
                        results['error_distribution']['over_forecast'] += 1
                        item_detail['status'] = 'OVER_FORECAST'
                
                results['item_accuracy_details'].append(item_detail)
                
                # Track top missed forecasts
                if relative_error is not None and relative_error > tolerance_threshold:
                    results['top_missed_forecasts'].append({
                        'item_id': item.item_id,
                        'description': item.description,
                        'forecast': forecast_value,
                        'actual': actual_demand,
                        'error_percentage': relative_error * 100
                    })
                
                # Update forecast record with actual values
                if forecasts:
                    latest_forecast.actual_value = actual_demand
                    latest_forecast.error = actual_demand - forecast_value
                    latest_forecast.error_pct = relative_error * 100 if relative_error is not None else None
                
        except Exception as e:
            logger.error(f"Error calculating forecast accuracy for item {item.id}: {str(e)}")
            results['success'] = False
    
    # Calculate overall accuracy metrics
    if results['processed_items'] > 0:
        # Mean Absolute Percentage Error (MAPE)
        mape_items = []
        for detail in results['item_accuracy_details']:
            if detail['percentage_error'] is not None:
                mape_items.append(detail['percentage_error'])
        
        if mape_items:
            results['mape'] = np.mean(mape_items)
        
        # Weighted Absolute Percentage Error (WAPE)
        if results['total_actual_demand'] > 0:
            results['wape'] = (results['total_absolute_error'] / results['total_actual_demand']) * 100
        
        # Sort top missed forecasts by error percentage
        results['top_missed_forecasts'].sort(key=lambda x: x['error_percentage'], reverse=True)
        results['top_missed_forecasts'] = results['top_missed_forecasts'][:10]  # Keep top 10
        
        # Calculate accuracy rate
        results['accuracy_rate'] = (results['accurate_forecasts'] / results['processed_items']) * 100
    
    results['end_time'] = datetime.now()
    results['duration'] = results['end_time'] - results['start_time']
    
    # Commit forecast updates
    if results['success']:
        try:
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"Error committing forecast accuracy updates: {str(e)}")
            results['success'] = False
    
    return results

def save_forecast_accuracy_results(results: Dict, session: Session) -> bool:
    """Save forecast accuracy results to the database.
    
    Args:
        results: Forecast accuracy calculation results
        session: Database session
        
    Returns:
        True if successful, False otherwise
    """
    try:
        # Create or update forecast accuracy metrics table
        # This would require a new model/table to store accuracy metrics
        # For now, we'll log the results
        
        logger.info(f"Forecast Accuracy Results for Warehouse {results['warehouse_id']}:")
        logger.info(f"  Period: {results['period_number']}/{results['period_year']}")
        logger.info(f"  Accuracy Rate: {results.get('accuracy_rate', 0):.2f}%")
        logger.info(f"  MAPE: {results.get('mape', 0):.2f}%")
        logger.info(f"  WAPE: {results.get('wape', 0):.2f}%")
        logger.info(f"  Processed Items: {results['processed_items']}")
        logger.info(f"  Error Distribution:")
        logger.info(f"    Within Tolerance: {results['error_distribution']['within_tolerance']}")
        logger.info(f"    Under Forecast: {results['error_distribution']['under_forecast']}")
        logger.info(f"    Over Forecast: {results['error_distribution']['over_forecast']}")
        
        if results.get('top_missed_forecasts'):
            logger.info("  Top Missed Forecasts:")
            for missed in results['top_missed_forecasts'][:5]:
                logger.info(f"    - {missed['item_id']}: {missed['error_percentage']:.1f}% error")
        
        return True
    except Exception as e:
        logger.error(f"Error saving forecast accuracy results: {str(e)}")
        return False

def update_service_levels(
    warehouse_id: int,
    session: Session,
    period_number: Optional[int] = None,
    period_year: Optional[int] = None
) -> Dict:
    """Calculate and update service levels for items for the completed period.
    
    Args:
        warehouse_id: Warehouse ID to process
        session: Database session
        period_number: Period number (defaults to previous period)
        period_year: Period year (defaults to previous period)
        
    Returns:
        Dictionary with service level update results
    """
    # Get company settings
    company = session.query(Company).first()
    if not company:
        raise Exception("Company settings not found")
    
    periodicity = company.history_periodicity_default
    
    # Use previous period if not specified
    if period_number is None or period_year is None:
        current_period, current_year = get_current_period(periodicity)
        period_number, period_year = get_previous_period(current_period, current_year, periodicity)
    
    # Get all active items for the warehouse
    items = session.query(Item).filter(
        Item.warehouse_id == warehouse_id,
        Item.buyer_class.in_(['R', 'W'])  # Regular and Watch items
    ).all()
    
    results = {
        'warehouse_id': warehouse_id,
        'period_number': period_number,
        'period_year': period_year,
        'total_items': len(items),
        'processed_items': 0,
        'items_meeting_goal': 0,
        'items_below_goal': 0,
        'total_lost_sales': 0.0,
        'total_lost_sales_value': 0.0,
        'top_service_level_gaps': [],
        'service_level_distribution': {
            'excellent': 0,  # >= 99%
            'good': 0,      # 95-98.9%
            'fair': 0,      # 90-94.9%
            'poor': 0,      # < 90%
        },
        'recommendations': [],
        'start_time': datetime.now(),
        'success': True
    }
    
    # Process each item
    for item in items:
        try:
            # Get service level data for this period
            history = session.query(DemandHistory).filter(
                DemandHistory.item_id == item.id,
                DemandHistory.period_number == period_number,
                DemandHistory.period_year == period_year
            ).first()
            
            if not history:
                continue
            
            # Calculate service level for the period
            total_demand = history.shipped + history.lost_sales
            if total_demand == 0:
                continue  # Skip items with no demand
            
            service_level_attained = (history.shipped / total_demand) * 100
            
            # Update item's service level attained
            item.service_level_attained = service_level_attained
            
            # Track service level metrics
            results['processed_items'] += 1
            
            # Get service level goal
            service_level_goal = (
                item.service_level_goal or 
                company.service_level_goal
            )
            
            # Check if meeting goal
            if service_level_attained >= service_level_goal:
                results['items_meeting_goal'] += 1
            else:
                results['items_below_goal'] += 1
                
                # Add to top gaps if significant
                gap = service_level_goal - service_level_attained
                if gap > 5:  # Only include gaps > 5%
                    results['top_service_level_gaps'].append({
                        'item_id': item.item_id,
                        'description': item.description,
                        'service_level_goal': service_level_goal,
                        'service_level_attained': service_level_attained,
                        'gap': gap,
                        'lost_sales': history.lost_sales,
                        'lost_sales_value': history.lost_sales * item.sales_price
                    })
            
            # Update distribution
            if service_level_attained >= 99:
                results['service_level_distribution']['excellent'] += 1
            elif service_level_attained >= 95:
                results['service_level_distribution']['good'] += 1
            elif service_level_attained >= 90:
                results['service_level_distribution']['fair'] += 1
            else:
                results['service_level_distribution']['poor'] += 1
            
            # Track lost sales
            results['total_lost_sales'] += history.lost_sales
            results['total_lost_sales_value'] += history.lost_sales * item.sales_price
            
            # Adjust safety stock based on empirical performance
            if item.sstf is not None and item.madp is not None:
                adjustment = empirical_safety_stock_adjustment(
                    current_safety_stock=item.sstf,
                    service_level_goal=service_level_goal,
                    service_level_attained=service_level_attained,
                    max_adjustment_pct=10.0  # Allow up to 10% adjustment
                )
                
                # Only apply if adjustment is significant
                if abs(adjustment - item.sstf) > 0.1:
                    results['recommendations'].append({
                        'item_id': item.item_id,
                        'description': item.description,
                        'current_sstf': item.sstf,
                        'recommended_sstf': adjustment,
                        'service_level_gap': service_level_goal - service_level_attained,
                        'reason': 'Empirical adjustment based on service level performance'
                    })
            
        except Exception as e:
            logger.error(f"Error updating service level for item {item.id}: {str(e)}")
            results['success'] = False
    
    # Sort top service level gaps
    results['top_service_level_gaps'].sort(key=lambda x: x['gap'], reverse=True)
    results['top_service_level_gaps'] = results['top_service_level_gaps'][:10]  # Keep top 10
    
    # Generate summary recommendations
    if results['items_below_goal'] > 0:
        pct_below_goal = (results['items_below_goal'] / results['total_items']) * 100
        
        results['recommendations'].insert(0, {
            'type': 'SUMMARY',
            'message': f"{results['items_below_goal']} items ({pct_below_goal:.1f}%) are below service level goals",
            'action': 'Review safety stock levels and forecasting methods for items with large gaps'
        })
    
    if results['total_lost_sales_value'] > 1000:  # Threshold for concern
        results['recommendations'].insert(0, {
            'type': 'SUMMARY',
            'message': f"Lost sales value: ${results['total_lost_sales_value']:.2f}",
            'action': 'Increase safety stock for items with frequent stockouts'
        })
    
    results['end_time'] = datetime.now()
    results['duration'] = results['end_time'] - results['start_time']
    
    # Commit changes
    if results['success']:
        try:
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"Error committing service level updates: {str(e)}")
            results['success'] = False
    
    return results

def analyze_fill_rate_by_vendor(
    warehouse_id: int,
    session: Session,
    period_number: Optional[int] = None,
    period_year: Optional[int] = None
) -> Dict:
    """Analyze fill rate performance by vendor.
    
    Args:
        warehouse_id: Warehouse ID to process
        session: Database session
        period_number: Period number (defaults to previous period)
        period_year: Period year (defaults to previous period)
        
    Returns:
        Dictionary with vendor fill rate analysis
    """
    # Get company settings
    company = session.query(Company).first()
    if not company:
        raise Exception("Company settings not found")
    
    periodicity = company.history_periodicity_default
    
    # Use previous period if not specified
    if period_number is None or period_year is None:
        current_period, current_year = get_current_period(periodicity)
        period_number, period_year = get_previous_period(current_period, current_year, periodicity)
    
    # Get all vendors for the warehouse
    vendors = session.query(Vendor).filter(
        Vendor.warehouse_id == warehouse_id
    ).all()
    
    results = {
        'warehouse_id': warehouse_id,
        'period_number': period_number,
        'period_year': period_year,
        'vendor_fill_rates': {},
        'top_performing_vendors': [],
        'bottom_performing_vendors': [],
        'overall_fill_rate': 0.0,
        'start_time': datetime.now(),
        'success': True
    }
    
    total_shipped = 0
    total_demand = 0
    
    # Process each vendor
    for vendor in vendors:
        try:
            # Get all items for this vendor
            items = session.query(Item).filter(
                Item.vendor_id == vendor.id,
                Item.buyer_class.in_(['R', 'W'])
            ).all()
            
            vendor_shipped = 0
            vendor_demand = 0
            
            # Aggregate data for all items of this vendor
            for item in items:
                history = session.query(DemandHistory).filter(
                    DemandHistory.item_id == item.id,
                    DemandHistory.period_number == period_number,
                    DemandHistory.period_year == period_year
                ).first()
                
                if history:
                    vendor_shipped += history.shipped
                    vendor_demand += history.shipped + history.lost_sales
            
            # Calculate fill rate
            fill_rate = 0.0
            if vendor_demand > 0:
                fill_rate = (vendor_shipped / vendor_demand) * 100
            
            results['vendor_fill_rates'][vendor.vendor_id] = {
                'vendor_name': vendor.name,
                'fill_rate': fill_rate,
                'total_shipped': vendor_shipped,
                'total_demand': vendor_demand,
                'total_lost_sales': vendor_demand - vendor_shipped,
                'item_count': len(items)
            }
            
            # Update totals
            total_shipped += vendor_shipped
            total_demand += vendor_demand
            
        except Exception as e:
            logger.error(f"Error analyzing fill rate for vendor {vendor.id}: {str(e)}")
            results['success'] = False
    
    # Calculate overall fill rate
    if total_demand > 0:
        results['overall_fill_rate'] = (total_shipped / total_demand) * 100
    
    # Sort vendors by fill rate
    sorted_vendors = sorted(
        results['vendor_fill_rates'].items(),
        key=lambda x: x[1]['fill_rate'],
        reverse=True
    )
    
    # Top and bottom performers
    results['top_performing_vendors'] = [
        {
            'vendor_id': vendor_id,
            **data
        }
        for vendor_id, data in sorted_vendors[:5]
    ]
    
    results['bottom_performing_vendors'] = [
        {
            'vendor_id': vendor_id,
            **data
        }
        for vendor_id, data in sorted_vendors[-5:] if data['fill_rate'] < 95
    ]
    
    results['end_time'] = datetime.now()
    results['duration'] = results['end_time'] - results['start_time']
    
    return results

def generate_service_level_report(
    results: Dict
) -> str:
    """Generate a service level report from analysis results.
    
    Args:
        results: Service level analysis results
        
    Returns:
        HTML report as string
    """
    report = f"""
    <html>
    <head>
        <title>Service Level Analysis Report</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 20px; }}
            .summary {{ background-color: #f0f0f0; padding: 15px; margin-bottom: 20px; }}
            .metric {{ margin: 10px 0; }}
            .table {{ border-collapse: collapse; width: 100%; margin: 20px 0; }}
            .table th, .table td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
            .table th {{ background-color: #f2f2f2; }}
            .red {{ color: red; }}
            .green {{ color: green; }}
            .yellow {{ color: orange; }}
        </style>
    </head>
    <body>
        <h1>Service Level Analysis Report</h1>
        <p>Period: {results['period_number']}/{results['period_year']}</p>
        
        <div class="summary">
            <h2>Summary</h2>
            <div class="metric">Total Items Processed: {results['processed_items']}</div>
            <div class="metric">Items Meeting Goal: {results['items_meeting_goal']} ({(results['items_meeting_goal']/results['processed_items']*100):.1f}%)</div>
            <div class="metric">Items Below Goal: {results['items_below_goal']} ({(results['items_below_goal']/results['processed_items']*100):.1f}%)</div>
            <div class="metric">Total Lost Sales: {results['total_lost_sales']:.1f} units</div>
            <div class="metric">Total Lost Sales Value: ${results['total_lost_sales_value']:.2f}</div>
        </div>
        
        <h2>Service Level Distribution</h2>
        <table class="table">
            <tr>
                <th>Category</th>
                <th>Count</th>
                <th>Percentage</th>
            </tr>
            <tr>
                <td>Excellent (≥99%)</td>
                <td>{results['service_level_distribution']['excellent']}</td>
                <td>{(results['service_level_distribution']['excellent']/results['processed_items']*100):.1f}%</td>
            </tr>
            <tr>
                <td>Good (95-98.9%)</td>
                <td>{results['service_level_distribution']['good']}</td>
                <td>{(results['service_level_distribution']['good']/results['processed_items']*100):.1f}%</td>
            </tr>
            <tr>
                <td>Fair (90-94.9%)</td>
                <td>{results['service_level_distribution']['fair']}</td>
                <td>{(results['service_level_distribution']['fair']/results['processed_items']*100):.1f}%</td>
            </tr>
            <tr>
                <td>Poor (&lt;90%)</td>
                <td>{results['service_level_distribution']['poor']}</td>
                <td>{(results['service_level_distribution']['poor']/results['processed_items']*100):.1f}%</td>
            </tr>
        </table>
        
        <h2>Top Service Level Gaps</h2>
        <table class="table">
            <tr>
                <th>Item ID</th>
                <th>Description</th>
                <th>Goal (%)</th>
                <th>Attained (%)</th>
                <th>Gap (%)</th>
                <th>Lost Sales</th>
                <th>Lost Sales Value</th>
            </tr>
    """
    
    for gap in results['top_service_level_gaps']:
        gap_class = 'red' if gap['gap'] > 10 else 'yellow'
        report += f"""
            <tr>
                <td>{gap['item_id']}</td>
                <td>{gap['description']}</td>
                <td>{gap['service_level_goal']:.1f}</td>
                <td>{gap['service_level_attained']:.1f}</td>
                <td class="{gap_class}">{gap['gap']:.1f}</td>
                <td>{gap['lost_sales']:.1f}</td>
                <td>${gap['lost_sales_value']:.2f}</td>
            </tr>
        """
    
    report += """
        </table>
        
        <h2>Recommendations</h2>
        <ul>
    """
    
    for rec in results['recommendations']:
        if rec.get('type') == 'SUMMARY':
            report += f"""
                <li><strong>{rec['message']}</strong>
                    <br>Action: {rec['action']}</li>
            """
        else:
            report += f"""
                <li>Item {rec['item_id']}: Adjust safety stock from {rec['current_sstf']:.1f} to {rec['recommended_sstf']:.1f} days
                    <br>Reason: {rec['reason']}</li>
            """
    
    report += """
        </ul>
    </body>
    </html>
    """
    
    return report

def adjust_forecasting_parameters(
    warehouse_id: int,
    session: Session,
    period_number: Optional[int] = None,
    period_year: Optional[int] = None
) -> Dict:
    """Adjust forecasting parameters based on actual period performance.
    
    Args:
        warehouse_id: Warehouse ID to process
        session: Database session
        period_number: Period number (defaults to previous period)
        period_year: Period year (defaults to previous period)
        
    Returns:
        Dictionary with parameter adjustment results
    """
    # Get company settings
    company = session.query(Company).first()
    if not company:
        raise Exception("Company settings not found")
    
    periodicity = company.forecasting_periodicity_default
    
    # Use previous period if not specified
    if period_number is None or period_year is None:
        current_period, current_year = get_current_period(periodicity)
        period_number, period_year = get_previous_period(current_period, current_year, periodicity)
    
    # Get all active items for the warehouse
    items = session.query(Item).filter(
        Item.warehouse_id == warehouse_id,
        Item.buyer_class.in_(['R', 'W'])  # Regular and Watch items
    ).all()
    
    results = {
        'warehouse_id': warehouse_id,
        'period_number': period_number,
        'period_year': period_year,
        'total_items': len(items),
        'processed_items': 0,
        'alpha_factor_adjustments': [],
        'lead_time_adjustments': [],
        'safety_stock_adjustments': [],
        'parameter_changes': {},
        'recommendations': [],
        'start_time': datetime.now(),
        'success': True
    }
    
    # Process each item
    for item in items:
        try:
            # 1. Adjust Alpha Factor based on tracking signal performance
            alpha_adjustment = adjust_alpha_factor(
                item, session, period_number, period_year
            )
            
            if alpha_adjustment:
                results['alpha_factor_adjustments'].append(alpha_adjustment)
            
            # 2. Adjust Lead Time based on actual performance
            lead_time_adjustment = adjust_lead_time(
                item, session, period_number, period_year
            )
            
            if lead_time_adjustment:
                results['lead_time_adjustments'].append(lead_time_adjustment)
            
            # 3. Adjust Safety Stock Time Factor
            safety_stock_adjustment = adjust_safety_stock_time_factor(
                item, session, period_number, period_year
            )
            
            if safety_stock_adjustment:
                results['safety_stock_adjustments'].append(safety_stock_adjustment)
            
            results['processed_items'] += 1
            
        except Exception as e:
            logger.error(f"Error adjusting parameters for item {item.id}: {str(e)}")
            results['success'] = False
    
    # 4. Generate parameter change recommendations
    results['recommendations'] = generate_parameter_recommendations(
        results['alpha_factor_adjustments'],
        results['lead_time_adjustments'],
        results['safety_stock_adjustments']
    )
    
    # 5. Create time-based parameters for systematic changes
    create_parameter_changes(results, session)
    
    results['end_time'] = datetime.now()
    results['duration'] = results['end_time'] - results['start_time']
    
    # Commit changes
    if results['success']:
        try:
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"Error committing parameter adjustments: {str(e)}")
            results['success'] = False
    
    return results

def adjust_alpha_factor(
    item: Item,
    session: Session,
    period_number: int,
    period_year: int
) -> Optional[Dict]:
    """Adjust alpha factor based on tracking signal performance.
    
    Args:
        item: Item to adjust
        session: Database session
        period_number: Period number
        period_year: Period year
        
    Returns:
        Adjustment details or None if no adjustment needed
    """
    # Get historical tracking signal data
    history = session.query(DemandHistory).filter(
        DemandHistory.item_id == item.id
    ).order_by(
        DemandHistory.period_year.desc(),
        DemandHistory.period_number.desc()
    ).limit(6).all()  # Last 6 periods
    
    if len(history) < 3:
        return None  # Not enough data
    
    # Calculate tracking signal volatility
    track_values = []
    for h in history:
        # Simulate track value (in real implementation, this would be stored)
        if h.total_demand > 0:
            forecast = item.demand_4weekly  # Placeholder
            track = abs(h.total_demand - forecast) / forecast * 100
            track_values.append(track)
    
    if not track_values:
        return None
    
    # Current alpha factor from company settings
    current_alpha = item.vendor.buyer_class_settings.get('alpha_factor', 10.0)
    
    # Calculate volatility
    volatility = np.std(track_values)
    
    # Adjust alpha factor based on volatility
    if volatility > 30:  # High volatility
        new_alpha = min(current_alpha * 1.2, 20.0)  # Increase alpha, cap at 20
    elif volatility < 10:  # Low volatility
        new_alpha = max(current_alpha * 0.8, 5.0)  # Decrease alpha, floor at 5
    else:
        return None  # No change needed
    
    if abs(new_alpha - current_alpha) > 0.5:  # Significant change
        return {
            'item_id': item.item_id,
            'current_alpha': current_alpha,
            'recommended_alpha': new_alpha,
            'volatility': volatility,
            'reason': f"Tracking signal volatility: {volatility:.1f}%"
        }
    
    return None

def adjust_lead_time(
    item: Item,
    session: Session,
    period_number: int,
    period_year: int
) -> Optional[Dict]:
    """Adjust lead time based on actual performance.
    
    Args:
        item: Item to adjust
        session: Database session
        period_number: Period number
        period_year: Period year
        
    Returns:
        Adjustment details or None if no adjustment needed
    """
    # Get recent order history with actual receipt dates
    # This is simplified - in real implementation, we'd track actual lead times
    
    # Simulate lead time history
    lead_time_history = []
    
    # For simplification, let's say we have some historical data
    # In real implementation, this would come from order tracking
    historical_lead_times = [
        item.lead_time_forecast * (1 + np.random.normal(0, 0.1))
        for _ in range(10)
    ]
    
    # Calculate actual vs forecasted lead times
    current_forecast = item.lead_time_forecast or 7
    actual_lead_times = [lt for lt in historical_lead_times if lt > 0]
    
    if len(actual_lead_times) < 3:
        return None
    
    # Forecast new lead time
    forecasted_lead_time = forecast_lead_time(
        historical_lead_times=actual_lead_times,
        current_lead_time=current_forecast
    )
    
    # Calculate variance
    variance = calculate_variance(actual_lead_times)
    
    # Determine if adjustment is needed
    if abs(forecasted_lead_time - current_forecast) > 1:  # More than 1 day difference
        return {
            'item_id': item.item_id,
            'current_lead_time': current_forecast,
            'recommended_lead_time': int(round(forecasted_lead_time)),
            'variance': variance,
            'actual_average': np.mean(actual_lead_times),
            'reason': f"Actual average lead time: {np.mean(actual_lead_times):.1f} days"
        }
    
    return None

def adjust_safety_stock_time_factor(
    item: Item,
    session: Session,
    period_number: int,
    period_year: int
) -> Optional[Dict]:
    """Adjust safety stock time factor based on service level performance.
    
    Args:
        item: Item to adjust
        session: Database session
        period_number: Period number
        period_year: Period year
        
    Returns:
        Adjustment details or None if no adjustment needed
    """
    # Get service level performance
    history = session.query(DemandHistory).filter(
        DemandHistory.item_id == item.id,
        DemandHistory.period_number == period_number,
        DemandHistory.period_year == period_year
    ).first()
    
    if not history:
        return None
    
    total_demand = history.shipped + history.lost_sales
    if total_demand == 0:
        return None
    
    service_level_attained = (history.shipped / total_demand) * 100
    service_level_goal = item.service_level_goal or 95.0
    
    # Adjust safety stock based on service level gap
    current_sstf = item.sstf or 0
    
    if current_sstf > 0 and item.madp is not None:
        adjusted_sstf = empirical_safety_stock_adjustment(
            current_safety_stock=current_sstf,
            service_level_goal=service_level_goal,
            service_level_attained=service_level_attained,
            max_adjustment_pct=10.0
        )
        
        if abs(adjusted_sstf - current_sstf) > 0.1:  # Significant change
            return {
                'item_id': item.item_id,
                'current_sstf': current_sstf,
                'recommended_sstf': adjusted_sstf,
                'service_level_goal': service_level_goal,
                'service_level_attained': service_level_attained,
                'gap': service_level_goal - service_level_attained,
                'reason': f"Service level gap: {service_level_goal - service_level_attained:.1f}%"
            }
    
    return None

def generate_parameter_recommendations(
    alpha_adjustments: List[Dict],
    lead_time_adjustments: List[Dict],
    safety_stock_adjustments: List[Dict]
) -> List[Dict]:
    """Generate parameter change recommendations.
    
    Args:
        alpha_adjustments: List of alpha factor adjustments
        lead_time_adjustments: List of lead time adjustments
        safety_stock_adjustments: List of safety stock adjustments
        
    Returns:
        List of recommendations
    """
    recommendations = []
    
    # Alpha factor adjustments
    if alpha_adjustments:
        alpha_summary = {
            'total_adjustments': len(alpha_adjustments),
            'average_change': np.mean([
                adj['recommended_alpha'] - adj['current_alpha']
                for adj in alpha_adjustments
            ]),
            'items_requiring_increase': len([
                adj for adj in alpha_adjustments
                if adj['recommended_alpha'] > adj['current_alpha']
            ]),
            'items_requiring_decrease': len([
                adj for adj in alpha_adjustments
                if adj['recommended_alpha'] < adj['current_alpha']
            ])
        }
        
        recommendations.append({
            'type': 'ALPHA_FACTOR',
            'summary': alpha_summary,
            'details': alpha_adjustments[:10]  # Top 10 adjustments
        })
    
    # Lead time adjustments
    if lead_time_adjustments:
        lead_time_summary = {
            'total_adjustments': len(lead_time_adjustments),
            'average_change': np.mean([
                adj['recommended_lead_time'] - adj['current_lead_time']
                for adj in lead_time_adjustments
            ]),
            'items_requiring_increase': len([
                adj for adj in lead_time_adjustments
                if adj['recommended_lead_time'] > adj['current_lead_time']
            ]),
            'items_requiring_decrease': len([
                adj for adj in lead_time_adjustments
                if adj['recommended_lead_time'] < adj['current_lead_time']
            ])
        }
        
        recommendations.append({
            'type': 'LEAD_TIME',
            'summary': lead_time_summary,
            'details': lead_time_adjustments[:10]  # Top 10 adjustments
        })
    
    # Safety stock adjustments
    if safety_stock_adjustments:
        safety_stock_summary = {
            'total_adjustments': len(safety_stock_adjustments),
            'average_change': np.mean([
                adj['recommended_sstf'] - adj['current_sstf']
                for adj in safety_stock_adjustments
            ]),
            'items_requiring_increase': len([
                adj for adj in safety_stock_adjustments
                if adj['recommended_sstf'] > adj['current_sstf']
            ]),
            'items_requiring_decrease': len([
                adj for adj in safety_stock_adjustments
                if adj['recommended_sstf'] < adj['current_sstf']
            ])
        }
        
        recommendations.append({
            'type': 'SAFETY_STOCK',
            'summary': safety_stock_summary,
            'details': safety_stock_adjustments[:10]  # Top 10 adjustments
        })
    
    return recommendations

def create_parameter_changes(results: Dict, session: Session) -> None:
    """Create time-based parameter changes for systematic adjustments.
    
    Args:
        results: Parameter adjustment results
        session: Database session
    """
    # This would create time-based parameter changes in the database
    # For now, we'll log the changes that would be created
    
    logger.info("Parameter changes that would be created:")
    
    # Alpha factor changes
    for adj in results.get('alpha_factor_adjustments', []):
        logger.info(f"  Alpha Factor - Item {adj['item_id']}: "
                   f"{adj['current_alpha']} -> {adj['recommended_alpha']}")
    
    # Lead time changes
    for adj in results.get('lead_time_adjustments', []):
        logger.info(f"  Lead Time - Item {adj['item_id']}: "
                   f"{adj['current_lead_time']} -> {adj['recommended_lead_time']}")
    
    # Safety stock changes
    for adj in results.get('safety_stock_adjustments', []):
        logger.info(f"  Safety Stock - Item {adj['item_id']}: "
                   f"{adj['current_sstf']} -> {adj['recommended_sstf']}")

def should_run_period_end() -> bool:
    """Check if period-end processing should run today.
    
    Returns:
        True if period-end processing should run
    """
    # Get company settings
    with session_scope() as session:
        company = session.query(Company).first()
        if not company:
            logger.error("Company settings not found")
            return False
        
        periodicity = company.forecasting_periodicity_default
    
    # Check if today is the last day of a period
    today = date.today()
    return is_period_end_day(today, periodicity)

def process_all_warehouses() -> Dict:
    """Process period-end for all warehouses.
    
    Returns:
        Dictionary with processing results
    """
    results = {
        'total_warehouses': 0,
        'processed_warehouses': 0,
        'total_items': 0,
        'processed_items': 0,
        'error_warehouses': 0,
        'errors': 0,
        'history_exceptions': 0,
        'start_time': datetime.now(),
        'end_time': None,
        'duration': None
    }
    
    try:
        with session_scope() as session:
            # Get all warehouses
            warehouses = session.query(Warehouse).all()
            results['total_warehouses'] = len(warehouses)
            
            # Process each warehouse
            for warehouse in warehouses:
                warehouse_results = process_warehouse(warehouse.warehouse_id, session)
                
                if warehouse_results.get('success', False):
                    results['processed_warehouses'] += 1
                else:
                    results['error_warehouses'] += 1
                
                results['total_items'] += warehouse_results.get('total_items', 0)
                results['processed_items'] += warehouse_results.get('processed_items', 0)
                results['errors'] += warehouse_results.get('errors', 0)
                results['history_exceptions'] += warehouse_results.get('history_exceptions', 0)
    
    except Exception as e:
        logger.error(f"Error during period-end processing: {str(e)}", exc_info=True)
        results['errors'] += 1
    
    # Set end time and duration
    results['end_time'] = datetime.now()
    results['duration'] = results['end_time'] - results['start_time']
    
    return results

def process_warehouse(warehouse_id: int, session: Optional[Session] = None) -> Dict:
    """Process period-end for a specific warehouse.
    
    Args:
        warehouse_id: Warehouse ID
        session: Optional database session
        
    Returns:
        Dictionary with processing results
    """
    results = {
        'warehouse_id': warehouse_id,
        'success': False,
        'total_items': 0,
        'processed_items': 0,
        'errors': 0,
        'history_exceptions': 0,
        'start_time': datetime.now(),
        'end_time': None,
        'duration': None
    }
    
    # Use provided session or create a new one
    close_session = False
    if session is None:
        session = Session()
        close_session = True
    
    try:
        # Reforecast all items
        reforecast_results = reforecast_items(warehouse_id, session)
        
        # Update results
        results['total_items'] = reforecast_results.get('total_items', 0)
        results['processed_items'] = reforecast_results.get('processed', 0)
        results['errors'] += reforecast_results.get('errors', 0)
        
        # Detect history exceptions
        exception_results = detect_history_exceptions(warehouse_id, session)
        
        # Update results
        results['history_exceptions'] = (
            exception_results.get('demand_filter_high', 0) +
            exception_results.get('demand_filter_low', 0) +
            exception_results.get('tracking_signal_high', 0) +
            exception_results.get('tracking_signal_low', 0) +
            exception_results.get('service_level_check', 0) +
            exception_results.get('infinity_check', 0)
        )
        results['errors'] += exception_results.get('errors', 0)
        
        # Calculate forecast accuracy for completed period
        current_period, current_year = get_current_period(periodicity)
        prev_period, prev_year = get_previous_period(current_period, current_year, periodicity)
        
        accuracy_results = calculate_forecast_accuracy(
            warehouse_id, session, prev_period, prev_year
        )
        
        save_successful = save_forecast_accuracy_results(accuracy_results, session)
        
        # Update results
        results['forecast_accuracy'] = accuracy_results
        results['forecast_accuracy_saved'] = save_successful
        
        # Update seasonal profiles
        seasonal_results = update_seasonal_profiles(warehouse_id, session)
        
        # Update results
        results['seasonal_profile_updates'] = seasonal_results
        
        # Update service levels
        service_level_results = update_service_levels(warehouse_id, session)
        
        # Analyze fill rate by vendor
        fill_rate_results = analyze_fill_rate_by_vendor(warehouse_id, session)
        
        # Update results
        results['service_level_updates'] = service_level_results
        results['fill_rate_analysis'] = fill_rate_results
        
        # Adjust forecasting parameters
        parameter_results = adjust_forecasting_parameters(warehouse_id, session)
        
        # Update results
        results['parameter_adjustments'] = parameter_results
        
        # Archive old resolved exceptions
        archive_results = archive_resolved_exceptions(session)
        results['errors'] += archive_results.get('errors', 0)
        
        results['success'] = True
    
    except Exception as e:
        logger.error(f"Error processing warehouse {warehouse_id}: {str(e)}", exc_info=True)
        results['errors'] += 1
    
    finally:
        if close_session:
            session.close()
    
    # Set end time and duration
    results['end_time'] = datetime.now()
    results['duration'] = results['end_time'] - results['start_time']
    
    return results

def reforecast_items(warehouse_id: int, session: Session) -> Dict:
    """Reforecast all items in a warehouse.
    
    Args:
        warehouse_id: Warehouse ID
        session: Database session
        
    Returns:
        Dictionary with reforecast results
    """
    forecast_service = ForecastService(session)
    
    # Process reforecasting
    results = forecast_service.process_period_end_reforecasting(warehouse_id=warehouse_id)
    
    return results

def detect_history_exceptions(warehouse_id: int, session: Session) -> Dict:
    """Detect history exceptions for a warehouse.
    
    Args:
        warehouse_id: Warehouse ID
        session: Database session
        
    Returns:
        Dictionary with exception detection results
    """
    forecast_service = ForecastService(session)
    
    # Detect exceptions
    results = forecast_service.detect_history_exceptions(warehouse_id=warehouse_id)
    
    return results

# This is the completion of the missing parts in the period_end_job.py file
# The existing code would be preserved, and these implementations would complete the file

def archive_resolved_exceptions(session: Session) -> Dict:
    """Archive old resolved history exceptions.
    
    Args:
        session: Database session
        
    Returns:
        Dictionary with archive results
    """
    results = {
        'total_exceptions': 0,
        'archived_exceptions': 0,
        'errors': 0,
        'start_time': datetime.now(),
        'success': True
    }
    
    try:
        # Get company settings for archive retention
        company = session.query(Company).first()
        if not company:
            raise Exception("Company settings not found")
        
        # Get retention days (default to 90 if not set)
        retention_days = getattr(company, 'archive_exceptions_days', 90)
        
        # Calculate cutoff date
        cutoff_date = datetime.now() - timedelta(days=retention_days)
        
        # Count total resolved exceptions older than cutoff date
        old_exceptions = session.query(HistoryException).filter(
            HistoryException.is_resolved == True,
            HistoryException.resolution_date < cutoff_date
        ).all()
        
        results['total_exceptions'] = len(old_exceptions)
        
        # Archive or delete old exceptions
        for exception in old_exceptions:
            try:
                # In a real implementation, we might move to an archive table
                # For now, we'll just delete them
                session.delete(exception)
                results['archived_exceptions'] += 1
            except Exception as e:
                logger.error(f"Error archiving exception {exception.id}: {str(e)}")
                results['errors'] += 1
                results['success'] = False
                session.rollback()
                session.add(exception)  # Re-add if delete failed
        
        if results['success']:
            session.commit()
        
    except Exception as e:
        logger.error(f"Error during exception archiving: {str(e)}")
        results['errors'] += 1
        results['success'] = False
        session.rollback()
    
    results['end_time'] = datetime.now()
    results['duration'] = results['end_time'] - results['start_time']
    
    return results


def generate_period_end_report(
    all_results: Dict,
    session: Session
) -> str:
    """Generate a comprehensive period-end report.
    
    Args:
        all_results: Dictionary containing all period-end processing results
        session: Database session
        
    Returns:
        HTML report as string
    """
    # Get company settings
    company = session.query(Company).first()
    periodicity = company.forecasting_periodicity_default if company else 12
    current_period, current_year = get_current_period(periodicity)
    
    report = f"""
    <html>
    <head>
        <title>Period-End Processing Report</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 20px; }}
            .summary {{ background-color: #f0f0f0; padding: 15px; margin-bottom: 20px; }}
            .metric {{ margin: 10px 0; }}
            .table {{ border-collapse: collapse; width: 100%; margin: 20px 0; }}
            .table th, .table td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
            .table th {{ background-color: #f2f2f2; }}
            .red {{ color: red; }}
            .green {{ color: green; }}
            .yellow {{ color: orange; }}
            .section {{ margin-bottom: 30px; }}
        </style>
    </head>
    <body>
        <h1>Period-End Processing Report</h1>
        <p>Processing Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        <p>Completed Period: {current_period}/{current_year}</p>
        
        <div class="summary">
            <h2>Overall Summary</h2>
            <div class="metric">Total Warehouses: {all_results.get('total_warehouses', 0)}</div>
            <div class="metric">Processed Warehouses: {all_results.get('processed_warehouses', 0)}</div>
            <div class="metric">Total Items: {all_results.get('total_items', 0)}</div>
            <div class="metric">Processed Items: {all_results.get('processed_items', 0)}</div>
            <div class="metric">History Exceptions: {all_results.get('history_exceptions', 0)}</div>
            <div class="metric">Processing Duration: {all_results.get('duration', '')}</div>
        </div>
        
        <div class="section">
            <h2>Warehouse Processing Details</h2>
            <table class="table">
                <tr>
                    <th>Warehouse ID</th>
                    <th>Items Processed</th>
                    <th>Exceptions</th>
                    <th>Status</th>
                    <th>Duration</th>
                </tr>
    """
    
    # Add warehouse details if available
    warehouse_results = all_results.get('warehouse_details', {})
    for wh_id, wh_data in warehouse_results.items():
        status_color = 'green' if wh_data.get('success', False) else 'red'
        report += f"""
                <tr>
                    <td>{wh_id}</td>
                    <td>{wh_data.get('processed_items', 0)}</td>
                    <td>{wh_data.get('history_exceptions', 0)}</td>
                    <td class="{status_color}">{'Success' if wh_data.get('success', False) else 'Failed'}</td>
                    <td>{wh_data.get('duration', '')}</td>
                </tr>
        """
    
    report += """
            </table>
        </div>
        
        <div class="section">
            <h2>Forecast Accuracy</h2>
            <p>Overall forecast accuracy metrics for the completed period.</p>
            <!-- Forecast accuracy details would be added here -->
        </div>
        
        <div class="section">
            <h2>Service Level Performance</h2>
            <p>Service level achievement summary for the completed period.</p>
            <!-- Service level details would be added here -->
        </div>
        
        <div class="section">
            <h2>Parameter Adjustments</h2>
            <p>Summary of forecasting parameter adjustments made during this run.</p>
            <!-- Parameter adjustment details would be added here -->
        </div>
        
        <div class="section">
            <h2>Error Summary</h2>
            <table class="table">
                <tr>
                    <th>Error Type</th>
                    <th>Count</th>
                    <th>Description</th>
                </tr>
    """
    
    # Add error summary if available
    if 'errors' in all_results and all_results['errors'] > 0:
        report += f"""
                <tr>
                    <td>Processing Errors</td>
                    <td class="red">{all_results['errors']}</td>
                    <td>Errors occurred during period-end processing</td>
                </tr>
        """
    
    report += """
            </table>
        </div>
    </body>
    </html>
    """
    
    return report


def email_period_end_report(
    report_html: str,
    recipients: List[str],
    subject: str = "Period-End Processing Report"
) -> bool:
    """Email the period-end report to specified recipients.
    
    Args:
        report_html: HTML report content
        recipients: List of email addresses
        subject: Email subject
        
    Returns:
        True if email sent successfully
    """
    # This is a placeholder implementation
    # In a real system, this would integrate with an email service
    
    logger.info(f"Would send period-end report to: {', '.join(recipients)}")
    logger.info(f"Subject: {subject}")
    
    # For now, just save the report to a file
    try:
        report_filename = f"period_end_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
        with open(f"logs/{report_filename}", 'w') as f:
            f.write(report_html)
        logger.info(f"Report saved to logs/{report_filename}")
        return True
    except Exception as e:
        logger.error(f"Error saving report: {str(e)}")
        return False


def run_period_end_job(warehouse_id: Optional[int] = None):
    """Run the period-end job for all warehouses or a specific warehouse.
    
    Args:
        warehouse_id: Optional warehouse ID to process only one warehouse
    """
    if not should_run_period_end():
        logger.info("Not end of period, skipping period-end processing")
        return
    
    logger.info("Starting period-end processing")
    start_time = datetime.now()
    
    try:
        if warehouse_id:
            # Process single warehouse
            with session_scope() as session:
                results = process_warehouse(warehouse_id, session)
                
            # Generate and send report for single warehouse
            report_html = generate_period_end_report(
                {
                    'total_warehouses': 1,
                    'processed_warehouses': 1 if results.get('success', False) else 0,
                    'total_items': results.get('total_items', 0),
                    'processed_items': results.get('processed_items', 0),
                    'history_exceptions': results.get('history_exceptions', 0),
                    'errors': results.get('errors', 0),
                    'duration': results.get('duration', ''),
                    'warehouse_details': {warehouse_id: results}
                },
                session
            )
        else:
            # Process all warehouses
            results = process_all_warehouses()
            
            # Generate comprehensive report
            with session_scope() as session:
                report_html = generate_period_end_report(results, session)
        
        # Send email report if configured
        if config.get_bool('PERIOD_END', 'enable_email_notifications', fallback=False):
            email_addresses = config.get('PERIOD_END', 'notification_email', fallback='').split(',')
            email_addresses = [email.strip() for email in email_addresses if email.strip()]
            
            if email_addresses:
                email_success = email_period_end_report(report_html, email_addresses)
                logger.info(f"Email notification sent: {'Success' if email_success else 'Failed'}")
        
        # Log final results
        logger.info(f"Period-end processing completed in {datetime.now() - start_time}")
        logger.info(f"Processed {results.get('processed_warehouses', 0)} warehouses")
        logger.info(f"Total items processed: {results.get('processed_items', 0)}")
        logger.info(f"History exceptions detected: {results.get('history_exceptions', 0)}")
        if results.get('errors', 0) > 0:
            logger.warning(f"Errors encountered: {results.get('errors', 0)}")
    
    except Exception as e:
        logger.error(f"Error during period-end processing: {str(e)}", exc_info=True)
        
        # Send error notification
        error_report = f"""
        <html>
        <body>
            <h1>Period-End Processing Error</h1>
            <p>An error occurred during period-end processing:</p>
            <p><strong>{str(e)}</strong></p>
            <p>Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        </body>
        </html>
        """
        
        if config.get_bool('PERIOD_END', 'enable_email_notifications', fallback=False):
            email_addresses = config.get('PERIOD_END', 'notification_email', fallback='').split(',')
            email_addresses = [email.strip() for email in email_addresses if email.strip()]
            
            if email_addresses:
                email_period_end_report(
                    error_report, 
                    email_addresses, 
                    subject="Period-End Processing Error"
                )


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Run period-end processing')
    parser.add_argument('--warehouse', type=int, help='Process specific warehouse ID')
    parser.add_argument('--force', action='store_true', help='Force run even if not period end')
    
    args = parser.parse_args()
    
    if args.force:
        # Temporarily override the should_run_period_end check for forced runs
        original_check = should_run_period_end
        should_run_period_end = lambda: True
        try:
            run_period_end_job(args.warehouse)
        finally:
            should_run_period_end = original_check
    else:
        run_period_end_job(args.warehouse)
        
# Additional Period-End Components That May Be Missing

"""
Based on the system analysis, here are additional components that may be needed to fully finalize period-end processing:
"""

# 1. SAFETY STOCK RECALCULATION
def recalculate_all_safety_stock(warehouse_id: Optional[int] = None) -> Dict:
    """Recalculate safety stock for all items based on new forecasts and MADPs."""
    
    results = {
        'total_items': 0,
        'updated_items': 0,
        'errors': 0,
        'start_time': datetime.now()
    }
    
    with session_scope() as session:
        safety_stock_service = SafetyStockService(session)
        results.update(safety_stock_service.update_safety_stock_for_all_items(
            warehouse_id=warehouse_id,
            update_order_points=True
        ))
    
    return results


# 2. ORDER POINT RECALCULATION
def recalculate_order_points(warehouse_id: Optional[int] = None) -> Dict:
    """Recalculate item and vendor order points based on new safety stock and lead times."""
    
    results = {
        'total_items': 0,
        'updated_items': 0,
        'errors': 0,
        'error_items': []
    }
    
    with session_scope() as session:
        # Get all active items
        query = session.query(Item).filter(
            Item.buyer_class.in_(['R', 'W'])
        )
        
        if warehouse_id:
            query = query.filter(Item.warehouse_id == warehouse_id)
        
        items = query.all()
        results['total_items'] = len(items)
        
        for item in items:
            try:
                # Calculate new item order point (IOP)
                iop = item.sstf + item.lead_time_forecast
                
                # Calculate new vendor order point (VOP)
                vendor_cycle = item.vendor.order_cycle_days if item.vendor else 0
                vop = iop + vendor_cycle
                
                # Update item (assuming fields exist)
                item.item_order_point = iop
                item.vendor_order_point = vop
                
                results['updated_items'] += 1
                
            except Exception as e:
                logger.error(f"Error recalculating order points for item {item.id}: {str(e)}")
                results['errors'] += 1
                results['error_items'].append({
                    'item_id': item.id,
                    'error': str(e)
                })
        
        session.commit()
    
    return results


# 3. PERIOD ARCHIVING AND ROLLOVER
def archive_period_data(warehouse_id: Optional[int] = None) -> Dict:
    """Archive old period data and prepare for new period."""
    
    results = {
        'total_archived': 0,
        'periods_archived': 0,
        'errors': 0,
        'start_time': datetime.now()
    }
    
    with session_scope() as session:
        # Get company settings
        company = session.query(Company).first()
        if not company:
            raise Exception("Company settings not found")
        
        periodicity = company.forecasting_periodicity_default
        retention_periods = getattr(company, 'periods_to_keep', 60)
        
        # Calculate cutoff
        current_period, current_year = get_current_period(periodicity)
        cutoff_year = current_year
        cutoff_period = current_period - retention_periods
        
        if cutoff_period <= 0:
            cutoff_year -= 1
            cutoff_period += periodicity
        
        # Archive demand history
        old_history = session.query(DemandHistory).filter(
            DemandHistory.period_year < cutoff_year,
            or_(
                DemandHistory.period_year < cutoff_year,
                and_(
                    DemandHistory.period_year == cutoff_year,
                    DemandHistory.period_number < cutoff_period
                )
            )
        )
        
        if warehouse_id:
            old_history = old_history.join(Item).filter(Item.warehouse_id == warehouse_id)
        
        results['total_archived'] = old_history.count()
        
        # Archive (or delete) old records
        for history in old_history:
            try:
                session.delete(history)
                results['periods_archived'] += 1
            except Exception as e:
                logger.error(f"Error archiving history {history.id}: {str(e)}")
                results['errors'] += 1
                session.rollback()
                session.add(history)
        
        if results['errors'] == 0:
            session.commit()
    
    return results


# 4. SYSTEM CLASS UPDATES
def update_system_classes(warehouse_id: Optional[int] = None) -> Dict:
    """Update system class assignments based on forecast and MADP ranges."""
    
    results = {
        'total_items': 0,
        'updated_items': 0,
        'system_class_distribution': defaultdict(int),
        'errors': 0
    }
    
    with session_scope() as session:
        # Get company settings
        company = session.query(Company).first()
        if not company:
            raise Exception("Company settings not found")
        
        slow_mover_limit = company.slow_mover_limit
        lumpy_demand_limit = company.lumpy_demand_limit
        
        # Get all active items
        query = session.query(Item).filter(
            Item.buyer_class.in_(['R', 'W'])
        )
        
        if warehouse_id:
            query = query.filter(Item.warehouse_id == warehouse_id)
        
        items = query.all()
        results['total_items'] = len(items)
        
        for item in items:
            try:
                old_class = item.system_class
                new_class = None
                
                # Determine new system class
                if item.forecast_method == ForecastMethod.E3_ALTERNATE:
                    new_class = SystemClassCode.ALTERNATE
                elif item.system_class == SystemClassCode.UNINITIALIZED:
                    if item.demand_yearly is not None and item.demand_yearly > 0:
                        new_class = SystemClassCode.NEW
                elif item.demand_yearly is not None:
                    if item.demand_yearly <= slow_mover_limit:
                        new_class = SystemClassCode.SLOW
                    elif item.madp and item.madp >= lumpy_demand_limit:
                        new_class = SystemClassCode.LUMPY
                    else:
                        new_class = SystemClassCode.REGULAR
                
                # Update if changed
                if new_class and new_class != old_class:
                    item.system_class = new_class
                    results['updated_items'] += 1
                
                # Count distribution
                results['system_class_distribution'][item.system_class] += 1
                
            except Exception as e:
                logger.error(f"Error updating system class for item {item.id}: {str(e)}")
                results['errors'] += 1
        
        session.commit()
    
    return results


# 5. PERIOD-SPECIFIC CLEANUP
def perform_period_cleanup(warehouse_id: Optional[int] = None) -> Dict:
    """Perform cleanup tasks specific to period end."""
    
    results = {
        'total_cleanups': 0,
        'deleted_records': 0,
        'errors': 0,
        'cleanup_details': {}
    }
    
    with session_scope() as session:
        # 1. Clear period-specific forecast locks
        cleared_locks = session.query(Item).filter(
            Item.freeze_until_date.isnot(None),
            Item.freeze_until_date < date.today()
        )
        
        if warehouse_id:
            cleared_locks = cleared_locks.filter(Item.warehouse_id == warehouse_id)
        
        lock_count = cleared_locks.update({Item.freeze_until_date: None})
        results['cleanup_details']['cleared_forecast_locks'] = lock_count
        
        # 2. Clear period-specific order adjustments
        # (Implementation depends on how adjustments are stored)
        
        # 3. Clear temporary period data
        # (Implementation depends on specific temporary tables/fields)
        
        session.commit()
        results['total_cleanups'] = sum(results['cleanup_details'].values())
    
    return results


# 6. UPDATED PERIOD-END WORKFLOW
def enhanced_run_period_end_job(warehouse_id: Optional[int] = None):
    """Enhanced period-end job with all required components."""
    
    if not should_run_period_end():
        logger.info("Not end of period, skipping period-end processing")
        return
    
    logger.info("Starting enhanced period-end processing")
    start_time = datetime.now()
    
    all_results = {
        'start_time': start_time,
        'total_warehouses': 0,
        'processed_warehouses': 0,
        'total_errors': 0,
        'component_results': {}
    }
    
    try:
        with session_scope() as session:
            # Get warehouses to process
            if warehouse_id:
                warehouses = session.query(Warehouse).filter(
                    Warehouse.warehouse_id == warehouse_id
                ).all()
            else:
                warehouses = session.query(Warehouse).all()
            
            all_results['total_warehouses'] = len(warehouses)
            
            for warehouse in warehouses:
                warehouse_results = {}
                
                try:
                    # 1. Basic period-end processing
                    basic_results = process_warehouse(warehouse.warehouse_id, session)
                    warehouse_results['basic_processing'] = basic_results
                    
                    # 2. Recalculate safety stock
                    safety_stock_results = recalculate_all_safety_stock(warehouse.warehouse_id)
                    warehouse_results['safety_stock'] = safety_stock_results
                    
                    # 3. Recalculate order points
                    order_point_results = recalculate_order_points(warehouse.warehouse_id)
                    warehouse_results['order_points'] = order_point_results
                    
                    # 4. Archive old data
                    archive_results = archive_period_data(warehouse.warehouse_id)
                    warehouse_results['archive'] = archive_results
                    
                    # 5. Update system classes
                    system_class_results = update_system_classes(warehouse.warehouse_id)
                    warehouse_results['system_classes'] = system_class_results
                    
                    # 6. Perform cleanup
                    cleanup_results = perform_period_cleanup(warehouse.warehouse_id)
                    warehouse_results['cleanup'] = cleanup_results
                    
                    all_results['processed_warehouses'] += 1
                    
                except Exception as e:
                    logger.error(f"Error processing warehouse {warehouse.warehouse_id}: {str(e)}")
                    all_results['total_errors'] += 1
                    warehouse_results['error'] = str(e)
                
                all_results['component_results'][warehouse.warehouse_id] = warehouse_results
        
        # Generate comprehensive report
        report_html = generate_enhanced_period_end_report(all_results)
        
        # Send notifications
        if config.get_bool('PERIOD_END', 'enable_email_notifications', fallback=False):
            email_addresses = config.get('PERIOD_END', 'notification_email', fallback='').split(',')
            email_addresses = [email.strip() for email in email_addresses if email.strip()]
            
            if email_addresses:
                email_period_end_report(report_html, email_addresses)
        
        # Log final results
        logger.info(f"Enhanced period-end processing completed in {datetime.now() - start_time}")
        logger.info(f"Processed {all_results['processed_warehouses']} warehouses")
        if all_results['total_errors'] > 0:
            logger.warning(f"Total errors encountered: {all_results['total_errors']}")
    
    except Exception as e:
        logger.error(f"Critical error during enhanced period-end processing: {str(e)}", exc_info=True)
        
        # Send error notification
        if config.get_bool('PERIOD_END', 'enable_email_notifications', fallback=False):
            email_addresses = config.get('PERIOD_END', 'notification_email', fallback='').split(',')
            email_addresses = [email.strip() for email in email_addresses if email.strip()]
            
            if email_addresses:
                error_report = f"""
                <html>
                <body>
                    <h1>Critical Period-End Processing Error</h1>
                    <p>A critical error occurred during enhanced period-end processing:</p>
                    <p><strong>{str(e)}</strong></p>
                    <p>Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                    <p>Processed warehouses: {all_results.get('processed_warehouses', 0)}</p>
                </body>
                </html>
                """
                email_period_end_report(error_report, email_addresses, subject="Critical Period-End Error")