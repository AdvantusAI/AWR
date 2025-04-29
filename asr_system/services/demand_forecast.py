"""
Demand forecasting services for the ASR system.

This module implements the algorithms and functions for demand forecasting, 
including E3 Regular AVS, E3 Enhanced AVS, and seasonal adjustments.
"""
import logging
import math
import numpy as np
from datetime import datetime
from sqlalchemy import and_, func

from models.sku import SKU, ForecastData, DemandHistory, SeasonalProfile
from utils.helpers import get_current_period, calculate_madp, calculate_tracking_signal, get_seasonal_index
from utils.db import get_session
from config.settings import ASR_CONFIG

logger = logging.getLogger(__name__)

def get_demand_history(session, sku_id, store_id, years=3):
    """
    Get demand history for a SKU.
    
    Args:
        session: SQLAlchemy session
        sku_id (str): SKU ID
        store_id (str): Store ID
        years (int): Number of years of history to retrieve
    
    Returns:
        dict: Dictionary with period data as keys and demand as values
    """
    try:
        # Get SKU
        sku = session.query(SKU).filter(
            and_(SKU.sku_id == sku_id, SKU.store_id == store_id)
        ).first()
        
        if not sku:
            logger.error(f"SKU {sku_id} not found in store {store_id}")
            return {}
        
        # Get current period
        forecast_data = session.query(ForecastData).filter(
            and_(ForecastData.sku_id == sku_id, ForecastData.store_id == store_id)
        ).first()
        
        # Determine periodicity
        periodicity = getattr(sku, 'forecasting_periodicity', 13)  # Default to 13 (4-weekly)
        
        # Get current period
        current_year, current_period = get_current_period(periodicity)
        
        # Build array of periods to retrieve
        periods = []
        for year_offset in range(years):
            year = current_year - year_offset
            
            if year_offset == 0:
                # Current year - include only periods up to current period
                for period in range(1, current_period + 1):
                    periods.append((year, period))
            else:
                # Previous years - include all periods
                for period in range(1, periodicity + 1):
                    periods.append((year, period))
        
        # Get history for periods
        history_data = {}
        
        for year, period in periods:
            history = session.query(DemandHistory).filter(
                and_(
                    DemandHistory.sku_id == sku_id,
                    DemandHistory.store_id == store_id,
                    DemandHistory.period_year == year,
                    DemandHistory.period_number == period
                )
            ).first()
            
            if history and not history.ignore_history:
                history_data[(year, period)] = history.total_demand
        
        return history_data
    
    except Exception as e:
        logger.error(f"Error getting demand history: {e}")
        return {}

def calculate_e3_regular_avs_forecast(session, sku_id, store_id):
    """
    Calculate a new forecast using the E3 Regular AVS method.
    
    Args:
        session: SQLAlchemy session
        sku_id (str): SKU ID
        store_id (str): Store ID
    
    Returns:
        dict: Dictionary with forecast results
    """
    try:
        # Get demand history
        history_data = get_demand_history(session, sku_id, store_id)
        
        if not history_data:
            logger.warning(f"No demand history found for SKU {sku_id} in store {store_id}")
            return None
        
        # Get current forecast data
        forecast_data = session.query(ForecastData).filter(
            and_(ForecastData.sku_id == sku_id, ForecastData.store_id == store_id)
        ).first()
        
        if not forecast_data:
            logger.warning(f"No forecast data found for SKU {sku_id} in store {store_id}")
            return None
        
        # Get current period
        periodicity = getattr(forecast_data, 'forecasting_periodicity', 13)  # Default to 13 (4-weekly)
        current_year, current_period = get_current_period(periodicity)
        
        # Get previous period
        prev_period = current_period - 1
        prev_year = current_year
        if prev_period < 1:
            prev_period = periodicity
            prev_year = current_year - 1
        
        # Get previous period's demand
        prev_demand = history_data.get((prev_year, prev_period), 0.0)
        
        # Get seasonal profile
        sku = session.query(SKU).filter(
            and_(SKU.sku_id == sku_id, SKU.store_id == store_id)
        ).first()
        
        seasonal_profile = None
        if sku and sku.demand_profile_id:
            seasonal_profile = session.query(SeasonalProfile).filter(
                SeasonalProfile.profile_id == sku.demand_profile_id
            ).first()
        
        # Get seasonal index for previous period
        if seasonal_profile:
            seasonal_index = get_seasonal_index(seasonal_profile, prev_period)
            # Adjust demand for seasonality
            if seasonal_index > 0:
                prev_demand = prev_demand / seasonal_index
        
        # Get track (trend percentage)
        track = forecast_data.track or 0.0
        
        # Calculate new forecast using E3 Regular AVS formula
        old_forecast = forecast_data.period_forecast or 0.0
        
        # Formula: New Forecast = (Track * Newest Demand) + ((1 - Track) * Old Forecast)
        new_forecast = (track * prev_demand) + ((1 - track) * old_forecast)
        
        # Update weekly and other period forecasts
        if periodicity == 13:  # 4-weekly
            weekly_forecast = new_forecast / 4.0
            quarterly_forecast = new_forecast * 3.0
            yearly_forecast = new_forecast * 13.0
        else:  # Weekly
            weekly_forecast = new_forecast
            quarterly_forecast = new_forecast * 13.0
            yearly_forecast = new_forecast * 52.0
        
        # Calculate MADP and Track
        history_values = list(history_data.values())
        forecast_values = [forecast_data.period_forecast] * len(history_values)
        
        # Update forecast values with new forecast for future comparison
        forecast_values[0] = new_forecast
        
        madp = calculate_madp(history_values, forecast_values)
        track = calculate_tracking_signal(history_values, forecast_values)
        
        # Return results
        return {
            'period_forecast': new_forecast,
            'weekly_forecast': weekly_forecast,
            'quarterly_forecast': quarterly_forecast,
            'yearly_forecast': yearly_forecast,
            'madp': madp,
            'track': track
        }
    
    except Exception as e:
        logger.error(f"Error calculating E3 Regular AVS forecast: {e}")
        return None

def calculate_e3_enhanced_avs_forecast(session, sku_id, store_id, forecast_demand_limit=None):
    """
    Calculate a new forecast using the E3 Enhanced AVS method for slow-moving or intermittent items.
    
    The Enhanced AVS method is specifically designed for items with intermittent demand
    patterns where many periods might have zero or very low demand interspersed with
    periods of activity. The algorithm accounts for the time since last demand occurrence
    to produce more accurate forecasts.
    
    Args:
        session: SQLAlchemy session
        sku_id (str): SKU ID
        store_id (str): Store ID
        forecast_demand_limit (float): Optional override for the forecasting demand limit
    
    Returns:
        dict: Dictionary with forecast results or None if insufficient data
    """
    try:
        # Get demand history
        history_data = get_demand_history(session, sku_id, store_id)
        
        if not history_data:
            logger.warning(f"No demand history found for SKU {sku_id} in store {store_id}")
            return None
        
        # Get current forecast data
        forecast_data = session.query(ForecastData).filter(
            and_(ForecastData.sku_id == sku_id, ForecastData.store_id == store_id)
        ).first()
        
        if not forecast_data:
            logger.warning(f"No forecast data found for SKU {sku_id} in store {store_id}")
            return None
        
        # Get the SKU
        sku = session.query(SKU).filter(
            and_(SKU.sku_id == sku_id, SKU.store_id == store_id)
        ).first()
        
        if not sku:
            logger.warning(f"SKU {sku_id} not found in store {store_id}")
            return None
        
        # Get the forecasting demand limit - either from parameter, SKU, source, or company default
        if forecast_demand_limit is None:
            forecast_demand_limit = getattr(sku, 'forecasting_demand_limit', None)
            
            if forecast_demand_limit is None and sku.source:
                forecast_demand_limit = getattr(sku.source, 'forecasting_demand_limit', None)
            
            if forecast_demand_limit is None:
                forecast_demand_limit = ASR_CONFIG.get('forecasting_demand_limit', 0.0)
        
        # Get current period
        periodicity = getattr(forecast_data, 'forecasting_periodicity', 13)  # Default to 13 (4-weekly)
        current_year, current_period = get_current_period(periodicity)
        
        # Get previous period
        prev_period = current_period - 1
        prev_year = current_year
        if prev_period < 1:
            prev_period = periodicity
            prev_year = current_year - 1
        
        # Get previous period's demand
        prev_demand = history_data.get((prev_year, prev_period), 0.0)
        
        # Check if demand exceeds the limit
        if prev_demand <= forecast_demand_limit:
            # For E3 Enhanced, unlike regular E3, we need to check when the last
            # significant demand occurred to correctly adjust the track value
            
            # Initialize tracking variables
            last_significant_demand = None
            last_significant_period = None
            last_significant_year = None
            
            # Find the most recent period with demand > limit by sorting history
            sorted_periods = sorted(history_data.keys(), reverse=True)
            
            for year, period in sorted_periods:
                demand = history_data.get((year, period), 0.0)
                if demand > forecast_demand_limit:
                    last_significant_demand = demand
                    last_significant_period = period
                    last_significant_year = year
                    break
            
            # If no significant demand found in history, don't update the forecast
            if last_significant_demand is None:
                return {
                    'period_forecast': forecast_data.period_forecast,
                    'weekly_forecast': forecast_data.weekly_forecast,
                    'quarterly_forecast': forecast_data.quarterly_forecast,
                    'yearly_forecast': forecast_data.yearly_forecast,
                    'madp': forecast_data.madp,
                    'track': forecast_data.track
                }
            
            # Calculate periods since last significant demand
            if last_significant_year == prev_year:
                periods_since_last_demand = prev_period - last_significant_period
            else:
                periods_since_last_demand = prev_period + (periodicity * (prev_year - last_significant_year)) - last_significant_period
            
            # Don't allow zero or negative periods
            periods_since_last_demand = max(1, periods_since_last_demand)
            
            # Get the update frequency impact control from Company Control Factors
            update_frequency_impact = ASR_CONFIG.get('update_frequency_impact', 0.95)
            
            # Adjust track for slow-moving items:
            # As periods without demand increase, the impact of the last demand decreases
            adjusted_track = forecast_data.track * (update_frequency_impact ** periods_since_last_demand)
            
            # Create forecast response with unchanged values as we didn't update
            return {
                'period_forecast': forecast_data.period_forecast,
                'weekly_forecast': forecast_data.weekly_forecast,
                'quarterly_forecast': forecast_data.quarterly_forecast,
                'yearly_forecast': forecast_data.yearly_forecast,
                'madp': forecast_data.madp,
                'track': adjusted_track  # Only track is adjusted
            }
        
        # Process demand that exceeds the limit - normal reforecast occurs
        
        # Get seasonal profile
        seasonal_profile = None
        if sku.demand_profile_id:
            seasonal_profile = session.query(SeasonalProfile).filter(
                SeasonalProfile.profile_id == sku.demand_profile_id
            ).first()
        
        # Get seasonal index for previous period
        if seasonal_profile:
            from utils.helpers import get_seasonal_index
            seasonal_index = get_seasonal_index(seasonal_profile, prev_period)
            # Adjust demand for seasonality
            if seasonal_index > 0:
                prev_demand = prev_demand / seasonal_index
        
        # Calculate periods since last significant demand
        periods_since_last_demand = 1  # Default to 1 period
        
        # Find the most recent period with demand > limit
        for i, (year, period) in enumerate(sorted_periods[1:], 1):  # Skip current period (index 0)
            demand = history_data.get((year, period), 0.0)
            if demand > forecast_demand_limit:
                # Calculate periods since last demand
                if year == prev_year:
                    periods_since_last_demand = prev_period - period
                else:
                    periods_since_last_demand = prev_period + (periodicity * (prev_year - year)) - period
                break
        
        # Get track (trend percentage)
        track = forecast_data.track or 0.2  # Default track if none exists
        
        # Adjust track based on time since last demand
        # This is a key differentiator of Enhanced E3 AVS - track decreases 
        # exponentially with time since last significant demand
        update_frequency_impact = ASR_CONFIG.get('update_frequency_impact', 0.95)
        adjusted_track = track * (update_frequency_impact ** (periods_since_last_demand - 1))
        
        # Cap the adjusted track to prevent it from getting too small
        min_track = 0.01  # Minimum track value
        adjusted_track = max(min_track, adjusted_track)
        
        # Calculate new forecast using E3 Enhanced AVS formula
        old_forecast = forecast_data.period_forecast or 0.0
        
        # Formula with adjustment for time between demands
        new_forecast = (adjusted_track * prev_demand) + ((1 - adjusted_track) * old_forecast)
        
        # Update weekly and other period forecasts
        if periodicity == 13:  # 4-weekly
            weekly_forecast = new_forecast / 4.0
            quarterly_forecast = new_forecast * 3.0
            yearly_forecast = new_forecast * 13.0
        else:  # Weekly
            weekly_forecast = new_forecast
            quarterly_forecast = new_forecast * 13.0
            yearly_forecast = new_forecast * 52.0
        
        # Calculate MADP and Track
        history_values = list(history_data.values())
        forecast_values = [forecast_data.period_forecast] * len(history_values)
        
        # Update forecast values with new forecast for future comparison
        forecast_values[0] = new_forecast
        
        # Calculate updated MADP and track
        from utils.helpers import calculate_madp, calculate_tracking_signal
        madp = calculate_madp(history_values, forecast_values)
        track = calculate_tracking_signal(history_values, forecast_values)
        
        # Return results
        return {
            'period_forecast': new_forecast,
            'weekly_forecast': weekly_forecast,
            'quarterly_forecast': quarterly_forecast,
            'yearly_forecast': yearly_forecast,
            'madp': madp,
            'track': track,
            'periods_since_demand': periods_since_last_demand
        }
    
    except Exception as e:
        logger.error(f"Error calculating E3 Enhanced AVS forecast: {e}")
        return None

def calculate_initial_forecast(session, sku_id, store_id, starting_forecast=None):
    """
    Calculate an initial forecast for a new SKU.
    
    Args:
        session: SQLAlchemy session
        sku_id (str): SKU ID
        store_id (str): Store ID
        starting_forecast (float): Optional starting forecast value
    
    Returns:
        dict: Dictionary with forecast results
    """
    try:
        # Get SKU
        sku = session.query(SKU).filter(
            and_(SKU.sku_id == sku_id, SKU.store_id == store_id)
        ).first()
        
        if not sku:
            logger.error(f"SKU {sku_id} not found in store {store_id}")
            return None
        
        # Get forecast periodicity
        periodicity = getattr(sku, 'forecasting_periodicity', 13)  # Default to 13 (4-weekly)
        
        # If starting forecast is provided, use it
        if starting_forecast is not None:
            period_forecast = starting_forecast
        else:
            # No starting forecast provided, try to estimate from similar SKUs
            similar_skus = session.query(SKU).filter(
                and_(
                    SKU.source_id == sku.source_id,
                    SKU.store_id == store_id,
                    SKU.id != sku.id,
                    SKU.system_class != 'U',  # Not uninitialized
                    SKU.buyer_class.in_(['R', 'W'])  # Regular or Watch
                )
            ).all()
            
            if similar_skus:
                # Get forecast data for similar SKUs
                sku_ids = [s.id for s in similar_skus]
                forecast_data = session.query(ForecastData).filter(
                    ForecastData.sku_id.in_(sku_ids)
                ).all()
                
                if forecast_data:
                    # Calculate average forecast
                    avg_forecast = sum(f.period_forecast for f in forecast_data) / len(forecast_data)
                    period_forecast = avg_forecast
                else:
                    # No forecast data for similar SKUs, use default
                    period_forecast = 1.0
            else:
                # No similar SKUs, use default
                period_forecast = 1.0
        
        # Calculate weekly and other period forecasts
        if periodicity == 13:  # 4-weekly
            weekly_forecast = period_forecast / 4.0
            quarterly_forecast = period_forecast * 3.0
            yearly_forecast = period_forecast * 13.0
        else:  # Weekly
            weekly_forecast = period_forecast
            quarterly_forecast = period_forecast * 13.0
            yearly_forecast = period_forecast * 52.0
        
        # For new SKUs, set default MADP and track
        madp = 30.0  # Default MADP
        track = 0.2   # Default track
        
        # Return results
        return {
            'period_forecast': period_forecast,
            'weekly_forecast': weekly_forecast,
            'quarterly_forecast': quarterly_forecast,
            'yearly_forecast': yearly_forecast,
            'madp': madp,
            'track': track
        }
    
    except Exception as e:
        logger.error(f"Error calculating initial forecast: {e}")
        return None

def update_forecast_data(session, sku_id, store_id, forecast_results, manual=False):
    """
    Update forecast data for a SKU.
    
    Args:
        session: SQLAlchemy session
        sku_id (str): SKU ID
        store_id (str): Store ID
        forecast_results (dict): Dictionary with forecast results
        manual (bool): True if manual forecast update
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Get forecast data
        forecast_data = session.query(ForecastData).filter(
            and_(ForecastData.sku_id == sku_id, ForecastData.store_id == store_id)
        ).first()
        
        if not forecast_data:
            # Create new forecast data
            forecast_data = ForecastData(
                sku_id=sku_id,
                store_id=store_id
            )
            session.add(forecast_data)
        
        # Update forecast data
        forecast_data.period_forecast = forecast_results['period_forecast']
        forecast_data.weekly_forecast = forecast_results['weekly_forecast']
        forecast_data.quarterly_forecast = forecast_results['quarterly_forecast']
        forecast_data.yearly_forecast = forecast_results['yearly_forecast']
        forecast_data.madp = forecast_results['madp']
        forecast_data.track = forecast_results['track']
        forecast_data.last_forecast_date = datetime.now()
        
        if manual:
            forecast_data.last_manual_forecast_date = datetime.now()
        
        # Update system class for SKU
        sku = session.query(SKU).filter(
            and_(SKU.sku_id == sku_id, SKU.store_id == store_id)
        ).first()
        
        if sku:
            # Update system class based on forecast values
            if sku.system_class == 'U':  # Uninitialized
                sku.system_class = 'N'  # New
            elif sku.system_class == 'N':  # Keep as New until 6 months old
                # Only change if SKU is more than 6 months old
                if sku.created_at and (datetime.now() - sku.created_at).days > 180:
                    # Check MADP for Lumpy
                    madp_high_threshold = ASR_CONFIG.get('madp_high_threshold', 50)
                    if forecast_results['madp'] > madp_high_threshold:
                        sku.system_class = 'L'  # Lumpy
                    else:
                        # Check for slow mover
                        slow_mover_limit = ASR_CONFIG.get('slow_mover_limit', 5)
                        if forecast_results['yearly_forecast'] < slow_mover_limit:
                            sku.system_class = 'S'  # Slow
                        else:
                            sku.system_class = 'R'  # Regular
            elif sku.system_class != 'A':  # Don't change Alternate
                # Check MADP for Lumpy
                madp_high_threshold = ASR_CONFIG.get('madp_high_threshold', 50)
                if forecast_results['madp'] > madp_high_threshold:
                    sku.system_class = 'L'  # Lumpy
                else:
                    # Check for slow mover
                    slow_mover_limit = ASR_CONFIG.get('slow_mover_limit', 5)
                    if forecast_results['yearly_forecast'] < slow_mover_limit:
                        sku.system_class = 'S'  # Slow
                    else:
                        sku.system_class = 'R'  # Regular
        
        # Commit changes
        session.commit()
        return True
    
    except Exception as e:
        logger.error(f"Error updating forecast data: {e}")
        session.rollback()
        return False

def run_period_end_forecasting(session):
    """
    Run period-end forecasting for all SKUs.
    
    Args:
        session: SQLAlchemy session
    
    Returns:
        dict: Statistics about the forecasting run
    """
    try:
        # Get active SKUs
        skus = session.query(SKU).filter(
            SKU.buyer_class.in_(['R', 'W'])
        ).all()
        
        # Statistics
        stats = {
            'total_skus': len(skus),
            'e3_regular_avs': 0,
            'e3_enhanced_avs': 0,
            'demand_import': 0,
            'e3_alternate': 0,
            'frozen_forecast': 0,
            'no_update': 0,
            'errors': 0
        }
        
        # Process each SKU
        for sku in skus:
            try:
                # Check for frozen forecast
                if hasattr(sku, 'freeze_forecast_until') and sku.freeze_forecast_until:
                    if sku.freeze_forecast_until > datetime.now():
                        # Skip this SKU if forecast is frozen
                        stats['frozen_forecast'] += 1
                        continue
                
                # Determine forecast method
                forecast_method = getattr(sku, 'forecast_method', 'E3 Regular AVS')
                
                if forecast_method == 'E3 Regular AVS':
                    # Calculate forecast using E3 Regular AVS
                    forecast_results = calculate_e3_regular_avs_forecast(session, sku.sku_id, sku.store_id)
                    if forecast_results:
                        update_forecast_data(session, sku.sku_id, sku.store_id, forecast_results)
                        stats['e3_regular_avs'] += 1
                    else:
                        stats['no_update'] += 1
                
                elif forecast_method == 'E3 Enhanced AVS':
                    # Calculate forecast using E3 Enhanced AVS
                    forecast_results = calculate_e3_enhanced_avs_forecast(session, sku.sku_id, sku.store_id)
                    if forecast_results:
                        update_forecast_data(session, sku.sku_id, sku.store_id, forecast_results)
                        stats['e3_enhanced_avs'] += 1
                    else:
                        stats['no_update'] += 1
                
                elif forecast_method == 'Demand Import':
                    # Skip SKUs with imported demand
                    stats['demand_import'] += 1
                
                elif forecast_method == 'E3 Alternate':
                    # Skip SKUs with alternate forecast method
                    stats['e3_alternate'] += 1
                
                else:
                    # Unknown forecast method
                    stats['no_update'] += 1
            
            except Exception as e:
                logger.error(f"Error forecasting SKU {sku.sku_id}: {e}")
                stats['errors'] += 1
        
        return stats
    
    except Exception as e:
        logger.error(f"Error running period-end forecasting: {e}")
        return {'error': str(e)}

def simulate_seasonal_profile(session, sku_id, store_id, ignore_years=None):
    """
    Simulate a seasonal profile for a SKU.
    
    Args:
        session: SQLAlchemy session
        sku_id (str): SKU ID
        store_id (str): Store ID
        ignore_years (list): List of years to ignore in calculation
    
    Returns:
        dict: Dictionary with seasonal indices
    """
    try:
        # Get demand history
        history_data = get_demand_history(session, sku_id, store_id)
        
        if not history_data:
            logger.warning(f"No demand history found for SKU {sku_id} in store {store_id}")
            return None
        
        # Determine periodicity
        sku = session.query(SKU).filter(
            and_(SKU.sku_id == sku_id, SKU.store_id == store_id)
        ).first()
        
        periodicity = getattr(sku, 'forecasting_periodicity', 13)  # Default to 13 (4-weekly)
        
        # Filter out ignored years
        if ignore_years:
            history_data = {k: v for k, v in history_data.items() if k[0] not in ignore_years}
        
        # Group by period
        period_data = {}
        for (year, period), demand in history_data.items():
            if period not in period_data:
                period_data[period] = []
            period_data[period].append(demand)
        
        # Calculate average demand per period
        period_averages = {}
        for period, demands in period_data.items():
            if demands:
                period_averages[period] = sum(demands) / len(demands)
        
        if not period_averages:
            logger.warning(f"No valid period averages found for SKU {sku_id} in store {store_id}")
            return None
        
        # Calculate average demand across all periods
        all_demands = [d for demands in period_data.values() for d in demands]
        overall_average = sum(all_demands) / len(all_demands) if all_demands else 0.0
        
        if overall_average == 0.0:
            logger.warning(f"Overall average demand is zero for SKU {sku_id} in store {store_id}")
            return None
        
        # Calculate seasonal indices
        seasonal_indices = {}
        for period in range(1, periodicity + 1):
            if period in period_averages and period_averages[period] > 0:
                seasonal_indices[period] = period_averages[period] / overall_average
            else:
                seasonal_indices[period] = 1.0  # Default to 1.0 if no data
        
        return seasonal_indices
    
    except Exception as e:
        logger.error(f"Error simulating seasonal profile: {e}")
        return None

def create_seasonal_profile(session, profile_id, seasonal_indices, description=None):
    """
    Create a seasonal profile.
    
    Args:
        session: SQLAlchemy session
        profile_id (str): Profile ID
        seasonal_indices (dict): Dictionary with seasonal indices
        description (str): Optional profile description
    
    Returns:
        SeasonalProfile: Created profile object
    """
    try:
        # Check if profile already exists
        profile = session.query(SeasonalProfile).filter(
            SeasonalProfile.profile_id == profile_id
        ).first()
        
        if not profile:
            # Create new profile
            profile = SeasonalProfile(
                profile_id=profile_id,
                description=description or f"Profile {profile_id}"
            )
            session.add(profile)
        
        # Update profile indices
        for period, index in seasonal_indices.items():
            # Set field based on period number
            setattr(profile, f"p{period}_index", index)
        
        # Commit changes
        session.commit()
        return profile
    
    except Exception as e:
        logger.error(f"Error creating seasonal profile: {e}")
        session.rollback()
        return None

def apply_seasonal_profile(session, sku_id, store_id, profile_id):
    """
    Apply a seasonal profile to a SKU.
    
    Args:
        session: SQLAlchemy session
        sku_id (str): SKU ID
        store_id (str): Store ID
        profile_id (str): Profile ID
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Get SKU
        sku = session.query(SKU).filter(
            and_(SKU.sku_id == sku_id, SKU.store_id == store_id)
        ).first()
        
        if not sku:
            logger.error(f"SKU {sku_id} not found in store {store_id}")
            return False
        
        # Check if profile exists
        profile = session.query(SeasonalProfile).filter(
            SeasonalProfile.profile_id == profile_id
        ).first()
        
        if not profile:
            logger.error(f"Profile {profile_id} not found")
            return False
        
        # Apply profile to SKU
        sku.demand_profile_id = profile_id
        
        # Reforecast with seasonal profile
        # Get forecast data
        forecast_data = session.query(ForecastData).filter(
            and_(ForecastData.sku_id == sku_id, ForecastData.store_id == store_id)
        ).first()
        
        if forecast_data:
            # Get current period
            periodicity = getattr(sku, 'forecasting_periodicity', 13)  # Default to 13 (4-weekly)
            current_year, current_period = get_current_period(periodicity)
            
            # Get seasonal index for current period
            seasonal_index = get_seasonal_index(profile, current_period)
            
            # Adjust forecast for seasonality
            if seasonal_index != 0:
                new_period_forecast = forecast_data.period_forecast / seasonal_index
                
                # Update forecast data
                forecast_data.period_forecast = new_period_forecast
                
                # Update other forecast periods
                if periodicity == 13:  # 4-weekly
                    forecast_data.weekly_forecast = new_period_forecast / 4.0
                    forecast_data.quarterly_forecast = new_period_forecast * 3.0
                    forecast_data.yearly_forecast = new_period_forecast * 13.0
                else:  # Weekly
                    forecast_data.weekly_forecast = new_period_forecast
                    forecast_data.quarterly_forecast = new_period_forecast * 13.0
                    forecast_data.yearly_forecast = new_period_forecast * 52.0
        
        # Commit changes
        session.commit()
        return True
    
    except Exception as e:
        logger.error(f"Error applying seasonal profile: {e}")
        session.rollback()
        return False

def get_seasonal_index_for_period(profile_id, period_number):
    """
    Get seasonal index for a specific period.
    
    Args:
        profile_id (str): Profile ID
        period_number (int): Period number
    
    Returns:
        float: Seasonal index
    """
    session = get_session()
    try:
        # Get profile
        profile = session.query(SeasonalProfile).filter(
            SeasonalProfile.profile_id == profile_id
        ).first()
        
        if not profile:
            logger.error(f"Profile {profile_id} not found")
            return 1.0  # Default to 1.0 if profile not found
        
        # Get index for period
        return get_seasonal_index(profile, period_number)
    
    except Exception as e:
        logger.error(f"Error getting seasonal index: {e}")
        return 1.0  # Default to 1.0 on error
    
    finally:
        session.close()
        
def analyze_intermittent_demand_pattern(session, sku_id, store_id, years=3):
    """
    Analyze a SKU's demand pattern to determine if it's intermittent.
    
    Args:
        session: SQLAlchemy session
        sku_id (str): SKU ID
        store_id (str): Store ID
        years (int): Number of years of history to analyze
    
    Returns:
        dict: Analysis results including intermittence metrics
    """
    try:
        # Get demand history
        history_data = get_demand_history(session, sku_id, store_id, years)
        
        if not history_data:
            return {
                'is_intermittent': False,
                'reason': 'insufficient_history',
                'zero_demand_periods': 0,
                'total_periods': 0,
                'zero_demand_percentage': 0.0,
                'coefficient_of_variation': 0.0,
                'average_demand_interval': 0.0
            }
        
        # Count periods with zero demand
        zero_demand_periods = sum(1 for demand in history_data.values() if demand == 0)
        total_periods = len(history_data)
        
        # Calculate zero demand percentage
        zero_demand_percentage = (zero_demand_periods / total_periods) * 100 if total_periods > 0 else 0
        
        # Calculate demand values
        demand_values = list(history_data.values())
        
        # Calculate coefficient of variation (CV)
        mean_demand = sum(demand_values) / len(demand_values) if demand_values else 0
        
        if mean_demand > 0:
            variance = sum((d - mean_demand) ** 2 for d in demand_values) / len(demand_values)
            std_dev = math.sqrt(variance)
            coefficient_of_variation = std_dev / mean_demand
        else:
            coefficient_of_variation = 0.0
        
        # Calculate average demand interval
        if zero_demand_periods < total_periods:  # Avoid division by zero
            average_demand_interval = total_periods / (total_periods - zero_demand_periods)
        else:
            average_demand_interval = float('inf')  # All periods have zero demand
        
        # Determine if the pattern is intermittent
        # Standard classification criteria:
        # - High zero_demand_percentage (>30%)
        # - High coefficient_of_variation (>1.0)
        # - High average_demand_interval (>1.3)
        
        is_intermittent = (
            zero_demand_percentage >= 30.0 or 
            coefficient_of_variation >= 1.0 or 
            average_demand_interval >= 1.3
        )
        
        return {
            'is_intermittent': is_intermittent,
            'zero_demand_periods': zero_demand_periods,
            'total_periods': total_periods,
            'zero_demand_percentage': zero_demand_percentage,
            'coefficient_of_variation': coefficient_of_variation,
            'average_demand_interval': average_demand_interval,
            'mean_demand': mean_demand
        }
    
    except Exception as e:
        logger.error(f"Error analyzing intermittent demand pattern: {e}")
        return {
            'is_intermittent': False,
            'error': str(e)
        }

def recommend_forecast_method(session, sku_id, store_id):
    """
    Recommend the appropriate forecasting method for a SKU based on its demand pattern.
    
    Args:
        session: SQLAlchemy session
        sku_id (str): SKU ID
        store_id (str): Store ID
    
    Returns:
        dict: Recommendation including recommended method and analysis
    """
    try:
        # Analyze the demand pattern
        analysis = analyze_intermittent_demand_pattern(session, sku_id, store_id)
        
        # Get the current forecast method
        sku = session.query(SKU).filter(
            and_(SKU.sku_id == sku_id, SKU.store_id == store_id)
        ).first()
        
        if not sku:
            return {
                'recommended_method': 'unknown',
                'current_method': 'unknown',
                'reason': 'sku_not_found'
            }
        
        # Get current method
        current_method = getattr(sku, 'forecast_method', 'E3 Regular AVS')
        
        # Determine recommended method based on analysis
        recommended_method = current_method  # Default to keeping current method
        reason = 'no_change_needed'
        
        # Check if item has a seasonal profile
        has_seasonal_profile = sku.demand_profile_id is not None
        
        # Use enhanced AVS for intermittent patterns
        if analysis['is_intermittent']:
            recommended_method = 'E3 Enhanced AVS'
            reason = 'intermittent_demand'
        
        # If very slow moving (very low mean demand), recommend Enhanced AVS
        elif analysis.get('mean_demand', 0) < ASR_CONFIG.get('slow_mover_limit', 5):
            recommended_method = 'E3 Enhanced AVS'
            reason = 'slow_moving'
        
        # If item has a seasonal profile, check if there's enough consistent history
        elif has_seasonal_profile and analysis.get('zero_demand_percentage', 0) > 15:
            # May need to reconsider seasonal profile if many zero periods
            reason = 'seasonal_with_gaps'
        
        # If regular AVS is appropriate
        elif not analysis['is_intermittent'] and analysis.get('coefficient_of_variation', 0) < 0.5:
            recommended_method = 'E3 Regular AVS'
            reason = 'stable_demand'
        
        return {
            'recommended_method': recommended_method,
            'current_method': current_method,
            'reason': reason,
            'analysis': analysis,
            'has_seasonal_profile': has_seasonal_profile
        }
    
    except Exception as e:
        logger.error(f"Error recommending forecast method: {e}")
        return {
            'recommended_method': 'E3 Regular AVS',  # Default to regular
            'current_method': 'unknown',
            'reason': 'error',
            'error': str(e)
        }

def apply_recommended_forecast_methods(session, buyer_id=None, store_id=None, source_id=None, 
                                       auto_apply=False, threshold=0.9):
    """
    Analyze and recommend forecast methods for a group of SKUs.
    
    Args:
        session: SQLAlchemy session
        buyer_id (str): Filter by buyer ID
        store_id (str): Filter by store ID
        source_id (str): Filter by source ID
        auto_apply (bool): Whether to automatically apply recommendations
        threshold (float): Confidence threshold for auto-application (0.0-1.0)
    
    Returns:
        dict: Results of the analysis
    """
    try:
        # Build query for SKUs
        query = session.query(SKU)
        
        # Apply filters
        if buyer_id:
            query = query.join(SKU.source).filter(Source.buyer_id == buyer_id)
        
        if store_id:
            query = query.filter(SKU.store_id == store_id)
        
        if source_id:
            query = query.join(SKU.source).filter(Source.source_id == source_id)
        
        # Get active SKUs
        skus = query.filter(SKU.buyer_class.in_(['R', 'W'])).all()
        
        # Results
        results = {
            'total_skus': len(skus),
            'analyzed': 0,
            'recommended_changes': 0,
            'auto_applied': 0,
            'errors': 0,
            'recommendations': []
        }
        
        # Process each SKU
        for sku in skus:
            try:
                # Get recommendation
                recommendation = recommend_forecast_method(session, sku.sku_id, sku.store_id)
                results['analyzed'] += 1
                
                # Check if recommendation differs from current
                if recommendation['recommended_method'] != recommendation['current_method']:
                    results['recommended_changes'] += 1
                    
                    # Determine confidence - this is a simplified example
                    # In a real implementation, this would be based on multiple factors
                    confidence = 0.0
                    
                    if recommendation['reason'] == 'intermittent_demand':
                        # Calculate confidence based on intermittence metrics
                        analysis = recommendation['analysis']
                        
                        if analysis['zero_demand_percentage'] > 50:
                            confidence = 0.9
                        elif analysis['coefficient_of_variation'] > 1.5:
                            confidence = 0.85
                        elif analysis['average_demand_interval'] > 2.0:
                            confidence = 0.8
                        else:
                            confidence = 0.7
                    
                    elif recommendation['reason'] == 'slow_moving':
                        confidence = 0.85
                    
                    elif recommendation['reason'] == 'stable_demand':
                        confidence = 0.75
                    
                    # Add to recommendations list
                    recommendation_record = {
                        'sku_id': sku.sku_id,
                        'store_id': sku.store_id,
                        'current_method': recommendation['current_method'],
                        'recommended_method': recommendation['recommended_method'],
                        'reason': recommendation['reason'],
                        'confidence': confidence,
                        'applied': False
                    }
                    
                    # Auto-apply if enabled and confidence is high enough
                    if auto_apply and confidence >= threshold:
                        try:
                            # Update forecast method
                            sku.forecast_method = recommendation['recommended_method']
                            recommendation_record['applied'] = True
                            results['auto_applied'] += 1
                        except Exception as e:
                            logger.error(f"Error applying forecast method: {e}")
                            recommendation_record['error'] = str(e)
                    
                    results['recommendations'].append(recommendation_record)
            
            except Exception as e:
                logger.error(f"Error analyzing SKU {sku.sku_id}: {e}")
                results['errors'] += 1
        
        # Commit changes if auto-apply is enabled
        if auto_apply and results['auto_applied'] > 0:
            session.commit()
        
        return results
    
    except Exception as e:
        logger.error(f"Error applying recommended forecast methods: {e}")
        if auto_apply:
            session.rollback()
        return {'error': str(e)}