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

def calculate_e3_enhanced_avs_forecast(session, sku_id, store_id):
    """
    Calculate a new forecast using the E3 Enhanced AVS method for slow-moving items.
    
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
        
        # Get demand limit from SKU or source or company settings
        sku = session.query(SKU).filter(
            and_(SKU.sku_id == sku_id, SKU.store_id == store_id)
        ).first()
        
        # Get the demand limit from configuration
        forecasting_demand_limit = ASR_CONFIG.get('forecasting_demand_limit', 0.0)
        
        # Check if demand exceeds the limit
        if prev_demand <= forecasting_demand_limit:
            # Do not update the forecast if demand is below limit
            # Return current forecast values
            return {
                'period_forecast': forecast_data.period_forecast,
                'weekly_forecast': forecast_data.weekly_forecast,
                'quarterly_forecast': forecast_data.quarterly_forecast,
                'yearly_forecast': forecast_data.yearly_forecast,
                'madp': forecast_data.madp,
                'track': forecast_data.track
            }
        
        # Get seasonal profile
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
        
        # Find time since last demand occurrence
        periods_since_last_demand = 1  # Default to 1 period
        
        # Sort history by recent to older
        sorted_periods = sorted(history_data.keys(), reverse=True)
        
        # Find the most recent period with demand > limit
        for i, (year, period) in enumerate(sorted_periods):
            if i == 0:  # Skip current period
                continue
                
            if history_data.get((year, period), 0.0) > forecasting_demand_limit:
                # Calculate periods since last demand
                if year == prev_year:
                    periods_since_last_demand = prev_period - period
                else:
                    periods_since_last_demand = prev_period + (periodicity * (prev_year - year)) - period
                break
        
        # Get track (trend percentage)
        track = forecast_data.track or 0.0
        
        # Adjust track based on time since last demand
        adjusted_track = track / periods_since_last_demand
        
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