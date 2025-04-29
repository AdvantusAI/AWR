"""
Seasonal Profile Creation for the ASR system.

This module provides enhanced functionality for creating and managing seasonal
profiles for SKUs with predictable seasonal demand patterns.
"""
import logging
import numpy as np
import math
from datetime import datetime
from sqlalchemy import and_, func, or_
import uuid

from models.sku import SKU, ForecastData, DemandHistory, SeasonalProfile
from utils.helpers import get_current_period, get_seasonal_index
from utils.db import get_session
from config.settings import ASR_CONFIG

logger = logging.getLogger(__name__)

def get_demand_history_for_profile(session, sku_id, store_id, years=3):
    """
    Get SKU demand history formatted for seasonal profile analysis.
    
    Args:
        session: SQLAlchemy session
        sku_id (str): SKU ID
        store_id (str): Store ID
        years (int): Number of years of history to retrieve
    
    Returns:
        dict: History data organized by year and period
    """
    try:
        # Get SKU
        sku = session.query(SKU).filter(
            and_(SKU.sku_id == sku_id, SKU.store_id == store_id)
        ).first()
        
        if not sku:
            logger.error(f"SKU {sku_id} not found in store {store_id}")
            return {}
        
        # Determine periodicity
        periodicity = getattr(sku, 'forecasting_periodicity', 13)  # Default to 13 (4-weekly)
        
        # Get current period
        current_year, current_period = get_current_period(periodicity)
        
        # Get demand history for all periods in requested years
        history_records = session.query(DemandHistory).filter(
            and_(
                DemandHistory.sku_id == sku_id,
                DemandHistory.store_id == store_id,
                DemandHistory.period_year >= current_year - years,
                ~DemandHistory.ignore_history  # Don't include ignored history
            )
        ).all()
        
        # Organize by year and period
        history_data = {}
        for record in history_records:
            year = record.period_year
            period = record.period_number
            
            if year not in history_data:
                history_data[year] = {}
            
            history_data[year][period] = {
                'total_demand': record.total_demand,
                'units_sold': record.units_sold,
                'units_lost': record.units_lost,
                'promotional_demand': record.promotional_demand
            }
        
        return history_data
    
    except Exception as e:
        logger.error(f"Error getting demand history for profile: {e}")
        return {}

def detect_seasonality(session, sku_id, store_id, years=3):
    """
    Detect if a SKU has a seasonal demand pattern.
    
    Args:
        session: SQLAlchemy session
        sku_id (str): SKU ID
        store_id (str): Store ID
        years (int): Number of years of history to analyze
    
    Returns:
        dict: Seasonality analysis results
    """
    try:
        # Get history data
        history_data = get_demand_history_for_profile(session, sku_id, store_id, years)
        
        if not history_data or len(history_data) < 2:
            return {
                'is_seasonal': False,
                'reason': 'insufficient_history',
                'confidence': 0.0,
                'years_analyzed': len(history_data)
            }
        
        # Get SKU
        sku = session.query(SKU).filter(
            and_(SKU.sku_id == sku_id, SKU.store_id == store_id)
        ).first()
        
        # Determine periodicity
        periodicity = getattr(sku, 'forecasting_periodicity', 13)  # Default to 13 (4-weekly)
        
        # Extract total demand by period for each year
        period_demands = {}
        for year, periods in history_data.items():
            for period, data in periods.items():
                if period not in period_demands:
                    period_demands[period] = []
                
                period_demands[period].append(data['total_demand'])
        
        # Calculate statistics for each period
        period_stats = {}
        all_demands = []
        
        for period, demands in period_demands.items():
            if len(demands) >= 2:  # Need at least 2 years of data
                avg_demand = sum(demands) / len(demands)
                all_demands.extend(demands)
                
                # Calculate variance
                variance = sum((d - avg_demand) ** 2 for d in demands) / len(demands)
                std_dev = math.sqrt(variance)
                
                period_stats[period] = {
                    'avg_demand': avg_demand,
                    'std_dev': std_dev,
                    'coefficient_of_variation': std_dev / avg_demand if avg_demand > 0 else 0,
                    'years': len(demands)
                }
        
        if not period_stats:
            return {
                'is_seasonal': False,
                'reason': 'insufficient_period_data',
                'confidence': 0.0,
                'years_analyzed': len(history_data)
            }
        
        # Calculate overall statistics
        overall_avg = sum(all_demands) / len(all_demands) if all_demands else 0
        
        # Calculate seasonal indices
        seasonal_indices = {}
        for period, stats in period_stats.items():
            seasonal_indices[period] = stats['avg_demand'] / overall_avg if overall_avg > 0 else 1.0
        
        # Analyze seasonality
        max_index = max(seasonal_indices.values())
        min_index = min(seasonal_indices.values())
        seasonal_range = max_index - min_index
        
        # Determine if pattern is seasonal based on range and consistency
        is_seasonal = seasonal_range >= 0.3  # At least 30% difference between high and low seasons
        
        # Calculate confidence based on consistency across years
        avg_cv = sum(stats['coefficient_of_variation'] for stats in period_stats.values()) / len(period_stats)
        
        # Higher confidence with lower coefficient of variation (more consistent)
        # and larger seasonal range
        confidence = min(1.0, (seasonal_range * 1.5) * (1.0 - min(avg_cv, 0.5)))
        
        return {
            'is_seasonal': is_seasonal,
            'seasonal_range': seasonal_range,
            'seasonal_indices': seasonal_indices,
            'max_period': max(seasonal_indices, key=seasonal_indices.get),
            'min_period': min(seasonal_indices, key=seasonal_indices.get),
            'confidence': confidence,
            'years_analyzed': len(history_data),
            'avg_coefficient_of_variation': avg_cv
        }
    
    except Exception as e:
        logger.error(f"Error detecting seasonality: {e}")
        return {
            'is_seasonal': False,
            'error': str(e),
            'confidence': 0.0
        }

def calculate_composite_line(session, sku_id, store_id, years=3, ignore_years=None):
    """
    Calculate composite demand line for seasonal profile creation.
    
    The composite line represents the weighted average of demand history 
    across years, with more recent years given higher weight.
    
    Args:
        session: SQLAlchemy session
        sku_id (str): SKU ID
        store_id (str): Store ID
        years (int): Number of years of history to include
        ignore_years (list): List of years to ignore
    
    Returns:
        dict: Composite demand values by period
    """
    try:
        # Get history data
        history_data = get_demand_history_for_profile(session, sku_id, store_id, years)
        
        if not history_data:
            return {}
        
        # Filter out ignored years
        if ignore_years:
            history_data = {year: periods for year, periods in history_data.items() 
                          if year not in ignore_years}
        
        if not history_data:
            return {}
        
        # Get SKU
        sku = session.query(SKU).filter(
            and_(SKU.sku_id == sku_id, SKU.store_id == store_id)
        ).first()
        
        # Determine periodicity
        periodicity = getattr(sku, 'forecasting_periodicity', 13)  # Default to 13 (4-weekly)
        
        # Get current period
        current_year, current_period = get_current_period(periodicity)
        
        # Calculate weights for each year (more recent years have higher weight)
        years_list = sorted(history_data.keys())
        max_year = max(years_list)
        
        year_weights = {}
        for year in years_list:
            # Weight decreases by 30% for each year back
            year_weights[year] = 0.7 ** (max_year - year)
        
        # Normalize weights to sum to 1
        weight_sum = sum(year_weights.values())
        if weight_sum > 0:
            for year in year_weights:
                year_weights[year] /= weight_sum
        
        # Calculate weighted average for each period
        composite_line = {}
        
        for period in range(1, periodicity + 1):
            period_values = []
            period_weights = []
            
            for year in years_list:
                if period in history_data[year]:
                    period_values.append(history_data[year][period]['total_demand'])
                    period_weights.append(year_weights[year])
            
            if period_values:
                # Calculate weighted average
                weighted_sum = sum(value * weight for value, weight in zip(period_values, period_weights))
                weight_sum = sum(period_weights)
                
                if weight_sum > 0:
                    composite_line[period] = weighted_sum / weight_sum
                else:
                    composite_line[period] = 0.0
            else:
                composite_line[period] = 0.0
        
        return composite_line
    
    except Exception as e:
        logger.error(f"Error calculating composite line: {e}")
        return {}

def generate_seasonal_indices(composite_line):
    """
    Generate seasonal indices from a composite demand line.
    
    Args:
        composite_line (dict): Composite demand values by period
    
    Returns:
        dict: Seasonal indices by period
    """
    if not composite_line:
        return {}
    
    # Calculate overall average
    overall_avg = sum(composite_line.values()) / len(composite_line)
    
    if overall_avg == 0:
        return {period: 1.0 for period in composite_line}
    
    # Calculate indices
    indices = {period: (demand / overall_avg) for period, demand in composite_line.items()}
    
    # Check if indices are valid (non-zero)
    has_zeros = any(index == 0 for index in indices.values())
    
    if has_zeros:
        # Replace zeros with small values (10% of average)
        min_value = 0.1
        indices = {period: max(index, min_value) for period, index in indices.items()}
        
        # Re-normalize to ensure average is 1.0
        avg_index = sum(indices.values()) / len(indices)
        indices = {period: (index / avg_index) for period, index in indices.items()}
    
    return indices

def smooth_seasonal_indices(indices, smoothing_factor=0.3):
    """
    Apply smoothing to seasonal indices to reduce noise.
    
    Args:
        indices (dict): Seasonal indices by period
        smoothing_factor (float): Smoothing factor (0.0-1.0)
    
    Returns:
        dict: Smoothed seasonal indices
    """
    if not indices:
        return {}
    
    periods = sorted(indices.keys())
    num_periods = len(periods)
    
    # Create a copy of indices for smoothing
    smoothed_indices = {}
    
    for i, period in enumerate(periods):
        # Get values for adjacent periods (wrapping around if necessary)
        prev_period = periods[(i - 1) % num_periods]
        next_period = periods[(i + 1) % num_periods]
        
        # Calculate smoothed value
        current_value = indices[period]
        prev_value = indices[prev_period]
        next_value = indices[next_period]
        
        # Weighted average with adjacent periods
        smoothed_value = (
            (1 - smoothing_factor) * current_value + 
            (smoothing_factor / 2) * prev_value + 
            (smoothing_factor / 2) * next_value
        )
        
        smoothed_indices[period] = smoothed_value
    
    # Ensure that average is still 1.0
    avg_index = sum(smoothed_indices.values()) / len(smoothed_indices)
    normalized_indices = {period: (index / avg_index) for period, index in smoothed_indices.items()}
    
    return normalized_indices

def create_seasonal_profile(session, sku_id, store_id, profile_id=None, description=None, 
                           years=3, ignore_years=None, smoothing_factor=0.3):
    """
    Create a seasonal profile for a SKU.
    
    Args:
        session: SQLAlchemy session
        sku_id (str): SKU ID for reference SKU
        store_id (str): Store ID for reference SKU
        profile_id (str): Optional profile ID (generated if None)
        description (str): Optional profile description
        years (int): Number of years of history to include
        ignore_years (list): List of years to ignore
        smoothing_factor (float): Smoothing factor for indices (0.0-1.0)
    
    Returns:
        dict: Result of profile creation
    """
    try:
        # Calculate composite line
        composite_line = calculate_composite_line(session, sku_id, store_id, years, ignore_years)
        
        if not composite_line:
            return {
                'success': False,
                'message': 'Insufficient history data for profile creation',
                'profile_id': None
            }
        
        # Generate indices
        indices = generate_seasonal_indices(composite_line)
        
        # Apply smoothing if requested
        if smoothing_factor > 0:
            indices = smooth_seasonal_indices(indices, smoothing_factor)
        
        # Generate profile ID if not provided
        if not profile_id:
            profile_id = f"SP{uuid.uuid4().hex[:8].upper()}"
        
        # Generate description if not provided
        if not description:
            sku = session.query(SKU).filter(
                and_(SKU.sku_id == sku_id, SKU.store_id == store_id)
            ).first()
            
            if sku:
                description = f"Profile for {sku.name} - Created {datetime.now().strftime('%Y-%m-%d')}"
            else:
                description = f"Profile created on {datetime.now().strftime('%Y-%m-%d')}"
        
        # Check if profile already exists
        existing_profile = session.query(SeasonalProfile).filter(
            SeasonalProfile.profile_id == profile_id
        ).first()
        
        if existing_profile:
            # Update existing profile
            existing_profile.description = description
            
            # Update indices
            for period, index in indices.items():
                field_name = f"p{period}_index"
                setattr(existing_profile, field_name, index)
            
            profile = existing_profile
            is_new = False
        else:
            # Create new profile
            profile = SeasonalProfile(
                profile_id=profile_id,
                description=description
            )
            
            # Set indices
            for period, index in indices.items():
                field_name = f"p{period}_index"
                setattr(profile, field_name, index)
            
            session.add(profile)
            is_new = True
        
        # Commit changes
        session.commit()
        
        return {
            'success': True,
            'message': 'Profile created successfully' if is_new else 'Profile updated successfully',
            'profile_id': profile_id,
            'is_new': is_new,
            'indices': indices,
            'composite_line': composite_line
        }
    
    except Exception as e:
        logger.error(f"Error creating seasonal profile: {e}")
        session.rollback()
        return {
            'success': False,
            'message': f"Error creating profile: {str(e)}",
            'error': str(e),
            'profile_id': None
        }

def apply_profile_to_sku(session, sku_id, store_id, profile_id, reforecast=True):
    """
    Apply a seasonal profile to a SKU.
    
    Args:
        session: SQLAlchemy session
        sku_id (str): SKU ID
        store_id (str): Store ID
        profile_id (str): Profile ID to apply
        reforecast (bool): Whether to reforecast item after applying profile
    
    Returns:
        dict: Result of profile application
    """
    try:
        # Get the SKU
        sku = session.query(SKU).filter(
            and_(SKU.sku_id == sku_id, SKU.store_id == store_id)
        ).first()
        
        if not sku:
            return {
                'success': False,
                'message': f"SKU {sku_id} not found in store {store_id}"
            }
        
        # Check if profile exists
        profile = session.query(SeasonalProfile).filter(
            SeasonalProfile.profile_id == profile_id
        ).first()
        
        if not profile:
            return {
                'success': False,
                'message': f"Profile {profile_id} not found"
            }
        
        # Get current period
        periodicity = getattr(sku, 'forecasting_periodicity', 13)
        current_year, current_period = get_current_period(periodicity)
        
        # Get seasonal index for current period
        seasonal_index = get_seasonal_index(profile, current_period)
        
        # Store old profile ID for reporting
        old_profile_id = sku.demand_profile_id
        
        # Apply profile to SKU
        sku.demand_profile_id = profile_id
        
        # If reforecast requested, adjust forecast for seasonality
        if reforecast:
            # Get forecast data
            forecast_data = session.query(ForecastData).filter(
                and_(ForecastData.sku_id == sku_id, ForecastData.store_id == store_id)
            ).first()
            
            if forecast_data:
                if seasonal_index > 0:
                    # If changing from one profile to another, first de-seasonalize 
                    # with old profile, then re-seasonalize with new profile
                    if old_profile_id:
                        old_profile = session.query(SeasonalProfile).filter(
                            SeasonalProfile.profile_id == old_profile_id
                        ).first()
                        
                        if old_profile:
                            old_index = get_seasonal_index(old_profile, current_period)
                            
                            if old_index > 0:
                                # De-seasonalize with old profile
                                base_forecast = forecast_data.period_forecast / old_index
                                
                                # Re-seasonalize with new profile
                                new_forecast = base_forecast * seasonal_index
                                forecast_data.period_forecast = new_forecast
                    else:
                        # No previous profile, just adjust forecast with new profile
                        base_forecast = forecast_data.period_forecast / seasonal_index
                        forecast_data.period_forecast = base_forecast
                    
                    # Update weekly, quarterly, and yearly forecasts
                    if periodicity == 13:  # 4-weekly
                        forecast_data.weekly_forecast = forecast_data.period_forecast / 4.0
                        forecast_data.quarterly_forecast = forecast_data.period_forecast * 3.0
                        forecast_data.yearly_forecast = forecast_data.period_forecast * 13.0
                    else:  # Weekly
                        forecast_data.weekly_forecast = forecast_data.period_forecast
                        forecast_data.quarterly_forecast = forecast_data.period_forecast * 13.0
                        forecast_data.yearly_forecast = forecast_data.period_forecast * 52.0
        
        # Commit changes
        session.commit()
        
        return {
            'success': True,
            'message': f"Profile {profile_id} applied to SKU {sku_id}",
            'old_profile_id': old_profile_id,
            'reforecasted': reforecast
        }
    
    except Exception as e:
        logger.error(f"Error applying profile to SKU: {e}")
        session.rollback()
        return {
            'success': False,
            'message': f"Error applying profile: {str(e)}",
            'error': str(e)
        }

def find_similar_skus(session, sku_id, store_id, min_similarity=0.7):
    """
    Find SKUs with similar demand patterns to a reference SKU.
    
    Args:
        session: SQLAlchemy session
        sku_id (str): Reference SKU ID
        store_id (str): Reference store ID
        min_similarity (float): Minimum similarity threshold (0.0-1.0)
    
    Returns:
        dict: List of similar SKUs with similarity scores
    """
    try:
        # Get reference SKU
        ref_sku = session.query(SKU).filter(
            and_(SKU.sku_id == sku_id, SKU.store_id == store_id)
        ).first()
        
        if not ref_sku:
            return {
                'success': False,
                'message': f"Reference SKU {sku_id} not found in store {store_id}"
            }
        
        # Get reference SKU demand history
        ref_history = get_demand_history_for_profile(session, sku_id, store_id)
        
        if not ref_history:
            return {
                'success': False,
                'message': "Insufficient history for reference SKU"
            }
        
        # Calculate composite line for reference SKU
        ref_composite = calculate_composite_line(session, sku_id, store_id)
        
        if not ref_composite:
            return {
                'success': False,
                'message': "Could not calculate reference composite line"
            }
        
        # Get candidate SKUs (same source, same store, active)
        candidates = session.query(SKU).filter(
            and_(
                SKU.source_id == ref_sku.source_id,
                SKU.store_id == store_id,
                SKU.buyer_class.in_(['R', 'W']),
                SKU.sku_id != sku_id  # Exclude reference SKU
            )
        ).all()
        
        # Results list
        similar_skus = []
        
        # Process each candidate
        for candidate in candidates:
            try:
                # Calculate composite line
                candidate_composite = calculate_composite_line(session, candidate.sku_id, store_id)
                
                if not candidate_composite:
                    continue
                
                # Calculate similarity score
                similarity = calculate_pattern_similarity(ref_composite, candidate_composite)
                
                # Check if meets threshold
                if similarity >= min_similarity:
                    similar_skus.append({
                        'sku_id': candidate.sku_id,
                        'name': candidate.name,
                        'similarity': similarity,
                        'has_profile': candidate.demand_profile_id is not None,
                        'profile_id': candidate.demand_profile_id
                    })
            except Exception as e:
                logger.warning(f"Error processing candidate SKU {candidate.sku_id}: {e}")
                continue
        
        # Sort by similarity (highest first)
        similar_skus.sort(key=lambda x: x['similarity'], reverse=True)
        
        return {
            'success': True,
            'reference_sku_id': sku_id,
            'similar_skus': similar_skus,
            'count': len(similar_skus)
        }
    
    except Exception as e:
        logger.error(f"Error finding similar SKUs: {e}")
        return {
            'success': False,
            'message': f"Error finding similar SKUs: {str(e)}",
            'error': str(e)
        }

def calculate_pattern_similarity(pattern1, pattern2):
    """
    Calculate similarity between two demand patterns.
    
    Uses correlation coefficient as a measure of similarity.
    
    Args:
        pattern1 (dict): First pattern (period -> demand)
        pattern2 (dict): Second pattern (period -> demand)
    
    Returns:
        float: Similarity score (0.0-1.0)
    """
    # Ensure both patterns have the same periods
    all_periods = sorted(set(pattern1.keys()) | set(pattern2.keys()))
    
    # Convert to arrays with consistent periods
    values1 = [pattern1.get(period, 0) for period in all_periods]
    values2 = [pattern2.get(period, 0) for period in all_periods]
    
    # Calculate correlation coefficient
    # First calculate means
    mean1 = sum(values1) / len(values1)
    mean2 = sum(values2) / len(values2)
    
    # Calculate numerator (covariance)
    numerator = sum((x - mean1) * (y - mean2) for x, y in zip(values1, values2))
    
    # Calculate denominator (standard deviations)
    std_dev1 = math.sqrt(sum((x - mean1) ** 2 for x in values1))
    std_dev2 = math.sqrt(sum((y - mean2) ** 2 for y in values2))
    
    denominator = std_dev1 * std_dev2
    
    # Calculate correlation
    if denominator == 0:
        return 0.0
    
    correlation = numerator / denominator
    
    # Convert to similarity score (0.0-1.0)
    # Correlation can be -1 to 1, but we're primarily interested in positive correlation
    similarity = max(0, correlation)
    
    return similarity

def apply_profile_to_similar_skus(session, ref_sku_id, store_id, profile_id, 
                                 min_similarity=0.8, auto_apply=False):
    """
    Find SKUs with similar demand patterns and apply the same profile.
    
    Args:
        session: SQLAlchemy session
        ref_sku_id (str): Reference SKU ID
        store_id (str): Store ID
        profile_id (str): Profile ID to apply
        min_similarity (float): Minimum similarity threshold
        auto_apply (bool): Whether to automatically apply the profile
    
    Returns:
        dict: Results of the operation
    """
    try:
        # Find similar SKUs
        similar_result = find_similar_skus(session, ref_sku_id, store_id, min_similarity)
        
        if not similar_result['success']:
            return similar_result
        
        similar_skus = similar_result['similar_skus']
        
        # Check if profile exists
        profile = session.query(SeasonalProfile).filter(
            SeasonalProfile.profile_id == profile_id
        ).first()
        
        if not profile:
            return {
                'success': False,
                'message': f"Profile {profile_id} not found"
            }
        
        # Results tracking
        results = {
            'success': True,
            'candidates': len(similar_skus),
            'applied': 0,
            'skipped': 0,
            'failed': 0,
            'details': []
        }
        
        # Process each similar SKU
        for similar in similar_skus:
            sku_id = similar['sku_id']
            
            # Skip if already has the same profile
            if similar['profile_id'] == profile_id:
                results['skipped'] += 1
                results['details'].append({
                    'sku_id': sku_id,
                    'status': 'skipped',
                    'reason': 'already_has_profile',
                    'similarity': similar['similarity']
                })
                continue
            
            # Apply profile if auto_apply is enabled
            if auto_apply:
                apply_result = apply_profile_to_sku(session, sku_id, store_id, profile_id)
                
                if apply_result['success']:
                    results['applied'] += 1
                    results['details'].append({
                        'sku_id': sku_id,
                        'status': 'applied',
                        'similarity': similar['similarity'],
                        'old_profile_id': apply_result.get('old_profile_id')
                    })
                else:
                    results['failed'] += 1
                    results['details'].append({
                        'sku_id': sku_id,
                        'status': 'failed',
                        'reason': apply_result.get('message', 'unknown_error'),
                        'similarity': similar['similarity']
                    })
            else:
                # Just add to candidates list
                results['details'].append({
                    'sku_id': sku_id,
                    'status': 'candidate',
                    'similarity': similar['similarity'],
                    'current_profile_id': similar['profile_id']
                })
        
        return results
    
    except Exception as e:
        logger.error(f"Error applying profile to similar SKUs: {e}")
        if auto_apply:
            session.rollback()
        return {
            'success': False,
            'message': f"Error applying profile to similar SKUs: {str(e)}",
            'error': str(e)
        }

def get_profile_info(session, profile_id):
    """
    Get detailed information about a seasonal profile.
    
    Args:
        session: SQLAlchemy session
        profile_id (str): Profile ID
    
    Returns:
        dict: Profile information
    """
    try:
        # Get the profile
        profile = session.query(SeasonalProfile).filter(
            SeasonalProfile.profile_id == profile_id
        ).first()
        
        if not profile:
            return {
                'success': False,
                'message': f"Profile {profile_id} not found"
            }
        
        # Extract indices
        indices = {}
        for period in range(1, 14):  # Assuming max 13 periods
            field_name = f"p{period}_index"
            if hasattr(profile, field_name):
                index_value = getattr(profile, field_name)
                if index_value is not None:
                    indices[period] = index_value
        
        # Get SKUs using this profile
        skus_count = session.query(func.count(SKU.id)).filter(
            SKU.demand_profile_id == profile_id
        ).scalar()
        
        # Calculate profile statistics
        max_index = max(indices.values()) if indices else 1.0
        min_index = min(indices.values()) if indices else 1.0
        avg_index = sum(indices.values()) / len(indices) if indices else 1.0
        seasonal_range = max_index - min_index
        
        return {
            'success': True,
            'profile_id': profile_id,
            'description': profile.description,
            'indices': indices,
            'statistics': {
                'max_index': max_index,
                'min_index': min_index,
                'avg_index': avg_index,
                'seasonal_range': seasonal_range,
                'max_period': max(indices, key=indices.get) if indices else None,
                'min_period': min(indices, key=indices.get) if indices else None
            },
            'skus_count': skus_count,
            'created_at': profile.created_at,
            'updated_at': profile.updated_at
        }
    
    except Exception as e:
        logger.error(f"Error getting profile info: {e}")
        return {
            'success': False,
            'message': f"Error getting profile info: {str(e)}",
            'error': str(e)
        }

def list_seasonal_profiles(session, filter_criteria=None):
    """
    List available seasonal profiles.
    
    Args:
        session: SQLAlchemy session
        filter_criteria (dict): Optional filtering criteria
    
    Returns:
        dict: List of profiles
    """
    try:
        # Build query
        query = session.query(SeasonalProfile)
        
        # Apply filters if provided
        if filter_criteria:
            if 'profile_id' in filter_criteria:
                query = query.filter(SeasonalProfile.profile_id.like(f"%{filter_criteria['profile_id']}%"))
            
            if 'description' in filter_criteria:
                query = query.filter(SeasonalProfile.description.like(f"%{filter_criteria['description']}%"))
        
        # Execute query
        profiles = query.all()
        
        # Format results
        profile_list = []
        
        for profile in profiles:
            # Extract indices
            indices = {}
            for period in range(1, 14):  # Assuming max 13 periods
                field_name = f"p{period}_index"
                if hasattr(profile, field_name):
                    index_value = getattr(profile, field_name)
                    if index_value is not None:
                        indices[period] = index_value
            
            # Calculate statistics
            max_index = max(indices.values()) if indices else 1.0
            min_index = min(indices.values()) if indices else 1.0
            seasonal_range = max_index - min_index
            
            # Get SKUs using this profile
            skus_count = session.query(func.count(SKU.id)).filter(
                SKU.demand_profile_id == profile.profile_id
            ).scalar()
            
            profile_list.append({
                'profile_id': profile.profile_id,
                'description': profile.description,
                'seasonal_range': seasonal_range,
                'skus_count': skus_count,
                'created_at': profile.created_at,
                'updated_at': profile.updated_at
            })
        
        return {
            'success': True,
            'profiles': profile_list,
            'count': len(profile_list)
        }
    
    except Exception as e:
        logger.error(f"Error listing seasonal profiles: {e}")
        return {
            'success': False,
            'message': f"Error listing profiles: {str(e)}",
            'error': str(e)
        }

def find_skus_needing_profiles(session, buyer_id=None, store_id=None, source_id=None, 
                              min_confidence=0.7, limit=100):
    """
    Find SKUs that appear to need seasonal profiles based on demand patterns.
    
    Args:
        session: SQLAlchemy session
        buyer_id (str): Filter by buyer ID
        store_id (str): Filter by store ID
        source_id (str): Filter by source ID
        min_confidence (float): Minimum confidence threshold
        limit (int): Maximum number of SKUs to return
    
    Returns:
        dict: List of SKUs needing profiles
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
        
        # Only include active SKUs without existing profiles
        query = query.filter(
            and_(
                SKU.buyer_class.in_(['R', 'W']),
                or_(SKU.demand_profile_id == None, SKU.demand_profile_id == '')
            )
        )
        
        # Get SKUs
        skus = query.limit(limit * 2).all()  # Get more than needed for filtering
        
        # Results list
        candidates = []
        
        # Process each SKU
        for sku in skus:
            try:
                # Detect seasonality
                seasonality = detect_seasonality(session, sku.sku_id, sku.store_id)
                
                # Check if meets criteria
                if seasonality.get('is_seasonal', False) and seasonality.get('confidence', 0) >= min_confidence:
                    candidates.append({
                        'sku_id': sku.sku_id,
                        'name': sku.name,
                        'store_id': sku.store_id,
                        'source_id': sku.source.source_id if sku.source else None,
                        'confidence': seasonality.get('confidence', 0),
                        'seasonal_range': seasonality.get('seasonal_range', 0),
                        'years_analyzed': seasonality.get('years_analyzed', 0),
                        'max_period': seasonality.get('max_period'),
                        'min_period': seasonality.get('min_period')
                    })
                    
                    # Break if we've found enough
                    if len(candidates) >= limit:
                        break
            except Exception as e:
                logger.warning(f"Error analyzing SKU {sku.sku_id}: {e}")
                continue
        
        # Sort by confidence (highest first)
        candidates.sort(key=lambda x: x['confidence'], reverse=True)
        
        return {
            'success': True,
            'candidates': candidates,
            'count': len(candidates)
        }
    
    except Exception as e:
        logger.error(f"Error finding SKUs needing profiles: {e}")
        return {
            'success': False,
            'message': f"Error finding SKUs: {str(e)}",
            'error': str(e)
        }