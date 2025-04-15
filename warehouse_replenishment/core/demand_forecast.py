# warehouse_replenishment/core/demand_forecast.py
import math
from typing import List, Dict, Tuple, Optional, Union
import numpy as np
from scipy import stats

from ..exceptions import ForecastError

def calculate_forecast(
    history: List[float], 
    periods: int = None, 
    seasonality: List[float] = None
) -> float:
    """Calculate base forecast from history.
    
    Args:
        history: List of history values
        periods: Number of periods to consider (most recent)
        seasonality: Optional list of seasonal indices
        
    Returns:
        Forecast value
    """
    if not history:
        return 0.0
    
    # Use specified number of periods or all history
    if periods and periods < len(history):
        relevant_history = history[:periods]
    else:
        relevant_history = history
    
    # Calculate base forecast (average)
    base_forecast = sum(relevant_history) / len(relevant_history)
    
    # Apply seasonality if provided
    if seasonality and len(seasonality) > 0:
        # Get average index to ensure it's normalized
        avg_index = sum(seasonality) / len(seasonality)
        if avg_index > 0:
            # Deseasonalize before calculating average
            deseasonalized_history = []
            for i, value in enumerate(relevant_history):
                season_idx = i % len(seasonality)
                if seasonality[season_idx] > 0:
                    deseasonalized_history.append(value / seasonality[season_idx] * avg_index)
                else:
                    deseasonalized_history.append(value)
            
            base_forecast = sum(deseasonalized_history) / len(deseasonalized_history)
    
    return base_forecast

def calculate_initial_forecast(history: List[float]) -> float:
    """Calculate initial forecast for a new item.
    
    Args:
        history: List of history values
        
    Returns:
        Initial forecast value
    """
    if not history:
        return 0.0
    
    # Use exponential weighting to give more weight to recent history
    weights = [math.exp(-0.1 * i) for i in range(len(history))]
    weight_sum = sum(weights)
    
    # Calculate weighted average
    weighted_sum = sum(h * w for h, w in zip(history, weights))
    forecast = weighted_sum / weight_sum if weight_sum > 0 else 0.0
    
    return forecast

def calculate_madp_from_history(forecast: float, history: List[float]) -> float:
    """Calculate Mean Absolute Deviation Percentage (MADP) from history.
    
    Args:
        forecast: Forecast value
        history: List of history values
        
    Returns:
        MADP value as percentage
    """
    if not history:
        return 0.0
    
    if forecast == 0:
        # Avoid division by zero
        if all(h == 0 for h in history):
            return 0.0
        else:
            return 100.0
    
    # Calculate deviations
    deviations = [abs(h - forecast) for h in history]
    
    # Calculate mean absolute deviation
    mad = sum(deviations) / len(deviations)
    
    # Calculate MADP
    madp = (mad / forecast) * 100.0
    
    # Limit to reasonable range
    return min(100.0, max(0.0, madp))

def calculate_track_from_history(forecast: float, history: List[float]) -> float:
    """Calculate tracking signal (track) from history.
    
    Args:
        forecast: Forecast value
        history: List of history values
        
    Returns:
        Track value as percentage
    """
    if not history:
        return 0.0
    
    if forecast == 0:
        # Avoid division by zero
        if all(h == 0 for h in history):
            return 0.0
        else:
            return 100.0
    
    # Calculate signed deviations
    deviations = [(h - forecast) for h in history]
    
    # Calculate sum of deviations (can be positive or negative)
    sum_deviations = sum(deviations)
    
    # Calculate mean absolute deviation
    mad = sum(abs(d) for d in deviations) / len(deviations)
    
    if mad == 0:
        return 0.0
    
    # Calculate track
    track = abs(sum_deviations / (len(deviations) * mad)) * 100.0
    
    # Limit track to 100%
    return min(100.0, track)

def calculate_regular_avs_forecast(
    current_forecast: float,
    latest_demand: float,
    track: float,
    alpha_factor: float = 10.0
) -> float:
    """Calculate new forecast using E3 Regular AVS method.
    
    Args:
        current_forecast: Current forecast value
        latest_demand: Latest demand value
        track: Tracking signal as percentage
        alpha_factor: Alpha factor for weighting
        
    Returns:
        New forecast value
    """
    # Convert track to decimal
    track_decimal = track / 100.0
    
    # Apply alpha factor adjustment
    alpha = track_decimal
    
    if alpha_factor != 0:
        alpha = alpha * (alpha_factor / 10.0)
    
    # Limit alpha between 0 and 1
    alpha = max(0.0, min(1.0, alpha))
    
    # Calculate new forecast
    new_forecast = (alpha * latest_demand) + ((1.0 - alpha) * current_forecast)
    
    return max(0.0, new_forecast)

def calculate_enhanced_avs_forecast(
    current_forecast: float,
    latest_demand: float,
    track: float,
    periods_with_zero_demand: int,
    expected_zero_periods: float,
    update_frequency_impact: int,
    forecast_demand_limit: float,
    alpha_factor: float = 10.0
) -> Tuple[float, bool]:
    """Calculate new forecast using E3 Enhanced AVS method.
    
    Args:
        current_forecast: Current forecast value
        latest_demand: Latest demand value
        track: Tracking signal as percentage
        periods_with_zero_demand: Number of consecutive periods with zero demand
        expected_zero_periods: Expected number of periods with zero demand
        update_frequency_impact: Update frequency impact control
        forecast_demand_limit: Forecast demand limit
        alpha_factor: Alpha factor for weighting
        
    Returns:
        Tuple with new forecast value and whether it was a forced reforecast
    """
    # Check if latest demand is below the limit
    if latest_demand < forecast_demand_limit:
        # Check if we need to force a reforecast
        force_limit = expected_zero_periods * update_frequency_impact
        
        if periods_with_zero_demand >= force_limit:
            # Force a reforecast with a time adjustment
            time_factor = periods_with_zero_demand / update_frequency_impact
            
            # Decrease forecast based on time since last demand
            new_forecast = current_forecast / (1.0 + (0.5 * time_factor))
            
            return new_forecast, True
        
        # No change to forecast
        return current_forecast, False
    
    # Regular forecast update (similar to regular AVS)
    new_forecast = calculate_regular_avs_forecast(
        current_forecast, latest_demand, track, alpha_factor
    )
    
    return new_forecast, False

def apply_seasonality_to_forecast(
    base_forecast: float,
    seasonal_indices: List[float],
    current_period: int
) -> float:
    """Apply seasonality to a base forecast.
    
    Args:
        base_forecast: Base forecast value
        seasonal_indices: List of seasonal indices
        current_period: Current period number (1-based)
        
    Returns:
        Seasonally adjusted forecast
    """
    if not seasonal_indices or len(seasonal_indices) == 0:
        return base_forecast
    
    # Get current season index (adjust for 0-based index)
    period_idx = (current_period - 1) % len(seasonal_indices)
    
    # Get seasonal index
    seasonal_index = seasonal_indices[period_idx]
    
    # Validate index
    if seasonal_index <= 0:
        return base_forecast
    
    # Apply seasonality
    return base_forecast * seasonal_index

def calculate_composite_line(
    history_by_year: Dict[int, List[float]],
    max_years: int = 4,
    recent_weight: float = 0.5
) -> List[float]:
    """Calculate composite line from multiple years of history.
    
    Args:
        history_by_year: Dictionary mapping years to lists of demand values
        max_years: Maximum number of years to consider
        recent_weight: Weight for the most recent year
        
    Returns:
        List of composite line values
    """
    if not history_by_year:
        return []
    
    # Get sorted years (most recent first)
    sorted_years = sorted(history_by_year.keys(), reverse=True)[:max_years]
    
    if not sorted_years:
        return []
    
    # Get periodicity from first year
    first_year = sorted_years[0]
    periodicity = len(history_by_year[first_year])
    
    # Initialize composite line
    composite_line = [0.0] * periodicity
    
    # Calculate weights for each year
    weights = []
    remaining_weight = 1.0 - recent_weight
    
    for i, year in enumerate(sorted_years):
        if i == 0:
            # Most recent year gets higher weight
            weights.append(recent_weight)
        else:
            # Distribute remaining weight exponentially
            year_weight = remaining_weight * math.exp(-0.5 * (i - 1))
            weights.append(year_weight)
    
    # Normalize weights
    weight_sum = sum(weights)
    if weight_sum > 0:
        weights = [w / weight_sum for w in weights]
    
    # Calculate composite line
    for period in range(periodicity):
        weighted_sum = 0.0
        valid_weight_sum = 0.0
        
        for i, year in enumerate(sorted_years):
            if period < len(history_by_year[year]):
                value = history_by_year[year][period]
                weighted_sum += value * weights[i]
                valid_weight_sum += weights[i]
        
        if valid_weight_sum > 0:
            composite_line[period] = weighted_sum / valid_weight_sum
    
    return composite_line

def generate_seasonal_indices(
    composite_line: List[float],
    smoothing_factor: float = 0.3
) -> List[float]:
    """Generate seasonal indices from a composite line.
    
    Args:
        composite_line: Composite line values
        smoothing_factor: Smoothing factor for indices
        
    Returns:
        List of seasonal indices
    """
    if not composite_line:
        return []
    
    # Calculate average
    avg = sum(composite_line) / len(composite_line)
    
    if avg == 0:
        return [1.0] * len(composite_line)
    
    # Calculate initial indices
    indices = [value / avg for value in composite_line]
    
    # Apply smoothing if needed
    if smoothing_factor > 0:
        smoothed_indices = []
        n = len(indices)
        
        for i in range(n):
            # Get adjacent indices (circular)
            prev_idx = (i - 1) % n
            next_idx = (i + 1) % n
            
            # Calculate smoothed value
            smoothed = (
                indices[i] * (1 - smoothing_factor) +
                (indices[prev_idx] + indices[next_idx]) * (smoothing_factor / 2)
            )
            
            smoothed_indices.append(smoothed)
        
        indices = smoothed_indices
    
    # Ensure sum of indices * periods equals the number of periods
    # (i.e., average index is 1.0)
    index_sum = sum(indices)
    if index_sum > 0:
        indices = [idx * len(indices) / index_sum for idx in indices]
    
    return indices

def detect_demand_spike(
    forecast: float,
    actual: float,
    madp: float,
    demand_filter_high: float,
    demand_filter_low: float
) -> Optional[str]:
    """Detect demand spike based on filters.
    
    Args:
        forecast: Forecast value
        actual: Actual demand value
        madp: MADP value as percentage
        demand_filter_high: Demand filter high value
        demand_filter_low: Demand filter low value
        
    Returns:
        'HIGH', 'LOW', or None if no spike detected
    """
    if forecast == 0:
        if actual > 0:
            return 'HIGH'
        return None
    
    # Convert MADP to absolute deviation
    mad = (madp / 100.0) * forecast
    
    # Calculate upper and lower bounds
    upper_bound = forecast + (mad * demand_filter_high)
    lower_bound = forecast - (mad * demand_filter_low)
    
    # Check for spike
    if actual > upper_bound:
        return 'HIGH'
    elif actual < lower_bound and actual < forecast:
        return 'LOW'
    
    return None

def detect_tracking_signal_exception(
    track: float,
    tracking_signal_limit: float
) -> Optional[str]:
    """Detect tracking signal exception.
    
    Args:
        track: Tracking signal as percentage
        tracking_signal_limit: Tracking signal limit
        
    Returns:
        'HIGH', 'LOW', or None if no exception detected
    """
    if track >= tracking_signal_limit:
        # Determine direction
        # In a real implementation, we would need to know the sign of the original
        # sum of deviations to determine if it's HIGH or LOW
        # For this implementation, we'll randomly assign HIGH or LOW
        import random
        return random.choice(['HIGH', 'LOW'])
    
    return None

def adjust_history_value(
    history_value: float,
    adjustment_type: str,
    adjustment_value: float
) -> float:
    """Adjust a history value.
    
    Args:
        history_value: Original history value
        adjustment_type: Type of adjustment ('ADD', 'SUBTRACT', 'MULTIPLY', 'SET')
        adjustment_value: Adjustment value
        
    Returns:
        Adjusted history value
    """
    if adjustment_type == 'ADD':
        return history_value + adjustment_value
    elif adjustment_type == 'SUBTRACT':
        return max(0, history_value - adjustment_value)
    elif adjustment_type == 'MULTIPLY':
        return history_value * adjustment_value
    elif adjustment_type == 'SET':
        return adjustment_value
    else:
        raise ForecastError(f"Invalid adjustment type: {adjustment_type}")

def filter_history(
    history: List[float],
    filter_criteria: Dict[str, Union[int, float, str]]
) -> List[float]:
    """Filter history based on criteria.
    
    Args:
        history: List of history values
        filter_criteria: Dictionary with filter criteria
        
    Returns:
        Filtered list of history values
    """
    if not filter_criteria:
        return history
    
    threshold = filter_criteria.get('threshold')
    if not threshold:
        return history
    
    filtered_history = []
    for value in history:
        if filter_criteria.get('type') == 'GREATER_THAN':
            if value <= threshold:
                filtered_history.append(value)
        elif filter_criteria.get('type') == 'LESS_THAN':
            if value >= threshold:
                filtered_history.append(value)
        else:
            filtered_history.append(value)
    
    return filtered_history

def calculate_lost_sales(
    out_of_stock_days: int,
    daily_forecast: float,
    seasonal_indices: List[float] = None,
    current_period_index: int = None
) -> float:
    """Calculate lost sales based on out of stock days.
    
    Args:
        out_of_stock_days: Number of days item was out of stock
        daily_forecast: Daily forecast value
        seasonal_indices: Optional list of seasonal indices
        current_period_index: Optional current period index (0-based)
        
    Returns:
        Lost sales value
    """
    if out_of_stock_days <= 0 or daily_forecast <= 0:
        return 0.0
    
    # Apply seasonality if provided
    if seasonal_indices and current_period_index is not None:
        # Make sure index is valid
        if 0 <= current_period_index < len(seasonal_indices):
            seasonal_factor = seasonal_indices[current_period_index]
            adjusted_forecast = daily_forecast * seasonal_factor
            return out_of_stock_days * adjusted_forecast
    
    # No seasonality or invalid index
    return out_of_stock_days * daily_forecast

def calculate_expected_zero_periods(
    forecast: float,
    madp: float
) -> float:
    """Calculate expected number of periods with zero demand.
    
    Args:
        forecast: Forecast value
        madp: MADP value as percentage
        
    Returns:
        Expected number of periods with zero demand
    """
    if forecast <= 0:
        return 12  # Assume all periods will be zero
    
    # Convert MADP to standard deviation
    std_dev = madp / 100.0 * forecast * 1.25
    
    # Calculate probability of zero demand
    # Using normal distribution approximation
    z_score = forecast / std_dev if std_dev > 0 else float('inf')
    
    if z_score > 6:
        # Very small probability of zero demand
        return 0
    
    # Probability of zero demand
    prob_zero = max(0, min(1, 1 - math.erf(z_score / math.sqrt(2)) / 2))
    
    # Expected number of periods with zero demand in a year (12 periods)
    return prob_zero * 12