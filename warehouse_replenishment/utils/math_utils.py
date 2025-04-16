# warehouse_replenishment/utils/math_utils.py
import math
from typing import List, Union, Optional
import numpy as np
import sys
import os
from pathlib import Path

# Add the parent directory to the path so we can import our modules
parent_dir = str(Path(__file__).parent.parent.parent)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from warehouse_replenishment.exceptions import CalculationError

def round_to_multiple(value: float, multiple: float) -> float:
    """Round a value to the nearest multiple.
    
    Args:
        value: Value to round
        multiple: Multiple to round to
        
    Returns:
        Rounded value
    """
    if multiple <= 0:
        return value
    
    return math.ceil(value / multiple) * multiple

def calculate_madp(forecast: float, history: List[float]) -> float:
    """Calculate Mean Absolute Deviation Percentage (MADP).
    
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
    
    return madp

def calculate_track(forecast: float, history: List[float]) -> float:
    """Calculate tracking signal (track).
    
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

def forecast_weighted_average(
    current_forecast: float,
    new_demand: float,
    track: float,
    alpha_factor: float = 10.0
) -> float:
    """Calculate new forecast using weighted average.
    
    Args:
        current_forecast: Current forecast value
        new_demand: New demand value
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
    new_forecast = (alpha * new_demand) + ((1.0 - alpha) * current_forecast)
    
    return new_forecast

def weighted_average(values: List[float], weights: List[float]) -> float:
    """Calculate weighted average.
    
    Args:
        values: List of values
        weights: List of weights
        
    Returns:
        Weighted average
    """
    if len(values) != len(weights):
        raise CalculationError("Length of values and weights must be the same")
    
    if not values:
        return 0.0
    
    if sum(weights) == 0:
        return sum(values) / len(values)
    
    weighted_sum = sum(v * w for v, w in zip(values, weights))
    return weighted_sum / sum(weights)

def moving_average(values: List[float], window: int = 3) -> float:
    """Calculate moving average.
    
    Args:
        values: List of values
        window: Window size
        
    Returns:
        Moving average
    """
    if not values:
        return 0.0
    
    window = min(window, len(values))
    return sum(values[:window]) / window

def exponential_smoothing(
    history: List[float],
    alpha: float = 0.2
) -> List[float]:
    """Calculate exponentially smoothed values.
    
    Args:
        history: List of history values
        alpha: Smoothing factor (0-1)
        
    Returns:
        List of smoothed values
    """
    if not history:
        return []
    
    # Limit alpha between 0 and 1
    alpha = max(0.0, min(1.0, alpha))
    
    smoothed = [history[0]]
    
    for i in range(1, len(history)):
        smoothed_value = alpha * history[i] + (1 - alpha) * smoothed[i-1]
        smoothed.append(smoothed_value)
    
    return smoothed

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

def linear_regression(
    x: List[float],
    y: List[float]
) -> tuple:
    """Calculate linear regression coefficients.
    
    Args:
        x: List of x values (typically time periods)
        y: List of y values (typically demand)
        
    Returns:
        Tuple with slope and intercept
    """
    if len(x) != len(y) or len(x) < 2:
        raise CalculationError("Invalid input for linear regression")
    
    n = len(x)
    
    # Calculate means
    mean_x = sum(x) / n
    mean_y = sum(y) / n
    
    # Calculate slope
    numerator = sum((x[i] - mean_x) * (y[i] - mean_y) for i in range(n))
    denominator = sum((x[i] - mean_x) ** 2 for i in range(n))
    
    if denominator == 0:
        slope = 0
    else:
        slope = numerator / denominator
    
    # Calculate intercept
    intercept = mean_y - slope * mean_x
    
    return (slope, intercept)

def trend_projection(
    history: List[float],
    periods_ahead: int = 1
) -> float:
    """Project trend based on historical data.
    
    Args:
        history: List of history values
        periods_ahead: Number of periods to project ahead
        
    Returns:
        Projected value
    """
    if not history or len(history) < 2:
        return history[0] if history else 0.0
    
    # Create x values (time periods)
    x = list(range(len(history)))
    
    # Calculate linear regression
    slope, intercept = linear_regression(x, history)
    
    # Project ahead
    projection = intercept + slope * (len(history) + periods_ahead - 1)
    
    return max(0.0, projection)  # Ensure non-negative projection