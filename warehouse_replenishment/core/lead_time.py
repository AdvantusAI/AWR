# warehouse_replenishment/core/lead_time.py
import math
from typing import Dict, List, Tuple, Optional, Union
import numpy as np
from scipy import stats

from ..exceptions import LeadTimeError

def forecast_lead_time(
    historical_lead_times: List[float],
    current_lead_time: float,
    order_history: List[Dict] = None
) -> float:
    """Forecast lead time based on historical data.
    
    Args:
        historical_lead_times: List of historical lead times
        current_lead_time: Current known lead time
        order_history: Optional order history for more context
        
    Returns:
        Forecast lead time
    """
    if not historical_lead_times:
        return current_lead_time
    
    # Use weighted average, giving more weight to recent lead times
    weights = [math.exp(-0.1 * i) for i in range(len(historical_lead_times))]
    
    # Calculate weighted average
    weighted_avg = np.average(historical_lead_times, weights=weights)
    
    # If order history is provided, look for trends or anomalies
    if order_history:
        # Analyze order timing and actual receipt dates
        actual_lead_times = [
            (order['receipt_date'] - order['order_date']).days 
            for order in order_history if 'receipt_date' in order and 'order_date' in order
        ]
        
        if actual_lead_times:
            actual_weighted_avg = np.average(actual_lead_times, weights=weights[:len(actual_lead_times)])
            # Blend forecasted and actual lead times
            weighted_avg = (weighted_avg + actual_weighted_avg) / 2
    
    return round(weighted_avg, 1)

def calculate_variance(
    historical_lead_times: List[float]
) -> float:
    """Calculate lead time variance.
    
    Args:
        historical_lead_times: List of historical lead times
        
    Returns:
        Lead time variance as a percentage
    """
    if not historical_lead_times or len(historical_lead_times) < 2:
        return 0.0
    
    # Calculate variance
    variance = np.var(historical_lead_times)
    mean = np.mean(historical_lead_times)
    
    # Convert to percentage
    variance_percentage = (math.sqrt(variance) / mean) * 100
    
    return round(variance_percentage, 2)

def detect_lead_time_anomalies(
    historical_lead_times: List[float],
    current_lead_time: float,
    confidence_level: float = 0.95
) -> List[Dict[str, Union[float, str]]]:
    """Detect lead time anomalies using statistical methods.
    
    Args:
        historical_lead_times: List of historical lead times
        current_lead_time: Current lead time
        confidence_level: Confidence level for anomaly detection
        
    Returns:
        List of detected anomalies
    """
    if not historical_lead_times:
        return []
    
    anomalies = []
    
    # Calculate mean and standard deviation
    mean = np.mean(historical_lead_times)
    std_dev = np.std(historical_lead_times)
    
    # Calculate z-score for current lead time
    z_score = abs(current_lead_time - mean) / std_dev if std_dev > 0 else 0
    
    # Get critical z-value for confidence level
    critical_z = stats.norm.ppf((1 + confidence_level) / 2)
    
    # Check if current lead time is an outlier
    if z_score > critical_z:
        anomalies.append({
            'type': 'OUTLIER',
            'current_lead_time': current_lead_time,
            'historical_mean': mean,
            'historical_std_dev': std_dev,
            'z_score': z_score
        })
    
    # Detect significant changes in lead time trend
    if len(historical_lead_times) > 2:
        trend_slope, _ = np.polyfit(range(len(historical_lead_times)), historical_lead_times, 1)
        
        if abs(trend_slope) > std_dev / 2:
            anomalies.append({
                'type': 'TREND_CHANGE',
                'trend_slope': trend_slope,
                'direction': 'INCREASING' if trend_slope > 0 else 'DECREASING'
            })
    
    return anomalies

def calculate_safety_stock_adjustment(
    lead_time_variance: float,
    service_level: float,
    current_safety_stock: float
) -> float:
    """Adjust safety stock based on lead time variance.
    
    Args:
        lead_time_variance: Lead time variance percentage
        service_level: Service level goal
        current_safety_stock: Current safety stock
        
    Returns:
        Adjusted safety stock
    """
    try:
        # Convert service level to Z-score
        z_score = stats.norm.ppf(service_level / 100.0)
        
        # Variance adjustment factor
        # Higher variance requires more safety stock
        variance_factor = 1 + (lead_time_variance / 100.0)
        
        # Calculate new safety stock
        adjusted_safety_stock = current_safety_stock * variance_factor * z_score
        
        return max(0, round(adjusted_safety_stock, 2))
    
    except Exception as e:
        raise LeadTimeError(f"Error adjusting safety stock: {str(e)}")

def predict_fill_in_lead_time(
    vendor_lead_time: float,
    historical_lead_times: List[float] = None,
    alternate_vendor_lead_time: float = None
) -> float:
    """Predict a fill-in lead time for supply chain disruptions.
    
    Args:
        vendor_lead_time: Primary vendor's lead time
        historical_lead_times: Optional historical lead times for more context
        alternate_vendor_lead_time: Optional alternate vendor's lead time
        
    Returns:
        Predicted fill-in lead time
    """
    if alternate_vendor_lead_time:
        # Prioritize alternate vendor lead time
        return alternate_vendor_lead_time
    
    if historical_lead_times:
        # Use historical data to predict a reasonable fill-in time
        mean_historical_lt = np.mean(historical_lead_times)
        
        # Add a buffer to historical average
        fill_in_lt = mean_historical_lt * 1.5
    else:
        # If no historical data, use vendor lead time as baseline
        fill_in_lt = vendor_lead_time * 1.25
    
    # Ensure reasonable bounds
    return max(3, min(round(fill_in_lt, 1), 45))  # Between 3 and 45 days

def evaluate_lead_time_reliability(
    expected_lead_time: float,
    actual_lead_times: List[float]
) -> Dict[str, Union[float, str]]:
    """Evaluate lead time reliability.
    
    Args:
        expected_lead_time: Expected lead time
        actual_lead_times: List of actual lead times
        
    Returns:
        Dictionary with reliability metrics
    """
    if not actual_lead_times:
        return {
            'reliability_score': 0.0,
            'status': 'INSUFFICIENT_DATA'
        }
    
    # Calculate metrics
    mean_actual_lt = np.mean(actual_lead_times)
    std_dev_lt = np.std(actual_lead_times)
    
    # Deviation from expected lead time
    deviation_pct = abs(mean_actual_lt - expected_lead_time) / expected_lead_time * 100
    
    # Determine reliability score
    # Lower deviation and standard deviation indicate higher reliability
    reliability_score = max(0, 100 - (deviation_pct + std_dev_lt))
    
    # Categorize reliability
    if reliability_score >= 90:
        status = 'EXCELLENT'
    elif reliability_score >= 75:
        status = 'GOOD'
    elif reliability_score >= 50:
        status = 'AVERAGE'
    elif reliability_score >= 25:
        status = 'POOR'
    else:
        status = 'UNRELIABLE'
    
    return {
        'reliability_score': round(reliability_score, 2),
        'mean_lead_time': round(mean_actual_lt, 2),
        'lead_time_std_dev': round(std_dev_lt, 2),
        'deviation_percentage': round(deviation_pct, 2),
        'status': status
    }