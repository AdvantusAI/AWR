# warehouse_replenishment/core/safety_stock.py
import math
from typing import Dict, List, Tuple, Optional, Union
import numpy as np
from scipy import stats
import sys
import os
from pathlib import Path

# Add the parent directory to the path so we can import our modules
parent_dir = str(Path(__file__).parent.parent.parent)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from warehouse_replenishment.exceptions import SafetyStockError

def calculate_safety_stock(
    service_level_goal: float,
    madp: float,
    lead_time: float,
    lead_time_variance: float,
    order_cycle: Optional[float] = None
) -> float:
    """Calculate safety stock days based on service level goal and variability.
    
    Args:
        service_level_goal: Service level goal as percentage (e.g., 95.0)
        madp: Mean Absolute Deviation Percentage
        lead_time: Lead time in days
        lead_time_variance: Lead time variance as percentage
        order_cycle: Order cycle in days (optional)
        
    Returns:
        Safety stock in days
    """
    try:
        # Convert service level goal to Z-score
        # For example, 95% service level = 1.645 standard deviations
        z_score = stats.norm.ppf(service_level_goal / 100.0)
        
        # Convert MADP to standard deviation
        # MADP is Mean Absolute Deviation as a Percentage
        # For normal distribution, std dev = MAD * 1.25
        std_dev = madp / 100.0 * 1.25
        
        # Calculate lead time variance in days
        lt_variance_days = lead_time * (lead_time_variance / 100.0)
        
        # Calculate safety stock using the formula:
        # SS = Z * √(LT * σ²D + D² * σ²LT)
        # where:
        # - Z is the service level Z-score
        # - LT is the lead time in days
        # - σD is the standard deviation of demand
        # - D is the daily demand (normalized to 1.0 for calculating days)
        # - σLT is the standard deviation of lead time
        
        # For calculating days of supply, we normalize daily demand to 1.0
        daily_demand = 1.0
        
        # Calculate safety stock in days
        safety_stock_days = z_score * math.sqrt(
            (lead_time * std_dev**2) + 
            (daily_demand**2 * lt_variance_days)
        )
        
        # Impact of order cycle on safety stock (inverse relationship)
        # As order cycle increases, the relative impact of safety stock decreases
        if order_cycle is not None and order_cycle > 0:
            cycle_factor = 1.0 - (0.1 * math.log10(order_cycle))
            cycle_factor = max(0.5, min(1.0, cycle_factor))  # Limit factor between 0.5 and 1.0
            safety_stock_days *= cycle_factor
        
        return float(max(0.1, safety_stock_days))  # Ensure minimum safety stock and convert to Python float
        
    except Exception as e:
        raise SafetyStockError(f"Error calculating safety stock: {str(e)}")

def calculate_service_level(
    safety_stock_days: float,
    madp: float,
    lead_time: float,
    lead_time_variance: float
) -> float:
    """Calculate the service level achieved with a given safety stock.
    
    Args:
        safety_stock_days: Safety stock in days
        madp: Mean Absolute Deviation Percentage
        lead_time: Lead time in days
        lead_time_variance: Lead time variance as percentage
        
    Returns:
        Service level as a percentage
    """
    try:
        # Convert MADP to standard deviation
        std_dev = madp / 100.0 * 1.25
        
        # Calculate lead time variance in days
        lt_variance_days = lead_time * (lead_time_variance / 100.0)
        
        # For calculating days of supply, we normalize daily demand to 1.0
        daily_demand = 1.0
        
        # Calculate the denominator of the Z-score formula
        denominator = math.sqrt(
            (lead_time * std_dev**2) + 
            (daily_demand**2 * lt_variance_days)
        )
        
        # Avoid division by zero
        if denominator == 0:
            return 100.0
        
        # Calculate Z-score
        z_score = safety_stock_days / denominator
        
        # Convert Z-score to service level
        service_level = stats.norm.cdf(z_score) * 100.0
        
        return min(100.0, service_level)
        
    except Exception as e:
        raise SafetyStockError(f"Error calculating service level: {str(e)}")

def empirical_safety_stock_adjustment(
    current_safety_stock: float,
    service_level_goal: float,
    service_level_attained: float,
    max_adjustment_pct: float = 10.0
) -> float:
    """Adjust safety stock based on empirical service level performance.
    
    Args:
        current_safety_stock: Current safety stock in days
        service_level_goal: Service level goal as percentage
        service_level_attained: Service level attained as percentage
        max_adjustment_pct: Maximum adjustment percentage
        
    Returns:
        Adjusted safety stock in days
    """
    try:
        # Calculate service level difference
        service_difference = service_level_goal - service_level_attained
        
        # Calculate adjustment factor
        # If negative, we're exceeding the goal and can reduce safety stock
        # If positive, we're below the goal and need to increase safety stock
        adjustment_factor = service_difference / 100.0
        
        # Limit maximum adjustment
        adjustment_factor = max(-max_adjustment_pct/100.0, min(max_adjustment_pct/100.0, adjustment_factor))
        
        # Apply adjustment
        adjusted_safety_stock = current_safety_stock * (1.0 + adjustment_factor)
        
        return max(0.1, adjusted_safety_stock)  # Ensure minimum safety stock
        
    except Exception as e:
        raise SafetyStockError(f"Error adjusting safety stock: {str(e)}")

def calculate_safety_stock_units(
    safety_stock_days: float,
    daily_demand: float
) -> float:
    """Convert safety stock days to units.
    
    Args:
        safety_stock_days: Safety stock in days
        daily_demand: Daily demand in units
        
    Returns:
        Safety stock in units
    """
    return safety_stock_days * daily_demand