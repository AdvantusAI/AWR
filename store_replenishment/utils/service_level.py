# app/utils/service_level.py - Fixed version

"""
Path: app/utils/service_level.py

Utility to resolve the correct service level goal for a SKU/location, following the hierarchy:
SKU > Vendor > Company.
"""

from scipy.stats import norm
import numpy as np
from typing import Optional

def get_service_level_goal(session, sku, vendor, company):
    """
    Returns the effective service level goal for a SKU, following the hierarchy.
    """
    try:
        if sku.service_level_goal is not None:
            return sku.service_level_goal
        elif vendor and vendor.service_level_goal is not None:
            return vendor.service_level_goal
        elif company and company.default_service_level is not None:
            return company.default_service_level
        else:
            # Fallback to a safe default (e.g., 0.95)
            return 0.95
    except Exception as e:
        print(f"Error getting service level goal: {e}")
        return 0.95  # Safe default

def service_level_to_z(service_level_goal: float) -> float:
    """
    Converts a service level (e.g., 0.95) to a Z-score for safety stock calculation.
    """
    try:
        return norm.ppf(service_level_goal)
    except Exception as e:
        print(f"Error converting service level to Z-score: {e}")
        # Default to ~1.65 (approximately 95% service level)
        return 1.65

def calculate_safety_stock(
    service_level_goal: float,
    demand_std: float,
    avg_lead_time: float,
    lead_time_std: float,
    avg_daily_demand: float
) -> float:
    """
    Calculate safety stock using the formula for independent demand and lead time variability.
    
    Args:
        service_level_goal: Desired service level (e.g., 0.95 for 95%).
        demand_std: Standard deviation of demand per day.
        avg_lead_time: Average lead time in days.
        lead_time_std: Standard deviation of lead time in days.
        avg_daily_demand: Average daily demand.
        
    Returns:
        Calculated safety stock (float).
    """
    try:
        z = service_level_to_z(service_level_goal)
        return z * np.sqrt(
            (avg_lead_time * (demand_std ** 2)) +
            ((avg_daily_demand ** 2) * (lead_time_std ** 2))
        )
    except Exception as e:
        print(f"Error calculating safety stock: {e}")
        # Calculate a safe default
        return avg_daily_demand * avg_lead_time * 0.2  # 20% of average demand during lead time