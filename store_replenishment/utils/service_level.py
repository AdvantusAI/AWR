# app/utils/service_level.py

"""
Path: app/utils/service_level.py

Utility to resolve the correct service level goal for a SKU/location, following the hierarchy:
SKU > Vendor > Company.
"""

def get_service_level_goal(session, sku, vendor, company):
    """
    Returns the effective service level goal for a SKU, following the hierarchy.
    """
    if sku.service_level_goal is not None:
        return sku.service_level_goal
    elif vendor and vendor.service_level_goal is not None:
        return vendor.service_level_goal
    elif company and company.default_service_level is not None:
        return company.default_service_level
    else:
        # Fallback to a safe default (e.g., 0.95)
        return 0.95

from scipy.stats import norm

def service_level_to_z(service_level_goal):
    """
    Converts a service level (e.g., 0.95) to a Z-score for safety stock calculation.
    """
    return norm.ppf(service_level_goal)

def calculate_safety_stock(
    service_level_goal: float,
    demand_std: float,
    avg_lead_time: float,
    lead_time_std: float,
    avg_daily_demand: float
) -> float:
    z = service_level_to_z(service_level_goal)
    return z * np.sqrt(
        (avg_lead_time * (demand_std ** 2)) +
        ((avg_daily_demand ** 2) * (lead_time_std ** 2))
    )

from app.utils.service_level import get_service_level_goal

def generate_replenishment_for_sku(session, sku, vendor, company, ...):
    service_level_goal = get_service_level_goal(session, sku, vendor, company)
    # Use this in your safety stock calculation
    safety_stock = calculate_safety_stock(
        service_level_goal,
        demand_std,
        avg_lead_time,
        lead_time_std,
        avg_daily_demand
    )
    # Continue with SOQ calculation...