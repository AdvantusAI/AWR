# app/services/order_calculation.py

"""
Path: app/services/order_calculation.py

Calculates Suggested Order Quantity (SOQ) for each SKU/location using dynamic safety stock.
Safety stock is computed using service level (Z), demand standard deviation, lead time, and lead time variance.
All major business rules (min/max order, buying multiples, vendor constraints) are applied.
"""

import numpy as np

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
    """
    Calculate safety stock using the standard formula for independent demand and lead time variability:
    Safety Stock = Z * sqrt( (avg_lead_time * demand_std^2) + (avg_daily_demand^2 * lead_time_std^2) )

    Args:
        service_level_goal: Desired service level (e.g., 0.95 for 95%).
        demand_std: Standard deviation of demand per day.
        avg_lead_time: Average lead time in days.
        lead_time_std: Standard deviation of lead time in days.
        avg_daily_demand: Average daily demand.

    Returns:
        Calculated safety stock (float).
    """
    # Convert service level to Z-score
    z = service_level_to_z(service_level_goal)
    
    # Formula for safety stock when both demand and lead time are variable and independent
    safety_stock = z * np.sqrt(
        (avg_lead_time * (demand_std ** 2)) +
        ((avg_daily_demand ** 2) * (lead_time_std ** 2))
    )
    return safety_stock

def get_effective_safety_stock(sku, safety_stock, is_promotion_active=False):
    """
    Returns the correct safety stock to use in OUTL calculation, following JDA ASR rules:
    - If event minimum is set and a promotion is active, use max(event_minimum, safety_stock)
    - Else if presentation stock is set and greater than safety stock, use presentation stock
    - Else, use safety stock
    """
    if is_promotion_active and sku.event_minimum is not None:
        return max(safety_stock, sku.event_minimum)
    elif sku.presentation_stock is not None and sku.presentation_stock > safety_stock:
        return sku.presentation_stock
    else:
        return safety_stock
    
    
def calculate_soq(
    forecasted_daily_demand: float,
    lead_time_days: float,
    review_cycle_days: float,
    safety_stock: float,
    current_stock: int,
    on_order: int = 0,
    backorder: int = 0,
    reserved: int = 0,
    min_order_qty: int = 1,
    max_order_qty: int = None,
    buying_multiple: int = 1,
    vendor_min: int = None,
    vendor_max: int = None,
    presentation_stock: int = None,
    event_minimum: int = None,
    is_promotion_active: bool = False
) -> int:
    """
    Calculate Suggested Order Quantity (SOQ) for a SKU at a location.

    Args:
        forecasted_daily_demand: Forecasted average daily demand for the SKU.
        lead_time_days: Number of days from order to receipt.
        review_cycle_days: Number of days between replenishment reviews (order cycle).
        safety_stock: Calculated safety stock for the SKU.
        current_stock: Current on-hand inventory.
        on_order: Quantity already on order but not yet received.
        backorder: Quantity committed but not available.
        reserved: Quantity reserved or held for other purposes.
        min_order_qty: Minimum order quantity for the SKU.
        max_order_qty: Maximum order quantity for the SKU (None if unlimited).
        buying_multiple: Orders must be in multiples of this value.
        vendor_min: Vendor-level minimum order quantity (None if not applicable).
        vendor_max: Vendor-level maximum order quantity (None if not applicable).
        presentation_stock: If greater than safety stock, use in place of safety stock.
        event_minimum: For events/promo periods, use in place of presentation stock.
        is_promotion_active: Boolean indicating if a promotion is active for the SKU.

    Returns:
        SOQ (Suggested Order Quantity), rounded up to the nearest buying multiple and subject to business constraints.
    """

    # 1. Use event minimum or presentation stock if applicable
    effective_safety_stock = safety_stock
    if is_promotion_active and event_minimum is not None:
        effective_safety_stock = max(safety_stock, event_minimum)
    elif presentation_stock is not None:
        effective_safety_stock = max(safety_stock, presentation_stock)

    # 2. Calculate Order Up To Level (OUTL)
    # OUTL = (Lead Time + Effective Order Cycle) * Forecasted Daily Demand + Effective Safety Stock
    order_cycle = max(lead_time_days, review_cycle_days)
    outl = (order_cycle * forecasted_daily_demand) + effective_safety_stock

    # 3. Calculate Available Balance
    available_balance = current_stock + on_order - backorder - reserved

    # 4. SOQ = OUTL - Available Balance
    soq = outl - available_balance

    # 5. Round up to nearest buying multiple
    if soq > 0:
        soq = int(np.ceil(soq / buying_multiple) * buying_multiple)  # Ceiling division
    else:
        soq = 0

    # 6. Apply SKU min/max order constraints
    soq = max(soq, min_order_qty if soq > 0 else 0)
    if max_order_qty is not None and soq > max_order_qty:
        soq = max_order_qty

    # 7. Apply vendor min/max order constraints
    if vendor_min is not None and soq < vendor_min:
        soq = 0  # Do not order if below vendor minimum
    if vendor_max is not None and soq > vendor_max:
        soq = vendor_max

    return soq

# Example usage for a single SKU/location
def example_usage():
    # Example values (replace with real data)
    service_level_z = 1.65  # 95% service level
    demand_std = 20         # units/day
    avg_lead_time = 10      # days
    lead_time_std = 2       # days
    avg_daily_demand = 100  # units/day

    safety_stock = calculate_safety_stock(
        service_level=service_level_z,
        demand_std=demand_std,
        avg_lead_time=avg_lead_time,
        lead_time_std=lead_time_std,
        avg_daily_demand=avg_daily_demand
    )

    soq = calculate_soq(
        forecasted_daily_demand=avg_daily_demand,
        lead_time_days=avg_lead_time,
        review_cycle_days=7,
        safety_stock=safety_stock,
        current_stock=200,
        on_order=50,
        backorder=10,
        reserved=0,
        min_order_qty=10,
        max_order_qty=500,
        buying_multiple=10,
        vendor_min=50,
        vendor_max=400
    )

    print(f"Safety Stock: {safety_stock:.2f}, SOQ: {soq}")

if __name__ == "__main__":
    example_usage()
