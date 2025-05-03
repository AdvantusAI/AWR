# app/services/exception_monitoring.py

"""
Path: app/services/exception_monitoring.py

Flags and reports replenishment anomalies such as high/low SOQ, lost sales, and other exceptions.
Exceptions are logged in the database for reporting and buyer review.
"""

from datetime import date
from typing import List, Dict

def flag_soq_anomalies(
    session,
    store_id: int,
    sku_id: int,
    soq: float,
    avg_daily_demand: float,
    max_weeks_of_supply: float = 26,
    min_weeks_of_supply: float = 0.5,
    lost_sales_threshold: int = 1
) -> List[Dict]:
    """
    Flags high/low SOQ, lost sales, and other anomalies for a given SKU/location.

    Returns a list of exception dicts.
    """
    from app.models.sales_history import SalesHistory
    from app.models.exception_log import ExceptionLog

    exceptions = []

    # Calculate weeks of supply for this SOQ
    weeks_of_supply = soq / (avg_daily_demand * 7) if avg_daily_demand > 0 else 0

    # High SOQ: exceeds max weeks of supply
    if weeks_of_supply > max_weeks_of_supply:
        exceptions.append({
            "store_id": store_id,
            "sku_id": sku_id,
            "exception_type": "HIGH_SOQ",
            "description": f"SOQ covers {weeks_of_supply:.1f} weeks, exceeds max {max_weeks_of_supply} weeks"
        })

    # Low SOQ: below min weeks of supply (but not zero)
    if 0 < weeks_of_supply < min_weeks_of_supply:
        exceptions.append({
            "store_id": store_id,
            "sku_id": sku_id,
            "exception_type": "LOW_SOQ",
            "description": f"SOQ covers only {weeks_of_supply:.2f} weeks, below min {min_weeks_of_supply} weeks"
        })

    # Lost sales: check recent history for lost sales
    recent_lost_sales = (
        session.query(SalesHistory)
        .filter(
            SalesHistory.store_id == store_id,
            SalesHistory.sku_id == sku_id,
            SalesHistory.sale_date >= date.today() - timedelta(days=30),
            SalesHistory.lost_sales >= lost_sales_threshold
        )
        .count()
    )
    if recent_lost_sales > 0:
        exceptions.append({
            "store_id": store_id,
            "sku_id": sku_id,
            "exception_type": "LOST_SALES",
            "description": f"Lost sales recorded in the last 30 days"
        })

    # Log exceptions to the database
    for ex in exceptions:
        log = ExceptionLog(
            store_id=ex["store_id"],
            sku_id=ex["sku_id"],
            exception_date=date.today(),
            exception_type=ex["exception_type"],
            description=ex["description"]
        )
        session.add(log)
    session.commit()

    return exceptions

