# app/services/lead_time.py

"""
Path: app/services/lead_time.py

Dynamic lead time forecasting and variance support for replenishment.
Calculates and stores rolling lead time mean and variance for each SKU/source using receipt history.
Supports hierarchy: SKU > Vendor(Source) > Company default.
"""

import numpy as np
import pandas as pd
from datetime import timedelta

def get_lead_time_history(session, sku_id, vendor_id, days=365):
    """
    Fetches recent lead time history (in days) for a SKU/source from the database.
    Returns a pandas Series of lead times (order_date -> receipt_date).
    """
    from app.models.purchase_order import PurchaseOrder  # You need a PO model with order/receipt dates

    records = (
        session.query(PurchaseOrder)
        .filter(
            PurchaseOrder.sku_id == sku_id,
            PurchaseOrder.vendor_id == vendor_id,
            PurchaseOrder.order_date != None,
            PurchaseOrder.receipt_date != None,
            PurchaseOrder.order_date >= pd.Timestamp.today() - timedelta(days=days)
        )
        .order_by(PurchaseOrder.order_date)
        .all()
    )
    if not records:
        return pd.Series(dtype=float)
    lead_times = [
        (rec.receipt_date - rec.order_date).days
        for rec in records
        if rec.receipt_date and rec.order_date and (rec.receipt_date - rec.order_date).days > 0
    ]
    return pd.Series(lead_times)

def dynamic_lead_time_forecast(session, sku, vendor, company=None, days=365):
    """
    Returns (lead_time_forecast, lead_time_variance) for a SKU/source,
    using SKU > Vendor > Company hierarchy and dynamic calculation from receipt history.
    """
    # 1. Try to use SKU-level override if present
    if sku.lead_time_forecast is not None and sku.lead_time_variance_pct is not None:
        return sku.lead_time_forecast, sku.lead_time_variance_pct

    # 2. Try to use rolling history for this SKU/vendor
    lt_series = get_lead_time_history(session, sku.id, vendor.id, days=days)
    if not lt_series.empty:
        lead_time_forecast = lt_series.mean()
        lead_time_std = lt_series.std()
        lead_time_variance_pct = (lead_time_std / lead_time_forecast) * 100 if lead_time_forecast > 0 else 0
        return lead_time_forecast, lead_time_variance_pct

    # 3. Fallback to vendor/source default if present
    if vendor.lead_time_forecast is not None and vendor.lead_time_variance_pct is not None:
        return vendor.lead_time_forecast, vendor.lead_time_variance_pct

    # 4. Fallback to company default if present
    if company and hasattr(company, "default_lead_time") and hasattr(company, "default_lead_time_variance_pct"):
        return company.default_lead_time, company.default_lead_time_variance_pct

    # 5. Fallback to a safe static default (e.g., 7 days, 0% variance)
    return 7, 0

def update_lead_time_fields(session, sku, vendor, company=None, days=365):
    """
    Updates the SKU and/or Vendor lead time fields in the database based on receipt history.
    Should be run as a nightly or periodic batch job.
    """
    lead_time_forecast, lead_time_variance_pct = dynamic_lead_time_forecast(session, sku, vendor, company, days)
    sku.lead_time_forecast = lead_time_forecast
    sku.lead_time_variance_pct = lead_time_variance_pct
    session.commit()
    return lead_time_forecast, lead_time_variance_pct

# Example usage in replenishment calculation:
def get_lead_time_for_replenishment(session, sku, vendor, company=None):
    """
    Returns the best available lead time forecast and variance for SOQ/safety stock calculation.
    """
    return dynamic_lead_time_forecast(session, sku, vendor, company)
