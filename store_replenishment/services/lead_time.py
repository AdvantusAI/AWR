# app/services/lead_time.py

"""
Path: app/services/lead_time.py

Dynamic lead time forecasting and variance support for replenishment.
Calculates and stores rolling lead time mean and variance for each SKU/source using receipt history.
Supports hierarchy: SKU > Vendor(Source) > Company default.
"""
import numpy as np
import pandas as pd
from datetime import timedelta, date
from sqlalchemy.orm import Session
from typing import Tuple, List, Optional

def get_lead_time_history(session: Session, sku_id: int, vendor_id: int, days: int = 365) -> pd.Series:
    """
    Fetches recent lead time history (in days) for a SKU/source from the database.
    Returns a pandas Series of lead times (order_date -> receipt_date).
    """
    from app.models.purchase_order import PurchaseOrder  # You need a PO model with order/receipt dates

    try:
        records = (
            session.query(PurchaseOrder)
            .filter(
                PurchaseOrder.sku_id == sku_id,
                PurchaseOrder.vendor_id == vendor_id,
                PurchaseOrder.order_date.is_not(None),
                PurchaseOrder.receipt_date.is_not(None),
                PurchaseOrder.order_date >= (date.today() - timedelta(days=days))
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
    except Exception as e:
        print(f"Error fetching lead time history: {e}")
        return pd.Series(dtype=float)


def dynamic_lead_time_forecast(session: Session, sku, vendor, company=None, days: int = 365) -> Tuple[float, float]:
    """
    Returns (lead_time_forecast, lead_time_variance_pct) for a SKU/source,
    using SKU > Vendor > Company hierarchy and dynamic calculation from receipt history.
    """
    try:
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
    
    except Exception as e:
        print(f"Error calculating lead time forecast: {e}")
        # Safe default
        return 7, 0

def update_lead_time_fields(session: Session, sku, vendor, company=None, days: int = 365) -> Tuple[float, float]:
    """
    Updates the SKU and/or Vendor lead time fields in the database based on receipt history.
    Should be run as a nightly or periodic batch job.
    """
    try:
        lead_time_forecast, lead_time_variance_pct = dynamic_lead_time_forecast(session, sku, vendor, company, days)
        
        sku.lead_time_forecast = lead_time_forecast
        sku.lead_time_variance_pct = lead_time_variance_pct
        
        session.commit()
        return lead_time_forecast, lead_time_variance_pct
    
    except Exception as e:
        print(f"Error updating lead time fields: {e}")
        session.rollback()
        return 7, 0  # Safe default

def batch_update_lead_times(session: Session, days: int = 365) -> List[dict]:
    """
    Updates lead time forecasts for all SKUs in the system.
    Returns a list of results with SKU ID, vendor ID, and the new forecasts.
    """
    from app.models.sku import SKU
    from app.models.vendor import Vendor
    from app.models.company import Company
    
    try:
        # Get all company settings (assuming there's only one company record)
        company = session.query(Company).first()
        
        # Get all SKUs and vendors
        skus = session.query(SKU).all()
        
        results = []
        
        for sku in skus:
            vendor = session.query(Vendor).filter(Vendor.id == sku.vendor_id).first()
            if vendor:
                lead_time_forecast, lead_time_variance_pct = update_lead_time_fields(
                    session, sku, vendor, company, days
                )
                
                results.append({
                    'sku_id': sku.id,
                    'vendor_id': vendor.id,
                    'lead_time_forecast': lead_time_forecast,
                    'lead_time_variance_pct': lead_time_variance_pct
                })
        
        return results
    
    except Exception as e:
        print(f"Error in batch update lead times: {e}")
        session.rollback()
        return []
    

# Example usage in replenishment calculation:
def get_lead_time_for_replenishment(session, sku, vendor, company=None):
    """
    Returns the best available lead time forecast and variance for SOQ/safety stock calculation.
    """
    return dynamic_lead_time_forecast(session, sku, vendor, company)
