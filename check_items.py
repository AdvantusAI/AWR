#!/usr/bin/env python
# check_items.py - Script to check item data

import sys
from pathlib import Path

# Add the parent directory to the path so we can import our modules
parent_dir = str(Path(__file__).parent)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from warehouse_replenishment.db import db, session_scope
from warehouse_replenishment.models import Item, Vendor
from sqlalchemy import func

def check_items():
    """Check item data for missing or invalid values."""
    print("\n=== Checking Item Data ===")
    
    with session_scope() as session:
        # Get all items
        items = session.query(Item).all()
        
        for item in items:
            vendor = session.query(Vendor).get(item.vendor_id)
            print(f"\nItem: {item.item_id} (ID: {item.id})")
            print(f"  Vendor: {vendor.name}")
            print(f"  Description: {item.description}")
            print(f"  Stock Status:")
            print(f"    On Hand: {item.on_hand}")
            print(f"    On Order: {item.on_order}")
            print(f"    Customer Back Order: {item.customer_back_order}")
            print(f"  Order Points:")
            print(f"    Item Order Point Units: {item.item_order_point_units}")
            print(f"    Item Order Point Days: {item.item_order_point_days}")
            print(f"    Vendor Order Point Days: {item.vendor_order_point_days}")
            print(f"  Demand:")
            print(f"    Weekly: {item.demand_weekly}")
            print(f"    4-Weekly: {item.demand_4weekly}")
            print(f"    Monthly: {item.demand_monthly}")
            print(f"  Lead Time:")
            print(f"    Forecast: {item.lead_time_forecast}")
            print(f"    Variance: {item.lead_time_variance}")
            print(f"  Price:")
            print(f"    Purchase Price: {item.purchase_price}")
            print(f"    Sales Price: {item.sales_price}")

if __name__ == "__main__":
    # Initialize database
    db.initialize()
    
    # Check items
    check_items() 