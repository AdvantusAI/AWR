#!/usr/bin/env python
# check_order_points.py - Script to check item order points and balances

import sys
from pathlib import Path

# Add the parent directory to the path so we can import our modules
parent_dir = str(Path(__file__).parent)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from warehouse_replenishment.db import db, session_scope
from warehouse_replenishment.models import Item, Vendor
from sqlalchemy import func

def check_order_points():
    """Check item order points and balances."""
    print("\n=== Checking Item Order Points ===")
    
    with session_scope() as session:
        # Get all items
        items = session.query(Item).all()
        
        for item in items:
            vendor = session.query(Vendor).get(item.vendor_id)
            balance = item.on_hand + item.on_order
            
            print(f"\nItem: {item.item_id} (ID: {item.id})")
            print(f"  Vendor: {vendor.name}")
            print(f"  Balance:")
            print(f"    On Hand: {item.on_hand}")
            print(f"    On Order: {item.on_order}")
            print(f"    Total Balance: {balance}")
            print(f"  Order Points:")
            print(f"    Item Order Point (IOP): {item.item_order_point_units}")
            print(f"    Order Up To Level (OUTL): {item.order_up_to_level_units}")
            print(f"  Status:")
            print(f"    At Order Point: {balance < item.item_order_point_units}")
            if balance < item.item_order_point_units:
                print(f"    Units Below IOP: {item.item_order_point_units - balance}")

if __name__ == "__main__":
    # Initialize database
    db.initialize()
    
    # Check order points
    check_order_points() 