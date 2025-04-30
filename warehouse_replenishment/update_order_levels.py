#!/usr/bin/env python
# update_order_levels.py - Script to update order up to levels for items

import sys
from pathlib import Path

# Add the parent directory to the path so we can import our modules
parent_dir = str(Path(__file__).parent)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from warehouse_replenishment.db import db, session_scope
from warehouse_replenishment.models import Item, Vendor
from sqlalchemy import func

def update_order_levels():
    """Update order up to levels for all items."""
    print("\n=== Updating Item Order Up To Levels ===")
    
    with session_scope() as session:
        # Get all items
        items = session.query(Item).all()
        
        for item in items:
            vendor = session.query(Vendor).get(item.vendor_id)
            
            # Calculate order up to level based on vendor order cycle and demand
            daily_demand = item.demand_4weekly / 28  # Convert 4-weekly demand to daily
            vendor_cycle = vendor.order_cycle or 30  # Default to 30 days if not set
            lead_time = vendor.lead_time_forecast or 7  # Default to 7 days if not set
            
            # Calculate safety stock based on service level and lead time variance
            service_level = item.service_level_goal or 95.0  # Default to 95%
            lead_time_variance = vendor.lead_time_variance or 20.0  # Default to 20%
            safety_factor = 1.645  # For 95% service level
            safety_stock = safety_factor * (daily_demand * lead_time_variance / 100)
            
            # Order Up To Level = Safety Stock + (Order Cycle + Lead Time) * Daily Demand
            # This ensures OUTL is always higher than IOP
            outl = safety_stock + (vendor_cycle + lead_time) * daily_demand
            
            # Ensure OUTL is at least 20% higher than IOP
            min_outl = item.item_order_point_units * 1.2
            outl = max(outl, min_outl)
            
            print(f"\nItem: {item.item_id} (ID: {item.id})")
            print(f"  Vendor: {vendor.name}")
            print(f"  Daily Demand: {daily_demand:.2f}")
            print(f"  Vendor Cycle: {vendor_cycle} days")
            print(f"  Lead Time: {lead_time} days")
            print(f"  Safety Stock: {safety_stock:.2f}")
            print(f"  IOP: {item.item_order_point_units}")
            print(f"  Previous OUTL: {item.order_up_to_level_units}")
            print(f"  New OUTL: {outl}")
            
            # Update order up to level
            item.order_up_to_level_units = outl
            item.order_up_to_level_days = vendor_cycle + lead_time
            
        # Commit changes
        session.commit()
        print("\nOrder up to levels updated successfully")

if __name__ == "__main__":
    # Initialize database
    db.initialize()
    
    # Update order levels
    update_order_levels() 