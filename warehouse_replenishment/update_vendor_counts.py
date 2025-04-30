#!/usr/bin/env python
# update_vendor_counts.py - Script to update vendor active item counts

import sys
from pathlib import Path

# Add the parent directory to the path so we can import our modules
parent_dir = str(Path(__file__).parent)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from warehouse_replenishment.db import db, session_scope
from warehouse_replenishment.models import Vendor, Item, BuyerClassCode
from sqlalchemy import func

def update_vendor_counts():
    """Update active item counts for all vendors."""
    print("\n=== Updating Vendor Active Item Counts ===")
    
    with session_scope() as session:
        # Get all vendors
        vendors = session.query(Vendor).all()
        
        for vendor in vendors:
            # Count active items for this vendor
            active_items = session.query(Item).filter(
                Item.vendor_id == vendor.id,
                Item.buyer_class != 'D'
            ).count()
            
            print(f"\nVendor: {vendor.name} (ID: {vendor.id})")
            print(f"  Previous active items count: {vendor.active_items_count}")
            print(f"  Actual active items count: {active_items}")
            
            # Update vendor's active items count
            vendor.active_items_count = active_items
            
        # Commit changes
        session.commit()
        print("\nVendor counts updated successfully")

if __name__ == "__main__":
    # Initialize database
    db.initialize()
    
    # Update vendor counts
    update_vendor_counts() 