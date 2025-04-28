#!/usr/bin/env python
# check_orders.py - Script to check database state and order generation conditions

import sys
from pathlib import Path

# Add the parent directory to the path so we can import our modules
parent_dir = str(Path(__file__).parent.parent)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from warehouse_replenishment.db import db, session_scope
from warehouse_replenishment.models import Vendor, Item, Order, VendorType, BuyerClassCode
from warehouse_replenishment.services.order_service import OrderService
from sqlalchemy import func

def check_database_state():
    """Check the current state of the database."""
    print("\n=== Database State ===")
    
    with session_scope() as session:
        # Check vendors
        total_vendors = session.query(Vendor).count()
        active_vendors = session.query(Vendor).filter(Vendor.active_items_count > 0).count()
        regular_vendors = session.query(Vendor).filter(Vendor.vendor_type == VendorType.REGULAR).count()
        
        print(f"\nVendors:")
        print(f"  Total vendors: {total_vendors}")
        print(f"  Active vendors: {active_vendors}")
        print(f"  Regular vendors: {regular_vendors}")
        
        # Check items
        total_items = session.query(Item).count()
        regular_items = session.query(Item).filter(Item.buyer_class == BuyerClassCode.REGULAR).count()
        watch_items = session.query(Item).filter(Item.buyer_class == BuyerClassCode.WATCH).count()
        manual_items = session.query(Item).filter(Item.buyer_class == BuyerClassCode.MANUAL).count()
        
        print(f"\nItems:")
        print(f"  Total items: {total_items}")
        print(f"  Regular items: {regular_items}")
        print(f"  Watch items: {watch_items}")
        print(f"  Manual items: {manual_items}")
        
        # Check items at order point
        items_at_order_point = session.query(Item).filter(
            Item.item_order_point_units > 0,
            Item.on_hand <= Item.item_order_point_units
        ).count()
        
        print(f"\nOrder Points:")
        print(f"  Items at order point: {items_at_order_point}")
        
        # Check existing orders
        total_orders = session.query(Order).count()
        open_orders = session.query(Order).filter(Order.status == 'OPEN').count()
        
        print(f"\nOrders:")
        print(f"  Total orders: {total_orders}")
        print(f"  Open orders: {open_orders}")

def check_order_generation():
    """Check order generation for a sample vendor."""
    print("\n=== Order Generation Test ===")
    
    with session_scope() as session:
        # Get first active vendor
        vendor = session.query(Vendor).filter(
            Vendor.active_items_count > 0,
            Vendor.vendor_type == VendorType.REGULAR
        ).first()
        
        if not vendor:
            print("No active regular vendors found")
            return
            
        print(f"\nTesting order generation for vendor: {vendor.name} (ID: {vendor.id})")
        
        # Initialize order service
        order_service = OrderService(session)
        
        # Try to generate order
        try:
            result = order_service.generate_vendor_order(
                vendor_id=vendor.id,
                include_watch=True,
                include_manual=True
            )
            
            print("\nOrder Generation Result:")
            print(f"  Success: {result['success']}")
            if result['success']:
                print(f"  Order ID: {result['order_id']}")
                print(f"  Total Items: {result['total_items']}")
                print(f"  Total Amount: {result['total_amount']:.2f}")
            else:
                print(f"  Message: {result['message']}")
                
        except Exception as e:
            print(f"Error generating order: {str(e)}")

def check_orders():
    db.initialize()
    with session_scope() as session:
        orders = session.query(Order).all()
        print("\nAvailable Orders:")
        for order in orders:
            print(f"Order ID: {order.id}, Vendor ID: {order.vendor_id}, Status: {order.status}")

if __name__ == "__main__":
    # Initialize database
    db.initialize()
    
    # Run checks
    check_database_state()
    check_order_generation()
    check_orders() 