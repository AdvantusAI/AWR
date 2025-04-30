#!/usr/bin/env python
import sys
from pathlib import Path

# Add the current directory to the path so we can import our modules
current_dir = str(Path(__file__).parent)
if current_dir not in sys.path:
    sys.path.append(current_dir)

from warehouse_replenishment.db import session_scope, db
from warehouse_replenishment.models import Order, OrderItem, Item, Vendor

def show_order_details(order_id: int):
    db.initialize()
    with session_scope() as session:
        order = session.query(Order).get(order_id)
        if not order:
            print(f"Order {order_id} not found")
            return
            
        vendor = session.query(Vendor).get(order.vendor_id)
        print(f"\nOrder Details for Order {order_id}:")
        print(f"Vendor: {vendor.name} (ID: {vendor.id})")
        print(f"Status: {order.status}")
        print(f"Total Amount: ${order.independent_amount:.2f}")
        print(f"Current Bracket: {order.current_bracket}")
        
        print("\nOrder Items:")
        order_items = session.query(OrderItem).filter(OrderItem.order_id == order_id).all()
        for order_item in order_items:
            item = session.query(Item).get(order_item.item_id)
            print(f"\nItem ID: {item.id}")
            print(f"Description: {item.description}")
            print(f"On Hand: {item.on_hand}")
            print(f"On Order: {item.on_order}")
            print(f"IOP: {item.item_order_point_units}")
            print(f"OUTL: {item.order_up_to_level_units}")
            print(f"Buying Multiple: {item.buying_multiple}")
            print(f"Min Quantity: {item.minimum_quantity}")
            print(f"4-Week Demand: {item.demand_4weekly}")
            print(f"SOQ Units: {order_item.soq_units}")
            print(f"SOQ Days: {order_item.soq_days}")
            print(f"Is Frozen: {order_item.is_frozen}")
            print(f"Is Manual: {order_item.is_manual}")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        order_id = int(sys.argv[1])
        show_order_details(order_id)
    else:
        print("Please provide an order ID") 