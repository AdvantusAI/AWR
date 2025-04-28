#!/usr/bin/env python
from warehouse_replenishment.db import session_scope, db
from warehouse_replenishment.models import Item
from warehouse_replenishment.services.order_service import OrderService

def show_calculations():
    db.initialize()
    with session_scope() as session:
        # Get items at order point
        items = session.query(Item).filter(
            Item.on_hand <= Item.item_order_point_units
        ).limit(3).all()
        
        print("\nItems at Order Point:")
        for item in items:
            print(f"\nItem ID: {item.id}")
            print(f"On Hand: {item.on_hand}")
            print(f"On Order: {item.on_order}")
            print(f"IOP: {item.item_order_point_units}")
            print(f"OUTL: {item.order_up_to_level_units}")
            print(f"Buying Multiple: {item.buying_multiple}")
            print(f"Min Quantity: {item.minimum_quantity}")
            print(f"4-Week Demand: {item.demand_4weekly}")
            
            # Calculate SOQ
            service = OrderService(session)
            soq_result = service.calculate_suggested_order_quantity(item.id)
            
            print("\nCalculations:")
            print(f"Balance = {soq_result['balance']} (on_hand + on_order)")
            print(f"Is at Order Point: {soq_result['is_order_point']}")
            print(f"Initial SOQ = {soq_result['soq_units']} (OUTL - balance)")
            print(f"SOQ in Days = {soq_result['soq_days']}")

if __name__ == "__main__":
    show_calculations() 