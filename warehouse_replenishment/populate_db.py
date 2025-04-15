#!/usr/bin/env python
# populate_db.py - Script to populate the AWR database with sample data

import sys
import os
from datetime import date, datetime, timedelta
from pathlib import Path
import random
import logging

# Add the parent directory to the path so we can import our modules
parent_dir = str(Path(__file__).parent.parent)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from warehouse_replenishment.config import config
from warehouse_replenishment.db import db, session_scope
from warehouse_replenishment.models import (
    Company, Warehouse, Vendor, VendorBracket, Item, DemandHistory,
    ItemPrice, Order, OrderItem, SeasonalProfile, SeasonalProfileIndex,
    BuyerClassCode, SystemClassCode, VendorType, ForecastMethod, SafetyStockType
)

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
app_logger = logging.getLogger('populate_db')

# Sample data constants
BUYER_IDS = ['B001', 'B002', 'B003', 'B004']
VENDOR_GROUP_CODES = ['GROCERY', 'ELECTRONICS', 'CLOTHING', 'HARDWARE', 'PERISHABLE']
ITEM_GROUP_CODES = ['FOOD', 'DAIRY', 'MEAT', 'PRODUCE', 'BAKERY', 'HOUSEHOLD', 'ELECTRONICS']
WAREHOUSE_COUNT = 2
VENDORS_PER_WAREHOUSE = 5
ITEMS_PER_VENDOR = 20
HISTORY_PERIODS = 13  # For 4-weekly periodicity (1 year)

def create_company():
    """Create the company record."""
    app_logger.info("Creating company record...")
    
    with session_scope() as session:
        # Check if company record exists
        existing_company = session.query(Company).first()
        if existing_company:
            app_logger.info("Company record already exists.")
            return existing_company.id
        
        company = Company(
            name="AWR Sample Company",
            basic_alpha_factor=10.0,
            demand_from_days_out=1,
            lumpy_demand_limit=50.0,
            slow_mover_limit=10.0,
            demand_filter_high=5.0,
            demand_filter_low=3.0,
            tracking_signal_limit=55.0,
            op_prime_limit_pct=95.0,
            forecast_demand_limit=5.0,
            update_frequency_impact_control=2,
            service_level_goal=95.0,
            borrowing_rate=5.0,
            capital_cost_rate=25.0,
            physical_carrying_cost=15.0,
            other_rate=0.0,
            total_carrying_rate=40.0,
            gross_margin=35.0,
            overhead_rate=25.0,
            cost_of_lost_sales=100.0,
            order_header_cost=25.0,
            order_line_cost=1.0,
            forward_buy_maximum=60,
            forward_buy_filter=30,
            discount_effect_rate=100.0,
            advertising_effect_rate=100.0,
            keep_old_tb_parms_days=30,
            keep_archived_exceptions_days=90,
            lead_time_forecast_control=1,
            history_periodicity_default=13,
            forecasting_periodicity_default=13
        )
        session.add(company)
        session.flush()  # Force ID assignment
        app_logger.info("Company record created.")
        return company.id

def create_warehouses(company_id):
    """Create warehouse records."""
    app_logger.info("Creating warehouse records...")
    
    warehouses = []
    
    with session_scope() as session:
        for i in range(1, WAREHOUSE_COUNT + 1):
            warehouse_id = f'WH{i:03d}'
            name = f'Warehouse {i}'
            
            # Check if warehouse already exists
            existing_warehouse = session.query(Warehouse).filter(
                Warehouse.warehouse_id == warehouse_id
            ).first()
            
            if existing_warehouse:
                app_logger.info(f"Warehouse {warehouse_id} already exists.")
                warehouses.append(existing_warehouse)
                continue
            
            warehouse = Warehouse(
                warehouse_id=warehouse_id,
                name=name,
                company_id=company_id,
                service_level_goal=95.0,
                lead_time_forecast_control=1,
                warehouse_control_factors_active=False
            )
            session.add(warehouse)
            session.flush()
            warehouses.append(warehouse)
            app_logger.info(f"Created warehouse: {name}")
    
    return warehouses

def create_vendors(warehouse_ids):
    """Create vendor records."""
    app_logger.info("Creating vendor records...")
    
    vendors = []
    
    with session_scope() as session:
        for warehouse_id in warehouse_ids:
            for i in range(1, VENDORS_PER_WAREHOUSE + 1):
                vendor_id = f'V{warehouse_id[-3:]}{i:03d}'
                name = f'Vendor {vendor_id}'
                
                # Check if vendor already exists
                existing_vendor = session.query(Vendor).filter(
                    Vendor.vendor_id == vendor_id,
                    Vendor.warehouse_id == warehouse_id
                ).first()
                
                if existing_vendor:
                    app_logger.info(f"Vendor {vendor_id} already exists for warehouse {warehouse_id}.")
                    vendors.append(existing_vendor)
                    continue
                
                # Create vendor with varied characteristics
                lead_time = random.randint(3, 21)
                lead_time_variance = random.uniform(10.0, 30.0)
                order_cycle = random.choice([7, 14, 21, 28])
                automatic_rebuild = random.choice([0, 2, 4, 5])
                service_level_goal = random.uniform(90.0, 99.0)
                
                vendor = Vendor(
                    vendor_id=vendor_id,
                    name=name,
                    warehouse_id=warehouse_id,
                    service_level_goal=service_level_goal,
                    order_cycle=order_cycle,
                    sub_vendor_approval=False,
                    buyer_id=random.choice(BUYER_IDS),
                    vendor_type=VendorType.REGULAR,
                    lead_time_quoted=lead_time,
                    lead_time_forecast=lead_time,
                    lead_time_variance=lead_time_variance,
                    current_bracket=1,
                    automatic_rebuild=automatic_rebuild,
                    vendor_group_codes=random.choice(VENDOR_GROUP_CODES),
                    order_days_in_week=None,
                    week=0,
                    history_periodicity=13,
                    forecasting_periodicity=13
                )
                session.add(vendor)
                session.flush()
                
                # Create brackets for this vendor
                create_vendor_brackets(session, vendor.id)
                
                vendors.append(vendor)
                app_logger.info(f"Created vendor: {name} for warehouse {warehouse_id}")
    
    return vendors

def create_vendor_brackets(session, vendor_id):
    """Create brackets for a vendor."""
    # Bracket 1 (base bracket)
    bracket1 = VendorBracket(
        vendor_id=vendor_id,
        bracket_number=1,
        minimum=1000.0,
        maximum=5000.0,
        unit=1,  # amount
        up_to_max_option=0,
        discount=0.0
    )
    session.add(bracket1)
    
    # Bracket 2 (medium discount)
    bracket2 = VendorBracket(
        vendor_id=vendor_id,
        bracket_number=2,
        minimum=5001.0,
        maximum=10000.0,
        unit=1,  # amount
        up_to_max_option=2,  # Partial/Balanced
        discount=2.0
    )
    session.add(bracket2)
    
    # Bracket 3 (high discount)
    bracket3 = VendorBracket(
        vendor_id=vendor_id,
        bracket_number=3,
        minimum=10001.0,
        maximum=0.0,  # No maximum
        unit=1,  # amount
        up_to_max_option=2,  # Partial/Balanced
        discount=5.0
    )
    session.add(bracket3)

def create_items(vendor_data):
    """Create item records."""
    app_logger.info("Creating item records...")
    
    all_items = []
    
    with session_scope() as session:
        for vendor_id, warehouse_id in vendor_data:
            for i in range(1, ITEMS_PER_VENDOR + 1):
                item_id = f'I{vendor_id[-3:]}{i:03d}'
                description = f'Item {item_id}'
                
                # Check if item already exists
                existing_item = session.query(Item).filter(
                    Item.item_id == item_id,
                    Item.vendor_id == vendor_id,
                    Item.warehouse_id == warehouse_id
                ).first()
                
                if existing_item:
                    app_logger.info(f"Item {item_id} already exists for vendor {vendor_id}.")
                    all_items.append(existing_item)
                    continue
                
                # Create item with varied characteristics
                purchase_price = round(random.uniform(1.0, 100.0), 2)
                sales_price = round(purchase_price * (1 + random.uniform(0.2, 0.5)), 2)
                
                # Determine system class and forecast values based on random patterns
                system_class = random.choices(
                    [SystemClassCode.REGULAR, SystemClassCode.SLOW, SystemClassCode.LUMPY, SystemClassCode.NEW],
                    weights=[0.6, 0.2, 0.15, 0.05],
                    k=1
                )[0]
                
                buyer_class = random.choices(
                    [BuyerClassCode.REGULAR, BuyerClassCode.WATCH, BuyerClassCode.MANUAL],
                    weights=[0.8, 0.15, 0.05],
                    k=1
                )[0]
                
                # Set forecast based on system class
                if system_class == SystemClassCode.SLOW:
                    demand_4weekly = round(random.uniform(1.0, 10.0), 2)
                    madp = round(random.uniform(10.0, 30.0), 2)
                elif system_class == SystemClassCode.LUMPY:
                    demand_4weekly = round(random.uniform(10.0, 50.0), 2)
                    madp = round(random.uniform(50.0, 90.0), 2)
                else:  # REGULAR or NEW
                    demand_4weekly = round(random.uniform(20.0, 200.0), 2)
                    madp = round(random.uniform(5.0, 40.0), 2)
                
                # Calculate other forecast values
                demand_weekly = round(demand_4weekly / 4.0, 2)
                demand_monthly = round(demand_4weekly * (365/12) / (365/13), 2)
                demand_quarterly = round(demand_4weekly * 3, 2)
                demand_yearly = round(demand_4weekly * 13, 2)
                
                # Track (trending signal)
                track = round(random.uniform(0.0, 50.0), 2)
                
                # Lead time and service level
                lead_time_forecast = random.randint(3, 21)
                lead_time_variance = round(random.uniform(10.0, 30.0), 2)
                service_level_goal = round(random.uniform(90.0, 99.0), 2)
                
                # Calculate safety stock time factor (SSTF)
                sstf = round(random.uniform(3.0, 10.0), 2)
                
                # Calculate order points and levels
                lead_time_days = lead_time_forecast
                safety_stock_days = sstf
                item_order_point_days = lead_time_days + safety_stock_days
                
                # Get vendor order cycle
                vendor = session.query(Vendor).filter(Vendor.id == vendor_id).first()
                vendor_order_cycle = vendor.order_cycle if vendor else 14
                
                # Item cycle - simulate variance from vendor cycle
                item_cycle_days = max(
                    vendor_order_cycle,
                    round(vendor_order_cycle * random.uniform(0.8, 1.5))
                )
                
                # Order up to level days
                order_up_to_level_days = item_order_point_days + max(vendor_order_cycle, item_cycle_days)
                
                # Compute units based on daily forecast
                daily_forecast = demand_4weekly / 28
                item_order_point_units = item_order_point_days * daily_forecast
                item_cycle_units = item_cycle_days * daily_forecast
                order_up_to_level_units = order_up_to_level_days * daily_forecast
                
                # Generate random inventory values
                on_hand = round(random.uniform(0, order_up_to_level_units), 2)
                on_order = 0
                if on_hand < item_order_point_units:
                    # Generate a pending order if below order point
                    on_order = round(order_up_to_level_units - on_hand, 2)
                
                # Create the item record
                item = Item(
                    item_id=item_id,
                    description=description,
                    vendor_id=vendor_id,
                    warehouse_id=warehouse_id,
                    
                    # Item detail
                    service_level_goal=service_level_goal,
                    service_level_maintained=True,
                    service_level_attained=service_level_goal * random.uniform(0.9, 1.0),
                    
                    # Stock status
                    on_hand=on_hand,
                    on_order=on_order,
                    customer_back_order=0.0,
                    reserved=0.0,
                    held_until=None,
                    quantity_held=0.0,
                    
                    # Lead time
                    lead_time_forecast=lead_time_forecast,
                    lead_time_variance=lead_time_variance,
                    lead_time_maintained=True,
                    calculated_in_days=lead_time_forecast,
                    calculated_variance=lead_time_variance,
                    
                    # Item parameters
                    units_per_case=random.choice([1, 6, 12, 24]),
                    weight_per_unit=round(random.uniform(0.1, 10.0), 2),
                    volume_per_unit=round(random.uniform(0.1, 5.0), 2),
                    buying_multiple=random.choice([1, 6, 12]),
                    minimum_quantity=1.0,
                    shelf_life_days=random.choice([0, 30, 60, 90]),
                    
                    # Demand forecasting
                    buyer_class=buyer_class,
                    system_class=system_class,
                    forecast_method=ForecastMethod.E3_REGULAR_AVS,
                    forecasting_periodicity=13,
                    history_periodicity=13,
                    
                    # Item classification
                    item_group_codes=random.choice(ITEM_GROUP_CODES),
                    
                    # Forecast data
                    demand_weekly=demand_weekly,
                    demand_4weekly=demand_4weekly,
                    demand_monthly=demand_monthly,
                    demand_quarterly=demand_quarterly,
                    demand_yearly=demand_yearly,
                    forecast_date=datetime.now(),
                    madp=madp,
                    track=track,
                    sstf=sstf,
                    
                    # Price information
                    purchase_price=purchase_price,
                    purchase_price_divisor=1.0,
                    sales_price=sales_price,
                    
                    # Calculated fields for ordering
                    item_order_point_units=item_order_point_units,
                    item_order_point_days=item_order_point_days,
                    vendor_order_point_days=lead_time_days + safety_stock_days + vendor_order_cycle,
                    order_up_to_level_units=order_up_to_level_units,
                    order_up_to_level_days=order_up_to_level_days,
                    item_cycle_units=item_cycle_units,
                    item_cycle_days=item_cycle_days
                )
                session.add(item)
                session.flush()
                
                # Update vendor active items count
                if vendor and buyer_class in [BuyerClassCode.REGULAR, BuyerClassCode.WATCH]:
                    vendor.active_items_count = vendor.active_items_count + 1
                
                all_items.append(item)
                app_logger.info(f"Created item: {description} for vendor {vendor_id}")
                
                # Generate history for this item
                create_item_history(session, item)
                
                # Determine if item needs a seasonal profile (20% of items)
                if random.random() < 0.2:
                    # Create or assign seasonal profile
                    profile_id = f"PROF{random.randint(1,5):03d}"
                    
                    # Check if profile exists
                    profile = session.query(SeasonalProfile).filter(
                        SeasonalProfile.profile_id == profile_id
                    ).first()
                    
                    if not profile:
                        # Create new profile
                        create_seasonal_profile(session, profile_id)
                    
                    # Assign profile to item
                    item.demand_profile = profile_id
    
    return all_items

def create_item_history(session, item):
    """Create history records for an item."""
    # Get today and calculate first period start date (1 year ago)
    today = date.today()
    first_period_start = today - timedelta(days=365)
    
    # Base demand (from the item's forecast)
    base_demand = item.demand_4weekly
    
    # Create history for multiple periods
    for period in range(1, HISTORY_PERIODS + 1):
        period_year = first_period_start.year
        
        # Calculate shipped, lost, and promo based on patterns
        # Add some randomness to create realistic history
        variation_factor = random.uniform(0.7, 1.3)
        
        # Base shipment for this period
        shipped = base_demand * variation_factor
        
        # Sometimes add spikes to create MADP and tracking signal exceptions
        if random.random() < 0.1:  # 10% chance of spike
            shipped *= random.uniform(1.5, 3.0)
        
        # Calculate lost sales (occasionally)
        lost_sales = 0
        if random.random() < 0.15:  # 15% chance of lost sales
            lost_sales = base_demand * random.uniform(0.05, 0.2)
        
        # Calculate promotional demand (occasionally)
        promotional_demand = 0
        if random.random() < 0.2:  # 20% chance of promotional demand
            promotional_demand = base_demand * random.uniform(0.1, 0.5)
        
        # Add randomness to out of stock days
        out_of_stock_days = 0
        if random.random() < 0.1:  # 10% chance of out of stock
            out_of_stock_days = random.randint(1, 5)
        
        # Create history record
        history = DemandHistory(
            item_id=item.id,
            period_number=period,
            period_year=period_year,
            shipped=round(shipped, 2),
            lost_sales=round(lost_sales, 2),
            promotional_demand=round(promotional_demand, 2),
            total_demand=round(shipped + lost_sales - promotional_demand, 2),
            out_of_stock_days=out_of_stock_days,
            is_ignored=False,
            is_adjusted=False
        )
        session.add(history)
    
    # Occasionally ignore a random period
    if random.random() < 0.1:  # 10% chance of ignored period
        random_period = random.randint(1, HISTORY_PERIODS)
        ignore_history = session.query(DemandHistory).filter(
            DemandHistory.item_id == item.id,
            DemandHistory.period_number == random_period
        ).first()
        
        if ignore_history:
            ignore_history.is_ignored = True

def create_seasonal_profile(session, profile_id):
    """Create a seasonal profile."""
    # Create profile record
    profile = SeasonalProfile(
        profile_id=profile_id,
        description=f"Seasonal Profile {profile_id}",
        periodicity=13  # 4-weekly
    )
    session.add(profile)
    
    # Generate seasonal indices
    # Create different patterns (winter peak, summer peak, etc)
    pattern_type = random.choice(['winter', 'summer', 'bimodal', 'gradual'])
    
    indices = []
    if pattern_type == 'winter':
        # Winter-peaking pattern (periods 11-13 and 1-2 are high)
        base_values = [0.7, 0.8, 0.9, 1.0, 1.1, 1.0, 0.9, 0.8, 0.7, 0.8, 1.2, 1.5, 1.3]
    elif pattern_type == 'summer':
        # Summer-peaking pattern (periods 6-8 are high)
        base_values = [0.7, 0.8, 0.9, 1.0, 1.1, 1.4, 1.6, 1.4, 1.0, 0.8, 0.7, 0.8, 0.8]
    elif pattern_type == 'bimodal':
        # Two peaks (spring and fall)
        base_values = [0.8, 1.0, 1.3, 1.2, 0.9, 0.7, 0.8, 0.9, 1.1, 1.4, 1.2, 0.9, 0.8]
    else:  # gradual
        # Gradual increase and decrease
        base_values = [0.7, 0.8, 0.9, 1.0, 1.1, 1.2, 1.3, 1.2, 1.1, 1.0, 0.9, 0.8, 0.7]
    
    # Add some randomness to the pattern
    indices = [round(v * random.uniform(0.95, 1.05), 2) for v in base_values]
    
    # Normalize to ensure average is approximately 1.0
    total = sum(indices)
    indices = [round(v * 13 / total, 2) for v in indices]
    
    # Create index records
    for period, index_value in enumerate(indices, 1):
        index = SeasonalProfileIndex(
            profile_id=profile_id,
            period_number=period,
            index_value=index_value
        )
        session.add(index)

def create_orders(vendor_data):
    """Create sample orders."""
    app_logger.info("Creating sample orders...")
    
    orders = []
    
    with session_scope() as session:
        for vendor_id, warehouse_id in vendor_data:
            # Decide if we should create an order for this vendor
            if random.random() < 0.7:  # 70% chance of having an order
                # Determine order status
                status_choice = random.random()
                if status_choice < 0.6:  # 60% chance of open order
                    status = 'OPEN'
                    is_due = random.random() < 0.4  # 40% chance of due order
                    is_order_point_a = not is_due and random.random() < 0.3  # 30% chance of order point A
                    is_order_point = not (is_due or is_order_point_a) and random.random() < 0.5  # 50% chance of order point
                    order_delay = random.randint(0, 14) if not is_due else 0
                else:
                    status = 'ACCEPTED'
                    is_due = False
                    is_order_point_a = False
                    is_order_point = False
                    order_delay = 0
                
                # Create the order
                order = Order(
                    vendor_id=vendor_id,
                    warehouse_id=warehouse_id,
                    order_date=datetime.now() - timedelta(days=random.randint(0, 7)),
                    is_due=is_due,
                    is_order_point_a=is_order_point_a,
                    is_order_point=is_order_point,
                    order_delay=order_delay,
                    status=status,
                    expected_delivery_date=date.today() + timedelta(days=random.randint(3, 21))
                )
                
                if status == 'ACCEPTED':
                    order.approval_date = datetime.now() - timedelta(days=random.randint(1, 3))
                
                session.add(order)
                session.flush()
                
                # Add order items - get all items for this vendor
                items = session.query(Item).filter(
                    Item.vendor_id == vendor_id,
                    Item.warehouse_id == warehouse_id
                ).all()
                
                # Randomly select items to include in the order
                selected_items = random.sample(items, min(len(items), random.randint(5, len(items))))
                
                total_amount = 0
                total_eaches = 0
                
                for item in selected_items:
                    # Calculate SOQ based on the item's OUTL and balance
                    soq_units = max(0, item.order_up_to_level_units - (item.on_hand + item.on_order))
                    
                    # If item doesn't need ordering, sometimes order anyway for forward buy
                    if soq_units <= 0 and random.random() < 0.2:
                        soq_units = random.uniform(0.1, 0.5) * item.order_up_to_level_units
                    
                    # Round to buying multiple
                    if item.buying_multiple > 1:
                        soq_units = math.ceil(soq_units / item.buying_multiple) * item.buying_multiple
                    
                    # Skip if still zero
                    if soq_units <= 0:
                        continue
                    
                    # Calculate SOQ days
                    daily_demand = item.demand_4weekly / 28
                    soq_days = round(soq_units / daily_demand, 1) if daily_demand > 0 else 0
                    
                    # Create order item
                    order_item = OrderItem(
                        order_id=order.id,
                        item_id=item.id,
                        soq_units=soq_units,
                        soq_days=soq_days,
                        is_frozen=random.random() < 0.1,  # 10% chance of frozen SOQ
                        is_order_point=item.on_hand < item.item_order_point_units,
                        is_manual=False,
                        is_deal=False,
                        is_planned=False,
                        is_forward_buy=False,
                        item_order_point_units=item.item_order_point_units,
                        balance_units=item.on_hand + item.on_order,
                        order_up_to_level_units=item.order_up_to_level_units
                    )
                    session.add(order_item)
                    
                    # Update order totals
                    total_amount += soq_units * item.purchase_price
                    total_eaches += soq_units
                
                # Update order totals in all columns
                order.independent_amount = total_amount
                order.independent_eaches = total_eaches
                order.auto_adj_amount = total_amount
                order.auto_adj_eaches = total_eaches
                order.final_adj_amount = total_amount
                order.final_adj_eaches = total_eaches
                
                # Occasionally add extra days
                if random.random() < 0.3:
                    extra_days = random.randint(1, 10)
                    order.extra_days = extra_days
                    # Adjust final amount to simulate additional days
                    order.final_adj_amount = total_amount * (1 + extra_days / 30)
                    order.final_adj_eaches = total_eaches * (1 + extra_days / 30)
                
                # Add order checks
                order.order_point_checks = sum(1 for item in selected_items if item.on_hand < item.item_order_point_units)
                order.watch_checks = sum(1 for item in selected_items if item.buyer_class == BuyerClassCode.WATCH)
                
                orders.append(order)
                app_logger.info(f"Created order for vendor {vendor_id} with {len(selected_items)} items")
    
    return orders

def main():
    """Main function to populate the database."""
    app_logger.info("Starting database population...")
    
    # Initialize database connection
    db.initialize()
    
    try:
        # Create company
        company_id = create_company()
        
        # Create warehouses
        warehouses = create_warehouses(company_id)
        warehouse_ids = [w.id for w in warehouses]
        
        # Create vendors
        vendors = create_vendors(warehouse_ids)
        vendor_data = [(v.id, v.warehouse_id) for v in vendors]
        
        # Create items
        items = create_items(vendor_data)
        
        # Create orders
        orders = create_orders(vendor_data)
        
        app_logger.info(f"Database population completed successfully.")
        app_logger.info(f"Created {len(warehouses)} warehouses")
        app_logger.info(f"Created {len(vendors)} vendors")
        app_logger.info(f"Created {len(items)} items")
        app_logger.info(f"Created {len(orders)} orders")
        
        return True
    
    except Exception as e:
        app_logger.error(f"Error populating database: {str(e)}")
        app_logger.exception(e)
        return False

if __name__ == "__main__":
    main()