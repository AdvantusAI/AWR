"""
Command line interface for due order management.

This module provides command line tools for identifying, analyzing, and managing
due orders based on service level requirements.
"""
import argparse
import logging
import sys
import json
from tabulate import tabulate

from services.due_order import (
    identify_due_orders,
    is_service_due_order,
    get_order_delay,
    calculate_projected_service_impact
)
from models.source import Source
from models.order import Order, OrderStatus, OrderCategory
from utils.db import get_session

logger = logging.getLogger(__name__)

def setup_logging():
    """Set up logging configuration."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('due_orders.log')
        ]
    )

def identify_orders(args):
    """Identify all due orders."""
    session = get_session()
    
    try:
        logger.info("Identifying due orders...")
        
        # Get filter parameters
        buyer_id = args.buyer_id
        store_id = args.store_id
        
        # Identify due orders
        due_orders = identify_due_orders(session, buyer_id, store_id)
        
        if not due_orders:
            logger.info("No due orders found")
            return
        
        # Display results in a table
        table_data = []
        for order in due_orders:
            table_data.append([
                order['order_id'],
                order['source_id'],
                order['source_name'],
                order['store_id'],
                order['reason'],
            ])
        
        print("\nDue Orders:")
        print(tabulate(table_data, headers=['Order ID', 'Source ID', 'Source Name', 'Store ID', 'Reason']))
        print(f"\nTotal Due Orders: {len(due_orders)}")
        
        # Update order status if requested
        if args.update:
            for due_order in due_orders:
                order_id = due_order['order_id']
                order = session.query(Order).filter(Order.id == order_id).first()
                
                if order:
                    order.status = OrderStatus.DUE
                    order.category = OrderCategory.DUE
            
            session.commit()
            logger.info(f"Updated {len(due_orders)} orders to DUE status")
    
    except Exception as e:
        logger.error(f"Error identifying due orders: {e}")
        if args.update:
            session.rollback()
    
    finally:
        session.close()

def check_order(args):
    """Check if an order for a specific source and store is due."""
    session = get_session()
    
    try:
        logger.info(f"Checking if order for source {args.source_id} in store {args.store_id} is due...")
        
        # Get source
        source = session.query(Source).filter(Source.source_id == args.source_id).first()
        
        if not source:
            logger.error(f"Source {args.source_id} not found")
            return
        
        # Check if order is due
        is_due, details = is_service_due_order(session, source.id, args.store_id)
        
        # Get order delay
        order_delay = get_order_delay(session, source.id, args.store_id)
        
        # Get impact if delayed by 1 day
        service_impact = calculate_projected_service_impact(session, source.id, args.store_id, 1)
        
        # Display results
        print(f"\nOrder for source {args.source_id} in store {args.store_id}:")
        print(f"Is Due: {'Yes' if is_due else 'No'}")
        print(f"Reason: {details.get('reason')}")
        print(f"Order Delay: {order_delay} days")
        print("\nService Impact if Delayed by 1 Day:")
        print(f"  SKUs Affected: {service_impact['skus_affected']} of {service_impact['total_skus']} ({service_impact['percent_affected']:.1f}%)")
        print(f"  High Service SKUs Affected: {service_impact['high_service_skus_affected']}")
        print(f"  Service Impact: {service_impact['service_impact'] * 100:.2f}%")
        
        # Show detailed reason
        print("\nDetailed Reason:")
        for key, value in details.items():
            if key != 'reason':
                print(f"  {key}: {value}")
        
        # Update order status if requested
        if args.update and is_due:
            # Find existing order
            order = session.query(Order).filter(
                Order.source_id == source.id,
                Order.store_id == args.store_id,
                Order.status.in_([OrderStatus.PLANNED, OrderStatus.DUE])
            ).first()
            
            if order:
                order.status = OrderStatus.DUE
                order.category = OrderCategory.DUE
                session.commit()
                logger.info(f"Updated order {order.id} to DUE status")
            else:
                logger.info("No order found to update")
    
    except Exception as e:
        logger.error(f"Error checking order: {e}")
        if args.update:
            session.rollback()
    
    finally:
        session.close()

def simulate_delay(args):
    """Simulate the impact of delaying an order by a specified number of days."""
    session = get_session()
    
    try:
        logger.info(f"Simulating delay of {args.days} days for order of source {args.source_id} in store {args.store_id}...")
        
        # Get source
        source = session.query(Source).filter(Source.source_id == args.source_id).first()
        
        if not source:
            logger.error(f"Source {args.source_id} not found")
            return
        
        # Calculate service impact
        service_impact = calculate_projected_service_impact(session, source.id, args.store_id, args.days)
        
        # Display results
        print(f"\nService Impact if Delayed by {args.days} Days:")
        print(f"  SKUs Affected: {service_impact['skus_affected']} of {service_impact['total_skus']} ({service_impact['percent_affected']:.1f}%)")
        print(f"  High Service SKUs Affected: {service_impact['high_service_skus_affected']}")
        print(f"  Service Impact: {service_impact['service_impact'] * 100:.2f}%")
        
        # If JSON output requested
        if args.json:
            print("\nJSON Output:")
            print(json.dumps(service_impact, indent=2))
    
    except Exception as e:
        logger.error(f"Error simulating delay: {e}")
    
    finally:
        session.close()

def run_nightly_check(args):
    """Run the nightly check to identify and update all due orders."""
    session = get_session()
    
    try:
        logger.info("Running nightly due order check...")
        
        # Identify all due orders
        due_orders = identify_due_orders(session)
        
        # Update order status and category for due orders
        for due_order in due_orders:
            order_id = due_order['order_id']
            order = session.query(Order).filter(Order.id == order_id).first()
            
            if order:
                order.status = OrderStatus.DUE
                order.category = OrderCategory.DUE
        
        # Update delay for all non-due orders
        from sqlalchemy import and_
        non_due_orders = session.query(Order).filter(
            and_(
                Order.status != OrderStatus.DUE,
                Order.status != OrderStatus.ACCEPTED,
                Order.status != OrderStatus.PURGED,
                Order.status != OrderStatus.DEACTIVATED
            )
        ).all()
        
        for order in non_due_orders:
            order.order_delay = get_order_delay(session, order.source_id, order.store_id)
        
        # Commit changes if not dry run
        if not args.dry_run:
            session.commit()
            logger.info(f"Updated {len(due_orders)} due orders and {len(non_due_orders)} non-due orders")
        else:
            logger.info(f"Dry run: Would have updated {len(due_orders)} due orders and {len(non_due_orders)} non-due orders")
        
        # Display results
        print(f"\nNightly Due Order Check:")
        print(f"  Due Orders: {len(due_orders)}")
        print(f"  Non-Due Orders: {len(non_due_orders)}")
        print(f"  Total Updated: {len(due_orders) + len(non_due_orders)}")
        print(f"  Mode: {'Dry Run (no changes committed)' if args.dry_run else 'Live Run (changes committed)'}")
    
    except Exception as e:
        logger.error(f"Error running nightly due order check: {e}")
        if not args.dry_run:
            session.rollback()
    
    finally:
        session.close()

def set_due(args):
    """Manually set an order as due."""
    session = get_session()
    
    try:
        logger.info(f"Setting order {args.order_id} as due...")
        
        # Get order
        order = session.query(Order).filter(Order.id == args.order_id).first()
        
        if not order:
            logger.error(f"Order {args.order_id} not found")
            return
        
        # Set as due
        order.status = OrderStatus.DUE
        order.category = OrderCategory.DUE
        
        # Commit changes
        session.commit()
        logger.info(f"Order {args.order_id} set as due")
        
        print(f"\nOrder {args.order_id} set as due")
        print(f"Source: {order.source.source_id} - {order.source.name}")
        print(f"Store: {order.store_id}")
        print(f"Reason: manual")
    
    except Exception as e:
        logger.error(f"Error setting order as due: {e}")
        session.rollback()
    
    finally:
        session.close()

def main():
    """Main entry point for the command line interface."""
    setup_logging()
    
    parser = argparse.ArgumentParser(description='Due Order Management CLI')
    subparsers = parser.add_subparsers(dest='command', help='Command to run')
    
    # Identify due orders command
    identify_parser = subparsers.add_parser('identify', help='Identify due orders')
    identify_parser.add_argument('--buyer-id', help='Filter by buyer ID')
    identify_parser.add_argument('--store-id', help='Filter by store ID')
    identify_parser.add_argument('--update', action='store_true', help='Update order status')
    
    # Check if order is due command
    check_parser = subparsers.add_parser('check', help='Check if an order is due')
    check_parser.add_argument('source_id', help='Source ID')
    check_parser.add_argument('store_id', help='Store ID')
    check_parser.add_argument('--update', action='store_true', help='Update order status if due')
    
    # Simulate delay command
    simulate_parser = subparsers.add_parser('simulate', help='Simulate order delay')
    simulate_parser.add_argument('source_id', help='Source ID')
    simulate_parser.add_argument('store_id', help='Store ID')
    simulate_parser.add_argument('--days', type=int, default=1, help='Number of days to delay')
    simulate_parser.add_argument('--json', action='store_true', help='Output in JSON format')
    
    # Run nightly check command
    nightly_parser = subparsers.add_parser('nightly', help='Run nightly due order check')
    nightly_parser.add_argument('--dry-run', action='store_true', help='Do not commit changes')
    
    # Set due command
    set_due_parser = subparsers.add_parser('set-due', help='Manually set an order as due')
    set_due_parser.add_argument('order_id', type=int, help='Order ID')
    
    args = parser.parse_args()
    
    if args.command == 'identify':
        identify_orders(args)
    elif args.command == 'check':
        check_order(args)
    elif args.command == 'simulate':
        simulate_delay(args)
    elif args.command == 'nightly':
        run_nightly_check(args)
    elif args.command == 'set-due':
        set_due(args)
    else:
        parser.print_help()

if __name__ == '__main__':
    main()