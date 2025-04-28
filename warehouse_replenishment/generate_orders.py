#!/usr/bin/env python
# generate_orders.py - Script to generate orders

import sys
import argparse
import logging
from datetime import datetime
from pathlib import Path

# Add the parent directory to the path so we can import our modules
parent_dir = str(Path(__file__).parent.parent)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from warehouse_replenishment.db import db
from warehouse_replenishment.logging_setup import get_logger
from warehouse_replenishment.services.order_service import OrderService
from warehouse_replenishment.db import session_scope

def setup_logging():
    """Setup logging for the script."""
    log_dir = Path(parent_dir) / 'logs'
    log_dir.mkdir(exist_ok=True)
    
    logger = get_logger('order_generation')
    
    # Add console handler for immediate feedback
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    logger.addHandler(console_handler)
    
    return logger

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Generate orders for warehouse replenishment')
    
    parser.add_argument('--warehouse-id', type=int, help='Generate orders for a specific warehouse')
    parser.add_argument('--vendor-id', type=int, help='Generate orders for a specific vendor')
    parser.add_argument('--include-watch', action='store_true', help='Include Watch items in orders')
    parser.add_argument('--include-manual', action='store_true', help='Include Manual items in orders')
    parser.add_argument('--verbose', '-v', action='store_true', help='Show detailed output')
    
    return parser.parse_args()

def main():
    """Main entry point for the script."""
    args = parse_args()
    logger = setup_logging()
    
    logger.info("Starting order generation")
    
    try:
        # Initialize database connection
        logger.info("Initializing database connection...")
        db.initialize()
        logger.info("Database connection initialized successfully")
        
        # Generate orders
        with session_scope() as session:
            order_service = OrderService(session)
            
            if args.vendor_id:
                logger.info(f"Generating order for vendor ID: {args.vendor_id}")
                result = order_service.generate_vendor_order(
                    vendor_id=args.vendor_id,
                    include_watch=args.include_watch,
                    include_manual=args.include_manual
                )
                
                if result['success']:
                    logger.info(f"Order {result['order_id']} generated successfully")
                    logger.info(f"Total items: {result['total_items']}")
                    logger.info(f"Total amount: {result['total_amount']:.2f}")
                else:
                    logger.warning(f"Failed to generate order: {result['message']}")
            else:
                logger.info("Generating orders for all vendors")
                if args.warehouse_id:
                    logger.info(f"Filtering by warehouse ID: {args.warehouse_id}")
                    
                result = order_service.generate_orders(
                    warehouse_id=args.warehouse_id,
                    include_watch=args.include_watch,
                    include_manual=args.include_manual
                )
                
                logger.info(f"Generated {result['generated_orders']} orders")
                logger.info(f"Total items: {result['total_items']}")
                
                if args.verbose and result['order_details']:
                    logger.info("Order details:")
                    for order in result['order_details']:
                        logger.info(f"  Order {order['order_id']} - Vendor: {order['vendor_name']} - Items: {order['total_items']}")
            
            if result.get('errors', 0) > 0:
                logger.warning(f"Encountered {result['errors']} errors during processing")
                sys.exit(1)
            
            sys.exit(0)
                
    except Exception as e:
        logger.error(f"Error generating orders: {str(e)}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()