#!/usr/bin/env python
# safety_stock.py - Script to manage safety stock calculations

import sys
import os
import logging
import argparse
from datetime import datetime, date, timedelta
from pathlib import Path

# Add the parent directory to the path so we can import our modules
parent_dir = str(Path(__file__).parent.parent)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from warehouse_replenishment.config import config
from warehouse_replenishment.db import db, session_scope
from warehouse_replenishment.logging_setup import get_logger
from warehouse_replenishment.models import Item, BuyerClassCode, SafetyStockType
from warehouse_replenishment.services.safety_stock_service import SafetyStockService

def setup_logging():
    """Setup logging for the script."""
    log_level = logging.INFO
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    log_dir = Path(parent_dir) / 'logs'
    log_dir.mkdir(exist_ok=True)
    
    logging.basicConfig(
        level=log_level,
        format=log_format,
        handlers=[
            logging.FileHandler(log_dir / f'safety_stock_{datetime.now().strftime("%Y%m%d")}.log'),
            logging.StreamHandler()
        ]
    )
    
    return get_logger('safety_stock')

def calculate_safety_stock(
    warehouse_id=None,
    vendor_id=None,
    item_id=None,
    service_level=None,
    update=False,
    verbose=False
):
    """Calculate safety stock for items matching criteria.
    
    Args:
        warehouse_id: Optional warehouse ID
        vendor_id: Optional vendor ID
        item_id: Optional item ID
        service_level: Optional service level override
        update: Whether to update the database
        verbose: Whether to print detailed output
        
    Returns:
        Dictionary with calculation results
    """
    logger = setup_logging()
    
    # Initialize database connection
    try:
        db.initialize()
    except Exception as e:
        logger.error(f"Error initializing database: {str(e)}")
        return {'success': False, 'error': str(e)}
    
    logger.info("Starting safety stock calculation...")
    logger.info(f"Parameters: warehouse_id={warehouse_id}, vendor_id={vendor_id}, "
                f"item_id={item_id}, service_level={service_level}, "
                f"update={update}")
    
    try:
        with session_scope() as session:
            # Create safety stock service
            ss_service = SafetyStockService(session)
            
            # If specific item ID is provided
            if item_id:
                if isinstance(item_id, int):
                    # Process single item by database ID
                    ss_result = ss_service.calculate_safety_stock_for_item(
                        item_id, 
                        service_level_override=service_level
                    )
                    
                    if update:
                        ss_service.update_safety_stock_for_item(
                            item_id,
                            update_sstf=True,
                            update_order_points=True,
                            service_level_override=service_level
                        )
                        
                    if verbose:
                        logger.info(f"Safety stock calculation for item ID {item_id}:")
                        logger.info(f"  Service Level: {ss_result['service_level']:.2f}%")
                        logger.info(f"  MADP: {ss_result['madp']:.2f}%")
                        logger.info(f"  Lead Time: {ss_result['lead_time']} days")
                        logger.info(f"  Lead Time Variance: {ss_result['lead_time_variance']:.2f}%")
                        logger.info(f"  Order Cycle: {ss_result['order_cycle']} days")
                        logger.info(f"  Safety Stock: {ss_result['safety_stock_days']:.2f} days")
                        logger.info(f"  Safety Stock: {ss_result['safety_stock_units']:.2f} units")
                        
                        if ss_result['manual_ss_applied']:
                            logger.info(f"  Manual Safety Stock Applied: {ss_result['manual_ss']} units")
                            logger.info(f"  Manual Safety Stock Type: {ss_result['manual_ss_type']}")
                    
                    return {
                        'success': True,
                        'items_processed': 1,
                        'item_result': ss_result
                    }
                else:
                    # Look up item by item code
                    item = session.query(Item).filter(Item.item_id == item_id).first()
                    if not item:
                        return {
                            'success': False,
                            'error': f"Item with code {item_id} not found"
                        }
                    
                    item_id = item.id
                    ss_result = ss_service.calculate_safety_stock_for_item(
                        item_id, 
                        service_level_override=service_level
                    )
                    
                    if update:
                        ss_service.update_safety_stock_for_item(
                            item_id,
                            update_sstf=True,
                            update_order_points=True,
                            service_level_override=service_level
                        )
                        
                    if verbose:
                        logger.info(f"Safety stock calculation for item {item.item_id}:")
                        logger.info(f"  Service Level: {ss_result['service_level']:.2f}%")
                        logger.info(f"  MADP: {ss_result['madp']:.2f}%")
                        logger.info(f"  Lead Time: {ss_result['lead_time']} days")
                        logger.info(f"  Lead Time Variance: {ss_result['lead_time_variance']:.2f}%")
                        logger.info(f"  Order Cycle: {ss_result['order_cycle']} days")
                        logger.info(f"  Safety Stock: {ss_result['safety_stock_days']:.2f} days")
                        logger.info(f"  Safety Stock: {ss_result['safety_stock_units']:.2f} units")
                        
                        if ss_result['manual_ss_applied']:
                            logger.info(f"  Manual Safety Stock Applied: {ss_result['manual_ss']} units")
                            logger.info(f"  Manual Safety Stock Type: {ss_result['manual_ss_type']}")
                    
                    return {
                        'success': True,
                        'items_processed': 1,
                        'item_result': ss_result
                    }
            
            # Process multiple items
            results = ss_service.update_safety_stock_for_all_items(
                warehouse_id=warehouse_id,
                vendor_id=vendor_id,
                update_order_points=update
            )
            
            logger.info(f"Safety stock calculation completed:")
            logger.info(f"  Total items: {results['total_items']}")
            logger.info(f"  Updated items: {results['updated_items'] if update else 0}")
            logger.info(f"  Errors: {results['errors']}")
            
            return {
                'success': True,
                **results
            }
    
    except Exception as e:
        logger.error(f"Error calculating safety stock: {str(e)}")
        logger.exception(e)
        return {
            'success': False,
            'error': str(e)
        }

def set_manual_safety_stock(
    item_id,
    manual_ss,
    ss_type='LESSER_OF',
    update_order_points=True,
    verbose=False
):
    """Set manual safety stock for an item.
    
    Args:
        item_id: Item ID
        manual_ss: Manual safety stock units
        ss_type: Safety stock type (NEVER, LESSER_OF, ALWAYS)
        update_order_points: Whether to update order points and levels
        verbose: Whether to print detailed output
        
    Returns:
        Dictionary with result of the operation
    """
    logger = setup_logging()
    
    # Initialize database connection
    try:
        db.initialize()
    except Exception as e:
        logger.error(f"Error initializing database: {str(e)}")
        return {'success': False, 'error': str(e)}
    
    logger.info("Setting manual safety stock...")
    logger.info(f"Parameters: item_id={item_id}, manual_ss={manual_ss}, "
                f"ss_type={ss_type}, update_order_points={update_order_points}")
    
    try:
        # Map string safety stock type to enum
        ss_type_map = {
            'NEVER': SafetyStockType.NEVER,
            'LESSER_OF': SafetyStockType.LESSER_OF,
            'ALWAYS': SafetyStockType.ALWAYS
        }
        
        ss_type_enum = ss_type_map.get(ss_type.upper(), SafetyStockType.LESSER_OF)
        
        with session_scope() as session:
            # Create safety stock service
            ss_service = SafetyStockService(session)
            
            # Look up item if needed
            if not isinstance(item_id, int):
                item = session.query(Item).filter(Item.item_id == item_id).first()
                if not item:
                    return {
                        'success': False,
                        'error': f"Item with code {item_id} not found"
                    }
                item_id = item.id
            
            # Set manual safety stock
            success = ss_service.set_manual_safety_stock(
                item_id,
                float(manual_ss),
                ss_type_enum,
                update_order_points
            )
            
            if success:
                # Get the updated safety stock calculation
                ss_result = ss_service.calculate_safety_stock_for_item(item_id)
                
                if verbose:
                    logger.info(f"Manual safety stock set successfully:")
                    logger.info(f"  Manual Safety Stock: {manual_ss} units")
                    logger.info(f"  Safety Stock Type: {ss_type}")
                    logger.info(f"  Resulting Safety Stock: {ss_result['safety_stock_units']:.2f} units")
                    logger.info(f"  Resulting Safety Stock: {ss_result['safety_stock_days']:.2f} days")
                
                return {
                    'success': True,
                    'item_id': item_id,
                    'manual_ss': manual_ss,
                    'ss_type': ss_type,
                    'effective_ss_units': ss_result['safety_stock_units'],
                    'effective_ss_days': ss_result['safety_stock_days']
                }
            else:
                return {
                    'success': False,
                    'error': "Failed to set manual safety stock"
                }
    
    except Exception as e:
        logger.error(f"Error setting manual safety stock: {str(e)}")
        logger.exception(e)
        return {
            'success': False,
            'error': str(e)
        }

def analyze_safety_stock_efficiency(
    item_id,
    verbose=True
):
    """Analyze safety stock efficiency for an item.
    
    Args:
        item_id: Item ID
        verbose: Whether to print detailed output
        
    Returns:
        Dictionary with analysis results
    """
    logger = setup_logging()
    
    # Initialize database connection
    try:
        db.initialize()
    except Exception as e:
        logger.error(f"Error initializing database: {str(e)}")
        return {'success': False, 'error': str(e)}
    
    logger.info(f"Analyzing safety stock efficiency for item {item_id}...")
    
    try:
        with session_scope() as session:
            # Create safety stock service
            ss_service = SafetyStockService(session)
            
            # Look up item if needed
            if not isinstance(item_id, int):
                item = session.query(Item).filter(Item.item_id == item_id).first()
                if not item:
                    return {
                        'success': False,
                        'error': f"Item with code {item_id} not found"
                    }
                item_id = item.id
                item_code = item.item_id
            else:
                item = session.query(Item).get(item_id)
                item_code = item.item_id
            
            # Analyze safety stock efficiency
            analysis = ss_service.analyze_safety_stock_efficiency(
                item_id,
                simulate_service_levels=True
            )
            
            if verbose:
                logger.info(f"Safety stock efficiency analysis for item {item_code}:")
                logger.info(f"  Current Service Level: {analysis['current_settings']['service_level']:.2f}%")
                logger.info(f"  Current Safety Stock: {analysis['current_settings']['safety_stock_days']:.2f} days")
                logger.info(f"  Current Safety Stock: {analysis['current_settings']['safety_stock_units']:.2f} units")
                logger.info(f"  Safety Stock Value: ${analysis['current_settings']['safety_stock_value']:.2f}")
                
                logger.info("  Simulated Service Levels:")
                for sim in analysis['simulated_levels']:
                    logger.info(f"    {sim['service_level']:.1f}%: {sim['safety_stock_units']:.2f} units, "
                               f"${sim['safety_stock_value']:.2f} ({sim['change_pct']:+.1f}%)")
            
            return {
                'success': True,
                'item_id': item_id,
                'item_code': item_code,
                'analysis': analysis
            }
    
    except Exception as e:
        logger.error(f"Error analyzing safety stock efficiency: {str(e)}")
        logger.exception(e)
        return {
            'success': False,
            'error': str(e)
        }

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Manage safety stock calculations')
    
    # Create subparsers for different commands
    subparsers = parser.add_subparsers(dest='command', help='Commands')
    
    # Calculate command
    calculate_parser = subparsers.add_parser('calculate', help='Calculate safety stock')
    calculate_parser.add_argument('--warehouse-id', type=str, help='Warehouse ID to filter items')
    calculate_parser.add_argument('--vendor-id', type=str, help='Vendor ID to filter items')
    calculate_parser.add_argument('--item-id', type=str, help='Specific item ID to calculate')
    calculate_parser.add_argument('--service-level', type=float, help='Service level override')
    calculate_parser.add_argument('--update', action='store_true', help='Update items with calculated values')
    calculate_parser.add_argument('--verbose', '-v', action='store_true', help='Display detailed output')
    
    # Set manual safety stock command
    manual_parser = subparsers.add_parser('manual', help='Set manual safety stock')
    manual_parser.add_argument('--item-id', type=str, required=True, help='Item ID')
    manual_parser.add_argument('--value', type=float, required=True, help='Manual safety stock units')
    manual_parser.add_argument('--type', type=str, choices=['NEVER', 'LESSER_OF', 'ALWAYS'], 
                              default='LESSER_OF', help='Safety stock type')
    manual_parser.add_argument('--no-update', action='store_true', 
                              help='Do not update order points and levels')
    manual_parser.add_argument('--verbose', '-v', action='store_true', help='Display detailed output')
    
    # Analyze command
    analyze_parser = subparsers.add_parser('analyze', help='Analyze safety stock efficiency')
    analyze_parser.add_argument('--item-id', type=str, required=True, help='Item ID')
    analyze_parser.add_argument('--verbose', '-v', action='store_true', help='Display detailed output')
    
    return parser.parse_args()

def main():
    """Main entry point for the script."""
    args = parse_args()
    
    if args.command == 'calculate':
        result = calculate_safety_stock(
            warehouse_id=args.warehouse_id,
            vendor_id=args.vendor_id,
            item_id=args.item_id,
            service_level=args.service_level,
            update=args.update,
            verbose=args.verbose
        )
    elif args.command == 'manual':
        result = set_manual_safety_stock(
            item_id=args.item_id,
            manual_ss=args.value,
            ss_type=args.type,
            update_order_points=not args.no_update,
            verbose=args.verbose
        )
    elif args.command == 'analyze':
        result = analyze_safety_stock_efficiency(
            item_id=args.item_id,
            verbose=args.verbose
        )
    else:
        print("Please specify a command: calculate, manual, or analyze")
        sys.exit(1)
    
    # Exit with appropriate status code
    if result and result.get('success', False):
        sys.exit(0)
    else:
        sys.exit(1)

if __name__ == "__main__":
    main()