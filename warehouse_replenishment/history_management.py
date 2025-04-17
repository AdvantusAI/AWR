#!/usr/bin/env python
# history_management.py - Script for history management operations

import sys
import argparse
from datetime import datetime, date
from pathlib import Path

# Add the parent directory to the path
parent_dir = str(Path(__file__).parent.parent)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from warehouse_replenishment.db import db, session_scope
from warehouse_replenishment.logging_setup import get_logger
from warehouse_replenishment.services.history_manager import HistoryManager

def parse_args():
    parser = argparse.ArgumentParser(description='Manage demand history')
    
    subparsers = parser.add_subparsers(dest='command', help='Commands')
    
    # Create history period command
    create_parser = subparsers.add_parser('create_period', help='Create a new history period')
    create_parser.add_argument('--item-id', type=int, required=True, help='Item ID')
    create_parser.add_argument('--period-number', type=int, required=True, help='Period number')
    create_parser.add_argument('--period-year', type=int, required=True, help='Period year')
    create_parser.add_argument('--shipped', type=float, default=0.0, help='Shipped quantity')
    create_parser.add_argument('--lost-sales', type=float, default=0.0, help='Lost sales quantity')
    create_parser.add_argument('--promotional-demand', type=float, default=0.0, help='Promotional demand')
    create_parser.add_argument('--out-of-stock-days', type=int, default=0, help='Out of stock days')
    
    # Purge old history command
    purge_parser = subparsers.add_parser('purge', help='Purge old history')
    purge_parser.add_argument('--keep-periods', type=int, default=52, 
                            help='Number of periods to keep')
    
    # Archive exceptions command
    archive_parser = subparsers.add_parser('archive', help='Archive resolved exceptions')
    archive_parser.add_argument('--days-to-keep', type=int, default=90,
                              help='Days to keep resolved exceptions')
    
    # Copy history command
    copy_parser = subparsers.add_parser('copy', help='Copy history between items')
    copy_parser.add_argument('--source-item', type=int, required=True, help='Source item ID')
    copy_parser.add_argument('--target-item', type=int, required=True, help='Target item ID')
    copy_parser.add_argument('--multiple', type=float, default=1.0, help='Multiple to apply')
    copy_parser.add_argument('--include-ignored', action='store_true', help='Include ignored periods')
    
    # Update history command
    update_parser = subparsers.add_parser('update', help='Update a history period')
    update_parser.add_argument('--item-id', type=int, required=True, help='Item ID')
    update_parser.add_argument('--period-number', type=int, required=True, help='Period number')
    update_parser.add_argument('--period-year', type=int, required=True, help='Period year')
    update_parser.add_argument('--shipped', type=float, help='New shipped quantity')
    update_parser.add_argument('--lost-sales', type=float, help='New lost sales quantity')
    update_parser.add_argument('--promotional-demand', type=float, help='New promotional demand')
    update_parser.add_argument('--out-of-stock-days', type=int, help='New out of stock days')
    update_parser.add_argument('--ignore', action='store_true', help='Ignore this period')
    update_parser.add_argument('--unignore', action='store_true', help='Unignore this period')
    
    return parser.parse_args()

def main():
    args = parse_args()
    
    # Initialize database
    db.initialize()
    
    logger = get_logger('history_management')
    
    if not args.command:
        logger.error("Please specify a command. Use --help for available commands.")
        sys.exit(1)
        
    try:
        with session_scope() as session:
            history_manager = HistoryManager(session)
            
            if args.command == 'create_period':
                logger.info(f"Creating history period for item {args.item_id}, period {args.period_number}/{args.period_year}")
                
                # Call create_history_period method
                period_id = history_manager.create_history_period(
                    item_id=args.item_id,
                    period_number=args.period_number,
                    period_year=args.period_year,
                    shipped=args.shipped,
                    lost_sales=args.lost_sales,
                    promotional_demand=args.promotional_demand,
                    out_of_stock_days=args.out_of_stock_days
                )
                
                logger.info(f"Created history period with ID: {period_id}")
                
            elif args.command == 'update':
                logger.info(f"Updating history period for item {args.item_id}, period {args.period_number}/{args.period_year}")
                
                # Handle ignore/unignore flags
                if args.ignore and args.unignore:
                    logger.error("Cannot both ignore and unignore a period")
                    sys.exit(1)
                
                if args.ignore:
                    success = history_manager.ignore_history_period(
                        args.item_id, args.period_number, args.period_year
                    )
                    logger.info("Period ignored successfully" if success else "Failed to ignore period")
                elif args.unignore:
                    success = history_manager.unignore_history_period(
                        args.item_id, args.period_number, args.period_year
                    )
                    logger.info("Period unignored successfully" if success else "Failed to unignore period")
                else:
                    # Update period data
                    success = history_manager.update_history_period(
                        item_id=args.item_id,
                        period_number=args.period_number,
                        period_year=args.period_year,
                        shipped=args.shipped,
                        lost_sales=args.lost_sales,
                        promotional_demand=args.promotional_demand,
                        out_of_stock_days=args.out_of_stock_days
                    )
                    logger.info("Period updated successfully" if success else "Failed to update period")
                
            elif args.command == 'purge':
                logger.info(f"Purging old history, keeping {args.keep_periods} periods")
                results = history_manager.purge_old_history(keep_periods=args.keep_periods)
                logger.info(f"Purged {results['purged_periods']} of {results['total_periods']} periods")
                
            elif args.command == 'archive':
                logger.info(f"Archiving resolved exceptions older than {args.days_to_keep} days")
                results = history_manager.archive_resolved_exceptions(days_to_keep=args.days_to_keep)
                logger.info(f"Archived {results['archived_exceptions']} of {results['total_exceptions']} exceptions")
                
            elif args.command == 'copy':
                logger.info(f"Copying history from item {args.source_item} to item {args.target_item}")
                results = history_manager.copy_history_between_items(
                    source_item_id=args.source_item,
                    target_item_id=args.target_item,
                    apply_multiple=args.multiple,
                    include_ignored=args.include_ignored
                )
                logger.info(f"Copied {results['copied_periods']} periods, updated {results['updated_periods']} periods")
                
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        sys.exit(1)
        
    logger.info("Operation completed successfully")
    sys.exit(0)

if __name__ == "__main__":
    main()