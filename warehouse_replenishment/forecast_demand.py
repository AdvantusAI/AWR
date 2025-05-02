#!/usr/bin/env python
# forecast_demand.py - Script to run demand forecasting for all or selected items

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
from warehouse_replenishment.models import Item, BuyerClassCode, ForecastMethod
from warehouse_replenishment.services.forecast_service import ForecastService
from warehouse_replenishment.utils.date_utils import get_current_period, get_previous_period
from warehouse_replenishment.core.demand_forecast import (
    calculate_forecast, calculate_madp_from_history,
    calculate_track_from_history, apply_seasonality_to_forecast
)

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
            logging.FileHandler(log_dir / f'forecast_{datetime.now().strftime("%Y%m%d")}.log'),
            logging.StreamHandler()
        ]
    )
    
    return get_logger('forecast_demand')

def run_forecast(
    warehouse_id=None, 
    vendor_id=None, 
    buyer_id=None,
    item_id=None,
    periods=12,
    update=False,
    recalculate_madp=True,
    include_inactive=False,
    dry_run=False,
    verbose=False
):
    """Run demand forecasting on items matching the specified criteria.
    
    Args:
        warehouse_id: Optional warehouse ID to filter items
        vendor_id: Optional vendor ID to filter items
        buyer_id: Optional buyer ID to filter items
        item_id: Optional specific item ID to forecast
        periods: Number of periods to consider
        update: Whether to update items with new forecasts
        recalculate_madp: Whether to recalculate MADP and track
        include_inactive: Whether to include inactive items
        dry_run: If True, simulates the run without making changes
        verbose: Whether to print detailed output
        
    Returns:
        Dictionary with forecasting results
    """
    logger = setup_logging()
    
    # Initialize database connection
    try:
        db.initialize()
    except Exception as e:
        logger.error(f"Error initializing database: {str(e)}")
        return False
    
    logger.info("Starting demand forecasting...")
    logger.info(f"Parameters: warehouse_id={warehouse_id}, vendor_id={vendor_id}, "
                f"buyer_id={buyer_id}, item_id={item_id}, periods={periods}, "
                f"update={update}, recalculate_madp={recalculate_madp}, "
                f"include_inactive={include_inactive}, dry_run={dry_run}")
    
    try:
        with session_scope() as session:
            # Create forecast service
            forecast_service = ForecastService(session)
            
            # Build query for items
            query = session.query(Item)
            
            # Apply filters
            if warehouse_id:
                query = query.filter(Item.warehouse_id == warehouse_id)
                
            if vendor_id:
                query = query.filter(Item.vendor_id == vendor_id)
                
            if buyer_id:
                query = query.filter(Item.buyer_id == buyer_id)
                
            if item_id:
                if isinstance(item_id, int):
                    query = query.filter(Item.id == item_id)
                else:
                    query = query.filter(Item.item_id == item_id)
            
            # Only include active items (Regular or Watch) unless specified
            if not include_inactive:
                query = query.filter(Item.buyer_class.in_(['R', 'W']))
            
            # Exclude items with frozen forecasts
            today = date.today()
            query = query.filter(
                (Item.freeze_until_date == None) | 
                (Item.freeze_until_date < today)
            )
            
            # Get items
            items = query.all()
            
            if not items:
                logger.warning("No items found matching criteria")
                return {
                    "success": False,
                    "message": "No items found matching criteria",
                    "items_processed": 0
                }
            
            logger.info(f"Found {len(items)} items to forecast")
            
            # Process results
            results = {
                'total_items': len(items),
                'processed': 0,
                'updated': 0,
                'errors': 0,
                'error_items': []
            }
            
            # Process each item
            for item in items:
                try:
                    # Get item history
                    history_data = forecast_service.get_item_demand_history(
                        item.id, 
                        periods=periods
                    )
                    
                    if not history_data:
                        logger.warning(f"No history data for item {item.item_id}")
                        continue
                    
                    # Get seasonal profile if available
                    seasonal_indices = None
                    if item.demand_profile:
                        seasonal_indices = forecast_service.get_seasonal_profile(item.demand_profile)
                    
                    if dry_run or verbose:
                        # Get current values for comparison
                        current_values = {
                            'demand_4weekly': item.demand_4weekly,
                            'madp': item.madp,
                            'track': item.track
                        }
                    
                    # Extract total_demand values for forecasting
                    history_values = [h['total_demand'] for h in history_data]
                    
                    # Calculate base forecast
                    base_forecast = calculate_forecast(
                        history_values, 
                        periods=periods,
                        seasonality=seasonal_indices
                    )
                    
                    # Calculate MADP and track if requested
                    if recalculate_madp:
                        madp = calculate_madp_from_history(base_forecast, history_values)
                        track = calculate_track_from_history(base_forecast, history_values)
                    else:
                        madp = item.madp
                        track = item.track
                    
                    # Apply seasonality if needed
                    final_forecast = base_forecast
                    if seasonal_indices:
                        # Get current period
                        current_period, _ = forecast_service.get_current_period(
                            item.forecasting_periodicity or forecast_service.company_settings['forecasting_periodicity_default']
                        )
                        
                        final_forecast = apply_seasonality_to_forecast(
                            base_forecast, 
                            seasonal_indices,
                            current_period
                        )
                    
                    # Calculate derived forecasts
                    weekly_forecast = final_forecast / 4
                    monthly_forecast = final_forecast * (365/12) / (365/13)
                    quarterly_forecast = final_forecast * 3
                    yearly_forecast = final_forecast * 13
                    
                    if verbose:
                        logger.info(f"Item: {item.item_id} ({item.description})")
                        logger.info(f"  Current forecast: {current_values['demand_4weekly']:.2f}")
                        logger.info(f"  New forecast: {final_forecast:.2f}")
                        logger.info(f"  Current MADP: {current_values['madp']:.2f}")
                        logger.info(f"  New MADP: {madp:.2f}")
                        logger.info(f"  Current Track: {current_values['track']:.2f}")
                        logger.info(f"  New Track: {track:.2f}")
                    
                    # Update item with new forecast if requested
                    if update and not dry_run:
                        # Update forecasts
                        item.demand_4weekly = final_forecast
                        item.demand_weekly = weekly_forecast
                        item.demand_monthly = monthly_forecast
                        item.demand_quarterly = quarterly_forecast
                        item.demand_yearly = yearly_forecast
                        
                        # Update MADP and track if recalculating
                        if recalculate_madp:
                            item.madp = madp
                            item.track = track
                        
                        # Set forecast date
                        item.forecast_date = datetime.now()
                        
                        # Update system class based on MADP and yearly demand
                        company_settings = forecast_service.company_settings
                        if item.demand_yearly <= company_settings['slow_mover_limit']:
                            item.system_class = 'S'  # Slow
                        elif item.madp >= company_settings['lumpy_demand_limit']:
                            item.system_class = 'L'  # Lumpy
                        else:
                            item.system_class = 'R'  # Regular
                        
                        results['updated'] += 1
                    
                    results['processed'] += 1
                    
                except Exception as e:
                    logger.error(f"Error forecasting item {item.item_id}: {str(e)}")
                    results['errors'] += 1
                    results['error_items'].append({
                        'item_id': item.item_id,
                        'error': str(e)
                    })
            
            if update and not dry_run:
                try:
                    session.commit()
                    logger.info(f"Updated forecasts for {results['updated']} items")
                except Exception as e:
                    session.rollback()
                    logger.error(f"Error committing forecast updates: {str(e)}")
                    results['success'] = False
                    results['message'] = f"Database error: {str(e)}"
                    return results
            elif dry_run:
                session.rollback()
                logger.info("Dry run completed, no changes committed")
            
            logger.info(f"Forecast generation completed: {results['processed']} processed, {results['errors']} errors")
            
            results['success'] = True
            return results
            
    except Exception as e:
        logger.error(f"Error generating forecasts: {str(e)}")
        logger.exception(e)
        return {
            "success": False,
            "message": f"Error: {str(e)}",
            "items_processed": 0
        }

def run_period_end_reforecasting(
    warehouse_id=None,
    vendor_id=None,
    buyer_id=None,
    dry_run=False,
    verbose=False
):
    """Run period-end reforecasting process.
    
    Args:
        warehouse_id: Optional warehouse ID to filter items
        vendor_id: Optional vendor ID to filter items
        buyer_id: Optional buyer ID to filter items
        dry_run: If True, simulates the run without making changes
        verbose: Whether to print detailed output
        
    Returns:
        Dictionary with reforecasting results
    """
    logger = setup_logging()
    
    # Initialize database connection
    try:
        db.initialize()
    except Exception as e:
        logger.error(f"Error initializing database: {str(e)}")
        return False
    
    logger.info("Starting period-end reforecasting...")
    
    try:
        with session_scope() as session:
            # Create forecast service
            forecast_service = ForecastService(session)
            
            # Build query for items
            query = session.query(Item)
            
            # Apply filters
            if warehouse_id:
                query = query.filter(Item.warehouse_id == warehouse_id)
                
            if vendor_id:
                query = query.filter(Item.vendor_id == vendor_id)
                
            if buyer_id:
                query = query.filter(Item.buyer_id == buyer_id)
            
            # Only include active items
            query = query.filter(Item.buyer_class.in_(['R', 'W']))
            
            # Exclude items with frozen forecasts
            today = date.today()
            query = query.filter(
                (Item.freeze_until_date == None) | 
                (Item.freeze_until_date < today)
            )
            
            # Get items
            items = query.all()
            
            if not items:
                logger.warning("No items found matching criteria")
                return {
                    "success": False,
                    "message": "No items found matching criteria",
                    "items_processed": 0
                }
            
            logger.info(f"Found {len(items)} items for period-end reforecasting")
            
            # Get period information
            company_settings = forecast_service.company_settings
            periodicity = company_settings['forecasting_periodicity_default']
            current_period, current_year = get_current_period(periodicity)
            previous_period, previous_year = get_previous_period(current_period, current_year, periodicity)
            
            # Process results
            results = {
                'total_items': len(items),
                'processed': 0,
                'updated': 0,
                'errors': 0,
                'error_items': []
            }
            
            # Process each item
            for item in items:
                try:
                    # Get latest history
                    history = forecast_service.get_item_demand_history(item.id, periods=1)
                    if not history:
                        logger.info(f"No history data for item {item.item_id}")
                        continue
                    
                    latest_history = history[0]
                    latest_demand = latest_history['total_demand']
                    
                    # Get current forecast values
                    current_forecast = item.demand_4weekly
                    current_madp = item.madp
                    current_track = item.track
                    
                    # Determine forecast method
                    forecast_method = item.forecast_method or ForecastMethod.E3_REGULAR_AVS
                    
                    if verbose:
                        logger.info(f"Processing item: {item.item_id} ({item.description})")
                        logger.info(f"  Current forecast: {current_forecast:.2f}")
                        logger.info(f"  Latest demand: {latest_demand:.2f}")
                        logger.info(f"  MADP: {current_madp:.2f}")
                        logger.info(f"  Track: {current_track:.2f}")
                        logger.info(f"  Forecast method: {forecast_method}")
                    
                    # Skip actual reforecasting in dry run mode
                    if dry_run:
                        results['processed'] += 1
                        continue
                    
                    # Perform reforecasting
                    reforecast_success = forecast_service.reforecast_item(item.id)
                    
                    if reforecast_success:
                        results['updated'] += 1
                    
                    results['processed'] += 1
                    
                    if verbose and reforecast_success:
                        logger.info(f"  New forecast: {item.demand_4weekly:.2f}")
                        logger.info(f"  New MADP: {item.madp:.2f}")
                        logger.info(f"  New Track: {item.track:.2f}")
                    
                except Exception as e:
                    logger.error(f"Error reforecasting item {item.item_id}: {str(e)}")
                    results['errors'] += 1
                    results['error_items'].append({
                        'item_id': item.item_id,
                        'error': str(e)
                    })
            
            if dry_run:
                session.rollback()
                logger.info("Dry run completed, no changes committed")
            
            logger.info(f"Period-end reforecasting completed: {results['processed']} processed, {results['updated']} updated, {results['errors']} errors")
            
            results['success'] = True
            return results
            
    except Exception as e:
        logger.error(f"Error in period-end reforecasting: {str(e)}")
        logger.exception(e)
        return {
            "success": False,
            "message": f"Error: {str(e)}",
            "items_processed": 0
        }

def detect_exceptions(
    warehouse_id=None,
    vendor_id=None,
    buyer_id=None,
    exception_types=None,
    verbose=False
):
    """Detect and create history exceptions.
    
    Args:
        warehouse_id: Optional warehouse ID to filter items
        vendor_id: Optional vendor ID to filter items
        buyer_id: Optional buyer ID to filter items
        exception_types: Optional list of exception types to detect
        verbose: Whether to print detailed output
        
    Returns:
        Dictionary with exception detection results
    """
    logger = setup_logging()
    
    # Initialize database connection
    try:
        db.initialize()
    except Exception as e:
        logger.error(f"Error initializing database: {str(e)}")
        return False
    
    logger.info("Starting history exception detection...")
    
    try:
        with session_scope() as session:
            # Create forecast service
            forecast_service = ForecastService(session)
            
            # Run exception detection
            results = forecast_service.detect_history_exceptions(
                warehouse_id=warehouse_id,
                vendor_id=vendor_id,
                item_ids=None  # We'll filter by buyer elsewhere
            )
            
            # Add success flag for consistency
            results['success'] = True
            
            # Log results
            logger.info(f"Exception detection completed:")
            logger.info(f"  Total items checked: {results['total_items']}")
            logger.info(f"  Demand filter high exceptions: {results['demand_filter_high']}")
            logger.info(f"  Demand filter low exceptions: {results['demand_filter_low']}")
            logger.info(f"  Tracking signal high exceptions: {results['tracking_signal_high']}")
            logger.info(f"  Tracking signal low exceptions: {results['tracking_signal_low']}")
            logger.info(f"  Service level check exceptions: {results['service_level_check']}")
            logger.info(f"  Infinity check exceptions: {results['infinity_check']}")
            logger.info(f"  Errors: {results['errors']}")
            
            return results
            
    except Exception as e:
        logger.error(f"Error in exception detection: {str(e)}")
        logger.exception(e)
        return {
            "success": False,
            "message": f"Error: {str(e)}"
        }

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Run demand forecasting operations')
    
    # Create subparsers for different commands
    subparsers = parser.add_subparsers(dest='command', help='Commands')
    
    # Forecast command
    forecast_parser = subparsers.add_parser('forecast', help='Generate demand forecasts')
    forecast_parser.add_argument('--warehouse-id', type=int, help='Warehouse ID to filter items')
    forecast_parser.add_argument('--vendor-id', type=int, help='Vendor ID to filter items')
    forecast_parser.add_argument('--buyer-id', type=str, help='Buyer ID to filter items')
    forecast_parser.add_argument('--item-id', type=str, help='Specific item ID to forecast')
    forecast_parser.add_argument('--periods', type=int, default=12, help='Number of history periods to consider')
    forecast_parser.add_argument('--update', action='store_true', help='Update items with new forecasts')
    forecast_parser.add_argument('--no-recalc-madp', action='store_true', help='Do not recalculate MADP and track')
    forecast_parser.add_argument('--include-inactive', action='store_true', help='Include inactive items')
    forecast_parser.add_argument('--dry-run', action='store_true', help='Simulate run without making changes')
    forecast_parser.add_argument('--verbose', '-v', action='store_true', help='Display detailed output')
    
    # Period-end reforecasting command
    period_end_parser = subparsers.add_parser('period-end', help='Run period-end reforecasting')
    period_end_parser.add_argument('--warehouse-id', type=int, help='Warehouse ID to filter items')
    period_end_parser.add_argument('--vendor-id', type=int, help='Vendor ID to filter items')
    period_end_parser.add_argument('--buyer-id', type=str, help='Buyer ID to filter items')
    period_end_parser.add_argument('--dry-run', action='store_true', help='Simulate run without making changes')
    period_end_parser.add_argument('--verbose', '-v', action='store_true', help='Display detailed output')
    
    # Exception detection command
    exception_parser = subparsers.add_parser('exceptions', help='Detect history exceptions')
    exception_parser.add_argument('--warehouse-id', type=int, help='Warehouse ID to filter items')
    exception_parser.add_argument('--vendor-id', type=int, help='Vendor ID to filter items')
    exception_parser.add_argument('--buyer-id', type=str, help='Buyer ID to filter items')
    exception_parser.add_argument('--types', type=str, nargs='+', help='Exception types to detect')
    exception_parser.add_argument('--verbose', '-v', action='store_true', help='Display detailed output')
    
    return parser.parse_args()

def main():
    """Main entry point for the script."""
    args = parse_args()
    
    if args.command == 'forecast':
        result = run_forecast(
            warehouse_id=args.warehouse_id,
            vendor_id=args.vendor_id,
            buyer_id=args.buyer_id,
            item_id=args.item_id,
            periods=args.periods,
            update=args.update,
            recalculate_madp=not args.no_recalc_madp,
            include_inactive=args.include_inactive,
            dry_run=args.dry_run,
            verbose=args.verbose
        )
    elif args.command == 'period-end':
        result = run_period_end_reforecasting(
            warehouse_id=args.warehouse_id,
            vendor_id=args.vendor_id,
            buyer_id=args.buyer_id,
            dry_run=args.dry_run,
            verbose=args.verbose
        )
    elif args.command == 'exceptions':
        result = detect_exceptions(
            warehouse_id=args.warehouse_id,
            vendor_id=args.vendor_id,
            buyer_id=args.buyer_id,
            exception_types=args.types,
            verbose=args.verbose
        )
    else:
        print("Please specify a command: forecast, period-end, or exceptions")
        sys.exit(1)
    
    # Exit with appropriate status code
    if result and result.get('success', False):
        sys.exit(0)
    else:
        sys.exit(1)

if __name__ == "__main__":
    main()