import argparse
import sys
import logging
from datetime import datetime, date
from pathlib import Path

# Fix the imports to use proper relative imports
from config import config
from db import db, session_scope
from logging_setup import logger, get_logger
from models import Item, Company

def init_application():
    """Initialize application components."""
    # Initialize database
    db.initialize()
    
    # Log application start
    log = logger.app_logger
    log.info("Warehouse Replenishment System initialized")
    log.info(f"Using database: {config.get('DATABASE', 'engine')} at {config.get('DATABASE', 'host')}:{config.get('DATABASE', 'port')}")
    log.info(f"Database name: {config.get('DATABASE', 'database')}")
    
    return True

def generate_forecast(args):
    """Generate demand forecast for specified items.
    
    Args:
        args: Command-line arguments with forecast parameters
    """
    from core.demand_forecast import (
        calculate_forecast, calculate_madp_from_history, 
        calculate_track_from_history, apply_seasonality_to_forecast
    )
    # Import the ForecastService implementation
    from services.forecast_service import ForecastService
    
    log = get_logger('forecast')
    log.info(f"Starting forecast generation with parameters: {args}")
    
    try:
        with session_scope() as session:
            # Get company settings for defaults
            company = session.query(Company).first()
            if not company:
                log.error("No company record found in database")
                return False
            
            # Create forecast service
            forecast_service = ForecastService(session)
            
            # Build query for items
            query = session.query(Item)
            
            # Apply filters based on args
            if args.item_id:
                query = query.filter(Item.item_id == args.item_id)
            
            if args.vendor_id:
                query = query.filter(Item.vendor_id == args.vendor_id)
                
            if args.warehouse_id:
                query = query.filter(Item.warehouse_id == args.warehouse_id)
            
            if args.buyer_id:
                query = query.filter(Item.buyer_id == args.buyer_id)
            
            # Only include active items (Regular or Watch)
            if not args.include_inactive:
                query = query.filter(Item.buyer_class.in_(['R', 'W']))  # Fixed: Use string values instead of enum values
            
            # Get items
            items = query.all()
            
            if not items:
                log.warning("No items found matching criteria")
                return False
            
            log.info(f"Found {len(items)} items to forecast")
            
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
                    # Get history data
                    history_data = forecast_service.get_item_demand_history(
                        item.id, 
                        periods=args.periods
                    )
                    
                    if not history_data:
                        log.warning(f"No history data for item {item.item_id}")
                        continue
                    
                    # Extract total_demand values for forecasting
                    history_values = [h['total_demand'] for h in history_data]
                    
                    # Calculate base forecast
                    seasonal_indices = None
                    if item.demand_profile and not args.ignore_seasonality:
                        seasonal_indices = forecast_service.get_seasonal_profile(item.demand_profile)
                    
                    base_forecast = calculate_forecast(
                        history_values, 
                        periods=args.periods,
                        seasonality=seasonal_indices
                    )
                    
                    # Calculate MADP and track
                    madp = calculate_madp_from_history(base_forecast, history_values)
                    track = calculate_track_from_history(base_forecast, history_values)
                    
                    # Apply seasonality if needed
                    final_forecast = base_forecast
                    if seasonal_indices and not args.ignore_seasonality:
                        # Get current period
                        current_period, _ = forecast_service.get_current_period(
                            item.forecasting_periodicity or company.forecasting_periodicity_default
                        )
                        
                        final_forecast = apply_seasonality_to_forecast(
                            base_forecast, 
                            seasonal_indices,
                            current_period
                        )
                    
                    results['processed'] += 1
                    
                    # Update item with new forecast if requested
                    if args.update:
                        # Update forecasts
                        item.demand_4weekly = final_forecast
                        item.demand_weekly = final_forecast / 4
                        item.demand_monthly = final_forecast * (365/12) / (365/13)
                        item.demand_quarterly = final_forecast * 3
                        item.demand_yearly = final_forecast * 13
                        
                        # Update MADP and track
                        item.madp = madp
                        item.track = track
                        
                        # Set forecast date
                        item.forecast_date = datetime.now()
                        
                        results['updated'] += 1
                    
                    # Print output if verbose
                    if args.verbose:
                        print(f"Item: {item.item_id}")
                        print(f"  Current forecast: {item.demand_4weekly}")
                        print(f"  New forecast: {final_forecast}")
                        print(f"  MADP: {madp}")
                        print(f"  Track: {track}")
                        print()
                    
                except Exception as e:
                    log.error(f"Error forecasting item {item.item_id}: {str(e)}")
                    results['errors'] += 1
                    results['error_items'].append({
                        'item_id': item.item_id,
                        'error': str(e)
                    })
            
            if args.update:
                session.commit()
                log.info(f"Updated forecasts for {results['updated']} items")
            
            log.info(f"Forecast generation completed: {results['processed']} processed, {results['errors']} errors")
            
            return results
            
    except Exception as e:
        log.error(f"Error generating forecasts: {str(e)}")
        log.exception(e)
        return False

def main():
    """Main application entry point."""
    parser = argparse.ArgumentParser(description='Warehouse Replenishment System')
    
    # Add command-line arguments
    parser.add_argument('--setup-db', action='store_true', 
                      help='Set up the database schema')
    parser.add_argument('--drop-db', action='store_true',
                      help='Drop existing tables before setup')
    
    # Add subparsers for different commands
    subparsers = parser.add_subparsers(dest='command', help='Commands')
    
    # Forecast command
    forecast_parser = subparsers.add_parser('forecast', help='Generate demand forecasts')
    forecast_parser.add_argument('--item-id', type=str, help='Specific item ID to forecast')
    forecast_parser.add_argument('--vendor-id', type=str, help='Forecast items for specific vendor')
    forecast_parser.add_argument('--warehouse-id', type=str, help='Forecast items for specific warehouse')
    forecast_parser.add_argument('--buyer-id', type=str, help='Forecast items for specific buyer')
    forecast_parser.add_argument('--periods', type=int, default=12, 
                               help='Number of history periods to consider')
    forecast_parser.add_argument('--update', action='store_true', 
                               help='Update items with new forecasts')
    forecast_parser.add_argument('--include-inactive', action='store_true',
                               help='Include items with inactive buyer class')
    forecast_parser.add_argument('--ignore-seasonality', action='store_true',
                               help='Ignore seasonal profiles when forecasting')
    forecast_parser.add_argument('--verbose', '-v', action='store_true',
                               help='Display detailed output')
    
    args = parser.parse_args()
    
    if args.setup_db:
        from warehouse_replenishment.scripts.setup_db import setup_database
        setup_database(args.drop_db)
        return
    
    # Handle different commands
    if args.command == 'forecast':
        generate_forecast(args)
        return
    
    # Normal application initialization
    init_application()
    
    # Here we would typically launch other processes or start a scheduler
    # for the night job, but for now just log that we're ready
    logger.app_logger.info("Warehouse Replenishment System ready")

if __name__ == "__main__":
    main()