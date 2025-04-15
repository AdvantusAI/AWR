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
from warehouse_replenishment.models import Item, BuyerClassCode
from warehouse_replenishment.services.forecast_service import ForecastService
from warehouse_replenishment.utils.date_utils import get_current_period, get_previous_period

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
                query = query.filter(Item.buyer_class.in_([BuyerClassCode.REGULAR, BuyerClassCode.WATCH]))
            
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
                            'demand_4weekly': item.demand_4weekly