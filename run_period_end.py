#!/usr/bin/env python
# run_period_end.py - CLI wrapper for period_end_job.py

import argparse
import sys
import logging
from datetime import datetime
from pathlib import Path

# Add parent directory to path for imports
parent_dir = str(Path(__file__).parent)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from warehouse_replenishment.db import db
from warehouse_replenishment.logging_setup import get_logger
from warehouse_replenishment.batch.period_end_job import run_period_end_job

def setup_logging():
    """Setup logging for the script."""
    log_dir = Path(parent_dir) / 'logs'
    log_dir.mkdir(exist_ok=True)
    
    logger = get_logger('period_end_runner')
    
    # Add console handler for immediate feedback
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    logger.addHandler(console_handler)
    
    return logger

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Run the warehouse replenishment period-end job')
    
    parser.add_argument('--warehouse-id', type=int, help='Process only this specific warehouse')
    parser.add_argument('--force', action='store_true', help='Force run even if not a period-end day')
    parser.add_argument('--dry-run', action='store_true', help='Show what would happen without making changes')
    parser.add_argument('--verbose', '-v', action='store_true', help='Show detailed output')
    
    return parser.parse_args()

def main():
    """Main entry point for the script."""
    args = parse_args()
    logger = setup_logging()
    
    logger.info("Starting period-end job runner")
    
    try:
        # Initialize database connection
        logger.info("Initializing database connection...")
        db.initialize()
        logger.info("Database connection initialized successfully")
        
        # Run the period-end job
        if args.warehouse_id:
            logger.info(f"Running period-end job for warehouse ID: {args.warehouse_id}")
            results = run_period_end_job(warehouse_id=args.warehouse_id)
        else:
            logger.info("Running period-end job for all warehouses")
            results = run_period_end_job()
        
        # Check results
        if results.get('success', False):
            logger.info("Period-end job completed successfully")
            
            # Show detailed results if verbose
            if args.verbose:
                logger.info(f"Processed {results.get('processed_warehouses', 0)} warehouses")
                logger.info(f"Processed {results.get('processed_items', 0)} items")
                logger.info(f"Generated {results.get('history_exceptions', 0)} history exceptions")
                logger.info(f"Duration: {results.get('duration')}")
                
                if results.get('errors', 0) > 0:
                    logger.warning(f"Encountered {results.get('errors', 0)} errors during processing")
            
            sys.exit(0)
        else:
            reason = results.get('reason', 'Unknown error')
            logger.warning(f"Period-end job not executed: {reason}")
            
            # Exit with non-zero code only for actual errors, not for "not a period-end day"
            if reason == 'Not a period-end day' and not args.force:
                logger.info("Use --force to run even if not a period-end day")
                sys.exit(0)
            else:
                sys.exit(1)
                
    except Exception as e:
        logger.error(f"Error running period-end job: {str(e)}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()