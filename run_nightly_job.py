#!/usr/bin/env python
# run_nightly_job.py - Script to run the nightly job

import sys
import os
import logging
import argparse
from datetime import datetime
from pathlib import Path

# Add the parent directory to the path so we can import our modules
parent_dir = str(Path(__file__).parent.parent)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from warehouse_replenishment.batch.nightly_job import run_nightly_job
from warehouse_replenishment.logging_setup import get_logger

def main():
    """Run the nightly job."""
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Run the AWR nightly job')
    parser.add_argument('--warehouse', '-w', type=int, help='Process only a specific warehouse ID')
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')
    
    args = parser.parse_args()
    
    # Set up logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(Path(parent_dir) / 'logs' / f'nightly_job_{datetime.now().strftime("%Y%m%d")}.log'),
            logging.StreamHandler()
        ]
    )
    
    logger = get_logger('nightly_job_runner')
    
    logger.info("Starting nightly job runner...")
    logger.info(f"Warehouse filter: {args.warehouse if args.warehouse is not None else 'All warehouses'}")
    
    try:
        # Run the nightly job
        results = run_nightly_job(args.warehouse)
        
        # Print results
        if results.get('success', False):
            logger.info(f"Nightly job completed successfully")
            logger.info(f"Duration: {results.get('duration')}")
            
            # Print process details
            for process_name, process_result in results.get('processes', {}).items():
                logger.info(f"Process '{process_name}': {process_result.get('success', False)}")
                
                if process_result.get('updated_items'):
                    logger.info(f"  Updated items: {process_result.get('updated_items')}")
                
                if process_result.get('generated_orders'):
                    logger.info(f"  Generated orders: {process_result.get('generated_orders')}")
            
            return 0
        else:
            logger.error(f"Nightly job failed: {results.get('error', 'Unknown error')}")
            return 1
    
    except Exception as e:
        logger.exception(f"Error running nightly job: {str(e)}")
        return 1

if __name__ == "__main__":
    sys.exit(main())