"""
Main entry point for the ASR system.

This script provides command-line access to various ASR functions,
including running nightly replenishment, processing period end tasks,
and handling exceptions.
"""
import argparse
import datetime
import logging
import sys

from utils.db import get_session, init_db
from services.demand_forecast import run_period_end_forecasting
from services.lead_time import run_lead_time_forecasting
from services.replenishment import run_nightly_replenishment, get_order_category_counts
from services.exceptions import process_history_exceptions

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('asr.log')
    ]
)

logger = logging.getLogger(__name__)

def run_nightly_job(store_id=None):
    """
    Run the nightly replenishment job.
    
    Args:
        store_id (str): Store ID to process (None for all)
    
    Returns:
        dict: Statistics about the job
    """
    session = get_session()
    try:
        logger.info("Starting nightly replenishment job")
        
        # Run nightly replenishment
        stats = run_nightly_replenishment(session, store_id)
        
        logger.info(f"Nightly replenishment completed: {stats}")
        return stats
    
    except Exception as e:
        logger.error(f"Error running nightly job: {e}")
        return {'error': str(e)}
    
    finally:
        session.close()

def run_period_end_job():
    """
    Run the period-end processing job.
    
    Returns:
        dict: Statistics about the job
    """
    session = get_session()
    try:
        logger.info("Starting period-end processing job")
        
        # Run period-end forecasting
        forecast_stats = run_period_end_forecasting(session)
        
        # Run lead time forecasting
        lead_time_stats = run_lead_time_forecasting(session)
        
        # Generate history exceptions
        exception_stats = process_history_exceptions(session)
        
        stats = {
            'forecast': forecast_stats,
            'lead_time': lead_time_stats,
            'exceptions': exception_stats
        }
        
        logger.info(f"Period-end processing completed: {stats}")
        return stats
    
    except Exception as e:
        logger.error(f"Error running period-end job: {e}")
        return {'error': str(e)}
    
    finally:
        session.close()

def initialize_database():
    """
    Initialize the database by creating tables.
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        logger.info("Initializing database")
        init_db()
        logger.info("Database initialization completed")
        return True
    
    except Exception as e:
        logger.error(f"Error initializing database: {e}")
        return False

def get_to_do_counts():
    """
    Get counts for the To Do menu.
    
    Returns:
        dict: Counts of orders in each category
    """
    session = get_session()
    try:
        logger.info("Getting To Do counts")
        counts = get_order_category_counts(session)
        logger.info(f"To Do counts: {counts}")
        return counts
    
    except Exception as e:
        logger.error(f"Error getting To Do counts: {e}")
        return {'error': str(e)}
    
    finally:
        session.close()

def parse_arguments():
    """
    Parse command-line arguments.
    
    Returns:
        argparse.Namespace: Parsed arguments
    """
    parser = argparse.ArgumentParser(description='ASR System')
    parser.add_argument('--init', action='store_true', help='Initialize the database')
    parser.add_argument('--nightly', action='store_true', help='Run nightly replenishment job')
    parser.add_argument('--period-end', action='store_true', help='Run period-end processing job')
    parser.add_argument('--todo', action='store_true', help='Get To Do counts')
    parser.add_argument('--store-id', help='Store ID to process')
    
    return parser.parse_args()

def main():
    """Main entry point for the ASR system."""
    args = parse_arguments()
    
    if args.init:
        initialize_database()
    
    if args.nightly:
        run_nightly_job(args.store_id)
    
    if args.period_end:
        run_period_end_job()
    
    if args.todo:
        get_to_do_counts()
    
    if not any([args.init, args.nightly, args.period_end, args.todo]):
        print("No action specified. Use --help for available options.")

if __name__ == '__main__':
    main()