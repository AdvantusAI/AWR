# warehouse_replenishment/warehouse_replenishment/batch/period_end_job.py
import logging
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional

from sqlalchemy.orm import Session
from warehouse_replenishment.logging_setup import get_logger
from warehouse_replenishment.config import config
from warehouse_replenishment.db import session_scope
from warehouse_replenishment.models import Company, Item, Warehouse
from warehouse_replenishment.services.forecast_service import ForecastService
from warehouse_replenishment.services.history_manager import HistoryManager
from warehouse_replenishment.utils.date_utils import (
    get_current_period, get_period_dates, is_period_end_day, 
    get_period_type, add_days
)
from warehouse_replenishment.exceptions import BatchProcessError
from warehouse_replenishment.logging_setup import logger

logger = get_logger('Warehouses')

    

def should_run_period_end() -> bool:
    """Check if period-end processing should run today.
    
    Returns:
        True if period-end processing should run
    """
    # Get company settings
    with session_scope() as session:
        company = session.query(Company).first()
        if not company:
            logger.error("Company settings not found")
            return False
        
        periodicity = company.forecasting_periodicity_default
    
    # Check if today is the last day of a period
    today = date.today()
    return True
    #return is_period_end_day(today, periodicity)

def process_all_warehouses() -> Dict:
    """Process period-end for all warehouses.
    
    Returns:
        Dictionary with processing results
    """
    results = {
        'total_warehouses': 0,
        'processed_warehouses': 0,
        'total_items': 0,
        'processed_items': 0,
        'error_warehouses': 0,
        'errors': 0,
        'history_exceptions': 0,
        'start_time': datetime.now(),
        'end_time': None,
        'duration': None
    }
    
    try:
        with session_scope() as session:
            # Get all warehouses
            warehouses = session.query(Warehouse).all()
           
            results['total_warehouses'] = len(warehouses)
            
            # Process each warehouse
            for warehouse in warehouses:
                warehouse_results = process_warehouse(warehouse.warehouse_id, session)
                
                if warehouse_results.get('success', False):
                    results['processed_warehouses'] += 1
                else:
                    results['error_warehouses'] += 1
                
                results['total_items'] += warehouse_results.get('total_items', 0)
                results['processed_items'] += warehouse_results.get('processed_items', 0)
                results['errors'] += warehouse_results.get('errors', 0)
                results['history_exceptions'] += warehouse_results.get('history_exceptions', 0)
    
    except Exception as e:
        logger.error(f"Error during period-end processing: {str(e)}", exc_info=True)
        results['errors'] += 1
    
    # Set end time and duration
    results['end_time'] = datetime.now()
    results['duration'] = results['end_time'] - results['start_time']
    
    return results

def process_warehouse(warehouse_id: int, session: Optional[Session] = None) -> Dict:
    """Process period-end for a specific warehouse.
    
    Args:
        warehouse_id: Warehouse ID
        session: Optional database session
        
    Returns:
        Dictionary with processing results
    """
    results = {
        'warehouse_id': warehouse_id,
        'success': False,
        'total_items': 0,
        'processed_items': 0,
        'errors': 0,
        'history_exceptions': 0,
        'start_time': datetime.now(),
        'end_time': None,
        'duration': None
    }
    
    # Use provided session or create a new one
    close_session = False
    if session is None:
        session = Session()
        close_session = True
    
    try:
        # Reforecast all items
        reforecast_results = reforecast_items(warehouse_id, session)
        
        # Update results
        results['total_items'] = reforecast_results.get('total_items', 0)
        results['processed_items'] = reforecast_results.get('processed', 0)
        results['errors'] += reforecast_results.get('errors', 0)
        
        # Detect history exceptions
        exception_results = detect_history_exceptions(warehouse_id, session)
        
        # Update results
        results['history_exceptions'] = (
            exception_results.get('demand_filter_high', 0) +
            exception_results.get('demand_filter_low', 0) +
            exception_results.get('tracking_signal_high', 0) +
            exception_results.get('tracking_signal_low', 0) +
            exception_results.get('service_level_check', 0) +
            exception_results.get('infinity_check', 0)
        )
        results['errors'] += exception_results.get('errors', 0)
        
        # Archive old resolved exceptions
        archive_results = archive_resolved_exceptions(session)
        results['errors'] += archive_results.get('errors', 0)
        
        results['success'] = True
    
    except Exception as e:
        logger.error(f"Error processing warehouse {warehouse_id}: {str(e)}", exc_info=True)
        results['errors'] += 1
    
    finally:
        if close_session:
            session.close()
    
    # Set end time and duration
    results['end_time'] = datetime.now()
    results['duration'] = results['end_time'] - results['start_time']
    
    return results

def reforecast_items(warehouse_id: int, session: Session) -> Dict:
    """Reforecast all items in a warehouse.
    
    Args:
        warehouse_id: Warehouse ID
        session: Database session
        
    Returns:
        Dictionary with reforecast results
    """
    forecast_service = ForecastService(session)
    
    # Process reforecasting
    results = forecast_service.process_period_end_reforecasting(warehouse_id=warehouse_id)
    
    return results

def detect_history_exceptions(warehouse_id: int, session: Session) -> Dict:
    """Detect history exceptions for a warehouse.
    
    Args:
        warehouse_id: Warehouse ID
        session: Database session
        
    Returns:
        Dictionary with exception detection results
    """
    forecast_service = ForecastService(session)
    
    # Detect exceptions
    results = forecast_service.detect_history_exceptions(warehouse_id=warehouse_id)
    
    return results

def archive_resolved_exceptions(session: Session) -> Dict:
    """Archive old resolved history exceptions.
    
    Args:
        session: Database session
        
    Returns:
        Dictionary with archive results
    """
    history_manager = HistoryManager(session)
    
    # Archive resolved exceptions
    results = history_manager.archive_resolved_exceptions()
    
    return results

def run_period_end_job(warehouse_id: Optional[int] = None) -> Dict:
    """Run the period-end job.
    
    Args:
        warehouse_id: Optional warehouse ID to process only a specific warehouse
        
    Returns:
        Dictionary with job results
    """
    # Set up logging
    job_logger = logging.getLogger('batch')
    
    start_time = datetime.now()
    job_logger.info(f"Starting period-end job at {start_time}")
    
    # Check if we should run period-end
    if not should_run_period_end():
        job_logger.info("Today is not a period-end day. Skipping period-end processing.")
        return {
            'success': False,
            'reason': 'Not a period-end day',
            'start_time': start_time,
            'end_time': datetime.now()
        }
    
    try:
        # Process all warehouses or a specific warehouse
        if warehouse_id is not None:
            with session_scope() as session:
                results = process_warehouse(warehouse_id, session)
        else:
            results = process_all_warehouses()
        
        # Log results
        job_logger.info(f"Period-end job completed successfully in {results.get('duration')}")
        job_logger.info(f"Processed {results.get('processed_items', 0)} items")
        job_logger.info(f"Generated {results.get('history_exceptions', 0)} history exceptions")
        
        if results.get('errors', 0) > 0:
            job_logger.warning(f"Encountered {results.get('errors', 0)} errors during processing")
        
        results['success'] = True
        
        return results
    
    except Exception as e:
        job_logger.error(f"Error during period-end job: {str(e)}", exc_info=True)
        
        return {
            'success': False,
            'error': str(e),
            'start_time': start_time,
            'end_time': datetime.now()
        }

if __name__ == "__main__":
    run_period_end_job()