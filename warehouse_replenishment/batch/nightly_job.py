# warehouse_replenishment/batch/nightly_job.py
import logging
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional

from sqlalchemy.orm import Session

from ..config import config
from ..db import session_scope
from ..models import Company, Warehouse
from ..services.order_service import OrderService
from ..services.item_service import ItemService
from ..exceptions import BatchProcessError
from ..logging_setup import logger

def update_stock_status(warehouse_id: Optional[int] = None) -> Dict:
    """Update stock status for all items in the specified warehouse.
    
    Args:
        warehouse_id: Optional warehouse ID (if not provided, updates all warehouses)
        
    Returns:
        Dictionary with update results
    """
    logger.info(f"Updating stock status for warehouse_id={warehouse_id}")
    
    with session_scope() as session:
        item_service = ItemService(session)
        results = item_service.update_item_stock_status(warehouse_id=warehouse_id)
    
    return results

def calculate_lost_sales(warehouse_id: Optional[int] = None) -> Dict:
    """Calculate lost sales for items in the specified warehouse.
    
    Args:
        warehouse_id: Optional warehouse ID (if not provided, updates all warehouses)
        
    Returns:
        Dictionary with calculation results
    """
    logger.info(f"Calculating lost sales for warehouse_id={warehouse_id}")
    
    with session_scope() as session:
        item_service = ItemService(session)
        results = item_service.calculate_lost_sales(warehouse_id=warehouse_id)
    
    return results

def generate_orders(warehouse_id: Optional[int] = None) -> Dict:
    """Generate orders for all vendors in the specified warehouse.
    
    Args:
        warehouse_id: Optional warehouse ID (if not provided, generates for all warehouses)
        
    Returns:
        Dictionary with order generation results
    """
    logger.info(f"Generating orders for warehouse_id={warehouse_id}")
    
    with session_scope() as session:
        order_service = OrderService(session)
        results = order_service.generate_orders(warehouse_id=warehouse_id)
    
    return results

def process_time_based_parameters() -> Dict:
    """Process time-based parameters that are due.
    
    Returns:
        Dictionary with processing results
    """
    logger.info("Processing time-based parameters")
    
    from ..services.time_based_parameter_service import TimeBasedParameterService
    
    with session_scope() as session:
        time_based_service = TimeBasedParameterService(session)
        results = time_based_service.process_due_parameters()
    
    return results

def expire_deals() -> Dict:
    """Expire deals that have reached their end date.
    
    Returns:
        Dictionary with expiration results
    """
    logger.info("Expiring outdated deals")
    
    from ..services.deal_service import DealService
    
    with session_scope() as session:
        deal_service = DealService(session)
        results = deal_service.expire_outdated_deals()
    
    return results

def purge_accepted_orders() -> Dict:
    """Purge accepted orders based on configuration.
    
    Returns:
        Dictionary with purge results
    """
    logger.info("Purging accepted orders")
    
    with session_scope() as session:
        order_service = OrderService(session)
        results = order_service.purge_accepted_orders()
    
    return results

def update_lead_time_forecasts(warehouse_id: Optional[int] = None) -> Dict:
    """Update lead time forecasts for all vendors in the specified warehouse.
    
    Args:
        warehouse_id: Optional warehouse ID (if not provided, updates all warehouses)
        
    Returns:
        Dictionary with update results
    """
    # Check if lead time forecasting is enabled
    with session_scope() as session:
        company = session.query(Company).first()
        if not company:
            return {
                'success': False,
                'reason': 'Company settings not found'
            }
        
        if company.lead_time_forecast_control == 0:
            return {
                'success': True,
                'message': 'Lead time forecasting is not enabled',
                'updated_items': 0
            }
    
    logger.info(f"Updating lead time forecasts for warehouse_id={warehouse_id}")
    
    from ..services.lead_time_service import LeadTimeService
    
    with session_scope() as session:
        lead_time_service = LeadTimeService(session)
        results = lead_time_service.update_lead_time_forecasts(warehouse_id=warehouse_id)
    
    return results
def update_stock_status(warehouse_id: Optional[int] = None) -> Dict:
    """Update stock status for all items in the specified warehouse.
    
    Args:
        warehouse_id: Optional warehouse ID (if not provided, updates all warehouses)
        
    Returns:
        Dictionary with update results
    """
    logger.info(f"Updating stock status for warehouse_id={warehouse_id}")
    
    with session_scope() as session:
        item_service = ItemService(session)
        results = item_service.update_item_stock_status(warehouse_id=warehouse_id)
    
    return results

def update_safety_stock(warehouse_id: Optional[int] = None) -> Dict:
    """Update safety stock for all items in the specified warehouse.
    
    Args:
        warehouse_id: Optional warehouse ID (if not provided, updates all warehouses)
        
    Returns:
        Dictionary with update results
    """
    logger.info(f"Updating safety stock for warehouse_id={warehouse_id}")
    
    with session_scope() as session:
        from ..services.safety_stock_service import SafetyStockService
        safety_stock_service = SafetyStockService(session)
        results = safety_stock_service.update_safety_stock_for_all_items(
            warehouse_id=warehouse_id,
            update_order_points=True
        )
    
    return results

def run_nightly_job(warehouse_id: Optional[int] = None) -> Dict:
    """Run the nightly job.
    
    Args:
        warehouse_id: Optional warehouse ID to process only a specific warehouse
        
    Returns:
        Dictionary with job results
    """
    # Set up logging
    job_logger = logging.getLogger('batch')
    
    start_time = datetime.now()
    job_logger.info(f"Starting nightly job at {start_time}")
    
    try:
        results = {
            'start_time': start_time,
            'end_time': None,
            'duration': None,
            'processes': {}
        }
        
        # Step 1: Update stock status
        results['processes']['update_stock_status'] = update_stock_status(warehouse_id)
        
        # Step 2: Calculate lost sales
        results['processes']['calculate_lost_sales'] = calculate_lost_sales(warehouse_id)
        
        # Step 3: Update safety stock levels
        results['processes']['update_safety_stock'] = update_safety_stock(warehouse_id)
        
        # Step 4: Process time-based parameters
        results['processes']['time_based_parameters'] = process_time_based_parameters()
        
        # Step 5: Expire deals
        results['processes']['expire_deals'] = expire_deals()
        
        # Step 6: Update lead time forecasts (weekly)
        # Check if today is the lead time update day (typically once a week)
        today = date.today()
        if today.weekday() == 0:  # Monday
            results['processes']['lead_time_forecasts'] = update_lead_time_forecasts(warehouse_id)
        
        # Step 7: Generate orders
        results['processes']['generate_orders'] = generate_orders(warehouse_id)
        
        # Step 8: Purge accepted orders
        results['processes']['purge_accepted_orders'] = purge_accepted_orders()
        
        # Set end time and duration
        results['end_time'] = datetime.now()
        results['duration'] = results['end_time'] - start_time
        results['success'] = True
        
        job_logger.info(f"Nightly job completed successfully in {results['duration']}")
        
        return results
    
    except Exception as e:
        job_logger.error(f"Error during nightly job: {str(e)}", exc_info=True)
        
        return {
            'success': False,
            'error': str(e),
            'start_time': start_time,
            'end_time': datetime.now()
        }

if __name__ == "__main__":
    run_nightly_job()