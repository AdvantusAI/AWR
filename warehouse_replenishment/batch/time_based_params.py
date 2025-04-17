# warehouse_replenishment/batch/time_based_params.py
import logging
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Union, Any
import re
import sys
from pathlib import Path

# Add the parent directory to the path so we can import our modules
parent_dir = str(Path(__file__).parent.parent.parent)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from sqlalchemy import and_, func, or_
from sqlalchemy.orm import Session

from warehouse_replenishment.db import session_scope
from warehouse_replenishment.logging_setup import logger
from warehouse_replenishment.models import (
    TimeBasedParameter, TimeBasedParameterItem, Item, Vendor,
    BuyerClassCode, SystemClassCode
)
from warehouse_replenishment.exceptions import TimeBasedParameterError

def evaluate_expression(expression: str, item: Item = None, **variables) -> Any:
    """Evaluate a time-based parameter expression.
    
    Args:
        expression: Expression string to evaluate
        item: Optional item object to use in evaluation
        variables: Additional variables to use in evaluation
        
    Returns:
        Result of the evaluation
    """
    # Create a safe local scope with allowed variables
    safe_locals = {}
    
    # Add item attributes if item is provided
    if item:
        # Common item attributes that may be used in expressions
        item_attrs = [
            'on_hand', 'on_order', 'demand_4weekly', 'demand_weekly', 
            'demand_monthly', 'demand_yearly', 'madp', 'track',
            'lead_time_forecast', 'lead_time_variance', 'service_level_goal',
            'purchase_price', 'sales_price'
        ]
        
        for attr in item_attrs:
            if hasattr(item, attr):
                safe_locals[attr] = getattr(item, attr)
    
    # Add other variables
    for key, value in variables.items():
        safe_locals[key] = value
    
    # Add safe math functions
    safe_locals.update({
        'abs': abs,
        'max': max,
        'min': min,
        'round': round,
        'int': int,
        'float': float
    })
    
    try:
        # Evaluate the expression in the safe context
        result = eval(expression, {"__builtins__": {}}, safe_locals)
        return result
    except Exception as e:
        raise TimeBasedParameterError(f"Error evaluating expression '{expression}': {str(e)}")

def parse_item_filter(filter_expression: str) -> Dict:
    """Parse an item filter expression into query conditions.
    
    Args:
        filter_expression: Filter expression (e.g., "buyer_class:R,W vendor_id:123")
        
    Returns:
        Dictionary of query conditions
    """
    conditions = {}
    
    # Split by whitespace to separate different conditions
    parts = filter_expression.split()
    
    for part in parts:
        if ':' in part:
            key, value = part.split(':', 1)
            
            # Handle multiple values (comma-separated)
            if ',' in value:
                conditions[key] = value.split(',')
            else:
                conditions[key] = value
    
    return conditions

def process_time_based_parameters(
    effective_date: Optional[date] = None,
    parameter_id: Optional[int] = None
) -> Dict:
    """Process time-based parameters.
    
    Args:
        effective_date: Optional effective date for parameters
        parameter_id: Optional specific parameter ID to process
        
    Returns:
        Dictionary with processing results
    """
    log = logger.get_logger('batch')
    log.info("Processing time-based parameters")
    
    if effective_date is None:
        effective_date = date.today()
    
    results = {
        'total_parameters': 0,
        'processed_parameters': 0,
        'affected_items': 0,
        'processed_items': 0,
        'errors': 0,
        'start_time': datetime.now(),
        'end_time': None,
        'duration': None
    }
    
    try:
        with session_scope() as session:
            # Get time-based parameters to process
            query = session.query(TimeBasedParameter).filter(
                TimeBasedParameter.effective_date <= effective_date,
                TimeBasedParameter.status.in_(['PENDING', 'APPROVED'])
            )
            
            if parameter_id:
                query = query.filter(TimeBasedParameter.id == parameter_id)
            
            parameters = query.all()
            results['total_parameters'] = len(parameters)
            
            # Process each parameter
            for param in parameters:
                try:
                    # Process this parameter
                    param_results = process_parameter(session, param, effective_date)
                    
                    # Update parameter status
                    if param_results['success']:
                        param.status = 'APPLIED'
                    else:
                        param.status = 'ERROR'
                        param.comment = param_results['error']
                    
                    # Update results
                    results['processed_parameters'] += 1
                    results['affected_items'] += param_results['affected_items']
                    results['processed_items'] += param_results['processed_items']
                    results['errors'] += param_results['errors']
                    
                except Exception as e:
                    log.error(f"Error processing parameter {param.id}: {str(e)}")
                    param.status = 'ERROR'
                    param.comment = str(e)
                    results['errors'] += 1
            
            # Commit changes
            session.commit()
    
    except Exception as e:
        log.error(f"Error during time-based parameter processing: {str(e)}")
        results['errors'] += 1
    
    # Set end time and duration
    results['end_time'] = datetime.now()
    results['duration'] = results['end_time'] - results['start_time']
    
    log.info(f"Time-based parameters processing completed. "
             f"Processed {results['processed_parameters']} of {results['total_parameters']} parameters, "
             f"affecting {results['processed_items']} items.")
    
    return results

def process_parameter(
    session: Session,
    parameter: TimeBasedParameter,
    effective_date: date
) -> Dict:
    """Process a single time-based parameter.
    
    Args:
        session: Database session
        parameter: Parameter to process
        effective_date: Effective date
        
    Returns:
        Dictionary with processing results
    """
    log = logger.get_logger('batch')
    log.info(f"Processing parameter {parameter.id}: {parameter.description}")
    
    results = {
        'parameter_id': parameter.id,
        'success': False,
        'affected_items': 0,
        'processed_items': 0,
        'errors': 0,
        'error': None
    }
    
    try:
        # Handle different parameter types
        if parameter.parameter_type == 'DEMAND_FORECAST':
            process_demand_forecast_parameter(session, parameter, effective_date, results)
        elif parameter.parameter_type == 'LEAD_TIME':
            process_lead_time_parameter(session, parameter, effective_date, results)
        elif parameter.parameter_type == 'SERVICE_LEVEL':
            process_service_level_parameter(session, parameter, effective_date, results)
        elif parameter.parameter_type == 'BUYER_CLASS':
            process_buyer_class_parameter(session, parameter, effective_date, results)
        elif parameter.parameter_type == 'PRICE_CHANGE':
            process_price_change_parameter(session, parameter, effective_date, results)
        else:
            # Unknown parameter type
            results['error'] = f"Unknown parameter type: {parameter.parameter_type}"
            return results
        
        # Set success flag if no errors
        if results['errors'] == 0:
            results['success'] = True
        
        return results
    
    except Exception as e:
        log.error(f"Error processing parameter {parameter.id}: {str(e)}")
        results['error'] = str(e)
        results['errors'] += 1
        return results

def get_items_for_parameter(
    session: Session,
    parameter: TimeBasedParameter
) -> List[Item]:
    """Get items that should be affected by a parameter.
    
    Args:
        session: Database session
        parameter: Parameter to process
        
    Returns:
        List of items to process
    """
    # Check if specific items are already defined for this parameter
    parameter_items = session.query(TimeBasedParameterItem).filter(
        TimeBasedParameterItem.parameter_id == parameter.id
    ).all()
    
    if parameter_items:
        # If specific items are defined, use those
        item_ids = [item.item_id for item in parameter_items]
        return session.query(Item).filter(Item.id.in_(item_ids)).all()
    
    # Otherwise, check if there's a buyer_id filter
    if parameter.buyer_id:
        return session.query(Item).filter(Item.buyer_id == parameter.buyer_id).all()
    
    # Parse expression as a filter if it contains filter syntax
    if ':' in parameter.expression:
        try:
            conditions = parse_item_filter(parameter.expression)
            query = session.query(Item)
            
            # Apply filters
            for key, value in conditions.items():
                if key == 'buyer_class':
                    if isinstance(value, list):
                        query = query.filter(Item.buyer_class.in_(value))
                    else:
                        query = query.filter(Item.buyer_class == value)
                elif key == 'system_class':
                    if isinstance(value, list):
                        query = query.filter(Item.system_class.in_(value))
                    else:
                        query = query.filter(Item.system_class == value)
                elif key == 'vendor_id':
                    query = query.filter(Item.vendor_id == int(value))
                elif key == 'warehouse_id':
                    query = query.filter(Item.warehouse_id == value)
                # Add more filters as needed
            
            return query.all()
        except Exception as e:
            raise TimeBasedParameterError(f"Error parsing filter expression: {str(e)}")
    
    # Default: affect all active items
    return session.query(Item).filter(
        Item.buyer_class.in_([BuyerClassCode.REGULAR, BuyerClassCode.WATCH])
    ).all()

def process_demand_forecast_parameter(
    session: Session,
    parameter: TimeBasedParameter,
    effective_date: date,
    results: Dict
) -> None:
    """Process a demand forecast parameter.
    
    Args:
        session: Database session
        parameter: Parameter to process
        effective_date: Effective date
        results: Results dictionary to update
        
    Returns:
        None (updates results dictionary)
    """
    log = logger.get_logger('batch')
    
    # Get affected items
    items = get_items_for_parameter(session, parameter)
    results['affected_items'] = len(items)
    
    # Process each item
    for item in items:
        try:
            # Create parameter item record
            param_item = TimeBasedParameterItem(
                parameter_id=parameter.id,
                item_id=item.id,
                effective_date=effective_date,
                expression=parameter.expression
            )
            
            # Evaluate the expression
            multiplier = evaluate_expression(parameter.expression, item)
            
            # Store original values for reference
            original_4weekly = item.demand_4weekly
            
            # Apply forecast changes
            item.demand_4weekly = item.demand_4weekly * multiplier
            item.demand_weekly = item.demand_4weekly / 4
            item.demand_monthly = item.demand_4weekly * (365/12) / (365/13)
            item.demand_quarterly = item.demand_4weekly * 3
            item.demand_yearly = item.demand_4weekly * 13
            
            # Record the change in the parameter item
            param_item.changes = json.dumps({
                'demand_4weekly': {
                    'before': original_4weekly,
                    'after': item.demand_4weekly
                }
            })
            
            # Set forecast date
            item.forecast_date = datetime.now()
            
            session.add(param_item)
            results['processed_items'] += 1
            
            log.info(f"Updated demand forecast for item {item.id}: "
                    f"{original_4weekly} → {item.demand_4weekly} "
                    f"(multiplier: {multiplier})")
            
        except Exception as e:
            log.error(f"Error updating forecast for item {item.id}: {str(e)}")
            
            # Create error record
            param_item = TimeBasedParameterItem(
                parameter_id=parameter.id,
                item_id=item.id,
                effective_date=effective_date,
                expression=parameter.expression,
                error_message=str(e)
            )
            session.add(param_item)
            
            results['errors'] += 1

def process_lead_time_parameter(
    session: Session,
    parameter: TimeBasedParameter,
    effective_date: date,
    results: Dict
) -> None:
    """Process a lead time parameter.
    
    Args:
        session: Database session
        parameter: Parameter to process
        effective_date: Effective date
        results: Results dictionary to update
        
    Returns:
        None (updates results dictionary)
    """
    log = logger.get_logger('batch')
    
    # Get affected items
    items = get_items_for_parameter(session, parameter)
    results['affected_items'] = len(items)
    
    # Check if expression format is for lead time
    is_adjustment = '+' in parameter.expression or '-' in parameter.expression or '*' in parameter.expression
    
    # Process each item
    for item in items:
        try:
            # Create parameter item record
            param_item = TimeBasedParameterItem(
                parameter_id=parameter.id,
                item_id=item.id,
                effective_date=effective_date,
                expression=parameter.expression
            )
            
            # Store original values for reference
            original_lead_time = item.lead_time_forecast
            
            # Apply lead time changes
            if is_adjustment:
                # Expression is an adjustment formula
                new_lead_time = evaluate_expression(
                    parameter.expression, 
                    item,
                    current_lead_time=item.lead_time_forecast
                )
            else:
                # Expression is a direct value
                try:
                    new_lead_time = float(parameter.expression)
                except ValueError:
                    # Try evaluating as an expression
                    new_lead_time = evaluate_expression(parameter.expression, item)
            
            # Update lead time
            item.lead_time_forecast = int(new_lead_time)
            item.lead_time_maintained = True
            
            # Record the change in the parameter item
            param_item.changes = json.dumps({
                'lead_time_forecast': {
                    'before': original_lead_time,
                    'after': item.lead_time_forecast
                }
            })
            
            session.add(param_item)
            results['processed_items'] += 1
            
            log.info(f"Updated lead time for item {item.id}: "
                    f"{original_lead_time} → {item.lead_time_forecast}")
            
            # Recalculate safety stock and order points if possible
            try:
                from warehouse_replenishment.services.safety_stock_service import SafetyStockService
                ss_service = SafetyStockService(session)
                ss_service.update_safety_stock_for_item(
                    item.id, 
                    update_sstf=True,
                    update_order_points=True
                )
            except ImportError:
                # Safety stock service not available, continue without updating
                pass
            
        except Exception as e:
            log.error(f"Error updating lead time for item {item.id}: {str(e)}")
            
            # Create error record
            param_item = TimeBasedParameterItem(
                parameter_id=parameter.id,
                item_id=item.id,
                effective_date=effective_date,
                expression=parameter.expression,
                error_message=str(e)
            )
            session.add(param_item)
            
            results['errors'] += 1

def process_service_level_parameter(
    session: Session,
    parameter: TimeBasedParameter,
    effective_date: date,
    results: Dict
) -> None:
    """Process a service level parameter.
    
    Args:
        session: Database session
        parameter: Parameter to process
        effective_date: Effective date
        results: Results dictionary to update
        
    Returns:
        None (updates results dictionary)
    """
    log = logger.get_logger('batch')
    
    # Get affected items
    items = get_items_for_parameter(session, parameter)
    results['affected_items'] = len(items)
    
    # Process each item
    for item in items:
        try:
            # Create parameter item record
            param_item = TimeBasedParameterItem(
                parameter_id=parameter.id,
                item_id=item.id,
                effective_date=effective_date,
                expression=parameter.expression
            )
            
            # Store original values for reference
            original_service_level = item.service_level_goal
            
            # Evaluate the expression
            try:
                new_service_level = float(parameter.expression)
            except ValueError:
                # Try evaluating as an expression
                new_service_level = evaluate_expression(parameter.expression, item)
            
            # Ensure service level is in valid range
            new_service_level = max(0, min(100, new_service_level))
            
            # Update service level
            item.service_level_goal = new_service_level
            item.service_level_maintained = True
            
            # Record the change in the parameter item
            param_item.changes = json.dumps({
                'service_level_goal': {
                    'before': original_service_level,
                    'after': item.service_level_goal
                }
            })
            
            session.add(param_item)
            results['processed_items'] += 1
            
            log.info(f"Updated service level for item {item.id}: "
                    f"{original_service_level} → {item.service_level_goal}")
            
            # Recalculate safety stock and order points if possible
            try:
                from warehouse_replenishment.services.safety_stock_service import SafetyStockService
                ss_service = SafetyStockService(session)
                ss_service.update_safety_stock_for_item(
                    item.id, 
                    update_sstf=True,
                    update_order_points=True
                )
            except ImportError:
                # Safety stock service not available, continue without updating
                pass
            
        except Exception as e:
            log.error(f"Error updating service level for item {item.id}: {str(e)}")
            
            # Create error record
            param_item = TimeBasedParameterItem(
                parameter_id=parameter.id,
                item_id=item.id,
                effective_date=effective_date,
                expression=parameter.expression,
                error_message=str(e)
            )
            session.add(param_item)
            
            results['errors'] += 1

def process_buyer_class_parameter(
    session: Session,
    parameter: TimeBasedParameter,
    effective_date: date,
    results: Dict
) -> None:
    """Process a buyer class parameter.
    
    Args:
        session: Database session
        parameter: Parameter to process
        effective_date: Effective date
        results: Results dictionary to update
        
    Returns:
        None (updates results dictionary)
    """
    log = logger.get_logger('batch')
    
    # Get affected items
    items = get_items_for_parameter(session, parameter)
    results['affected_items'] = len(items)
    
    # Validate buyer class
    try:
        new_buyer_class = parameter.expression.strip()
        if not any(bc.value == new_buyer_class for bc in BuyerClassCode):
            raise TimeBasedParameterError(f"Invalid buyer class: {new_buyer_class}")
    except Exception as e:
        results['error'] = str(e)
        results['errors'] += 1
        return
    
    # Process each item
    for item in items:
        try:
            # Create parameter item record
            param_item = TimeBasedParameterItem(
                parameter_id=parameter.id,
                item_id=item.id,
                effective_date=effective_date,
                expression=parameter.expression
            )
            
            # Store original values for reference
            original_buyer_class = item.buyer_class.value if item.buyer_class else None
            
            # Update buyer class
            item.buyer_class = new_buyer_class
            
            # Record the change in the parameter item
            param_item.changes = json.dumps({
                'buyer_class': {
                    'before': original_buyer_class,
                    'after': new_buyer_class
                }
            })
            
            session.add(param_item)
            results['processed_items'] += 1
            
            log.info(f"Updated buyer class for item {item.id}: "
                    f"{original_buyer_class} → {new_buyer_class}")
            
        except Exception as e:
            log.error(f"Error updating buyer class for item {item.id}: {str(e)}")
            
            # Create error record
            param_item = TimeBasedParameterItem(
                parameter_id=parameter.id,
                item_id=item.id,
                effective_date=effective_date,
                expression=parameter.expression,
                error_message=str(e)
            )
            session.add(param_item)
            
            results['errors'] += 1

def process_price_change_parameter(
    session: Session,
    parameter: TimeBasedParameter,
    effective_date: date,
    results: Dict
) -> None:
    """Process a price change parameter.
    
    Args:
        session: Database session
        parameter: Parameter to process
        effective_date: Effective date
        results: Results dictionary to update
        
    Returns:
        None (updates results dictionary)
    """
    log = logger.get_logger('batch')
    
    # Get affected items
    items = get_items_for_parameter(session, parameter)
    results['affected_items'] = len(items)
    
    # Check if expression format is for price change
    is_percentage = '%' in parameter.expression
    is_adjustment = '+' in parameter.expression or '-' in parameter.expression
    
    # Process each item
    for item in items:
        try:
            # Create parameter item record
            param_item = TimeBasedParameterItem(
                parameter_id=parameter.id,
                item_id=item.id,
                effective_date=effective_date,
                expression=parameter.expression
            )
            
            # Store original values for reference
            original_purchase_price = item.purchase_price
            
            # Apply price changes
            if is_percentage:
                # Handle percentage change (e.g., '+5%', '-10%')
                match = re.match(r'([+-])(\d+(?:\.\d+)?)%', parameter.expression)
                if match:
                    sign, value = match.groups()
                    percentage = float(value) / 100.0
                    
                    if sign == '+':
                        new_purchase_price = item.purchase_price * (1 + percentage)
                    else:  # sign == '-'
                        new_purchase_price = item.purchase_price * (1 - percentage)
                else:
                    raise TimeBasedParameterError(f"Invalid percentage format: {parameter.expression}")
            elif is_adjustment:
                # Expression is an adjustment formula
                new_purchase_price = evaluate_expression(
                    parameter.expression, 
                    item,
                    current_price=item.purchase_price
                )
            else:
                # Expression is a direct value
                try:
                    new_purchase_price = float(parameter.expression)
                except ValueError:
                    # Try evaluating as an expression
                    new_purchase_price = evaluate_expression(parameter.expression, item)
            
            # Ensure price is not negative
            new_purchase_price = max(0, new_purchase_price)
            
            # Update price
            item.purchase_price = new_purchase_price
            
            # Record the change in the parameter item
            param_item.changes = json.dumps({
                'purchase_price': {
                    'before': original_purchase_price,
                    'after': item.purchase_price
                }
            })
            
            session.add(param_item)
            results['processed_items'] += 1
            
            log.info(f"Updated purchase price for item {item.id}: "
                    f"{original_purchase_price} → {item.purchase_price}")
            
        except Exception as e:
            log.error(f"Error updating price for item {item.id}: {str(e)}")
            
            # Create error record
            param_item = TimeBasedParameterItem(
                parameter_id=parameter.id,
                item_id=item.id,
                effective_date=effective_date,
                expression=parameter.expression,
                error_message=str(e)
            )
            session.add(param_item)
            
            results['errors'] += 1

def run_time_based_parameters_job(
    effective_date: Optional[date] = None,
    parameter_id: Optional[int] = None
) -> Dict:
    """Run the time-based parameters job.
    
    Args:
        effective_date: Optional effective date for parameters
        parameter_id: Optional specific parameter ID to process
        
    Returns:
        Dictionary with job results
    """
    # Set up logging
    job_logger = logging.getLogger('batch')
    
    start_time = datetime.now()
    job_logger.info(f"Starting time-based parameters job at {start_time}")
    
    try:
        # Process parameters
        results = process_time_based_parameters(effective_date, parameter_id)
        
        # Log results
        job_logger.info(f"Time-based parameters job completed successfully in {results.get('duration')}")
        job_logger.info(f"Processed {results.get('processed_parameters')} of {results.get('total_parameters')} parameters")
        job_logger.info(f"Processed {results.get('processed_items')} of {results.get('affected_items')} items")
        
        if results.get('errors', 0) > 0:
            job_logger.warning(f"Encountered {results.get('errors')} errors during processing")
        
        results['success'] = True
        
        return results
    
    except Exception as e:
        job_logger.error(f"Error during time-based parameters job: {str(e)}")
        
        return {
            'success': False,
            'error': str(e),
            'start_time': start_time,
            'end_time': datetime.now()
        }

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Process time-based parameters')
    parser.add_argument('--date', type=str, help='Effective date (YYYY-MM-DD)')
    parser.add_argument('--parameter-id', type=int, help='Process specific parameter ID')
    
    args = parser.parse_args()
    
    effective_date = None
    if args.date:
        effective_date = datetime.strptime(args.date, '%Y-%m-%d').date()
    
    run_time_based_parameters_job(effective_date, args.parameter_id)