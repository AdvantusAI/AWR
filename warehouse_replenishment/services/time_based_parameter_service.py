# warehouse_replenishment/services/time_based_parameter_service.py
from datetime import date, datetime, timedelta
from typing import Dict, List, Tuple, Optional, Union, Any
import logging
import json
import sys
import os
from pathlib import Path

# Add the parent directory to the path so we can import our modules
parent_dir = str(Path(__file__).parent.parent.parent)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from sqlalchemy import and_, func, or_
from sqlalchemy.orm import Session

from warehouse_replenishment.models import (
    TimeBasedParameter, TimeBasedParameterItem, Item, Vendor
)
from warehouse_replenishment.exceptions import TimeBasedParameterError
from warehouse_replenishment.logging_setup import logger

logger = logging.getLogger(__name__)

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

class TimeBasedParameterService:
    """Service for handling time-based parameter operations."""
    
    def __init__(self, session: Session):
        """Initialize the time-based parameter service.
        
        Args:
            session: Database session
        """
        self.session = session
    
    def get_due_parameters(self, for_date: Optional[date] = None) -> List[TimeBasedParameter]:
        """Get time-based parameters that are due as of a specific date.
        
        Args:
            for_date: Optional date to check (defaults to current date)
            
        Returns:
            List of time-based parameters that are due
        """
        if for_date is None:
            for_date = date.today()
        
        # Get approved or pending parameters that are due on or before the specified date
        return self.session.query(TimeBasedParameter).filter(
            TimeBasedParameter.effective_date <= for_date,
            TimeBasedParameter.status.in_(['PENDING', 'APPROVED'])
        ).all()
    
    def process_due_parameters(self, for_date: Optional[date] = None) -> Dict:
        """Process time-based parameters that are due.
        
        Args:
            for_date: Optional date to check (defaults to current date)
            
        Returns:
            Dictionary with processing results
        """
        if for_date is None:
            for_date = date.today()
        
        parameters = self.get_due_parameters(for_date)
        
        results = {
            'total_parameters': len(parameters),
            'processed_parameters': 0,
            'affected_items': 0,
            'processed_items': 0,
            'errors': 0,
            'start_time': datetime.now(),
            'end_time': None,
            'duration': None
        }
        
        # Process each parameter
        for param in parameters:
            param_results = self.process_parameter(param, for_date)
            
            # Update parameter status
            if param_results['success']:
                param.status = 'APPLIED'
            else:
                param.status = 'ERROR'
                param.comment = param_results.get('error', 'Unknown error')
            
            # Update results
            results['processed_parameters'] += 1
            results['affected_items'] += param_results.get('affected_items', 0)
            results['processed_items'] += param_results.get('processed_items', 0)
            results['errors'] += param_results.get('errors', 0)
        
        # Commit changes
        try:
            self.session.commit()
        except Exception as e:
            self.session.rollback()
            logger.error(f"Error committing parameter updates: {str(e)}")
            results['errors'] += 1
        
        # Set end time and duration
        results['end_time'] = datetime.now()
        results['duration'] = results['end_time'] - results['start_time']
        
        return results
    
    def process_parameter(self, parameter: TimeBasedParameter, effective_date: date) -> Dict:
        """Process a single time-based parameter.
        
        Args:
            parameter: Parameter to process
            effective_date: Effective date
            
        Returns:
            Dictionary with processing results
        """
        logger.info(f"Processing parameter {parameter.id}: {parameter.description}")
        
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
                self._process_demand_forecast_parameter(parameter, effective_date, results)
            elif parameter.parameter_type == 'LEAD_TIME':
                self._process_lead_time_parameter(parameter, effective_date, results)
            elif parameter.parameter_type == 'SERVICE_LEVEL':
                self._process_service_level_parameter(parameter, effective_date, results)
            elif parameter.parameter_type == 'BUYER_CLASS':
                self._process_buyer_class_parameter(parameter, effective_date, results)
            elif parameter.parameter_type == 'PRICE_CHANGE':
                self._process_price_change_parameter(parameter, effective_date, results)
            else:
                # Unknown parameter type
                results['error'] = f"Unknown parameter type: {parameter.parameter_type}"
                return results
            
            # Set success flag if no errors
            if results['errors'] == 0:
                results['success'] = True
            
            return results
        
        except Exception as e:
            logger.error(f"Error processing parameter {parameter.id}: {str(e)}")
            results['error'] = str(e)
            results['errors'] += 1
            return results
    
    def get_items_for_parameter(self, parameter: TimeBasedParameter) -> List[Item]:
        """Get items that should be affected by a parameter.
        
        Args:
            parameter: Parameter to process
            
        Returns:
            List of items to process
        """
        # Check if specific items are already defined for this parameter
        parameter_items = self.session.query(TimeBasedParameterItem).filter(
            TimeBasedParameterItem.parameter_id == parameter.id
        ).all()
        
        if parameter_items:
            # If specific items are defined, use those
            item_ids = [param_item.item_id for param_item in parameter_items]
            return self.session.query(Item).filter(Item.id.in_(item_ids)).all()
        
        # Otherwise, check if there's a buyer_id filter
        if parameter.buyer_id:
            return self.session.query(Item).filter(Item.buyer_id == parameter.buyer_id).all()
        
        # Default: affect all active items (Regular and Watch)
        return self.session.query(Item).filter(
            Item.buyer_class.in_(['R', 'W'])
        ).all()
    
    def _process_demand_forecast_parameter(
        self,
        parameter: TimeBasedParameter,
        effective_date: date,
        results: Dict
    ) -> None:
        """Process a demand forecast parameter.
        
        Args:
            parameter: Parameter to process
            effective_date: Effective date
            results: Results dictionary to update
        """
        # Get affected items
        items = self.get_items_for_parameter(parameter)
        results['affected_items'] = len(items)
        
        # Process each item
        for item in items:
            try:
                # Check if this item is already in TimeBasedParameterItem
                param_item = self.session.query(TimeBasedParameterItem).filter(
                    TimeBasedParameterItem.parameter_id == parameter.id,
                    TimeBasedParameterItem.item_id == item.id
                ).first()
                
                if not param_item:
                    # Create parameter item record
                    param_item = TimeBasedParameterItem(
                        parameter_id=parameter.id,
                        item_id=item.id,
                        effective_date=effective_date,
                        expression=parameter.expression
                    )
                    self.session.add(param_item)
                
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
                
                results['processed_items'] += 1
                
                logger.info(f"Updated demand forecast for item {item.id}: "
                          f"{original_4weekly} → {item.demand_4weekly} "
                          f"(multiplier: {multiplier})")
                
            except Exception as e:
                logger.error(f"Error updating forecast for item {item.id}: {str(e)}")
                
                # Update or create error record
                if param_item:
                    param_item.error_message = str(e)
                else:
                    param_item = TimeBasedParameterItem(
                        parameter_id=parameter.id,
                        item_id=item.id,
                        effective_date=effective_date,
                        expression=parameter.expression,
                        error_message=str(e)
                    )
                    self.session.add(param_item)
                
                results['errors'] += 1
    
    def _process_lead_time_parameter(
        self,
        parameter: TimeBasedParameter,
        effective_date: date,
        results: Dict
    ) -> None:
        """Process a lead time parameter.
        
        Args:
            parameter: Parameter to process
            effective_date: Effective date
            results: Results dictionary to update
        """
        # Get affected items
        items = self.get_items_for_parameter(parameter)
        results['affected_items'] = len(items)
        
        # Check if expression format is for lead time
        is_adjustment = '+' in parameter.expression or '-' in parameter.expression or '*' in parameter.expression
        
        # Process each item
        for item in items:
            try:
                # Check if this item is already in TimeBasedParameterItem
                param_item = self.session.query(TimeBasedParameterItem).filter(
                    TimeBasedParameterItem.parameter_id == parameter.id,
                    TimeBasedParameterItem.item_id == item.id
                ).first()
                
                if not param_item:
                    # Create parameter item record
                    param_item = TimeBasedParameterItem(
                        parameter_id=parameter.id,
                        item_id=item.id,
                        effective_date=effective_date,
                        expression=parameter.expression
                    )
                    self.session.add(param_item)
                
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
                
                results['processed_items'] += 1
                
                logger.info(f"Updated lead time for item {item.id}: "
                          f"{original_lead_time} → {item.lead_time_forecast}")
                
            except Exception as e:
                logger.error(f"Error updating lead time for item {item.id}: {str(e)}")
                
                # Update or create error record
                if param_item:
                    param_item.error_message = str(e)
                else:
                    param_item = TimeBasedParameterItem(
                        parameter_id=parameter.id,
                        item_id=item.id,
                        effective_date=effective_date,
                        expression=parameter.expression,
                        error_message=str(e)
                    )
                    self.session.add(param_item)
                
                results['errors'] += 1
    
    def _process_service_level_parameter(
        self,
        parameter: TimeBasedParameter,
        effective_date: date,
        results: Dict
    ) -> None:
        """Process a service level parameter.
        
        Args:
            parameter: Parameter to process
            effective_date: Effective date
            results: Results dictionary to update
        """
        # Get affected items
        items = self.get_items_for_parameter(parameter)
        results['affected_items'] = len(items)
        
        # Process each item
        for item in items:
            try:
                # Check if this item is already in TimeBasedParameterItem
                param_item = self.session.query(TimeBasedParameterItem).filter(
                    TimeBasedParameterItem.parameter_id == parameter.id,
                    TimeBasedParameterItem.item_id == item.id
                ).first()
                
                if not param_item:
                    # Create parameter item record
                    param_item = TimeBasedParameterItem(
                        parameter_id=parameter.id,
                        item_id=item.id,
                        effective_date=effective_date,
                        expression=parameter.expression
                    )
                    self.session.add(param_item)
                
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
                
                results['processed_items'] += 1
                
                logger.info(f"Updated service level for item {item.id}: "
                          f"{original_service_level} → {item.service_level_goal}")
                
            except Exception as e:
                logger.error(f"Error updating service level for item {item.id}: {str(e)}")
                
                # Update or create error record
                if param_item:
                    param_item.error_message = str(e)
                else:
                    param_item = TimeBasedParameterItem(
                        parameter_id=parameter.id,
                        item_id=item.id,
                        effective_date=effective_date,
                        expression=parameter.expression,
                        error_message=str(e)
                    )
                    self.session.add(param_item)
                
                results['errors'] += 1
    
    def _process_buyer_class_parameter(
        self,
        parameter: TimeBasedParameter,
        effective_date: date,
        results: Dict
    ) -> None:
        """Process a buyer class parameter.
        
        Args:
            parameter: Parameter to process
            effective_date: Effective date
            results: Results dictionary to update
        """
        # Get affected items
        items = self.get_items_for_parameter(parameter)
        results['affected_items'] = len(items)
        
        # Process each item
        for item in items:
            try:
                # Check if this item is already in TimeBasedParameterItem
                param_item = self.session.query(TimeBasedParameterItem).filter(
                    TimeBasedParameterItem.parameter_id == parameter.id,
                    TimeBasedParameterItem.item_id == item.id
                ).first()
                
                if not param_item:
                    # Create parameter item record
                    param_item = TimeBasedParameterItem(
                        parameter_id=parameter.id,
                        item_id=item.id,
                        effective_date=effective_date,
                        expression=parameter.expression
                    )
                    self.session.add(param_item)
                
                # Store original values for reference
                original_buyer_class = item.buyer_class.value if item.buyer_class else None
                
                # Get new buyer class
                new_buyer_class = parameter.expression.strip()
                
                # Validate buyer class (basic validation)
                valid_classes = ['R', 'W', 'M', 'D', 'U']
                if new_buyer_class not in valid_classes:
                    raise TimeBasedParameterError(f"Invalid buyer class: {new_buyer_class}")
                
                # Update buyer class
                item.buyer_class = new_buyer_class
                
                # Record the change in the parameter item
                param_item.changes = json.dumps({
                    'buyer_class': {
                        'before': original_buyer_class,
                        'after': new_buyer_class
                    }
                })
                
                results['processed_items'] += 1
                
                logger.info(f"Updated buyer class for item {item.id}: "
                          f"{original_buyer_class} → {new_buyer_class}")
                
            except Exception as e:
                logger.error(f"Error updating buyer class for item {item.id}: {str(e)}")
                
                # Update or create error record
                if param_item:
                    param_item.error_message = str(e)
                else:
                    param_item = TimeBasedParameterItem(
                        parameter_id=parameter.id,
                        item_id=item.id,
                        effective_date=effective_date,
                        expression=parameter.expression,
                        error_message=str(e)
                    )
                    self.session.add(param_item)
                
                results['errors'] += 1
    
    def _process_price_change_parameter(
        self,
        parameter: TimeBasedParameter,
        effective_date: date,
        results: Dict
    ) -> None:
        """Process a price change parameter.
        
        Args:
            parameter: Parameter to process
            effective_date: Effective date
            results: Results dictionary to update
        """
        # Get affected items
        items = self.get_items_for_parameter(parameter)
        results['affected_items'] = len(items)
        
        # Check if expression format is for price change
        is_percentage = '%' in parameter.expression
        is_adjustment = '+' in parameter.expression or '-' in parameter.expression or '*' in parameter.expression
        
        # Process each item
        for item in items:
            try:
                # Check if this item is already in TimeBasedParameterItem
                param_item = self.session.query(TimeBasedParameterItem).filter(
                    TimeBasedParameterItem.parameter_id == parameter.id,
                    TimeBasedParameterItem.item_id == item.id
                ).first()
                
                if not param_item:
                    # Create parameter item record
                    param_item = TimeBasedParameterItem(
                        parameter_id=parameter.id,
                        item_id=item.id,
                        effective_date=effective_date,
                        expression=parameter.expression
                    )
                    self.session.add(param_item)
                
                # Store original values for reference
                original_purchase_price = item.purchase_price
                
                # Apply price changes
                if is_percentage:
                    # Handle percentage change (e.g., '+5%', '-10%')
                    import re
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
                
                # Calculate and update sales price based on gross margin if available
                if hasattr(item, 'gross_margin') and item.gross_margin is not None and item.gross_margin > 0:
                    margin_multiplier = 1 + (item.gross_margin / 100)
                    item.sales_price = item.purchase_price * margin_multiplier
                
                # Record the change in the parameter item
                param_item.changes = json.dumps({
                    'purchase_price': {
                        'before': original_purchase_price,
                        'after': item.purchase_price
                    }
                })
                
                results['processed_items'] += 1
                
                logger.info(f"Updated purchase price for item {item.id}: "
                          f"{original_purchase_price} → {item.purchase_price}")
                
            except Exception as e:
                logger.error(f"Error updating price for item {item.id}: {str(e)}")
                
                # Update or create error record
                if param_item:
                    param_item.error_message = str(e)
                else:
                    param_item = TimeBasedParameterItem(
                        parameter_id=parameter.id,
                        item_id=item.id,
                        effective_date=effective_date,
                        expression=parameter.expression,
                        error_message=str(e)
                    )
                    self.session.add(param_item)
                
                results['errors'] += 1