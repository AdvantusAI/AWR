"""
Order Policy Analysis (OPA) services for the ASR system.

This module implements the algorithms and functions needed to determine
the most profitable order cycle for a source based on acquisition and 
carrying costs.
"""
import logging
import math
from sqlalchemy import and_

from models.sku import SKU, ForecastData
from models.source import Source, SourceBracket
from utils.db import get_session
from config.settings import ASR_CONFIG

logger = logging.getLogger(__name__)

def calculate_annual_acquisition_cost(header_cost, line_cost, num_lines, order_cycle):
    """
    Calculate annual acquisition cost based on order cycle.
    
    Args:
        header_cost (float): Cost per order header
        line_cost (float): Cost per order line
        num_lines (int): Number of lines per order
        order_cycle (int): Days between orders
    
    Returns:
        float: Annual acquisition cost
    """
    # Calculate number of orders per year
    orders_per_year = 365 / order_cycle
    
    # Calculate total acquisition cost per order
    acquisition_cost_per_order = header_cost + (line_cost * num_lines)
    
    # Calculate annual acquisition cost
    annual_acquisition_cost = acquisition_cost_per_order * orders_per_year
    
    return annual_acquisition_cost

def calculate_annual_carrying_cost(average_inventory, carrying_cost_rate, bracket_discount=0.0):
    """
    Calculate annual carrying cost based on average inventory.
    
    Args:
        average_inventory (float): Average inventory value in dollars
        carrying_cost_rate (float): Annual carrying cost rate as a decimal
        bracket_discount (float): Discount percentage for bracket
    
    Returns:
        float: Annual carrying cost
    """
    # Apply bracket discount to inventory value
    discounted_inventory = average_inventory * (1.0 - (bracket_discount / 100.0))
    
    # Calculate annual carrying cost
    annual_carrying_cost = discounted_inventory * carrying_cost_rate
    
    return annual_carrying_cost

def calculate_average_inventory(total_annual_inventory, order_cycle, safety_stock_value):
    """
    Calculate average inventory based on order cycle.
    
    Args:
        total_annual_inventory (float): Total annual inventory in dollars
        order_cycle (int): Days between orders
        safety_stock_value (float): Safety stock value in dollars
    
    Returns:
        float: Average inventory value
    """
    # Calculate average cycle stock (half of order quantity)
    average_cycle_stock = (total_annual_inventory * (order_cycle / 365.0)) / 2.0
    
    # Add safety stock
    average_inventory = average_cycle_stock + safety_stock_value
    
    return average_inventory

def calculate_order_value(skus, order_cycle, daily_demand_values=None):
    """
    Calculate the total order value based on order cycle.
    
    Args:
        skus (list): List of SKUs
        order_cycle (int): Days between orders
        daily_demand_values (dict): Pre-calculated daily demand values by SKU ID
    
    Returns:
        dict: Dictionary with order totals by unit of measure
    """
    # Initialize totals
    totals = {
        'amount': 0.0,
        'eaches': 0.0,
        'weight': 0.0,
        'volume': 0.0,
        'dozens': 0.0,
        'cases': 0.0,
        'layers': 0.0,
        'pallets': 0.0
    }
    
    # Calculate demand for each SKU
    for sku in skus:
        # Get daily demand value
        if daily_demand_values and sku.id in daily_demand_values:
            daily_demand = daily_demand_values[sku.id]
        else:
            # Get forecast data
            forecast_data = sku.forecast_data if hasattr(sku, 'forecast_data') else None
            if not forecast_data:
                continue
            
            # Calculate daily demand
            daily_demand = forecast_data.weekly_forecast / 7.0
        
        # Calculate order quantity
        order_quantity = daily_demand * order_cycle
        
        # Round to buying multiple if needed
        if sku.buying_multiple > 1:
            order_quantity = math.ceil(order_quantity / sku.buying_multiple) * sku.buying_multiple
        
        # Add to totals
        totals['amount'] += order_quantity * sku.purchase_price
        totals['eaches'] += order_quantity
        
        # Add other units based on conversion factors
        if hasattr(sku, 'weight') and sku.weight:
            totals['weight'] += order_quantity * sku.weight
        
        if hasattr(sku, 'cube') and sku.cube:
            totals['volume'] += order_quantity * sku.cube
        
        if hasattr(sku, 'units_per_case') and sku.units_per_case:
            totals['cases'] += order_quantity / sku.units_per_case
        
        # Add more units as needed
    
    return totals

def determine_bracket_for_order(brackets, order_value, unit_code):
    """
    Determine which bracket an order value fits into.
    
    Args:
        brackets (list): List of bracket objects
        order_value (float): Order value
        unit_code (int): Unit code (1=Amount, 2=Eaches, etc.)
    
    Returns:
        tuple: (bracket_number, discount_percentage)
    """
    # Filter brackets by unit code
    matching_brackets = [b for b in brackets if b.unit == unit_code]
    
    # Sort brackets by minimum (ascending)
    sorted_brackets = sorted(matching_brackets, key=lambda b: b.minimum)
    
    # Find the highest bracket the order fits into
    applicable_bracket = None
    
    for bracket in sorted_brackets:
        if order_value >= bracket.minimum and (bracket.maximum == 0 or order_value <= bracket.maximum):
            applicable_bracket = bracket
    
    # Return bracket information
    if applicable_bracket:
        return (applicable_bracket.bracket_number, applicable_bracket.discount_percentage)
    else:
        return (0, 0.0)  # No bracket applies

def run_order_policy_analysis(session, source_id, store_id):
    """
    Run Order Policy Analysis for a source.
    
    Args:
        session: SQLAlchemy session
        source_id (str): Source ID
        store_id (str): Store ID
    
    Returns:
        dict: OPA results
    """
    try:
        # Get the source
        source = session.query(Source).filter(Source.source_id == source_id).first()
        
        if not source:
            logger.error(f"Source {source_id} not found")
            return None
        
        # Get the bracket
        bracket_obj = next((b for b in source.brackets if b.bracket_number == bracket), None)
        
        if not bracket_obj:
            logger.error(f"Bracket {bracket} not found for source {source_id}")
            return None
        
        # Get active SKUs for this source
        skus = session.query(SKU).filter(
            and_(
                SKU.source_id == source.id,
                SKU.store_id == store_id,
                SKU.buyer_class.in_(['R', 'W'])
            )
        ).all()
        
        if not skus:
            logger.error(f"No active SKUs found for source {source_id}")
            return 0
        
        # Get base order cycle
        base_order_cycle = source.order_cycle
        
        # Calculate base order value
        base_order_values = calculate_order_value(skus, base_order_cycle)
        
        # Check if order already meets the bracket minimum
        if base_order_values['amount'] >= bracket_obj.minimum:
            return 0  # No additional days needed
        
        # Calculate daily demand value
        daily_demand_values = {}
        total_daily_demand_value = 0.0
        
        for sku in skus:
            # Get forecast data
            forecast_data = sku.forecast_data if hasattr(sku, 'forecast_data') else None
            if not forecast_data:
                continue
            
            # Calculate daily demand in units
            daily_demand_units = forecast_data.weekly_forecast / 7.0
            
            # Calculate daily demand in dollars
            daily_demand_value = daily_demand_units * sku.purchase_price
            
            # Add to total daily demand
            total_daily_demand_value += daily_demand_value
        
        # Calculate additional days needed
        additional_amount_needed = bracket_obj.minimum - base_order_values['amount']
        
        if total_daily_demand_value <= 0:
            return 0  # Avoid division by zero
        
        # Calculate days needed to reach minimum
        days_needed = math.ceil(additional_amount_needed / total_daily_demand_value)
        
        return days_needed
    
    except Exception as e:
        logger.error(f"Error calculating days to meet bracket: {e}")
        return 0

def get_effective_order_cycle(sku):
    """
    Get the effective order cycle for a SKU (greater of source cycle or SKU cycle).
    
    Args:
        sku: SKU object
    
    Returns:
        int: Effective order cycle in days
    """
    # Get source order cycle
    source_cycle = sku.source.order_cycle if sku.source else 0
    
    # Get SKU cycle (if available)
    sku_cycle = getattr(sku, 'sku_cycle', 0)
    
    # Return the greater of the two
    return max(source_cycle, sku_cycle)

def calculate_sku_order_cycle(session, sku_id, store_id):
    """
    Calculate the most economic order cycle for a specific SKU.
    
    Args:
        session: SQLAlchemy session
        sku_id (str): SKU ID
        store_id (str): Store ID
    
    Returns:
        int: SKU order cycle in days
    """
    try:
        # Get the SKU
        sku = session.query(SKU).filter(
            and_(SKU.sku_id == sku_id, SKU.store_id == store_id)
        ).first()
        
        if not sku:
            logger.error(f"SKU {sku_id} not found in store {store_id}")
            return None
        
        # Get forecast data
        forecast_data = sku.forecast_data if hasattr(sku, 'forecast_data') else None
        if not forecast_data:
            logger.error(f"Forecast data not found for SKU {sku_id}")
            return None
        
        # Get acquisition costs
        header_cost = getattr(sku.source, 'header_cost', ASR_CONFIG.get('default_header_cost', 25.0))
        line_cost = getattr(sku.source, 'line_cost', ASR_CONFIG.get('default_line_cost', 1.0))
        
        # Get carrying cost rate
        carrying_cost_rate = ASR_CONFIG.get('carrying_cost_rate', 0.40)  # 40%
        
        # Calculate daily demand in units
        daily_demand_units = forecast_data.weekly_forecast / 7.0
        
        # Calculate daily demand in dollars
        daily_demand_value = daily_demand_units * sku.purchase_price
        
        # Calculate annual demand value
        annual_demand_value = daily_demand_value * 365.0
        
        # If item has very low value, return a longer cycle
        if annual_demand_value < header_cost:
            return 90  # Default to 90 days for very low value items
        
        # Calculate Economic Order Quantity (EOQ)
        # EOQ = sqrt((2 * Annual Demand * Order Cost) / (Carrying Cost Rate * Unit Cost))
        order_cost = line_cost  # Just the line cost for a single SKU
        carrying_cost = carrying_cost_rate * sku.purchase_price
        
        if carrying_cost <= 0:
            return 30  # Default to 30 days if carrying cost is zero
        
        eoq = math.sqrt((2 * annual_demand_value * order_cost) / carrying_cost)
        
        # Convert EOQ to days of supply
        if daily_demand_units <= 0:
            return 30  # Default to 30 days if demand is zero
        
        days_of_supply = eoq / daily_demand_units
        
        # Round to nearest multiple of 7 (one week)
        days_of_supply = round(days_of_supply / 7) * 7
        
        # Ensure minimum and maximum values
        days_of_supply = max(7, min(90, days_of_supply))
        
        return int(days_of_supply)
    
    except Exception as e:
        logger.error(f"Error calculating SKU order cycle: {e}")
        return None

def accept_opa_result(session, source_id, order_cycle, bracket):
    """
    Accept an OPA result by updating the source's order cycle and current bracket.
    
    Args:
        session: SQLAlchemy session
        source_id (str): Source ID
        order_cycle (int): Order cycle in days
        bracket (int): Bracket number
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Get the source
        source = session.query(Source).filter(Source.source_id == source_id).first()
        
        if not source:
            logger.error(f"Source {source_id} not found")
            return False
        
        # Update source
        source.order_cycle = order_cycle
        source.current_bracket = bracket
        
        # Commit changes
        session.commit()
        return True
    
    except Exception as e:
        logger.error(f"Error accepting OPA result: {e}")
        session.rollback()
        return False

def simulate_bracket_build(session, source_id, store_id, bracket, add_days=0):
    """
    Simulate building an order to meet a specific bracket.
    
    Args:
        session: SQLAlchemy session
        source_id (str): Source ID
        store_id (str): Store ID
        bracket (int): Bracket number
        add_days (int): Additional days to add to order
    
    Returns:
        dict: Simulated order totals
    """
    try:
        # Get the source
        source = session.query(Source).filter(Source.source_id == source_id).first()
        
        if not source:
            logger.error(f"Source {source_id} not found")
            return None
        
        # Get the bracket
        bracket_obj = next((b for b in source.brackets if b.bracket_number == bracket), None)
        
        if not bracket_obj:
            logger.error(f"Bracket {bracket} not found for source {source_id}")
            return None
        
        # Get active SKUs for this source
        skus = session.query(SKU).filter(
            and_(
                SKU.source_id == source.id,
                SKU.store_id == store_id,
                SKU.buyer_class.in_(['R', 'W'])
            )
        ).all()
        
        if not skus:
            logger.error(f"No active SKUs found for source {source_id}")
            return None
        
        # Get base order cycle
        base_order_cycle = source.order_cycle
        
        # Add extra days
        effective_order_cycle = base_order_cycle + add_days
        
        # Calculate order value
        order_values = calculate_order_value(skus, effective_order_cycle)
        
        # Check if order meets the bracket minimum
        meets_minimum = order_values['amount'] >= bracket_obj.minimum
        
        # Check if order exceeds the bracket maximum
        exceeds_maximum = bracket_obj.maximum > 0 and order_values['amount'] > bracket_obj.maximum
        
        # Return results
        return {
            'order_cycle': base_order_cycle,
            'add_days': add_days,
            'effective_order_cycle': effective_order_cycle,
            'order_amount': order_values['amount'],
            'order_eaches': order_values['eaches'],
            'order_weight': order_values['weight'],
            'order_volume': order_values['volume'],
            'bracket_minimum': bracket_obj.minimum,
            'bracket_maximum': bracket_obj.maximum,
            'meets_minimum': meets_minimum,
            'exceeds_maximum': exceeds_maximum,
            'discount_percentage': bracket_obj.discount_percentage
        }
    
    except Exception as e:
        logger.error(f"Error simulating bracket build: {e}")
        return None

def calculate_days_to_meet_bracket(session, source_id, store_id, bracket):
    """
    Calculate the number of days to add to meet a specific bracket.
    
    Args:
        session: SQLAlchemy session
        source_id (str): Source ID
        store_id (str): Store ID
        bracket (int): Bracket number
    
    Returns:
        int: Number of days to add
    """
    try:
        # Get the source
        source = session.query(Source).filter(Source.source_id == source_id).first()
        
        if not source:
            logger.error(f"Source {source_id} not found")
            return None
        
        # Get active SKUs for this source
        skus = session.query(SKU).filter(
            and_(
                SKU.source_id == source.id,
                SKU.store_id == store_id,
                SKU.buyer_class.in_(['R', 'W'])
            )
        ).all()
        
        if not skus:
            logger.error(f"No active SKUs found for source {source_id}")
            return None
        
        # Get source brackets
        brackets = source.brackets
        
        # Get acquisition costs
        header_cost = getattr(source, 'header_cost', ASR_CONFIG.get('default_header_cost', 25.0))
        line_cost = getattr(source, 'line_cost', ASR_CONFIG.get('default_line_cost', 1.0))
        
        # Get carrying cost rate
        carrying_cost_rate = ASR_CONFIG.get('carrying_cost_rate', 0.40)  # 40%
        
        # Calculate daily demand values for all SKUs
        daily_demand_values = {}
        total_annual_demand_value = 0.0
        safety_stock_value = 0.0
        
        for sku in skus:
            # Get forecast data
            forecast_data = sku.forecast_data if hasattr(sku, 'forecast_data') else None
            if not forecast_data:
                continue
            
            # Calculate daily demand in units
            daily_demand_units = forecast_data.weekly_forecast / 7.0
            
            # Calculate daily demand in dollars
            daily_demand_value = daily_demand_units * sku.purchase_price
            
            # Store for later use
            daily_demand_values[sku.id] = daily_demand_value
            
            # Add to total annual demand value
            total_annual_demand_value += daily_demand_value * 365.0
            
            # Add safety stock value (this is a simplification)
            # In a real implementation, you would calculate safety stock properly
            safety_stock_days = 3.0  # Example value
            safety_stock_value += safety_stock_days * daily_demand_value
        
        # Test different order cycles
        # Start with a range of order cycles (e.g., 1, 3, 7, 14, 21, 28, 35, 42, 56, 70, 84 days)
        test_cycles = [1, 3, 7, 14, 21, 28, 35, 42, 56, 70, 84]
        
        results = []
        
        for cycle in test_cycles:
            # Calculate order value
            order_values = calculate_order_value(skus, cycle, daily_demand_values)
            
            # Determine applicable bracket
            bracket_number, discount_percentage = determine_bracket_for_order(
                brackets, order_values['amount'], 1  # Assuming Amount (1) is the unit code
            )
            
            # Calculate average inventory
            average_inventory = calculate_average_inventory(
                total_annual_demand_value, cycle, safety_stock_value
            )
            
            # Calculate annual acquisition cost
            annual_acquisition_cost = calculate_annual_acquisition_cost(
                header_cost, line_cost, len(skus), cycle
            )
            
            # Calculate annual carrying cost
            annual_carrying_cost = calculate_annual_carrying_cost(
                average_inventory, carrying_cost_rate, discount_percentage
            )
            
            # Calculate total annual cost
            total_annual_cost = annual_acquisition_cost + annual_carrying_cost
            
            # Calculate savings from discount
            annual_discount_savings = (total_annual_demand_value * discount_percentage) / 100.0
            
            # Calculate profit impact
            profit_impact = annual_discount_savings - total_annual_cost
            
            # Store results
            results.append({
                'order_cycle': cycle,
                'bracket': bracket_number,
                'discount_percentage': discount_percentage,
                'order_amount': order_values['amount'],
                'order_eaches': order_values['eaches'],
                'order_weight': order_values['weight'],
                'order_volume': order_values['volume'],
                'annual_acquisition_cost': annual_acquisition_cost,
                'annual_carrying_cost': annual_carrying_cost,
                'total_annual_cost': total_annual_cost,
                'annual_discount_savings': annual_discount_savings,
                'profit_impact': profit_impact
            })
        
        # Find the most profitable order cycle
        most_profitable = max(results, key=lambda r: r['profit_impact'])
        
        # Return results sorted by profit impact
        return {
            'results': sorted(results, key=lambda r: r['profit_impact'], reverse=True),
            'most_profitable': most_profitable
        }
    
    except Exception as e:
        logger.error(f"Error running Order Policy Analysis: {e}")
        return None