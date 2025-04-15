# warehouse_replenishment/core/order_policy.py
import math
from typing import Dict, List, Tuple, Optional, Union
import numpy as np
from scipy import optimize

from ..exceptions import OPAError

def calculate_acquisition_cost(
    order_header_cost: float, 
    order_line_cost: float,
    num_lines: int
) -> float:
    """Calculate total acquisition cost for an order.
    
    Args:
        order_header_cost: Fixed cost for creating an order
        order_line_cost: Cost per line item
        num_lines: Number of lines in the order
        
    Returns:
        Total acquisition cost
    """
    return order_header_cost + (order_line_cost * num_lines)

def calculate_carrying_cost(
    inventory_value: float, 
    carrying_rate: float
) -> float:
    """Calculate annual carrying cost for inventory.
    
    Args:
        inventory_value: Total inventory value
        carrying_rate: Annual carrying cost rate (as percentage)
        
    Returns:
        Annual carrying cost
    """
    if inventory_value < 0:
        raise OPAError("Inventory value cannot be negative")
    
    return inventory_value * (carrying_rate / 100.0)

def analyze_order_policy(
    demand_forecast: float, 
    carrying_cost_rate: float,
    acquisition_cost: float,
    min_order_quantity: float = None,
    max_order_quantity: float = None
) -> Dict[str, Union[float, str]]:
    """Optimize order policy using Economic Order Quantity (EOQ) model.
    
    Args:
        demand_forecast: Annual demand forecast
        carrying_cost_rate: Annual carrying cost rate (as percentage)
        acquisition_cost: Cost of placing an order
        min_order_quantity: Optional minimum order quantity
        max_order_quantity: Optional maximum order quantity
        
    Returns:
        Dictionary with order policy optimization results
    """
    def total_cost_function(order_quantity):
        """Calculate total annual cost for a given order quantity."""
        if order_quantity <= 0:
            return float('inf')
        
        # Number of orders per year
        num_orders = demand_forecast / order_quantity
        
        # Acquisition cost
        total_acquisition_cost = num_orders * acquisition_cost
        
        # Average inventory carrying cost 
        # (assuming order quantity / 2 as average inventory)
        avg_inventory = order_quantity / 2
        total_carrying_cost = avg_inventory * (carrying_cost_rate / 100.0)
        
        return total_acquisition_cost + total_carrying_cost
    
    # If min or max are not provided, use None to allow unconstrained optimization
    bounds = []
    if min_order_quantity is not None:
        bounds.append((min_order_quantity, None))
    
    try:
        # Use scipy to minimize total cost
        if bounds:
            result = optimize.minimize_scalar(
                total_cost_function, 
                bounds=bounds, 
                method='bounded'
            )
        else:
            # Classic EOQ formula when no bounds
            # Order Quantity = sqrt((2 * Demand * Acquisition Cost) / (Carrying Cost Rate))
            classic_eoq = math.sqrt(
                (2 * demand_forecast * acquisition_cost) / 
                (carrying_cost_rate / 100.0)
            )
            result = optimize.minimize_scalar(total_cost_function)
        
        # Constrain to max order quantity if provided
        optimal_order_quantity = min(
            result.x, 
            max_order_quantity if max_order_quantity is not None else float('inf')
        )
        
        # Calculate key metrics
        num_orders_per_year = demand_forecast / optimal_order_quantity
        avg_inventory = optimal_order_quantity / 2
        total_acquisition_cost = num_orders_per_year * acquisition_cost
        total_carrying_cost = avg_inventory * (carrying_cost_rate / 100.0)
        total_annual_cost = total_acquisition_cost + total_carrying_cost
        
        return {
            'optimal_order_quantity': round(optimal_order_quantity, 2),
            'num_orders_per_year': round(num_orders_per_year, 2),
            'avg_inventory_level': round(avg_inventory, 2),
            'total_acquisition_cost': round(total_acquisition_cost, 2),
            'total_carrying_cost': round(total_carrying_cost, 2),
            'total_annual_cost': round(total_annual_cost, 2),
            'status': 'OPTIMIZED'
        }
    
    except Exception as e:
        raise OPAError(f"Error in order policy analysis: {str(e)}")

def evaluate_order_cycle_efficiency(
    current_order_cycle: int,
    demand_forecast: float,
    carrying_cost_rate: float,
    acquisition_cost: float,
    alternative_cycles: List[int] = None
) -> List[Dict[str, Union[float, str]]]:
    """Compare efficiency of different order cycles.
    
    Args:
        current_order_cycle: Current order cycle in days
        demand_forecast: Annual demand forecast
        carrying_cost_rate: Annual carrying cost rate (as percentage)
        acquisition_cost: Cost of placing an order
        alternative_cycles: Optional list of alternative order cycles to evaluate
        
    Returns:
        List of order cycle evaluation results
    """
    if alternative_cycles is None:
        # Default alternative cycles
        alternative_cycles = [7, 14, 21, 28, 45, 60]
    
    # Include current order cycle
    cycles_to_evaluate = list(set([current_order_cycle] + alternative_cycles))
    
    results = []
    
    for cycle in cycles_to_evaluate:
        try:
            # Calculate order quantity for this cycle
            num_orders_per_year = 365 / cycle
            order_quantity = demand_forecast / num_orders_per_year
            
            # Calculate carrying cost (average inventory is order quantity / 2)
            avg_inventory = order_quantity / 2
            total_carrying_cost = avg_inventory * (carrying_cost_rate / 100.0)
            
            # Calculate total acquisition cost
            total_acquisition_cost = num_orders_per_year * acquisition_cost
            
            # Total annual cost
            total_annual_cost = total_acquisition_cost + total_carrying_cost
            
            # Efficiency calculation relative to current cycle
            efficiency_score = 100.0
            comparison_status = 'CURRENT'
            
            if cycle != current_order_cycle:
                # Compare to current cycle's total annual cost
                current_num_orders = 365 / current_order_cycle
                current_order_quantity = demand_forecast / current_num_orders
                current_avg_inventory = current_order_quantity / 2
                current_total_carrying_cost = current_avg_inventory * (carrying_cost_rate / 100.0)
                current_total_acquisition_cost = current_num_orders * acquisition_cost
                current_total_annual_cost = current_total_acquisition_cost + current_total_carrying_cost
                
                # Calculate efficiency score
                # Lower total cost is better
                efficiency_score = (current_total_annual_cost / total_annual_cost) * 100
                
                # Determine comparison status
                if efficiency_score > 100:
                    comparison_status = 'BETTER'
                elif efficiency_score < 100:
                    comparison_status = 'WORSE'
                else:
                    comparison_status = 'EQUIVALENT'
            
            results.append({
                'order_cycle_days': cycle,
                'num_orders_per_year': round(num_orders_per_year, 2),
                'order_quantity': round(order_quantity, 2),
                'avg_inventory_level': round(avg_inventory, 2),
                'total_acquisition_cost': round(total_acquisition_cost, 2),
                'total_carrying_cost': round(total_carrying_cost, 2),
                'total_annual_cost': round(total_annual_cost, 2),
                'efficiency_score': round(efficiency_score, 2),
                'status': comparison_status
            })
        
        except Exception as e:
            results.append({
                'order_cycle_days': cycle,
                'error': str(e),
                'status': 'ERROR'
            })
    
    # Sort results by total annual cost (lowest first)
    results.sort(key=lambda x: x.get('total_annual_cost', float('inf')))
    
    return results

def calculate_vendor_discount_impact(
    base_price: float,
    order_quantity: float,
    discount_brackets: List[Dict[str, Union[float, int]]]
) -> Dict[str, Union[float, str]]:
    """Calculate the impact of vendor discount brackets on order economics.
    
    Args:
        base_price: Base item price
        order_quantity: Order quantity
        discount_brackets: List of discount brackets
        
    Returns:
        Dictionary with discount impact analysis
    """
    if not discount_brackets or base_price <= 0 or order_quantity <= 0:
        raise OPAError("Invalid input parameters for discount analysis")
    
    # Sort brackets by minimum quantity in descending order
    sorted_brackets = sorted(discount_brackets, key=lambda x: x.get('minimum', 0), reverse=True)
    
    # Find applicable bracket
    applicable_bracket = None
    for bracket in sorted_brackets:
        if order_quantity >= bracket.get('minimum', 0):
            applicable_bracket = bracket
            break
    
    # If no bracket found, use base price
    if not applicable_bracket:
        return {
            'base_total_cost': round(base_price * order_quantity, 2),
            'discounted_total_cost': round(base_price * order_quantity, 2),
            'discount_percentage': 0.0,
            'savings': 0.0,
            'discount_bracket': None,
            'status': 'NO_DISCOUNT'
        }
    
    # Calculate discounted price
    discount_percentage = applicable_bracket.get('discount', 0.0)
    discounted_price = base_price * (1 - discount_percentage / 100.0)
    
    # Calculate costs and savings
    base_total_cost = base_price * order_quantity
    discounted_total_cost = discounted_price * order_quantity
    savings = base_total_cost - discounted_total_cost
    
    return {
        'base_total_cost': round(base_total_cost, 2),
        'discounted_total_cost': round(discounted_total_cost, 2),
        'discount_percentage': round(discount_percentage, 2),
        'savings': round(savings, 2),
        'discount_bracket': {
            'minimum': applicable_bracket.get('minimum', 0),
            'maximum': applicable_bracket.get('maximum', float('inf')),
            'unit': applicable_bracket.get('unit', 'UNITS')
        },
        'status': 'DISCOUNTED'
    }

def optimize_multi_vendor_strategy(
    vendor_options: List[Dict],
    demand_forecast: float
) -> Dict[str, Union[float, str, List]]:
    """Optimize purchasing strategy across multiple vendors.
    
    Args:
        vendor_options: List of vendor dictionaries with pricing and constraints
        demand_forecast: Annual demand forecast
        
    Returns:
        Dictionary with optimized multi-vendor strategy
    """
    if not vendor_options or demand_forecast <= 0:
        raise OPAError("Invalid input for multi-vendor optimization")
    
    # Validate vendor options schema
    for vendor in vendor_options:
        required_keys = ['vendor_id', 'price', 'min_order_quantity', 'max_order_quantity']
        for key in required_keys:
            if key not in vendor:
                raise OPAError(f"Missing required key {key} for vendor")
    
    # Sort vendors by price
    sorted_vendors = sorted(vendor_options, key=lambda x: x['price'])
    
    # Allocation strategy
    allocation_results = []
    remaining_demand = demand_forecast
    total_cost = 0.0
    
    for vendor in sorted_vendors:
        # Determine allocation quantity
        allocation_quantity = min(remaining_demand, vendor['max_order_quantity'])
        
        # Ensure minimum order quantity is met
        if allocation_quantity < vendor['min_order_quantity']:
            continue
        
        # Calculate cost for this allocation
        allocation_cost = allocation_quantity * vendor['price']
        
        allocation_results.append({
            'vendor_id': vendor['vendor_id'],
            'allocation_quantity': round(allocation_quantity, 2),
            'price_per_unit': round(vendor['price'], 2),
            'total_cost': round(allocation_cost, 2)
        })
        
        # Update remaining demand and total cost
        remaining_demand -= allocation_quantity
        total_cost += allocation_cost
        
        # Stop if demand is fulfilled
        if remaining_demand <= 0:
            break
    
    # Check if full demand was met
    status = 'FULLY_ALLOCATED' if remaining_demand <= 0 else 'PARTIALLY_ALLOCATED'
    
    return {
        'total_demand': round(demand_forecast, 2),
        'remaining_demand': round(remaining_demand, 2),
        'total_cost': round(total_cost, 2),
        'vendor_allocations': allocation_results,
        'status': status
    }