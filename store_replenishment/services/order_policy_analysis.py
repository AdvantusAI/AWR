# app/services/order_policy_analysis.py

"""
Path: app/services/order_policy_analysis.py

Order Policy Analysis (OPA): 
Determines the most profitable order cycle and order quantity by balancing acquisition (ordering) costs, 
carrying costs, vendor brackets, and discounts. 
Recommends the optimal order cycle (in days) and corresponding order quantity for a vendor or SKU.
"""

from typing import List, Optional, Dict
import numpy as np



class Bracket:
    def __init__(self, min_qty, max_qty, unit, discount_pct=0.0):
        self.min_qty = min_qty
        self.max_qty = max_qty
        self.unit = unit  # e.g., 'units', 'dollars', 'weight'
        self.discount_pct = discount_pct


def calculate_eoq(
    annual_demand: float,
    order_cost: float,
    annual_carrying_cost_rate: float,
    unit_cost: float
) -> float:
    """
    Classic Economic Order Quantity (EOQ) formula.
    """
    if annual_demand == 0 or annual_carrying_cost_rate == 0:
        return 0
    return np.sqrt((2 * annual_demand * order_cost) / (annual_carrying_cost_rate * unit_cost))

def opa_simulation_db(
    session,
    vendor_id: int,
    sku_id: int,
    forecasted_daily_demand: float,
    order_cost: float,
    annual_carrying_cost_rate: float,
    unit_cost: float,
    max_cycle_days: int = 90
) -> Optional[Dict]:
    """
    Simulates total cost for different order cycles and brackets for a given vendor and SKU,
    using bracket data fetched from the database.
    Returns the most profitable (lowest total cost) policy.
    """
    from app.models.vendor import Vendor
    from app.models.sku import SKU
    from app.models.bracket import Bracket

    # Fetch vendor and SKU from the database
    vendor = session.query(Vendor).filter_by(id=vendor_id).first()
    sku = session.query(SKU).filter_by(id=sku_id).first()
    if not vendor or not sku:
        return None

    # Fetch all brackets for this vendor (and location if applicable)
    brackets = session.query(Bracket).filter_by(vendor_id=vendor.id).order_by(Bracket.min_qty).all()
    if not brackets:
        # If no brackets, treat as a single bracket with no discount
        brackets = [Bracket(min_qty=0, max_qty=9999999, unit='units', discount_pct=0.0)]

    annual_demand = forecasted_daily_demand * 365
    results = []

    # Simulate all feasible cycles and brackets
    for cycle_days in range(7, max_cycle_days + 1):
        order_qty = forecasted_daily_demand * cycle_days
        num_orders = 365 / cycle_days if cycle_days > 0 else 0
        avg_inventory = order_qty / 2

        for bracket in brackets:
            if bracket.min_qty <= order_qty <= bracket.max_qty:
                # Apply bracket discount
                effective_unit_cost = unit_cost * (1 - bracket.discount_pct / 100.0)
                carrying_cost = avg_inventory * effective_unit_cost * annual_carrying_cost_rate / 365
                total_order_cost = num_orders * order_cost
                total_cost = carrying_cost + total_order_cost
                results.append({
                    'cycle_days': cycle_days,
                    'order_qty': order_qty,
                    'bracket': bracket,
                    'carrying_cost': carrying_cost,
                    'order_cost': total_order_cost,
                    'total_cost': total_cost,
                    'effective_unit_cost': effective_unit_cost
                })

    if not results:
        return None

    # Find the lowest total cost
    best = min(results, key=lambda x: x['total_cost'])
    return {
        'best_cycle_days': best['cycle_days'],
        'best_order_qty': best['order_qty'],
        'best_bracket': best['bracket'],
        'cost_breakdown': results
    }



def opa_simulation(
    forecasted_daily_demand: float,
    order_cost: float,
    annual_carrying_cost_rate: float,
    unit_cost: float,
    brackets: List[Bracket],
    max_cycle_days: int = 90
) -> Dict:
    """
    Simulates total cost for different order cycles and brackets, 
    recommends the most profitable (lowest total cost) policy.

    Returns:
        {
            'best_cycle_days': int,
            'best_order_qty': float,
            'best_bracket': Bracket,
            'cost_breakdown': List[Dict]  # For graphing or review
        }
    """
    results = []
    annual_demand = forecasted_daily_demand * 365

    for cycle_days in range(7, max_cycle_days + 1, 1):  # Test from 1 week up
        order_qty = forecasted_daily_demand * cycle_days
        num_orders = 365 / cycle_days if cycle_days > 0 else 0
        avg_inventory = order_qty / 2

        for bracket in brackets:
            if bracket.min_qty <= order_qty <= bracket.max_qty:
                # Apply bracket discount
                effective_unit_cost = unit_cost * (1 - bracket.discount_pct / 100.0)
                carrying_cost = avg_inventory * effective_unit_cost * annual_carrying_cost_rate / 365
                total_order_cost = num_orders * order_cost
                total_cost = carrying_cost + total_order_cost
                results.append({
                    'cycle_days': cycle_days,
                    'order_qty': order_qty,
                    'bracket': bracket,
                    'carrying_cost': carrying_cost,
                    'order_cost': total_order_cost,
                    'total_cost': total_cost,
                    'effective_unit_cost': effective_unit_cost
                })

    # Find the lowest total cost
    if not results:
        return {}

    best = min(results, key=lambda x: x['total_cost'])
    return {
        'best_cycle_days': best['cycle_days'],
        'best_order_qty': best['order_qty'],
        'best_bracket': best['bracket'],
        'cost_breakdown': results
    }

# Example usage
def example_opa():
    # Sample brackets (min_qty, max_qty, unit, discount_pct)
    brackets = [
        Bracket(0, 999, 'units', 0.0),
        Bracket(1000, 1999, 'units', 2.0),
        Bracket(2000, 99999, 'units', 5.0)
    ]
    result = opa_simulation(
        forecasted_daily_demand=100,
        order_cost=50,
        annual_carrying_cost_rate=0.20,
        unit_cost=10,
        brackets=brackets,
        max_cycle_days=60
    )
    print(f"Best order cycle: {result['best_cycle_days']} days")
    print(f"Best order quantity: {result['best_order_qty']}")
    print(f"Best bracket: {vars(result['best_bracket'])}")
    print(f"Total cost breakdown: {result['cost_breakdown'][:3]} ...")  # Show first 3 for brevity


# Example usage
def example_opa_db(session, vendor_id, sku_id):
    # Example: fetch forecast, order_cost, carrying_cost_rate, and unit_cost as needed
    forecasted_daily_demand = 100
    order_cost = 50
    annual_carrying_cost_rate = 0.20
    unit_cost = 10

    result = opa_simulation_db(
        session=session,
        vendor_id=vendor_id,
        sku_id=sku_id,
        forecasted_daily_demand=forecasted_daily_demand,
        order_cost=order_cost,
        annual_carrying_cost_rate=annual_carrying_cost_rate,
        unit_cost=unit_cost,
        max_cycle_days=60
    )
    if result:
        print(f"Best order cycle: {result['best_cycle_days']} days")
        print(f"Best order quantity: {result['best_order_qty']}")
        print(f"Best bracket: min_qty={result['best_bracket'].min_qty}, max_qty={result['best_bracket'].max_qty}, discount={result['best_bracket'].discount_pct}%")
    else:
        print("No feasible OPA solution found.")




if __name__ == "__main__":
    # Example usage of OPA simulation with hardcoded brackets
    example_opa()
    
    # Example usage of OPA simulation with database
    # Note: In a real application, you would get these from your database
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    
    # Create a database session (replace with your actual database URL)
    engine = create_engine('sqlite:///example.db')
    Session = sessionmaker(bind=engine)
    session = Session()
    
    # Example vendor and SKU IDs (replace with actual IDs from your database)
    vendor_id = 1
    sku_id = 1
    
    example_opa_db(session, vendor_id, sku_id)

