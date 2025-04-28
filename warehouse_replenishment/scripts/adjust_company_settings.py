#!/usr/bin/env python
# adjust_company_settings.py - Script to adjust company settings for different business scenarios

import sys
import os
from pathlib import Path
from typing import Dict, Any

# Add the parent directory to the path so we can import our modules
parent_dir = str(Path(__file__).parent.parent)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from warehouse_replenishment.db import session_scope
from warehouse_replenishment.models import Company
from warehouse_replenishment.logging_setup import get_logger

logger = get_logger('adjust_company_settings')

# Predefined business scenarios
BUSINESS_SCENARIOS = {
    'high_service': {
        'description': 'High service level with aggressive forward buying',
        'settings': {
            'service_level_goal': 98.0,  # Higher service level
            'forward_buy_maximum': 90,   # Longer forward buy window
            'forward_buy_filter': 15,    # More aggressive forward buying
            'discount_effect_rate': 150.0,  # More responsive to discounts
            'cost_of_lost_sales': 200.0,    # Higher cost of lost sales
            'total_carrying_rate': 35.0     # Slightly lower carrying cost
        }
    },
    'cost_focused': {
        'description': 'Cost-focused with minimal inventory',
        'settings': {
            'service_level_goal': 90.0,  # Lower service level
            'forward_buy_maximum': 30,   # Shorter forward buy window
            'forward_buy_filter': 45,    # Less aggressive forward buying
            'discount_effect_rate': 80.0,   # Less responsive to discounts
            'cost_of_lost_sales': 50.0,     # Lower cost of lost sales
            'total_carrying_rate': 45.0     # Higher carrying cost
        }
    },
    'seasonal': {
        'description': 'Seasonal business with high demand variability',
        'settings': {
            'service_level_goal': 95.0,  # Standard service level
            'forward_buy_maximum': 120,  # Very long forward buy window
            'forward_buy_filter': 20,    # Aggressive forward buying
            'discount_effect_rate': 120.0,  # Responsive to discounts
            'lumpy_demand_limit': 75.0,     # Higher tolerance for lumpy demand
            'slow_mover_limit': 15.0,       # Higher tolerance for slow movers
            'tracking_signal_limit': 65.0    # More tolerant tracking signal
        }
    },
    'fast_moving': {
        'description': 'Fast-moving consumer goods',
        'settings': {
            'service_level_goal': 97.0,  # High service level
            'forward_buy_maximum': 45,   # Moderate forward buy window
            'forward_buy_filter': 25,    # Moderate forward buying
            'discount_effect_rate': 110.0,  # Slightly responsive to discounts
            'slow_mover_limit': 5.0,        # Lower tolerance for slow movers
            'demand_filter_high': 4.0,      # Stricter demand filtering
            'demand_filter_low': 2.0,       # Stricter demand filtering
            'update_frequency_impact_control': 1  # More frequent updates
        }
    },
    'default': {
        'description': 'Default balanced settings',
        'settings': {
            'service_level_goal': 95.0,
            'forward_buy_maximum': 60,
            'forward_buy_filter': 30,
            'discount_effect_rate': 100.0,
            'cost_of_lost_sales': 100.0,
            'total_carrying_rate': 40.0,
            'lumpy_demand_limit': 50.0,
            'slow_mover_limit': 10.0,
            'demand_filter_high': 5.0,
            'demand_filter_low': 3.0,
            'tracking_signal_limit': 55.0,
            'update_frequency_impact_control': 2
        }
    }
}

def update_company_settings(scenario: str) -> bool:
    """Update company settings based on the specified scenario.
    
    Args:
        scenario: Name of the business scenario to apply
        
    Returns:
        True if settings were updated successfully
    """
    if scenario not in BUSINESS_SCENARIOS:
        logger.error(f"Unknown scenario: {scenario}")
        return False
        
    scenario_data = BUSINESS_SCENARIOS[scenario]
    logger.info(f"Applying {scenario} scenario: {scenario_data['description']}")
    
    try:
        with session_scope() as session:
            company = session.query(Company).first()
            if not company:
                logger.error("Company record not found")
                return False
                
            # Update settings
            for key, value in scenario_data['settings'].items():
                if hasattr(company, key):
                    setattr(company, key, value)
                    logger.info(f"Updated {key} to {value}")
                else:
                    logger.warning(f"Unknown setting: {key}")
                    
            session.commit()
            logger.info("Company settings updated successfully")
            return True
            
    except Exception as e:
        logger.error(f"Error updating company settings: {str(e)}")
        return False

def print_current_settings():
    """Print current company settings."""
    try:
        with session_scope() as session:
            company = session.query(Company).first()
            if not company:
                logger.error("Company record not found")
                return
                
            logger.info("Current Company Settings:")
            for key, value in company.__dict__.items():
                if not key.startswith('_'):
                    logger.info(f"{key}: {value}")
                    
    except Exception as e:
        logger.error(f"Error printing company settings: {str(e)}")

def main():
    """Main entry point."""
    if len(sys.argv) < 2:
        print("Usage: python adjust_company_settings.py <scenario>")
        print("\nAvailable scenarios:")
        for scenario, data in BUSINESS_SCENARIOS.items():
            print(f"\n{scenario}:")
            print(f"  Description: {data['description']}")
            print("  Settings:")
            for key, value in data['settings'].items():
                print(f"    {key}: {value}")
        return
        
    scenario = sys.argv[1]
    if scenario == 'list':
        print_current_settings()
    else:
        if update_company_settings(scenario):
            print(f"Successfully applied {scenario} scenario")
            print_current_settings()
        else:
            print(f"Failed to apply {scenario} scenario")

if __name__ == "__main__":
    main() 