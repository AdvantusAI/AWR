from typing import Dict, List, Optional, Union
import sys
import os
from pathlib import Path

# Add the parent directory to the path so we can import our modules
parent_dir = str(Path(__file__).parent.parent.parent)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from warehouse_replenishment.models import Item, Vendor, Order
from warehouse_replenishment.exceptions import ValidationError

def validate_item(item: Item) -> Dict[str, str]:
    """Validate an item.
    
    Args:
        item: Item to validate
        
    Returns:
        Dictionary with validation errors
    """
    errors = {}
    
    if not item.item_id:
        errors['item_id'] = 'Item ID is required'
    
    if not item.vendor_id:
        errors['vendor_id'] = 'Vendor ID is required'
    
    if not item.warehouse_id:
        errors['warehouse_id'] = 'Warehouse ID is required'
    
    return errors

def validate_vendor(vendor: Vendor) -> Dict[str, str]:
    """Validate a vendor.
    
    Args:
        vendor: Vendor to validate
        
    Returns:
        Dictionary with validation errors
    """
    errors = {}
    
    if not vendor.vendor_id:
        errors['vendor_id'] = 'Vendor ID is required'
    
    if not vendor.name:
        errors['name'] = 'Vendor name is required'
    
    if not vendor.warehouse_id:
        errors['warehouse_id'] = 'Warehouse ID is required'
    
    return errors

def validate_order(order: Order) -> Dict[str, str]:
    """Validate an order.
    
    Args:
        order: Order to validate
        
    Returns:
        Dictionary with validation errors
    """
    errors = {}
    
    if not order.vendor_id:
        errors['vendor_id'] = 'Vendor ID is required'
    
    if not order.warehouse_id:
        errors['warehouse_id'] = 'Warehouse ID is required'
    
    return errors
