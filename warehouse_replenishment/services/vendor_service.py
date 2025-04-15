# warehouse_replenishment/services/vendor_service.py
from typing import List, Dict, Optional, Union
import sys
import os
from pathlib import Path

# Add the parent directory to the path so we can import our modules
parent_dir = str(Path(__file__).parent.parent.parent)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from warehouse_replenishment.models import (
    Vendor, Item, Company, VendorBracket, SuperVendorMember, SubVendorItem
)
from warehouse_replenishment.exceptions import VendorError
from sqlalchemy.orm import Session

class VendorService:
    """Service for handling vendor-related operations."""
    
    def __init__(self, session: Session):
        """Initialize the vendor service.
        
        Args:
            session: Database session
        """
        self.session = session
    
    def get_vendor(self, vendor_id: int) -> Optional[Vendor]:
        """Get a vendor by ID.
        
        Args:
            vendor_id: Vendor ID
            
        Returns:
            Vendor object or None if not found
        """
        return self.session.query(Vendor).filter(Vendor.id == vendor_id).first()
    
    def get_all_vendors(self) -> List[Vendor]:
        """Get all vendors.
        
        Returns:
            List of vendor objects
        """
        return self.session.query(Vendor).all()
    
    def get_vendor_items(self, vendor_id: int) -> List[Item]:
        """Get all items for a vendor.
        
        Args:
            vendor_id: Vendor ID
            
        Returns:
            List of item objects
        """
        return self.session.query(Item).filter(Item.vendor_id == vendor_id).all()
    
    def get_vendor_brackets(self, vendor_id: int) -> List[VendorBracket]:
        """Get all price brackets for a vendor.
        
        Args:
            vendor_id: Vendor ID
            
        Returns:
            List of vendor bracket objects
        """
        return self.session.query(VendorBracket).filter(VendorBracket.vendor_id == vendor_id).all()