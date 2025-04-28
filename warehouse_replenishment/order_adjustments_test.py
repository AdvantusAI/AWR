#!/usr/bin/env python
# order_adjustments_test.py - Test script for order adjustments functionality

import unittest
from unittest.mock import patch, MagicMock
import sys
from pathlib import Path
from datetime import datetime

# Add parent directory to path for imports
parent_dir = str(Path(__file__).parent)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from order_adjustments import AdvancedOrderAdjustments
from warehouse_replenishment.exceptions import OrderError
from warehouse_replenishment.models import (
    Order, OrderItem, Vendor, Item, VendorBracket, Company, BuyerClassCode
)


class TestAdvancedOrderAdjustments(unittest.TestCase):
    """Test suite for AdvancedOrderAdjustments class."""
    
    def setUp(self):
        """Set up the test environment."""
        # Create mock session and services
        self.session = MagicMock()
        self.order_service = MagicMock()
        self.item_service = MagicMock()
        self.vendor_service = MagicMock()
        
        # Create mock company settings
        self.company = MagicMock()
        self.company.forward_buy_maximum = 60
        self.company.forward_buy_filter = 30
        
        # Configure session to return company
        self.session.query.return_value.first.return_value = self.company
        
        # Patch the initialization of services
        with patch('order_adjustments.OrderService', return_value=self.order_service), \
             patch('order_adjustments.ItemService', return_value=self.item_service), \
             patch('order_adjustments.VendorService', return_value=self.vendor_service):
            self.adjustments = AdvancedOrderAdjustments(self.session)
    
    def test_rebuild_order_order_not_found(self):
        """Test rebuilding a non-existent order."""
        self.order_service.get_order.return_value = None
        
        with self.assertRaises(OrderError):
            self.adjustments.rebuild_order(999)
        
        self.order_service.get_order.assert_called_once_with(999)
    
    def test_rebuild_order_invalid_status(self):
        """Test rebuilding an order with invalid status."""
        # Create mock order with ACCEPTED status
        mock_order = MagicMock()
        mock_order.status = 'ACCEPTED'
        mock_order.id = 123
        
        self.order_service.get_order.return_value = mock_order
        
        with self.assertRaises(OrderError):
            self.adjustments.rebuild_order(123)
        
        self.order_service.get_order.assert_called_once_with(123)
    
    def test_rebuild_order_no_items(self):
        """Test rebuilding an order with no items."""
        # Create mock order
        mock_order = MagicMock()
        mock_order.status = 'OPEN'
        mock_order.id = 123
        mock_order.vendor_id = 456
        
        # Create mock vendor
        mock_vendor = MagicMock()
        
        # Configure mocks
        self.order_service.get_order.return_value = mock_order
        self.session.query.return_value.get.return_value = mock_vendor
        self.order_service.get_order_items.return_value = []
        
        # Execute rebuild
        result = self.adjustments.rebuild_order(123)
        
        # Assert results
        self.assertFalse(result['success'])
        self.assertEqual("Order has no items to rebuild", result['message'])
        self.assertEqual(123, result['order_id'])
        
        # Verify calls
        self.order_service.get_order.assert_called_once_with(123)
        self.order_service.get_order_items.assert_called_once_with(123)
    
    def test_rebuild_order_success(self):
        """Test successful order rebuild."""
        # Create mock order
        mock_order = MagicMock()
        mock_order.status = 'OPEN'
        mock_order.id = 123
        mock_order.vendor_id = 456
        mock_order.independent_amount = 1000
        mock_order.independent_eaches = 100
        mock_order.current_bracket = 1
        
        # Create mock vendor
        mock_vendor = MagicMock()
        
        # Create mock item and order item
        mock_item = MagicMock()
        mock_item.id = 789
        mock_item.item_id = "I001"
        mock_item.on_hand = 10
        mock_item.on_order = 5
        mock_item.item_order_point_units = 20
        mock_item.order_up_to_level_units = 40
        mock_item.demand_4weekly = 28
        mock_item.buying_multiple = 1
        
        mock_order_item = MagicMock()
        mock_order_item.item_id = 789
        mock_order_item.soq_units = 20
        mock_order_item.is_frozen = False
        
        # Configure mocks
        self.order_service.get_order.side_effect = [mock_order, mock_order]  # First call and after update
        self.session.query.return_value.get.return_value = mock_item
        self.order_service.get_order_items.return_value = [mock_order_item]
        
        # Execute percentage adjustment (10% increase)
        result = self.adjustments.apply_percentage_adjustment(123, 10.0)
        
        # Assert results
        self.assertTrue(result['success'])
        self.assertEqual(1, result['items_adjusted'])
        self.assertEqual(1000, result['original_total'])
        
        # Verify calls
        self.order_service.update_item_soq.assert_called_once_with(123, 789, 22)  # 20 * 1.1 = 22
    
    def test_optimize_order_to_bracket(self):
        """Test optimizing order to reach a target bracket."""
        # Create mock order
        mock_order = MagicMock()
        mock_order.status = 'OPEN'
        mock_order.id = 123
        mock_order.vendor_id = 456
        mock_order.independent_amount = 800
        mock_order.current_bracket = 1
        
        # Create mock brackets
        mock_bracket1 = MagicMock()
        mock_bracket1.bracket_number = 1
        mock_bracket1.minimum = 500
        mock_bracket1.maximum = 999
        
        mock_bracket2 = MagicMock()
        mock_bracket2.bracket_number = 2
        mock_bracket2.minimum = 1000
        mock_bracket2.maximum = 1999
        
        # Create mock item and order item
        mock_item = MagicMock()
        mock_item.id = 789
        mock_item.item_id = "I001"
        mock_item.on_hand = 10
        mock_item.on_order = 5
        mock_item.order_up_to_level_units = 40
        mock_item.demand_4weekly = 28
        mock_item.buying_multiple = 1
        mock_item.purchase_price = 10
        
        mock_order_item = MagicMock()
        mock_order_item.item_id = 789
        mock_order_item.soq_units = 20
        mock_order_item.is_frozen = False
        
        # Configure mocks
        self.order_service.get_order.side_effect = [mock_order, mock_order]  # First call and after update
        self.session.query.return_value.filter.return_value.order_by.return_value.all.return_value = [mock_bracket1, mock_bracket2]
        self.order_service.get_order_items.return_value = [mock_order_item]
        self.session.query.return_value.get.return_value = mock_item
        
        # Execute bracket optimization
        result = self.adjustments.optimize_order_to_bracket(123, 2)
        
        # Assert results
        self.assertEqual(123, result['order_id'])
        self.assertEqual(1, result['current_bracket'])
        self.assertEqual(2, result['target_bracket'])
        self.assertEqual(800, result['current_amount'])
        self.assertEqual(1000, result['target_amount'])
        self.assertEqual(200, result['amount_needed'])
        
        # Verify calls to add 20 more units ($200 value) to reach target
        self.order_service.update_item_soq.assert_called_once()
    
    def test_apply_forward_buy(self):
        """Test applying forward buy to order items."""
        # Create mock order
        mock_order = MagicMock()
        mock_order.status = 'OPEN'
        mock_order.id = 123
        mock_order.independent_amount = 1000
        
        # Create mock item and order item
        mock_item = MagicMock()
        mock_item.id = 789
        mock_item.item_id = "I001"
        mock_item.on_hand = 10
        mock_item.on_order = 5
        mock_item.demand_4weekly = 28  # 1 unit per day
        mock_item.buying_multiple = 1
        mock_item.purchase_price = 10
        mock_item.vendor_id = 456
        
        mock_order_item = MagicMock()
        mock_order_item.item_id = 789
        mock_order_item.soq_units = 20  # 20 days of supply
        mock_order_item.is_frozen = False
        
        # Create mock vendor
        mock_vendor = MagicMock()
        
        # Configure mocks
        self.order_service.get_order.side_effect = [mock_order, mock_order]  # First call and after update
        self.session.query.return_value.get.side_effect = [mock_item, mock_vendor]
        self.order_service.get_order_items.return_value = [mock_order_item]
        
        # Execute forward buy with 40 days
        result = self.adjustments.apply_forward_buy(123, 40)
        
        # Assert results
        self.assertTrue(result['success'])
        self.assertEqual(1, result['items_processed'])
        self.assertEqual(1, result['items_forward_bought'])
        self.assertEqual(40, result['forward_buy_days'])
        
        # Should add 20 more days to reach 40 days total
        self.order_service.update_item_soq.assert_called_once()
    
    def test_forward_buy_already_exceeds_max(self):
        """Test forward buy when item already exceeds maximum days."""
        # Create mock order
        mock_order = MagicMock()
        mock_order.status = 'OPEN'
        mock_order.id = 123
        
        # Create mock item and order item
        mock_item = MagicMock()
        mock_item.id = 789
        mock_item.item_id = "I001"
        mock_item.demand_4weekly = 28  # 1 unit per day
        mock_item.vendor_id = 456
        
        mock_order_item = MagicMock()
        mock_order_item.item_id = 789
        mock_order_item.soq_units = 50  # Already 50 days of supply
        mock_order_item.is_frozen = False
        
        # Create mock vendor
        mock_vendor = MagicMock()
        
        # Configure mocks
        self.order_service.get_order.return_value = mock_order
        self.session.query.return_value.get.side_effect = [mock_item, mock_vendor]
        self.order_service.get_order_items.return_value = [mock_order_item]
        
        # Execute forward buy with 40 days (less than current 50)
        result = self.adjustments.apply_forward_buy(123, 40)
        
        # Assert results
        self.assertTrue(result['success'])
        self.assertEqual(1, result['items_processed'])
        self.assertEqual(0, result['items_forward_bought'])  # No items should be forward bought
        
        # Should not update any items
        self.order_service.update_item_soq.assert_not_called()


if __name__ == '__main__':
    unittest.main()_item.soq_units = 15
        mock_order_item.is_frozen = False
        mock_order_item.is_manual = False
        
        # Configure mocks
        self.order_service.get_order.return_value = mock_order
        self.session.query.return_value.get.side_effect = [mock_vendor, mock_item]
        self.order_service.get_order_items.return_value = [mock_order_item]
        
        # Configure calculate_suggested_order_quantity mock
        self.order_service.calculate_suggested_order_quantity.return_value = {
            "soq_units": 25,
            "is_order_point": True,
            "is_orderable": True
        }
        
        # Configure updated order after rebuild
        updated_order = MagicMock()
        updated_order.independent_amount = 1200
        updated_order.independent_eaches = 110
        updated_order.current_bracket = 1
        
        # Mock get_order to return updated order on second call
        self.order_service.get_order.side_effect = [mock_order, updated_order]
        
        # Execute rebuild
        result = self.adjustments.rebuild_order(123)
        
        # Assert results
        self.assertTrue(result['success'])
        self.assertIn("Order rebuilt successfully", result['message'])
        self.assertEqual(123, result['order_id'])
        self.assertEqual(1, result['items']['increased'])
        self.assertEqual(0, result['items']['unchanged'])
        self.assertEqual(0, result['items']['decreased'])
        
        # Verify calls
        self.order_service.calculate_suggested_order_quantity.assert_called_once_with(
            item_id=789, force_recalculation=True
        )
        self.order_service.update_item_soq.assert_called_once_with(123, 789, 25)
    
    def test_apply_percentage_adjustment(self):
        """Test applying percentage adjustment to order items."""
        # Create mock order
        mock_order = MagicMock()
        mock_order.status = 'OPEN'
        mock_order.id = 123
        mock_order.independent_amount = 1000
        
        # Create mock item and order item
        mock_item = MagicMock()
        mock_item.id = 789
        mock_item.item_id = "I001"
        mock_item.buying_multiple = 1
        mock_item.minimum_quantity = 1
        
        mock_order_item = MagicMock()
        mock_order