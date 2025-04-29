"""
Tests for the Due Order service.
"""
import unittest
from unittest.mock import patch, MagicMock, PropertyMock
import datetime
import pytest
from sqlalchemy.orm import Session

from services.due_order import (
    calculate_urgency_score,
    get_prioritized_due_orders,
    get_order_risk_metrics,
    identify_critical_due_orders,
    get_due_order_summary_metrics,
    update_due_order_status,
    check_fixed_order_schedule,
    check_service_level_risk,
    check_minimum_met
)
from models.order import Order, OrderLine, OrderStatus, OrderCategory
from models.sku import SKU, StockStatus, ForecastData
from models.source import Source, SourceBracket
from utils.helpers import calculate_available_balance


class TestDueOrderServices(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures."""
        self.session_mock = MagicMock(spec=Session)
        
        # Mock config settings
        self.config_patcher = patch('services.due_order.ASR_CONFIG', {
            'order_point_prime_limit': 95,
            'due_order_risk_threshold': 20.0
        })
        self.mock_config = self.config_patcher.start()

    def tearDown(self):
        """Tear down test fixtures."""
        self.config_patcher.stop()

    def test_calculate_urgency_score(self):
        """Test urgency score calculation."""
        # Mock order and related data
        mock_order = MagicMock(spec=Order)
        mock_order.id = 1
        mock_order.final_adjust_amount = 5000.0
        
        # Mock SKU with stock status and forecast data
        mock_sku = MagicMock(spec=SKU)
        mock_sku.sku_id = "SKU123"
        mock_sku.store_id = "STORE1"
        mock_sku.service_level_goal = 95.0
        mock_sku.attained_service_level = 92.0
        mock_sku.lead_time_forecast = 7
        
        # Mock stock status
        mock_stock_status = MagicMock(spec=StockStatus)
        mock_stock_status.on_hand = 100
        mock_stock_status.on_order = 50
        mock_stock_status.customer_back_order = 0
        mock_stock_status.reserved = 0
        mock_stock_status.quantity_held = 0
        
        # Set stock status as property of SKU
        type(mock_sku).stock_status = PropertyMock(return_value=mock_stock_status)
        
        # Mock forecast data
        mock_forecast = MagicMock(spec=ForecastData)
        mock_forecast.weekly_forecast = 70  # 10 per day
        
        # Mock order line
        mock_line = MagicMock(spec=OrderLine)
        mock_line.sku_id = mock_sku.id
        mock_line.item_delay = 5  # Safety stock days
        
        # Set up session query mocks
        self.session_mock.query.return_value.filter.return_value.first.side_effect = [
            mock_order,  # First query for order
            mock_sku,    # Query for SKU
            mock_forecast  # Query for forecast data
        ]
        
        self.session_mock.query.return_value.filter.return_value.all.return_value = [mock_line]
        
        # Run the function
        score = calculate_urgency_score(self.session_mock, 1)
        
        # Assertions
        self.assertGreaterEqual(score, 0.0)
        self.assertLessEqual(score, 100.0)
        self.session_mock.query.assert_called()

    def test_get_prioritized_due_orders(self):
        """Test getting prioritized due orders."""
        # Mock orders
        mock_order1 = MagicMock(spec=Order)
        mock_order1.id = 1
        mock_order1.source_id = 101
        mock_order1.store_id = "STORE1"
        mock_order1.order_date = datetime.datetime.now()
        mock_order1.expected_delivery_date = datetime.datetime.now() + datetime.timedelta(days=7)
        mock_order1.final_adjust_amount = 5000.0
        mock_order1.status = OrderStatus.DUE
        mock_order1.category = OrderCategory.DUE
        
        mock_order2 = MagicMock(spec=Order)
        mock_order2.id = 2
        mock_order2.source_id = 102
        mock_order2.store_id = "STORE1"
        mock_order2.order_date = datetime.datetime.now()
        mock_order2.expected_delivery_date = datetime.datetime.now() + datetime.timedelta(days=7)
        mock_order2.final_adjust_amount = 3000.0
        mock_order2.status = OrderStatus.DUE
        mock_order2.category = OrderCategory.DUE
        
        # Mock sources
        mock_source1 = MagicMock(spec=Source)
        mock_source1.id = 101
        mock_source1.name = "Source A"
        
        mock_source2 = MagicMock(spec=Source)
        mock_source2.id = 102
        mock_source2.name = "Source B"
        
        # Set source as property of order
        type(mock_order1).source = PropertyMock(return_value=mock_source1)
        type(mock_order2).source = PropertyMock(return_value=mock_source2)
        
        # Set up session query mocks
        self.session_mock.query.return_value.filter.return_value.all.return_value = [mock_order1, mock_order2]
        
        # Mock the calculate_urgency_score and get_order_risk_metrics functions
        with patch('services.due_order.calculate_urgency_score') as mock_calc_urgency, \
             patch('services.due_order.get_order_risk_metrics') as mock_get_metrics:
                
            # Configure mocks to return different urgency scores
            mock_calc_urgency.side_effect = [80.0, 65.0]  # Order 1 is more urgent
            
            # Configure metrics return values
            mock_get_metrics.side_effect = [
                {
                    'order_point_count': 5,
                    'high_service_at_risk': 3,
                    'service_gap': 2.5,
                    'days_to_stockout': 1.5
                },
                {
                    'order_point_count': 3,
                    'high_service_at_risk': 1,
                    'service_gap': 1.5,
                    'days_to_stockout': 3.0
                }
            ]
            
            # Run the function
            results = get_prioritized_due_orders(self.session_mock)
            
            # Assertions
            self.assertEqual(len(results), 2)
            self.assertEqual(results[0]['order_id'], 1)  # First result should be the more urgent order
            self.assertEqual(results[0]['urgency_score'], 80.0)
            self.assertEqual(results[0]['is_critical'], True)  # Should be critical with score >= 75
            self.assertEqual(results[1]['order_id'], 2)
    
    def test_get_order_risk_metrics(self):
        """Test order risk metrics calculation."""
        # Mock order and related data
        mock_order = MagicMock(spec=Order)
        mock_order.id = 1
        
        # Mock SKUs with different risk profiles
        mock_sku1 = MagicMock(spec=SKU)
        mock_sku1.id = 101
        mock_sku1.sku_id = "SKU101"
        mock_sku1.store_id = "STORE1"
        mock_sku1.service_level_goal = 98.0  # High service
        mock_sku1.attained_service_level = 95.0
        mock_sku1.lead_time_forecast = 5
        
        mock_sku2 = MagicMock(spec=SKU)
        mock_sku2.id = 102
        mock_sku2.sku_id = "SKU102"
        mock_sku2.store_id = "STORE1"
        mock_sku2.service_level_goal = 90.0  # Standard service
        mock_sku2.attained_service_level = 92.0  # Meeting goal
        mock_sku2.lead_time_forecast = 7
        
        # Mock stock statuses
        mock_status1 = MagicMock(spec=StockStatus)
        mock_status1.on_hand = 10  # Low stock
        mock_status1.on_order = 0
        mock_status1.customer_back_order = 0
        mock_status1.reserved = 0
        mock_status1.quantity_held = 0
        
        mock_status2 = MagicMock(spec=StockStatus)
        mock_status2.on_hand = 100  # Good stock level
        mock_status2.on_order = 50
        mock_status2.customer_back_order = 0
        mock_status2.reserved = 0
        mock_status2.quantity_held = 0
        
        # Set stock status as property
        type(mock_sku1).stock_status = PropertyMock(return_value=mock_status1)
        type(mock_sku2).stock_status = PropertyMock(return_value=mock_status2)
        
        # Mock forecast data
        mock_forecast1 = MagicMock(spec=ForecastData)
        mock_forecast1.weekly_forecast = 21  # 3 per day = 3 days of stock
        
        mock_forecast2 = MagicMock(spec=ForecastData)
        mock_forecast2.weekly_forecast = 35  # 5 per day = 20 days of stock
        
        # Mock order lines
        mock_line1 = MagicMock(spec=OrderLine)
        mock_line1.order_id = 1
        mock_line1.sku_id = 101
        mock_line1.item_delay = 4  # Safety stock days
        
        mock_line2 = MagicMock(spec=OrderLine)
        mock_line2.order_id = 1
        mock_line2.sku_id = 102
        mock_line2.item_delay = 3  # Safety stock days
        
        # Set up session query mocks
        self.session_mock.query.return_value.filter.return_value.first.side_effect = [
            mock_order,     # First query for order
            mock_sku1,      # First SKU query
            mock_forecast1, # First forecast query
            mock_sku2,      # Second SKU query
            mock_forecast2  # Second forecast query
        ]
        
        self.session_mock.query.return_value.filter.return_value.all.return_value = [mock_line1, mock_line2]
        
        # Run the function
        metrics = get_order_risk_metrics(self.session_mock, 1)
        
        # Assertions
        self.assertIsInstance(metrics, dict)
        self.assertIn('order_point_count', metrics)
        self.assertIn('high_service_at_risk', metrics)
        self.assertIn('service_gap', metrics)
        self.assertIn('days_to_stockout', metrics)
    
    def test_identify_critical_due_orders(self):
        """Test identification of critical due orders."""
        # Mock the get_prioritized_due_orders function
        with patch('services.due_order.get_prioritized_due_orders') as mock_get_orders:
            # Configure mock to return a mix of critical and non-critical orders
            mock_get_orders.return_value = [
                {
                    'order_id': 1,
                    'urgency_score': 85.0,
                    'is_critical': True,
                    'high_service_at_risk': 4,
                    'avg_days_to_stockout': 0.5
                },
                {
                    'order_id': 2,
                    'urgency_score': 60.0,
                    'is_critical': False,
                    'high_service_at_risk': 1,
                    'avg_days_to_stockout': 5.0
                },
                {
                    'order_id': 3,
                    'urgency_score': 78.0,
                    'is_critical': True,
                    'high_service_at_risk': 2,
                    'avg_days_to_stockout': 1.2
                }
            ]
            
            # Run the function
            critical_orders = identify_critical_due_orders(self.session_mock)
            
            # Assertions
            self.assertEqual(len(critical_orders), 2)  # Should find 2 critical orders
            self.assertEqual(critical_orders[0]['order_id'], 1)
            self.assertEqual(critical_orders[1]['order_id'], 3)
    
    def test_get_due_order_summary_metrics(self):
        """Test getting summary metrics for due orders."""
        # Mock the get_prioritized_due_orders function
        with patch('services.due_order.get_prioritized_due_orders') as mock_get_orders:
            # Configure mock to return a set of orders with various metrics
            mock_get_orders.return_value = [
                {
                    'order_id': 1,
                    'urgency_score': 85.0,
                    'is_critical': True,
                    'high_service_at_risk': 4,
                    'avg_days_to_stockout': 0.5,
                    'order_value': 5000.0
                },
                {
                    'order_id': 2,
                    'urgency_score': 60.0,
                    'is_critical': False,
                    'high_service_at_risk': 1,
                    'avg_days_to_stockout': 2.5,
                    'order_value': 3000.0
                },
                {
                    'order_id': 3,
                    'urgency_score': 78.0,
                    'is_critical': True,
                    'high_service_at_risk': 2,
                    'avg_days_to_stockout': 1.2,
                    'order_value': 7000.0
                },
                {
                    'order_id': 4,
                    'urgency_score': 45.0,
                    'is_critical': False,
                    'high_service_at_risk': 0,
                    'avg_days_to_stockout': 8.0,
                    'order_value': 2000.0
                }
            ]
            
            # Run the function
            summary = get_due_order_summary_metrics(self.session_mock)
            
            # Assertions
            self.assertEqual(summary['total_due_orders'], 4)
            self.assertEqual(summary['critical_orders'], 2)
            self.assertEqual(summary['total_order_value'], 17000.0)
            self.assertEqual(summary['high_service_skus_at_risk'], 7)
            self.assertEqual(summary['urgency_breakdown']['high'], 2)
            self.assertEqual(summary['urgency_breakdown']['medium'], 1)
            self.assertEqual(summary['urgency_breakdown']['low'], 1)
            self.assertEqual(summary['stockout_risk_breakdown']['immediate'], 1)
            self.assertEqual(summary['stockout_risk_breakdown']['short_term'], 2)
    
    def test_update_due_order_status(self):
        """Test updating due order status."""
        # Mock orders
        mock_order1 = MagicMock(spec=Order)
        mock_order1.id = 1
        mock_order1.status = OrderStatus.PLANNED
        mock_order1.category = OrderCategory.PLANNED
        
        mock_order2 = MagicMock(spec=Order)
        mock_order2.id = 2
        mock_order2.status = OrderStatus.PLANNED
        mock_order2.category = OrderCategory.PLANNED
        
        # Set up session query mocks
        self.session_mock.query.return_value.filter.return_value.all.return_value = [mock_order1, mock_order2]
        
        # Mock the order status check functions
        with patch('services.due_order.check_fixed_order_schedule') as mock_check_schedule, \
             patch('services.due_order.check_service_level_risk') as mock_check_service, \
             patch('services.due_order.check_minimum_met') as mock_check_minimum:
                
            # Configure mocks to return different results for each order
            mock_check_schedule.side_effect = [True, False]  # Order 1 is due by schedule
            mock_check_service.side_effect = [False, True]   # Order 2 is due by service risk
            mock_check_minimum.side_effect = [False, False]  # No orders due by minimum
            
            # Run the function
            stats = update_due_order_status(self.session_mock)
            
            # Assertions
            self.assertEqual(stats['orders_processed'], 2)
            self.assertEqual(stats['orders_marked_due'], 2)
            self.assertEqual(stats['orders_due_to_fixed_schedule'], 1)
            self.assertEqual(stats['orders_due_to_service_risk'], 1)
            self.assertEqual(stats['orders_due_to_minimum_met'], 0)
            
            # Verify order status updates
            self.assertEqual(mock_order1.status, OrderStatus.DUE)
            self.assertEqual(mock_order1.category, OrderCategory.DUE)
            self.assertEqual(mock_order2.status, OrderStatus.DUE)
            self.assertEqual(mock_order2.category, OrderCategory.DUE)
            
            # Verify session commit was called
            self.session_mock.commit.assert_called_once()
    
    def test_check_fixed_order_schedule(self):
        """Test checking fixed order schedule."""
        # Mock order with source
        mock_order = MagicMock(spec=Order)
        
        # Mock source with various schedule settings
        mock_source1 = MagicMock(spec=Source)
        mock_source1.order_days_in_week = "135"  # Mon, Wed, Fri
        mock_source1.order_week = 0  # Every week
        mock_source1.order_day_in_month = None
        mock_source1.next_order_date = None
        
        mock_source2 = MagicMock(spec=Source)
        mock_source2.order_days_in_week = None
        mock_source2.order_week = None
        mock_source2.order_day_in_month = 15  # 15th of month
        mock_source2.next_order_date = None
        
        mock_source3 = MagicMock(spec=Source)
        mock_source3.order_days_in_week = None
        mock_source3.order_week = None
        mock_source3.order_day_in_month = None
        mock_source3.next_order_date = datetime.datetime.now() - datetime.timedelta(days=1)  # Yesterday
        
        # Test weekly schedule
        type(mock_order).source = PropertyMock(return_value=mock_source1)
        
        # Mock today as Monday (1)
        with patch('datetime.datetime') as mock_datetime:
            mock_datetime.now.return_value = datetime.datetime(2025, 4, 28)  # Monday
            self.assertTrue(check_fixed_order_schedule(self.session_mock, mock_order))
        
        # Mock today as Tuesday (2)
        with patch('datetime.datetime') as mock_datetime:
            mock_datetime.now.return_value = datetime.datetime(2025, 4, 29)  # Tuesday
            self.assertFalse(check_fixed_order_schedule(self.session_mock, mock_order))
        
        # Test monthly schedule
        type(mock_order).source = PropertyMock(return_value=mock_source2)
        
        # Mock today as 15th
        with patch('datetime.datetime') as mock_datetime:
            mock_datetime.now.return_value = datetime.datetime(2025, 4, 15)
            self.assertTrue(check_fixed_order_schedule(self.session_mock, mock_order))
        
        # Mock today as 16th
        with patch('datetime.datetime') as mock_datetime:
            mock_datetime.now.return_value = datetime.datetime(2025, 4, 16)
            self.assertFalse(check_fixed_order_schedule(self.session_mock, mock_order))
        
        # Test next order date
        type(mock_order).source = PropertyMock(return_value=mock_source3)
        
        # Mock today
        with patch('datetime.datetime') as mock_datetime:
            mock_datetime.now.return_value = datetime.datetime(2025, 4, 29)
            self.assertTrue(check_fixed_order_schedule(self.session_mock, mock_order))
    
    def test_check_service_level_risk(self):
        """Test checking service level risk."""
        # Mock order with source
        mock_order = MagicMock(spec=Order)
        mock_order.store_id = "STORE1"
        
        # Mock source
        mock_source = MagicMock(spec=Source)
        mock_source.id = 101
        
        type(mock_order).source = PropertyMock(return_value=mock_source)
        
        # Mock SKUs with different risk profiles
        mock_sku1 = MagicMock(spec=SKU)
        mock_sku1.sku_id = "SKU101"
        mock_sku1.store_id = "STORE1"
        mock_sku1.source_id = 101
        mock_sku1.buyer_class = "R"
        
        mock_sku2 = MagicMock(spec=SKU)
        mock_sku2.sku_id = "SKU102"
        mock_sku2.store_id = "STORE1"
        mock_sku2.source_id = 101
        mock_sku2.buyer_class = "R"
        
        # Mock stock statuses
        mock_status1 = MagicMock(spec=StockStatus)
        mock_status1.on_hand = 10
        mock_status1.on_order = 0
        mock_status1.customer_back_order = 0
        mock_status1.reserved = 0
        mock_status1.quantity_held = 0
        
        mock_status2 = MagicMock(spec=StockStatus)
        mock_status2.on_hand = 100
        mock_status2.on_order = 50
        mock_status2.customer_back_order = 0
        mock_status2.reserved = 0
        mock_status2.quantity_held = 0
        
        type(mock_sku1).stock_status = PropertyMock(return_value=mock_status1)
        type(mock_sku2).stock_status = PropertyMock(return_value=mock_status2)
        
        # Set up session query mocks
        self.session_mock.query.return_value.filter.return_value.all.return_value = [mock_sku1, mock_sku2]
        
        # Mock the calculate_vendor_order_point function
        with patch('services.due_order.calculate_vendor_order_point') as mock_calc_vop:
            # Setup different VOP scenarios
            mock_calc_vop.side_effect = [
                {'units': 15},  # SKU1 is below VOP (10 < 15)
                {'units': 120}  # SKU2 is above VOP (150 > 120)
            ]
            
            # Run the function - 50% of SKUs at risk
            result = check_service_level_risk(self.session_mock, mock_order)
            
            # Assertions - should be True with 50% at risk (above 20% threshold)
            self.assertTrue(result)
            
            # Change second SKU to be at risk too
            mock_calc_vop.side_effect = [
                {'units': 15},   # SKU1 is below VOP (10 < 15)
                {'units': 200}   # SKU2 is below VOP (150 < 200)
            ]
            
            # Run again - now 100% of SKUs at risk
            result = check_service_level_risk(self.session_mock, mock_order)
            
            # Assertions - should still be True
            self.assertTrue(result)
            
            # Change both SKUs to not be at risk
            mock_calc_vop.side_effect = [
                {'units': 5},    # SKU1 is above VOP (10 > 5)
                {'units': 120}   # SKU2 is above VOP (150 > 120)
            ]
            
            # Run again - now 0% of SKUs at risk
            result = check_service_level_risk(self.session_mock, mock_order)
            
            # Assertions - should be False
            self.assertFalse(result)
    
    def test_check_minimum_met(self):
        """Test checking if minimum order value is met."""
        # Mock order
        mock_order = MagicMock(spec=Order)
        mock_order.auto_adjust_amount = 5000.0
        
        # Mock source with different bracket settings
        mock_source1 = MagicMock(spec=Source)
        mock_source1.order_when_minimum_met = True
        mock_source1.current_bracket = 2
        
        mock_source2 = MagicMock(spec=Source)
        mock_source2.order_when_minimum_met = False
        mock_source2.current_bracket = 2
        
        mock_source3 = MagicMock(spec=Source)
        mock_source3.order_when_minimum_met = True
        mock_source3.current_bracket = 0  # No current bracket
        
        # Mock brackets
        mock_bracket = MagicMock(spec=SourceBracket)
        mock_bracket.bracket_number = 2
        mock_bracket.minimum = 4000.0
        
        # Test source with order_when_minimum_met and valid bracket
        type(mock_order).source = PropertyMock(return_value=mock_source1)
        type(mock_source1).brackets = PropertyMock(return_value=[mock_bracket])
        
        # Order meets minimum
        self.assertTrue(check_minimum_met(self.session_mock, mock_order))
        
        # Order doesn't meet minimum
        mock_order.auto_adjust_amount = 3000.0
        self.assertFalse(check_minimum_met(self.session_mock, mock_order))
        
        # Test source with order_when_minimum_met disabled
        type(mock_order).source = PropertyMock(return_value=mock_source2)
        type(mock_source2).brackets = PropertyMock(return_value=[mock_bracket])
        
        mock_order.auto_adjust_amount = 5000.0  # Reset to meeting minimum
        self.assertFalse(check_minimum_met(self.session_mock, mock_order))
        
        # Test source with no current bracket
        type(mock_order).source = PropertyMock(return_value=mock_source3)
        self.assertFalse(check_minimum_met(self.session_mock, mock_order))


if __name__ == '__main__':
    unittest.main()