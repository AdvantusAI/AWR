"""
Unit tests for the Lead Time Forecasting service.
"""
import unittest
from unittest.mock import MagicMock, patch
from datetime import datetime, timedelta
import sys
import os

# Add parent directory to path to import modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services.lead_time import (
    calculate_actual_lead_time,
    get_receipt_history,
    filter_special_receipts,
    calculate_lead_time_stats,
    detect_lead_time_trend,
    detect_lead_time_seasonality,
    forecast_lead_time,
    forecast_source_lead_time,
    run_lead_time_forecasting
)

class TestLeadTimeService(unittest.TestCase):
    """Test cases for Lead Time Forecasting service."""
    
    def setUp(self):
        """Set up test fixtures."""
        # Create mock session
        self.session = MagicMock()
        
        # Create mock SKU
        self.sku = MagicMock()
        self.sku.sku_id = "SKU001"
        self.sku.store_id = "STORE1"
        self.sku.source_id = 1
        self.sku.lead_time_forecast = 7.0
        self.sku.lead_time_variance = 10.0
        
        # Create mock source
        self.source = MagicMock()
        self.source.id = 1
        self.source.source_id = "SOURCE001"
        self.source.lead_time_forecast = 7.0
        self.source.lead_time_quoted = 6.0
        self.source.lead_time_variance = 10.0
        
        # Link SKU to source
        self.sku.source = self.source
        
        # Create mock orders/receipts
        self.orders = []
        base_date = datetime(2023, 1, 1)
        
        # Create 10 orders with varying lead times
        for i in range(10):
            order = MagicMock()
            order.id = i
            order.order_date = base_date + timedelta(days=i*10)
            order.receipt_date = order.order_date + timedelta(days=7 + i % 3)  # Lead times of 7, 8, 9 days
            order.expected_delivery_date = order.order_date + timedelta(days=7)
            order.is_expedited = False
            order.is_delayed = False
            self.orders.append(order)
        
        # Configure session query mocks
        self.session.query().filter().first.side_effect = [
            self.sku,  # First call returns SKU
            self.source  # Second call returns source
        ]
        
        self.session.query().filter().all.return_value = self.orders
    
    def test_calculate_actual_lead_time(self):
        """Test calculation of actual lead time."""
        # Test normal case
        order_date = datetime(2023, 1, 1)
        receipt_date = datetime(2023, 1, 8)
        lead_time = calculate_actual_lead_time(order_date, receipt_date)
        self.assertEqual(lead_time, 7)
        
        # Test missing dates
        lead_time_none = calculate_actual_lead_time(None, receipt_date)
        self.assertIsNone(lead_time_none)
        
        lead_time_none2 = calculate_actual_lead_time(order_date, None)
        self.assertIsNone(lead_time_none2)
    
    @patch('services.lead_time.logger')
    def test_get_receipt_history(self, mock_logger):
        """Test getting receipt history."""
        # Configure session mock
        self.session.query().filter().all.return_value = self.orders
        
        # Test basic functionality
        receipts = get_receipt_history(self.session, store_id="STORE1")
        self.assertEqual(len(receipts), 10)
        
        # Test with exception
        self.session.query.side_effect = Exception("Database error")
        receipts_error = get_receipt_history(self.session, store_id="STORE1")
        self.assertEqual(receipts_error, [])
        mock_logger.error.assert_called()
    
    def test_filter_special_receipts(self):
        """Test filtering special receipts."""
        # Test normal filtering
        filtered_receipts = filter_special_receipts(self.orders)
        self.assertEqual(len(filtered_receipts), 10)  # All receipts are normal
        
        # Make some orders expedited or delayed
        self.orders[0].receipt_date = self.orders[0].order_date + timedelta(days=3)  # Expedited
        self.orders[1].receipt_date = self.orders[1].order_date + timedelta(days=15)  # Delayed
        
        # Test with exclude_expedited=True, exclude_delayed=True
        filtered_receipts = filter_special_receipts(self.orders)
        self.assertEqual(len(filtered_receipts), 8)  # 2 should be filtered out
        
        # Test with exclude_expedited=False
        filtered_receipts = filter_special_receipts(self.orders, exclude_expedited=False)
        self.assertEqual(len(filtered_receipts), 9)  # Only delayed should be filtered
        
        # Test with exclude_delayed=False
        filtered_receipts = filter_special_receipts(self.orders, exclude_delayed=False)
        self.assertEqual(len(filtered_receipts), 9)  # Only expedited should be filtered
        
        # Test with both False
        filtered_receipts = filter_special_receipts(self.orders, exclude_expedited=False, exclude_delayed=False)
        self.assertEqual(len(filtered_receipts), 10)  # None should be filtered
    
    def test_calculate_lead_time_stats(self):
        """Test calculation of lead time statistics."""
        # Test normal case
        stats = calculate_lead_time_stats(self.orders)
        self.assertIsNotNone(stats)
        self.assertIn('mean', stats)
        self.assertIn('median', stats)
        self.assertIn('min', stats)
        self.assertIn('max', stats)
        self.assertIn('std_dev', stats)
        self.assertIn('variance', stats)
        self.assertIn('trend', stats)
        self.assertIn('count', stats)
        
        # Test empty list
        stats_empty = calculate_lead_time_stats([])
        self.assertIsNone(stats_empty)
        
        # Test with negative lead times (should be excluded)
        bad_order = MagicMock()
        bad_order.order_date = datetime(2023, 1, 10)
        bad_order.receipt_date = datetime(2023, 1, 5)  # Negative lead time
        
        stats_with_bad = calculate_lead_time_stats([bad_order])
        self.assertIsNone(stats_with_bad)  # Should return None as no valid lead times
    
    def test_detect_lead_time_trend(self):
        """Test detection of lead time trend."""
        # Test with no trend
        mock_stats = {
            'mean': 7.0,
            'trend': 0.1  # Small trend
        }
        trend_info = detect_lead_time_trend(mock_stats, trend_threshold=0.1)
        self.assertFalse(trend_info['has_trend'])
        
        # Test with significant trend
        mock_stats = {
            'mean': 7.0,
            'trend': 1.0  # Significant trend
        }
        trend_info = detect_lead_time_trend(mock_stats, trend_threshold=0.1)
        self.assertTrue(trend_info['has_trend'])
        self.assertEqual(trend_info['direction'], 'increasing')
        
        # Test with negative trend
        mock_stats = {
            'mean': 7.0,
            'trend': -1.0  # Negative trend
        }
        trend_info = detect_lead_time_trend(mock_stats, trend_threshold=0.1)
        self.assertTrue(trend_info['has_trend'])
        self.assertEqual(trend_info['direction'], 'decreasing')
        
        # Test with None stats
        trend_info = detect_lead_time_trend(None)
        self.assertFalse(trend_info['has_trend'])
    
    @patch('services.lead_time.get_receipt_history')
    @patch('services.lead_time.filter_special_receipts')
    def test_detect_lead_time_seasonality(self, mock_filter, mock_get_receipts):
        """Test detection of lead time seasonality."""
        # Configure mocks
        receipts = []
        base_date = datetime(2022, 1, 1)
        
        # Create receipts with seasonal pattern (shorter lead times in summer)
        for month in range(1, 13):
            for _ in range(5):  # 5 orders per month
                order = MagicMock()
                order.order_date = datetime(2022, month, 15)
                
                # Shorter lead times in summer months
                lead_time = 10 if month in [6, 7, 8] else 14
                order.receipt_date = order.order_date + timedelta(days=lead_time)
                
                receipts.append(order)
        
        mock_get_receipts.return_value = receipts
        mock_filter.return_value = receipts
        
        # Test seasonality detection
        seasonality = detect_lead_time_seasonality(self.session, "SOURCE001")
        self.assertTrue(seasonality['has_seasonality'])
        self.assertIn('monthly_indices', seasonality)
        
        # Verify seasonal pattern (summer months should have lower indices)
        monthly_indices = seasonality['monthly_indices']
        self.assertLess(monthly_indices[6], monthly_indices[1])  # June should be lower than January
        self.assertLess(monthly_indices[7], monthly_indices[1])  # July should be lower than January
        self.assertLess(monthly_indices[8], monthly_indices[1])  # August should be lower than January
    
    @patch('services.lead_time.calculate_lead_time_stats')
    @patch('services.lead_time.detect_lead_time_trend')
    def test_forecast_lead_time(self, mock_detect_trend, mock_calc_stats):
        """Test forecasting lead time for a SKU."""
        # Configure mocks
        mock_calc_stats.return_value = {
            'median': 8.0,
            'variance_pct': 12.0,
            'count': 10
        }
        
        mock_detect_trend.return_value = {
            'has_trend': True,
            'trend_value': 0.5,
            'direction': 'increasing'
        }
        
        # Test normal case
        forecast = forecast_lead_time(self.session, "SKU001", "STORE1")
        
        self.assertIsNotNone(forecast)
        self.assertEqual(forecast['lead_time_forecast'], 8)  # Rounded from 8.25 (8.0 + 0.5/2)
        self.assertEqual(forecast['lead_time_variance'], 12.0)
        self.assertEqual(forecast['data_source'], 'receipt_history')
        
        # Test with no stats (should use source default)
        mock_calc_stats.return_value = None
        forecast_default = forecast_lead_time(self.session, "SKU001", "STORE1")
        
        self.assertIsNotNone(forecast_default)
        self.assertEqual(forecast_default['lead_time_forecast'], 7.0)  # From source
        self.assertEqual(forecast_default['lead_time_variance'], 10.0)  # From source
        self.assertEqual(forecast_default['data_source'], 'source_default')
    
    @patch('services.lead_time.calculate_lead_time_stats')
    @patch('services.lead_time.detect_lead_time_trend')
    @patch('services.lead_time.detect_lead_time_seasonality')
    def test_forecast_source_lead_time(self, mock_detect_season, mock_detect_trend, mock_calc_stats):
        """Test forecasting lead time for a source."""
        # Configure mocks
        mock_calc_stats.return_value = {
            'median': 8.0,
            'variance_pct': 12.0,
            'count': 30
        }
        
        mock_detect_trend.return_value = {
            'has_trend': True,
            'trend_value': 0.5,
            'direction': 'increasing'
        }
        
        mock_detect_season.return_value = {
            'has_seasonality': True,
            'monthly_indices': {1: 1.1, 6: 0.9}
        }
        
        # Test normal case
        forecast = forecast_source_lead_time(self.session, "SOURCE001")
        
        self.assertIsNotNone(forecast)
        self.assertEqual(forecast['lead_time_forecast'], 8)  # Rounded from 8.25
        self.assertEqual(forecast['lead_time_variance'], 12.0)
        self.assertEqual(forecast['data_source'], 'receipt_history')
        self.assertTrue(forecast['has_seasonality'])
        
        # Test with no stats (should use existing source values)
        mock_calc_stats.return_value = None
        forecast_default = forecast_source_lead_time(self.session, "SOURCE001")
        
        self.assertIsNotNone(forecast_default)
        self.assertEqual(forecast_default['lead_time_forecast'], 7.0)  # From source
        self.assertEqual(forecast_default['lead_time_variance'], 10.0)  # From source
        self.assertEqual(forecast_default['data_source'], 'existing_source_values')
    
    @patch('services.lead_time.forecast_source_lead_time')
    @patch('services.lead_time.apply_source_lead_time_forecast')
    @patch('services.lead_time.forecast_lead_time')
    @patch('services.lead_time.apply_lead_time_forecast')
    def test_run_lead_time_forecasting(self, mock_apply_sku, mock_forecast_sku, 
                                       mock_apply_source, mock_forecast_source):
        """Test running lead time forecasting for all sources and SKUs."""
        # Configure mocks
        source1 = MagicMock()
        source1.source_id = "SOURCE001"
        source2 = MagicMock()
        source2.source_id = "SOURCE002"
        
        self.session.query().all.side_effect = [
            [source1, source2],  # Sources
            [self.sku]  # SKUs
        ]
        
        mock_forecast_source.return_value = {
            'data_source': 'receipt_history',
            'lead_time_forecast': 8,
            'lead_time_variance': 12.0
        }
        
        mock_apply_source.return_value = True
        
        mock_forecast_sku.return_value = {
            'data_source': 'receipt_history',
            'lead_time_forecast': 9,
            'lead_time_variance': 15.0
        }
        
        mock_apply_sku.return_value = True
        
        # Test running forecasting
        stats = run_lead_time_forecasting(self.session)
        
        self.assertEqual(stats['sources_processed'], 2)
        self.assertEqual(stats['source_updates'], 2)
        self.assertEqual(stats['skus_processed'], 1)
        self.assertEqual(stats['sku_updates'], 1)
        self.assertEqual(stats['errors'], 0)
        
        # Test with source errors
        mock_forecast_source.side_effect = Exception("Source error")
        
        stats_with_errors = run_lead_time_forecasting(self.session)
        self.assertEqual(stats_with_errors['errors'], 2)

if __name__ == '__main__':
    unittest.main()