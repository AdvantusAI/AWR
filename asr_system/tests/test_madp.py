"""
Unit tests for the MADP (Mean Absolute Deviation Percentage) calculation module.
"""
import unittest
from unittest.mock import MagicMock, patch
from datetime import datetime, timedelta
import sys
import os

# Add parent directory to path to import modules (adjust as needed)
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services.demand_forecast import (
    calculate_madp,
    calculate_tracking_signal,
    get_current_period,
    e3_regular_avs_reforecast,
    e3_enhanced_avs_reforecast
)

class TestMADPCalculation(unittest.TestCase):
    """Test cases for MADP calculation functions."""
    
    def test_calculate_madp_basic(self):
        """Test basic MADP calculation with simple values."""
        actuals = [100, 120, 90, 110]
        forecasts = [100, 100, 100, 100]
        
        # Expected MADP values: 
        # |100-100|/100*100 = 0
        # |120-100|/100*100 = 20
        # |90-100|/100*100 = 10
        # |110-100|/100*100 = 10
        # Average = (0 + 20 + 10 + 10) / 4 = 10
        
        expected_madp = 10.0
        calculated_madp = calculate_madp(actuals, forecasts)
        
        self.assertEqual(calculated_madp, expected_madp)
    
    def test_calculate_madp_empty_lists(self):
        """Test MADP calculation with empty lists."""
        self.assertEqual(calculate_madp([], []), 0.0)
    
    def test_calculate_madp_unequal_lists(self):
        """Test MADP calculation with unequal list lengths."""
        actuals = [100, 120, 90]
        forecasts = [100, 100]
        
        with self.assertRaises(ValueError):
            calculate_madp(actuals, forecasts)
    
    def test_calculate_madp_zero_forecasts(self):
        """Test MADP calculation with zero forecasts."""
        actuals = [100, 120, 90, 110]
        forecasts = [100, 0, 100, 100]
        
        # Expected MADP values: 
        # |100-100|/100*100 = 0
        # (skip due to zero forecast)
        # |90-100|/100*100 = 10
        # |110-100|/100*100 = 10
        # Average = (0 + 10 + 10) / 3 = 6.67
        
        expected_madp = 6.67
        calculated_madp = calculate_madp(actuals, forecasts)
        
        self.assertAlmostEqual(calculated_madp, expected_madp, places=2)
    
    def test_calculate_madp_all_zero_forecasts(self):
        """Test MADP calculation with all zero forecasts."""
        actuals = [100, 120, 90, 110]
        forecasts = [0, 0, 0, 0]
        
        self.assertEqual(calculate_madp(actuals, forecasts), 0.0)
    
    def test_calculate_madp_high_deviation(self):
        """Test MADP calculation with high deviation."""
        actuals = [50, 200, 20, 300]
        forecasts = [100, 100, 100, 100]
        
        # Expected MADP values:
        # |50-100|/100*100 = 50
        # |200-100|/100*100 = 100
        # |20-100|/100*100 = 80
        # |300-100|/100*100 = 200
        # Average = (50 + 100 + 80 + 200) / 4 = 107.5
        
        expected_madp = 107.5
        calculated_madp = calculate_madp(actuals, forecasts)
        
        self.assertEqual(calculated_madp, expected_madp)
    
    def test_calculate_tracking_signal_basic(self):
        """Test basic tracking signal calculation."""
        actuals = [110, 120, 90, 120]
        forecasts = [100, 100, 100, 100]
        
        # Error values: 
        # 110-100 = 10
        # 120-100 = 20
        # 90-100 = -10
        # 120-100 = 20
        # Sum of errors = 40
        # Absolute errors = [10, 20, 10, 20]
        # MAD = (10 + 20 + 10 + 20) / 4 = 15
        # Tracking signal = 40 / (15 * 4) = 0.67
        
        expected_track = 0.67
        calculated_track = calculate_tracking_signal(actuals, forecasts)
        
        self.assertAlmostEqual(calculated_track, expected_track, places=2)
    
    def test_calculate_tracking_signal_trending_down(self):
        """Test tracking signal calculation for downward trend."""
        actuals = [90, 80, 70, 60]
        forecasts = [100, 100, 100, 100]
        
        # Error values:
        # 90-100 = -10
        # 80-100 = -20
        # 70-100 = -30
        # 60-100 = -40
        # Sum of errors = -100
        # Absolute errors = [10, 20, 30, 40]
        # MAD = (10 + 20 + 30 + 40) / 4 = 25
        # Tracking signal = -100 / (25 * 4) = -1.0
        
        expected_track = -1.0
        calculated_track = calculate_tracking_signal(actuals, forecasts)
        
        self.assertAlmostEqual(calculated_track, expected_track, places=2)
    
    def test_calculate_tracking_signal_mixed_trend(self):
        """Test tracking signal with mixed trend (no clear trend)."""
        actuals = [110, 90, 110, 90]
        forecasts = [100, 100, 100, 100]
        
        # Error values:
        # 110-100 = 10
        # 90-100 = -10
        # 110-100 = 10
        # 90-100 = -10
        # Sum of errors = 0
        # Absolute errors = [10, 10, 10, 10]
        # MAD = (10 + 10 + 10 + 10) / 4 = 10
        # Tracking signal = 0 / (10 * 4) = 0.0
        
        expected_track = 0.0
        calculated_track = calculate_tracking_signal(actuals, forecasts)
        
        self.assertAlmostEqual(calculated_track, expected_track, places=2)

    @patch('services.demand_forecast.datetime')
    def test_get_current_period_4_weekly(self, mock_datetime):
        """Test calculating current period for 4-weekly periodicity."""
        # Test for day 29 of the year (January 29)
        mock_date = MagicMock()
        mock_date.timetuple.return_value.tm_yday = 29
        mock_date.month = 1
        mock_date.year = 2025
        mock_datetime.date.today.return_value = mock_date
        
        year, period = get_current_period(periodicity=13)
        
        # Day 29 should be in period 2 of 13 (4-weekly)
        self.assertEqual(year, 2025)
        self.assertEqual(period, 2)
    
    @patch('services.demand_forecast.datetime')
    def test_get_current_period_weekly(self, mock_datetime):
        """Test calculating current period for weekly periodicity."""
        # Set up a date in week 5 of 2025
        mock_date = MagicMock()
        mock_date.isocalendar.return_value = (2025, 5, 3)  # Year, week, day of week
        mock_date.month = 2
        mock_date.year = 2025
        mock_datetime.date.today.return_value = mock_date
        
        year, period = get_current_period(periodicity=52)
        
        # Should be period 5 (week 5)
        self.assertEqual(year, 2025)
        self.assertEqual(period, 5)
    
    @patch('services.demand_forecast.datetime')
    def test_e3_regular_avs_reforecast(self, mock_datetime):
        """Test E3 Regular AVS reforecasting calculation."""
        # Create mock session and data
        session = MagicMock()
        
        # Mock SKU and ForecastData
        mock_sku = MagicMock()
        mock_sku.sku_id = "SKU001"
        mock_sku.store_id = "STORE1"
        mock_sku.forecast_periodicity = 13
        mock_sku.freeze_forecast_until = None
        
        mock_forecast_data = MagicMock()
        mock_forecast_data.period_forecast = 100.0
        mock_forecast_data.weekly_forecast = 25.0
        mock_forecast_data.quarterly_forecast = 300.0
        mock_forecast_data.yearly_forecast = 1300.0
        
        # Setup session query returns
        session.query().filter().first.side_effect = [mock_sku, mock_forecast_data]
        
        # Mock history collection
        actuals = [120.0, 110.0, 90.0, 100.0]
        forecasts = [100.0, 100.0, 100.0, 100.0]
        periods_list = [(2025, 1), (2024, 13), (2024, 12), (2024, 11)]
        
        with patch('services.demand_forecast.collect_demand_history', 
                  return_value=(actuals, forecasts, periods_list)):
            with patch('services.demand_forecast.calculate_madp', return_value=15.0):
                with patch('services.demand_forecast.calculate_tracking_signal', return_value=0.3):
                    # Execute reforecasting
                    result = e3_regular_avs_reforecast(session, "SKU001", "STORE1")
                    
                    # Check the results
                    self.assertTrue(result['reforecast'])
                    self.assertEqual(result['madp'], 15.0)
                    self.assertEqual(result['track'], 0.3)
                    
                    # Check the track weight calculation
                    self.assertEqual(result['track_weight'], 0.3)
                    
                    # Check that forecast was updated using the formula:
                    # [Track × Most Recent Demand] + [(1-Track) × Old Forecast]
                    # [0.3 * 120] + [0.7 * 100] = 36 + 70 = 106
                    expected_new_forecast = 106.0
                    
                    # Verify the forecast was updated in the database
                    mock_forecast_data.period_forecast = expected_new_forecast
                    mock_forecast_data.weekly_forecast = expected_new_forecast / 4
                    mock_forecast_data.quarterly_forecast = expected_new_forecast * 3
                    mock_forecast_data.yearly_forecast = expected_new_forecast * 13
                    
                    session.commit.assert_called_once()

if __name__ == '__main__':
    unittest.main()