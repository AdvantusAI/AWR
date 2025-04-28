"""
Configuration settings for the ASR system.
"""
import os
from sqlalchemy.engine import URL

# Database configuration
DB_CONFIG = {
    'drivername': 'postgresql',
    'username': os.environ.get('DB_USERNAME', 'postgres'),
    'password': os.environ.get('DB_PASSWORD', 'Admin0606'),
    'host': os.environ.get('DB_HOST', 'localhost'),
    'port': os.environ.get('DB_PORT', '5433'),
    'database': os.environ.get('DB_NAME', 'warehouse_replenishment'),
}
     
        
# Connection string for SQLAlchemy
DB_URL = URL.create(**DB_CONFIG)

# ASR system configuration
ASR_CONFIG = {
    # Forecasting settings
    'forecasting_periodicity': 13,  # 13 periods of 4 weeks (default)
    'madp_high_threshold': 50,      # MADP above this is considered high
    'track_high_threshold': 50,     # Track above this is considered high
    
    # Safety stock settings
    'default_service_level': 95,    # Default service level goal (%)
    'order_point_prime_limit': 95,  # Order point 'A' threshold (%)
    
    # Lead time settings
    'default_lead_time': 7,         # Default lead time in days
    'default_lead_time_variance': 10, # Default lead time variance (%)
    
    # Order policy settings
    'default_header_cost': 25.0,    # Default header cost for OPA
    'default_line_cost': 1.0,       # Default line cost for OPA
    'carrying_cost_rate': 0.40,     # 40% annual carrying cost
    
    # Order building settings
    'max_order_cycle': 90,          # Maximum order cycle in days
    'max_forward_buy_days': 180,    # Maximum forward buy in days
}

# Exception thresholds
EXCEPTION_THRESHOLDS = {
    'demand_filter_high': 3.0,      # Standard deviations above forecast
    'demand_filter_low': 3.0,       # Standard deviations below forecast
    'tracking_signal_limit': 0.55,  # Tracking signal threshold
}