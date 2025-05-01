import os
import configparser
import json
from pathlib import Path

class Config:
    """Configuration manager for the Warehouse Replenishment System."""
    
    _instance = None
    
    def __new__(cls):
        """Singleton pattern implementation."""
        if cls._instance is None:
            cls._instance = super(Config, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        """Initialize the configuration if not already initialized."""
        if self._initialized:
            return
            
        self._config_dir = Path('config')
        self._config_path = self._config_dir / 'settings.ini'
        self._config = configparser.ConfigParser(interpolation=None)
        
        # Create config directory if it doesn't exist
        if not self._config_dir.exists():
            self._config_dir.mkdir(parents=True)
        
        # Load config or create default
        if self._config_path.exists():
            self._config.read(self._config_path)
        else:
            self._create_default_config()
            
        self._initialized = True
    
    def _create_default_config(self):
        """Create default configuration file."""
        self._config['DATABASE'] = {
            'engine': 'postgresql',
            'host': 'localhost',
            'port': '5432',
            'database': 'm8_aws',
            'username': 'postgres',
            'password': 'postgres',
            'echo': 'False'
        }
        #M8Platform2@25
        
        self._config['LOGGING'] = {
            'level': 'INFO',
            'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            'directory': 'logs',
            'max_size_mb': '10',
            'backup_count': '5',
            'console_output': 'True'
        }
        
        self._config['BATCH_PROCESS'] = {
            'nightly_start_time': '00:00',
            'weekly_update_day': '1',  # Monday
            'period_end_day': '28',   # 28th of month for monthly processing
            'max_workers': '4',
            'timeout_minutes': '60'
        }
        
        self._config['BUSINESS_RULES'] = {
            'default_service_level': '95.0',
            'default_lead_time': '7',
            'default_lead_time_variance': '20.0',
            'history_periods_to_keep': '52',
            'calculate_lost_sales': 'True'
        }
        
        self._save_config()
    
    def _save_config(self):
        """Save configuration to file."""
        with open(self._config_path, 'w') as configfile:
            self._config.write(configfile)
    
    def get(self, section, key, default=None):
        """Get configuration value."""
        try:
            return self._config.get(section, key)
        except (configparser.NoSectionError, configparser.NoOptionError):
            return default
    
    def get_int(self, section, key, default=None):
        """Get configuration value as integer."""
        try:
            return self._config.getint(section, key)
        except (configparser.NoSectionError, configparser.NoOptionError, ValueError):
            return default
    
    def get_float(self, section, key, default=None):
        """Get configuration value as float."""
        try:
            return self._config.getfloat(section, key)
        except (configparser.NoSectionError, configparser.NoOptionError, ValueError):
            return default
    
    def get_boolean(self, section, key, default=None):
        """Get configuration value as boolean."""
        try:
            return self._config.getboolean(section, key)
        except (configparser.NoSectionError, configparser.NoOptionError, ValueError):
            return default
    
    def set(self, section, key, value):
        """Set configuration value."""
        if not self._config.has_section(section):
            self._config.add_section(section)
        
        self._config.set(section, key, str(value))
        self._save_config()
    
    def get_db_url(self):
        """Generate SQLAlchemy database URL."""
        engine = self.get('DATABASE', 'engine', 'postgresql')
        username = self.get('DATABASE', 'username', 'postgres')
        password = self.get('DATABASE', 'password', 'Admin0606')
        host = self.get('DATABASE', 'host', 'localhost')
        port = self.get('DATABASE', 'port', '5433')
        database = self.get('DATABASE', 'database', 'm8_aws')
        
        return f"{engine}://{username}:{password}@{host}:{port}/{database}"
    
    @property
    def log_config(self):
        """Get logging configuration."""
        return {
            'level': self.get('LOGGING', 'level', 'INFO'),
            'format': self.get('LOGGING', 'format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s'),
            'directory': self.get('LOGGING', 'directory', 'logs'),
            'max_size_mb': self.get_int('LOGGING', 'max_size_mb', 10),
            'backup_count': self.get_int('LOGGING', 'backup_count', 5),
            'console_output': self.get_boolean('LOGGING', 'console_output', True)
        }
    
    @property
    def batch_config(self):
        """Get batch processing configuration."""
        return {
            'nightly_start_time': self.get('BATCH_PROCESS', 'nightly_start_time', '00:00'),
            'weekly_update_day': self.get_int('BATCH_PROCESS', 'weekly_update_day', 1),
            'period_end_day': self.get_int('BATCH_PROCESS', 'period_end_day', 28),
            'max_workers': self.get_int('BATCH_PROCESS', 'max_workers', 4),
            'timeout_minutes': self.get_int('BATCH_PROCESS', 'timeout_minutes', 60)
        }
    
    @property
    def business_rules(self):
        """Get business rules configuration."""
        return {
            'default_service_level': self.get_float('BUSINESS_RULES', 'default_service_level', 95.0),
            'default_lead_time': self.get_int('BUSINESS_RULES', 'default_lead_time', 7),
            'default_lead_time_variance': self.get_float('BUSINESS_RULES', 'default_lead_time_variance', 20.0),
            'history_periods_to_keep': self.get_int('BUSINESS_RULES', 'history_periods_to_keep', 52),
            'calculate_lost_sales': self.get_boolean('BUSINESS_RULES', 'calculate_lost_sales', True)
        }

# Global config instance
config = Config()