import os
import logging
import logging.handlers
from pathlib import Path
import time
import traceback
from datetime import datetime
import sys

from warehouse_replenishment.config import config

class Logger:
    """Logging manager for the Warehouse Replenishment System."""
    
    _instance = None
    _loggers = {}
    
    def __new__(cls):
        """Singleton pattern implementation."""
        if cls._instance is None:
            cls._instance = super(Logger, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        """Initialize the logger if not already initialized."""
        if self._initialized:
            return
            
        self._log_config = config.log_config
        self._log_dir = Path(self._log_config['directory'])
        
        # Create log directory if it doesn't exist
        if not self._log_dir.exists():
            self._log_dir.mkdir(parents=True)
        
        # Set up global logging configuration
        self._configure_root_logger()
        
        # Application logger
        self._app_logger = self.get_logger('app')
        
        self._initialized = True
    
    def _configure_root_logger(self):
        """Configure the root logger."""
        root_logger = logging.getLogger()
        
        # Get log level from config
        level_name = self._log_config['level'].upper()
        level = getattr(logging, level_name, logging.INFO)
        root_logger.setLevel(level)
        
        # Remove existing handlers
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)
        
        # Add console handler if enabled
        if self._log_config['console_output']:
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(logging.Formatter(self._log_config['format']))
            root_logger.addHandler(console_handler)
    
    def get_logger(self, name):
        """Get a logger with the specified name.
        
        Args:
            name: Name of the logger
            
        Returns:
            Configured logger instance
        """
        if name in self._loggers:
            return self._loggers[name]
        
        # Create new logger
        logger = logging.getLogger(name)
        
        # Get log level from config
        level_name = self._log_config['level'].upper()
        level = getattr(logging, level_name, logging.INFO)
        logger.setLevel(level)
        
        # Remove existing handlers to prevent duplicates
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)
        
        # Create log file handler with rotation
        log_file = self._log_dir / f"{name}.log"
        file_handler = logging.handlers.RotatingFileHandler(
            log_file,
            maxBytes=self._log_config['max_size_mb'] * 1024 * 1024,
            backupCount=self._log_config['backup_count']
        )
        file_handler.setFormatter(logging.Formatter(self._log_config['format']))
        logger.addHandler(file_handler)
        
        # Add console handler if enabled
        if self._log_config['console_output']:
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(logging.Formatter(self._log_config['format']))
            logger.addHandler(console_handler)
        
        # Prevent propagation to root logger to avoid duplicates
        logger.propagate = False
        
        self._loggers[name] = logger
        return logger
    
    def log_exception(self, logger_name, exception, message=None):
        """Log an exception with stack trace.
        
        Args:
            logger_name: Logger name
            exception: Exception object
            message: Optional message to include
        """
        logger = self.get_logger(logger_name)
        
        if message:
            logger.error(f"{message}: {str(exception)}")
        else:
            logger.error(str(exception))
        
        logger.error(traceback.format_exc())
    
    @property
    def app_logger(self):
        """Get the application logger."""
        return self._app_logger
    
    def batch_start_log(self, process_name, additional_info=None):
        """Log the start of a batch process.
        
        Args:
            process_name: Name of the batch process
            additional_info: Optional additional information
            
        Returns:
            Dictionary with batch process logging information
        """
        batch_logger = self.get_logger('batch')
        start_time = datetime.now()
        
        log_info = {
            'process_name': process_name,
            'start_time': start_time,
            'additional_info': additional_info
        }
        
        batch_logger.info(f"Starting batch process: {process_name}")
        if additional_info:
            batch_logger.info(f"Process info: {additional_info}")
        
        return log_info
    
    def batch_end_log(self, log_info, success=True, result_info=None):
        """Log the end of a batch process.
        
        Args:
            log_info: Dictionary with batch process logging information
            success: Whether the batch process succeeded
            result_info: Optional result information
        """
        batch_logger = self.get_logger('batch')
        end_time = datetime.now()
        
        process_name = log_info.get('process_name', 'Unknown')
        start_time = log_info.get('start_time', datetime.now())
        duration = end_time - start_time
        
        if success:
            batch_logger.info(f"Completed batch process: {process_name}")
        else:
            batch_logger.error(f"Failed batch process: {process_name}")
        
        batch_logger.info(f"Process duration: {duration}")
        
        if result_info:
            batch_logger.info(f"Process results: {result_info}")
    
    def create_database_logger(self, db_session):
        """Create a logger that logs to the database.
        
        Args:
            db_session: Database session
            
        Returns:
            Logger that logs to the database
        """
        # This would be implemented if we need to log to the database
        pass

# Global logger instance
logger = Logger()

def get_logger(name):
    """Get a logger with the specified name."""
    return logger.get_logger(name)

def log_exception(logger_name, exception, message=None):
    """Log an exception with stack trace."""
    logger.log_exception(logger_name, exception, message)