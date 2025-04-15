import argparse
import sys
from pathlib import Path

from config import config
from db import db
from logging_setup import logger

def init_application():
    """Initialize application components."""
    # Initialize database
    db.initialize()
    
    # Log application start
    log = logger.app_logger
    log.info("Warehouse Replenishment System initialized")
    log.info(f"Using database: {config.get('DATABASE', 'engine')} at {config.get('DATABASE', 'host')}:{config.get('DATABASE', 'port')}")
    log.info(f"Database name: {config.get('DATABASE', 'database')}")
    
    return True

def main():
    """Main application entry point."""
    parser = argparse.ArgumentParser(description='Warehouse Replenishment System')
    
    # Add command-line arguments
    parser.add_argument('--setup-db', action='store_true', 
                      help='Set up the database schema')
    parser.add_argument('--drop-db', action='store_true',
                      help='Drop existing database tables before setup')
    
    args = parser.parse_args()
    
    if args.setup_db:
        from scripts.setup_db import setup_database
        setup_database(args.drop_db)
        return
    
    # Normal application initialization
    init_application()
    
    # Here we would typically launch other processes or start a scheduler
    # for the night job, but for now just log that we're ready
    logger.app_logger.info("Warehouse Replenishment System ready")

if __name__ == "__main__":
    main()