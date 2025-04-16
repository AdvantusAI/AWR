#!/usr/bin/env python
# create_db_tables.py - Script to create the AWR database tables

import sys
import os
import logging
import argparse
from pathlib import Path

# Add the parent directory to the path so we can import our modules
parent_dir = str(Path(__file__).parent.parent)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from warehouse_replenishment.config import config
from warehouse_replenishment.db import db
from warehouse_replenishment.models import Base
from warehouse_replenishment.logging_setup import get_logger

def create_tables(drop_existing=False):
    """Create database tables.
    
    Args:
        drop_existing: If True, drop existing tables before creating new ones
        
    Returns:
        True if tables were created successfully
    """
    logger = get_logger('create_tables')
    logger.info("Initializing database connection...")
    
    try:
        # Initialize database connection
        db.initialize()
        
        if drop_existing:
            logger.info("Dropping existing tables...")
            Base.metadata.drop_all(db.engine)
            logger.info("Existing tables dropped successfully.")
        
        logger.info("Creating database tables...")
        Base.metadata.create_all(db.engine)
        logger.info("Database tables created successfully.")
        
        return True
    
    except Exception as e:
        logger.error(f"Error creating database tables: {str(e)}")
        logger.exception(e)
        return False

def main():
    """Create database tables."""
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Create AWR database tables')
    parser.add_argument('--drop', '-d', action='store_true', help='Drop existing tables before creating new ones')
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')
    
    args = parser.parse_args()
    
    # Set up logging
    log_dir = Path(parent_dir) / 'logs'
    log_dir.mkdir(exist_ok=True)  # Create logs directory if it doesn't exist
    
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_dir / 'create_tables.log'),
            logging.StreamHandler()
        ]
    )
    
    logger = get_logger('create_tables_runner')
    
    logger.info("Starting database table creation...")
    logger.info(f"Drop existing tables: {args.drop}")
    
    try:
        # Create tables
        success = create_tables(args.drop)
        
        if success:
            logger.info("Database tables created successfully.")
            return 0
        else:
            logger.error("Failed to create database tables.")
            return 1
    
    except Exception as e:
        logger.exception(f"Error creating database tables: {str(e)}")
        return 1

if __name__ == "__main__":
    sys.exit(main())