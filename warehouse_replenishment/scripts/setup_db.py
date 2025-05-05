# warehouse_replenishment/scripts/setup_db.py
import argparse
import sys
from pathlib import Path

# Add the parent directory to the path so we can import our modules
sys.path.append(str(Path(__file__).parent.parent))

from models import Base
from db import (
    db, 
    initialize, 
    get_db_type, 
    create_all_tables, 
    drop_all_tables,
    session_scope
)
from config import config
from logging_setup import get_logger

logger = get_logger('db_setup')

def setup_database(drop_existing=False):
    """Set up the database schema.
    
    Args:
        drop_existing: If True, drop existing tables before creating new ones
        
    Returns:
        True if setup was successful, False otherwise
    """
    try:
        # Initialize database connection
        initialize()
        db_type = get_db_type()
        
        logger.info(f"Database type: {db_type}")
        
        if db_type == "postgresql":
            if drop_existing:
                logger.info("Dropping all existing tables...")
                drop_all_tables()
                logger.info("All tables dropped successfully.")
            
            logger.info("Creating database tables...")
            create_all_tables()
            logger.info("Database tables created successfully.")
            
            # Initialize with default configuration
            initialize_default_configuration()
            
        else:  # Supabase
            logger.info("Using Supabase. Tables must be created via SQL migrations.")
            logger.info("Please run your Supabase migration scripts.")
            
            # Still initialize default configuration
            initialize_default_configuration()
        
        return True
    except Exception as e:
        logger.error(f"Error setting up database: {str(e)}")
        logger.exception(e)
        return False

def initialize_default_configuration():
    """Initialize the database with default configuration."""
    from warehouse_replenishment.models import Company
    from warehouse_replenishment.db import database_adapter
    
    logger.info("Initializing default configuration...")
    
    try:
        # Try to get existing company record
        company = database_adapter.get_by_id(Company, 1)
        
        if not company:
            logger.info("Creating default company record...")
            default_company = Company(
                name="Default Company",
                basic_alpha_factor=10.0,
                demand_from_days_out=1,
                lumpy_demand_limit=50.0,
                slow_mover_limit=10.0,
                demand_filter_high=5.0,
                demand_filter_low=3.0,
                tracking_signal_limit=55.0,
                op_prime_limit_pct=95.0,
                forecast_demand_limit=5.0,
                update_frequency_impact_control=2,
                service_level_goal=config.get_float('BUSINESS_RULES', 'default_service_level', 95.0),
                borrowing_rate=5.0,
                capital_cost_rate=25.0,
                physical_carrying_cost=15.0,
                other_rate=0.0,
                total_carrying_rate=40.0,
                gross_margin=35.0,
                overhead_rate=25.0,
                cost_of_lost_sales=100.0,
                order_header_cost=25.0,
                order_line_cost=1.0,
                forward_buy_maximum=60,
                forward_buy_filter=30,
                discount_effect_rate=100.0,
                advertising_effect_rate=100.0,
                keep_old_tb_parms_days=30,
                keep_archived_exceptions_days=90,
                lead_time_forecast_control=1,
                history_periodicity_default=13,
                forecasting_periodicity_default=13
            )
            
            # Save using database adapter (works for both PostgreSQL and Supabase)
            database_adapter.save(default_company)
            logger.info("Default company record created.")
        else:
            logger.info("Company record already exists. Skipping initialization.")
        
    except Exception as e:
        logger.error(f"Error initializing default configuration: {str(e)}")
        logger.exception(e)

def run_supabase_migration_file(file_path):
    """Run a Supabase migration SQL file (for PostgreSQL mode test)."""
    if get_db_type() == "postgresql":
        with session_scope() as session:
            with open(file_path, 'r') as f:
                sql = f.read()
                session.execute(sql)
        logger.info(f"Migration file {file_path} executed successfully.")
    else:
        logger.info("Cannot run SQL migration file directly for Supabase.")
        logger.info("Please run these migrations through the Supabase dashboard or CLI.")

def main():
    """Main function for database setup script."""
    parser = argparse.ArgumentParser(description='Set up the Warehouse Replenishment database.')
    parser.add_argument('--drop', action='store_true', help='Drop existing tables before creating new ones')
    parser.add_argument('--migration', type=str, help='Run a SQL migration file (PostgreSQL only)')
    parser.add_argument('--test-connection', action='store_true', help='Test database connection')
    
    args = parser.parse_args()
    
    if args.test_connection:
        try:
            initialize()
            db_type = get_db_type()
            logger.info(f"Successfully connected to {db_type} database!")
            
            if db_type == "postgresql":
                with session_scope() as session:
                    session.execute("SELECT 1")
                    logger.info("PostgreSQL connection test passed.")
            else:
                client = db.get_supabase()
                result = client.table('company').select('id').limit(1).execute()
                logger.info(f"Supabase connection test passed. Tables available.")
            
            return
        except Exception as e:
            logger.error(f"Connection test failed: {str(e)}")
            sys.exit(1)
    
    if args.migration:
        run_supabase_migration_file(args.migration)
        return
    
    logger.info("Starting database setup...")
    
    if setup_database(args.drop):
        logger.info("Database setup completed successfully.")
    else:
        logger.error("Database setup failed.")
        sys.exit(1)

if __name__ == '__main__':
    main()