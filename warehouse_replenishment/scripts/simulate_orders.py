from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
import random
from datetime import datetime, timedelta
import configparser
import os
from pathlib import Path

# Get the project root directory
project_root = Path(__file__).parent.parent

# Read database configuration from settings.ini
config = configparser.ConfigParser()
config.read(project_root / 'config' / 'settings.ini')

# Get database connection details
db_config = config['DATABASE']
DATABASE_URL = f"postgresql://{db_config['username']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"

# Create database engine and session
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)
session = Session()

def simulate_orders():
    """Simulate more orders and lost sales by updating database values."""
    
    # 1. Update company settings to enable lost sales calculation
    session.execute(text("""
        UPDATE company 
        SET demand_from_days_out = 7,
            service_level_goal = 99.0
        WHERE id = 1
    """))
    
    # 2. Update items to generate more orders
    session.execute(text("""
        UPDATE item 
        SET item_order_point_units = item_order_point_units * 0.7,  -- Lower IOP by 30%
            demand_4weekly = demand_4weekly * 1.5,  -- Increase demand by 50%
            service_level_goal = 99.0,  -- Set high service level
            on_hand = CASE 
                WHEN random() < 0.3 THEN 0  -- 30% chance of being out of stock
                ELSE on_hand * 0.5  -- Otherwise reduce stock by 50%
            END
        WHERE buyer_class IN ('R', 'W')  -- Only active items (Regular or Watch)
    """))
    
    # 3. Add some out of stock days to demand history
    # First, get some items to simulate stockouts
    items = session.execute(text("""
        SELECT id FROM item 
        WHERE buyer_class IN ('R', 'W')  -- Only active items
        ORDER BY random() 
        LIMIT 20
    """)).fetchall()
    
    # Add out of stock days for these items
    current_date = datetime.now()
    current_year = current_date.year
    current_period = (current_date.month - 1) // 4 + 1  # Convert month to 4-weekly period (1-13)
    
    for item in items:
        # Add 2-5 days of stockouts in the last 30 days
        stockout_days = random.randint(2, 5)
        for _ in range(stockout_days):
            # Calculate period and year for the stockout
            stockout_date = current_date - timedelta(days=random.randint(1, 30))
            period_year = stockout_date.year
            period_number = (stockout_date.month - 1) // 4 + 1  # Convert month to 4-weekly period (1-13)
            
            # Use UPSERT to handle existing records
            session.execute(text("""
                INSERT INTO demand_history 
                (item_id, period_number, period_year, out_of_stock_days, total_demand)
                VALUES (:item_id, :period_number, :period_year, 1, 0)
                ON CONFLICT (item_id, period_number, period_year) 
                DO UPDATE SET 
                    out_of_stock_days = demand_history.out_of_stock_days + 1,
                    total_demand = demand_history.total_demand
            """), {
                'item_id': item[0],
                'period_number': period_number,
                'period_year': period_year
            })
    
    # 4. Update some items to have high lost sales
    session.execute(text("""
        UPDATE item 
        SET lost_sales = CASE 
            WHEN random() < 0.2 THEN demand_4weekly * 0.1  -- 20% chance of having lost sales
            ELSE 0
        END
        WHERE buyer_class IN ('R', 'W')  -- Only active items
    """))
    
    try:
        session.commit()
        print("Database updated successfully to simulate more orders and lost sales")
    except Exception as e:
        session.rollback()
        print(f"Error updating database: {str(e)}")
    finally:
        session.close()

if __name__ == "__main__":
    simulate_orders() 