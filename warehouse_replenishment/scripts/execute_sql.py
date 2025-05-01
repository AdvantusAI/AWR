import sys
from pathlib import Path

# Add the parent directory to the path so we can import our modules
parent_dir = str(Path(__file__).parent.parent)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from warehouse_replenishment.database import session_scope
from sqlalchemy import text

def execute_sql_file(file_path):
    """Execute SQL commands from a file."""
    with open(file_path, 'r') as f:
        sql_commands = f.read()
    
    with session_scope() as session:
        session.execute(text(sql_commands))
        session.commit()

if __name__ == "__main__":
    # Get the SQL file path
    sql_file = Path(__file__).parent / 'add_shelf_life_days.sql'
    execute_sql_file(sql_file) 