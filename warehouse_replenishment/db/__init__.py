# warehouse_replenishment/db/__init__.py
import sys
import os
from contextlib import contextmanager
from pathlib import Path

from .connection import DatabaseConnection, db, session_scope, get_engine, get_session
from .interface import DatabaseAdapter

# Add parent directories to path for imports
parent_dir = str(Path(__file__).parent.parent.parent)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from warehouse_replenishment.exceptions import DatabaseError

# Get the global database adapter
database_adapter = DatabaseAdapter(db)

# Initialize database based on configuration
def initialize():
    """Initialize database connection and create tables if needed."""
    try:
        if db.db_type == "postgresql":
            from warehouse_replenishment.models import Base
            with db.session_scope() as session:
                # Test connection
                session.execute("SELECT 1")
                # Create tables if they don't exist
                Base.metadata.create_all(bind=db.engine)
        else:
            # Test Supabase connection
            client = db.get_supabase()
            # Test with a simple query
            result = client.table('company').select('*').limit(1).execute()
            
            # Note: Supabase requires tables to be created through SQL migrations
            # rather than programmatically like SQLAlchemy. You'll need to run
            # the appropriate SQL scripts to create tables in Supabase.
            
    except Exception as e:
        raise DatabaseError(f"Database initialization failed: {str(e)}")

def get_db_type() -> str:
    """Get current database type."""
    return db.db_type

def get_db_interface():
    """Get database interface for current database type."""
    return database_adapter.interface

# Create convenience functions for backward compatibility
def create_all_tables():
    """Create all tables (PostgreSQL only)."""
    if db.db_type != "postgresql":
        raise DatabaseError("create_all_tables is only available for PostgreSQL")
    
    from warehouse_replenishment.models import Base
    Base.metadata.create_all(bind=db.engine)

def drop_all_tables():
    """Drop all tables (PostgreSQL only)."""
    if db.db_type != "postgresql":
        raise DatabaseError("drop_all_tables is only available for PostgreSQL")
    
    from warehouse_replenishment.models import Base
    Base.metadata.drop_all(bind=db.engine)

# Export all public functions and classes
__all__ = [
    'db',
    'initialize',
    'session_scope',
    'get_engine',
    'get_session',
    'get_db_type',
    'get_db_interface',
    'database_adapter',
    'create_all_tables',
    'drop_all_tables',
    'DatabaseAdapter',
    'DatabaseConnection'
]