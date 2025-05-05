# warehouse_replenishment/db/connection.py
import os
import sys
from typing import Dict, Any, Optional, Union, Literal
from pathlib import Path
from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from supabase import create_client, Client

# Add parent directories to path for imports
parent_dir = str(Path(__file__).parent.parent.parent)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from warehouse_replenishment.config import config
from warehouse_replenishment.exceptions import DatabaseError
from warehouse_replenishment.models import Base

DatabaseType = Literal["postgresql", "supabase"]

class DatabaseConfig:
    """Configuration for database connections."""
    
    @staticmethod
    def get_db_type() -> DatabaseType:
        """Get database type from configuration."""
        db_type = config.get('DATABASE', 'type', default='postgresql').lower()
        # Remove any comments from the value
        db_type = db_type.split('#')[0].strip()
        return db_type
    
    @staticmethod
    def get_postgresql_config() -> Dict[str, Any]:
        """Get PostgreSQL connection configuration."""
        return {
            'engine': config.get('DATABASE', 'engine', default='postgresql'),
            'host': config.get('DATABASE', 'host', default='localhost'),
            'port': config.get_int('DATABASE', 'port', default=5433),
            'database': config.get('DATABASE', 'database', default='m8_aws'),
            'username': config.get('DATABASE', 'username', default='postgres'),
            'password': config.get('DATABASE', 'password', default='Admin0606'),
            'pool_size': config.get_int('DATABASE', 'pool_size', default=10),
            'max_overflow': config.get_int('DATABASE', 'max_overflow', default=20),
            'pool_timeout': config.get_int('DATABASE', 'pool_timeout', default=30),
            'pool_recycle': config.get_int('DATABASE', 'pool_recycle', default=1800),
            'echo': config.get_boolean('DATABASE', 'echo', default=False)
        }
    
    @staticmethod
    def get_supabase_config() -> Dict[str, str]:
        """Get Supabase connection configuration."""
        # Try environment variables first
        if os.getenv('SUPABASE_URL') and os.getenv('SUPABASE_KEY'):
            return {
                'url': os.getenv('SUPABASE_URL'),
                'key': os.getenv('SUPABASE_KEY')
            }
        
        # Fall back to config file
        return {
            'url': config.get('SUPABASE', 'url', default=''),
            'key': config.get('SUPABASE', 'key', default='')
        }
    
    @staticmethod
    def get_connection_string() -> str:
        """Get PostgreSQL connection string."""
        pg_config = DatabaseConfig.get_postgresql_config()
        return (
            f"{pg_config['engine']}://{pg_config['username']}:{pg_config['password']}"
            f"@{pg_config['host']}:{pg_config['port']}/{pg_config['database']}"
        )

class DatabaseConnection:
    """Unified database connection handler for PostgreSQL and Supabase."""
    
    _instance = None
    _engine = None
    _SessionLocal = None
    _supabase = None
    _db_type: DatabaseType = None
    
    def __new__(cls):
        """Ensure singleton pattern."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        """Initialize database connection."""
        if self._db_type is None:
            self._db_type = DatabaseConfig.get_db_type()
            self._initialize_connection()
    
    def _initialize_connection(self):
        """Initialize the database connection based on type."""
        db_type = DatabaseConfig.get_db_type()
        
        if db_type == "supabase":
            self._db_type = "supabase"
            self._initialize_supabase()
        elif db_type == "postgresql":
            self._db_type = "postgresql"
            self._initialize_postgresql()
        else:
            raise DatabaseError(f"Unknown database type: {db_type}")
    
    def _initialize_postgresql(self):
        """Initialize PostgreSQL connection."""
        try:
            pg_config = DatabaseConfig.get_postgresql_config()
            connection_string = DatabaseConfig.get_connection_string()
            
            self._engine = create_engine(
                connection_string,
                pool_size=pg_config['pool_size'],
                max_overflow=pg_config['max_overflow'],
                pool_timeout=pg_config['pool_timeout'],
                pool_recycle=pg_config['pool_recycle'],
                echo=pg_config['echo']
            )
            
            self._SessionLocal = sessionmaker(
                autocommit=False,
                autoflush=False,
                bind=self._engine
            )
            
            # Test connection
            self._test_postgresql_connection()
            
        except Exception as e:
            raise DatabaseError(f"Failed to initialize PostgreSQL connection: {str(e)}")
    
    def _initialize_supabase(self):
        """Initialize Supabase connection."""
        try:
            supabase_config = DatabaseConfig.get_supabase_config()
            
            if not supabase_config['url'] or not supabase_config['key']:
                raise ValueError("Supabase URL and key must be provided")
            
            self._supabase = create_client(
                supabase_config['url'],
                supabase_config['key']
            )
            
            # Test connection
            self._test_supabase_connection()
            
        except Exception as e:
            raise DatabaseError(f"Failed to initialize Supabase connection: {str(e)}")
    
    def _test_postgresql_connection(self):
        """Test PostgreSQL connection."""
        try:
            with self._engine.connect() as conn:
                conn.execute("SELECT 1")
        except Exception as e:
            raise DatabaseError(f"PostgreSQL connection test failed: {str(e)}")
    
    def _test_supabase_connection(self):
        """Test Supabase connection."""
        try:
            # Test connection by querying any table
            result = self._supabase.table('company').select('*').limit(1).execute()
            if result.data is None and hasattr(result, 'error') and result.error:
                raise DatabaseError(f"Supabase test query failed: {result.error}")
        except Exception as e:
            raise DatabaseError(f"Supabase connection test failed: {str(e)}")
    
    @contextmanager
    def session_scope(self) -> Session:
        """Provide transaction scope for database operations."""
        if self._db_type != "postgresql":
            raise DatabaseError("session_scope is only available for PostgreSQL connections")
        
        session = self._SessionLocal()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
    
    def get_session(self) -> Session:
        """Get a database session (PostgreSQL only)."""
        if self._db_type != "postgresql":
            raise DatabaseError("get_session is only available for PostgreSQL connections")
        
        return self._SessionLocal()
    
    def get_supabase(self) -> Client:
        """Get Supabase client (Supabase only)."""
        if self._db_type != "supabase":
            raise DatabaseError("get_supabase is only available for Supabase connections")
        
        return self._supabase
    
    @property
    def engine(self):
        """Get SQLAlchemy engine (PostgreSQL only)."""
        if self._db_type != "postgresql":
            raise DatabaseError("engine is only available for PostgreSQL connections")
        
        return self._engine
    
    @property
    def db_type(self) -> DatabaseType:
        """Get current database type."""
        return self._db_type

# Singleton instance
db = DatabaseConnection()

# Convenience function for session scope
@contextmanager
def session_scope():
    """Context manager for database sessions."""
    with db.session_scope() as session:
        yield session

# Backwards compatibility
def get_engine():
    """Get SQLAlchemy engine for PostgreSQL connections."""
    return db.engine

def get_session():
    """Get database session for PostgreSQL connections."""
    return db.get_session()