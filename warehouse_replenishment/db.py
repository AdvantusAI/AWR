from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.ext.declarative import declarative_base
from contextlib import contextmanager
import sys
from pathlib import Path
import urllib.parse

# Add parent directory to path for imports
parent_dir = str(Path(__file__).parent.parent)
if parent_dir not in sys.path:
    sys.path.append(parent_dir)

from warehouse_replenishment.config import config

Base = declarative_base()

class Database:
    """Database connection manager for the Warehouse Replenishment System."""
    
    _instance = None
    
    def __new__(cls):
        """Singleton pattern implementation."""
        if cls._instance is None:
            cls._instance = super(Database, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        """Initialize the database connection if not already initialized."""
        if self._initialized:
            return
        
        self._engine = None
        self._session_factory = None
        self._session = None
        self._initialized = True
    
    def initialize(self, connection_string=None):
        """Initialize database connection.
        
        Args:
            connection_string: Optional database connection string.
                              If not provided, will use configuration.
        """
        if connection_string is None:
            # Get database configuration from settings.ini
            engine = config.get('DATABASE', 'engine', 'postgresql')
            username = config.get('DATABASE', 'username', 'postgres')
            password = config.get('DATABASE', 'password', 'Admin0606')
            host = config.get('DATABASE', 'host', 'localhost')
            port = config.get('DATABASE', 'port', '5433')
            database = config.get('DATABASE', 'database', 'm8_aws')
            
            # Get connection pool settings
            pool_size = config.get_int('DATABASE', 'pool_size', 10)
            max_overflow = config.get_int('DATABASE', 'max_overflow', 20)
            pool_timeout = config.get_int('DATABASE', 'pool_timeout', 30)
            pool_recycle = config.get_int('DATABASE', 'pool_recycle', 1800)
            
            # URL encode the password to handle special characters
            password = urllib.parse.quote_plus(password)
            
            # Construct the connection string
            connection_string = f"{engine}://{username}:{password}@{host}:{port}/{database}"
        
        echo = config.get_boolean('DATABASE', 'echo', False)
        
        # Create engine with connection pooling
        self._engine = create_engine(
            connection_string,
            echo=echo,
            pool_size=pool_size,
            max_overflow=max_overflow,
            pool_timeout=pool_timeout,
            pool_recycle=pool_recycle
        )
        
        self._session_factory = sessionmaker(bind=self._engine)
        self._session = scoped_session(self._session_factory)
    
    def create_all_tables(self):
        """Create all tables defined in the models."""
        from warehouse_replenishment.models import Base
        Base.metadata.create_all(self._engine)
    
    def drop_all_tables(self):
        """Drop all tables from the database."""
        from warehouse_replenishment.models import Base
        Base.metadata.drop_all(self._engine)
    
    @property
    def session(self):
        """Get the current database session."""
        if self._session is None:
            self.initialize()
        return self._session
    
    @property
    def engine(self):
        """Get the database engine."""
        if self._engine is None:
            self.initialize()
        return self._engine
    
    @contextmanager
    def session_scope(self):
        """Provide a transactional scope around a series of operations."""
        session = self.session()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()
    
    def execute_raw_sql(self, sql, params=None):
        """Execute raw SQL query.
        
        Args:
            sql: SQL query string
            params: Parameters for the query
            
        Returns:
            Query result
        """
        with self.engine.connect() as connection:
            if params:
                return connection.execute(sql, params)
            return connection.execute(sql)
    
    def execute_and_fetch_all(self, sql, params=None):
        """Execute raw SQL query and fetch all results.
        
        Args:
            sql: SQL query string
            params: Parameters for the query
            
        Returns:
            List of query results
        """
        result = self.execute_raw_sql(sql, params)
        return result.fetchall()

# Global database instance
db = Database()

def get_session():
    """Get current database session."""
    return db.session()

@contextmanager
def session_scope():
    """Session scope context manager."""
    with db.session_scope() as session:
        yield session