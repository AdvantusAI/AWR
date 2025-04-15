from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.ext.declarative import declarative_base
from contextlib import contextmanager

# Fix the import path
from config import config

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
            connection_string = config.get_db_url()
        
        echo = config.get_boolean('DATABASE', 'echo', False)
        self._engine = create_engine(connection_string, echo=echo)
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