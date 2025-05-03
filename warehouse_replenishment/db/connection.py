from typing import Union, Optional
from enum import Enum
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from supabase import create_client, Client
import os
from dotenv import load_dotenv

class DatabaseType(Enum):
    """Enum for supported database types."""
    SQLALCHEMY = "sqlalchemy"
    SUPABASE = "supabase"

class DatabaseConnection:
    """Factory class for creating database connections."""
    
    def __init__(self, db_type: DatabaseType, connection_string: Optional[str] = None):
        """Initialize database connection.
        
        Args:
            db_type: Type of database connection to create
            connection_string: Connection string for SQLAlchemy (optional for Supabase)
        """
        self.db_type = db_type
        self.connection_string = connection_string
        self._session = None
        self._supabase = None
        
    def connect(self) -> Union[Session, Client]:
        """Create and return a database connection.
        
        Returns:
            Either a SQLAlchemy Session or Supabase Client
        """
        if self.db_type == DatabaseType.SQLALCHEMY:
            if not self.connection_string:
                raise ValueError("Connection string is required for SQLAlchemy")
                
            engine = create_engine(self.connection_string)
            SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
            self._session = SessionLocal()
            return self._session
            
        elif self.db_type == DatabaseType.SUPABASE:
            load_dotenv()
            supabase_url = os.getenv("SUPABASE_URL")
            supabase_key = os.getenv("SUPABASE_KEY")
            
            if not supabase_url or not supabase_key:
                raise ValueError("Supabase URL and key must be set in environment variables")
                
            self._supabase = create_client(supabase_url, supabase_key)
            return self._supabase
            
        else:
            raise ValueError(f"Unsupported database type: {self.db_type}")
    
    def close(self):
        """Close the database connection."""
        if self.db_type == DatabaseType.SQLALCHEMY and self._session:
            self._session.close()
            self._session = None
        elif self.db_type == DatabaseType.SUPABASE:
            self._supabase = None
    
    def __enter__(self):
        """Context manager entry."""
        return self.connect()
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()

# Example usage:
# SQLAlchemy:
# db = DatabaseConnection(DatabaseType.SQLALCHEMY, "sqlite:///warehouse_replenishment.db")
# with db as session:
#     # Use session for database operations
#
# Supabase:
# db = DatabaseConnection(DatabaseType.SUPABASE)
# with db as supabase:
#     # Use supabase for database operations 