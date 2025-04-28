"""
Database utility functions for the ASR system.
"""
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.ext.declarative import declarative_base

from config.settings import DB_URL

# Create the SQLAlchemy engine
engine = create_engine(DB_URL, echo=False)

# Create a session factory
session_factory = sessionmaker(bind=engine)
Session = scoped_session(session_factory)

def get_session():
    """
    Get a database session.
    
    Returns:
        SQLAlchemy session object
    """
    return Session()

def close_session(session):
    """
    Close a database session.
    
    Args:
        session: SQLAlchemy session to close
    """
    session.close()

def init_db():
    """
    Initialize the database by creating all tables.
    """
    from models.base import Base
    Base.metadata.create_all(engine)

def drop_db():
    """
    Drop all tables from the database.
    """
    from models.base import Base
    Base.metadata.drop_all(engine)