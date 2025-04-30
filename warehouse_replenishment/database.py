from contextlib import contextmanager
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from configparser import ConfigParser
import os
from pathlib import Path

# Get the config file path
config_path = Path(__file__).parent / 'config' / 'settings.ini'
config = ConfigParser()
config.read(config_path)

# Database connection settings
DB_HOST = config.get('DATABASE', 'host')
DB_PORT = config.get('DATABASE', 'port')
DB_NAME = config.get('DATABASE', 'database')
DB_USER = config.get('DATABASE', 'username')
DB_PASS = config.get('DATABASE', 'password')

# Create database engine
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URL)

# Create session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

@contextmanager
def session_scope():
    """Provide a transactional scope around a series of operations."""
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close() 