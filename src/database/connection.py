from typing import Generator
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, DeclarativeBase, Session
from src.core.config import settings

# ------------------------------------------------------------------------------
# Database Engine Configuration
# ------------------------------------------------------------------------------

# Create the SQLAlchemy engine.
# - pool_pre_ping=True: Vital for production. It checks if the connection is alive 
#   before handing it to the application, preventing "server closed connection" errors.
# - echo=False: Set to True during debugging to see raw SQL queries in the console.
engine = create_engine(
    settings.SQLALCHEMY_DATABASE_URI,
    pool_pre_ping=True,
    echo=False 
)

# ------------------------------------------------------------------------------
# Session Factory
# ------------------------------------------------------------------------------

# Create a configured "Session" class.
# This serves as a factory for new Session objects.
# - autocommit=False: We want to manually commit transactions to ensure atomicity.
# - autoflush=False: We want to manually flush changes to the DB.
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# ------------------------------------------------------------------------------
# Base Model Class
# ------------------------------------------------------------------------------

class Base(DeclarativeBase):
    """
    Base class for all SQLAlchemy ORM models.
    In SQLAlchemy 2.0 style, models inherit from this DeclarativeBase.
    """
    pass

# ------------------------------------------------------------------------------
# Dependency Injection
# ------------------------------------------------------------------------------

def get_db() -> Generator[Session, None, None]:
    """
    Dependency generator that yields a database session.
    
    Designed to be used with FastAPI's `Depends` or context managers.
    Ensures that the database session is always closed after the request is finished,
    even if an exception occurs during processing.

    Yields:
        Session: An active SQLAlchemy database session.
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        # Standard practice: Always close the session to release connection to the pool.
        db.close()