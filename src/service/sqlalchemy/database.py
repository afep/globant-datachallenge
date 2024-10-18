"""
SQLAlchemy Database
"""
from contextlib import contextmanager

from sqlalchemy import create_engine, text
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker

# Declarative base.
Base = declarative_base()

# Retrieve database uri
database_uri = 'postgresql://postgres:Gl0b4nt12345@globant-datachallenge.c58ckue6w9yw.us-east-1.rds.amazonaws.com:5432/postgres'

# Session local
engine = create_engine(database_uri)
SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)

@contextmanager
def create_database_session():
    """Get database local session."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def create_database_tables() -> None:
    """Creates a database from the models."""
    Base.metadata.create_all(engine)