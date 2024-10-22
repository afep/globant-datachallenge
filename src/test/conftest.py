import pytest
from flask import Flask
from unittest.mock import MagicMock

# Create a Flask app instance for testing
def create_app():
    app = Flask(__name__)
    return app

@pytest.fixture
def client():
    app = create_app()
    with app.test_client() as client:
        yield client

@pytest.fixture
def mock_db_creators():
    """Mock the DB_CREATORS for testing."""
    return {
        "job": MagicMock(),
        "department": MagicMock(),
        "employee": MagicMock(),
        "reports": MagicMock()
    }