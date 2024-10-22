import pytest
import jwt
from flask import Flask, jsonify
from security.auth_middleware import token_required 

# Create a function to instantiate the Flask app
def create_app():
    app = Flask(__name__)
    app.config["JWT_SECRET_KEY"] = "your_jwt_secret_key"  # Use a fixed secret for testing

    @app.route("/protected", methods=["GET"])
    @token_required
    def protected():
        return jsonify({"message": "Access granted!"}), 200

    return app

@pytest.fixture
def client():
    app = create_app()  # Use the factory function to create the app
    with app.test_client() as client:
        yield client


def test_missing_token(client):
    response = client.get("/protected")
    assert response.status_code == 401
    assert response.json["message"] == "Authentication Token is missing!"
    assert response.json["error"] == "Unauthorized"

def test_invalid_token(client):
    # Pass an invalid token
    invalid_token = "Bearer invalid_token"
    response = client.get("/protected", headers={"Authorization": invalid_token})
    assert response.status_code == 500
    assert response.json["message"] == "Toke is invalid/expired"

def test_valid_token(client):
    # Create a valid token
    valid_token = jwt.encode({"user_id": 1}, "your_jwt_secret_key", algorithm="HS256")
    response = client.get("/protected", headers={"Authorization": f"Bearer {valid_token}"})
    assert response.status_code == 200
    assert response.json["message"] == "Access granted!"
