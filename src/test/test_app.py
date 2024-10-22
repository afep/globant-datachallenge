import pytest
from app import app
import jwt
from datetime import datetime, timedelta, timezone

SECRET_KEY = app.config['JWT_SECRET_KEY']

@pytest.fixture
def client():
    """Flask test client fixture."""
    with app.test_client() as client:
        yield client


def generate_token():
    """Helper function to generate a JWT token."""
    return jwt.encode(
        {'user': 'user1', 'exp': datetime.now(timezone.utc) + timedelta(minutes=30)},
        SECRET_KEY
    )

def test_spec(client):
    """Test the /spec endpoint returns the Swagger specification."""
    response = client.get('/spec')
    assert response.status_code == 200
    assert response.json['info']['title'] == 'Datachallenge API'


def test_login_success(client, mocker):
    """Test successful login returns a token."""
    mocker.patch('service.api_methods.fake_users_db', {'user1': {'password': 'password123'}})
    
    response = client.get('/login', headers={'Authorization': 'Basic dXNlcjE6cGFzc3dvcmQxMjM='})  # base64(user1:password123)
    assert response.status_code == 200
    assert 'token' in response.json


def test_login_failure(client, mocker):
    """Test login with invalid credentials returns 401."""
    mocker.patch('service.api_methods.fake_users_db', {'user1': {'password': 'password123'}})
    
    response = client.get('/login', headers={'Authorization': 'Basic dXNlcjE6aW52YWxpZA=='})  # base64(user1:invalid)
    assert response.status_code == 401


def test_upload_without_token(client):
    """Test uploading without a token returns 401."""
    response = client.post('/upload', data={'file_type': 'job'})
    assert response.status_code == 401


def test_upload_with_token(client, mocker):
    """Test uploading with a valid token."""
    mocker.patch('app.upload_file', return_value=("Success", 200))
    token = generate_token()
    response = client.post(
        '/upload',
        data={'file_type': 'job'},
        headers={'Authorization': f'Bearer {token}'}
    )
    assert response.status_code == 200


def test_upload_missing_file_type(client, mocker):
    """Test upload with missing file_type returns 400."""
    token = generate_token()
    response = client.post(
        '/upload',
        headers={'Authorization': f'Bearer {token}'}
    )
    assert response.status_code == 400
    assert response.json['error'] == 'file_type is required'


@pytest.mark.parametrize("endpoint", [
    '/jobs/upload', '/departments/upload', '/employees/upload'
])
def test_upload_endpoints(client, endpoint, mocker):
    """Test uploading to specific endpoints for jobs, departments, and employees."""
    mocker.patch('app.upload_file', return_value=("Success", 200))
    token = generate_token()
    response = client.post(
        endpoint,
        headers={'Authorization': f'Bearer {token}'}
    )
    assert response.status_code == 200


def test_backup_database(client, mocker):
    """Test the backup endpoint."""
    mocker.patch('app.backup_table_to_avro', return_value="Backup successful")
    token = generate_token()
    response = client.get('/backup', headers={'Authorization': f'Bearer {token}'})
    assert response.status_code == 200
    assert "Backup successful" in response.json['message']


def test_restore_database(client, mocker):
    """Test the restore endpoint."""
    mocker.patch('app.restore_table_from_s3_avro', return_value="Restore successful")
    token = generate_token()
    response = client.get('/restore', headers={'Authorization': f'Bearer {token}'})
    assert response.status_code == 200
    assert "Restore successful" in response.json['message']
