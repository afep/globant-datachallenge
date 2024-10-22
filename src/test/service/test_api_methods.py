import pytest
from sqlalchemy import text
from app import app
import io
import pandas as pd
from unittest.mock import patch, MagicMock
import jwt
from datetime import datetime, timedelta, timezone
from service.api_methods import (startup_event, upload_file, get_required_columns,
                         process_validation_response, backup_table_to_avro,
                         restore_table_from_s3_avro, truncate_table,
                         execute_query, paginate_query)

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

def test_startup_event(mock_db_creators):
    with patch('service.api_methods.DB_CREATORS', new=mock_db_creators):
        startup_event()
        assert mock_db_creators["job"].session is not None

def test_upload_file_no_file(client):
    token = generate_token()
    response = client.post(
        '/upload',
        data={'file_type': 'job'},
        headers={'Authorization': f'Bearer {token}'}
    )
    assert response.json == {'error': 'No file provided'}

def test_upload_file_no_selected_file(client):
    data = {'file_type': 'job', 'file': (io.BytesIO(b''), '')}
    token = generate_token()
    response = client.post(
        '/upload', 
        data=data,
        headers={'Authorization': f'Bearer {token}'})
    assert response.status_code == 400
    assert response.json == {'error': 'No selected file'}

@patch('service.api_methods.pd.read_csv')
@patch('service.api_methods.DB_CREATORS')
def test_upload_file_invalid_columns(mock_db_creators, mock_read_csv, client):
    mock_read_csv.return_value = pd.DataFrame(columns=['wrong_column'])
    mock_db_creators.get.return_value = MagicMock()
    token = generate_token()
    response = client.post(
        '/upload', 
        data={'file_type': 'job','file': (io.BytesIO(b'wrong_column'), 'test.csv')},
        headers={'Authorization': f'Bearer {token}'}, 
        content_type='multipart/form-data')
    assert response.status_code == 400
    assert response.json == {'error': 'CSV must contain id, job columns'}

@patch('util.logger.save_error_log')
@patch('service.api_methods.pd.read_csv')
@patch('service.api_methods.validate_data')
@patch('service.api_methods.DB_CREATORS')
def test_upload_file_valid(mock_db_creators, mock_validate_data, mock_read_csv, mock_log_error, client):
    mock_read_csv.return_value = pd.DataFrame({'id': [1], 'job': ['Developer']})
    mock_validate_data.return_value = (pd.DataFrame({'id': [1], 'job': ['Developer']}), pd.DataFrame())
    mock_log_error.return_value = 'bucket_name/object_key/log.csv'
    token = generate_token()
    response = client.post(
        '/upload', 
        data={'file_type': 'job','file': (io.BytesIO(b'id,job\n1,Developer'), 'test.csv')}, 
        headers={'Authorization': f'Bearer {token}'}, 
        content_type='multipart/form-data')
    assert response.status_code == 201
    assert response.json == {'message': 'Data added successfully'}

def test_process_validation_response(client):
   # All data valid case
    df_data_valid = pd.DataFrame({'id': [1], 'job': ['Developer']})
    df_valid = pd.DataFrame({'id': [1], 'job': ['Developer']})
    df_invalid = pd.DataFrame()

    with client.application.app_context():
        response = process_validation_response(df_data_valid, df_valid, df_invalid, "")
        assert response[1] == 201
        assert response[0].json == {"message": "Data added successfully"}

    # Partial data valid case
    df_data_partial = pd.DataFrame({'id': [1, 2], 'job': ['Developer', '']})
    df_valid_partial = pd.DataFrame({'id': [1], 'job': ['Developer']})
    df_invalid_partial = pd.DataFrame({'id': [2], 'job': ['']})

    with client.application.app_context():
        response = process_validation_response(df_data_partial, df_valid_partial, df_invalid_partial, "s3://bucket/path")
        assert response[1] == 201
        assert response[0].json == {"message": "Data added partially, please check the log s3://bucket/path"}
        
def test_get_required_columns():
    assert get_required_columns("job") == ['id', 'job']
    assert get_required_columns("department") == ['id', 'department']
    assert get_required_columns("employee") == ['id', 'name', 'datetime', 'department_id', 'job_id']
    assert get_required_columns("unknown") == []

def test_truncate_table(client):
    # Set up the application context for the test
    with client.application.app_context():
        with patch('service.api_methods.db.session.execute') as mock_execute:
            truncate_table('data_challenge.jobs')
        
        # Assert that the correct SQL command was executed
        mock_execute.assert_called_once()
        actual_query = mock_execute.call_args[0][0].__str__()  # Get the actual SQL command as a string
        expected_query = "TRUNCATE TABLE data_challenge.jobs RESTART IDENTITY CASCADE"
        assert actual_query == expected_query

@patch('service.api_methods.DB_CREATORS')
def test_execute_query(mock_db_creators):
    mock_query_method = MagicMock(return_value='test_data')
    mock_db_creators.get.return_value.get_employees_by_quarter = mock_query_method

    result = execute_query('get_employees_by_quarter', 2021)
    assert result == 'test_data'
    mock_query_method.assert_called_once_with(2021)

def test_paginate_query(mock_db_creators):
    mock_query = MagicMock()
    mock_query.paginate.return_value = MagicMock(page=1, per_page=10, pages=5, total=50)
    
    items, metadata = paginate_query(mock_query, page=1, per_page=10)
    assert len(items) == 0  # No items as we are mocking the response
    assert metadata['page'] == 1
    assert metadata['per_page'] == 10
    assert metadata['total_pages'] == 5
    assert metadata['total_items'] == 50