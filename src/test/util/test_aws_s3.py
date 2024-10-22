import pytest
from unittest.mock import patch
import pandas as pd
from io import BytesIO, StringIO
from moto import mock_aws
import boto3

from util.aws_s3 import read_file, save_to_s3, get_from_s3

BUCKET_NAME = "test-bucket"
FILE_KEY = "test_file.csv"
CSV_CONTENT = "id,name\n1,Alice\n2,Bob"


@patch("util.aws_s3.s3.get_object")
def test_read_file_with_headers(s3_client):
    """Test reading a file with headers from S3."""
    # Prepare CSV data
    csv_data = "id,name\n1,Alice\n2,Bob\n"
    
    # Mock the S3 get_object response
    s3_client.return_value = {
        "Body": BytesIO(csv_data.encode('utf-8'))  
    }
    
    df = read_file(BUCKET_NAME, FILE_KEY, use_headers=True)
    expected_df = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})

    pd.testing.assert_frame_equal(df, expected_df)

@patch("util.aws_s3.s3.get_object")
def test_read_file_without_headers(s3_client):
    """Test reading a file without headers from S3."""
    # Prepare CSV data
    csv_data = "1,Alice\n2,Bob\n"
    
    # Mock the S3 get_object response
    s3_client.return_value = {
        "Body": BytesIO(csv_data.encode('utf-8'))    
    }

    df = read_file(BUCKET_NAME, FILE_KEY, use_headers=False)
    expected_df = pd.DataFrame({"column1": [1, 2], "column2": ["Alice", "Bob"]})
    expected_df.columns = ["column1", "column2"]

    pd.testing.assert_frame_equal(df, expected_df)


@patch("util.aws_s3.s3.put_object")
def test_save_to_s3(mock_put_object):
    """Test saving a file to S3."""
    output_file = BytesIO(b"sample data")
    save_to_s3(output_file, BUCKET_NAME, FILE_KEY)
    
    mock_put_object.assert_called_once_with(
        Bucket=BUCKET_NAME, Key=FILE_KEY, Body=output_file
    )


@patch("util.aws_s3.s3.download_fileobj")
def test_get_from_s3(mock_download_fileobj):
    """Test getting a file from S3."""
    buffer = BytesIO()
    get_from_s3(BUCKET_NAME, FILE_KEY, buffer)
    
    mock_download_fileobj.assert_called_once_with(BUCKET_NAME, FILE_KEY, buffer)
