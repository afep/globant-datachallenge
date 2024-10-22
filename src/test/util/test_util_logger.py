from unittest.mock import patch
import pandas as pd
from util.logger import generate_s3_file_path, save_file_to_s3, save_error_log

# Sample DataFrame for testing
df_sample = pd.DataFrame({
    "id": [1, 2],
    "name": ["Alice", "Bob"]
})

def test_generate_s3_file_path():
    base_path = "folder/file.csv"
    expected_log_folder = "error_log"
    expected_extension = ".csv"
    
    # Call the function
    result = generate_s3_file_path(base_path, log_folder=expected_log_folder, extension=expected_extension)
    
    # Extract timestamp for dynamic comparison
    timestamp = result.split('_')[-1].split('.')[0]  # Extract timestamp part
    assert expected_log_folder in result
    assert result.startswith("folder/error_log/file_")
    assert result.endswith(f"_{timestamp}.csv")

@patch("util.aws_s3.save_to_s3")
@patch("util.transversal.clean_dataframe")
def test_save_file_to_s3(mock_clean_dataframe, mock_save_to_s3):
    bucket = "test-bucket"
    s3_file_path = "folder/error_log/file.csv"
    
    # Mock the clean_dataframe function to return the same DataFrame for simplicity
    mock_clean_dataframe.return_value = df_sample
    
    # Call the function
    save_file_to_s3(df_sample, bucket, s3_file_path)
    
    # Assert that clean_dataframe was called with the right DataFrame
    mock_clean_dataframe.assert_called_once_with(df_sample)

    # Assert that save_to_s3 was called with correct parameters
    expected_csv = df_sample.to_csv(index=False)
    mock_save_to_s3.assert_called_once_with(output_file=expected_csv, bucket=bucket, s3_file_path=s3_file_path)

@patch("util.aws_s3.save_to_s3")
@patch("util.logger.generate_s3_file_path")  # Adjust the module path accordingly
@patch("util.transversal.clean_dataframe")
def test_save_error_log(mock_clean_dataframe, mock_generate_s3_file_path, mock_save_to_s3):
    df_errors = df_sample
    bucket_name = "test-bucket"
    base_file_name = "folder/file.csv"
    
    # Mock the generate_s3_file_path to return a fixed path
    mock_generate_s3_file_path.return_value = "folder/error_log/file_2024-01-01_12-00-00.csv"
    
    # Call the function
    s3_file_path = save_error_log(df_errors, bucket_name, base_file_name)
    
    # Assert that generate_s3_file_path was called with correct parameters
    mock_generate_s3_file_path.assert_called_once_with(base_file_name)

    # Assert that save_file_to_s3 was called with correct parameters
    mock_save_to_s3.assert_called_once()

    # Check if returned path matches the mocked return value
    assert s3_file_path == "folder/error_log/file_2024-01-01_12-00-00.csv"
