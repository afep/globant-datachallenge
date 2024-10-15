import boto3
from io import StringIO
import util.transversal as transversal

# AWS S3 client
s3 = boto3.client('s3')

def generate_s3_file_path(base_path, log_folder="error_log", extension=".csv"):
    """
    Generate the full S3 file path, appending the log folder and a timestamp.
    
    Args:
        base_path (str): The base S3 path (e.g., 'folder/file.csv').
        log_folder (str): The folder where logs will be saved.
        extension (str): The file extension to use (default is '.csv').
        
    Returns:
        str: The complete S3 file path including the timestamp and log folder.
    """
    timestamp = transversal.get_current_timestamp()
    base_path = transversal.replace_last_occurrence(base_path, '/', f'/{log_folder}/')
    return base_path.replace(extension, f'_{timestamp}{extension}')

def save_file_to_s3(df_data, bucket, s3_file_path):
    """
    Save a DataFrame to save in a S3 bucket as a CSV file.
    
    Args:
        df_data (pd.DataFrame): The DataFrame containing the data.
        s3_file_path (str): The file path where the CSV will be stored in S3.
    """
    # Clean the DataFrame
    cleaned_df = transversal.clean_dataframe(df_data)
    
    # Convert the DataFrame to CSV and store in memory buffer
    csv_buffer = StringIO()
    cleaned_df.to_csv(csv_buffer, index=False)
    
    # Upload the CSV to S3
    s3.put_object(Bucket=bucket, Key=s3_file_path, Body=csv_buffer.getvalue())
    
    print(f"File log saved as CSV to s3://{bucket}/{s3_file_path}")

def save_error_log(df_errors, bucket_name, base_file_name):
    """
    High-level function to save the error log to S3, with a dynamic file path.
    
    Args:
        df_errors (pd.DataFrame): The DataFrame containing the errors.
        base_file_name (str): The base file name (e.g., 'folder/file.csv').
    """
    s3_file_path = generate_s3_file_path(base_file_name)
    save_file_to_s3(df_errors, bucket_name, s3_file_path)
