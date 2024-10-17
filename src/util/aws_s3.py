import boto3
import pandas as pd
import logging
from io import StringIO
from util.transversal import set_dynamic_column_names
logging.basicConfig(
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# AWS configurations
s3 = boto3.client('s3')

def read_file(bucket:str, file_key: str, use_headers=False):
    """
        Reads a CSV file from an S3 bucket and returns its content as a Pandas DataFrame.

        If `use_headers` is set to `False`, the function dynamically assigns column names 
        in the format 'column1', 'column2', ..., 'columnN'. 

        Parameters:
        -----------
        file_key : str
            The key (path) of the CSV file stored in the S3 bucket.
        use_headers : bool, optional
            If `True`, uses the first row of the CSV as column headers. 
            If `False`, assigns dynamic column names. Default is `False`.

        Returns:
        --------
        pd.DataFrame
            A Pandas DataFrame containing the content of the CSV file.
    """
    # Connect to S3 and read CSV file
    response = s3.get_object(Bucket=bucket, Key=file_key)
    csv_data = response['Body'].read().decode('utf-8')
    # Change CSV into Pandas DataFrame
    csv_io = StringIO(csv_data)
    df_data = pd.read_csv(csv_io, header=0 if use_headers else None)
    if not use_headers:
        df_data = set_dynamic_column_names(df_data)
    logger.debug('DATA FROM FILE ##########################################')
    df_data.info()
    logger.debug(df_data)
    return df_data

def save_to_s3(output_file, bucket, s3_file_path):
    """
    Take a file to save as an oject to save in a S3 bucket.
    
    Args:
        output_file (tempfile): The element containing the data.
        buket (str): Name of bucket to save data
        s3_file_path (str): The file path where the CSV will be stored in S3.
    """
    s3.put_object(Bucket=bucket, Key=s3_file_path, Body=output_file)

def get_from_s3(bucket, s3_file_path, avro_buffer):
    """
    Get object from S3 bucket and return a buffer.
    
    Args:
        buket (str): Name of bucket to save data
        s3_file_path (str): The file path where the CSV will be stored in S3.
        avro_buffer (io.ByteoIO): Data buffer for avro
    """
    return s3.download_fileobj(bucket, s3_file_path, avro_buffer)