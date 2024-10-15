import pandas as pd
import boto3
import re
from io import StringIO
from datetime import datetime
from typing import List, Dict
import logging
logging.basicConfig(
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
# AWS configurations
s3 = boto3.client('s3')

def get_current_timestamp(format_str="%Y-%m-%d_%H-%M-%S"):
    """
    Get the current timestamp formatted as a string.
    
    Args:
        format_str (str): The format string for datetime formatting.
        
    Returns:
        str: The current timestamp as a formatted string.
    """
    return datetime.now().strftime(format_str)

def replace_last_occurrence(s: str, old: str, new: str) -> str:
    return re.sub(f'{old}(?=[^%{old}]*$)', new, s)

def clean_dataframe(df, value_to_replace=-1, replace_with=None):
    """
    Clean the DataFrame by replacing specific values.
    
    Args:
        df (pd.DataFrame): The DataFrame to be cleaned.
        value_to_replace: The value to replace in the DataFrame (default is -1).
        replace_with: The value to replace it with (default is None).
        
    Returns:
        pd.DataFrame: The cleaned DataFrame.
    """
    return df.replace(value_to_replace, replace_with)

def set_dynamic_column_names(df_param: pd.DataFrame):
    """
    Assigns dynamic column names to a DataFrame based on its number of columns.

    The column names will follow the format 'column1', 'column2', ..., 'columnN', 
    where N is the total number of columns in the input DataFrame.

    Parameters:
    -----------
    df_param : pd.DataFrame
        The input DataFrame whose columns will be renamed.

    Returns:
    --------
    pd.DataFrame
        The input DataFrame with updated column names.
    """
    # Get the number of columns
    num_columns = df_param.shape[1]
    # Create column names dynamically (e.g., 'column1', 'column2', etc.)
    column_names = [f"column{i+1}" for i in range(num_columns)]
    # Assign the new column names to the DataFrame
    df_param.columns = column_names
    return df_param

def cast_fields(df_data: pd.DataFrame, string_columns: List[str] = None, int_columns: Dict[str, int] = None
) -> pd.DataFrame:
    """
    Converts and fills specific columns in a DataFrame.

    Parameters:
    - df_data (pd.DataFrame): DataFrame to modify.
    - string_columns (List[str]): Columns to convert to string, replacing nan by None.
    - int_columns (Dict[str, int]): Columns to convert to int, with a fill value for NaNs.

    Returns:
    - pd.DataFrame: Modified DataFrame with converted columns.
    """
    # Convert numeric columns
    if string_columns:
        for column in string_columns:
            df_data[column] = df_data[column].astype(str).replace('nan',None)

    # Convert integer columns with fill value
    if int_columns:
        for column, fill_value in int_columns.items():
            df_data[column] = df_data[column].fillna(fill_value).astype(int)
    
    return df_data

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