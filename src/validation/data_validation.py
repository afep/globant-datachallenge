import pandas as pd
import pandera as pa
from pandera import Column, Check
import logging
logging.basicConfig(
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Pandera validation schema for jobs
jobs_schema = pa.DataFrameSchema({
    "column1": Column(pa.Int, checks=Check.ge(0)),  # Column 1 must be int >= 0
    "column2": Column(pa.String, nullable=False),  # Column 2 must be string
})

# Pandera validation schema for departments
departments_schema = pa.DataFrameSchema({
    "column1": Column(pa.Int, checks=Check.ge(0)),  # Column 1 must be int >= 0
    "column2": Column(pa.String, nullable=False),  # Column 2 must be string
})

# Pandera validation schema for employees
employees_schema = pa.DataFrameSchema({
    "column1": Column(pa.Int, checks=Check.ge(0)),  # Column 1 must be int >= 0
    "column2": Column(pa.String, nullable=False),  # Column 2 must be string
    "column3": Column(pa.String, nullable=False),  # Column 3 must be string
    "column4": Column(pa.Int, checks=Check.ge(0)),  # Column 4 must be int >= 0
    "column5": Column(pa.Int, checks=Check.ge(0)),  # Column 5 must be int >= 0
})

# Function to validate each row
def validate_row(row, schema):
    try:
        #logger.debug(row)
        schema.validate(pd.DataFrame([row]))
        return True
    except pa.errors.SchemaError as e:
        #logger.debug('ERROR')
        #logger.debug(e)
        return False

# Function to validate the entire dataframe
def validate_data(df_data: pd.DataFrame, schema):
    # Apply validation to each row
    valid_rows = df_data.apply(lambda row: validate_row(row, schema), axis=1)
    # Split valid/invalid elements
    valid_df = df_data[valid_rows]
    invalid_df = df_data[~valid_rows]
    logger.debug(' ###################################')
    logger.debug(' ##### DATA VALIDATION SUMMARY #####')
    logger.debug(' ###################################')
    logger.debug(f' INPUT DATA COUNT: {len(df_data.index)}')
    logger.debug(f' VALID DATA COUNT: {len(valid_df.index)}')
    logger.debug(f' INVALID DATA COUNT: {len(invalid_df.index)}')
    logger.debug(' ###################################')

    #logger.debug(' INPUT DATA ########################################')
    #logger.debug(df_data)
    #logger.debug(' CORRECT DATA ######################################')
    #logger.debug(valid_df)
    #logger.debug(' BAD DATA ##########################################')
    #logger.debug(invalid_df)
    return valid_df, invalid_df