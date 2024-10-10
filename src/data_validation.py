import pandas as pd
import pandera as pa
from pandera import Column, Check

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
        #print(row)
        schema.validate(pd.DataFrame([row]))
        return True
    except pa.errors.SchemaError as e:
        #print('ERROR')
        #print(e)
        return False

