import boto3
import pandas as pd
import psycopg2
from io import StringIO
from datetime import datetime
from data_validation import validate_row, jobs_schema, departments_schema, employees_schema
from jobs_db_creator import Jobs_Db_Creator
from departments_db_creator import Departments_Db_Creator
from employees_db_creator import Employees_Db_Creator

# AWS configurations
s3 = boto3.client('s3')
bucket = 'globant-datachallenge'
file_keys = ['jobs/jobs.csv', 'departments/departments.csv', 'employees/hired_employees (1).csv']
schemas = [jobs_schema, departments_schema, employees_schema]

# Database configurations
db_host = 'globant-datachallenge.c58ckue6w9yw.us-east-1.rds.amazonaws.com'
db_name = 'postgres'
db_user = 'postgres'
db_password = 'Gl0b4nt12345'
db_port = '5432'

def read_file(file_key: str):
    # Connect to S3 and read CSV file
    response = s3.get_object(Bucket=bucket, Key=file_key)
    csv_data = response['Body'].read().decode('utf-8')
    # Change CSV into Pandas DataFrame
    csv_io = StringIO(csv_data)
    df_data = pd.read_csv(csv_io, header=None)
    # Manually assign column names (since the CSV has no headers)
    df_data = set_dynamic_column_names(df_data)
    print('1 ##########################################')
    df_data.info()
    print(df_data)
    return df_data

def validate_data(df_data: pd.DataFrame, schema):
    # Apply validation to each row
    valid_rows = df_data.apply(lambda row: validate_row(row, schema), axis=1)
    print('2 ##########################################')
    print(valid_rows)
    # Split valid/invalid elements
    valid_df = df_data[valid_rows]
    print('3 ##########################################')
    print(valid_df)
    invalid_df = df_data[~valid_rows]
    print('4 ##########################################')
    print(invalid_df)
    return valid_df, invalid_df

def set_dynamic_column_names(df_param: pd.DataFrame):
    # Get the number of columns
    num_columns = df_param.shape[1]
    # Create column names dynamically (e.g., 'column1', 'column2', etc.)
    column_names = [f"column{i+1}" for i in range(num_columns)]
    # Assign the new column names to the DataFrame
    df_param.columns = column_names
    return df_param

def convert_fileds(df_data):
    df_data['column2'] = df_data['column2'].astype(str).replace('nan',None)
    df_data['column3'] = df_data['column3'].astype(str).replace('nan',None)
    df_data['column4'] = df_data['column4'].fillna(-1).astype(int)
    df_data['column5'] = df_data['column5'].fillna(-1).astype(int)
    return df_data

def save_error_log(df_errors, file_name):
    # Convertir el DataFrame a CSV y almacenar en un buffer en memoria
    csv_buffer = StringIO()
    df_errors.to_csv(csv_buffer, index=False)
    # Get the current datetime
    now = datetime.now()
    # Format the datetime into a string for a log file name
    log_name = now.strftime("%Y-%m-%d_%H-%M-%S")
    file_name = file_name.replace('.csv',f'{log_name}.csv')
    # Subir el archivo CSV a S3
    s3.put_object(Bucket=bucket, Key=file_name, Body=csv_buffer.getvalue())
    print(f"DataFrame almacenado como CSV en s3://{bucket}/{file_name}")

if __name__ == '__main__':
    try:
        print('PROCESS STARTED')
        # Connect to PostgreSQL Database
        conn = psycopg2.connect(
            host=db_host,
            database=db_name,
            user=db_user,
            password=db_password,
            port=db_port
        )

        for index, file_name in enumerate(file_keys):
            df_data = read_file(file_name)
            if file_name == 'jobs/jobs.csv':
                dao_db = Jobs_Db_Creator(conn)
            elif file_name == 'departments/departments.csv':
                dao_db = Departments_Db_Creator(conn)
            elif file_name == 'employees/hired_employees (1).csv':
                df_data = convert_fileds(df_data)
                dao_db = Employees_Db_Creator(conn)
            else:
                print('WARNING: New table dao needed!!')
            df_input, df_errors = validate_data(df_data, schemas[index])
            dao_db.factory_create_table()
            dao_db.factory_insert_data(df_input)
            if df_errors.size > 0:
                save_error_log(df_errors.replace(-1, None), file_name.replace('/','/error_log/'))
        
    except Exception as e:
        print("Something went wrong: " + str(e))
    finally:
        conn.close
        print('PROCESS FINISHED')
    