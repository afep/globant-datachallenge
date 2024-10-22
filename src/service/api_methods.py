from flask import request, jsonify
from service.flask_sqlalchemy.api_database import db
from sqlalchemy.orm import Query
from typing import Any, Dict, List, Tuple
from sqlalchemy import text
from dao.jobs_db_creator import Jobs_Db_Creator
from dao.departments_db_creator import Departments_Db_Creator
from dao.employees_db_creator import Employees_Db_Creator
from dao.queries_db_reports import Queries_Db_Reports
from validation.data_validation import validate_data
from util.logger import save_error_log
from util.aws_s3 import save_to_s3, get_from_s3
from util.transversal import set_dynamic_column_names
from validation.data_validation import jobs_schema, departments_schema, employees_schema
import logging
import avro.schema
import avro.datafile
import avro.io
import io
import pandas as pd

logging.basicConfig(
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

bucket = 'globant-datachallenge'

schema_map = {
    'job': jobs_schema,
    'department': departments_schema,
    'employee': employees_schema
}

# Simulating a user from a database
fake_users_db = {
    "user1": {
        "username": "user1",
        "password": "password123"
    }
}

def startup_event():
    """Initialize database connections and creators.

    This function sets up the database connection and initializes the 
    database creator instances for jobs, departments, and employees.
    
    Logs a message indicating that the database connection has started.
    """
    global DB_CREATORS
    jobs_db_creator = Jobs_Db_Creator(db.session)
    departments_db_creator = Departments_Db_Creator(db.session)
    employees_db_creator = Employees_Db_Creator(db.session)
    queries_db_report = Queries_Db_Reports(db.session)
    # Strategy to selecct the data table
    DB_CREATORS = {
        "job": jobs_db_creator,
        "department": departments_db_creator,
        "employee": employees_db_creator,
        "reports": queries_db_report
    }
    logger.info("DB Connections Startup")

def upload_file(file_type):
    """Upload a specified file type to the database.

    This function handles file uploads for jobs, departments, or employees,
    validates the contents of the uploaded file, and inserts data into 
    the database.

    Args:
        file_type (str): The type of file being uploaded (e.g., job, department, employee).

    Returns:
        jsonify: A response indicating success or failure of the upload process.
    """
    if 'file' not in request.files:
        return jsonify({'error': 'No file provided'}), 400

    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No selected file'}), 400

    try:
        df_data = pd.read_csv(file)
        required_columns = get_required_columns(file_type)
        
        if not all(col in df_data.columns for col in required_columns):
            return jsonify({'error': f'CSV must contain {", ".join(required_columns)} columns'}), 400
        schema = schema_map.get(file_type)
        df_data_val= set_dynamic_column_names(df_data)
        df_valid, df_invalid = validate_data(df_data_val, schema)
        if not df_valid.empty:
            s3_key = file_type+"s/"+str(file.filename)
            db_creator = DB_CREATORS.get(file_type)
            db_creator.factory_orm_insert_data(df_data, headers=False)
        s3_file_path = ''
        if not df_invalid.empty:
            s3_key = file_type+"s/"+str(file.filename)
            s3_file_path = save_error_log(df_invalid, bucket, s3_key)

        return process_validation_response(df_data, df_valid, df_invalid, s3_file_path)

    except Exception as e:
        return jsonify({'error': str(e)}), 500

def get_required_columns(file_type):
    """Get required columns based on file type.

    This function returns a list of required column names for the specified 
    file type (job, department, or employee).

    Args:
        file_type (str): The type of file (job/department/employee).

    Returns:
        list: A list of required column names.
    """
    if file_type == "job":
        return ['id', 'job']
    elif file_type == "department":
        return ['id', 'department']
    elif file_type == "employee":
        return ['id', 'name', 'datetime', 'department_id', 'job_id']
    return []

def process_validation_response(df_data, df_valid, df_invalid, s3_file_path):
    """
    Processes the validation results of the DataFrame and generates a response message
    depending on the results.

    Args:
        df_data (pd.DataFrame): The original DataFrame with all the data.
        df_valid (pd.DataFrame): The DataFrame with rows that passed validation.
        df_invalid (pd.DataFrame): The DataFrame with rows that failed validation.
        s3_file_path (str): name of the log file

    Returns:
        dict: A dictionary containing the message and the status of the process.
    """

    # Initial check if no data was provided
    if df_data.empty:
        return jsonify({"message": "No data provided"}), 400

    # Check if all data is valid
    if df_data.size == df_valid.size:
        return jsonify({"message": "Data added successfully"}), 201
    
    # If there are both valid and invalid rows
    if not df_valid.empty and not df_invalid.empty:
        return jsonify({"message": f"Data added partially, please check the log {s3_file_path}"}), 201

    # If all data is invalid
    if df_data.size == df_invalid.size:
        return jsonify({"message": f"No data added, please check the log {s3_file_path}"}), 400

    # Default case (if none of the above conditions are met)
    return jsonify({"message": "Unknown processing state"}), 400

# Factory to get schemas AVRO
def get_avro_schema(file_type: str):
    """Convert a row from the database in a dictionary compatible with AVRO.
    Args:
        file_type (str): The type of file (job/department/employee).

    Returns:
        schema: A schema avro definition."""
    schemas = {
        "job": '{"type": "record", "name": "Job", "fields": [{"name": "id", "type": "int"}, {"name": "job", "type": "string"}]}',
        "department": '{"type": "record", "name": "Department", "fields": [{"name": "id", "type": "int"}, {"name": "department", "type": "string"}]}',
        "employee": '{"type": "record", "name": "Employee", "fields": [{"name": "id", "type": "int"}, {"name": "name", "type": "string"}, {"name": "datetime", "type": "string"}, {"name": "department_id", "type": "int"}, {"name": "job_id", "type": "int"}]}'
    }
    if file_type not in schemas:
        raise ValueError(f"Invalid file type: {file_type}")
    return avro.schema.parse(schemas[file_type])

def serialize_row(row, file_type: str) -> dict:
    """Convert a row from the database in a dictionary compatible with AVRO.
    Args:
        file_type (str): The type of file (job/department/employee).

    Returns:
        dict: A dictionary with required column names."""
    if file_type == "job":
        return {"id": row.id, "job": row.job}
    elif file_type == "department":
        return {"id": row.id, "department": row.department}
    elif file_type == "employee":
        return {
            "id": row.id,
            "name": row.name,
            "datetime": row.datetime,
            "department_id": row.department_id,
            "job_id": row.job_id
        }
    raise ValueError(f"Unsupported file type: {file_type}")

def backup_table_to_avro(file_type):
    """
    Backs up the Jobs table from PostgreSQL to an AVRO file.

    Args:
        file_type (str): The type of file being backed up (e.g., job, department, employee).
    """
    try:
        # Execute a query to fetch all rows from the Jobs table
        db_creator = DB_CREATORS.get(file_type)
        rows = db_creator.get_all_data()
        
        # Getting AVRO schema definition
        schema = get_avro_schema(file_type)
              
        # Creating buffer in memory
        with io.BytesIO() as avro_buffer:
            writer = avro.datafile.DataFileWriter(avro_buffer, avro.io.DatumWriter(), schema)
            # Assuming `rows` has data
            for row in rows:
                writer.append(serialize_row(row, file_type))
            writer.flush()
            avro_buffer.seek(0)
            # Upload to S3
            s3_key = f'backups/{file_type}_backup.avro'
            save_to_s3(avro_buffer, bucket, s3_key)
                    
        logger.info(f"Backup completed successfully. Data saved to {s3_key}")
        return f"Backup complete for {file_type} in {s3_key}"
    except Exception as e:
        logger.error(f"Message: {e}")
        return f"Error in backup for {file_type}: {e}"

def restore_table_from_s3_avro(file_type, truncate_option=False):
    """
    Restores data from an AVRO file in an S3 bucket to a PostgreSQL database.

    Args:
        file_type (str): The type of file being backed up (e.g., job, department, employee).
        truncate_option (boolean): Option to truncate the table before try restoring data

    """
    try:
        s3_key = f'backups/{file_type}_backup.avro'
        # Download the AVRO file into memory
        avro_buffer = io.BytesIO()
        get_from_s3(bucket, s3_key, avro_buffer)
        # Move back to the start of the BytesIO buffer
        avro_buffer.seek(0)
        # Read data from the AVRO file using avro.datafile.DataFileReader
        reader = avro.datafile.DataFileReader(avro_buffer, avro.io.DatumReader())
        # Load records in memory
        records = [r for r in reader]
        # Populate pandas.DataFrame with records
        df_data = pd.DataFrame.from_records(records)
        if truncate_option:
            truncate_table(f'data_challenge.{file_type}s')
        db_creator = DB_CREATORS.get(file_type)
        db_creator.factory_orm_insert_data(df_data, headers=True)
        logger.info(f"Successfully restored data from s3://{bucket}/{s3_key} to the database.")
        return f"Restore complete for {file_type} in {s3_key}"
    except Exception as e:
        logger.error(f"Message: {e}")
        return f"Error in restore for {file_type}: {e}"
    
def truncate_table(table_name) -> None:
    """
    Truncate a specific table and reset its identity.

    This function performs a raw SQL execution to truncate a given table, 
    removing all rows and resetting the primary key sequence (identity).

    Args:
        table_name (str): The name of the table to truncate.

    Raises:
        SQLAlchemyError: If there is an issue executing the SQL command.
    
        Example:
        truncate_table('data_challenge.employees')
    """
    db.session.execute(text(f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE"))
    db.session.commit()
    
def execute_query(function_name, *args, **kwargs):
    """
    Execute a query method dynamically by its name and return the result.

    This function retrieves the appropriate query method from the reports
    database creator and invokes it with the provided arguments.

    Args:
        function_name (str): The name of the query function to execute.
        *args: Positional arguments to pass to the query function.
        **kwargs: Keyword arguments to pass to the query function.

    Returns:
        Any: The result of the query execution.

    Raises:
        AttributeError: If the specified method does not exist.

    Example:
        result = execute_query("get_employees_by_quarter", 2021)
    """
    db_report = DB_CREATORS.get("reports")
    # getattr to call methon name dinamically
    query_method = getattr(db_report, function_name, None)

    if query_method is None:
        raise AttributeError(f"Method '{function_name}' not found in {db_report}")

    query_data = query_method(*args, **kwargs)
    return(query_data)

def paginate_query(query: Query, page: int, per_page: int) -> Tuple[List[Any], Dict[str, Any]]:
    """
    Apply pagination to a SQLAlchemy query and return the results with metadata.

    This function paginates the given query and returns both the items in the 
    specified page and a dictionary containing pagination metadata.

    Args:
        query (Query): The SQLAlchemy query to paginate.
        page (int): The page number to retrieve.
        per_page (int): The number of items per page.

    Returns:
        Tuple[List[Any], Dict[str, Any]]: 
            A tuple containing the items in the requested page and the 
            pagination metadata (page number, items per page, total pages, and total items).

    Example:
        items, metadata = paginate_query(employee_query, page=1, per_page=10)
    """
    paginated_result = query.paginate(page=page, per_page=per_page, error_out=False)
    metadata = {
        'page': paginated_result.page,
        'per_page': paginated_result.per_page,
        'total_pages': paginated_result.pages,
        'total_items': paginated_result.total
    }
    return paginated_result.items, metadata