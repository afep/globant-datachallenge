from service.sqlalchemy.database import create_database_session, create_database_tables
import traceback
from validation.data_validation import validate_data, jobs_schema, departments_schema, employees_schema
from dao.jobs_db_creator import Jobs_Db_Creator
from dao.departments_db_creator import Departments_Db_Creator
from dao.employees_db_creator import Employees_Db_Creator
import util.transversal as utilities
from util.logger import save_error_log
from util.aws_s3 import read_file
import logging
logging.basicConfig(
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
bucket = 'globant-datachallenge'

# Database configurations
db_host = 'globant-datachallenge.c58ckue6w9yw.us-east-1.rds.amazonaws.com'
db_name = 'postgres'
db_user = 'postgres'
db_password = 'Gl0b4nt12345'
db_port = '5432'

file_types = ['job', 'department', 'employee']

file_keys_map = {
    'job': 'jobs/jobs.csv',
    'department': 'departments/departments.csv',
    'employee':'employees/hired_employees (1).csv'
}

schema_map = {
    'job': jobs_schema,
    'department': departments_schema,
    'employee': employees_schema
}

dao_map = {
    'job': Jobs_Db_Creator,
    'department': Departments_Db_Creator,
    'employee': Employees_Db_Creator
}

if __name__ == '__main__':
    try:
        logger.debug('PROCESS STARTED')
        # Connect to PostgreSQL Database
        with create_database_session() as db:
            create_database_tables()

            for file_type in file_types:
                file_name = file_keys_map.get(file_type)
                df_data = read_file(bucket, file_name)
                if file_type == 'employee':
                    df_data = utilities.cast_fields(df_data=df_data, 
                                            string_columns=['column2', 'column3'], 
                                            int_columns={'column4': -1, 'column5': -1})
                
                dao_class = dao_map.get(file_type)
                schema_definition = schema_map.get(file_type)
                if dao_class and schema_definition:
                    dao_db = dao_class(db)
                    df_input, df_errors = validate_data(df_data, schema_definition)
                    dao_db.factory_orm_insert_data(df_input)
                    if df_errors.size > 0:
                        save_error_log(df_errors, bucket, file_name)
                else:
                    logger.warning(f'No Schema or DAO class mapped for {file_type}!')
     
    except Exception as e:
        logger.error("Something went wrong: " + str(e))
        logger.error(traceback.format_exc())
    finally:
        logger.debug('PROCESS FINISHED')