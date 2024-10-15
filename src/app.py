from flask import Flask, request, jsonify, make_response
from service.sql_alchemy.database import create_database_session
import pandas as pd
from validation.data_validation import validate_data, jobs_schema, departments_schema, employees_schema
from dao.jobs_db_creator import Jobs_Db_Creator
from dao.departments_db_creator import Departments_Db_Creator
from dao.employees_db_creator import Employees_Db_Creator
from util.logger import save_error_log
import logging
import datetime
import jwt
from security.auth_middleware import token_required
from flask_swagger_ui import get_swaggerui_blueprint

bucket = 'globant-datachallenge'

logging.basicConfig(
    format="{asctime} - {levelname} - {message}",
    style="{",
    datefmt="%Y-%m-%d %H:%M",
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
jobs_db_creator = None
departments_db_creator = None
employees_db_creator = None
pwd_context = None

app = Flask(__name__)

# Security using JWT Token
app.config["JWT_SECRET_KEY"] = "datachallenge-secret-key"  # This will be my secret key

# Configuración de Swagger
SWAGGER_URL = '/swagger'
API_URL = '/spec'  # URL donde se servirá la especificación de Swagger
swaggerui_blueprint = get_swaggerui_blueprint(
    SWAGGER_URL,
    API_URL,
    config={'app_name': "Mi API"}
)
app.register_blueprint(swaggerui_blueprint, url_prefix=SWAGGER_URL)

@app.route("/spec")
def spec():
    """Generate the Swagger specification.

    Returns:
        jsonify: A JSON representation of the Swagger specification.
    """
    swag = {
        'swagger': '2.0',
        'info': {
            'title': 'Datachallenge API',
            'version': '1.0',
            'description': 'Documentation for Globant Datachallenge API'
        },
        'securityDefinitions': {
            'BearerAuth': {
                'type': 'apiKey',
                'name': 'Authorization',
                'in': 'header',
                'description': 'Enter your Bearer token as `Bearer <token>`'
            }
        },
        'paths': {
            '/login': {
                'get': {
                    'summary': 'User login',
                    'parameters': [
                        {
                            'name': 'username',
                            'in': 'formData',
                            'required': True,
                            'type': 'string'
                        },
                        {
                            'name': 'password',
                            'in': 'formData',
                            'required': True,
                            'type': 'string'
                        }
                    ],
                    'responses': {
                        200: {
                            'description': 'Login successful, returns a Bearer token',
                            'schema': {
                                'type': 'object',
                                'properties': {
                                    'token': {
                                        'type': 'string',
                                        'description': "JWT Bearer token"
                                    }
                                }
                            }
                        },
                        401: {'description': 'Invalid credentials'},
                        500: {'description': 'Internal server error'}
                    }
                }
            },
            '/jobs/upload': {
                'post': {
                    'summary': 'Upload job file',
                    'parameters': [
                        {
                            'name': 'file',
                            'in': 'formData',
                            'required': True,
                            'type': 'file'
                        }
                    ],
                    'responses': {
                        200: {'description': 'File uploaded successfully'},
                        400: {'description': 'Error in the file'},
                        500: {'description': 'Internal server error'}
                    }
                }
            },
            '/departments/upload': {
                'post': {
                    'summary': 'Upload department file',
                    'parameters': [
                        {
                            'name': 'file',
                            'in': 'formData',
                            'required': True,
                            'type': 'file'
                        }
                    ],
                    'responses': {
                        200: {'description': 'File uploaded successfully'},
                        400: {'description': 'Error in the file'},
                        500: {'description': 'Internal server error'}
                    }
                }
            },
            '/employees/upload': {
                'post': {
                    'summary': 'Upload employee file',
                    'parameters': [
                        {
                            'name': 'file',
                            'in': 'formData',
                            'required': True,
                            'type': 'file'
                        }
                    ],
                    'responses': {
                        200: {'description': 'File uploaded successfully'},
                        400: {'description': 'Error in the file'},
                        500: {'description': 'Internal server error'}
                    }
                }
            },
        }
    }
    return jsonify(swag)

def startup_event():
    """Initialize database connections and creators.

    This function sets up the database connection and initializes the 
    database creator instances for jobs, departments, and employees.
    
    Logs a message indicating that the database connection has started.
    """
    global jobs_db_creator
    global departments_db_creator
    global employees_db_creator
    with create_database_session() as db:
        jobs_db_creator = Jobs_Db_Creator(db)
        departments_db_creator = Departments_Db_Creator(db)
        employees_db_creator = Employees_Db_Creator(db)
    logger.info("DB Conn Startup")

# Simulating a user from a database
fake_users_db = {
    "user1": {
        "username": "user1",
        "password": "password123"
    }
}

@app.route("/login")
def login():
    """Authenticate user and return a Bearer token.

    This endpoint verifies user credentials and returns a JWT Bearer token 
    if authentication is successful.

    Returns:
        jsonify: A JSON object containing the Bearer token if successful,
                  or a 401 error response if authentication fails.
    """
    auth = request.authorization
    if auth and auth.password == fake_users_db[auth.username]["password"]:
        token = jwt.encode({'user': auth.username, 'exp': datetime.datetime.utcnow(
        ) + datetime.timedelta(seconds=1800)}, app.config['JWT_SECRET_KEY'])
        return jsonify({'token': f'{token}'})
    return make_response('Could not Verify', 401, {'WWW-Authenticate': 'Basic realm ="Login Required"'})

@app.route('/jobs/upload', methods=['POST'])
@token_required
def upload_job_file():
    """Upload job file to the database.

    This endpoint allows users to upload a CSV file containing job data.
    
    Returns:
        jsonify: A response indicating success or failure of the upload process.
    """
    return upload_file(file_type="job", schema=jobs_schema, db_creator=jobs_db_creator)

@app.route('/departments/upload', methods=['POST'])
@token_required
def upload_department_file():
    """Upload department file to the database.

    This endpoint allows users to upload a CSV file containing department data.
    
    Returns:
        jsonify: A response indicating success or failure of the upload process.
    """
    return upload_file(file_type="department", schema=departments_schema, db_creator=departments_db_creator)

@app.route('/employees/upload', methods=['POST'])
@token_required
def upload_employee_file():
    """Upload employee file to the database.

    This endpoint allows users to upload a CSV file containing employee data.
    
    Returns:
        jsonify: A response indicating success or failure of the upload process.
    """
    return upload_file(file_type="employee", schema=employees_schema, db_creator=employees_db_creator)

def upload_file(file_type, schema, db_creator):
    """Upload a specified file type to the database.

    This function handles file uploads for jobs, departments, or employees,
    validates the contents of the uploaded file, and inserts data into 
    the database.

    Args:
        file_type (str): The type of file being uploaded (e.g., job, department).
        schema (dict): The validation schema for the uploaded data.
        db_creator: The database creator instance used to insert data.

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

        df_valid, df_invalid = validate_data(df_data, schema)
        
        db_creator.factory_orm_insert_data(df_data, headers=True)

        if not df_invalid.empty:
            save_error_log(df_invalid)

        return process_validation_response(df_data, df_valid, df_invalid)

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

def process_validation_response(df_data, df_valid, df_invalid):
    """
    Processes the validation results of the DataFrame and generates a response message
    depending on the results.

    Args:
        df_data (pd.DataFrame): The original DataFrame with all the data.
        df_valid (pd.DataFrame): The DataFrame with rows that passed validation.
        df_invalid (pd.DataFrame): The DataFrame with rows that failed validation.

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
        return jsonify({"message": "Data added partially"}), 201

    # If all data is invalid
    if df_data.size == df_invalid.size:
        return jsonify({"message": "No data added, please check the file"}), 400

    # Default case (if none of the above conditions are met)
    return jsonify({"message": "Unknown processing state"}), 400


if __name__ == '__main__':
    startup_event()
    app.run(debug=True)

# TEST API BY TERMINAL
# curl -X POST -F "file=@C:\Users\a_f_e\Downloads\Data Challenge\jobs2.csv" http://localhost:5000/jobs/upload