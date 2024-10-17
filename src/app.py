from flask import Flask, request, jsonify, make_response
from service.api_methods import startup_event, fake_users_db, upload_file, backup_table_to_avro, restore_table_from_s3_avro
import datetime
from datetime import timezone
import jwt
from security.auth_middleware import token_required
from flask_swagger_ui import get_swaggerui_blueprint

DB_CREATORS = None
file_types = ["job", "department", "employee"]

app = Flask(__name__)

# Security using JWT Token
app.config["JWT_SECRET_KEY"] = "datachallenge-secret-key"  # This will be my secret key

# Configuración de Swagger
SWAGGER_URL = '/swagger'
API_URL = '/spec'  # URL donde se servirá la especificación de Swagger
swaggerui_blueprint = get_swaggerui_blueprint(
    SWAGGER_URL,
    API_URL,
    config={'app_name': "DataChallenge API"}
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
        'tags': [
            {'name': 'Auth', 'description': 'Authentication-related endpoints'},
            {'name': 'Upload', 'description': 'Data load operations'}
        ],
        'paths': {
            '/login': {
                'get': {
                    'summary': 'User login',
                    'tags': ['Auth'],
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
            '/upload': {
                'post': {
                    'summary': 'Upload a file specifying its type',
                    'tags': ['Upload'],
                    'parameters': [
                        {'name': 'file_type', 'in': 'formData', 'required': True, 'type': 'string'},
                        {'name': 'file', 'in': 'formData', 'required': True, 'type': 'file'}
                    ],
                    'responses': {
                        200: {'description': 'File uploaded successfully'},
                        400: {'description': 'Invalid file or missing file_type'},
                        500: {'description': 'Internal server error'}
                    }
                }
            },
            '/jobs/upload': {
                'post': {
                    'summary': 'Upload job file',
                    'tags': ['Upload'],
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
                    'tags': ['Upload'],
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
                    'tags': ['Upload'],
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
            '/backup': {
                'get': {
                    'summary': 'Create backups for all tables',
                    'tags': ['Backup'],
                    'responses': {
                        200: {'description': 'Backup completed for all tables'},
                        500: {'description': 'Internal server error'}
                    }
                }
            },
            '/restore': {
                'get': {
                    'summary': 'Restore tables from backups',
                    'tags': ['Backup'],
                    'responses': {
                        200: {'description': 'Restore completed for all tables'},
                        500: {'description': 'Internal server error'}
                    }
                }
            }
        }
    }
    return jsonify(swag)

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
        token = jwt.encode({'user': auth.username, 'exp': datetime.datetime.now(timezone.utc) + 
                            datetime.timedelta(seconds=1800)}, app.config['JWT_SECRET_KEY'])
        return jsonify({'token': f'{token}'})
    return make_response('Could not Verify', 401, {'WWW-Authenticate': 'Basic realm ="Login Required"'})

@app.route('/upload', methods=['POST'])
@token_required
def upload_any_file():
    """Upload file to the database specifying the type of the file.

    This endpoint allows users to upload a CSV file containing any data.
    
    Returns:
        jsonify: A response indicating success or failure of the backup process.
    """    
    file_type = request.form.get('file_type')
    if not file_type:
        return jsonify({"error": "file_type is required"}), 400
    return upload_file(file_type=file_type)

@app.route('/jobs/upload', methods=['POST'])
@token_required
def upload_job_file():
    """Upload job file to the database.

    This endpoint allows users to upload a CSV file containing job data.
    
    Returns:
        jsonify: A response indicating success or failure of the upload process.
    """
    return upload_file(file_type="job")

@app.route('/departments/upload', methods=['POST'])
@token_required
def upload_department_file():
    """Upload department file to the database.

    This endpoint allows users to upload a CSV file containing department data.
    
    Returns:
        jsonify: A response indicating success or failure of the upload process.
    """
    return upload_file(file_type="department")

@app.route('/employees/upload', methods=['POST'])
@token_required
def upload_employee_file():
    """Upload employee file to the database.

    This endpoint allows users to upload a CSV file containing employee data.
    
    Returns:
        jsonify: A response indicating success or failure of the upload process.
    """
    return upload_file(file_type="employee")

@app.route('/backup', methods=['GET'])
@token_required
def backup_database():
    """Create the backup file for all the tables in the database.

    This endpoint allows users to request the creation of a AVRO file as a backup
    
    Returns:
        jsonify: A response indicating success or failure of the backup process.
    """    
    messages = [backup_table_to_avro(file_type) for file_type in file_types]
    return jsonify({"message": " - ".join(messages)}), 200

@app.route('/restore', methods=['GET'])
@token_required
def restore_database():
    """Create the restored table from all the existing avro backup files in the database.

    This endpoint allows users to request the creation of the table using the existin AVRO file backup
    
    Returns:
        jsonify: A response indicating success or failure of the backup process.
    """
    messages = [restore_table_from_s3_avro(file_type) for file_type in file_types]
    return jsonify({"message": " - ".join(messages)}), 200

if __name__ == '__main__':
    startup_event()
    app.run(debug=True)
    #app.run(host='0.0.0.0', port=5000)

# TEST API BY TERMINAL
# curl -X POST -F "file=@C:\Users\a_f_e\Downloads\Data Challenge\jobs2.csv" http://localhost:5000/jobs/upload