from flask import Flask, request, jsonify, make_response
from service.flask_sqlalchemy.api_database import db
from service.api_methods import startup_event, fake_users_db, upload_file, paginate_query 
from service.api_methods import backup_table_to_avro, restore_table_from_s3_avro, execute_query
import datetime
import jwt
from security.auth_middleware import token_required
from flask_swagger_ui import get_swaggerui_blueprint
import json
from config import config

file_types = ["job", "department", "employee"]

app = Flask(__name__)
# Retrieve database uri
database_uri = (
    f"{config.database_config['DATABASE_DIALECT']}://{config.database_config['DATABASE_USER']}:{config.database_config['DATABASE_PASSWORD']}"
    f"@{config.database_config['DATABASE_HOST']}:{config.database_config['DATABASE_PORT']}/{config.database_config['DATABASE_NAME']}"
)
app.config['SQLALCHEMY_DATABASE_URI'] = database_uri

db.init_app(app)
startup_event()

# Security using JWT Token
app.config["JWT_SECRET_KEY"] = config.jwt_config["JWT_SECRET_KEY"]  # This will be my secret key

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
    with open("src/swagger_spec.json") as spec_file:
        swag = json.load(spec_file)
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
        token = jwt.encode({'user': auth.username, 'exp': datetime.datetime.now(datetime.timezone.utc) + 
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

@app.route('/employees/by_quarter', methods=['GET'])
@token_required
def hired_employees_by_quarter():
    """
    Get the number of employees hired for each job and department in one year (Ex. 2021) divided by quarter.
    """
    # Get pagination parameters by request
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 10, type=int)
    query_year = request.args.get('year', 2021, type=str)

    query = execute_query("get_employees_by_quarter", query_year)
     
    # Apply pagination
    items, metadata = paginate_query(query, page, per_page)
    
    # Serialización del resultado
    result = [
        {
            'department': row.department,
            'job': row.job,
            'Q1': row.Q1,
            'Q2': row.Q2,
            'Q3': row.Q3,
            'Q4': row.Q4
        }
        for row in items
    ]

    return jsonify({'data': result, **metadata}), 200

@app.route('/departments/hired_above_mean', methods=['GET'])
@token_required
def get_departments_hired_above_mean():
        # Get pagination parameters by request
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 50, type=int)
    query_year = request.args.get('year', 2021, type=str)

    # Step 1: Calculate the mean number of employees hired in 2021
    mean_query = execute_query("get_employees_mean", query_year)
    mean_hired = mean_query.scalar()
    if mean_hired is None:
        mean_hired = 0
    # Step 2: Get departments that hired more than the mean
    query =  execute_query("get_departments_above_mean", query_year, mean_hired)

    # Apply pagination
    items, metadata = paginate_query(query, page, per_page)

    # Step 5: Format the results
    result = [
        {
            "id": row.id,
            "department": row.department,
            "hired": row.hired
        }
        for row in items
    ]

    return jsonify({'data': result, **metadata}), 200


if __name__ == '__main__':
    app.run(debug=True)
    #app.run(host='0.0.0.0', port=5000)

# TEST API BY TERMINAL
# curl -X POST -F "file=@C:\Users\a_f_e\Downloads\Data Challenge\jobs2.csv" http://localhost:5000/jobs/upload