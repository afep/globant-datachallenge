from functools import wraps
import jwt
from flask import request
from flask import current_app

def token_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = None
        if "Authorization" in request.headers:
            token = request.headers["Authorization"].split(" ")[1]
        if not token:
            return {
                "message": "Authentication Token is missing!",
                "data": None,
                "error": "Unauthorized"
            }, 401
        try:
            jwt.decode(token, current_app.config["JWT_SECRET_KEY"], algorithms=["HS256"])
        except Exception as e:
            return {
                "message": "Toke is invalid/expired",
                "data": None,
                "error": str(e)
            }, 500

        return f(*args, **kwargs)
    return decorated