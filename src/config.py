import configparser

class Config:
    general_config = None
    database_config = None
    jwt_config = None

    def __init__(self):
        self.load_ini_config()

    def load_ini_config(self):
        _config = configparser.ConfigParser()
        _config.read("src/config.ini")

        self.general_config = {
            "PROJECT_NAME": _config.get("general", "project_name"),
            "SERVICE_NAME": _config.get("general", "service_name"),
        }

        self.database_config = {
            "DATABASE_DIALECT": _config.get("database", "dialect"),
            "DATABASE_HOST": _config.get("database", "host"),
            "DATABASE_PORT": int(_config.get("database", "port")),
            "DATABASE_USER": _config.get("database", "user"),
            "DATABASE_PASSWORD": _config.get("database", "password"),
            "DATABASE_NAME": _config.get("database", "name")
        }

        self.jwt_config = {
            "JWT_SECRET_KEY": _config.get("jwt", "secret_key"),
        }

# Create a global instance of the Config class
config = Config()