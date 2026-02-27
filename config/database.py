import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()

class DatabaseConfig:
    """Database configuration and connection manager"""
    
    def __init__(self):
        self.host = os.getenv('DB_HOST')
        self.port = os.getenv('DB_PORT')
        self.database = os.getenv('DB_NAME')
        self.user = os.getenv('DB_USER')
        self.password = os.getenv('DB_PASSWORD')
    
    def get_connection_string(self):
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
    
    def get_engine(self):
        return create_engine(self.get_connection_string())

# Global database config instance
db_config = DatabaseConfig()