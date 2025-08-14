import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class Config:
    # General
    APP_ENV = os.getenv("APP_ENV", "development")
    SECRET_KEY = os.getenv("SECRET_KEY", "default-secret-key")

    # S3 Configuration
    S3BUCKET = os.getenv("S3BUCKET")
    ACCESSKEYID = os.getenv("ACCESSKEYID")
    SECRETACCESSKEY = os.getenv("SECRETACCESSKEY")
    S3_REGION = os.getenv("S3_REGION")

    # Gemini API Key
    GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

    # PostgreSQL Database Configuration
    PG_HOST = os.getenv("PG_DB_HOST", "localhost")
    PG_PORT = os.getenv("PG_DB_PORT", "5432")
    PG_DB = os.getenv("PG_DB_NAME")
    PG_USER = os.getenv("PG_DB_USER")
    PG_PASSWORD = os.getenv("PG_DB_PASSWORD")
    APP_ENV = os.getenv("APP_ENV", "development")

    # Default Table Name (if needed)
    DB_TABLE_NAME = os.getenv("DB_TABLE_NAME")
    PG_DB_NAME_CHAT_HISTORY = os.getenv("PG_DB_NAME_CHAT_HISTORY")
    PG_DB_CHAT_HISTORY_TABLE = os.getenv("PG_DB_CHAT_HISTORY_TABLE")
    GOOGLE_CLOUD_PROJECT = os.getenv("GOOGLE_CLOUD_PROJECT")
    GOOGLE_CLOUD_LOCATION = os.getenv("GOOGLE_CLOUD_LOCATION")
    GOOGLE_GENAI_USE_VERTEXAI = os.getenv("GOOGLE_GENAI_USE_VERTEXAI")
