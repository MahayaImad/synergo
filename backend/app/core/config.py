# backend/app/core/config.py
from pydantic_settings import BaseSettings
from typing import Optional
import os

class Settings(BaseSettings):
    # Database
    DATABASE_URL: str = "postgresql://synergo_user:synergo_2024!@localhost:5439/synergo_db"
    ASYNC_DATABASE_URL: str = "postgresql+asyncpg://synergo_user:synergo_2024!@localhost:5439/synergo_db"
    
    # Redis
    REDIS_URL: str = "redis://localhost:6379/0"
    
    # HFSQL
    HFSQL_SERVER: str = "localhost"
    HFSQL_PORT: int = 4900
    HFSQL_DATABASE: str = "EASYPHARM" 
    HFSQL_USER: str = "admin"
    HFSQL_PASSWORD: str = "25061986"
    
    # Security
    SECRET_KEY: str = "synergo-pharm"
    ALGORITHM: str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES: int = 30
    
    # Sync
    SYNC_INTERVAL_MINUTES: int = 30
    SYNC_BATCH_SIZE: int = 1000
    SYNC_MAX_RETRIES: int = 3
    
    # Logging
    LOG_LEVEL: str = "INFO"
    LOG_FILE: str = "logs/synergo.log"
    
    class Config:
        env_file = ".env"
        case_sensitive = True

settings = Settings()
