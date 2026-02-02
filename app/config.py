"""
Configuration settings for Task Scheduler.
"""

from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    """Application settings."""
    
    # Application
    app_name: str = "Task Scheduler"
    environment: str = "development"
    debug: bool = True
    
    # Database
    database_url: str = "postgresql+asyncpg://postgres:postgres@localhost:5432/taskscheduler"
    
    # Redis
    redis_url: str = "redis://localhost:6379"
    
    # Scheduler
    scheduler_interval_seconds: int = 1  # How often to check for due jobs
    max_concurrent_jobs: int = 10  # Max jobs running simultaneously
    
    # Retry Settings
    max_retries: int = 3
    retry_base_delay: int = 60  # Base delay in seconds
    retry_max_delay: int = 3600  # Max delay (1 hour)
    retry_exponential_base: float = 2.0  # Exponential backoff base
    
    # Dead Letter Queue
    dlq_enabled: bool = True
    dlq_retention_days: int = 7
    
    # Worker Settings
    worker_heartbeat_interval: int = 30  # Seconds between heartbeats
    worker_timeout: int = 300  # Job timeout in seconds
    
    # WebSocket
    ws_heartbeat_interval: int = 30
    
    class Config:
        env_file = ".env"


@lru_cache()
def get_settings() -> Settings:
    return Settings()


settings = get_settings()
