from functools import lru_cache

from pydantic import BaseSettings


class Settings(BaseSettings):
    """
    Default settings for the reflection service.
    """

    core_service: str = "http://dj:8000"  # Default value
    query_service: str = "http://djqs:8001"  # Default value
    celery_broker: str = "redis://djrs-redis:6379/1"  # Default value
    celery_results_backend: str = "redis://djrs-redis:6379/2"  # Default value
    polling_interval: int = 3600  # Default value

    class Config:
        env_prefix = "REFLECTION_"  # Prefix for environment variables


@lru_cache
def get_settings() -> Settings:
    """
    Return a cached settings object.
    """
    return Settings()
