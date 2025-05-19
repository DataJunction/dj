"""Reflection service settings."""

from functools import lru_cache

from pydantic import BaseSettings


class Settings(BaseSettings):
    """
    Default settings for the reflection service.
    """

    core_service: str = "http://dj:8000"
    query_service: str = "http://djqs:8001"
    celery_broker: str = "redis://djrs-redis:6379/1"
    celery_results_backend: str = "redis://djrs-redis:6379/2"

    # Set the number of seconds to wait in between polling
    polling_interval: int = 3600


@lru_cache
def get_settings() -> Settings:
    """
    Return a cached settings object.
    """
    return Settings()
