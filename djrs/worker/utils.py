"""Utility functions for retrieving API clients."""
import os
from typing import Optional

import djopenapi
from celery import Celery
from djopenapi.apis.tags import default_api

from djrs.config import get_settings


def get_dj_client(
    core_service: Optional[str] = None,
) -> default_api.DefaultApi:
    """
    Return DJ API client
    """
    settings = get_settings()
    configuration = djopenapi.Configuration(
        host=core_service or settings.core_service,
    )
    api_client = djopenapi.ApiClient(configuration)
    return default_api.DefaultApi(api_client)


def get_celery() -> Celery:
    """
    core celery app
    """

    settings = get_settings()

    celery_app = Celery(__name__, include=["djrs.worker.app", "djrs.worker.tasks"])
    celery_app.conf.broker_url = os.environ.get(
        "CELERY_BROKER_URL",
        settings.celery_broker,
    )
    celery_app.conf.result_backend = os.environ.get(
        "CELERY_RESULT_BACKEND",
        settings.celery_results_backend,
    )
    celery_app.conf.imports = ["djrs.worker.app", "djrs.worker.tasks"]
    celery_app.conf.beat_schedule = {
        "refresh": {
            "task": "djrs.worker.app.refresh",
            "schedule": settings.polling_interval,
        },
    }
    return celery_app
