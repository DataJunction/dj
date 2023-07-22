"""Utility functions for retrieving API clients."""
import os

from celery import Celery

from datajunction_reflection.config import get_settings


def get_celery() -> Celery:
    """
    core celery app
    """

    settings = get_settings()

    celery_app = Celery(
        __name__,
        include=[
            "datajunction_reflection.worker.app",
            "datajunction_reflection.worker.tasks",
        ],
    )
    celery_app.conf.broker_url = os.environ.get(
        "CELERY_BROKER_URL",
        settings.celery_broker,
    )
    celery_app.conf.result_backend = os.environ.get(
        "CELERY_RESULT_BACKEND",
        settings.celery_results_backend,
    )
    celery_app.conf.imports = [
        "datajunction_reflection.worker.app",
        "datajunction_reflection.worker.tasks",
    ]
    celery_app.conf.beat_schedule = {
        "refresh": {
            "task": "datajunction_reflection.worker.app.refresh",
            "schedule": settings.polling_interval,
        },
    }
    return celery_app
