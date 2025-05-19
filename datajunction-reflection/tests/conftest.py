"""Test configuration."""

import threading

import pytest

from datajunction_reflection.worker.app import celery_app as celeryapp

# pytest_plugins = ("celery.contrib.pytest", )


@pytest.fixture()
def celery_app():
    """
    Configure celery app for unit tests. This uses an in-memory broker
    and starts a single worker.
    """
    celeryapp.conf.update(CELERY_ALWAYS_EAGER=True)
    celeryapp.conf.broker_url = "memory://localhost/"
    celeryapp.conf.result_backend = ""
    celeryapp.conf.CELERYD_CONCURRENCY = 1
    celeryapp.conf.CELERYD_POOL = "solo"
    celeryapp.all_tasks = []

    def run_worker():
        """Celery worker."""
        celeryapp.worker_main()

    thread = threading.Thread(target=run_worker)
    thread.daemon = True
    thread.start()
    return celeryapp
