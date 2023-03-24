"""
Celery app that does the polling of nodes in DJ and then subsequent
queueing of reflection tasks.
"""
from djrs.worker.utils import get_celery

celery_app = get_celery()
