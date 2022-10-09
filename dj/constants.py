"""
Useful constants.
"""

from datetime import timedelta

DJ_DATABASE_ID = 0
SQLITE_DATABASE_ID = -1

DEFAULT_DIMENSION_COLUMN = "id"

# used by the SQLAlchemy client
QUERY_EXECUTE_TIMEOUT = timedelta(seconds=60)
GET_COLUMNS_TIMEOUT = timedelta(seconds=60)
