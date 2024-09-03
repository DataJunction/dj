"""
Useful constants.
"""

from datetime import timedelta
from uuid import UUID

DJ_DATABASE_ID = 0
DJ_DATABASE_UUID = UUID("594804bf-47cb-426c-83c4-94a348e95972")
SQLITE_DATABASE_ID = -1
SQLITE_DATABASE_UUID = UUID("3619eeba-d628-4ab1-9dd5-65738ab3c02f")

DEFAULT_DIMENSION_COLUMN = "id"

# used by the SQLAlchemy client
QUERY_EXECUTE_TIMEOUT = timedelta(seconds=60)
GET_COLUMNS_TIMEOUT = timedelta(seconds=60)

AUTH_COOKIE = "__dj"
LOGGED_IN_FLAG_COOKIE = "__djlif"
