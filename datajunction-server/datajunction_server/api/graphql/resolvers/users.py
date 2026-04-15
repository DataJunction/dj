"""
User resolvers — shared load options for the User model in GraphQL queries.
"""

from sqlalchemy.orm import load_only, noload

from datajunction_server.database.user import User as DBUser

# GraphQL User fields to DB User columns. Note that id is always included as it's the PK.
_USER_FIELD_TO_COLUMN = {
    "username": DBUser.username,
    "email": DBUser.email,
    "name": DBUser.name,
    "oauth_provider": DBUser.oauth_provider,
    "is_admin": DBUser.is_admin,
}


def user_load_options(requested_fields):
    """Build load options for a User relationship based on requested fields."""
    if not requested_fields:
        return []
    cols = [DBUser.id] + [
        col for field, col in _USER_FIELD_TO_COLUMN.items() if field in requested_fields
    ]
    return [
        load_only(*cols),
        noload(DBUser.created_by),
        noload(DBUser.notification_preferences),
        noload(DBUser.role_assignments),
    ]
