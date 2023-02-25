"""
A base SQLModel class with a default naming convention.
"""
from sqlmodel import SQLModel

NAMING_CONVENTION = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(auto_constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}


class BaseSQLModel(SQLModel):
    """
    Base model object with naming convention for constraints. This forces alembic's
    autogenerate functionality to generate constraints with explicit names.
    """

    metadata = SQLModel.metadata
    metadata.naming_convention = NAMING_CONVENTION
