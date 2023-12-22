"""
A base SQLModel class with a default naming convention.
"""
import sqlalchemy as sa
from sqlalchemy.engine.default import DefaultExecutionContext
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql.schema import ForeignKey

from datajunction_server.database.connection import Base

NAMING_CONVENTION = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(auto_constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}


def sqlalchemy_enum_with_name(enum_type):
    """
    SQLAlchemy-compatible enum type using the enum's names as values
    """
    return sa.Enum(enum_type, values_callable=lambda obj: [e.name for e in obj])


def sqlalchemy_enum_with_value(enum_type):
    """
    SQLAlchemy-compatible enum type using the enum's values as values
    """
    return sa.Enum(
        enum_type,
        values_callable=lambda obj: [e.value for e in obj],
    )


def labelize(value: str) -> str:
    """
    Turn a system name into a human-readable name.
    """

    return value.replace(".", ": ").replace("_", " ").title()


def generate_display_name(column_name: str):
    """
    SQLAlchemy helper to generate a human-readable version of the given system name.
    """

    def default_function(context: DefaultExecutionContext) -> str:
        column_value = context.current_parameters.get(column_name)
        return labelize(column_value)

    return default_function


class NodeColumns(Base):  # pylint: disable=too-few-public-methods
    """
    Join table for node columns.
    """

    __tablename__ = "nodecolumns"

    node_id: Mapped[int] = mapped_column(
        ForeignKey("noderevision.id"),
        primary_key=True,
    )
    column_id: Mapped[int] = mapped_column(
        ForeignKey("column.id"),
        primary_key=True,
    )
