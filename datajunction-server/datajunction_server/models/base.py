"""
A base SQLModel class with a default naming convention.
"""
from typing import Optional

from sqlalchemy.engine.default import DefaultExecutionContext
from sqlmodel import Field, SQLModel

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

    def update(self, data: dict) -> "BaseSQLModel":
        """
        Helper method that updates the current model with new data and validates.
        """
        update = self.dict()
        update.update(data)
        for key, value in self.validate(update).dict(exclude_defaults=True).items():
            setattr(self, key, value)
        return self


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


class NodeColumns(BaseSQLModel, table=True):  # type: ignore
    """
    Join table for node columns.
    """

    node_id: Optional[int] = Field(
        default=None,
        foreign_key="noderevision.id",
        primary_key=True,
    )
    column_id: Optional[int] = Field(
        default=None,
        foreign_key="column.id",
        primary_key=True,
    )
