"""
A base SQLModel class with a default naming convention.
"""
import sqlalchemy as sa

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
