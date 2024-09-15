"""Initial migration

Revision ID: b8f22b3549c7
Revises:
Create Date: 2024-09-09 06:00:00.000000+00:00

"""
# pylint: disable=no-member, invalid-name, missing-function-docstring, unused-import, no-name-in-module

from alembic import op

# revision identifiers, used by Alembic.
revision = "b8f22b3549c7"
down_revision = None
branch_labels = None
depends_on = None


def upgrade():
    op.execute(
        """
    CREATE TABLE query (
        id UUID PRIMARY KEY,
        catalog_name VARCHAR NOT NULL,
        engine_name VARCHAR NOT NULL,
        engine_version VARCHAR NOT NULL,
        submitted_query VARCHAR NOT NULL,
        async_ BOOLEAN NOT NULL,
        executed_query VARCHAR,
        scheduled TIMESTAMP,
        started TIMESTAMP,
        finished TIMESTAMP,
        state VARCHAR NOT NULL,
        progress FLOAT NOT NULL
    )
    """,
    )


def downgrade():
    op.execute("DROP TABLE query")
