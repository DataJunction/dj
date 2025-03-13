"""Add description to column table

Revision ID: column_description
Revises: c3d5f327296c
Create Date: 2025-03-13 00:00:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = "column_description"
down_revision = "c3d5f327296c"
branch_labels = None
depends_on = None


def upgrade():
    """
    Add description column to the column table
    """
    op.add_column("column", sa.Column("description", sa.String(), server_default=""))


def downgrade():
    """
    Remove description column from the column table
    """
    op.drop_column("column", "description")