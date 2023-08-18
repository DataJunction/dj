"""Add query_ast to node revision

Revision ID: ab36a0560a57
Revises: cde75f986a62
Create Date: 2023-08-18 04:46:01.781108+00:00

"""
# pylint: disable=no-member, invalid-name, missing-function-docstring, unused-import, no-name-in-module

import sqlalchemy as sa
import sqlmodel
from alembic import op


# revision identifiers, used by Alembic.
revision = 'ab36a0560a57'
down_revision = 'cde75f986a62'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('noderevision', sa.Column('query_ast', sa.JSON(), nullable=True))

def downgrade():
    op.drop_column('noderevision', 'query_ast')
