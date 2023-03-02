"""Add status and mode attributes to Node

Revision ID: 20321580ea8a
Revises: f620c4521c80
Create Date: 2023-01-12 01:41:09.414015+00:00

"""
# pylint: disable=no-member, invalid-name, missing-function-docstring, unused-import, no-name-in-module

import enum

import sqlalchemy as sa
from sqlmodel import Field, Session, SQLModel, select
from sqlmodel.sql import sqltypes

from alembic import op

# revision identifiers, used by Alembic.
revision = "20321580ea8a"
down_revision = "f620c4521c80"
branch_labels = None
depends_on = None


class NodeMode(str, enum.Enum):
    """
    Node mode.

    A node can be in one of the following modes:

    1. PUBLISHED - Must be valid and not cause any child nodes to be invalid
    2. DRAFT - Can be invalid, have invalid parents, and include dangling references
    """

    PUBLISHED = "published"
    DRAFT = "draft"


class NodeStatus(str, enum.Enum):
    """
    Node status.

    A node can have one of the following statuses:

    1. VALID - All references to other nodes and node columns are valid
    2. INVALID - One or more parent nodes are incompatible or do not exist
    """

    VALID = "valid"
    INVALID = "invalid"


'''
class Node(SQLModel, table=True):
    """
    A node with only attributes needed for migration.
    """

    id: Optional[int] = Field(default=None, primary_key=True)
    status: NodeStatus = NodeStatus.INVALID
    mode: NodeMode = NodeMode.PUBLISHED
'''


def upgrade():
    # add columns as nullable, set to default, and make it non-nullable
    op.add_column(
        "node",
        sa.Column("mode", sqltypes.AutoString(), nullable=True),
    )
    op.add_column(
        "node",
        sa.Column("status", sqltypes.AutoString(), nullable=True),
    )

    """
    for node in session.exec(select(Node)).all():
        node.mode = NodeMode.PUBLISHED
        node.status = NodeStatus.INVALID
        session.add(node)
    session.commit()

    op.alter_column("node", "mode", nullable=False)
    op.alter_column("node", "status", nullable=False)
    """


def downgrade():
    with op.batch_alter_table("node") as bop:
        bop.drop_column("status")
        bop.drop_column("mode")
