"""Switch enum values

Revision ID: 20f060b02772
Revises: c74b11566d82
Create Date: 2024-01-18 20:11:08.521879+00:00

"""
# pylint: disable=no-member, invalid-name, missing-function-docstring, unused-import, no-name-in-module

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '20f060b02772'
down_revision = 'c74b11566d82'
branch_labels = None
depends_on = None


def upgrade():
    op.execute("ALTER TYPE oauthprovider RENAME VALUE 'basic' TO 'BASIC'")
    op.execute("ALTER TYPE oauthprovider RENAME VALUE 'github' TO 'GITHUB'")
    op.execute("ALTER TYPE oauthprovider RENAME VALUE 'google' TO 'GOOGLE'")

    op.execute("ALTER TYPE materializationstrategy RENAME VALUE 'full' TO 'FULL'")
    op.execute("ALTER TYPE materializationstrategy RENAME VALUE 'snapshot' TO 'SNAPSHOT'")
    op.execute("ALTER TYPE materializationstrategy RENAME VALUE 'snapshot_partition' TO 'SNAPSHOT_PARTITION'")
    op.execute("ALTER TYPE materializationstrategy RENAME VALUE 'incremental_time' TO 'INCREMENTAL_TIME'")
    op.execute("ALTER TYPE materializationstrategy RENAME VALUE 'view' TO 'VIEW'")

    op.execute("ALTER TYPE aggregationrule RENAME VALUE 'additive' TO 'ADDITIVE'")
    op.execute("ALTER TYPE aggregationrule RENAME VALUE 'non_additive' TO 'NON_ADDITIVE'")
    op.execute("ALTER TYPE aggregationrule RENAME VALUE 'semi_additive' TO 'SEMI_ADDITIVE'")

    op.execute("ALTER TYPE entitytype RENAME VALUE 'attribute' TO 'ATTRIBUTE'")
    op.execute("ALTER TYPE entitytype RENAME VALUE 'availability' TO 'AVAILABILITY'")
    op.execute("ALTER TYPE entitytype RENAME VALUE 'backfill' TO 'BACKFILL'")
    op.execute("ALTER TYPE entitytype RENAME VALUE 'catalog' TO 'CATALOG'")
    op.execute("ALTER TYPE entitytype RENAME VALUE 'column_attribute' TO 'COLUMN_ATTRIBUTE'")
    op.execute("ALTER TYPE entitytype RENAME VALUE 'dependency' TO 'DEPENDENCY'")
    op.execute("ALTER TYPE entitytype RENAME VALUE 'engine' TO 'ENGINE'")
    op.execute("ALTER TYPE entitytype RENAME VALUE 'link' TO 'LINK'")
    op.execute("ALTER TYPE entitytype RENAME VALUE 'materialization' TO 'MATERIALIZATION'")
    op.execute("ALTER TYPE entitytype RENAME VALUE 'namespace' TO 'NAMESPACE'")
    op.execute("ALTER TYPE entitytype RENAME VALUE 'node' TO 'NODE'")
    op.execute("ALTER TYPE entitytype RENAME VALUE 'partition' TO 'PARTITION'")
    op.execute("ALTER TYPE entitytype RENAME VALUE 'query' TO 'QUERY'")
    op.execute("ALTER TYPE entitytype RENAME VALUE 'tag' TO 'TAG'")

    op.execute("ALTER TYPE activitytype RENAME VALUE 'create' TO 'CREATE'")
    op.execute("ALTER TYPE activitytype RENAME VALUE 'delete' TO 'DELETE'")
    op.execute("ALTER TYPE activitytype RENAME VALUE 'restore' TO 'RESTORE'")
    op.execute("ALTER TYPE activitytype RENAME VALUE 'update' TO 'UPDATE'")
    op.execute("ALTER TYPE activitytype RENAME VALUE 'refresh' TO 'REFRESH'")
    op.execute("ALTER TYPE activitytype RENAME VALUE 'tag' TO 'TAG'")
    op.execute("ALTER TYPE activitytype RENAME VALUE 'set_attribute' TO 'SET_ATTRIBUTE'")
    op.execute("ALTER TYPE activitytype RENAME VALUE 'status_change' TO 'STATUS_CHANGE'")


def downgrade():
    op.execute("ALTER TYPE materializationstrategy RENAME VALUE 'VIEW' TO 'view'")
    op.execute("ALTER TYPE materializationstrategy RENAME VALUE 'INCREMENTAL_TIME' TO 'incremental_time'")
    op.execute("ALTER TYPE materializationstrategy RENAME VALUE 'SNAPSHOT_PARTITION' TO 'snapshot_partition'")
    op.execute("ALTER TYPE materializationstrategy RENAME VALUE 'SNAPSHOT' TO 'snapshot'")
    op.execute("ALTER TYPE materializationstrategy RENAME VALUE 'FULL' TO 'full'")

    op.execute("ALTER TYPE oauthprovider RENAME VALUE 'GOOGLE' TO 'google'")
    op.execute("ALTER TYPE oauthprovider RENAME VALUE 'GITHUB' TO 'github'")
    op.execute("ALTER TYPE oauthprovider RENAME VALUE 'BASIC' TO 'basic'")
