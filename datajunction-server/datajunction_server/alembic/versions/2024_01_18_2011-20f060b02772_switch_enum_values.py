"""Switch enum values

Revision ID: 20f060b02772
Revises: c74b11566d82
Create Date: 2024-01-18 20:11:08.521879+00:00

"""
# pylint: disable=no-member, invalid-name, missing-function-docstring, unused-import, no-name-in-module

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision = "20f060b02772"
down_revision = "c74b11566d82"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("nodenamespace", schema=None) as batch_op:
        batch_op.create_unique_constraint("uq_nodenamespace_namespace", ["namespace"])

    op.execute("ALTER TYPE oauthprovider RENAME VALUE 'basic' TO 'BASIC'")
    op.execute("ALTER TYPE oauthprovider RENAME VALUE 'github' TO 'GITHUB'")
    op.execute("ALTER TYPE oauthprovider RENAME VALUE 'google' TO 'GOOGLE'")
    op.execute(
        "UPDATE users set oauth_provider = upper(oauth_provider::text)::oauthprovider",
    )

    op.execute("ALTER TYPE materializationstrategy RENAME VALUE 'full' TO 'FULL'")
    op.execute(
        "ALTER TYPE materializationstrategy RENAME VALUE 'snapshot' TO 'SNAPSHOT'",
    )
    op.execute(
        "ALTER TYPE materializationstrategy "
        "RENAME VALUE 'snapshot_partition' TO 'SNAPSHOT_PARTITION'",
    )
    op.execute(
        "ALTER TYPE materializationstrategy RENAME VALUE 'incremental_time' TO 'INCREMENTAL_TIME'",
    )
    op.execute("ALTER TYPE materializationstrategy RENAME VALUE 'view' TO 'VIEW'")
    op.execute(
        "UPDATE materialization set strategy = upper(strategy::text)::materializationstrategy",
    )

    op.execute("ALTER TYPE aggregationrule RENAME VALUE 'additive' TO 'ADDITIVE'")
    op.execute(
        "ALTER TYPE aggregationrule RENAME VALUE 'non_additive' TO 'NON_ADDITIVE'",
    )
    op.execute(
        "ALTER TYPE aggregationrule RENAME VALUE 'semi_additive' TO 'SEMI_ADDITIVE'",
    )
    op.execute("UPDATE measures set additive = upper(additive::text)::aggregationrule")

    op.execute("ALTER TYPE entitytype RENAME VALUE 'attribute' TO 'ATTRIBUTE'")
    op.execute("ALTER TYPE entitytype RENAME VALUE 'availability' TO 'AVAILABILITY'")
    op.execute("ALTER TYPE entitytype RENAME VALUE 'backfill' TO 'BACKFILL'")
    op.execute("ALTER TYPE entitytype RENAME VALUE 'catalog' TO 'CATALOG'")
    op.execute(
        "ALTER TYPE entitytype RENAME VALUE 'column_attribute' TO 'COLUMN_ATTRIBUTE'",
    )
    op.execute("ALTER TYPE entitytype RENAME VALUE 'dependency' TO 'DEPENDENCY'")
    op.execute("ALTER TYPE entitytype RENAME VALUE 'engine' TO 'ENGINE'")
    op.execute("ALTER TYPE entitytype RENAME VALUE 'link' TO 'LINK'")
    op.execute(
        "ALTER TYPE entitytype RENAME VALUE 'materialization' TO 'MATERIALIZATION'",
    )
    op.execute("ALTER TYPE entitytype RENAME VALUE 'namespace' TO 'NAMESPACE'")
    op.execute("ALTER TYPE entitytype RENAME VALUE 'node' TO 'NODE'")
    op.execute("ALTER TYPE entitytype RENAME VALUE 'partition' TO 'PARTITION'")
    op.execute("ALTER TYPE entitytype RENAME VALUE 'query' TO 'QUERY'")
    op.execute("ALTER TYPE entitytype RENAME VALUE 'tag' TO 'TAG'")
    op.execute("UPDATE history set entity_type = upper(entity_type::text)::entitytype")

    op.execute("ALTER TYPE activitytype RENAME VALUE 'create' TO 'CREATE'")
    op.execute("ALTER TYPE activitytype RENAME VALUE 'delete' TO 'DELETE'")
    op.execute("ALTER TYPE activitytype RENAME VALUE 'restore' TO 'RESTORE'")
    op.execute("ALTER TYPE activitytype RENAME VALUE 'update' TO 'UPDATE'")
    op.execute("ALTER TYPE activitytype RENAME VALUE 'refresh' TO 'REFRESH'")
    op.execute("ALTER TYPE activitytype RENAME VALUE 'tag' TO 'TAG'")
    op.execute(
        "ALTER TYPE activitytype RENAME VALUE 'set_attribute' TO 'SET_ATTRIBUTE'",
    )
    op.execute(
        "ALTER TYPE activitytype RENAME VALUE 'status_change' TO 'STATUS_CHANGE'",
    )
    op.execute(
        "UPDATE history set activity_type = upper(activity_type::text)::activitytype",
    )

    op.execute("CREATE TYPE nodestatus AS ENUM ('VALID', 'INVALID')")
    op.execute("CREATE TYPE nodemode AS ENUM ('DRAFT', 'PUBLISHED')")
    op.execute(
        "ALTER TABLE noderevision ALTER COLUMN status "
        "TYPE nodestatus USING upper(status)::nodestatus",
    )
    op.execute(
        "ALTER TABLE noderevision ALTER COLUMN mode TYPE nodemode USING upper(mode)::nodemode",
    )

    op.execute("CREATE TYPE partitiontype AS ENUM ('TEMPORAL', 'CATEGORICAL')")
    op.execute(
        "CREATE TYPE granularity AS ENUM "
        "('SECOND', 'MINUTE', 'HOUR', 'DAY', 'WEEK', 'MONTH', 'QUARTER', 'YEAR')",
    )
    op.execute(
        "ALTER TABLE partition ALTER COLUMN type_ "
        "TYPE partitiontype USING upper(type_)::partitiontype",
    )
    op.execute(
        "ALTER TABLE partition ALTER COLUMN granularity "
        "TYPE granularity USING upper(granularity)::granularity",
    )


def downgrade():
    op.execute("DROP TYPE partitiontype")
    op.execute("DROP TYPE granularity")
    op.execute(
        "ALTER TABLE partition ALTER COLUMN type_ TYPE text USING lower(type_::text)",
    )
    op.execute(
        "ALTER TABLE partition ALTER COLUMN granularity TYPE text USING lower(granularity::text)",
    )

    op.execute("DROP TYPE nodestatus")
    op.execute("DROP TYPE nodemode")
    op.execute(
        "ALTER TABLE noderevision ALTER COLUMN status TYPE text USING lower(status::text)",
    )
    op.execute(
        "ALTER TABLE noderevision ALTER COLUMN mode TYPE text USING lower(mode::text)",
    )

    op.execute("ALTER TYPE activitytype RENAME VALUE 'CREATE' TO 'create'")
    op.execute("ALTER TYPE activitytype RENAME VALUE 'DELETE' TO 'delete'")
    op.execute("ALTER TYPE activitytype RENAME VALUE 'RESTORE' TO 'restore'")
    op.execute("ALTER TYPE activitytype RENAME VALUE 'UPDATE' TO 'update'")
    op.execute("ALTER TYPE activitytype RENAME VALUE 'REFRESH' TO 'refresh'")
    op.execute("ALTER TYPE activitytype RENAME VALUE 'TAG' TO 'tag'")
    op.execute(
        "ALTER TYPE activitytype RENAME VALUE 'SET_ATTRIBUTE' TO 'set_attribute'",
    )
    op.execute(
        "ALTER TYPE activitytype RENAME VALUE 'STATUS_CHANGE' TO 'status_change'",
    )
    op.execute(
        "UPDATE history set activity_type = lower(activity_type::text)::activitytype",
    )

    op.execute("ALTER TYPE entitytype RENAME VALUE 'ATTRIBUTE' TO 'attribute'")
    op.execute("ALTER TYPE entitytype RENAME VALUE 'AVAILABILITY' TO 'availability'")
    op.execute("ALTER TYPE entitytype RENAME VALUE 'BACKFILL' TO 'backfill'")
    op.execute("ALTER TYPE entitytype RENAME VALUE 'CATALOG' TO 'catalog'")
    op.execute(
        "ALTER TYPE entitytype RENAME VALUE 'COLUMN_ATTRIBUTE' TO 'column_attribute'",
    )
    op.execute("ALTER TYPE entitytype RENAME VALUE 'DEPENDENCY' TO 'dependency'")
    op.execute("ALTER TYPE entitytype RENAME VALUE 'ENGINE' TO 'engine'")
    op.execute("ALTER TYPE entitytype RENAME VALUE 'LINK' TO 'link'")
    op.execute(
        "ALTER TYPE entitytype RENAME VALUE 'MATERIALIZATION' TO 'materialization'",
    )
    op.execute("ALTER TYPE entitytype RENAME VALUE 'NAMESPACE' TO 'namespace'")
    op.execute("ALTER TYPE entitytype RENAME VALUE 'NODE' TO 'node'")
    op.execute("ALTER TYPE entitytype RENAME VALUE 'PARTITION' TO 'partition'")
    op.execute("ALTER TYPE entitytype RENAME VALUE 'QUERY' TO 'query'")
    op.execute("ALTER TYPE entitytype RENAME VALUE 'TAG' TO 'tag'")
    op.execute("UPDATE history set entity_type = lower(entity_type::text)::entitytype")

    op.execute("ALTER TYPE aggregationrule RENAME VALUE 'ADDITIVE' TO 'additive'")
    op.execute(
        "ALTER TYPE aggregationrule RENAME VALUE 'NON_ADDITIVE' TO 'non_additive'",
    )
    op.execute(
        "ALTER TYPE aggregationrule RENAME VALUE 'SEMI_ADDITIVE' TO 'semi_additive'",
    )
    op.execute("UPDATE measures set additive = lower(additive::text)::aggregationrule")

    op.execute("ALTER TYPE materializationstrategy RENAME VALUE 'VIEW' TO 'view'")
    op.execute(
        "ALTER TYPE materializationstrategy RENAME VALUE 'INCREMENTAL_TIME' TO 'incremental_time'",
    )
    op.execute(
        "ALTER TYPE materializationstrategy "
        "RENAME VALUE 'SNAPSHOT_PARTITION' TO 'snapshot_partition'",
    )
    op.execute(
        "ALTER TYPE materializationstrategy RENAME VALUE 'SNAPSHOT' TO 'snapshot'",
    )
    op.execute("ALTER TYPE materializationstrategy RENAME VALUE 'FULL' TO 'full'")
    op.execute(
        "UPDATE materialization set strategy = lower(strategy::text)::materializationstrategy",
    )

    op.execute("ALTER TYPE oauthprovider RENAME VALUE 'GOOGLE' TO 'google'")
    op.execute("ALTER TYPE oauthprovider RENAME VALUE 'GITHUB' TO 'github'")
    op.execute("ALTER TYPE oauthprovider RENAME VALUE 'BASIC' TO 'basic'")
    op.execute(
        "UPDATE users set oauth_provider = lower(oauth_provider::text)::oauthprovider",
    )

    with op.batch_alter_table("nodenamespace", schema=None) as batch_op:
        batch_op.drop_constraint("uq_nodenamespace_namespace", type_="unique")
