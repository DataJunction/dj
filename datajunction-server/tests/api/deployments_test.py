import asyncio
import json
import uuid
from contextlib import asynccontextmanager
from datetime import UTC, date, datetime, timedelta
from types import SimpleNamespace
from unittest import mock
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy import select
from sqlalchemy.orm import selectinload

import datajunction_server.internal.materializations
from datajunction_server.api.deployments import (
    InProcessExecutor,
    _normalize_repo_path,
)
from datajunction_server.database.availabilitystate import AvailabilityState
from datajunction_server.database.materialization import Materialization
from datajunction_server.database.node import Node, NodeRelationship
from datajunction_server.database.tag import Tag
from datajunction_server.errors import DJInvalidInputException
from datajunction_server.internal.git.github_service import GitHubServiceError
from datajunction_server.models import access
from datajunction_server.models.deployment import (
    ColumnSpec,
    CubeSpec,
    DeploymentSpec,
    DeploymentStatus,
    DimensionJoinLinkSpec,
    DimensionReferenceLinkSpec,
    DimensionSpec,
    GitDeploymentSource,
    Granularity,
    LocalDeploymentSource,
    MaterializationAction,
    MaterializationSpec,
    MetricSpec,
    PartitionSpec,
    PartitionType,
    PreAggSpec,
    SourceSpec,
    TagSpec,
    TransformSpec,
)
from datajunction_server.models.dimensionlink import JoinCardinality, JoinType
from datajunction_server.models.materialization import (
    MaterializationInfo,
    MaterializationStrategy,
    SparkSpec,
)
from datajunction_server.models.node import (
    MetricDirection,
    MetricUnit,
    NodeMode,
    NodeType,
)
from datajunction_server.database.user import PrincipalKind
from datajunction_server.models.cube_materialization import PrincipalRef
from datajunction_server.models.preaggregation import CubeBackfillInput
from datajunction_server.utils import get_query_service_client
from tests.authz import VALIDATOR_AUTH_SERVICE, deny
from tests.construction.build_v3 import assert_sql_equal


@pytest.fixture(autouse=True, scope="module")
def patch_effective_writer_concurrency():
    from datajunction_server.internal.deployment.deployment import settings

    with mock.patch.object(
        settings.__class__,
        "effective_writer_concurrency",
        new_callable=mock.PropertyMock,
        return_value=1,
    ):
        yield


def _assert_deployment_spec_matches(
    input_spec,
    deployed_spec,
    namespace: str,
    node_name: str,
):
    """
    Assert that a deployed node's spec matches the input spec.

    Handles namespace transformation (${prefix} -> namespace.) and
    compares key fields based on node type.
    """
    prefix = f"{namespace}."

    def transform_value(value):
        """Transform ${prefix} placeholders to actual namespace."""
        if isinstance(value, str):
            return value.replace("${prefix}", prefix)
        elif isinstance(value, list):
            return [transform_value(v) for v in value]
        elif isinstance(value, dict):
            return {k: transform_value(v) for k, v in value.items()}
        return value

    # Common fields for all node types
    assert deployed_spec.name == f"{prefix}{input_spec.name}", (
        f"Name mismatch for {node_name}: "
        f"expected {prefix}{input_spec.name}, got {deployed_spec.name}"
    )

    # Validate owners
    if input_spec.owners:
        assert set(deployed_spec.owners) == set(input_spec.owners), (
            f"Owners mismatch for {node_name}"
        )

    # Validate description
    if input_spec.description:
        # Allow whitespace normalization in description
        expected_desc = " ".join(input_spec.description.split())
        actual_desc = " ".join((deployed_spec.description or "").split())
        assert actual_desc == expected_desc, (
            f"Description mismatch for {node_name}: "
            f"expected '{expected_desc}', got '{actual_desc}'"
        )

    # Type-specific validations
    if isinstance(input_spec, SourceSpec):
        # Validate catalog/schema/table
        assert deployed_spec.catalog == input_spec.catalog
        assert deployed_spec.schema == input_spec.schema
        assert deployed_spec.table == input_spec.table

        # Validate columns
        input_cols = {col.name: col for col in (input_spec.columns or [])}
        deployed_cols = {col.name: col for col in (deployed_spec.columns or [])}
        assert set(input_cols.keys()) == set(deployed_cols.keys()), (
            f"Column name mismatch for {node_name}: "
            f"expected {set(input_cols.keys())}, got {set(deployed_cols.keys())}"
        )

    elif isinstance(input_spec, (DimensionSpec, TransformSpec)):
        # Validate query exists (content may have minor reformatting)
        assert deployed_spec.query is not None, f"Query missing for {node_name}"

        # Validate primary key for dimensions
        if isinstance(input_spec, DimensionSpec) and input_spec.primary_key:
            deployed_pk = deployed_spec.primary_key or []
            assert set(deployed_pk) == set(input_spec.primary_key), (
                f"Primary key mismatch for {node_name}"
            )

        # Validate dimension links
        if input_spec.dimension_links:
            assert len(deployed_spec.dimension_links or []) == len(
                input_spec.dimension_links,
            ), f"Dimension link count mismatch for {node_name}"

    elif isinstance(input_spec, MetricSpec):
        # Validate query exists
        assert deployed_spec.query is not None, f"Query missing for {node_name}"

        # Validate required_dimensions if present
        if input_spec.required_dimensions:
            assert set(deployed_spec.required_dimensions or []) == set(
                input_spec.required_dimensions,
            ), f"Required dimensions mismatch for {node_name}"

    elif isinstance(input_spec, CubeSpec):
        # Validate metrics
        expected_metrics = [transform_value(m) for m in input_spec.metrics]
        assert set(deployed_spec.metrics) == set(expected_metrics), (
            f"Metrics mismatch for {node_name}: "
            f"expected {expected_metrics}, got {deployed_spec.metrics}"
        )

        # Validate dimensions
        expected_dims = [transform_value(d) for d in input_spec.dimensions]
        assert set(deployed_spec.dimensions) == set(expected_dims), (
            f"Dimensions mismatch for {node_name}: "
            f"expected {expected_dims}, got {deployed_spec.dimensions}"
        )


@pytest.fixture
def default_repair_orders():
    return SourceSpec(
        name="default.repair_orders",
        description="""All repair orders""",
        catalog="default",
        schema="roads",
        table="repair_orders",
        columns=[
            ColumnSpec(
                name="repair_order_id",
                type="int",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="municipality_id",
                type="string",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="hard_hat_id",
                type="int",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="order_date",
                type="timestamp",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="required_date",
                type="timestamp",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="dispatched_date",
                type="timestamp",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="dispatcher_id",
                type="int",
                display_name=None,
                description=None,
            ),
        ],
        dimension_links=[
            DimensionJoinLinkSpec(
                dimension_node="${prefix}default.repair_order",
                join_type="inner",
                join_on="${prefix}default.repair_orders.repair_order_id = ${prefix}default.repair_order.repair_order_id",
            ),
            DimensionJoinLinkSpec(
                dimension_node="${prefix}default.dispatcher",
                join_type="inner",
                join_on="${prefix}default.repair_orders.dispatcher_id = ${prefix}default.dispatcher.dispatcher_id",
            ),
        ],
        owners=["dj"],
    )


@pytest.fixture
def default_repair_orders_view():
    return SourceSpec(
        name="default.repair_orders_view",
        description="""All repair orders (view)""",
        query="""CREATE OR REPLACE VIEW roads.repair_orders_view AS SELECT * FROM roads.repair_orders""",
        catalog="default",
        schema="roads",
        table="repair_orders_view",
        columns=[
            ColumnSpec(
                name="repair_order_id",
                type="int",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="municipality_id",
                type="string",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="hard_hat_id",
                type="int",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="order_date",
                type="timestamp",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="required_date",
                type="timestamp",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="dispatched_date",
                type="timestamp",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="dispatcher_id",
                type="int",
                display_name=None,
                description=None,
            ),
        ],
        dimension_links=[],
        owners=["dj"],
    )


@pytest.fixture
def default_repair_order_details():
    return SourceSpec(
        name="default.repair_order_details",
        description="""Details on repair orders""",
        catalog="default",
        schema="roads",
        table="repair_order_details",
        columns=[
            ColumnSpec(
                name="repair_order_id",
                type="int",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="repair_type_id",
                type="int",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="price",
                type="float",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="quantity",
                type="int",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="discount",
                type="float",
                display_name=None,
                description=None,
            ),
        ],
        dimension_links=[
            DimensionJoinLinkSpec(
                dimension_node="${prefix}default.repair_order",
                join_type=JoinType.INNER,
                join_on="${prefix}default.repair_order_details.repair_order_id = ${prefix}default.repair_order.repair_order_id",
            ),
        ],
        owners=["dj"],
    )


@pytest.fixture
def default_repair_type():
    return SourceSpec(
        name="default.repair_type",
        description="""Information on types of repairs""",
        catalog="default",
        schema="roads",
        table="repair_type",
        columns=[
            ColumnSpec(
                name="repair_type_id",
                type="int",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="repair_type_name",
                type="string",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="contractor_id",
                type="int",
                display_name=None,
                description=None,
            ),
        ],
        dimension_links=[
            DimensionJoinLinkSpec(
                dimension_node="${prefix}default.contractor",
                join_type=JoinType.INNER,
                join_on="${prefix}default.repair_type.contractor_id = ${prefix}default.contractor.contractor_id",
            ),
        ],
        owners=["dj"],
    )


@pytest.fixture
def default_contractors():
    return SourceSpec(
        name="default.contractors",
        description="""Information on contractors""",
        catalog="default",
        schema="roads",
        table="contractors",
        columns=[
            ColumnSpec(
                name="contractor_id",
                type="int",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="company_name",
                type="string",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="contact_name",
                type="string",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="contact_title",
                type="string",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="address",
                type="string",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="city",
                type="string",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="state",
                type="string",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="postal_code",
                type="string",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="country",
                type="string",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="phone",
                type="string",
                display_name=None,
                description=None,
            ),
        ],
        dimension_links=[
            DimensionJoinLinkSpec(
                dimension_node="${prefix}default.us_state",
                join_type=JoinType.INNER,
                join_on="${prefix}default.contractors.state = ${prefix}default.us_state.state_short",
            ),
        ],
        owners=["dj"],
    )


@pytest.fixture
def default_municipality_municipality_type():
    return SourceSpec(
        name="default.municipality_municipality_type",
        description="""Lookup table for municipality and municipality types""",
        catalog="default",
        schema="roads",
        table="municipality_municipality_type",
        columns=[
            ColumnSpec(
                name="municipality_id",
                type="string",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="municipality_type_id",
                type="string",
                display_name=None,
                description=None,
            ),
        ],
        dimension_links=[],
        owners=["dj"],
    )


@pytest.fixture
def default_municipality_type():
    return SourceSpec(
        name="default.municipality_type",
        description="""Information on municipality types""",
        catalog="default",
        schema="roads",
        table="municipality_type",
        columns=[
            ColumnSpec(
                name="municipality_type_id",
                type="string",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="municipality_type_desc",
                type="string",
                display_name=None,
                description=None,
            ),
        ],
        dimension_links=[],
        owners=["dj"],
    )


@pytest.fixture
def default_municipality():
    return SourceSpec(
        name="default.municipality",
        description="""Information on municipalities""",
        catalog="default",
        schema="roads",
        table="municipality",
        columns=[
            ColumnSpec(
                name="municipality_id",
                type="string",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="contact_name",
                type="string",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="contact_title",
                type="string",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="local_region",
                type="string",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="phone",
                type="string",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="state_id",
                type="int",
                display_name=None,
                description=None,
            ),
        ],
        dimension_links=[],
        owners=["dj"],
    )


@pytest.fixture
def default_dispatchers():
    return SourceSpec(
        name="default.dispatchers",
        description="""Information on dispatchers""",
        catalog="default",
        schema="roads",
        table="dispatchers",
        columns=[
            ColumnSpec(
                name="dispatcher_id",
                type="int",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="company_name",
                type="string",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="phone",
                type="string",
                display_name=None,
                description=None,
            ),
        ],
        dimension_links=[],
        owners=["dj"],
    )


@pytest.fixture
def default_hard_hats():
    return SourceSpec(
        name="default.hard_hats",
        description="""Information on employees""",
        catalog="default",
        schema="roads",
        table="hard_hats",
        columns=[
            ColumnSpec(
                name="hard_hat_id",
                type="int",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="last_name",
                type="string",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="first_name",
                type="string",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="title",
                type="string",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="birth_date",
                type="timestamp",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="hire_date",
                type="timestamp",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="address",
                type="string",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="city",
                type="string",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="state",
                type="string",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="postal_code",
                type="string",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="country",
                type="string",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="manager",
                type="int",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="contractor_id",
                type="int",
                display_name=None,
                description=None,
            ),
        ],
        dimension_links=[],
        owners=["dj"],
    )


@pytest.fixture
def default_hard_hat_state():
    return SourceSpec(
        name="default.hard_hat_state",
        description="""Lookup table for employee's current state""",
        catalog="default",
        schema="roads",
        table="hard_hat_state",
        columns=[
            ColumnSpec(
                name="hard_hat_id",
                type="int",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="state_id",
                type="string",
                display_name=None,
                description=None,
            ),
        ],
        dimension_links=[],
        owners=["dj"],
    )


@pytest.fixture
def default_us_states():
    return SourceSpec(
        name="default.us_states",
        description="""Information on different types of repairs""",
        catalog="default",
        schema="roads",
        table="us_states",
        columns=[
            ColumnSpec(
                name="state_id",
                type="int",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="state_name",
                type="string",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="state_abbr",
                type="string",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="state_region",
                type="int",
                display_name=None,
                description=None,
            ),
        ],
        dimension_links=[],
        owners=["dj"],
    )


@pytest.fixture
def default_us_region():
    return SourceSpec(
        name="default.us_region",
        description="""Information on US regions""",
        catalog="default",
        schema="roads",
        table="us_region",
        columns=[
            ColumnSpec(
                name="us_region_id",
                type="int",
                display_name=None,
                description=None,
            ),
            ColumnSpec(
                name="us_region_description",
                type="string",
                display_name=None,
                description=None,
            ),
        ],
        dimension_links=[],
        owners=["dj"],
    )


@pytest.fixture
def default_repair_order():
    return DimensionSpec(
        name="default.repair_order",
        description="""Repair order dimension""",
        query="""
                        SELECT
                        repair_order_id,
                        municipality_id,
                        hard_hat_id,
                        order_date,
                        required_date,
                        dispatched_date,
                        dispatcher_id
                        FROM ${prefix}default.repair_orders
                    """,
        primary_key=["repair_order_id"],
        dimension_links=[
            DimensionJoinLinkSpec(
                dimension_node="${prefix}default.dispatcher",
                join_type="inner",
                join_on="${prefix}default.repair_order.dispatcher_id = ${prefix}default.dispatcher.dispatcher_id",
            ),
            DimensionJoinLinkSpec(
                dimension_node="${prefix}default.municipality_dim",
                join_type="inner",
                join_on="${prefix}default.repair_order.municipality_id = ${prefix}default.municipality_dim.municipality_id",
            ),
            DimensionJoinLinkSpec(
                dimension_node="${prefix}default.hard_hat",
                join_type="inner",
                join_on="${prefix}default.repair_order.hard_hat_id = ${prefix}default.hard_hat.hard_hat_id",
            ),
        ],
        owners=["dj"],
    )


@pytest.fixture
def default_contractor():
    return DimensionSpec(
        name="default.contractor",
        description="""Contractor dimension""",
        query="""
                        SELECT
                        contractor_id,
                        company_name,
                        contact_name,
                        contact_title,
                        address,
                        city,
                        state,
                        postal_code,
                        country,
                        phone
                        FROM ${prefix}default.contractors
                    """,
        primary_key=["contractor_id"],
        dimension_links=[],
        owners=["dj"],
    )


@pytest.fixture
def default_hard_hat():
    return DimensionSpec(
        name="default.hard_hat",
        description="""Hard hat dimension""",
        query="""
                        SELECT
                        hard_hat_id,
                        last_name,
                        first_name,
                        title,
                        birth_date,
                        hire_date,
                        address,
                        city,
                        state,
                        postal_code,
                        country,
                        manager,
                        contractor_id
                        FROM ${prefix}default.hard_hats
                    """,
        primary_key=["hard_hat_id"],
        dimension_links=[
            DimensionJoinLinkSpec(
                dimension_node="${prefix}default.us_state",
                join_type="inner",
                join_on="${prefix}default.hard_hat.state = ${prefix}default.us_state.state_short",
            ),
        ],
        owners=["dj"],
    )


@pytest.fixture
def default_us_state():
    return DimensionSpec(
        name="default.us_state",
        display_name="US State",
        description="""US state dimension""",
        query="""
                        SELECT
                        state_id,
                        state_name,
                        state_abbr AS state_short,
                        state_region
                        FROM ${prefix}default.us_states s
                    """,
        primary_key=["state_short"],
        dimension_links=[],
        owners=["dj"],
    )


@pytest.fixture
def default_dispatcher():
    return DimensionSpec(
        name="default.dispatcher",
        description="""Dispatcher dimension""",
        query="""
                        SELECT
                        dispatcher_id,
                        company_name,
                        phone
                        FROM ${prefix}default.dispatchers
                    """,
        primary_key=["dispatcher_id"],
        dimension_links=[],
        owners=["dj"],
    )


@pytest.fixture
def default_municipality_dim():
    return DimensionSpec(
        name="default.municipality_dim",
        description="""Municipality dimension""",
        query="""
                        SELECT
                        m.municipality_id AS municipality_id,
                        contact_name,
                        contact_title,
                        local_region,
                        state_id,
                        mmt.municipality_type_id AS municipality_type_id,
                        mt.municipality_type_desc AS municipality_type_desc
                        FROM ${prefix}default.municipality AS m
                        LEFT JOIN ${prefix}default.municipality_municipality_type AS mmt
                        ON m.municipality_id = mmt.municipality_id
                        LEFT JOIN ${prefix}default.municipality_type AS mt
                        ON mmt.municipality_type_id = mt.municipality_type_desc
                    """,
        primary_key=["municipality_id"],
        dimension_links=[],
        owners=["dj"],
    )


@pytest.fixture
def default_regional_level_agg():
    return TransformSpec(
        name="default.regional_level_agg",
        description="""Regional-level aggregates""",
        query="""
WITH ro as (SELECT
        repair_order_id,
        municipality_id,
        hard_hat_id,
        order_date,
        required_date,
        dispatched_date,
        dispatcher_id
    FROM ${prefix}default.repair_orders)
            SELECT
    usr.us_region_id,
    us.state_name,
    CONCAT(us.state_name, '-', usr.us_region_description) AS location_hierarchy,
    EXTRACT(YEAR FROM ro.order_date) AS order_year,
    EXTRACT(MONTH FROM ro.order_date) AS order_month,
    EXTRACT(DAY FROM ro.order_date) AS order_day,
    COUNT(DISTINCT CASE WHEN ro.dispatched_date IS NOT NULL THEN ro.repair_order_id ELSE NULL END) AS completed_repairs,
    COUNT(DISTINCT ro.repair_order_id) AS total_repairs_dispatched,
    SUM(rd.price * rd.quantity) AS total_amount_in_region,
    AVG(rd.price * rd.quantity) AS avg_repair_amount_in_region,
    -- ELEMENT_AT(ARRAY_SORT(COLLECT_LIST(STRUCT(COUNT(*) AS cnt, rt.repair_type_name AS repair_type_name)), (left, right) -> case when left.cnt < right.cnt then 1 when left.cnt > right.cnt then -1 else 0 end), 0).repair_type_name AS most_common_repair_type,
    AVG(DATEDIFF(ro.dispatched_date, ro.order_date)) AS avg_dispatch_delay,
    COUNT(DISTINCT c.contractor_id) AS unique_contractors
FROM ro
JOIN
    ${prefix}default.municipality m ON ro.municipality_id = m.municipality_id
JOIN
    ${prefix}default.us_states us ON m.state_id = us.state_id
                         AND AVG(rd.price * rd.quantity) >
                            (SELECT AVG(price * quantity) FROM ${prefix}default.repair_order_details WHERE repair_order_id = ro.repair_order_id)
JOIN
    ${prefix}default.us_states us ON m.state_id = us.state_id
JOIN
    ${prefix}default.us_region usr ON us.state_region = usr.us_region_id
JOIN
    ${prefix}default.repair_order_details rd ON ro.repair_order_id = rd.repair_order_id
JOIN
    ${prefix}default.repair_type rt ON rd.repair_type_id = rt.repair_type_id
JOIN
    ${prefix}default.contractors c ON rt.contractor_id = c.contractor_id
GROUP BY
    usr.us_region_id,
    EXTRACT(YEAR FROM ro.order_date),
    EXTRACT(MONTH FROM ro.order_date),
    EXTRACT(DAY FROM ro.order_date)""",
        primary_key=[
            "us_region_id",
            "state_name",
            "order_year",
            "order_month",
            "order_day",
        ],
        dimension_links=[],
        owners=["dj"],
    )


@pytest.fixture
def default_national_level_agg():
    return TransformSpec(
        name="default.national_level_agg",
        description="""National level aggregates""",
        query="""SELECT SUM(rd.price * rd.quantity) AS total_amount_nationwide FROM ${prefix}default.repair_order_details rd""",
        dimension_links=[],
        owners=["dj"],
    )


@pytest.fixture
def default_repair_orders_fact():
    return TransformSpec(
        name="default.repair_orders_fact",
        description="""Fact transform with all details on repair orders""",
        query="""SELECT
  repair_orders.repair_order_id,
  repair_orders.municipality_id,
  repair_orders.hard_hat_id,
  repair_orders.dispatcher_id,
  repair_orders.order_date,
  repair_orders.dispatched_date,
  repair_orders.required_date,
  repair_order_details.discount,
  repair_order_details.price,
  repair_order_details.quantity,
  repair_order_details.repair_type_id,
  repair_order_details.price * repair_order_details.quantity AS total_repair_cost,
  repair_orders.dispatched_date - repair_orders.order_date AS time_to_dispatch,
  repair_orders.dispatched_date - repair_orders.required_date AS dispatch_delay
FROM
  ${prefix}default.repair_orders repair_orders
JOIN
  ${prefix}default.repair_order_details repair_order_details
ON repair_orders.repair_order_id = repair_order_details.repair_order_id""",
        dimension_links=[
            DimensionJoinLinkSpec(
                dimension_node="${prefix}default.municipality_dim",
                join_type="inner",
                join_on="${prefix}default.repair_orders_fact.municipality_id = ${prefix}default.municipality_dim.municipality_id",
            ),
            DimensionJoinLinkSpec(
                dimension_node="${prefix}default.hard_hat",
                join_type="inner",
                join_on="${prefix}default.repair_orders_fact.hard_hat_id = ${prefix}default.hard_hat.hard_hat_id",
            ),
            DimensionJoinLinkSpec(
                dimension_node="${prefix}default.dispatcher",
                join_type="inner",
                join_on="${prefix}default.repair_orders_fact.dispatcher_id = ${prefix}default.dispatcher.dispatcher_id",
            ),
        ],
        owners=["dj"],
    )


@pytest.fixture
def default_regional_repair_efficiency():
    return MetricSpec(
        name="default.regional_repair_efficiency",
        description="""For each US region (as defined in the us_region table), we want to calculate:
            Regional Repair Efficiency = (Number of Completed Repairs / Total Repairs Dispatched) ×
                                         (Total Repair Amount in Region / Total Repair Amount Nationwide) × 100
            Here:
                A "Completed Repair" is one where the dispatched_date is not null.
                "Total Repair Amount in Region" is the total amount spent on repairs in a given region.
                "Total Repair Amount Nationwide" is the total amount spent on all repairs nationwide.""",
        query="""SELECT
    (SUM(rm.completed_repairs) * 1.0 / NULLIF(SUM(rm.total_repairs_dispatched), 0)) *
    (SUM(rm.total_amount_in_region) * 1.0 / NULLIF(SUM(na.total_amount_nationwide), 0)) * 100
FROM
    ${prefix}default.regional_level_agg rm
CROSS JOIN
    ${prefix}default.national_level_agg na""",
        dimension_links=[],
        owners=["dj"],
    )


@pytest.fixture
def default_num_repair_orders():
    return MetricSpec(
        name="default.num_repair_orders",
        description="""Number of repair orders""",
        query="""SELECT count(repair_order_id) FROM ${prefix}default.repair_orders_fact""",
        dimension_links=[],
        owners=["dj"],
    )


@pytest.fixture
def default_avg_repair_price():
    return MetricSpec(
        name="default.avg_repair_price",
        description="""Average repair price""",
        query="""SELECT avg(repair_orders_fact.price) FROM ${prefix}default.repair_orders_fact repair_orders_fact""",
        dimension_links=[],
        owners=["dj"],
    )


@pytest.fixture
def default_total_repair_cost():
    return MetricSpec(
        name="default.total_repair_cost",
        description="""Total repair cost""",
        query="""SELECT sum(total_repair_cost) FROM ${prefix}default.repair_orders_fact""",
        dimension_links=[],
        owners=["dj"],
    )


@pytest.fixture
def default_avg_length_of_employment():
    return MetricSpec(
        name="default.avg_length_of_employment",
        description="""Average length of employment""",
        query="""SELECT avg(CAST(NOW() AS DATE) - hire_date) FROM ${prefix}default.hard_hat""",
        dimension_links=[],
        owners=["dj"],
        required_dimensions=["hard_hat_id"],
    )


@pytest.fixture
def default_discounted_orders_rate():
    return MetricSpec(
        name="default.discounted_orders_rate",
        description="""Proportion of Discounted Orders""",
        query="""
                SELECT
                  cast(sum(if(discount > 0.0, 1, 0)) as double) / count(*)
                    AS default_DOT_discounted_orders_rate
                FROM ${prefix}default.repair_orders_fact
                """,
        dimension_links=[],
        owners=["dj"],
        direction=MetricDirection.HIGHER_IS_BETTER,
        unit=MetricUnit.PROPORTION,
    )


@pytest.fixture
def default_total_repair_order_discounts():
    return MetricSpec(
        name="default.total_repair_order_discounts",
        description="""Total repair order discounts""",
        query="""SELECT sum(price * discount) FROM ${prefix}default.repair_orders_fact""",
        dimension_links=[],
        owners=["dj"],
    )


@pytest.fixture
def default_avg_repair_order_discounts():
    return MetricSpec(
        name="default.avg_repair_order_discounts",
        description="""Average repair order discounts""",
        query="""SELECT avg(price * discount) FROM ${prefix}default.repair_orders_fact""",
        dimension_links=[],
        owners=["dj"],
    )


@pytest.fixture
def default_avg_time_to_dispatch():
    return MetricSpec(
        name="default.avg_time_to_dispatch",
        description="""Average time to dispatch a repair order""",
        query="""SELECT avg(cast(repair_orders_fact.time_to_dispatch as int)) FROM ${prefix}default.repair_orders_fact repair_orders_fact""",
        dimension_links=[],
        owners=["dj"],
    )


@pytest.fixture
def default_repairs_cube():
    return CubeSpec(
        name="default.repairs_cube",
        display_name="Repairs Cube",
        description="""Cube for analyzing repair orders""",
        dimensions=[
            "${prefix}default.hard_hat.state",
            "${prefix}default.dispatcher.company_name",
            "${prefix}default.municipality_dim.local_region",
        ],
        metrics=[
            "${prefix}default.num_repair_orders",
            "${prefix}default.avg_repair_price",
            "${prefix}default.total_repair_cost",
        ],
        owners=["dj"],
    )


@pytest.fixture
def roads_nodes(
    default_repair_orders,
    default_repair_orders_view,
    default_repair_order_details,
    default_repair_type,
    default_contractors,
    default_municipality_municipality_type,
    default_municipality_type,
    default_municipality,
    default_dispatchers,
    default_hard_hats,
    default_hard_hat_state,
    default_us_states,
    default_us_region,
    default_repair_order,
    default_contractor,
    default_hard_hat,
    default_us_state,
    default_dispatcher,
    default_municipality_dim,
    default_regional_level_agg,
    default_national_level_agg,
    default_repair_orders_fact,
    default_regional_repair_efficiency,
    default_num_repair_orders,
    default_avg_repair_price,
    default_total_repair_cost,
    default_avg_length_of_employment,
    default_discounted_orders_rate,
    default_total_repair_order_discounts,
    default_avg_repair_order_discounts,
    default_avg_time_to_dispatch,
    default_repairs_cube,
):
    return [
        default_repair_orders,
        default_repair_orders_view,
        default_repair_order_details,
        default_repair_type,
        default_contractors,
        default_municipality_municipality_type,
        default_municipality_type,
        default_municipality,
        default_dispatchers,
        default_hard_hats,
        default_hard_hat_state,
        default_us_states,
        default_us_region,
        default_repair_order,
        default_contractor,
        default_hard_hat,
        default_us_state,
        default_dispatcher,
        default_municipality_dim,
        default_regional_level_agg,
        default_national_level_agg,
        default_repair_orders_fact,
        default_regional_repair_efficiency,
        default_num_repair_orders,
        default_avg_repair_price,
        default_total_repair_cost,
        default_avg_length_of_employment,
        default_discounted_orders_rate,
        default_total_repair_order_discounts,
        default_avg_repair_order_discounts,
        default_avg_time_to_dispatch,
        default_repairs_cube,
    ]


@pytest.fixture
def mock_qs(client):
    """
    A query service that accepts everything a cube materialization asks of it,
    installed for the duration of the test.

    Plain `MagicMock` rather than a spec'd double so `method_calls` records the whole
    sequence in order, which is what the materialization tests below compare against.
    """
    query_service = MagicMock()
    materialized = MaterializationInfo(
        urls=["http://fake.url/job"],
        output_tables=[],
    )
    query_service.materialize_cube.return_value = materialized
    query_service.materialize.return_value = materialized
    query_service.run_cube_backfill.return_value = {
        "job_url": "http://fake.url/backfill",
    }
    client.app.dependency_overrides[get_query_service_client] = lambda: query_service
    yield query_service
    del client.app.dependency_overrides[get_query_service_client]


def deployment_payload(deployment_spec: DeploymentSpec) -> dict:
    """
    The spec as a serializing caller would send it.

    Deliberately a plain `model_dump`, which emits every optional field explicitly
    null. Nothing in the deployment path may read a key's mere presence as intent --
    `materialization: none` is a value for exactly that reason -- so posting the
    lossiest payload shape keeps every test below honest about it.
    """
    return deployment_spec.model_dump()


async def deploy_and_wait(client, deployment_spec: DeploymentSpec):
    response = await client.post(
        "/deployments",
        json=deployment_payload(deployment_spec),
    )
    data = response.json()
    deployment_uuid = data["uuid"]
    while data["status"] not in (
        DeploymentStatus.FAILED.value,
        DeploymentStatus.SUCCESS.value,
    ):
        await asyncio.sleep(1)
        response = await client.get(f"/deployments/{deployment_uuid}")
        data = response.json()
    return data


@pytest.mark.xdist_group(name="deployments")
class TestDeploymentAuthorization:
    @pytest.mark.asyncio
    async def test_deploy_delete_denied_without_delete_is_fail_closed(
        self,
        client,
        default_hard_hats,
        mocker,
    ):
        """
        A deploy runs detached and can DELETE nodes/namespaces, but the HTTP
        entrypoint only checks WRITE on the root namespace -- and WRITE does not
        imply DELETE. So a WRITE-but-not-DELETE caller passes the entrypoint yet
        must be denied by the orchestrator's internal DELETE authorization; the
        deletion fails and the node survives (fail-closed).
        """
        namespace = "deploy_delete_denied"

        # First deploy (default passthrough auth) creates the node.
        created = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=[default_hard_hats]),
        )
        assert created["status"] == "success", created

        # Now deny DELETE (WRITE still allowed, so the HTTP entrypoint passes) and
        # try to delete everything via an empty spec.
        mocker.patch(
            VALIDATOR_AUTH_SERVICE,
            deny(access.ResourceAction.DELETE),
        )
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=[], allow_empty=True),
        )

        assert data["status"] == DeploymentStatus.FAILED.value, data
        assert "Access denied" in json.dumps(data["results"])

        # Fail-closed: the node was not deleted.
        node_check = await client.get(f"/nodes/{namespace}.default.hard_hats/")
        assert node_check.status_code == 200


@pytest.mark.xdist_group(name="deployments")
class TestDeployments:
    @pytest.mark.asyncio
    async def test_deploy_failed_on_non_existent_upstream_deps(
        self,
        client,
        default_hard_hat,
        default_hard_hats,
    ):
        """
        Test deployment with non-existent upstream dependencies.

        The new behavior (vs raising DJInvalidDeploymentConfig) is to proceed
        with deployment and mark affected nodes as INVALID, so the caller
        can see exactly which nodes failed and why.
        """
        namespace = "missing_upstreams"
        data = await deploy_and_wait(
            client,
            DeploymentSpec(
                namespace=namespace,
                nodes=[default_hard_hat],
            ),
        )
        assert data["uuid"] == mock.ANY
        assert data["namespace"] == namespace
        assert data["status"] == "failed"
        assert data["downstream_impacts"] == []
        # hard_hat node deployed as INVALID (missing hard_hats upstream)
        node_result = next(r for r in data["results"] if r["deploy_type"] == "node")
        assert node_result["name"] == f"{namespace}.default.hard_hat"
        assert node_result["status"] == "invalid"
        assert node_result["operation"] == "create"
        # link to us_state failed (node doesn't exist)
        link_result = next(r for r in data["results"] if r["deploy_type"] == "link")
        assert f"{namespace}.default.us_state" in link_result["name"]
        assert link_result["status"] == "failed"

    @pytest.mark.asyncio
    async def test_deploy_failed_on_non_existent_link_deps(
        self,
        client,
        default_hard_hat,
        default_hard_hats,
    ):
        """
        Test deployment failures for a node that has a dimension link to a node that doesn't exist
        """
        namespace = "missing_dimension_node"
        data = await deploy_and_wait(
            client,
            DeploymentSpec(
                namespace=namespace,
                nodes=[default_hard_hats, default_hard_hat],
            ),
        )
        # New behavior: proceed with deployment — the node deploys but the link fails
        assert data["uuid"] == mock.ANY
        assert data["namespace"] == namespace
        assert data["status"] == "failed"
        assert data["downstream_impacts"] == []
        link_result = next(r for r in data["results"] if r["deploy_type"] == "link")
        assert f"{namespace}.default.us_state" in link_result["name"]
        assert link_result["status"] == "failed"

    @pytest.mark.asyncio
    async def test_deploy_failed_with_bad_node_spec_pk(
        self,
        client,
        default_hard_hats,
        default_us_states,
        default_us_state,
    ):
        """
        Test deployment failures with bad node specifications (primary key that doesn't exist in the query)
        """
        bad_dim_spec = DimensionSpec(
            name="default.hard_hat",
            description="""Hard hat dimension""",
            query="""SELECT last_name, first_name FROM ${prefix}default.hard_hats""",
            primary_key=["hard_hat_id"],
            owners=["dj"],
            dimension_links=[
                DimensionJoinLinkSpec(
                    dimension_node="${prefix}default.us_state",
                    join_type="inner",
                    join_on="${prefix}default.hard_hat.state = ${prefix}default.us_state.state_short",
                ),
            ],
        )
        namespace = "bad_node_spec"
        data = await deploy_and_wait(
            client,
            DeploymentSpec(
                namespace=namespace,
                nodes=[
                    bad_dim_spec,
                    default_hard_hats,
                    default_us_states,
                    default_us_state,
                ],
            ),
        )
        # Node is INVALID due to PK error; link validation error is reported
        # separately during link deployment (not duplicated in node message)
        assert data["uuid"] == mock.ANY
        assert data["namespace"] == namespace

        node_results = {
            r["name"]: r for r in data["results"] if r["deploy_type"] == "node"
        }
        link_results = {
            r["name"]: r for r in data["results"] if r["deploy_type"] == "link"
        }

        # Sources created successfully
        assert node_results[f"{namespace}.default.hard_hats"]["status"] == "success"
        assert node_results[f"{namespace}.default.us_states"]["status"] == "success"
        assert node_results[f"{namespace}.default.us_state"]["status"] == "success"

        # Dimension with bad PK is INVALID
        hard_hat = node_results[f"{namespace}.default.hard_hat"]
        assert hard_hat["status"] == "invalid"
        assert "primary key ['hard_hat_id']" in hard_hat["message"]

        # Link is created but warns about INVALID node
        link = link_results[
            f"{namespace}.default.hard_hat -> {namespace}.default.us_state"
        ]
        assert link["status"] == "success"
        assert "INVALID" in link["message"]

    @pytest.mark.asyncio
    async def test_deploy_with_dimension_link_removal(
        self,
        session,
        client,
        default_hard_hats,
        default_us_states,
        default_us_state,
    ):
        """
        Test that removing a dimension link from a node works as expected
        """
        namespace = "link_removal"
        dim_spec = DimensionSpec(
            name="default.hard_hat",
            description="""Hard hat dimension""",
            query="""
            SELECT
                hard_hat_id,
                state
            FROM ${prefix}default.hard_hats
            """,
            primary_key=["hard_hat_id"],
            owners=["dj"],
            dimension_links=[
                DimensionJoinLinkSpec(
                    dimension_node="${prefix}default.us_state",
                    join_type="inner",
                    join_on="${prefix}default.hard_hat.state = ${prefix}default.us_state.state_short",
                ),
            ],
        )
        nodes_list = [dim_spec, default_hard_hats, default_us_states, default_us_state]
        data = await deploy_and_wait(
            client,
            DeploymentSpec(
                namespace=namespace,
                nodes=nodes_list,
            ),
        )
        assert data["status"] == "success"
        hard_hat = await Node.get_by_name(session, "link_removal.default.hard_hat")
        assert len(hard_hat.current.dimension_links) == 1

        # Remove the dimension link and redeploy
        dim_spec.dimension_links = []
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=nodes_list),
        )
        assert data["status"] == "success"
        assert data["results"][-1] == {
            "deploy_type": "link",
            "message": "",
            "name": "link_removal.default.hard_hat -> link_removal.default.us_state",
            "operation": "delete",
            "changed_fields": [],
            "status": "success",
        }

    @pytest.mark.asyncio
    async def test_deploy_with_dimension_link_update(
        self,
        session,
        client,
        default_hard_hats,
        default_us_states,
        default_us_state,
    ):
        """
        Test that updating a dimension link from a node works as expected
        """
        namespace = "link_update"
        dim_spec = DimensionSpec(
            name="default.hard_hat",
            description="""Hard hat dimension""",
            query="""
            SELECT
                hard_hat_id,
                state
            FROM ${prefix}default.hard_hats
            """,
            primary_key=["hard_hat_id"],
            owners=["dj"],
            dimension_links=[
                DimensionJoinLinkSpec(
                    dimension_node="${prefix}default.us_state",
                    join_type="inner",
                    join_on="${prefix}default.hard_hat.state = ${prefix}default.us_state.state_short",
                ),
            ],
        )
        nodes_list = [dim_spec, default_hard_hats, default_us_states, default_us_state]
        data = await deploy_and_wait(
            client,
            DeploymentSpec(
                namespace=namespace,
                nodes=nodes_list,
            ),
        )
        assert data["status"] == "success"
        hard_hat = await Node.get_by_name(session, "link_update.default.hard_hat")
        assert len(hard_hat.current.dimension_links) == 1

        # Update the dimension link and redeploy
        dim_spec.dimension_links = [
            DimensionJoinLinkSpec(
                dimension_node="${prefix}default.us_state",
                join_type="left",
                join_on="${prefix}default.hard_hat.state = ${prefix}default.us_state.state_short",
            ),
        ]
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=nodes_list),
        )
        assert data["status"] == "success"
        assert data["results"][-1] == {
            "deploy_type": "link",
            "message": "Join link successfully deployed",
            "name": "link_update.default.hard_hat -> link_update.default.us_state",
            "operation": "update",
            "changed_fields": [],
            "status": "success",
        }

    @pytest.mark.asyncio
    async def test_deploy_dimension_link_join_cardinality_round_trip(
        self,
        session,
        client,
        default_hard_hats,
        default_us_states,
        default_us_state,
    ):
        """
        A non-default join_cardinality set in a deployment spec must persist onto
        the DimensionLink (orchestrator threads it into the JoinLinkInput) and
        survive DimensionLink.to_spec().
        """
        namespace = "link_cardinality"
        dim_spec = DimensionSpec(
            name="default.hard_hat",
            query="SELECT hard_hat_id, state FROM ${prefix}default.hard_hats",
            primary_key=["hard_hat_id"],
            owners=["dj"],
            dimension_links=[
                # one_to_many: a worker can be assigned across several states, so
                # one hard_hat row matches many us_state rows (a fan-out link).
                DimensionJoinLinkSpec(
                    dimension_node="${prefix}default.us_state",
                    join_type="inner",
                    join_on="${prefix}default.hard_hat.state = ${prefix}default.us_state.state_short",
                    join_cardinality=JoinCardinality.ONE_TO_MANY,
                ),
            ],
        )
        data = await deploy_and_wait(
            client,
            DeploymentSpec(
                namespace=namespace,
                nodes=[
                    dim_spec,
                    default_hard_hats,
                    default_us_states,
                    default_us_state,
                ],
            ),
        )
        assert data["status"] == "success"

        hard_hat = await Node.get_by_name(session, "link_cardinality.default.hard_hat")
        (link,) = hard_hat.current.dimension_links
        # Persisted onto the link, and preserved by to_spec().
        assert link.join_cardinality == JoinCardinality.ONE_TO_MANY
        assert link.to_spec().join_cardinality == JoinCardinality.ONE_TO_MANY

        # Cardinality is what makes this link fan out at query time, so the history
        # entry has to record it alongside the join it describes.
        response = await client.get(
            "/history?node=link_cardinality.default.hard_hat",
        )
        assert response.status_code == 200, response.json()
        link_events = [
            event for event in response.json() if event["entity_type"] == "link"
        ]
        assert link_events, response.json()
        assert link_events[0]["details"]["join_cardinality"] == "one_to_many"

    @pytest.mark.asyncio
    async def test_deploy_with_reference_dimension_link(
        self,
        client,
        default_hard_hats,
        default_us_states,
        default_us_state,
    ):
        """
        Test that removing a dimension link from a node works as expected
        """
        namespace = "reference_link"
        dim_spec = DimensionSpec(
            name="default.hard_hat",
            description="""Hard hat dimension""",
            query="""
            SELECT
                hard_hat_id,
                state
            FROM ${prefix}default.hard_hats
            """,
            primary_key=["hard_hat_id"],
            owners=["dj"],
            dimension_links=[
                DimensionReferenceLinkSpec(
                    node_column="state",
                    dimension="${prefix}default.us_state.state_short",
                ),
            ],
        )
        nodes_list = [dim_spec, default_hard_hats, default_us_states, default_us_state]
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=nodes_list),
        )
        assert data["status"] == "success"
        assert data["results"][-1] == {
            "deploy_type": "link",
            "message": "Reference link successfully deployed",
            "name": "reference_link.default.hard_hat -> reference_link.default.us_state",
            "operation": "create",
            "changed_fields": [],
            "status": "success",
        }
        dim_spec.dimension_links = [
            DimensionReferenceLinkSpec(
                node_column="state",
                dimension="${prefix}default.us_state.random",
            ),
        ]
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=nodes_list),
        )
        assert data["status"] == "failed"
        assert any(
            "Dimension attribute 'random' not found in dimension" in r["message"]
            or "INVALID" in r["message"]
            for r in data["results"]
        )

    @pytest.mark.asyncio
    async def test_required_dimension_from_linked_dimension_roundtrips(
        self,
        client,
        default_hard_hats,
        default_us_states,
        default_us_state,
    ):
        """
        A metric whose required dimension lives on a dimension linked to its
        upstream transform must survive a pull-from-A / push-to-B round trip.

        Regression test for the bug where such a required dimension exported as
        its bare column name, which then failed to resolve against the metric's
        parents in the target namespace ("references to columns as required
        dimensions that are not on parent nodes").
        """
        # A transform that carries a dimension link to us_state, and a metric on
        # that transform whose required dimension is a *us_state* column — i.e. a
        # dimension elsewhere on the graph, not a direct column of the parent.
        hard_hat_facts = TransformSpec(
            name="default.hard_hat_facts",
            node_type=NodeType.TRANSFORM,
            query="SELECT hard_hat_id, state FROM ${prefix}default.hard_hats",
            dimension_links=[
                DimensionJoinLinkSpec(
                    dimension_node="${prefix}default.us_state",
                    join_type="inner",
                    join_on=(
                        "${prefix}default.hard_hat_facts.state = "
                        "${prefix}default.us_state.state_short"
                    ),
                ),
            ],
            owners=["dj"],
        )
        # Two required dims on the SAME linked dimension node — exercises the
        # dedup when adding deploy-ordering edges (the node is only added once).
        num_hard_hats = MetricSpec(
            name="default.num_hard_hats",
            node_type=NodeType.METRIC,
            query="SELECT COUNT(*) FROM ${prefix}default.hard_hat_facts",
            required_dimensions=[
                "${prefix}default.us_state.state_name",
                "${prefix}default.us_state.state_region",
            ],
            owners=["dj"],
        )
        nodes = [
            default_hard_hats,
            default_us_states,
            default_us_state,
            hard_hat_facts,
            num_hard_hats,
        ]

        # Deploy to namespace A.
        data_a = await deploy_and_wait(
            client,
            DeploymentSpec(namespace="rt_a", nodes=nodes),
        )
        assert data_a["status"] == "success", data_a["results"]

        # Export A: the required dimension is on a linked dimension (not a parent
        # column), so it must export as a ${prefix}-parameterized full path.
        export_a = (await client.get("/namespaces/rt_a/export/spec")).json()["nodes"]
        metric_a = next(
            spec
            for spec in export_a
            if spec["name"] == "${prefix}default.num_hard_hats"
        )
        assert metric_a["required_dimensions"] == [
            "${prefix}default.us_state.state_name",
            "${prefix}default.us_state.state_region",
        ]

        # Push the exported specs to namespace B — this is what failed before.
        deployment_b = DeploymentSpec.model_validate(
            {"namespace": "rt_b", "nodes": export_a},
        )
        data_b = await deploy_and_wait(client, deployment_b)
        assert data_b["status"] == "success", data_b["results"]

        # The required dimension survived the round trip and re-bound in B.
        export_b = (await client.get("/namespaces/rt_b/export/spec")).json()["nodes"]
        metric_b = next(
            spec
            for spec in export_b
            if spec["name"] == "${prefix}default.num_hard_hats"
        )
        assert metric_b["required_dimensions"] == [
            "${prefix}default.us_state.state_name",
            "${prefix}default.us_state.state_region",
        ]

    @pytest.mark.asyncio
    async def test_deploy_reconciles_external_preaggregation(
        self,
        client,
        default_hard_hats,
        default_us_states,
        default_us_state,
    ):
        """
        An externally-built pre-aggregation declared in the deployment spec is
        registered at deploy time and reconciled away when dropped from the spec.
        """
        hard_hat_facts = TransformSpec(
            name="default.hard_hat_facts",
            node_type=NodeType.TRANSFORM,
            query="SELECT hard_hat_id, state FROM ${prefix}default.hard_hats",
            dimension_links=[
                DimensionJoinLinkSpec(
                    dimension_node="${prefix}default.us_state",
                    join_type="inner",
                    join_on=(
                        "${prefix}default.hard_hat_facts.state = "
                        "${prefix}default.us_state.state_short"
                    ),
                ),
            ],
            owners=["dj"],
        )
        count_hard_hats = MetricSpec(
            name="default.count_hard_hats",
            node_type=NodeType.METRIC,
            query="SELECT COUNT(*) FROM ${prefix}default.hard_hat_facts",
            owners=["dj"],
        )
        nodes = [
            default_hard_hats,
            default_us_states,
            default_us_state,
            hard_hat_facts,
            count_hard_hats,
        ]

        async def _fake_columns(*args, **kwargs):
            return [
                SimpleNamespace(name="hard_hat_count", type="bigint"),
                SimpleNamespace(name="state_name", type="string"),
            ]

        mock_qs = MagicMock()
        mock_qs.get_columns_for_table = _fake_columns
        client.app.dependency_overrides[get_query_service_client] = lambda: mock_qs
        fact_node = "preagg_deploy.default.hard_hat_facts"
        try:
            preagg_spec = PreAggSpec(
                name="hard_hats_by_state",
                metrics={"${prefix}default.count_hard_hats": "hard_hat_count"},
                dimensions={"${prefix}default.us_state.state_name": "state_name"},
                catalog="default",
                schema="analytics",
                table="hard_hats_agg",
                valid_through_ts=1700000000,
            )
            data = await deploy_and_wait(
                client,
                DeploymentSpec(
                    namespace="preagg_deploy",
                    nodes=nodes,
                    preaggregations=[preagg_spec],
                ),
            )
            assert data["status"] == "success", data["results"]

            # The external pre-agg was registered against the fact node.
            listing = await client.get("/preaggs/", params={"node_name": fact_node})
            assert listing.status_code == 200, listing.text
            items = listing.json()["items"]
            assert len(items) == 1
            preagg = items[0]
            assert preagg["name"] == "preagg_deploy.hard_hats_by_state"
            assert preagg["strategy"] == "external"
            assert any(
                measure["source_column"] == "hard_hat_count"
                for measure in preagg["measures"]
            )

            # Re-deploy without the preaggregation -> declaring none never
            # mass-deregisters, so allow_empty is required to reconcile it away.
            data = await deploy_and_wait(
                client,
                DeploymentSpec(
                    namespace="preagg_deploy",
                    nodes=nodes,
                    allow_empty=True,
                ),
            )
            assert data["status"] == "success", data["results"]
            listing = await client.get("/preaggs/", params={"node_name": fact_node})
            assert listing.json()["items"] == []
        finally:
            del client.app.dependency_overrides[get_query_service_client]

    @pytest.mark.asyncio
    async def test_deploy_preagg_applies_dimension_columns(
        self,
        client,
        default_hard_hats,
        default_us_states,
        default_us_state,
    ):
        """
        A pre-agg declared in a deployment spec with ``dimension_columns`` binds
        each grain dimension to its physical column, and the generated measures
        SQL reads that physical column (aliased back to the DJ name) instead of
        the DJ dimension name.

        Regression: the ``dimension_columns`` feature was only covered through the
        POST /preaggs/register API path; the deployment/orchestrator path (what
        ``dj push`` uses) had no coverage. This exercises the two shapes that path
        must get right: a joined dimension's *key* satisfied by a differently
        named parent foreign-key column (``dim.state_short`` via ``fact.state``),
        and a *local* fact column (``fact.hard_hat_id``).
        """
        fact = TransformSpec(
            name="default.dimcol_facts",
            node_type=NodeType.TRANSFORM,
            query="SELECT hard_hat_id, state FROM ${prefix}default.hard_hats",
            dimension_links=[
                DimensionJoinLinkSpec(
                    dimension_node="${prefix}default.us_state",
                    join_type="inner",
                    join_on=(
                        "${prefix}default.dimcol_facts.state = "
                        "${prefix}default.us_state.state_short"
                    ),
                ),
            ],
            primary_key=["hard_hat_id"],
            owners=["dj"],
        )
        metric = MetricSpec(
            name="default.dimcol_count",
            node_type=NodeType.METRIC,
            query="SELECT COUNT(*) FROM ${prefix}default.dimcol_facts",
            owners=["dj"],
        )

        async def _fake_columns(*args, **kwargs):
            return [
                SimpleNamespace(name="cnt", type="bigint"),
                SimpleNamespace(name="st_code", type="string"),
                SimpleNamespace(name="hh_id_col", type="int"),
            ]

        mock_qs = MagicMock()
        mock_qs.get_columns_for_table = _fake_columns
        client.app.dependency_overrides[get_query_service_client] = lambda: mock_qs
        try:
            preagg = PreAggSpec(
                name="dimcol_agg",
                metrics={"${prefix}default.dimcol_count": "cnt"},
                dimensions={
                    "${prefix}default.us_state.state_short": "st_code",
                    "${prefix}default.dimcol_facts.hard_hat_id": "hh_id_col",
                },
                catalog="default",
                schema="analytics",
                table="dimcol_agg_tbl",
                valid_through_ts=1700000000,
            )
            data = await deploy_and_wait(
                client,
                DeploymentSpec(
                    namespace="preagg_dimcol",
                    nodes=[
                        default_hard_hats,
                        default_us_states,
                        default_us_state,
                        fact,
                        metric,
                    ],
                    preaggregations=[preagg],
                ),
            )
            assert data["status"] == "success", data["results"]

            # The registered dimensions carry their physical source_column binding.
            listing = await client.get(
                "/preaggs/",
                params={"node_name": "preagg_dimcol.default.dimcol_facts"},
            )
            preagg_row = listing.json()["items"][0]
            source_cols = {
                col["source_column"]
                for col in preagg_row["columns"]
                if col.get("semantic_type") == "dimension"
            }
            assert source_cols == {"st_code", "hh_id_col"}, preagg_row["columns"]

            # The measures SQL reads the physical columns from the agg table and
            # re-aggregates over them -- no join back to the dimension node.
            response = await client.get(
                "/sql/measures/v3/",
                params={
                    "metrics": ["preagg_dimcol.default.dimcol_count"],
                    "dimensions": [
                        "preagg_dimcol.default.us_state.state_short",
                        "preagg_dimcol.default.dimcol_facts.hard_hat_id",
                    ],
                },
            )
            assert response.status_code == 200, response.text
            grain_sql = response.json()["grain_groups"][0]["sql"]
            # Reads the mapped physical columns (st_code, hh_id_col) from the agg
            # table, aliased to the DJ dimension aliases (state_short,
            # hard_hat_id) the metrics layer references; groups by the physical
            # columns; no join back to the dimension node.
            assert_sql_equal(
                grain_sql,
                """
                SELECT st_code state_short,
                       hh_id_col hard_hat_id,
                       SUM(cnt) cnt
                FROM default.analytics.dimcol_agg_tbl
                GROUP BY st_code, hh_id_col
                """,
            )

            # The outer metrics query references the SAME aliases the CTE exposes
            # (state_short, hard_hat_id), so inner/outer aliasing is consistent.
            metrics_response = await client.get(
                "/sql/metrics/v3/",
                params={
                    "metrics": ["preagg_dimcol.default.dimcol_count"],
                    "dimensions": [
                        "preagg_dimcol.default.us_state.state_short",
                        "preagg_dimcol.default.dimcol_facts.hard_hat_id",
                    ],
                },
            )
            assert metrics_response.status_code == 200, metrics_response.text
            assert_sql_equal(
                metrics_response.json()["sql"],
                """
                WITH dimcol_facts_0 AS (
                    SELECT st_code state_short,
                           hh_id_col hard_hat_id,
                           SUM(cnt) cnt
                    FROM default.analytics.dimcol_agg_tbl
                    GROUP BY st_code, hh_id_col
                )
                SELECT dimcol_facts_0.state_short AS state_short,
                       dimcol_facts_0.hard_hat_id AS hard_hat_id,
                       SUM(dimcol_facts_0.cnt) AS dimcol_count
                FROM dimcol_facts_0
                GROUP BY dimcol_facts_0.state_short, dimcol_facts_0.hard_hat_id
                """,
            )
        finally:
            del client.app.dependency_overrides[get_query_service_client]

    @pytest.mark.asyncio
    async def test_deploy_dimension_with_update(
        self,
        client,
        session,
        default_hard_hats,
        default_us_states,
        default_us_state,
    ):
        """
        Test that updating a dimension node's query works as expected
        """
        namespace = "node_update"
        dim_spec = DimensionSpec(
            name="default.hard_hat",
            display_name="Hard Hat",
            description="""Hard hat dimension""",
            query="""
            SELECT
                hard_hat_id,
                state
            FROM ${prefix}default.hard_hats
            """,
            primary_key=["hard_hat_id"],
            owners=["dj"],
            dimension_links=[
                DimensionJoinLinkSpec(
                    dimension_node="${prefix}default.us_state",
                    join_type="inner",
                    join_on="${prefix}default.hard_hat.state = ${prefix}default.us_state.state_short",
                ),
            ],
        )
        nodes_list = [
            dim_spec,
            default_hard_hats,
            default_us_states,
            default_us_state,
        ]
        data = await deploy_and_wait(
            client,
            DeploymentSpec(
                namespace=namespace,
                nodes=nodes_list,
            ),
        )
        assert data["status"] == "success"
        assert len(data["results"]) == 5

        node = await Node.get_by_name(session, f"{namespace}.default.hard_hat")
        assert [col.name for col in node.current.primary_key()] == ["hard_hat_id"]

        data = await deploy_and_wait(
            client,
            DeploymentSpec(
                namespace=namespace,
                nodes=nodes_list,
            ),
        )
        assert all(res["status"] == "skipped" for res in data["results"])

        dim_spec.query = """
        SELECT
            hard_hat_id,
            state,
            first_name,
            last_name
        FROM ${prefix}default.hard_hats
        """
        nodes_list = [dim_spec, default_hard_hats, default_us_states, default_us_state]
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=nodes_list),
        )
        assert data["status"] == "success"
        assert len(data["results"]) == 5
        assert len([res for res in data["results"] if res["status"] == "skipped"]) == 4
        update_hard_hat = next(
            res
            for res in data["results"]
            if res["name"] == "node_update.default.hard_hat"
        )
        assert update_hard_hat == {
            "deploy_type": "node",
            "name": f"{namespace}.default.hard_hat",
            "status": "success",
            "operation": "update",
            "changed_fields": ["query", "columns"],
            "message": "Updated dimension (v2.0)\n└─ Column removed: hard_hat_id, state\n└─ Updated query, columns",
        }
        update_us_state = next(
            res
            for res in data["results"]
            if res["name"] == "node_update.default.us_state"
        )
        assert update_us_state == {
            "deploy_type": "node",
            "message": "Unchanged",
            "name": "node_update.default.us_state",
            "operation": "noop",
            "changed_fields": [],
            "status": "skipped",
        }

    @pytest.mark.asyncio
    async def test_deploy_metric_with_update(
        self,
        client,
        default_hard_hats,
        default_hard_hat,
        default_us_states,
        default_us_state,
        default_avg_length_of_employment,
    ):
        """
        Test that updating a metric node's works as expected
        """
        namespace = "metric_update"
        nodes_list = [
            default_hard_hats,
            default_hard_hat,
            default_us_states,
            default_us_state,
            default_avg_length_of_employment,
        ]
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=nodes_list),
        )
        assert data["status"] == "success"

        # Bad query - metric should fail to deploy
        default_avg_length_of_employment.query = """
        SELECT hard_hat_id FROM ${prefix}default.hard_hat
        """
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=nodes_list),
        )
        assert data["status"] == "success"
        metric_result = next(
            res
            for res in data["results"]
            if res["name"] == "metric_update.default.avg_length_of_employment"
        )
        assert metric_result == {
            "deploy_type": "node",
            "message": "Updated metric (v2.0)\n└─ Updated query, display_name\n"
            "[invalid] Metric metric_update.default.avg_length_of_employment has an invalid "
            "query, should have an aggregate expression",
            "name": "metric_update.default.avg_length_of_employment",
            "operation": "update",
            "changed_fields": ["query", "display_name"],
            "status": "invalid",
        }

        # Fix query - metric should deploy successfully
        default_avg_length_of_employment.query = """
        SELECT COUNT(hard_hat_id) FROM ${prefix}default.hard_hat
        """
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=nodes_list),
        )
        assert data["status"] == "success"
        metric_result = next(
            res
            for res in data["results"]
            if res["name"] == "metric_update.default.avg_length_of_employment"
        )
        assert metric_result == {
            "deploy_type": "node",
            "message": "Updated metric (v3.0)\n└─ Updated query, display_name",
            "name": "metric_update.default.avg_length_of_employment",
            "operation": "update",
            "changed_fields": ["query", "display_name"],
            "status": "success",
        }

    @pytest.mark.asyncio
    async def test_deploy_populates_derived_expression_and_measures(
        self,
        session,
        client,
        default_hard_hats,
        default_hard_hat,
        default_us_states,
        default_us_state,
        default_avg_length_of_employment,
    ):
        """
        Deploying a metric must populate NodeRevision.derived_expression and
        the associated FrozenMeasure rows inline (not via background task) so
        the deployment result is atomically consistent: when the deployment
        reports success, measure derivation has already happened.

        Regression test for the pre-cutover gap where single-node create used
        a FastAPI BackgroundTask but bulk deployment had no equivalent path,
        leaving metrics with derived_expression = NULL after deployment.
        """
        from sqlalchemy.orm import joinedload, selectinload

        from datajunction_server.database.node import Node, NodeRevision

        namespace = "derive_measures"
        data = await deploy_and_wait(
            client,
            DeploymentSpec(
                namespace=namespace,
                nodes=[
                    default_hard_hats,
                    default_hard_hat,
                    default_us_states,
                    default_us_state,
                    default_avg_length_of_employment,
                ],
            ),
        )
        assert data["status"] == "success"

        metric = await Node.get_by_name(
            session,
            f"{namespace}.default.avg_length_of_employment",
            options=[
                joinedload(Node.current).options(
                    selectinload(NodeRevision.frozen_measures),
                ),
            ],
        )
        assert metric is not None
        assert metric.current is not None
        assert metric.current.derived_expression is not None, (
            "derived_expression should be populated inline by the orchestrator — "
            "a NULL value means the bulk-deployment path is skipping derivation."
        )
        assert len(metric.current.frozen_measures) > 0, (
            "at least one FrozenMeasure row should be linked after deployment"
        )

    @pytest.mark.asyncio
    async def test_deploy_cube_with_update(
        self,
        client,
        default_hard_hats,
        default_hard_hat,
        default_us_states,
        default_us_state,
        default_avg_length_of_employment,
    ):
        """
        Test that updating a cube node's works as expected
        """
        namespace = "cube_update"
        cube = CubeSpec(
            name="default.repairs_cube",
            display_name="Repairs Cube",
            description="""Cube for analyzing repair orders""",
            dimensions=[
                "${prefix}default.hard_hat.state",
            ],
            metrics=[
                "${prefix}default.avg_length_of_employment",
            ],
            owners=["dj"],
        )
        nodes_list = [
            default_hard_hats,
            default_hard_hat,
            default_us_states,
            default_us_state,
            default_avg_length_of_employment,
            cube,
        ]
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=nodes_list),
        )
        assert data["status"] == "success"

        # Update cube to have a bad dimension - cube should fail to deploy
        cube.dimensions = [
            "${prefix}default.hard_hat.state",
            "${prefix}default.us_state.state_region",
            "${prefix}default.us_state.non_existent_column",
        ]
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=nodes_list),
        )
        assert data["status"] == "success"
        assert data["results"][-1] == {
            "deploy_type": "node",
            "message": "Updated cube (v2.0)\n"
            "└─ Updated dimensions\n"
            "[invalid] One or more dimensions not found for cube "
            "cube_update.default.repairs_cube: cube_update.default.us_state.non_existent_column",
            "name": "cube_update.default.repairs_cube",
            "operation": "update",
            "changed_fields": ["dimensions"],
            "status": "invalid",
        }

        # Remove the bad dimension — the cube is still INVALID from the previous
        # deploy, so it gets re-deployed and should now succeed.
        cube.dimensions = [
            "${prefix}default.hard_hat.state",
            "${prefix}default.us_state.state_region",
        ]
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=nodes_list),
        )
        assert data["status"] == "success"
        cube_result = data["results"][-1]
        assert cube_result["name"] == "cube_update.default.repairs_cube"
        assert cube_result["deploy_type"] == "node"
        assert cube_result["operation"] == "update"
        assert cube_result["status"] == "success"

    @pytest.mark.asyncio
    async def test_deploy_cube_metadata_only_change_is_a_minor_bump(
        self,
        client,
        default_hard_hats,
        default_hard_hat,
        default_us_states,
        default_us_state,
        default_avg_length_of_employment,
    ):
        """
        A description-only edit re-deployed must earn a minor version, not a new
        major one. Deploying such an edit used to mint v2.0 because every node that
        reached the update path was given `next_major_version()` unconditionally.
        """
        namespace = "cube_metadata_only"
        cube = CubeSpec(
            name="default.repairs_cube",
            display_name="Repairs Cube",
            description="""Cube for analyzing repair orders""",
            dimensions=["${prefix}default.hard_hat.state"],
            metrics=["${prefix}default.avg_length_of_employment"],
            owners=["dj"],
        )
        nodes_list = [
            default_hard_hats,
            default_hard_hat,
            default_us_states,
            default_us_state,
            default_avg_length_of_employment,
            cube,
        ]
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=nodes_list),
        )
        assert data["status"] == "success"
        fetched = (await client.get(f"/nodes/{namespace}.default.repairs_cube/")).json()
        assert fetched["version"] == "v1.0"

        cube.description = "Cube for analyzing repair orders, revised"
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=nodes_list),
        )
        assert data["status"] == "success"
        assert data["results"][-1] == {
            "deploy_type": "node",
            "message": "Updated cube (v1.1)\n└─ Updated description",
            "name": f"{namespace}.default.repairs_cube",
            "operation": "update",
            "changed_fields": ["description"],
            "status": "success",
        }
        fetched = (await client.get(f"/nodes/{namespace}.default.repairs_cube/")).json()
        assert fetched["version"] == "v1.1"
        assert fetched["description"] == "Cube for analyzing repair orders, revised"

    @pytest.mark.asyncio
    async def test_deploy_cube_partition_change_is_major(
        self,
        client,
        default_hard_hats,
        default_hard_hat,
        default_us_states,
        default_us_state,
        default_avg_length_of_employment,
    ):
        """
        A cube's partition config lives on its columns, which `diff()` does not
        compare, so a partition-only edit would otherwise reach the update path with
        nothing named as changed. It is reported as `columns` and earns a major
        version: the partition decides how the cube is built and materialized.
        """
        namespace = "cube_partition_change"
        cube = CubeSpec(
            name="default.repairs_cube",
            display_name="Repairs Cube",
            description="""Cube for analyzing repair orders""",
            dimensions=[
                "${prefix}default.hard_hat.state",
                "${prefix}default.hard_hat.birth_date",
            ],
            metrics=["${prefix}default.avg_length_of_employment"],
            columns=[
                ColumnSpec(
                    name="${prefix}default.hard_hat.birth_date",
                    partition={
                        "type": "temporal",
                        "granularity": "day",
                        "format": "yyyyMMdd",
                    },
                ),
            ],
            owners=["dj"],
        )
        nodes_list = [
            default_hard_hats,
            default_hard_hat,
            default_us_states,
            default_us_state,
            default_avg_length_of_employment,
            cube,
        ]
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=nodes_list),
        )
        assert data["status"] == "success"
        fetched = (await client.get(f"/nodes/{namespace}.default.repairs_cube/")).json()
        assert fetched["version"] == "v1.0"

        cube.columns[0].partition.format = "yyyy-MM-dd"
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=nodes_list),
        )
        assert data["status"] == "success"
        assert [
            result
            for result in data["results"]
            if result["name"] == f"{namespace}.default.repairs_cube"
        ] == [
            {
                "deploy_type": "node",
                "message": (
                    "Updated cube (v2.0)\n"
                    "└─ Set properties for 1 columns\n"
                    "└─ Updated columns"
                ),
                "name": f"{namespace}.default.repairs_cube",
                "operation": "update",
                "changed_fields": ["columns"],
                "status": "success",
            },
        ]
        fetched = (await client.get(f"/nodes/{namespace}.default.repairs_cube/")).json()
        assert fetched["version"] == "v2.0"

    @pytest.mark.asyncio
    async def test_deploy_cube_dimension_reorder(
        self,
        client,
        default_hard_hats,
        default_hard_hat,
        default_us_states,
        default_us_state,
        default_avg_length_of_employment,
    ):
        """
        Reordering a cube's dimensions in YAML is a real change: the ordering sets
        the cube's column order. It used to be invisible to the deployment path,
        which compared dimensions as sets and so skipped the node entirely; now it
        earns a minor version and the new order persists. Reordering the filters at
        the same time changes nothing, because filters are ANDed.
        """
        namespace = "cube_dimension_reorder"
        dimensions = [
            "${prefix}default.hard_hat.state",
            "${prefix}default.hard_hat.city",
        ]
        filters = [
            "${prefix}default.hard_hat.state='AZ'",
            "${prefix}default.hard_hat.city='Phoenix'",
        ]
        cube = CubeSpec(
            name="default.repairs_cube",
            display_name="Repairs Cube",
            description="""Cube for analyzing repair orders""",
            dimensions=dimensions,
            metrics=["${prefix}default.avg_length_of_employment"],
            filters=filters,
            owners=["dj"],
        )
        nodes_list = [
            default_hard_hats,
            default_hard_hat,
            default_us_states,
            default_us_state,
            default_avg_length_of_employment,
            cube,
        ]
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=nodes_list),
        )
        assert data["status"] == "success"

        cube_url = f"/cubes/{namespace}.default.repairs_cube/"
        assert (await client.get(cube_url)).json()["cube_node_dimensions"] == [
            f"{namespace}.default.hard_hat.state",
            f"{namespace}.default.hard_hat.city",
        ]

        cube.dimensions = list(reversed(dimensions))
        cube.filters = list(reversed(filters))
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=nodes_list),
        )
        assert data["status"] == "success"
        assert data["results"][-1] == {
            "deploy_type": "node",
            "message": "Updated cube (v1.1)\n└─ Reordered dimensions",
            "name": f"{namespace}.default.repairs_cube",
            "operation": "update",
            "changed_fields": [],
            "status": "success",
        }
        fetched = (await client.get(cube_url)).json()
        assert fetched["version"] == "v1.1"
        assert fetched["cube_node_dimensions"] == [
            f"{namespace}.default.hard_hat.city",
            f"{namespace}.default.hard_hat.state",
        ]

    @pytest.mark.asyncio
    async def test_patch_and_deployment_agree_on_an_upstream_change(
        self,
        client,
        default_hard_hats,
        default_hard_hat,
        default_us_states,
        default_us_state,
        default_avg_length_of_employment,
    ):
        """
        A change to a node the cube sits on must bump the cube the same on both paths.

        The sibling of `test_patch_and_deployment_agree_on_version`, which only covers
        edits to the cube itself. Here the cube's own spec is byte-identical across the
        two deploys -- it still names the same metric and dimensions -- and only the
        metric underneath it changes. That is the case a deploy currently cannot see:
        an unchanged spec is skipped, so the cube keeps a revision compiled against the
        old definition, while the PATCH path propagates into it.
        """
        upstreams = [
            default_hard_hats,
            default_hard_hat,
            default_us_states,
            default_us_state,
        ]

        def build_metric(query: str) -> MetricSpec:
            return default_avg_length_of_employment.model_copy(
                deep=True,
                update={"query": query},
            )

        original = default_avg_length_of_employment.query
        # Same shape, different rows: the cube's elements are untouched, so nothing
        # about the cube's own spec records that this happened.
        edited = (
            "SELECT avg(IF(state = 'AZ', CAST(NOW() AS DATE) - hire_date, NULL)) "
            "FROM ${prefix}default.hard_hat"
        )

        cube = CubeSpec(
            name="default.repairs_cube",
            display_name="Repairs Cube",
            description="Cube for analyzing repair orders",
            dimensions=["${prefix}default.hard_hat.state"],
            metrics=["${prefix}default.avg_length_of_employment"],
            owners=["dj"],
        )

        async def deploy(namespace: str, metric_query: str) -> None:
            data = await deploy_and_wait(
                client,
                DeploymentSpec(
                    namespace=namespace,
                    nodes=(
                        [spec.model_copy(deep=True) for spec in upstreams]
                        + [build_metric(metric_query), cube.model_copy(deep=True)]
                    ),
                ),
            )
            assert data["status"] == "success", data

        async def version_of(namespace: str, node: str) -> str:
            response = await client.get(f"/nodes/{namespace}.default.{node}/")
            assert response.status_code == 200, response.text
            return response.json()["version"]

        patch_ns, deploy_ns = (
            "upstream_equivalence_patch",
            "upstream_equivalence_deploy",
        )
        await deploy(patch_ns, original)
        await deploy(deploy_ns, original)
        assert await version_of(patch_ns, "repairs_cube") == "v1.0"
        assert await version_of(deploy_ns, "repairs_cube") == "v1.0"

        # The same upstream edit, once through PATCH and once through a deployment.
        response = await client.patch(
            f"/nodes/{patch_ns}.default.avg_length_of_employment",
            json={"query": edited.replace("${prefix}", f"{patch_ns}.")},
        )
        assert response.status_code == 200, response.json()
        await deploy(deploy_ns, edited)

        # The metric moved on both paths -- that part already agreed.
        assert await version_of(
            patch_ns,
            "avg_length_of_employment",
        ) == await version_of(
            deploy_ns,
            "avg_length_of_employment",
        )
        # And so must the cube above it.
        assert await version_of(patch_ns, "repairs_cube") == await version_of(
            deploy_ns,
            "repairs_cube",
        )

    @pytest.mark.asyncio
    async def test_deployment_bumps_a_cube_over_a_changed_source(
        self,
        client,
        default_hard_hats,
        default_hard_hat,
        default_us_states,
        default_us_state,
        default_avg_length_of_employment,
    ):
        """
        A cube bumps for a change anywhere above it, not just in its own parents.

        Here the edited node is the source two levels down: neither the metric nor
        the dimension the cube names changes, so nothing the cube points at directly
        moved, and only walking the whole way up finds the edit. The cube lands on
        the tier the source earned rather than one of its own.
        """
        cube = CubeSpec(
            name="default.repairs_cube",
            display_name="Repairs Cube",
            description="Cube for analyzing repair orders",
            dimensions=["${prefix}default.hard_hat.state"],
            metrics=["${prefix}default.avg_length_of_employment"],
            owners=["dj"],
        )

        async def deploy(source: SourceSpec) -> None:
            data = await deploy_and_wait(
                client,
                DeploymentSpec(
                    namespace="upstream_source_deploy",
                    nodes=[
                        spec.model_copy(deep=True)
                        for spec in (
                            source,
                            default_hard_hat,
                            default_us_states,
                            default_us_state,
                            default_avg_length_of_employment,
                            cube,
                        )
                    ],
                ),
            )
            assert data["status"] == "success", data

        async def version_of(node: str) -> str:
            response = await client.get(
                f"/nodes/upstream_source_deploy.default.{node}/",
            )
            assert response.status_code == 200, response.text
            return response.json()["version"]

        await deploy(default_hard_hats)
        assert await version_of("repairs_cube") == "v1.0"

        widened = default_hard_hats.model_copy(
            deep=True,
            update={
                "columns": default_hard_hats.columns
                + [ColumnSpec(name="badge_number", type="int")],
            },
        )
        await deploy(widened)

        assert await version_of("hard_hats") == "v2.0"
        # Untouched, so they keep the revisions they had.
        assert await version_of("hard_hat") == "v1.0"
        assert await version_of("avg_length_of_employment") == "v1.0"
        # The cube alone follows the source, at the tier the source earned.
        assert await version_of("repairs_cube") == "v2.0"

    @pytest.mark.asyncio
    async def test_patch_and_deployment_agree_on_version(
        self,
        client,
        default_hard_hats,
        default_hard_hat,
        default_us_states,
        default_us_state,
        default_avg_length_of_employment,
    ):
        """
        The same edit, applied once through `PATCH /nodes/{name}` and once through a
        deployment, must land on the same version. Both paths classify significance
        through the one shared classifier, and this is the test that keeps that a
        fact rather than an aspiration.
        """
        upstreams = [
            default_hard_hats,
            default_hard_hat,
            default_us_states,
            default_us_state,
            default_avg_length_of_employment,
        ]

        def build_cube(**overrides) -> CubeSpec:
            fields: dict = {
                "name": "default.repairs_cube",
                "display_name": "Repairs Cube",
                "description": "Cube for analyzing repair orders",
                "dimensions": ["${prefix}default.hard_hat.state"],
                "metrics": ["${prefix}default.avg_length_of_employment"],
                "filters": ["${prefix}default.hard_hat.state='AZ'"],
                "owners": ["dj"],
            }
            return CubeSpec(**{**fields, **overrides})

        async def deploy(namespace: str, cube: CubeSpec) -> None:
            data = await deploy_and_wait(
                client,
                DeploymentSpec(
                    namespace=namespace,
                    nodes=[spec.model_copy(deep=True) for spec in upstreams] + [cube],
                ),
            )
            assert data["status"] == "success", data

        async def version_of(namespace: str) -> str:
            response = await client.get(f"/nodes/{namespace}.default.repairs_cube/")
            return response.json()["version"]

        patch_ns, deploy_ns = "cube_equivalence_patch", "cube_equivalence_deploy"
        await deploy(patch_ns, build_cube())
        await deploy(deploy_ns, build_cube())
        assert await version_of(patch_ns) == "v1.0"
        assert await version_of(deploy_ns) == "v1.0"

        # A metadata-only edit is minor on both paths.
        response = await client.patch(
            f"/nodes/{patch_ns}.default.repairs_cube",
            json={"description": "Cube for analyzing repair orders, revised"},
        )
        assert response.status_code == 200, response.json()
        await deploy(
            deploy_ns,
            build_cube(description="Cube for analyzing repair orders, revised"),
        )
        assert await version_of(patch_ns) == "v1.1"
        assert await version_of(deploy_ns) == "v1.1"

        # A filters-only edit is major on both paths.
        response = await client.patch(
            f"/nodes/{patch_ns}.default.repairs_cube",
            json={"filters": [f"{patch_ns}.default.hard_hat.state='CA'"]},
        )
        assert response.status_code == 200, response.json()
        await deploy(
            deploy_ns,
            build_cube(
                description="Cube for analyzing repair orders, revised",
                filters=["${prefix}default.hard_hat.state='CA'"],
            ),
        )
        assert await version_of(patch_ns) == "v2.0"
        assert await version_of(deploy_ns) == "v2.0"

    @pytest.mark.asyncio
    async def test_get_node_reads_past_a_stale_session_copy(
        self,
        client,
        session,
        default_hard_hats,
    ):
        """
        `GET /nodes/{name}/` must answer from the database once a deployment has moved
        the node on to a new revision in a session of its own.

        The test client shares one session across requests, so a node an earlier
        request left in that session's identity map is what the next request gets --
        SQLAlchemy leaves an already-loaded relationship alone no matter what load
        options the next query carries. That served a superseded revision, and where
        the earlier request had no use for `parents` it served one that could not be
        serialized at all. Holding the node here makes it deterministic; in CI it
        depended on whether the earlier request's node had been collected yet.
        """
        namespace = "stale_session_copy"

        async def deploy(**overrides) -> None:
            data = await deploy_and_wait(
                client,
                DeploymentSpec(
                    namespace=namespace,
                    nodes=[default_hard_hats.model_copy(deep=True, update=overrides)],
                ),
            )
            assert data["status"] == "success", data

        await deploy()
        name = f"{namespace}.default.hard_hats"
        assert (await client.get(f"/nodes/{name}/")).json()["version"] == "v1.0"

        # An earlier request's view of the node, kept alive in the session with its
        # revision loaded but not that revision's parents.
        held = await Node.get_by_name(
            session,
            name,
            options=[selectinload(Node.current)],
        )
        assert "parents" not in held.current.__dict__

        await deploy(description="Hard hats, revised")
        response = await client.get(f"/nodes/{name}/")
        assert response.status_code == 200, response.json()
        assert response.json()["version"] == "v2.0"
        assert response.json()["description"] == "Hard hats, revised"

    @pytest.mark.asyncio
    async def test_patch_and_deployment_agree_on_materialization_state(
        self,
        client,
        default_hard_hats,
        default_hard_hat,
        default_us_states,
        default_us_state,
        default_avg_length_of_employment,
        mock_qs,
    ):
        """
        The same edit, applied once through `PATCH /nodes/{name}` and once through a
        deployment, must leave identical materialization state: the same active
        materialization rebuilt on the new current revision, and the same query
        service calls to schedule it and stop the superseded revision's workflow.

        The materialization itself is set up through `POST
        /nodes/{name}/materialization/` in both namespaces, because that is the only
        place it can come from -- a cube materialized through the UI has no YAML. The
        temporal partition it needs is declared on the spec so that a re-deploy keeps
        it, the same way the patch path carries partitions onto a new revision.
        """
        upstreams = [
            default_hard_hats,
            default_hard_hat,
            default_us_states,
            default_us_state,
            default_avg_length_of_employment,
        ]

        def build_cube(**overrides) -> CubeSpec:
            fields: dict = {
                "name": "default.repairs_cube",
                "display_name": "Repairs Cube",
                "description": "Cube for analyzing repair orders",
                "dimensions": [
                    "${prefix}default.hard_hat.state",
                    "${prefix}default.hard_hat.hire_date",
                ],
                "metrics": ["${prefix}default.avg_length_of_employment"],
                "owners": ["dj"],
                "columns": [
                    ColumnSpec(
                        name="${prefix}default.hard_hat.hire_date",
                        partition=PartitionSpec(
                            type=PartitionType.TEMPORAL,
                            granularity=Granularity.DAY,
                            format="yyyyMMdd",
                        ),
                    ),
                ],
            }
            return CubeSpec(**{**fields, **overrides})

        async def deploy(namespace: str, cube: CubeSpec) -> None:
            data = await deploy_and_wait(
                client,
                DeploymentSpec(
                    namespace=namespace,
                    nodes=[spec.model_copy(deep=True) for spec in upstreams] + [cube],
                ),
            )
            assert data["status"] == "success", data

        async def materialization_state(namespace: str) -> tuple:
            body = (
                await client.get(f"/nodes/{namespace}.default.repairs_cube/")
            ).json()
            return (
                body["version"],
                [
                    (
                        mat["name"],
                        mat["job"],
                        mat["strategy"],
                        mat["schedule"],
                        mat["config"]["cube"]["version"],
                        mat["config"]["lookback_window"],
                        mat["deactivated_at"],
                    )
                    for mat in body["materializations"]
                ],
            )

        def query_service_calls(namespace: str) -> list:
            """
            The materialization-related query service calls, with the namespace
            stripped so the two paths' calls are directly comparable.
            """
            calls: list[tuple] = []
            for name, args, kwargs in mock_qs.method_calls:
                if name == "materialize_cube":
                    materialization = kwargs["materialization_input"]
                    calls.append(
                        (
                            name,
                            materialization.name,
                            materialization.cube.name.removeprefix(f"{namespace}."),
                            materialization.cube.version,
                        ),
                    )
                elif name in ("deactivate_cube_workflow", "deactivate_workflows"):
                    calls.append(
                        (
                            name,
                            args[0].removeprefix(f"{namespace}."),
                            kwargs.get("version"),
                        ),
                    )
            return calls

        patch_ns, deploy_ns = "mat_equivalence_patch", "mat_equivalence_deploy"
        for namespace in (patch_ns, deploy_ns):
            await deploy(namespace, build_cube())
            cube_name = f"{namespace}.default.repairs_cube"
            response = await client.post(
                f"/nodes/{cube_name}/materialization/",
                json={
                    "job": "druid_cube",
                    "strategy": "incremental_time",
                    "schedule": "@daily",
                    "lookback_window": "7 DAY",
                },
            )
            assert response.status_code == 200, response.json()

        # The same metadata-only edit through each path.
        revised = "Cube for analyzing repair orders, revised"
        mock_qs.reset_mock()
        response = await client.patch(
            f"/nodes/{patch_ns}.default.repairs_cube/",
            json={"description": revised},
        )
        assert response.status_code == 200, response.json()
        patch_calls = query_service_calls(patch_ns)

        mock_qs.reset_mock()
        await deploy(deploy_ns, build_cube(description=revised))
        deploy_calls = query_service_calls(deploy_ns)

        expected = (
            "v1.1",
            [
                (
                    "druid_cube__incremental_time__"
                    f"{patch_ns}.default.hard_hat.hire_date",
                    "DruidCubeMaterializationJob",
                    "incremental_time",
                    "@daily",
                    "v1.1",
                    "7 DAY",
                    None,
                ),
            ],
        )
        assert await materialization_state(patch_ns) == expected
        assert await materialization_state(deploy_ns) == (
            expected[0],
            [
                (
                    name.replace(patch_ns, deploy_ns),
                    *rest,
                )
                for name, *rest in expected[1]
            ],
        )
        assert patch_calls == [
            (
                "materialize_cube",
                f"druid_cube__incremental_time__{patch_ns}.default.hard_hat.hire_date",
                "default.repairs_cube",
                "v1.1",
            ),
            ("deactivate_cube_workflow", "default.repairs_cube", "v1.0"),
        ]
        assert deploy_calls == [
            (
                "materialize_cube",
                f"druid_cube__incremental_time__{deploy_ns}.default.hard_hat.hire_date",
                "default.repairs_cube",
                "v1.1",
            ),
            ("deactivate_cube_workflow", "default.repairs_cube", "v1.0"),
        ]

    @pytest.mark.asyncio
    async def test_dry_run_deploy_does_not_swap_cube_materializations(
        self,
        client,
        default_hard_hats,
        default_hard_hat,
        default_us_states,
        default_us_state,
        default_avg_length_of_employment,
        mock_qs,
    ):
        """
        A dry run must schedule nothing and stop nothing: the materialization stays on
        the revision it was built for and no query service call is made.
        """
        namespace = "mat_dry_run"
        cube = CubeSpec(
            name="default.repairs_cube",
            display_name="Repairs Cube",
            description="Cube for analyzing repair orders",
            dimensions=[
                "${prefix}default.hard_hat.state",
                "${prefix}default.hard_hat.hire_date",
            ],
            metrics=["${prefix}default.avg_length_of_employment"],
            owners=["dj"],
        )
        nodes = [
            default_hard_hats,
            default_hard_hat,
            default_us_states,
            default_us_state,
            default_avg_length_of_employment,
            cube,
        ]
        cube_name = f"{namespace}.default.repairs_cube"
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=nodes),
        )
        assert data["status"] == "success", data
        response = await client.post(
            f"/nodes/{cube_name}/columns/"
            f"{namespace}.default.hard_hat.hire_date/partition",
            json={
                "type_": "temporal",
                "granularity": "day",
                "format": "yyyyMMdd",
            },
        )
        assert response.status_code == 201, response.json()
        response = await client.post(
            f"/nodes/{cube_name}/materialization/",
            json={
                "job": "druid_cube",
                "strategy": "incremental_time",
                "schedule": "@daily",
                "lookback_window": "1 DAY",
            },
        )
        assert response.status_code == 200, response.json()

        mock_qs.reset_mock()
        cube.description = "Cube for analyzing repair orders, revised"
        response = await client.post(
            "/deployments/impact",
            json=deployment_payload(
                DeploymentSpec(namespace=namespace, nodes=nodes),
            ),
        )
        assert response.status_code == 200, response.json()
        assert mock_qs.method_calls == []

        body = (await client.get(f"/nodes/{cube_name}/")).json()
        assert body["version"] == "v1.0"
        assert [
            (mat["config"]["cube"]["version"], mat["deactivated_at"])
            for mat in body["materializations"]
        ] == [("v1.0", None)]

    @pytest.mark.asyncio
    async def test_deploy_cube_with_materialization_block(
        self,
        session,
        client,
        default_hard_hats,
        default_hard_hat,
        default_us_states,
        default_us_state,
        default_avg_length_of_employment,
        mock_qs,
    ):
        """
        A cube can declare `materialization:` alongside its definition, the block
        survives the deploy/export round trip, and it is what gets scheduled.

        The second push is the case the reconciler exists for and the one an
        implementation keyed off the deploy's change list would drop: editing only
        the schedule leaves the cube comparing equal, so it is skipped as a node --
        and still rescheduled, without earning a version.
        """
        namespace = "cube_materialization"
        cube = CubeSpec(
            name="default.repairs_cube",
            display_name="Repairs Cube",
            description="""Cube for analyzing repair orders""",
            dimensions=[
                "${prefix}default.hard_hat.state",
                "${prefix}default.hard_hat.birth_date",
            ],
            metrics=[
                "${prefix}default.avg_length_of_employment",
            ],
            columns=[
                ColumnSpec(
                    name="${prefix}default.hard_hat.birth_date",
                    partition=PartitionSpec(
                        type=PartitionType.TEMPORAL,
                        granularity=Granularity.DAY,
                        format="yyyyMMdd",
                    ),
                ),
            ],
            materialization=MaterializationSpec(
                schedule="0 6 * * *",
                lookback_window="1 DAY",
            ),
            owners=["dj"],
        )
        nodes_list = [
            default_hard_hats,
            default_hard_hat,
            default_us_states,
            default_us_state,
            default_avg_length_of_employment,
            cube,
        ]
        cube_name = f"{namespace}.default.repairs_cube"
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=nodes_list),
        )
        assert data["status"] == "success"
        assert [
            result for result in data["results"] if result["name"] == cube_name
        ] == [
            {
                "deploy_type": "node",
                "message": "Created cube (v1.0)",
                "name": cube_name,
                "operation": "create",
                "changed_fields": [],
                "status": "success",
            },
            {
                "deploy_type": "materialization",
                "message": "cube materialization on schedule 0 6 * * *",
                "name": cube_name,
                "operation": "create",
                "changed_fields": [],
                "status": "success",
            },
        ]

        session.expire_all()
        deployed = await Node.get_by_name(
            session,
            cube_name,
            options=Node.cube_load_options(),
        )
        assert deployed.current.version == "v1.0"
        assert (await deployed.to_spec(session)).materialization == (
            MaterializationSpec(
                schedule="0 6 * * *",
                strategy=MaterializationStrategy.INCREMENTAL_TIME,
                lookback_window="1 DAY",
            )
        )

        # Editing only the schedule is not a definition change: the cube compares
        # equal, is skipped rather than updated, and never earns a version. The
        # reconciler runs off the spec rather than the change list, so the new
        # schedule still reaches the query service.
        mock_qs.reset_mock()
        cube.materialization = MaterializationSpec(
            schedule="0 3 * * *",
            lookback_window="1 DAY",
        )
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=nodes_list),
        )
        assert data["status"] == "success"
        assert [
            result for result in data["results"] if result["name"] == cube_name
        ] == [
            {
                "deploy_type": "node",
                "message": "Unchanged",
                "name": cube_name,
                "operation": "noop",
                "changed_fields": [],
                "status": "skipped",
            },
            {
                "deploy_type": "materialization",
                "message": "cube materialization on schedule 0 3 * * *",
                "name": cube_name,
                "operation": "update",
                "changed_fields": [],
                "status": "success",
            },
        ]

        session.expire_all()
        deployed = await Node.get_by_name(
            session,
            cube_name,
            options=Node.cube_load_options(),
        )
        assert deployed.current.version == "v1.0"
        assert (await deployed.to_spec(session)).materialization == (
            MaterializationSpec(
                schedule="0 3 * * *",
                strategy=MaterializationStrategy.INCREMENTAL_TIME,
                lookback_window="1 DAY",
            )
        )
        assert mock_qs.materialize_cube.call_count == 1
        assert mock_qs.deactivate_workflows.call_count == 0
        assert mock_qs.deactivate_cube_workflow.call_count == 0
        scheduled = mock_qs.materialize_cube.call_args.kwargs["materialization_input"]
        assert (
            scheduled.cube.name,
            scheduled.cube.version,
            scheduled.schedule,
        ) == (
            cube_name,
            "v1.0",
            "0 3 * * *",
        )
        # A block that declares no coverage says nothing about how much history the
        # cube should hold, so there is no span to backfill.
        assert mock_qs.run_cube_backfill.call_args_list == []

    @pytest.mark.asyncio
    async def test_deploy_cube_with_declared_coverage(
        self,
        session,
        client,
        default_hard_hats,
        default_hard_hat,
        default_us_states,
        default_us_state,
        default_avg_length_of_employment,
        mock_qs,
    ):
        """
        A declared `coverage:` block survives the whole trip -- into the stored
        materialization config and back out through `to_spec` as the same YAML the
        author wrote -- under both of its forms.

        The value reaches Druid through no part of the materialization the query
        service is handed. It drives a backfill instead, which is a call of its own.
        """
        namespace = "cube_coverage"
        cube = CubeSpec(
            name="default.repairs_cube",
            display_name="Repairs Cube",
            description="""Cube for analyzing repair orders""",
            dimensions=[
                "${prefix}default.hard_hat.state",
                "${prefix}default.hard_hat.birth_date",
            ],
            metrics=["${prefix}default.avg_length_of_employment"],
            columns=[
                ColumnSpec(
                    name="${prefix}default.hard_hat.birth_date",
                    partition=PartitionSpec(
                        type=PartitionType.TEMPORAL,
                        granularity=Granularity.DAY,
                        format="yyyyMMdd",
                    ),
                ),
            ],
            materialization=MaterializationSpec(
                schedule="0 6 * * *",
                coverage={"from": "2024-01-01", "to": "2024-06-30"},
            ),
            owners=["dj"],
        )
        nodes_list = [
            default_hard_hats,
            default_hard_hat,
            default_us_states,
            default_us_state,
            default_avg_length_of_employment,
            cube,
        ]
        cube_name = f"{namespace}.default.repairs_cube"
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=nodes_list),
        )
        assert data["status"] == "success"

        session.expire_all()
        deployed = await Node.get_by_name(
            session,
            cube_name,
            options=Node.cube_load_options(),
        )
        # Stored as the authored keys, and JSON-safe -- the config column cannot
        # hold a `date`.
        assert [
            materialization.config["coverage"]
            for materialization in deployed.current.materializations
        ] == [{"from": "2024-01-01", "to": "2024-06-30", "window": None}]
        exported = await deployed.to_spec(session)
        assert exported.materialization == MaterializationSpec(
            schedule="0 6 * * *",
            strategy=MaterializationStrategy.INCREMENTAL_TIME,
            lookback_window="1 DAY",
            coverage={"from": "2024-01-01", "to": "2024-06-30"},
        )
        # What `dj pull` writes back out is the block the author wrote.
        assert exported.model_dump(mode="json", exclude_none=True)[
            "materialization"
        ] == {
            "schedule": "0 6 * * *",
            "strategy": "incremental_time",
            "lookback_window": "1 DAY",
            "retention": "400 DAYS",
            "coverage": {"from": "2024-01-01", "to": "2024-06-30"},
        }
        scheduled = mock_qs.materialize_cube.call_args.kwargs["materialization_input"]
        assert not hasattr(scheduled, "coverage")

        # The cube's first datasource is empty, so the declared span is backfilled
        # into it, and the deployment says what that costs.
        assert mock_qs.run_cube_backfill.call_args.args == (
            CubeBackfillInput(
                cube_name=cube_name,
                cube_version="v1.0",
                start_date=date(2024, 1, 1),
                end_date=date(2024, 6, 30),
            ),
        )
        assert [
            result
            for result in data["results"]
            if result["deploy_type"] == "materialization"
        ] == [
            {
                "name": cube_name,
                "deploy_type": "materialization",
                "status": "success",
                "operation": "create",
                "message": "cube materialization on schedule 0 6 * * *",
                "changed_fields": [],
            },
            {
                "name": cube_name,
                "deploy_type": "materialization",
                "status": "success",
                "operation": "create",
                "message": "backfilling 2024-01-01 to 2024-06-30, 182 partition runs",
                "changed_fields": [],
            },
        ]

        # The rolling form replaces the fixed one rather than accumulating alongside it.
        mock_qs.reset_mock()
        cube.materialization = MaterializationSpec(
            schedule="0 6 * * *",
            coverage={"window": "800 DAYS"},
        )
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=nodes_list),
        )
        assert data["status"] == "success"

        session.expire_all()
        deployed = await Node.get_by_name(
            session,
            cube_name,
            options=Node.cube_load_options(),
        )
        assert [
            materialization.config["coverage"]
            for materialization in deployed.current.materializations
        ] == [{"from": None, "to": None, "window": "800 DAYS"}]
        exported = await deployed.to_spec(session)
        assert exported.materialization == MaterializationSpec(
            schedule="0 6 * * *",
            strategy=MaterializationStrategy.INCREMENTAL_TIME,
            lookback_window="1 DAY",
            coverage={"window": "800 DAYS"},
        )
        assert exported.model_dump(mode="json", exclude_none=True)[
            "materialization"
        ] == {
            "schedule": "0 6 * * *",
            "strategy": "incremental_time",
            "lookback_window": "1 DAY",
            "retention": "400 DAYS",
            "coverage": {"window": "800 DAYS"},
        }
        # Editing the coverage of a cube that already has its datasource does not
        # refill it: the span moved, the datasource it is served from did not.
        assert mock_qs.run_cube_backfill.call_args_list == []

        # Re-declaring the same coverage changes nothing, so the reconciler leaves
        # the live workflow alone.
        mock_qs.reset_mock()
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=nodes_list),
        )
        assert data["status"] == "success"
        assert mock_qs.materialize_cube.call_count == 0
        assert mock_qs.run_cube_backfill.call_args_list == []

        # A new cube version means a datasource nothing has written to yet, so the
        # rolling window is counted back from yesterday and filled in.
        mock_qs.reset_mock()
        cube.description = "Cube for analyzing repair orders, revised"
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=nodes_list),
        )
        assert data["status"] == "success"
        yesterday = datetime.now(UTC).date() - timedelta(days=1)
        assert mock_qs.run_cube_backfill.call_args.args == (
            CubeBackfillInput(
                cube_name=cube_name,
                cube_version="v1.1",
                start_date=yesterday - timedelta(days=799),
                end_date=yesterday,
            ),
        )

        # Once the cube reports what it holds, a span declared before that is the
        # gap, and only the days ahead of the earliest one it reports are filled.
        mock_qs.reset_mock()
        session.expire_all()
        deployed = await Node.get_by_name(
            session,
            cube_name,
            options=Node.cube_load_options(),
        )
        deployed.current.availability = AvailabilityState(
            catalog="default",
            table="a_cube",
            valid_through_ts=0,
            min_temporal_partition=["20250301"],
        )
        session.add(deployed.current)
        await session.commit()
        cube.materialization = MaterializationSpec(
            schedule="0 6 * * *",
            coverage={"from": "2024-01-01"},
        )
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=nodes_list),
        )
        assert data["status"] == "success"
        assert mock_qs.run_cube_backfill.call_args.args == (
            CubeBackfillInput(
                cube_name=cube_name,
                cube_version="v1.1",
                start_date=date(2024, 1, 1),
                end_date=date(2025, 2, 28),
            ),
        )
        assert [
            result
            for result in data["results"]
            if result["message"].startswith("backfilling")
        ] == [
            {
                "name": cube_name,
                "deploy_type": "materialization",
                "status": "success",
                "operation": "create",
                "message": "backfilling 2024-01-01 to 2025-02-28, 425 partition runs",
                "changed_fields": [],
            },
        ]

        # The gap does not close until the backfill lands, so the next deploy sees
        # the same one. What DJ launched it does not ask for again.
        mock_qs.reset_mock()
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=nodes_list),
        )
        assert data["status"] == "success"
        assert mock_qs.run_cube_backfill.call_args_list == []

        # A launch the query service refuses writes nothing down, so the cube is
        # not locked out of the span: the next deploy asks for the same days.
        mock_qs.reset_mock()
        mock_qs.run_cube_backfill.side_effect = Exception("429 Too Many Requests")
        cube.materialization = MaterializationSpec(
            schedule="0 6 * * *",
            coverage={"from": "2023-06-01"},
        )
        refused = CubeBackfillInput(
            cube_name=cube_name,
            cube_version="v1.1",
            start_date=date(2023, 6, 1),
            end_date=date(2025, 2, 28),
        )
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=nodes_list),
        )
        assert data["status"] == "success"
        assert [actual.args for actual in mock_qs.run_cube_backfill.call_args_list] == [
            (refused,),
        ]

        mock_qs.reset_mock()
        mock_qs.run_cube_backfill.side_effect = None
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=nodes_list),
        )
        assert data["status"] == "success"
        assert [actual.args for actual in mock_qs.run_cube_backfill.call_args_list] == [
            (refused,),
        ]

        # And once it lands, it is not asked for a third time.
        mock_qs.reset_mock()
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=nodes_list),
        )
        assert data["status"] == "success"
        assert mock_qs.run_cube_backfill.call_args_list == []

    @pytest.mark.asyncio
    async def test_deploy_reports_a_rejected_materialization_push(
        self,
        client,
        default_hard_hats,
        default_hard_hat,
        default_us_states,
        default_us_state,
        default_avg_length_of_employment,
        mock_qs,
    ):
        """
        A query service that refuses to schedule the declared materialization fails
        the deployment, and the refusal reaches the report intact.

        The push happens after the deploy has committed, so everything else about
        the deploy still stands -- the cube is created, its DJ-side materialization
        row exists -- and until this was reported the deployment said `success` for
        a cube that is not materialized.
        """
        namespace = "cube_materialization_rejected"
        rejection = (
            "User `dj` is not an owner of data project `analytics`, so Jenkins job "
            "`analytics-repairs_cube` cannot write `prodhive.analytics.repairs_cube`"
        )
        mock_qs.materialize_cube.side_effect = Exception(rejection)
        cube = CubeSpec(
            name="default.repairs_cube",
            display_name="Repairs Cube",
            description="""Cube for analyzing repair orders""",
            dimensions=[
                "${prefix}default.hard_hat.state",
                "${prefix}default.hard_hat.birth_date",
            ],
            metrics=["${prefix}default.avg_length_of_employment"],
            columns=[
                ColumnSpec(
                    name="${prefix}default.hard_hat.birth_date",
                    partition=PartitionSpec(
                        type=PartitionType.TEMPORAL,
                        granularity=Granularity.DAY,
                        format="yyyyMMdd",
                    ),
                ),
            ],
            materialization=MaterializationSpec(
                schedule="0 6 * * *",
                lookback_window="1 DAY",
            ),
            owners=["dj"],
        )
        nodes_list = [
            default_hard_hats,
            default_hard_hat,
            default_us_states,
            default_us_state,
            default_avg_length_of_employment,
            cube,
        ]
        cube_name = f"{namespace}.default.repairs_cube"
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=nodes_list),
        )
        assert data["status"] == "failed"
        assert [
            result for result in data["results"] if result["name"] == cube_name
        ] == [
            {
                "deploy_type": "node",
                "message": "Created cube (v1.0)",
                "name": cube_name,
                "operation": "create",
                "changed_fields": [],
                "status": "success",
            },
            {
                "deploy_type": "materialization",
                "message": (
                    f"Cube `{cube_name}`: the query service rejected the request to "
                    f"schedule its materialization: {rejection}"
                ),
                "name": cube_name,
                "operation": "create",
                "changed_fields": [],
                "status": "failed",
            },
        ]

    @pytest.mark.asyncio
    async def test_deploy_cube_with_custom_metadata(
        self,
        client,
        default_hard_hats,
        default_hard_hat,
        default_us_states,
        default_us_state,
        default_avg_length_of_employment,
    ):
        """
        Cubes deployed with custom_metadata should persist that metadata,
        and updates should replace it without clobbering on unrelated changes.
        """
        namespace = "cube_custom_metadata"
        cube = CubeSpec(
            name="default.repairs_cube",
            display_name="Repairs Cube",
            description="""Cube for analyzing repair orders""",
            dimensions=["${prefix}default.hard_hat.state"],
            metrics=["${prefix}default.avg_length_of_employment"],
            custom_metadata={"owner_team": "finance", "tier": "1"},
            owners=["dj"],
        )
        nodes_list = [
            default_hard_hats,
            default_hard_hat,
            default_us_states,
            default_us_state,
            default_avg_length_of_employment,
            cube,
        ]
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=nodes_list),
        )
        assert data["status"] == "success"

        fetched = (await client.get(f"/nodes/{namespace}.default.repairs_cube/")).json()
        assert fetched["custom_metadata"] == {"owner_team": "finance", "tier": "1"}

        # Update custom_metadata and re-deploy — should persist the new value
        cube.custom_metadata = {"owner_team": "growth", "tier": "2"}
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=nodes_list),
        )
        assert data["status"] == "success"

        fetched = (await client.get(f"/nodes/{namespace}.default.repairs_cube/")).json()
        assert fetched["custom_metadata"] == {"owner_team": "growth", "tier": "2"}

    @pytest.mark.asyncio
    async def test_deploy_cube_fails_with_unreachable_dimension(
        self,
        client,
        default_hard_hats,
        default_hard_hat,
        default_us_states,
        default_us_state,
        default_dispatchers,
        default_dispatcher,
        default_avg_length_of_employment,
    ):
        """
        A cube whose dimension is not reachable from every metric should fail
        deployment with a clear 'not available on every metric' error.

        default.avg_length_of_employment queries directly from default.hard_hat,
        so only dimensions reachable from default.hard_hat are valid.
        default.dispatcher.company_name exists as a valid dimension attribute but
        has no join path to default.hard_hat, so it is unreachable.
        """
        namespace = "cube_dim_reachability"

        # Deploy with a dimension that IS reachable — should succeed
        cube = CubeSpec(
            name="default.repairs_cube_dim_check",
            display_name="Repairs Cube Dim Check",
            description="Cube for validating dimension reachability",
            dimensions=["${prefix}default.hard_hat.state"],
            metrics=["${prefix}default.avg_length_of_employment"],
            owners=["dj"],
        )
        nodes_list = [
            default_hard_hats,
            default_us_states,
            default_us_state,
            default_hard_hat,
            default_dispatchers,
            default_dispatcher,
            default_avg_length_of_employment,
            cube,
        ]
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=nodes_list),
        )
        assert data["status"] == "success"

        # Update to add a dimension that EXISTS but is NOT reachable from
        # avg_length_of_employment (dispatcher has no link to hard_hat)
        cube.dimensions = [
            "${prefix}default.hard_hat.state",
            "${prefix}default.dispatcher.company_name",
        ]
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=nodes_list),
        )
        assert data["status"] == "success"
        failed_result = next(
            r for r in data["results"] if r["status"] in ("failed", "invalid")
        )
        assert "is not reachable from parent node" in failed_result["message"]

    @pytest.mark.asyncio
    async def test_deploy_cube_filter_bad_column(
        self,
        client,
        default_hard_hats,
        default_hard_hat,
        default_us_states,
        default_us_state,
        default_avg_length_of_employment,
    ):
        """
        A cube filter referencing a nonexistent column on a reachable dimension
        should produce a clear invalid error.
        """
        namespace = "cube_filter_col"
        cube = CubeSpec(
            name="default.filter_cube",
            display_name="Filter Cube",
            description="Cube for filter column validation",
            dimensions=["${prefix}default.hard_hat.state"],
            metrics=["${prefix}default.avg_length_of_employment"],
            filters=["${prefix}default.hard_hat.nonexistent_col = 'X'"],
            owners=["dj"],
        )
        nodes_list = [
            default_hard_hats,
            default_us_states,
            default_us_state,
            default_hard_hat,
            default_avg_length_of_employment,
            cube,
        ]
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=nodes_list),
        )
        assert data["status"] == "success"
        cube_result = next(
            r
            for r in data["results"]
            if r["name"] == f"{namespace}.default.filter_cube"
        )
        assert cube_result["status"] == "invalid"
        assert "nonexistent_col" in cube_result["message"]
        assert "does not exist" in cube_result["message"]

    @pytest.mark.asyncio
    async def test_deploy_cube_filter_unreachable_dim(
        self,
        client,
        default_hard_hats,
        default_hard_hat,
        default_us_states,
        default_us_state,
        default_dispatchers,
        default_dispatcher,
        default_avg_length_of_employment,
    ):
        """
        A cube filter referencing a dimension not reachable from the cube's
        metrics should produce a clear invalid error.
        """
        namespace = "cube_filter_dim"
        cube = CubeSpec(
            name="default.filter_dim_cube",
            display_name="Filter Dim Cube",
            description="Cube for filter dimension validation",
            dimensions=["${prefix}default.hard_hat.state"],
            metrics=["${prefix}default.avg_length_of_employment"],
            filters=["${prefix}default.dispatcher.company_name = 'X'"],
            owners=["dj"],
        )
        nodes_list = [
            default_hard_hats,
            default_us_states,
            default_us_state,
            default_hard_hat,
            default_dispatchers,
            default_dispatcher,
            default_avg_length_of_employment,
            cube,
        ]
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=nodes_list),
        )
        assert data["status"] == "success"
        cube_result = next(
            r
            for r in data["results"]
            if r["name"] == f"{namespace}.default.filter_dim_cube"
        )
        assert cube_result["status"] == "invalid"
        assert "is not reachable from parent node" in cube_result["message"]

    @pytest.mark.asyncio
    async def test_deploy_failed_with_bad_node_spec_links(
        self,
        client,
        default_hard_hats,
        default_us_states,
        default_us_state,
    ):
        """
        Test deployment failures with bad node specifications (dimension link to a column that doesn't exist)
        """
        namespace = "bad_node_spec_links"
        bad_dim_spec = DimensionSpec(
            name="default.hard_hat",
            description="""Hard hat dimension""",
            query="""
            SELECT
                hard_hat_id,
                last_name,
                first_name
            FROM ${prefix}default.hard_hats
            """,
            primary_key=["hard_hat_id"],
            owners=["dj"],
            dimension_links=[
                DimensionJoinLinkSpec(
                    dimension_node="${prefix}default.us_state",
                    join_type="inner",
                    join_on="${prefix}default.hard_hat.state = ${prefix}default.us_state.state_short",
                ),
            ],
        )
        data = await deploy_and_wait(
            client,
            DeploymentSpec(
                namespace=namespace,
                nodes=[
                    bad_dim_spec,
                    default_hard_hats,
                    default_us_states,
                    default_us_state,
                ],
            ),
        )
        assert data == {
            "status": "success",
            "uuid": mock.ANY,
            "namespace": namespace,
            "results": [
                {
                    "deploy_type": "node",
                    "message": "Created source (v1.0)",
                    "name": f"{namespace}.default.hard_hats",
                    "status": "success",
                    "operation": "create",
                    "changed_fields": [],
                },
                {
                    "deploy_type": "node",
                    "message": "Created source (v1.0)",
                    "name": f"{namespace}.default.us_states",
                    "status": "success",
                    "operation": "create",
                    "changed_fields": [],
                },
                {
                    "deploy_type": "node",
                    "message": "Created dimension (v1.0)\n"
                    f"[invalid] Column 'state' referenced in join_on for "
                    f"'{namespace}.default.us_state' not found on node "
                    f"'{namespace}.default.hard_hat'",
                    "name": f"{namespace}.default.hard_hat",
                    "status": "invalid",
                    "operation": "create",
                    "changed_fields": [],
                },
                {
                    "deploy_type": "node",
                    "message": "Created dimension (v1.0)",
                    "name": f"{namespace}.default.us_state",
                    "status": "success",
                    "operation": "create",
                    "changed_fields": [],
                },
                {
                    "deploy_type": "link",
                    "message": "Join link successfully deployed\n"
                    f"[invalid] Node '{namespace}.default.hard_hat' is INVALID "
                    "— link may not function until the node is fixed",
                    "name": f"{namespace}.default.hard_hat -> {namespace}.default.us_state",
                    "operation": "create",
                    "status": "success",
                    "changed_fields": [],
                },
            ],
            "created_at": None,
            "created_by": None,
            "warnings": [],
            "downstream_impacts": [],
            "source": None,
        }

    @pytest.mark.asyncio
    async def test_deploy_succeeds_with_existing_deps(
        self,
        client,
        default_hard_hats,
        default_hard_hat,
        default_us_state,
        default_us_states,
    ):
        """
        Test that deploying with all dependencies included succeeds
        """
        namespace = "existing_deps"
        mini_setup = DeploymentSpec(
            namespace=namespace,
            nodes=[
                default_hard_hats,
                default_hard_hat,
                default_us_state,
                default_us_states,
            ],
        )
        data = await deploy_and_wait(client, mini_setup)
        assert data == {
            "status": "success",
            "uuid": mock.ANY,
            "namespace": namespace,
            "results": [
                {
                    "deploy_type": "node",
                    "message": "Created source (v1.0)",
                    "name": f"{namespace}.default.hard_hats",
                    "status": "success",
                    "operation": "create",
                    "changed_fields": [],
                },
                {
                    "deploy_type": "node",
                    "message": "Created source (v1.0)",
                    "name": f"{namespace}.default.us_states",
                    "status": "success",
                    "operation": "create",
                    "changed_fields": [],
                },
                {
                    "deploy_type": "node",
                    "message": "Created dimension (v1.0)",
                    "name": f"{namespace}.default.hard_hat",
                    "status": "success",
                    "operation": "create",
                    "changed_fields": [],
                },
                {
                    "deploy_type": "node",
                    "message": "Created dimension (v1.0)",
                    "name": f"{namespace}.default.us_state",
                    "status": "success",
                    "operation": "create",
                    "changed_fields": [],
                },
                {
                    "deploy_type": "link",
                    "message": "Join link successfully deployed",
                    "name": f"{namespace}.default.hard_hat -> {namespace}.default.us_state",
                    "status": "success",
                    "operation": "create",
                    "changed_fields": [],
                },
            ],
            "created_at": mock.ANY,
            "created_by": mock.ANY,
            "warnings": [],
            "downstream_impacts": [],
            "source": mock.ANY,
        }

        # Re-deploying the same setup should be a noop
        data = await deploy_and_wait(client, mini_setup)
        assert all(res["status"] == "skipped" for res in data["results"])
        assert all(res["operation"] == "noop" for res in data["results"])

    @pytest.mark.asyncio
    async def test_deploy_node_delete(
        self,
        client,
        default_hard_hats,
    ):
        """
        Test that removing a node from the deployment spec will result in deletion
        """
        namespace = "node_update"
        data = await deploy_and_wait(
            client,
            DeploymentSpec(
                namespace=namespace,
                nodes=[default_hard_hats],
            ),
        )
        assert data["status"] == "success"
        # Deleting every node via an empty spec is intentional here, so opt in
        # with allow_empty (the accidental-wipe guard otherwise refuses it).
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=[], allow_empty=True),
        )
        deletes = {
            (r["deploy_type"], r["name"]): r
            for r in data["results"]
            if r["operation"] == "delete"
        }
        assert deletes[("node", "node_update.default.hard_hats")] == {
            "deploy_type": "node",
            "message": "Node node_update.default.hard_hats has been removed.",
            "name": "node_update.default.hard_hats",
            "operation": "delete",
            "changed_fields": [],
            "status": "success",
        }
        # The child namespace is no longer referenced by any local node —
        # dj push syncs the namespace with the folder, so it should be
        # pruned. The deployment root namespace itself is preserved.
        assert deletes[("namespace", "node_update.default")] == {
            "deploy_type": "namespace",
            "message": "Namespace node_update.default has been removed.",
            "name": "node_update.default",
            "operation": "delete",
            "changed_fields": [],
            "status": "success",
        }
        assert ("namespace", "node_update") not in deletes

    @pytest.mark.asyncio
    async def test_deploy_prunes_nested_namespaces(
        self,
        client,
    ):
        """
        dj push should sync the namespace with the spec contents: when a
        whole sub-namespace's nodes disappear from the spec, the matching
        ``NodeNamespace`` rows are pruned too (deepest-first), but the
        deployment root namespace itself is preserved.
        """
        namespace = "ns_sync"

        def src(name: str) -> SourceSpec:
            return SourceSpec(
                name=name,
                catalog="default",
                schema="roads",
                table=name.rsplit(".", 1)[-1],
                columns=[ColumnSpec(name="id", type="int")],
            )

        data = await deploy_and_wait(
            client,
            DeploymentSpec(
                namespace=namespace,
                nodes=[src("a.deep.x"), src("b.y")],
            ),
        )
        assert data["status"] == "success"

        data = await deploy_and_wait(
            client,
            DeploymentSpec(
                namespace=namespace,
                nodes=[src("a.deep.x")],
            ),
        )
        deletes = {
            (r["deploy_type"], r["name"])
            for r in data["results"]
            if r["operation"] == "delete" and r["status"] == "success"
        }
        assert ("node", "ns_sync.b.y") in deletes
        assert ("namespace", "ns_sync.b") in deletes
        # Surviving branch and the deployment root are preserved.
        assert ("namespace", "ns_sync.a") not in deletes
        assert ("namespace", "ns_sync.a.deep") not in deletes
        assert ("namespace", "ns_sync") not in deletes

        # Verify on the server: ns_sync.b is gone, the rest remain.
        resp = await client.get("/namespaces/")
        all_namespaces = {n["namespace"] for n in resp.json()}
        assert "ns_sync" in all_namespaces
        assert "ns_sync.a" in all_namespaces
        assert "ns_sync.a.deep" in all_namespaces
        assert "ns_sync.b" not in all_namespaces

    @pytest.mark.asyncio
    async def test_deploy_namespace_prune_blocked_by_external_ref(
        self,
        client,
    ):
        """
        When a node delete is blocked because it's referenced by a node
        outside the deployment, the surrounding namespace can't be pruned
        either — surface that as a FAILED namespace result instead of
        silently dropping the row or letting the FK trip.
        """
        source = SourceSpec(
            name="drop.metric_b",
            catalog="default",
            schema="roads",
            table="metric_b",
            columns=[ColumnSpec(name="id", type="int")],
        )

        await deploy_and_wait(
            client,
            DeploymentSpec(
                namespace="blocked_sync",
                nodes=[
                    SourceSpec(
                        name="keep.metric_a",
                        catalog="default",
                        schema="roads",
                        table="metric_a",
                        columns=[ColumnSpec(name="id", type="int")],
                    ),
                    source,
                ],
            ),
        )

        # Outside deployment that depends on blocked_sync.drop.metric_b —
        # establishes the NodeRelationship that blocks deletion later.
        await deploy_and_wait(
            client,
            DeploymentSpec(
                namespace="outside_ns",
                nodes=[
                    TransformSpec(
                        name="consumer",
                        description="External consumer of blocked_sync.drop.metric_b",
                        query="SELECT id FROM blocked_sync.drop.metric_b",
                        dimension_links=[],
                        owners=["dj"],
                    ),
                ],
            ),
        )

        data = await deploy_and_wait(
            client,
            DeploymentSpec(
                namespace="blocked_sync",
                nodes=[
                    SourceSpec(
                        name="keep.metric_a",
                        catalog="default",
                        schema="roads",
                        table="metric_a",
                        columns=[ColumnSpec(name="id", type="int")],
                    ),
                ],
            ),
        )

        node_delete = next(
            r
            for r in data["results"]
            if r["deploy_type"] == "node"
            and r["name"] == "blocked_sync.drop.metric_b"
            and r["operation"] == "delete"
        )
        assert node_delete["status"] == "failed"
        assert "outside_ns.consumer" in node_delete["message"]

        ns_delete = next(
            r
            for r in data["results"]
            if r["deploy_type"] == "namespace" and r["name"] == "blocked_sync.drop"
        )
        assert ns_delete["status"] == "failed"
        assert ns_delete["operation"] == "delete"
        assert "1 node(s) remain" in ns_delete["message"]

        # Namespace row is still present on the server.
        resp = await client.get("/namespaces/")
        all_namespaces = {n["namespace"] for n in resp.json()}
        assert "blocked_sync.drop" in all_namespaces

    @pytest.mark.asyncio
    async def test_deploy_tags(
        self,
        session,
        client,
        current_user,
        default_us_states,
        default_us_state,
    ):
        """
        Test that adding tags to a node in the deployment spec will result in an update
        """
        namespace = "node_update"
        data = await deploy_and_wait(
            client,
            DeploymentSpec(
                namespace=namespace,
                nodes=[default_us_states, default_us_state],
            ),
        )
        assert data["status"] == "success"
        default_us_state.tags = ["tag1"]

        tag = Tag(name="tag1", created_by_id=current_user.id, tag_type="default")
        session.add(tag)
        await session.commit()

        data = await deploy_and_wait(
            client,
            DeploymentSpec(
                namespace=namespace,
                nodes=[default_us_states, default_us_state],
            ),
        )
        assert data["results"][-1] == {
            "deploy_type": "node",
            "message": "Updated dimension (v2.0)\n└─ Column removed: state_id, state_name, state_region, state_short\n└─ Updated tags, columns",
            "name": "node_update.default.us_state",
            "operation": "update",
            "changed_fields": ["tags", "columns"],
            "status": "success",
        }
        node = await Node.get_by_name(session, f"{namespace}.default.us_state")
        assert [tag.name for tag in node.tags] == ["tag1"]

    @pytest.mark.asyncio
    async def test_deploy_tag_metadata(
        self,
        client,
        default_us_states,
        default_us_state,
    ):
        """
        Test that tag_metadata declared on a tag in the deployment spec is
        persisted, both when the tag is created and when it is later updated.

        The deployment spec is the source of truth: the declared bag replaces
        the stored one, so keys added out-of-band or dropped from the spec do
        not linger on the server.
        """
        namespace = "tag_metadata_deploy"
        default_us_state.tags = ["inventory"]

        def spec(tag_metadata):
            return DeploymentSpec(
                namespace=namespace,
                nodes=[default_us_states, default_us_state],
                tags=[
                    TagSpec(
                        name="inventory",
                        display_name="Inventory",
                        description="Inventory tag",
                        tag_type="group",
                        tag_metadata=tag_metadata,
                    ),
                ],
            )

        # Create path
        data = await deploy_and_wait(
            client,
            spec({"order": 1, "display": {"color": "blue"}}),
        )
        assert data["status"] == "success"
        response = await client.get("/tags/inventory/")
        assert response.json() == {
            "name": "inventory",
            "display_name": "Inventory",
            "description": "Inventory tag",
            "tag_type": "group",
            "tag_metadata": {"order": 1, "display": {"color": "blue"}},
        }

        # A key is added out-of-band, outside the deployment
        response = await client.patch(
            "/tags/inventory/",
            json={
                "tag_metadata": {
                    "order": 1,
                    "display": {"color": "blue"},
                    "added_out_of_band": {"foo": "bar"},
                },
            },
        )
        assert response.status_code == 200

        # Update path: the spec replaces the stored bag, dropping the
        # out-of-band key and replacing nested contents rather than merging
        data = await deploy_and_wait(
            client,
            spec({"order": 2, "display": {"icon": "box"}}),
        )
        assert data["status"] == "success"
        response = await client.get("/tags/inventory/")
        assert response.json() == {
            "name": "inventory",
            "display_name": "Inventory",
            "description": "Inventory tag",
            "tag_type": "group",
            "tag_metadata": {"order": 2, "display": {"icon": "box"}},
        }

        # Removing tag_metadata from the spec clears it on the server
        data = await deploy_and_wait(client, spec(None))
        assert data["status"] == "success"
        response = await client.get("/tags/inventory/")
        assert response.json() == {
            "name": "inventory",
            "display_name": "Inventory",
            "description": "Inventory tag",
            "tag_type": "group",
            "tag_metadata": {},
        }

    @pytest.mark.asyncio
    async def test_deploy_column_properties(
        self,
        client,
        default_us_states,
        default_us_state,
    ):
        """
        Test that adding tags to a node in the deployment spec will result in an update
        """
        namespace = "node_update"
        data = await deploy_and_wait(
            client,
            DeploymentSpec(
                namespace=namespace,
                nodes=[default_us_states, default_us_state],
            ),
        )
        assert data["status"] == "success"

        # Update display name and description of a column
        default_us_state.columns = [
            ColumnSpec(
                name="state_name",
                type="string",
                display_name="State Name 1122",
                description="State name",
            ),
        ]
        data = await deploy_and_wait(
            client,
            DeploymentSpec(
                namespace=namespace,
                nodes=[default_us_states, default_us_state],
            ),
        )
        assert data["status"] == "success"
        assert data["results"] == [
            {
                "deploy_type": "node",
                "message": "Unchanged",
                "name": "node_update.default.us_states",
                "operation": "noop",
                "changed_fields": [],
                "status": "skipped",
            },
            {
                "deploy_type": "node",
                "message": "Updated dimension (v2.0)\n└─ Set properties for 1 columns\n"
                "└─ Column removed: state_id, state_region, state_short\n"
                "└─ Column 'state_name': display_name 'State Name' → 'State Name 1122'; description changed\n"
                "└─ Updated columns",
                "name": "node_update.default.us_state",
                "operation": "update",
                "changed_fields": ["columns"],
                "status": "success",
            },
        ]

    @pytest.mark.asyncio
    async def test_roads_deployment(self, session, client, roads_nodes):
        namespace = "base"
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=roads_nodes),
        )
        assert data == {
            "status": "success",
            "uuid": mock.ANY,
            "namespace": namespace,
            "results": [
                {
                    "deploy_type": "node",
                    "message": "Created source (v1.0)",
                    "name": f"{namespace}.default.contractors",
                    "status": "success",
                    "operation": "create",
                    "changed_fields": [],
                },
                {
                    "deploy_type": "node",
                    "message": "Created source (v1.0)",
                    "name": f"{namespace}.default.hard_hats",
                    "status": "success",
                    "operation": "create",
                    "changed_fields": [],
                },
                {
                    "deploy_type": "node",
                    "message": "Created source (v1.0)",
                    "name": f"{namespace}.default.municipality",
                    "status": "success",
                    "operation": "create",
                    "changed_fields": [],
                },
                {
                    "deploy_type": "node",
                    "message": "Created source (v1.0)",
                    "name": f"{namespace}.default.repair_order_details",
                    "status": "success",
                    "operation": "create",
                    "changed_fields": [],
                },
                {
                    "deploy_type": "node",
                    "message": "Created source (v1.0)",
                    "name": f"{namespace}.default.repair_orders",
                    "status": "success",
                    "operation": "create",
                    "changed_fields": [],
                },
                {
                    "deploy_type": "node",
                    "message": "Created source (v1.0)",
                    "name": f"{namespace}.default.repair_type",
                    "status": "success",
                    "operation": "create",
                    "changed_fields": [],
                },
                {
                    "deploy_type": "node",
                    "message": "Created source (v1.0)",
                    "name": f"{namespace}.default.us_region",
                    "status": "success",
                    "operation": "create",
                    "changed_fields": [],
                },
                {
                    "deploy_type": "node",
                    "message": "Created source (v1.0)",
                    "name": f"{namespace}.default.us_states",
                    "status": "success",
                    "operation": "create",
                    "changed_fields": [],
                },
                {
                    "deploy_type": "node",
                    "message": "Created source (v1.0)",
                    "name": f"{namespace}.default.dispatchers",
                    "status": "success",
                    "operation": "create",
                    "changed_fields": [],
                },
                {
                    "deploy_type": "node",
                    "message": "Created dimension (v1.0)",
                    "name": f"{namespace}.default.hard_hat",
                    "status": "success",
                    "operation": "create",
                    "changed_fields": [],
                },
                {
                    "deploy_type": "node",
                    "message": "Created source (v1.0)",
                    "name": f"{namespace}.default.municipality_municipality_type",
                    "status": "success",
                    "operation": "create",
                    "changed_fields": [],
                },
                {
                    "deploy_type": "node",
                    "message": "Created source (v1.0)",
                    "name": f"{namespace}.default.municipality_type",
                    "status": "success",
                    "operation": "create",
                    "changed_fields": [],
                },
                {
                    "deploy_type": "node",
                    "message": "Created transform (v1.0)",
                    "name": f"{namespace}.default.national_level_agg",
                    "status": "success",
                    "operation": "create",
                    "changed_fields": [],
                },
                {
                    "deploy_type": "node",
                    "message": "Created transform (v1.0)",
                    "name": f"{namespace}.default.regional_level_agg",
                    "status": "success",
                    "operation": "create",
                    "changed_fields": [],
                },
                {
                    "deploy_type": "node",
                    "message": "Created transform (v1.0)",
                    "name": f"{namespace}.default.repair_orders_fact",
                    "status": "success",
                    "operation": "create",
                    "changed_fields": [],
                },
                {
                    "deploy_type": "node",
                    "message": "Created metric (v1.0)",
                    "name": f"{namespace}.default.avg_length_of_employment",
                    "status": "success",
                    "operation": "create",
                    "changed_fields": [],
                },
                {
                    "deploy_type": "node",
                    "message": "Created metric (v1.0)",
                    "name": f"{namespace}.default.avg_repair_order_discounts",
                    "status": "success",
                    "operation": "create",
                    "changed_fields": [],
                },
                {
                    "deploy_type": "node",
                    "message": "Created metric (v1.0)",
                    "name": f"{namespace}.default.avg_repair_price",
                    "status": "success",
                    "operation": "create",
                    "changed_fields": [],
                },
                {
                    "deploy_type": "node",
                    "message": "Created metric (v1.0)",
                    "name": f"{namespace}.default.avg_time_to_dispatch",
                    "status": "success",
                    "operation": "create",
                    "changed_fields": [],
                },
                {
                    "deploy_type": "node",
                    "message": "Created dimension (v1.0)",
                    "name": f"{namespace}.default.contractor",
                    "status": "success",
                    "operation": "create",
                    "changed_fields": [],
                },
                {
                    "deploy_type": "node",
                    "message": "Created metric (v1.0)",
                    "name": f"{namespace}.default.discounted_orders_rate",
                    "status": "success",
                    "operation": "create",
                    "changed_fields": [],
                },
                {
                    "deploy_type": "node",
                    "message": "Created dimension (v1.0)",
                    "name": f"{namespace}.default.dispatcher",
                    "status": "success",
                    "operation": "create",
                    "changed_fields": [],
                },
                {
                    "deploy_type": "node",
                    "message": "Created source (v1.0)",
                    "name": f"{namespace}.default.hard_hat_state",
                    "status": "success",
                    "operation": "create",
                    "changed_fields": [],
                },
                {
                    "deploy_type": "node",
                    "message": "Created dimension (v1.0)",
                    "name": f"{namespace}.default.municipality_dim",
                    "status": "success",
                    "operation": "create",
                    "changed_fields": [],
                },
                {
                    "deploy_type": "node",
                    "message": "Created metric (v1.0)",
                    "name": f"{namespace}.default.num_repair_orders",
                    "status": "success",
                    "operation": "create",
                    "changed_fields": [],
                },
                {
                    "deploy_type": "node",
                    "message": "Created metric (v1.0)",
                    "name": f"{namespace}.default.regional_repair_efficiency",
                    "status": "success",
                    "operation": "create",
                    "changed_fields": [],
                },
                {
                    "deploy_type": "node",
                    "message": "Created dimension (v1.0)",
                    "name": f"{namespace}.default.repair_order",
                    "status": "success",
                    "operation": "create",
                    "changed_fields": [],
                },
                {
                    "deploy_type": "node",
                    "message": "Created source (v1.0)",
                    "name": f"{namespace}.default.repair_orders_view",
                    "status": "success",
                    "operation": "create",
                    "changed_fields": [],
                },
                {
                    "deploy_type": "node",
                    "message": "Created metric (v1.0)",
                    "name": f"{namespace}.default.total_repair_cost",
                    "status": "success",
                    "operation": "create",
                    "changed_fields": [],
                },
                {
                    "deploy_type": "node",
                    "message": "Created metric (v1.0)",
                    "name": f"{namespace}.default.total_repair_order_discounts",
                    "status": "success",
                    "operation": "create",
                    "changed_fields": [],
                },
                {
                    "deploy_type": "node",
                    "message": "Created dimension (v1.0)",
                    "name": f"{namespace}.default.us_state",
                    "status": "success",
                    "operation": "create",
                    "changed_fields": [],
                },
                {
                    "deploy_type": "link",
                    "message": "Join link successfully deployed",
                    "name": f"{namespace}.default.repair_orders -> base.default.repair_order",
                    "status": "success",
                    "operation": "create",
                    "changed_fields": [],
                },
                {
                    "deploy_type": "link",
                    "message": "Join link successfully deployed",
                    "name": f"{namespace}.default.repair_orders -> base.default.dispatcher",
                    "status": "success",
                    "operation": "create",
                    "changed_fields": [],
                },
                {
                    "deploy_type": "link",
                    "message": "Join link successfully deployed",
                    "name": f"{namespace}.default.repair_order_details -> base.default.repair_order",
                    "status": "success",
                    "operation": "create",
                    "changed_fields": [],
                },
                {
                    "deploy_type": "link",
                    "message": "Join link successfully deployed",
                    "name": f"{namespace}.default.repair_type -> base.default.contractor",
                    "status": "success",
                    "operation": "create",
                    "changed_fields": [],
                },
                {
                    "deploy_type": "link",
                    "message": "Join link successfully deployed",
                    "name": f"{namespace}.default.contractors -> base.default.us_state",
                    "status": "success",
                    "operation": "create",
                    "changed_fields": [],
                },
                {
                    "deploy_type": "link",
                    "message": "Join link successfully deployed",
                    "name": f"{namespace}.default.repair_order -> base.default.dispatcher",
                    "status": "success",
                    "operation": "create",
                    "changed_fields": [],
                },
                {
                    "deploy_type": "link",
                    "message": "Join link successfully deployed",
                    "name": f"{namespace}.default.repair_order -> base.default.municipality_dim",
                    "status": "success",
                    "operation": "create",
                    "changed_fields": [],
                },
                {
                    "deploy_type": "link",
                    "message": "Join link successfully deployed",
                    "name": f"{namespace}.default.repair_order -> base.default.hard_hat",
                    "status": "success",
                    "operation": "create",
                    "changed_fields": [],
                },
                {
                    "deploy_type": "link",
                    "message": "Join link successfully deployed",
                    "name": f"{namespace}.default.hard_hat -> base.default.us_state",
                    "status": "success",
                    "operation": "create",
                    "changed_fields": [],
                },
                {
                    "deploy_type": "link",
                    "message": "Join link successfully deployed",
                    "name": f"{namespace}.default.repair_orders_fact -> base.default.municipality_dim",
                    "status": "success",
                    "operation": "create",
                    "changed_fields": [],
                },
                {
                    "deploy_type": "link",
                    "message": "Join link successfully deployed",
                    "name": f"{namespace}.default.repair_orders_fact -> base.default.hard_hat",
                    "status": "success",
                    "operation": "create",
                    "changed_fields": [],
                },
                {
                    "deploy_type": "link",
                    "message": "Join link successfully deployed",
                    "name": f"{namespace}.default.repair_orders_fact -> base.default.dispatcher",
                    "status": "success",
                    "operation": "create",
                    "changed_fields": [],
                },
                {
                    "deploy_type": "node",
                    "message": "Created cube (v1.0)",
                    "name": f"{namespace}.default.repairs_cube",
                    "status": "success",
                    "operation": "create",
                    "changed_fields": [],
                },
            ],
            "created_at": mock.ANY,
            "created_by": mock.ANY,
            "warnings": [],
            "downstream_impacts": [],
            "source": mock.ANY,
        }

        response = await client.get("/nodes?prefix=base")
        data = response.json()
        assert len(data) == len(roads_nodes)

        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace="base", nodes=roads_nodes),
        )
        assert all(res["status"] == "skipped" for res in data["results"])
        assert all(res["operation"] == "noop" for res in data["results"])

        # Comprehensive spec validation: verify deployed nodes match input specs
        # Select a representative sample covering all node types
        nodes_to_validate = [
            # (input_spec, deployed_name, node_type)
            ("default.repair_orders", f"{namespace}.default.repair_orders", "source"),
            ("default.hard_hat", f"{namespace}.default.hard_hat", "dimension"),
            (
                "default.repair_orders_fact",
                f"{namespace}.default.repair_orders_fact",
                "transform",
            ),
            (
                "default.num_repair_orders",
                f"{namespace}.default.num_repair_orders",
                "metric",
            ),
            ("default.repairs_cube", f"{namespace}.default.repairs_cube", "cube"),
        ]

        # Build lookup of input specs by name
        input_specs_by_name = {spec.name: spec for spec in roads_nodes}

        for input_name, deployed_name, node_type in nodes_to_validate:
            input_spec = input_specs_by_name[input_name]

            # Cubes need special load options to avoid lazy loading issues
            options = Node.cube_load_options() if node_type == "cube" else None
            deployed_node = await Node.get_by_name(
                session,
                deployed_name,
                options=options,
            )
            assert deployed_node is not None, f"Deployed node {deployed_name} not found"

            deployed_spec = await deployed_node.to_spec(session)

            # Validate key fields match (with namespace transformation)
            _assert_deployment_spec_matches(
                input_spec,
                deployed_spec,
                namespace=namespace,
                node_name=input_name,
            )

    @pytest.mark.asyncio
    async def test_deploy_nested_namespace_not_treated_as_missing_dependency(
        self,
        client,
    ):
        """
        Test that nested namespaces don't cause parent namespace to be treated as missing dependency.
        This is a regression test for Issue #1775.

        Scenario:
        - Deploy external dimensions to 'external' namespace
        - Deploy metric to 'analytics' namespace that references external dimensions in query
        - The query parsing extracts column references like 'external.dimension.user_type.user_type_id'
        - And parent path 'external.dimension.user_type'
        - The code should filter out namespace prefix 'external.dimension' at line 1558
        """
        # STEP 1: Deploy external dimensions to 'external' namespace
        external_source = SourceSpec(
            name="source.users",
            description="Users source",
            catalog="default",
            schema="public",
            table="users",
            columns=[
                ColumnSpec(name="user_id", type="int"),
                ColumnSpec(name="user_type_id", type="int"),
                ColumnSpec(name="user_type_name", type="string"),
            ],
            dimension_links=[],
            owners=["dj"],
        )
        external_dimension = DimensionSpec(
            name="dimension.user_type",
            description="User type dimension",
            query="""
                SELECT
                    user_type_id,
                    user_type_name
                FROM ${prefix}source.users
            """,
            primary_key=["user_type_id"],
            owners=["dj"],
        )

        data1 = await deploy_and_wait(
            client,
            DeploymentSpec(
                namespace="external",
                nodes=[external_source, external_dimension],
            ),
        )
        assert data1["status"] == "success"

        # STEP 2: Deploy metric and cube that reference external dimensions
        # The metric query references 'external.dimension.user_type.user_type_id'
        # which will extract:
        # - 'external.dimension.user_type.user_type_id' (column ref)
        # - 'external.dimension.user_type' (parent, exists in DB)
        # - possibly 'external.dimension' (namespace prefix, should be filtered at line 1558)
        metric_with_external_ref = MetricSpec(
            name="metric.user_count_by_type",
            description="Count users by type",
            query="SELECT count(*) as cnt FROM external.source.users",
            dimension_links=[],
            owners=["dj"],
        )

        # Cube also references the external dimension
        cube_spec = CubeSpec(
            name="cube.user_analysis",
            description="User analysis cube",
            metrics=["${prefix}metric.user_count_by_type"],
            dimensions=[
                "external.dimension.user_type.user_type_id",
                "external.dimension.user_type.user_type_name",
            ],
            owners=["dj"],
        )

        data2 = await deploy_and_wait(
            client,
            DeploymentSpec(
                namespace="analytics",
                nodes=[metric_with_external_ref, cube_spec],
            ),
        )

        # The metric should succeed; the cube should fail because the external
        # dimension attributes aren't reachable from the metric (no join path).
        # Critically, the failure must NOT be "external.dimension is a missing
        # dependency" — that intermediate namespace prefix must NOT be flagged
        # as a missing node (regression test for Issue #1775).
        assert data2["namespace"] == "analytics"
        node_results = [r for r in data2["results"] if r["deploy_type"] == "node"]
        assert len(node_results) == 2

        metric_result = next(
            r for r in node_results if "metric.user_count_by_type" in r["name"]
        )
        assert metric_result["status"] == "success"

        cube_result = next(r for r in node_results if "cube.user_analysis" in r["name"])
        assert cube_result["status"] in ("failed", "invalid")
        # The failure is about dimension reachability, not a missing namespace prefix
        assert "is not reachable from parent node" in cube_result["message"]
        assert not any(
            "external.dimension" in r.get("message", "")
            and "missing" in r.get("message", "")
            for r in data2["results"]
        )


@pytest.mark.asyncio
async def test_node_to_spec_source(module__session, module__client_with_roads):
    """
    Test that a source node can be converted to a spec correctly
    """
    repair_orders = await Node.get_by_name(
        module__session,
        "default.repair_orders",
    )
    repair_orders_spec = await repair_orders.to_spec(module__session)
    assert repair_orders_spec == SourceSpec(
        name="default.repair_orders",
        node_type=NodeType.SOURCE,
        owners=["dj"],
        display_name="default.roads.repair_orders",
        description="All repair orders",
        tags=[],
        mode=NodeMode.PUBLISHED,
        custom_metadata={},
        columns=[
            ColumnSpec(
                name="repair_order_id",
                type="int",
                display_name="Repair Order Id",
                description=None,
            ),
            ColumnSpec(
                name="municipality_id",
                type="string",
                display_name="Municipality Id",
                description=None,
            ),
            ColumnSpec(
                name="hard_hat_id",
                type="int",
                display_name="Hard Hat Id",
                description=None,
            ),
            ColumnSpec(
                name="order_date",
                type="timestamp",
                display_name="Order Date",
                description=None,
            ),
            ColumnSpec(
                name="required_date",
                type="timestamp",
                display_name="Required Date",
                description=None,
            ),
            ColumnSpec(
                name="dispatched_date",
                type="timestamp",
                display_name="Dispatched Date",
                description=None,
            ),
            ColumnSpec(
                name="dispatcher_id",
                type="int",
                display_name="Dispatcher Id",
                description=None,
            ),
        ],
        dimension_links=[
            DimensionJoinLinkSpec(
                dimension_node="default.repair_order",
                join_type=JoinType.INNER,
                join_on="default.repair_orders.repair_order_id = default.repair_order.repair_order_id",
            ),
            DimensionJoinLinkSpec(
                dimension_node="default.dispatcher",
                join_type=JoinType.INNER,
                join_on="default.repair_orders.dispatcher_id = default.dispatcher.dispatcher_id",
            ),
        ],
        primary_key=[],
        catalog="default",
        schema="roads",
        table="repair_orders",
    )


@pytest.mark.asyncio
async def test_node_to_spec_transform(module__session, module__client_with_roads):
    """
    Test that a transform node can be converted to a spec correctly
    """
    repair_orders_fact = await Node.get_by_name(
        module__session,
        "default.repair_orders_fact",
    )
    repair_orders_fact_spec = await repair_orders_fact.to_spec(module__session)
    assert repair_orders_fact_spec == TransformSpec(
        name="default.repair_orders_fact",
        node_type=NodeType.TRANSFORM,
        owners=["dj"],
        display_name="Repair Orders Fact",
        description="Fact transform with all details on repair orders",
        tags=[],
        mode=NodeMode.PUBLISHED,
        custom_metadata={"foo": "bar"},
        columns=[
            ColumnSpec(
                name="repair_order_id",
                type="int",
                display_name="Repair Order Id",
                description=None,
                attributes=[],
                partition=None,
            ),
            ColumnSpec(
                name="municipality_id",
                type="string",
                display_name="Municipality Id",
                description=None,
                attributes=[],
                partition=None,
            ),
            ColumnSpec(
                name="hard_hat_id",
                type="int",
                display_name="Hard Hat Id",
                description=None,
                attributes=[],
                partition=None,
            ),
            ColumnSpec(
                name="dispatcher_id",
                type="int",
                display_name="Dispatcher Id",
                description=None,
                attributes=[],
                partition=None,
            ),
            ColumnSpec(
                name="order_date",
                type="timestamp",
                display_name="Order Date",
                description=None,
                attributes=[],
                partition=None,
            ),
            ColumnSpec(
                name="dispatched_date",
                type="timestamp",
                display_name="Dispatched Date",
                description=None,
                attributes=[],
                partition=None,
            ),
            ColumnSpec(
                name="required_date",
                type="timestamp",
                display_name="Required Date",
                description=None,
                attributes=[],
                partition=None,
            ),
            ColumnSpec(
                name="discount",
                type="float",
                display_name="Discount",
                description=None,
                attributes=[],
                partition=None,
            ),
            ColumnSpec(
                name="price",
                type="float",
                display_name="Price",
                description=None,
                attributes=[],
                partition=None,
            ),
            ColumnSpec(
                name="quantity",
                type="int",
                display_name="Quantity",
                description=None,
                attributes=[],
                partition=None,
            ),
            ColumnSpec(
                name="repair_type_id",
                type="int",
                display_name="Repair Type Id",
                description=None,
                attributes=[],
                partition=None,
            ),
            ColumnSpec(
                name="total_repair_cost",
                type="float",
                display_name="Total Repair Cost",
                description=None,
                attributes=[],
                partition=None,
            ),
            ColumnSpec(
                name="time_to_dispatch",
                type="timestamp",
                display_name="Time To Dispatch",
                description=None,
                attributes=[],
                partition=None,
            ),
            ColumnSpec(
                name="dispatch_delay",
                type="timestamp",
                display_name="Dispatch Delay",
                description=None,
                attributes=[],
                partition=None,
            ),
        ],
        dimension_links=[
            DimensionJoinLinkSpec(
                dimension_node="default.municipality_dim",
                join_type=JoinType.INNER,
                join_on="default.repair_orders_fact.municipality_id = default.municipality_dim.municipality_id",
            ),
            DimensionJoinLinkSpec(
                dimension_node="default.hard_hat",
                join_type=JoinType.INNER,
                join_on="default.repair_orders_fact.hard_hat_id = default.hard_hat.hard_hat_id",
            ),
            DimensionJoinLinkSpec(
                dimension_node="default.hard_hat_to_delete",
                join_type=JoinType.LEFT,
                join_on="default.repair_orders_fact.hard_hat_id = default.hard_hat_to_delete.hard_hat_id",
            ),
            DimensionJoinLinkSpec(
                dimension_node="default.dispatcher",
                join_type=JoinType.INNER,
                join_on="default.repair_orders_fact.dispatcher_id = default.dispatcher.dispatcher_id",
            ),
        ],
        primary_key=[],
        query=repair_orders_fact.current.query,
    )


@pytest.mark.asyncio
async def test_node_to_spec_respects_column_order(
    module__session,
    module__client_with_roads,
):
    """
    Test that to_spec() returns columns sorted by their order field,
    regardless of the order they were loaded from the database.

    This test simulates the bug where selectinload might not respect
    the relationship's order_by, by manually reordering columns before
    calling to_spec().
    """
    # Get a node with columns
    repair_orders_fact = await Node.get_by_name(
        module__session,
        "default.repair_orders_fact",
        options=Node.cube_load_options(),
    )

    # Get the original column order values
    original_columns = repair_orders_fact.current.columns
    column_orders = [(col.name, col.order) for col in original_columns]

    # Manually shuffle columns to simulate selectinload not respecting order_by
    # Reverse the list to put them in wrong order
    repair_orders_fact.current.columns = list(reversed(original_columns))

    # Call to_spec - it should sort them back to correct order
    spec = await repair_orders_fact.to_spec(module__session)

    # Verify columns are in the correct order (by order field, not by list position)
    sorted_column_names = [
        name
        for name, _ in sorted(
            column_orders,
            key=lambda x: x[1] if x[1] is not None else float("inf"),
        )
    ]
    actual_column_names = [col.name for col in spec.columns]

    assert actual_column_names == sorted_column_names, (
        f"Columns not in correct order. "
        f"Expected: {sorted_column_names}, "
        f"Got: {actual_column_names}"
    )


@pytest.mark.asyncio
async def test_node_to_spec_dimension(module__session, module__client_with_roads):
    """
    Test that a dimension node can be converted to a spec correctly
    """
    hard_hat = await Node.get_by_name(
        module__session,
        "default.hard_hat",
    )
    hard_hat_spec = await hard_hat.to_spec(module__session)
    assert hard_hat_spec == DimensionSpec(
        name="default.hard_hat",
        node_type=NodeType.DIMENSION,
        owners=["dj"],
        display_name="Hard Hat",
        description="Hard hat dimension",
        tags=[],
        mode=NodeMode.PUBLISHED,
        columns=[
            ColumnSpec(
                name="hard_hat_id",
                type="int",
                display_name="Hard Hat Id",
                description=None,
                attributes=["primary_key"],
                partition=None,
            ),
            ColumnSpec(
                name="last_name",
                type="string",
                display_name="Last Name",
                description=None,
                attributes=[],
                partition=None,
            ),
            ColumnSpec(
                name="first_name",
                type="string",
                display_name="First Name",
                description=None,
                attributes=[],
                partition=None,
            ),
            ColumnSpec(
                name="title",
                type="string",
                display_name="Title",
                description=None,
                attributes=[],
                partition=None,
            ),
            ColumnSpec(
                name="birth_date",
                type="timestamp",
                display_name="Birth Date",
                description=None,
                attributes=[],
                partition=None,
            ),
            ColumnSpec(
                name="hire_date",
                type="timestamp",
                display_name="Hire Date",
                description=None,
                attributes=[],
                partition=None,
            ),
            ColumnSpec(
                name="address",
                type="string",
                display_name="Address",
                description=None,
                attributes=[],
                partition=None,
            ),
            ColumnSpec(
                name="city",
                type="string",
                display_name="City",
                description=None,
                attributes=[],
                partition=None,
            ),
            ColumnSpec(
                name="state",
                type="string",
                display_name="State",
                description=None,
                attributes=[],
                partition=None,
            ),
            ColumnSpec(
                name="postal_code",
                type="string",
                display_name="Postal Code",
                description=None,
                attributes=[],
                partition=None,
            ),
            ColumnSpec(
                name="country",
                type="string",
                display_name="Country",
                description=None,
                attributes=[],
                partition=None,
            ),
            ColumnSpec(
                name="manager",
                type="int",
                display_name="Manager",
                description=None,
                attributes=[],
                partition=None,
            ),
            ColumnSpec(
                name="contractor_id",
                type="int",
                display_name="Contractor Id",
                description=None,
                attributes=[],
                partition=None,
            ),
        ],
        dimension_links=[
            DimensionJoinLinkSpec(
                dimension_node="default.us_state",
                join_type=JoinType.INNER,
                join_on="default.hard_hat.state = default.us_state.state_short",
            ),
        ],
        primary_key=["hard_hat_id"],
        query=hard_hat.current.query,
    )


@pytest.mark.asyncio
async def test_node_to_spec_metric(module__session, module__client_with_roads):
    """
    Test that a metric node can be converted to a spec correctly
    """
    num_repair_orders = await Node.get_by_name(
        module__session,
        "default.num_repair_orders",
    )
    num_repair_orders_spec = await num_repair_orders.to_spec(module__session)
    assert num_repair_orders_spec == MetricSpec(
        name="default.num_repair_orders",
        node_type=NodeType.METRIC,
        owners=["dj"],
        display_name="Num Repair Orders",
        description="Number of repair orders",
        tags=[],
        mode=NodeMode.PUBLISHED,
        custom_metadata={"foo": "bar"},
        query=num_repair_orders.current.query,
        required_dimensions=[],
        direction=MetricDirection.HIGHER_IS_BETTER,
        unit_enum=MetricUnit.DOLLAR,
        significant_digits=None,
        min_decimal_exponent=None,
        max_decimal_exponent=None,
    )


@pytest.mark.asyncio
async def test_node_to_spec_cube(module__session, module__client_with_roads):
    """
    Test that a cube node can be converted to a spec correctly, including cube_filters.
    """
    # Create a cube with filters via the API
    response = await module__client_with_roads.post(
        "/nodes/cube/",
        json={
            "name": "default.repairs_cube_spec_test",
            "description": "Cube for to_spec test",
            "metrics": ["default.num_repair_orders"],
            "dimensions": ["default.hard_hat.state"],
            "filters": ["default.hard_hat.state='AZ'"],
            "mode": "published",
        },
    )
    assert response.status_code == 201

    cube_node = await Node.get_cube_by_name(
        module__session,
        "default.repairs_cube_spec_test",
    )
    spec = await cube_node.to_spec(module__session)
    assert isinstance(spec, CubeSpec)
    assert spec.filters == ["default.hard_hat.state='AZ'"]


def test_node_spec_equality():
    """
    Test that two node specs are equal.
    """
    namespace = "base"
    orig_spec = DimensionSpec(
        namespace="base",
        name="hard_hat",
        description="Hard hat dimension",
        query="""SELECT
    hard_hat_id,
    last_name,
    first_name
FROM ${prefix}default.hard_hats""",
        primary_key=["hard_hat_id"],
        owners=["dj"],
        dimension_links=[
            DimensionJoinLinkSpec(
                dimension_node="default.us_state",
                join_type=JoinType.INNER,
                join_on="${prefix}default.hard_hat.state = ${prefix}default.us_state.state_short",
            ),
        ],
    )
    spec_with_same_query = DimensionSpec(
        namespace="base",
        name="hard_hat",
        description="Hard hat dimension",
        query="""SELECT hard_hat_id, last_name, first_name FROM ${prefix}default.hard_hats""",
        primary_key=["hard_hat_id"],
        owners=["dj"],
        dimension_links=[
            DimensionJoinLinkSpec(
                dimension_node="default.us_state",
                join_type=JoinType.INNER,
                join_on="${prefix}default.hard_hat.state = ${prefix}default.us_state.state_short",
            ),
        ],
    )
    assert orig_spec == spec_with_same_query

    spec_with_diff_namespace = DimensionSpec(
        name=f"{namespace}.hard_hat",
        description="Hard hat dimension",
        query="""SELECT hard_hat_id, last_name, first_name FROM base.default.hard_hats""",
        primary_key=["hard_hat_id"],
        owners=["dj"],
        dimension_links=[
            DimensionJoinLinkSpec(
                dimension_node="default.us_state",
                join_type=JoinType.INNER,
                join_on=f"{namespace}.default.hard_hat.state = base.default.us_state.state_short",
            ),
        ],
    )
    assert orig_spec == spec_with_diff_namespace

    @pytest.mark.asyncio
    async def test_deploy_with_spark_hints(
        self,
        session,
        client,
        default_hard_hats,
        default_us_states,
        default_us_state,
    ):
        """
        Test that spark_hints on a DimensionJoinLinkSpec is persisted on the
        deployed DimensionLink and survives a subsequent redeploy (revision copy).
        """
        namespace = "spark_hints_deploy"
        dim_spec = DimensionSpec(
            name="default.hard_hat",
            description="Hard hat dimension",
            query="""
            SELECT hard_hat_id, state
            FROM ${prefix}default.hard_hats
            """,
            primary_key=["hard_hat_id"],
            owners=["dj"],
            dimension_links=[
                DimensionJoinLinkSpec(
                    dimension_node="${prefix}default.us_state",
                    join_type="left",
                    join_on="${prefix}default.hard_hat.state = ${prefix}default.us_state.state_short",
                    spark_hints="broadcast",
                ),
            ],
        )
        nodes_list = [dim_spec, default_hard_hats, default_us_states, default_us_state]
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=nodes_list),
        )
        assert data["status"] == "success"

        # Verify spark_hints was persisted on the deployed dimension link
        hard_hat = await Node.get_by_name(session, f"{namespace}.default.hard_hat")
        assert len(hard_hat.current.dimension_links) == 1
        link = hard_hat.current.dimension_links[0]
        assert link.spark_hints.value == "broadcast"

        # Redeploy with a different spark_hints to verify the update path
        dim_spec.dimension_links = [
            DimensionJoinLinkSpec(
                dimension_node="${prefix}default.us_state",
                join_type="left",
                join_on="${prefix}default.hard_hat.state = ${prefix}default.us_state.state_short",
                spark_hints="merge",
            ),
        ]
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=nodes_list),
        )
        assert data["status"] == "success"

        hard_hat = await Node.get_by_name(session, f"{namespace}.default.hard_hat")
        link = hard_hat.current.dimension_links[0]
        assert link.spark_hints.value == "merge"


@pytest.mark.asyncio
@pytest.mark.skip(reason="For debugging with full roads spec")
async def test_print_roads_spec(roads_nodes):
    spec = DeploymentSpec(
        namespace="roads",
        nodes=roads_nodes,
    )
    print("Roads Spec:", json.dumps(spec.model_dump()))
    assert 1 == 2


@pytest.mark.xdist_group(name="deployments")
class TestDeclaredCubeMaterializations:
    """
    Reconciling a cube's `materialization:` block against what is materialized.
    """

    @pytest.fixture
    def upstreams(
        self,
        default_hard_hats,
        default_hard_hat,
        default_us_states,
        default_us_state,
        default_avg_length_of_employment,
    ) -> list:
        """The sources, dimensions and metric every cube in this class is built on."""
        return [
            default_hard_hats,
            default_hard_hat,
            default_us_states,
            default_us_state,
            default_avg_length_of_employment,
        ]

    @staticmethod
    def _undeclared_warning(cube_name: str) -> str:
        """The warning a materialized cube with no declared block earns."""
        return (
            f"Cube `{cube_name}` is materialized but its spec declares no "
            "`materialization:` block, so the existing materialization was left "
            "running. Run `dj pull` to adopt it into YAML, or add "
            "`materialization: none` to remove it."
        )

    @staticmethod
    def _materialization_name(namespace: str) -> str:
        """The name `create_new_materialization` derives for this class's cube."""
        return f"druid_cube__incremental_time__{namespace}.default.hard_hat.hire_date"

    @staticmethod
    def _cube(**overrides) -> CubeSpec:
        """A partitioned cube, ready to declare a materialization."""
        fields: dict = {
            "name": "default.repairs_cube",
            "display_name": "Repairs Cube",
            "description": "Cube for analyzing repair orders",
            "dimensions": [
                "${prefix}default.hard_hat.state",
                "${prefix}default.hard_hat.hire_date",
            ],
            "metrics": ["${prefix}default.avg_length_of_employment"],
            "owners": ["dj"],
            "columns": [
                ColumnSpec(
                    name="${prefix}default.hard_hat.hire_date",
                    partition=PartitionSpec(
                        type=PartitionType.TEMPORAL,
                        granularity=Granularity.DAY,
                        format="yyyyMMdd",
                    ),
                ),
            ],
        }
        return CubeSpec(**{**fields, **overrides})

    @staticmethod
    def _materialization_results(data: dict) -> list[tuple]:
        """The reconciler's own results, as (name, operation, status, message)."""
        return [
            (
                result["name"],
                result["operation"],
                result["status"],
                result["message"],
            )
            for result in data["results"]
            if result["deploy_type"] == "materialization"
        ]

    @staticmethod
    async def _persisted_materializations(client, cube_name: str) -> list[tuple]:
        """The cube's materializations, as (name, schedule, strategy, active)."""
        body = (await client.get(f"/nodes/{cube_name}/")).json()
        return [
            (
                materialization["name"],
                materialization["schedule"],
                materialization["strategy"],
                materialization["deactivated_at"] is None,
            )
            for materialization in body["materializations"]
        ]

    @pytest.mark.asyncio
    async def test_recorded_workflow_names_outlive_a_redeploy(
        self,
        session,
        client,
        upstreams,
        mock_qs,
    ):
        """
        DJ writes down the workflows the query service names and backfills the one
        called `.backfill`, rather than letting the query service derive a name that
        does not match what the materialize call pushed.

        The names go on the materialization row, not in its config, which every
        deploy rebuilds from the revision and compares to decide whether anything
        moved -- so a second push that changes nothing still finds them there.
        """
        namespace = "cube_mat_workflow_names"
        cube_name = f"{namespace}.default.repairs_cube"
        workflow_names = [
            f"dj.{cube_name}.v1_0.main",
            f"dj.{cube_name}.v1_0.backfill",
        ]
        mock_qs.materialize_cube.return_value = MaterializationInfo(
            urls=["http://fake.url/job"],
            output_tables=[],
            workflow_names=workflow_names,
        )
        cube = self._cube(
            materialization=MaterializationSpec(
                schedule="0 6 * * *",
                lookback_window="1 DAY",
                coverage={"from": "2024-01-01", "to": "2024-01-05"},
            ),
        )
        nodes = [*upstreams, cube]
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=nodes),
        )
        assert data["status"] == "success", data
        assert mock_qs.run_cube_backfill.call_args_list == [
            mock.call(
                CubeBackfillInput(
                    cube_name=cube_name,
                    cube_version="v1.0",
                    start_date=date(2024, 1, 1),
                    end_date=date(2024, 1, 5),
                    workflow_name=f"dj.{cube_name}.v1_0.backfill",
                ),
                request_headers=mock.ANY,
            ),
        ]

        session.expire_all()
        deployed = await Node.get_by_name(
            session,
            cube_name,
            options=Node.cube_load_options(),
        )
        assert [
            materialization.workflow_names
            for materialization in deployed.current.materializations
        ] == [workflow_names]

        mock_qs.reset_mock()
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=nodes),
        )
        assert data["status"] == "success", data
        assert mock_qs.method_calls == []

        session.expire_all()
        deployed = await Node.get_by_name(
            session,
            cube_name,
            options=Node.cube_load_options(),
        )
        assert [
            materialization.workflow_names
            for materialization in deployed.current.materializations
        ] == [workflow_names]

    @pytest.mark.asyncio
    async def test_unchanged_declaration_touches_nothing(
        self,
        client,
        upstreams,
        mock_qs,
    ):
        """
        Re-pushing an unchanged repo must not churn a live workflow: the second
        deploy reports a no-op and makes no query service call at all.
        """
        namespace = "cube_mat_unchanged"
        cube_name = f"{namespace}.default.repairs_cube"
        cube = self._cube(
            materialization=MaterializationSpec(
                schedule="0 6 * * *",
                lookback_window="1 DAY",
            ),
        )
        nodes = [
            *upstreams,
            cube,
        ]
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=nodes),
        )
        assert data["status"] == "success", data
        assert mock_qs.materialize_cube.call_count == 1

        mock_qs.reset_mock()
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=nodes),
        )
        assert data["status"] == "success", data
        assert self._materialization_results(data) == [
            (
                cube_name,
                "noop",
                "success",
                "cube materialization on schedule 0 6 * * *",
            ),
        ]
        assert mock_qs.method_calls == []
        assert await self._persisted_materializations(client, cube_name) == [
            (
                self._materialization_name(namespace),
                "0 6 * * *",
                "incremental_time",
                True,
            ),
        ]

    @pytest.mark.asyncio
    async def test_declared_materialization_is_attributed_to_the_cube_owners(
        self,
        client,
        upstreams,
        mock_qs,
    ):
        """
        A cube deployed from YAML tells the query service who owns it.

        This is the path with nothing loaded on the revision it materializes -- the
        deployment created that revision moments ago -- so it is also where a missing
        eager load on `owners` would surface, as a lazy load from async code that
        fails intermittently rather than a wrong answer.
        """
        namespace = "cube_mat_attribution"
        cube = self._cube(
            custom_metadata={"ownership_override": {"team": "metrics"}},
            materialization=MaterializationSpec(
                schedule="0 6 * * *",
                lookback_window="1 DAY",
            ),
        )
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=[*upstreams, cube]),
        )
        assert data["status"] == "success", data
        assert mock_qs.materialize_cube.call_count == 1
        scheduled = mock_qs.materialize_cube.call_args.kwargs["materialization_input"]
        assert (scheduled.owners, scheduled.custom_metadata) == (
            [PrincipalRef(username="dj", kind=PrincipalKind.USER)],
            {"ownership_override": {"team": "metrics"}},
        )

    @pytest.mark.asyncio
    async def test_definition_and_schedule_edit_rebuilds_once(
        self,
        client,
        upstreams,
        default_num_repair_orders,
        mock_qs,
    ):
        """
        Editing the cube's metrics and its schedule in one push must rebuild once.

        The new revision's swap already builds from the declared block, so the
        reconciler that follows has to find the result matching and stand down --
        otherwise the query service is asked to schedule the same cube twice.
        """
        namespace = "cube_mat_one_rebuild"
        cube_name = f"{namespace}.default.repairs_cube"
        cube = self._cube(
            materialization=MaterializationSpec(
                schedule="0 6 * * *",
                lookback_window="1 DAY",
            ),
        )
        nodes = [
            *upstreams,
            default_num_repair_orders,
            cube,
        ]
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=nodes),
        )
        assert data["status"] == "success", data

        mock_qs.reset_mock()
        cube.metrics = [
            "${prefix}default.avg_length_of_employment",
            "${prefix}default.num_repair_orders",
        ]
        cube.materialization = MaterializationSpec(
            schedule="0 3 * * *",
            lookback_window="1 DAY",
        )
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=nodes),
        )
        assert data["status"] == "success", data
        assert self._materialization_results(data) == [
            (
                cube_name,
                "update",
                "success",
                "cube materialization on schedule 0 3 * * *",
            ),
        ]

        # One schedule call, for the new revision and the new schedule, and one
        # stop for the superseded revision's workflow.
        assert mock_qs.materialize_cube.call_count == 1
        scheduled = mock_qs.materialize_cube.call_args.kwargs["materialization_input"]
        assert (
            scheduled.cube.name,
            scheduled.cube.version,
            scheduled.schedule,
        ) == (cube_name, "v2.0", "0 3 * * *")
        assert mock_qs.deactivate_cube_workflow.call_args_list == [
            mock.call(cube_name, version="v1.0", request_headers=mock.ANY),
        ]
        assert await self._persisted_materializations(client, cube_name) == [
            (
                self._materialization_name(namespace),
                "0 3 * * *",
                "incremental_time",
                True,
            ),
        ]

    @pytest.mark.asyncio
    async def test_explicit_none_removes_the_materialization(
        self,
        client,
        upstreams,
        mock_qs,
    ):
        """
        `materialization: none` is the one thing that tears a materialization down:
        the row is deactivated and its workflow stopped.
        """
        namespace = "cube_mat_removed"
        cube_name = f"{namespace}.default.repairs_cube"
        cube = self._cube(
            materialization=MaterializationSpec(
                schedule="0 6 * * *",
                lookback_window="1 DAY",
            ),
        )
        nodes = [
            *upstreams,
            cube,
        ]
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=nodes),
        )
        assert data["status"] == "success", data

        mock_qs.reset_mock()
        cube.materialization = MaterializationAction.NONE
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=nodes),
        )
        assert data["status"] == "success", data
        assert self._materialization_results(data) == [
            (
                cube_name,
                "delete",
                "success",
                "cube materialization removed by `materialization: none`",
            ),
        ]
        assert mock_qs.materialize_cube.call_count == 0
        assert mock_qs.deactivate_cube_workflow.call_args_list == [
            mock.call(cube_name, version="v1.0", request_headers=mock.ANY),
        ]
        assert await self._persisted_materializations(client, cube_name) == [
            (
                self._materialization_name(namespace),
                "0 6 * * *",
                "incremental_time",
                False,
            ),
        ]

    @pytest.mark.asyncio
    async def test_declared_block_supersedes_a_competing_materialization(
        self,
        client,
        upstreams,
        mock_qs,
    ):
        """
        A cube gets one materialization: the one it declares.

        A materialization's name carries its strategy, so declaring a strategy the
        cube was not already materialized with builds a differently named row -- and
        leaving the old one active gives one Druid datasource two writers, where the
        full rebuild deletes what the incremental run added. The competing row is
        deactivated and its workflow stopped before the declared one is scheduled,
        since both sit on the same cube version and the stop can only be addressed by
        cube and version.
        """
        namespace = "cube_mat_supersede"
        cube_name = f"{namespace}.default.repairs_cube"
        full_name = f"druid_cube__full__{namespace}.default.hard_hat.hire_date"
        nodes = [
            *upstreams,
            self._cube(),
        ]
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=nodes),
        )
        assert data["status"] == "success", data

        # `full` is what the node page's popover defaults to for this cube.
        response = await client.post(
            f"/nodes/{cube_name}/materialization/",
            json={
                "job": "druid_cube",
                "strategy": "full",
                "schedule": "@daily",
            },
        )
        assert response.status_code == 200, response.json()

        mock_qs.reset_mock()
        nodes[-1] = self._cube(
            materialization=MaterializationSpec(
                schedule="0 6 * * *",
                lookback_window="1 DAY",
            ),
        )
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=nodes),
        )
        assert data["status"] == "success", data
        assert self._materialization_results(data) == [
            (
                cube_name,
                "update",
                "success",
                "cube materialization on schedule 0 6 * * *",
            ),
        ]
        assert [call[0] for call in mock_qs.method_calls] == [
            "deactivate_cube_workflow",
            "materialize_cube",
        ]
        assert mock_qs.deactivate_cube_workflow.call_args_list == [
            mock.call(cube_name, version="v1.0", request_headers=mock.ANY),
        ]
        assert await self._persisted_materializations(client, cube_name) == [
            (full_name, "@daily", "full", False),
            (
                self._materialization_name(namespace),
                "0 6 * * *",
                "incremental_time",
                True,
            ),
        ]

    @pytest.mark.asyncio
    async def test_competing_materializations_rebuild_to_one_row(
        self,
        client,
        upstreams,
        default_num_repair_orders,
        mock_qs,
    ):
        """
        The state a declared block prevents already exists on cubes materialized
        before it did: a `full` and an `incremental_time` materialization on one cube.

        Editing such a cube's definition rebuilds both against the new revision under
        the strategy the block declares, so both derive the same name -- and `(name,
        node_revision_id)` is unique. The cube lands on the one materialization it
        declared instead of failing the deployment on an integrity error.
        """
        namespace = "cube_mat_two_writers"
        cube_name = f"{namespace}.default.repairs_cube"
        cube = self._cube()
        nodes = [
            *upstreams,
            default_num_repair_orders,
            cube,
        ]
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=nodes),
        )
        assert data["status"] == "success", data

        for strategy in ("full", "incremental_time"):
            response = await client.post(
                f"/nodes/{cube_name}/materialization/",
                json={
                    "job": "druid_cube",
                    "strategy": strategy,
                    "schedule": "@daily",
                },
            )
            assert response.status_code == 200, response.json()

        mock_qs.reset_mock()
        nodes[-1] = self._cube(
            metrics=[
                "${prefix}default.avg_length_of_employment",
                "${prefix}default.num_repair_orders",
            ],
            materialization=MaterializationSpec(
                schedule="0 6 * * *",
                lookback_window="1 DAY",
            ),
        )
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=nodes),
        )
        assert data["status"] == "success", data
        assert self._materialization_results(data) == [
            (
                cube_name,
                "update",
                "success",
                "cube materialization on schedule 0 6 * * *",
            ),
        ]
        assert [call[0] for call in mock_qs.method_calls] == [
            "materialize_cube",
            "deactivate_cube_workflow",
        ]
        assert mock_qs.deactivate_cube_workflow.call_args_list == [
            mock.call(cube_name, version="v1.0", request_headers=mock.ANY),
        ]
        assert await self._persisted_materializations(client, cube_name) == [
            (
                self._materialization_name(namespace),
                "0 6 * * *",
                "incremental_time",
                True,
            ),
        ]

    @pytest.mark.asyncio
    async def test_explicit_none_removes_a_materialization_of_any_job(
        self,
        client,
        upstreams,
        mock_qs,
    ):
        """
        A teardown takes down whatever the cube actually has, not only what a
        `materialization:` block could have described.

        Only the fused Druid cube job projects back into declarative form, so a cube
        materialized by the older `druid_measures_cube` job reads there as
        unmaterialized -- and answering `materialization: none` from that projection
        reported a green no-op while the workflow kept running.
        """
        namespace = "cube_mat_legacy_removed"
        cube_name = f"{namespace}.default.repairs_cube"
        materialization_name = (
            f"druid_measures_cube__incremental_time__"
            f"{namespace}.default.hard_hat.hire_date"
        )
        nodes = [
            *upstreams,
            self._cube(),
        ]
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=nodes),
        )
        assert data["status"] == "success", data

        response = await client.post(
            f"/nodes/{cube_name}/materialization/",
            json={
                "job": "druid_measures_cube",
                "strategy": "incremental_time",
                "schedule": "@daily",
                "config": {"spark": {}, "lookback_window": "1 DAY"},
            },
        )
        assert response.status_code == 200, response.json()

        mock_qs.reset_mock()
        nodes[-1] = self._cube(materialization=MaterializationAction.NONE)
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=nodes),
        )
        assert data["status"] == "success", data
        assert self._materialization_results(data) == [
            (
                cube_name,
                "delete",
                "success",
                "cube materialization removed by `materialization: none`",
            ),
        ]
        assert mock_qs.deactivate_cube_workflow.call_args_list == [
            mock.call(cube_name, version="v1.0", request_headers=mock.ANY),
        ]
        assert await self._persisted_materializations(client, cube_name) == [
            (materialization_name, "@daily", "incremental_time", False),
        ]

    @pytest.mark.asyncio
    async def test_explicit_none_without_a_materialization_is_a_noop(
        self,
        client,
        upstreams,
        mock_qs,
    ):
        """
        A cube that says `materialization: none` and never had one reports a no-op
        and asks the query service to stop nothing.
        """
        namespace = "cube_mat_null_noop"
        cube_name = f"{namespace}.default.repairs_cube"
        nodes = [
            *upstreams,
            self._cube(materialization=MaterializationAction.NONE),
        ]
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=nodes),
        )
        assert data["status"] == "success", data
        assert self._materialization_results(data) == [
            (
                cube_name,
                "noop",
                "success",
                "cube declares no materialization",
            ),
        ]
        assert mock_qs.method_calls == []
        assert await self._persisted_materializations(client, cube_name) == []

    @pytest.mark.asyncio
    async def test_undeclared_materialization_is_left_running(
        self,
        client,
        upstreams,
        mock_qs,
    ):
        """
        Omitting the block is not the same as removing it. A cube materialized
        outside YAML keeps running, and the deploy says so and names both ways out.
        """
        namespace = "cube_mat_undeclared"
        cube_name = f"{namespace}.default.repairs_cube"
        nodes = [
            *upstreams,
            self._cube(),
        ]
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=nodes),
        )
        assert data["status"] == "success", data
        assert self._materialization_results(data) == []

        response = await client.post(
            f"/nodes/{cube_name}/materialization/",
            json={
                "job": "druid_cube",
                "strategy": "incremental_time",
                "schedule": "@daily",
                "lookback_window": "1 DAY",
            },
        )
        assert response.status_code == 200, response.json()

        mock_qs.reset_mock()
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=nodes),
        )
        assert data["status"] == "success", data
        warning = self._undeclared_warning(cube_name)
        assert self._materialization_results(data) == [
            (cube_name, "noop", "warning", warning),
        ]
        assert [item["message"] for item in data["warnings"]] == [warning]
        assert mock_qs.method_calls == []
        assert await self._persisted_materializations(client, cube_name) == [
            (
                self._materialization_name(namespace),
                "@daily",
                "incremental_time",
                True,
            ),
        ]

    @pytest.mark.asyncio
    async def test_cube_planner_materialization_is_left_alone(
        self,
        session,
        client,
        upstreams,
        default_num_repair_orders,
        mock_qs,
    ):
        """
        A cube materialized by the cube planner is not adopted into the fused dialect.

        The two dialects are indistinguishable by job -- `POST
        /cubes/{name}/materialize` stores the same `DruidCubeMaterializationJob` the
        fused path derives -- so a planner row read as intent to rebuild recovers as a
        fused materialization: the planner's workflow stopped and a fused one
        scheduled in its place. It is left exactly where it is instead, and the
        warning the cube earns says something true about it.
        """
        namespace = "cube_mat_planner"
        cube_name = f"{namespace}.default.repairs_cube"
        nodes = [
            *upstreams,
            default_num_repair_orders,
            self._cube(),
        ]
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=nodes),
        )
        assert data["status"] == "success", data

        cube = await Node.get_by_name(session, cube_name)
        session.add(
            Materialization(
                node_revision_id=cube.current.id,
                name="druid_cube_v3",
                strategy=MaterializationStrategy.INCREMENTAL_TIME,
                schedule="@daily",
                config={"version": "v3", "workflow_names": ["planner-workflow"]},
                job="DruidCubeMaterializationJob",
            ),
        )
        await session.commit()
        planner_row = ("druid_cube_v3", "@daily", "incremental_time", True)

        # An unchanged push leaves it running, and says so in terms that apply to it:
        # `dj pull` cannot write a block that describes a planner materialization.
        mock_qs.reset_mock()
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=nodes),
        )
        assert data["status"] == "success", data
        warning = (
            f"Cube `{cube_name}` is materialized by the cube planner, which a "
            "`materialization:` block cannot describe, so the existing "
            "materialization was left running. Add `materialization: none` to "
            "remove it."
        )
        assert self._materialization_results(data) == [
            (cube_name, "noop", "warning", warning),
        ]
        assert [item["message"] for item in data["warnings"]] == [warning]
        assert mock_qs.method_calls == []
        assert await self._persisted_materializations(client, cube_name) == [
            planner_row,
        ]

        # And so does a push that gives the cube a new revision, which would
        # otherwise carry the planner's row over as a fused one.
        nodes[-1] = self._cube(
            metrics=[
                "${prefix}default.avg_length_of_employment",
                "${prefix}default.num_repair_orders",
            ],
        )
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=nodes),
        )
        assert data["status"] == "success", data
        assert self._materialization_results(data) == []
        assert mock_qs.method_calls == []
        assert await self._persisted_materializations(client, cube_name) == []
        session.expire_all()
        assert [
            (row.name, row.deactivated_at)
            for row in (
                (
                    await session.execute(
                        select(Materialization).where(
                            Materialization.name == "druid_cube_v3",
                        ),
                    )
                )
                .scalars()
                .all()
            )
        ] == [("druid_cube_v3", None)]

    @pytest.mark.asyncio
    async def test_serialized_spec_tears_nothing_down(
        self,
        client,
        upstreams,
        mock_qs,
    ):
        """
        A caller that serializes a spec and re-posts it gets the deploy it asked for,
        not a mass teardown.

        `model_dump` emits every optional field explicitly, so a spec round-tripped
        through it says `materialization: null` for every cube -- which, read as
        "the author wrote that key", is a request to stop every workflow in the
        namespace. Teardown is a value (`materialization: none`) so that a round trip
        preserves what the cube actually declared, whether that is a schedule or
        nothing at all.
        """
        namespace = "cube_mat_round_trip"
        cube_name = f"{namespace}.default.repairs_cube"
        block = MaterializationSpec(schedule="0 6 * * *", lookback_window="1 DAY")
        nodes = [
            *upstreams,
        ]
        materialized = [
            (
                self._materialization_name(namespace),
                "0 6 * * *",
                "incremental_time",
                True,
            ),
        ]
        data = await deploy_and_wait(
            client,
            DeploymentSpec(
                namespace=namespace,
                nodes=[*nodes, self._cube(materialization=block)],
            ),
        )
        assert data["status"] == "success", data
        assert await self._persisted_materializations(client, cube_name) == (
            materialized
        )

        # A re-declare that has been through `model_dump` and back is still a
        # re-declare: the schedule stands, unchanged.
        mock_qs.reset_mock()
        declared = DeploymentSpec(
            namespace=namespace,
            nodes=[*nodes, self._cube(materialization=block)],
        )
        data = await deploy_and_wait(
            client,
            DeploymentSpec.model_validate(declared.model_dump()),
        )
        assert data["status"] == "success", data
        assert self._materialization_results(data) == [
            (
                cube_name,
                "noop",
                "success",
                "cube materialization on schedule 0 6 * * *",
            ),
        ]
        assert mock_qs.method_calls == []
        assert await self._persisted_materializations(client, cube_name) == (
            materialized
        )

        # And a cube that never mentioned `materialization:` keeps its
        # materialization once serialized, rather than losing it to the null the
        # dump invents.
        undeclared = DeploymentSpec(
            namespace=namespace,
            nodes=[*nodes, self._cube()],
        )
        data = await deploy_and_wait(
            client,
            DeploymentSpec.model_validate(undeclared.model_dump()),
        )
        assert data["status"] == "success", data
        assert self._materialization_results(data) == [
            (
                cube_name,
                "noop",
                "warning",
                self._undeclared_warning(cube_name),
            ),
        ]
        assert mock_qs.method_calls == []
        assert await self._persisted_materializations(client, cube_name) == (
            materialized
        )

    @pytest.mark.asyncio
    async def test_config_drift_under_an_unchanged_block_is_corrected(
        self,
        session,
        client,
        upstreams,
        mock_qs,
    ):
        """
        The block is only three fields; the built config is much more. When the two
        disagree -- something reconfigured the materialization outside YAML -- the
        repo wins, even though the block itself did not change.
        """
        namespace = "cube_mat_drift"
        cube_name = f"{namespace}.default.repairs_cube"
        materialization_name = self._materialization_name(namespace)
        cube = self._cube(
            materialization=MaterializationSpec(
                schedule="0 6 * * *",
                lookback_window="1 DAY",
            ),
        )
        nodes = [
            *upstreams,
            cube,
        ]
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=nodes),
        )
        assert data["status"] == "success", data

        materialization = (
            (
                await session.execute(
                    select(Materialization).where(
                        Materialization.name == materialization_name,
                    ),
                )
            )
            .scalars()
            .one()
        )
        materialization.config = {
            **materialization.config,
            "workflow_names": ["configured-elsewhere"],
        }
        await session.commit()

        mock_qs.reset_mock()
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=nodes),
        )
        assert data["status"] == "success", data
        assert self._materialization_results(data) == [
            (
                cube_name,
                "update",
                "success",
                "cube materialization on schedule 0 6 * * *",
            ),
        ]
        assert mock_qs.materialize_cube.call_count == 1
        # Read from the database rather than through `GET /nodes/{name}/`: the
        # revision did not change, so that response is still cached.
        session.expire_all()
        assert [
            (row.name, row.config.get("workflow_names"), row.deactivated_at)
            for row in (
                (
                    await session.execute(
                        select(Materialization).where(
                            Materialization.name == materialization_name,
                        ),
                    )
                )
                .scalars()
                .all()
            )
        ] == [(materialization_name, None, None)]

    @pytest.mark.asyncio
    async def test_dry_run_plans_the_change_without_making_it(
        self,
        client,
        upstreams,
        mock_qs,
    ):
        """
        A dry run reports the operation the wet run would perform, and performs none
        of it: nothing scheduled, nothing stopped, the persisted schedule untouched.
        """
        namespace = "cube_mat_dry_run"
        cube_name = f"{namespace}.default.repairs_cube"
        cube = self._cube(
            materialization=MaterializationSpec(
                schedule="0 6 * * *",
                lookback_window="1 DAY",
            ),
        )
        nodes = [
            *upstreams,
            cube,
        ]
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=nodes),
        )
        assert data["status"] == "success", data

        mock_qs.reset_mock()
        cube.materialization = MaterializationSpec(
            schedule="0 3 * * *",
            lookback_window="1 DAY",
        )
        response = await client.post(
            "/deployments/impact",
            json=deployment_payload(
                DeploymentSpec(namespace=namespace, nodes=nodes),
            ),
        )
        assert response.status_code == 200, response.json()
        assert self._materialization_results(response.json()) == [
            (
                cube_name,
                "update",
                "success",
                "cube materialization on schedule 0 3 * * *",
            ),
        ]
        assert mock_qs.method_calls == []
        assert await self._persisted_materializations(client, cube_name) == [
            (
                self._materialization_name(namespace),
                "0 6 * * *",
                "incremental_time",
                True,
            ),
        ]

        # A planned teardown is reported and equally not carried out.
        cube.materialization = MaterializationAction.NONE
        response = await client.post(
            "/deployments/impact",
            json=deployment_payload(
                DeploymentSpec(namespace=namespace, nodes=nodes),
            ),
        )
        assert response.status_code == 200, response.json()
        assert self._materialization_results(response.json()) == [
            (
                cube_name,
                "delete",
                "success",
                "cube materialization removed by `materialization: none`",
            ),
        ]
        assert mock_qs.method_calls == []
        assert await self._persisted_materializations(client, cube_name) == [
            (
                self._materialization_name(namespace),
                "0 6 * * *",
                "incremental_time",
                True,
            ),
        ]

    @pytest.mark.asyncio
    async def test_unmaterializable_cube_fails_alone(
        self,
        client,
        mocker,
        upstreams,
        default_num_repair_orders,
        mock_qs,
    ):
        """
        A cube DJ cannot build a materialization for -- measures queries at different
        grains, say -- fails with an error naming it, and does not take the other
        declared cube in the same push down with it.
        """
        namespace = "cube_mat_ungrainable"
        broken_name = f"{namespace}.default.broken_cube"
        working_name = f"{namespace}.default.repairs_cube"
        grain_error = (
            "DJ cannot manage materializations for cubes that have underlying "
            "measures queries at different grains"
        )
        real_build = (
            datajunction_server.internal.materializations.build_cube_materialization
        )

        async def build(*args, **kwargs):
            if kwargs["current_revision"].name == broken_name:
                raise DJInvalidInputException(grain_error)
            return await real_build(*args, **kwargs)

        mocker.patch(
            "datajunction_server.internal.materializations.build_cube_materialization",
            side_effect=build,
        )
        nodes = [
            *upstreams,
            default_num_repair_orders,
            self._cube(
                materialization=MaterializationSpec(
                    schedule="0 6 * * *",
                    lookback_window="1 DAY",
                ),
            ),
            self._cube(
                name="default.broken_cube",
                metrics=["${prefix}default.num_repair_orders"],
                materialization=MaterializationSpec(
                    schedule="0 7 * * *",
                    lookback_window="1 DAY",
                ),
            ),
        ]
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=nodes),
        )
        assert self._materialization_results(data) == [
            (
                working_name,
                "create",
                "success",
                "cube materialization on schedule 0 6 * * *",
            ),
            (
                broken_name,
                "unknown",
                "failed",
                f"Cube `{broken_name}`: {grain_error}",
            ),
        ]
        assert mock_qs.materialize_cube.call_count == 1
        scheduled = mock_qs.materialize_cube.call_args.kwargs["materialization_input"]
        assert scheduled.cube.name == working_name
        assert await self._persisted_materializations(client, broken_name) == []

    @pytest.mark.asyncio
    async def test_two_declared_materializations_are_both_deployed(
        self,
        client,
        session,
        upstreams,
        mock_qs,
    ):
        """
        A cube declaring an incremental build beside a periodic full rebuild gets
        both, and neither supersedes the other.

        This is what production already runs on cubes materialized outside YAML: the
        incremental one keeps the datasource fresh, and the full one periodically
        rewrites it to pick up late-arriving data, out-of-order events and dimension
        backfills.
        """
        namespace = "cube_mat_two_declared"
        cube_name = f"{namespace}.default.repairs_cube"
        full_name = f"druid_cube__full__{namespace}.default.hard_hat.hire_date"
        nodes = [
            *upstreams,
            self._cube(
                materialization=[
                    MaterializationSpec(
                        schedule="59 23 * * *",
                        lookback_window="1 DAY",
                    ),
                    MaterializationSpec(
                        schedule="0 6 * * *",
                        strategy=MaterializationStrategy.FULL,
                    ),
                ],
            ),
        ]
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=nodes),
        )
        assert data["status"] == "success", data
        assert self._materialization_results(data) == [
            (
                cube_name,
                "create",
                "success",
                "cube materialization on schedule 59 23 * * *",
            ),
            (
                cube_name,
                "create",
                "success",
                "cube materialization on schedule 0 6 * * *",
            ),
        ]
        assert mock_qs.materialize_cube.call_count == 2
        assert mock_qs.deactivate_cube_workflow.call_args_list == []
        assert await self._persisted_materializations(client, cube_name) == [
            (
                self._materialization_name(namespace),
                "59 23 * * *",
                "incremental_time",
                True,
            ),
            (full_name, "0 6 * * *", "full", True),
        ]

        # Both round-trip back out, so a cube pulled and pushed keeps both.
        session.expire_all()
        deployed = await Node.get_by_name(
            session,
            cube_name,
            options=Node.cube_load_options(),
        )
        assert (await deployed.to_spec(session)).materialization == [
            MaterializationSpec(
                schedule="0 6 * * *",
                strategy=MaterializationStrategy.FULL,
                lookback_window=None,
            ),
            MaterializationSpec(
                schedule="59 23 * * *",
                strategy=MaterializationStrategy.INCREMENTAL_TIME,
                lookback_window="1 DAY",
            ),
        ]

        # And re-pushing the pair unchanged churns neither live workflow.
        mock_qs.reset_mock()
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=nodes),
        )
        assert data["status"] == "success", data
        assert self._materialization_results(data) == [
            (
                cube_name,
                "noop",
                "success",
                "cube materialization on schedule 59 23 * * *",
            ),
            (
                cube_name,
                "noop",
                "success",
                "cube materialization on schedule 0 6 * * *",
            ),
        ]
        assert mock_qs.method_calls == []

    @pytest.mark.asyncio
    async def test_platform_settings_follow_each_declared_block(
        self,
        client,
        session,
        upstreams,
        mock_qs,
    ):
        """
        `druid`, `spark` and `platform` are declared per block, so the incremental
        build and the full rebuild of one cube each carry their own.

        `spark.default` rides every stage and `spark.combiner` merges over it on
        the combine stage alone, `druid` is merged into the generated ingestion
        spec, and `platform` reaches the query service untouched. The block that
        declares none of the three keeps all three absent.
        """
        namespace = "cube_mat_platform"
        cube_name = f"{namespace}.default.repairs_cube"
        incremental_name = self._materialization_name(namespace)
        full_name = f"druid_cube__full__{namespace}.default.hard_hat.hire_date"
        nodes = [
            *upstreams,
            self._cube(
                materialization=[
                    MaterializationSpec(
                        schedule="59 23 * * *",
                        lookback_window="1 DAY",
                        druid={
                            "tuningConfig": {
                                "partitionsSpec": {"targetRowsPerSegment": 1000000},
                            },
                        },
                        spark=SparkSpec(
                            default={
                                "spark.executor.memory": "8g",
                                "spark.sql.shuffle.partitions": "800",
                            },
                            combiner={"spark.sql.shuffle.partitions": "200"},
                        ),
                        platform={"wait_for_vtts": False},
                    ),
                    MaterializationSpec(
                        schedule="0 6 * * *",
                        strategy=MaterializationStrategy.FULL,
                    ),
                ],
            ),
        ]
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=nodes),
        )
        assert data["status"] == "success", data

        session.expire_all()
        deployed = await Node.get_by_name(
            session,
            cube_name,
            options=Node.cube_load_options(),
        )
        configs = {
            materialization.name: materialization.config
            for materialization in deployed.current.materializations
        }
        assert configs[incremental_name]["druid"] == {
            "tuningConfig": {"partitionsSpec": {"targetRowsPerSegment": 1000000}},
        }
        assert configs[incremental_name]["spark"] == {
            "default": {
                "spark.executor.memory": "8g",
                "spark.sql.shuffle.partitions": "800",
            },
            "combiner": {"spark.sql.shuffle.partitions": "200"},
            "measures": None,
        }
        assert configs[incremental_name]["platform"] == {"wait_for_vtts": False}
        assert configs[full_name]["druid"] is None
        assert configs[full_name]["spark"] is None
        assert configs[full_name]["platform"] is None

        # `default` reaches both stages, `combiner` overrides only the combine one.
        payloads = {
            call.kwargs["materialization_input"].name: call.kwargs[
                "materialization_input"
            ]
            for call in mock_qs.materialize_cube.call_args_list
        }
        incremental = payloads[incremental_name]
        assert incremental.platform == {"wait_for_vtts": False}
        assert [
            measures.spark_conf for measures in incremental.measures_materializations
        ] == [
            {
                "spark.executor.memory": "8g",
                "spark.sql.shuffle.partitions": "800",
            },
        ]
        assert [combiner.spark_conf for combiner in incremental.combiners] == [
            {
                "spark.executor.memory": "8g",
                "spark.sql.shuffle.partitions": "200",
            },
        ]
        # `druid` overrides one leaf and leaves its siblings alone.
        assert incremental.combiners[0].druid_spec["tuningConfig"] == {
            "partitionsSpec": {
                "targetPartitionSize": 5000000,
                "targetRowsPerSegment": 1000000,
                "type": "hashed",
            },
            "useCombiner": True,
            "type": "hadoop",
        }
        full = payloads[full_name]
        assert full.platform is None
        assert [measures.spark_conf for measures in full.measures_materializations] == [
            None,
        ]
        assert [combiner.spark_conf for combiner in full.combiners] == [None]
        assert full.combiners[0].druid_spec["tuningConfig"] == {
            "partitionsSpec": {"targetPartitionSize": 5000000, "type": "hashed"},
            "useCombiner": True,
            "type": "hadoop",
        }

        # All three round-trip, and the block that declared none keeps none.
        exported = await deployed.to_spec(session)
        assert exported.materialization == [
            MaterializationSpec(
                schedule="0 6 * * *",
                strategy=MaterializationStrategy.FULL,
                lookback_window=None,
            ),
            MaterializationSpec(
                schedule="59 23 * * *",
                strategy=MaterializationStrategy.INCREMENTAL_TIME,
                lookback_window="1 DAY",
                druid={
                    "tuningConfig": {
                        "partitionsSpec": {"targetRowsPerSegment": 1000000},
                    },
                },
                spark=SparkSpec(
                    default={
                        "spark.executor.memory": "8g",
                        "spark.sql.shuffle.partitions": "800",
                    },
                    combiner={"spark.sql.shuffle.partitions": "200"},
                ),
                platform={"wait_for_vtts": False},
            ),
        ]
        assert exported.model_dump(mode="json", exclude_none=True)[
            "materialization"
        ] == [
            {
                "schedule": "0 6 * * *",
                "strategy": "full",
                "retention": "400 DAYS",
            },
            {
                "schedule": "59 23 * * *",
                "strategy": "incremental_time",
                "lookback_window": "1 DAY",
                "retention": "400 DAYS",
                "druid": {
                    "tuningConfig": {
                        "partitionsSpec": {"targetRowsPerSegment": 1000000},
                    },
                },
                "spark": {
                    "default": {
                        "spark.executor.memory": "8g",
                        "spark.sql.shuffle.partitions": "800",
                    },
                    "combiner": {"spark.sql.shuffle.partitions": "200"},
                },
                "platform": {"wait_for_vtts": False},
            },
        ]

        # Re-pushing the same settings churns neither live workflow.
        mock_qs.reset_mock()
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=nodes),
        )
        assert data["status"] == "success", data
        assert self._materialization_results(data) == [
            (
                cube_name,
                "noop",
                "success",
                "cube materialization on schedule 59 23 * * *",
            ),
            (
                cube_name,
                "noop",
                "success",
                "cube materialization on schedule 0 6 * * *",
            ),
        ]
        assert mock_qs.method_calls == []

        # Editing one of them registers as a change, and reschedules only it.
        mock_qs.reset_mock()
        nodes[-1].materialization[0].spark = SparkSpec(
            default={"spark.executor.memory": "8g"},
            combiner={"spark.sql.shuffle.partitions": "200"},
        )
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=nodes),
        )
        assert data["status"] == "success", data
        rescheduled = [
            call.kwargs["materialization_input"]
            for call in mock_qs.materialize_cube.call_args_list
        ]
        assert [payload.name for payload in rescheduled] == [incremental_name]
        assert [
            measures.spark_conf for measures in rescheduled[0].measures_materializations
        ] == [{"spark.executor.memory": "8g"}]

    @staticmethod
    def _fact(name: str) -> TransformSpec:
        """A fact transform over the hard hats source, joined to `hard_hat`."""
        return TransformSpec(
            name=name,
            description="Hard hat activity",
            query=(
                "SELECT hard_hat_id, state, hire_date FROM ${prefix}default.hard_hats"
            ),
            dimension_links=[
                DimensionJoinLinkSpec(
                    dimension_node="${prefix}default.hard_hat",
                    join_type="inner",
                    join_on=(
                        f"${{prefix}}{name}.hard_hat_id = "
                        "${prefix}default.hard_hat.hard_hat_id"
                    ),
                ),
            ],
            owners=["dj"],
        )

    @pytest.mark.asyncio
    async def test_spark_conf_is_declared_per_parent(
        self,
        client,
        session,
        default_hard_hats,
        default_hard_hat,
        default_us_states,
        default_us_state,
        mock_qs,
    ):
        """
        A cube spanning two parents sizes each parent's measures job on its own.

        `spark.measures` is keyed by parent node name and merges over `default`, so
        the parent it names gets its own conf while the other keeps `default`. A key
        naming no parent is a typo: it warns and the deploy goes on.
        """
        namespace = "cube_mat_spark_parents"
        cube_name = f"{namespace}.default.hard_hat_cube"
        big = f"{namespace}.default.hard_hat_repairs"
        small = f"{namespace}.default.hard_hat_hires"
        materialization_name = (
            f"druid_cube__incremental_time__{namespace}.default.hard_hat.hire_date"
        )
        typo = f"{namespace}.default.hard_hat_repair"
        nodes = [
            default_hard_hats,
            default_hard_hat,
            default_us_states,
            default_us_state,
            self._fact("default.hard_hat_repairs"),
            self._fact("default.hard_hat_hires"),
            MetricSpec(
                name="default.num_repairs",
                description="Number of repairs",
                query=(
                    "SELECT count(hard_hat_id) FROM ${prefix}default.hard_hat_repairs"
                ),
                owners=["dj"],
            ),
            MetricSpec(
                name="default.num_hires",
                description="Number of hires",
                query=(
                    "SELECT count(hard_hat_id) FROM ${prefix}default.hard_hat_hires"
                ),
                owners=["dj"],
            ),
            CubeSpec(
                name="default.hard_hat_cube",
                display_name="Hard Hat Cube",
                description="Cube spanning two fact tables",
                dimensions=[
                    "${prefix}default.hard_hat.state",
                    "${prefix}default.hard_hat.hire_date",
                ],
                metrics=[
                    "${prefix}default.num_repairs",
                    "${prefix}default.num_hires",
                ],
                owners=["dj"],
                columns=[
                    ColumnSpec(
                        name="${prefix}default.hard_hat.hire_date",
                        partition=PartitionSpec(
                            type=PartitionType.TEMPORAL,
                            granularity=Granularity.DAY,
                            format="yyyyMMdd",
                        ),
                    ),
                ],
                materialization=MaterializationSpec(
                    schedule="0 6 * * *",
                    spark=SparkSpec(
                        default={"spark.executor.memory": "8g"},
                        measures={
                            "${prefix}default.hard_hat_repairs": {
                                "spark.executor.memory": "32g",
                                "spark.executor.cores": "8",
                            },
                            "${prefix}default.hard_hat_repair": {
                                "spark.executor.memory": "64g",
                            },
                        },
                    ),
                ),
            ),
        ]
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=nodes),
        )
        assert data["status"] == "success", data

        warning = (
            f"Cube `{cube_name}`: `spark.measures` names "
            f"`{typo}`, which no measures job of this cube is "
            "built on, so that config was ignored."
        )
        assert self._materialization_results(data) == [
            (cube_name, "noop", "warning", warning),
            (
                cube_name,
                "create",
                "success",
                "cube materialization on schedule 0 6 * * *",
            ),
        ]

        payload = mock_qs.materialize_cube.call_args.kwargs["materialization_input"]
        assert {
            measures.node.name: measures.spark_conf
            for measures in payload.measures_materializations
        } == {
            big: {"spark.executor.memory": "32g", "spark.executor.cores": "8"},
            small: {"spark.executor.memory": "8g"},
        }
        assert [combiner.spark_conf for combiner in payload.combiners] == [
            {"spark.executor.memory": "8g"},
        ]

        # The structured form round-trips, unmatched key and all.
        session.expire_all()
        deployed = await Node.get_by_name(
            session,
            cube_name,
            options=Node.cube_load_options(),
        )
        exported = await deployed.to_spec(session)
        assert exported.materialization == MaterializationSpec(
            schedule="0 6 * * *",
            strategy=MaterializationStrategy.INCREMENTAL_TIME,
            lookback_window="1 DAY",
            spark=SparkSpec(
                default={"spark.executor.memory": "8g"},
                measures={
                    big: {
                        "spark.executor.memory": "32g",
                        "spark.executor.cores": "8",
                    },
                    typo: {"spark.executor.memory": "64g"},
                },
            ),
        )

        # Re-pushing the same conf churns nothing.
        mock_qs.reset_mock()
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=nodes),
        )
        assert data["status"] == "success", data
        assert self._materialization_results(data) == [
            (cube_name, "noop", "warning", warning),
            (
                cube_name,
                "noop",
                "success",
                "cube materialization on schedule 0 6 * * *",
            ),
        ]
        assert mock_qs.method_calls == []

        # Retuning one parent reschedules the cube with only that parent moved.
        mock_qs.reset_mock()
        nodes[-1].materialization.spark = SparkSpec(
            default={"spark.executor.memory": "8g"},
            measures={
                "${prefix}default.hard_hat_repairs": {
                    "spark.executor.memory": "64g",
                },
            },
        )
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=nodes),
        )
        assert data["status"] == "success", data
        payload = mock_qs.materialize_cube.call_args.kwargs["materialization_input"]
        assert payload.name == materialization_name
        assert {
            measures.node.name: measures.spark_conf
            for measures in payload.measures_materializations
        } == {
            big: {"spark.executor.memory": "64g"},
            small: {"spark.executor.memory": "8g"},
        }

    @pytest.mark.asyncio
    async def test_adding_a_full_rebuild_leaves_the_incremental_alone(
        self,
        client,
        upstreams,
        mock_qs,
    ):
        """
        Growing a scalar declaration into a pair adds the second materialization
        without touching the first: the incremental build is unchanged, so its
        workflow is neither stopped nor rescheduled.
        """
        namespace = "cube_mat_grown"
        cube_name = f"{namespace}.default.repairs_cube"
        full_name = f"druid_cube__full__{namespace}.default.hard_hat.hire_date"
        incremental = MaterializationSpec(
            schedule="59 23 * * *",
            lookback_window="1 DAY",
        )
        nodes = [*upstreams, self._cube(materialization=incremental)]
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=nodes),
        )
        assert data["status"] == "success", data
        assert mock_qs.materialize_cube.call_count == 1

        mock_qs.reset_mock()
        nodes[-1] = self._cube(
            materialization=[
                incremental,
                MaterializationSpec(
                    schedule="0 6 * * *",
                    strategy=MaterializationStrategy.FULL,
                ),
            ],
        )
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=nodes),
        )
        assert data["status"] == "success", data
        assert self._materialization_results(data) == [
            (
                cube_name,
                "update",
                "success",
                "cube materialization on schedule 59 23 * * *",
            ),
            (
                cube_name,
                "update",
                "success",
                "cube materialization on schedule 0 6 * * *",
            ),
        ]
        # Only the new one is scheduled, and nothing is stopped.
        assert [call[0] for call in mock_qs.method_calls] == ["materialize_cube"]
        scheduled = mock_qs.materialize_cube.call_args.kwargs["materialization_input"]
        assert scheduled.strategy == MaterializationStrategy.FULL
        assert await self._persisted_materializations(client, cube_name) == [
            (
                self._materialization_name(namespace),
                "59 23 * * *",
                "incremental_time",
                True,
            ),
            (full_name, "0 6 * * *", "full", True),
        ]

    @pytest.mark.asyncio
    async def test_definition_edit_rebuilds_every_declared_materialization(
        self,
        client,
        upstreams,
        default_num_repair_orders,
        mock_qs,
    ):
        """
        A cube edit swaps its materializations onto the new revision, and a cube that
        declares two must come out of that with two -- rebuilt under the strategy
        each was declared with rather than collapsed onto whichever block came first.
        """
        namespace = "cube_mat_two_rebuilt"
        cube_name = f"{namespace}.default.repairs_cube"
        full_name = f"druid_cube__full__{namespace}.default.hard_hat.hire_date"
        blocks = [
            MaterializationSpec(schedule="59 23 * * *", lookback_window="1 DAY"),
            MaterializationSpec(
                schedule="0 6 * * *",
                strategy=MaterializationStrategy.FULL,
            ),
        ]
        nodes = [
            *upstreams,
            default_num_repair_orders,
            self._cube(materialization=blocks),
        ]
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=nodes),
        )
        assert data["status"] == "success", data

        mock_qs.reset_mock()
        nodes[-1] = self._cube(
            metrics=[
                "${prefix}default.avg_length_of_employment",
                "${prefix}default.num_repair_orders",
            ],
            materialization=blocks,
        )
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=nodes),
        )
        assert data["status"] == "success", data
        assert mock_qs.deactivate_cube_workflow.call_args_list == [
            mock.call(cube_name, version="v1.0", request_headers=mock.ANY),
        ]
        assert await self._persisted_materializations(client, cube_name) == [
            (
                self._materialization_name(namespace),
                "59 23 * * *",
                "incremental_time",
                True,
            ),
            (full_name, "0 6 * * *", "full", True),
        ]

    @pytest.mark.asyncio
    async def test_explicit_none_removes_every_declared_materialization(
        self,
        client,
        upstreams,
        mock_qs,
    ):
        """
        The teardown sentinel still means the whole cube, not one of its
        materializations: a cube that declared two comes down entirely.
        """
        namespace = "cube_mat_two_removed"
        cube_name = f"{namespace}.default.repairs_cube"
        full_name = f"druid_cube__full__{namespace}.default.hard_hat.hire_date"
        nodes = [
            *upstreams,
            self._cube(
                materialization=[
                    MaterializationSpec(
                        schedule="59 23 * * *",
                        lookback_window="1 DAY",
                    ),
                    MaterializationSpec(
                        schedule="0 6 * * *",
                        strategy=MaterializationStrategy.FULL,
                    ),
                ],
            ),
        ]
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=nodes),
        )
        assert data["status"] == "success", data

        mock_qs.reset_mock()
        nodes[-1] = self._cube(materialization=MaterializationAction.NONE)
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=nodes),
        )
        assert data["status"] == "success", data
        assert self._materialization_results(data) == [
            (
                cube_name,
                "delete",
                "success",
                "cube materialization removed by `materialization: none`",
            ),
        ]
        assert mock_qs.deactivate_cube_workflow.call_args_list == [
            mock.call(cube_name, version="v1.0", request_headers=mock.ANY),
        ]
        assert await self._persisted_materializations(client, cube_name) == [
            (
                self._materialization_name(namespace),
                "59 23 * * *",
                "incremental_time",
                False,
            ),
            (full_name, "0 6 * * *", "full", False),
        ]

    @pytest.mark.asyncio
    async def test_dropping_a_block_stops_it(
        self,
        client,
        upstreams,
        mock_qs,
    ):
        """
        Shrinking a declared pair back to one takes the dropped materialization down.

        The blocks are the whole list, so deleting the full rebuild from YAML has to
        stop it -- left running it keeps rewriting the datasource the incremental
        build loads, and nothing in the repo records that it exists.

        The stop is what makes the survivor churn. DJ has no workflow name for a
        materialization it built here, so it falls back to the cube-and-version
        endpoint, which stops every workflow on the revision including the one still
        declared. The reconciler answers that by treating an otherwise unchanged
        block as changed whenever anything was superseded, so the incremental build
        is scheduled again rather than silently left stopped.
        """
        namespace = "cube_mat_shrunk"
        cube_name = f"{namespace}.default.repairs_cube"
        full_name = f"druid_cube__full__{namespace}.default.hard_hat.hire_date"
        incremental = MaterializationSpec(
            schedule="59 23 * * *",
            lookback_window="1 DAY",
        )
        nodes = [
            *upstreams,
            self._cube(
                materialization=[
                    incremental,
                    MaterializationSpec(
                        schedule="0 6 * * *",
                        strategy=MaterializationStrategy.FULL,
                    ),
                ],
            ),
        ]
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=nodes),
        )
        assert data["status"] == "success", data
        assert await self._persisted_materializations(client, cube_name) == [
            (
                self._materialization_name(namespace),
                "59 23 * * *",
                "incremental_time",
                True,
            ),
            (full_name, "0 6 * * *", "full", True),
        ]

        mock_qs.reset_mock()
        nodes[-1] = self._cube(materialization=incremental)
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=nodes),
        )
        assert data["status"] == "success", data
        assert self._materialization_results(data) == [
            (
                cube_name,
                "update",
                "success",
                "cube materialization on schedule 59 23 * * *",
            ),
        ]
        assert [call[0] for call in mock_qs.method_calls] == [
            "deactivate_cube_workflow",
            "materialize_cube",
        ]
        assert mock_qs.deactivate_cube_workflow.call_args_list == [
            mock.call(cube_name, version="v1.0", request_headers=mock.ANY),
        ]
        rescheduled = mock_qs.materialize_cube.call_args.kwargs["materialization_input"]
        assert rescheduled.strategy == MaterializationStrategy.INCREMENTAL_TIME
        assert await self._persisted_materializations(client, cube_name) == [
            (
                self._materialization_name(namespace),
                "59 23 * * *",
                "incremental_time",
                True,
            ),
            (full_name, "0 6 * * *", "full", False),
        ]

    @pytest.mark.asyncio
    async def test_declared_block_supersedes_a_legacy_row(
        self,
        client,
        upstreams,
        mock_qs,
    ):
        """
        A materialization made outside the deploy is taken down by a deploy that
        declares a block, even when no block could have built it.

        This is the two-writers case: someone materialized the cube through the API
        or the UI, and a later push of YAML that declares its own block removes what
        they set up. The older `druid_measures_cube` job cannot be declared at all,
        so no block ever builds its name -- and it is superseded anyway, because
        every materialization on a cube writes the one Druid datasource and matching
        on job type would leave this one running.
        """
        namespace = "cube_mat_legacy_supersede"
        cube_name = f"{namespace}.default.repairs_cube"
        legacy_name = (
            f"druid_measures_cube__incremental_time__"
            f"{namespace}.default.hard_hat.hire_date"
        )
        nodes = [
            *upstreams,
            self._cube(),
        ]
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=nodes),
        )
        assert data["status"] == "success", data

        response = await client.post(
            f"/nodes/{cube_name}/materialization/",
            json={
                "job": "druid_measures_cube",
                "strategy": "incremental_time",
                "schedule": "@daily",
                "config": {"spark": {}, "lookback_window": "1 DAY"},
            },
        )
        assert response.status_code == 200, response.json()

        mock_qs.reset_mock()
        nodes[-1] = self._cube(
            materialization=MaterializationSpec(
                schedule="0 6 * * *",
                lookback_window="1 DAY",
            ),
        )
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=nodes),
        )
        assert data["status"] == "success", data
        assert self._materialization_results(data) == [
            (
                cube_name,
                "create",
                "success",
                "cube materialization on schedule 0 6 * * *",
            ),
        ]
        assert [call[0] for call in mock_qs.method_calls] == [
            "deactivate_cube_workflow",
            "materialize_cube",
        ]
        assert mock_qs.deactivate_cube_workflow.call_args_list == [
            mock.call(cube_name, version="v1.0", request_headers=mock.ANY),
        ]
        assert await self._persisted_materializations(client, cube_name) == [
            (legacy_name, "@daily", "incremental_time", False),
            (
                self._materialization_name(namespace),
                "0 6 * * *",
                "incremental_time",
                True,
            ),
        ]

    @pytest.mark.asyncio
    async def test_absent_key_leaves_a_pair_alone(
        self,
        client,
        upstreams,
        mock_qs,
    ):
        """
        Omitting the key still means "not managed here" for a cube with more than one
        materialization, so both keep running and the deploy warns once.

        Worth pinning beside the pair the sentinel tears down: the two spellings are
        the only thing between adopting a cube into YAML and stopping every workflow
        it has.
        """
        namespace = "cube_mat_pair_undeclared"
        cube_name = f"{namespace}.default.repairs_cube"
        full_name = f"druid_cube__full__{namespace}.default.hard_hat.hire_date"
        nodes = [
            *upstreams,
            self._cube(
                materialization=[
                    MaterializationSpec(
                        schedule="59 23 * * *",
                        lookback_window="1 DAY",
                    ),
                    MaterializationSpec(
                        schedule="0 6 * * *",
                        strategy=MaterializationStrategy.FULL,
                    ),
                ],
            ),
        ]
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=nodes),
        )
        assert data["status"] == "success", data

        mock_qs.reset_mock()
        nodes[-1] = self._cube()
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=nodes),
        )
        assert data["status"] == "success", data
        warning = self._undeclared_warning(cube_name)
        assert self._materialization_results(data) == [
            (cube_name, "noop", "warning", warning),
        ]
        assert [item["message"] for item in data["warnings"]] == [warning]
        assert mock_qs.method_calls == []
        assert await self._persisted_materializations(client, cube_name) == [
            (
                self._materialization_name(namespace),
                "59 23 * * *",
                "incremental_time",
                True,
            ),
            (full_name, "0 6 * * *", "full", True),
        ]

    @pytest.mark.asyncio
    async def test_one_strategy_exports_once(
        self,
        session,
        client,
        upstreams,
        mock_qs,
    ):
        """
        A cube carrying two rows of one strategy exports a block naming it once.

        Two rows of a strategy differ only by their partition suffix, so a block
        cannot describe both -- and strategy is what tells declared blocks apart, so
        naming it twice is YAML the parser rejects. Only the first by name is
        exported, which keeps `dj pull` on such a cube writing a file that pushes.
        """
        namespace = "cube_mat_dup_strategy"
        cube_name = f"{namespace}.default.repairs_cube"
        nodes = [
            *upstreams,
            self._cube(
                materialization=MaterializationSpec(
                    schedule="0 6 * * *",
                    lookback_window="1 DAY",
                ),
            ),
        ]
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=nodes),
        )
        assert data["status"] == "success", data

        # A second row of the same strategy, on another partition.
        cube = await Node.get_by_name(session, cube_name)
        session.add(
            Materialization(
                node_revision_id=cube.current.id,
                name="druid_cube__incremental_time__zzz.other_partition",
                strategy=MaterializationStrategy.INCREMENTAL_TIME,
                schedule="@daily",
                config={"lookback_window": "3 DAY"},
                job="DruidCubeMaterializationJob",
            ),
        )
        await session.commit()

        session.expire_all()
        deployed = await Node.get_by_name(
            session,
            cube_name,
            options=Node.cube_load_options(),
        )
        assert (await deployed.to_spec(session)).materialization == (
            MaterializationSpec(
                schedule="0 6 * * *",
                strategy=MaterializationStrategy.INCREMENTAL_TIME,
                lookback_window="1 DAY",
            )
        )


@pytest.mark.xdist_group(name="deployments")
class TestDeploymentHistoryTracking:
    """Tests for history tracking during YAML deployments"""

    @pytest.mark.asyncio
    async def test_deployment_creates_history_for_nodes(
        self,
        client,
        default_hard_hats,
        default_us_states,
        default_us_state,
    ):
        """
        Test that deploying nodes creates history entries for each create/update operation
        """
        namespace = "history_test"
        dim_spec = DimensionSpec(
            name="default.hard_hat",
            description="""Hard hat dimension""",
            query="""
                SELECT
                    hard_hat_id,
                    last_name,
                    first_name,
                    state
                FROM ${prefix}default.hard_hats
            """,
            primary_key=["hard_hat_id"],
            owners=["dj"],
        )

        # Deploy nodes
        data = await deploy_and_wait(
            client,
            DeploymentSpec(
                namespace=namespace,
                nodes=[
                    default_hard_hats,
                    default_us_states,
                    default_us_state,
                    dim_spec,
                ],
            ),
        )
        assert data["status"] == DeploymentStatus.SUCCESS.value

        # Check history for source node
        response = await client.get(
            f"/history/node/{namespace}.default.hard_hats/",
        )
        assert response.status_code == 200
        history = response.json()
        assert len(history) >= 1
        # Find the create event
        create_events = [h for h in history if h["activity_type"] == "create"]
        assert len(create_events) == 1
        assert create_events[0]["entity_type"] == "node"
        assert "deployment_id" in create_events[0]["details"]

        # Check history for dimension node
        response = await client.get(
            f"/history/node/{namespace}.default.hard_hat/",
        )
        assert response.status_code == 200
        history = response.json()
        assert len(history) >= 1
        create_events = [h for h in history if h["activity_type"] == "create"]
        assert len(create_events) == 1
        assert create_events[0]["entity_type"] == "node"
        assert "deployment_id" in create_events[0]["details"]

    @pytest.mark.asyncio
    async def test_deployment_creates_history_for_updates(
        self,
        client,
        default_hard_hats,
    ):
        """
        Test that updating nodes via deployment creates history entries
        """
        namespace = "history_update_test"

        # First deployment - create
        data = await deploy_and_wait(
            client,
            DeploymentSpec(
                namespace=namespace,
                nodes=[default_hard_hats],
            ),
        )
        assert data["status"] == DeploymentStatus.SUCCESS.value

        # Second deployment - update description
        updated_hard_hats = SourceSpec(
            name=default_hard_hats.name,
            description="Updated description for hard hats table",
            catalog=default_hard_hats.catalog,
            schema=default_hard_hats.schema_,
            table=default_hard_hats.table,
            columns=default_hard_hats.columns,
            owners=default_hard_hats.owners,
        )
        data = await deploy_and_wait(
            client,
            DeploymentSpec(
                namespace=namespace,
                nodes=[updated_hard_hats],
            ),
        )
        assert data["status"] == DeploymentStatus.SUCCESS.value

        # Check history shows both create and update
        response = await client.get(
            f"/history/node/{namespace}.default.hard_hats/",
        )
        assert response.status_code == 200
        history = response.json()
        create_events = [h for h in history if h["activity_type"] == "create"]
        update_events = [h for h in history if h["activity_type"] == "update"]
        assert len(create_events) >= 1
        assert len(update_events) >= 1
        # Verify deployment_id is tracked
        assert "deployment_id" in create_events[0]["details"]
        assert "deployment_id" in update_events[0]["details"]

    @pytest.mark.asyncio
    async def test_deployment_creates_history_for_dimension_links(
        self,
        client,
        default_hard_hats,
        default_us_states,
        default_us_state,
    ):
        """
        Test that deploying nodes with dimension links creates history entries for links
        """
        namespace = "history_link_test"
        dim_spec = DimensionSpec(
            name="default.hard_hat",
            description="""Hard hat dimension""",
            query="""
                SELECT
                    hard_hat_id,
                    last_name,
                    first_name,
                    state
                FROM ${prefix}default.hard_hats
            """,
            primary_key=["hard_hat_id"],
            dimension_links=[
                DimensionJoinLinkSpec(
                    dimension_node="${prefix}default.us_state",
                    join_type="inner",
                    join_on="${prefix}default.hard_hat.state = ${prefix}default.us_state.state_short",
                ),
            ],
            owners=["dj"],
        )

        # Deploy nodes with dimension links
        data = await deploy_and_wait(
            client,
            DeploymentSpec(
                namespace=namespace,
                nodes=[
                    default_hard_hats,
                    default_us_states,
                    default_us_state,
                    dim_spec,
                ],
            ),
        )
        assert data["status"] == DeploymentStatus.SUCCESS.value

        # Check history for dimension links
        response = await client.get(
            f"/history?node={namespace}.default.hard_hat",
        )
        assert response.status_code == 200
        history = response.json()

        # Should have link creation history
        link_events = [h for h in history if h["entity_type"] == "link"]
        assert len(link_events) >= 1
        link_create_events = [h for h in link_events if h["activity_type"] == "create"]
        assert len(link_create_events) >= 1
        # Verify link details are tracked
        assert "dimension_node" in link_create_events[0]["details"]
        assert "deployment_id" in link_create_events[0]["details"]

    @pytest.mark.asyncio
    async def test_git_author_recorded_in_history(
        self,
        client,
        default_hard_hats,
    ):
        """
        When a deployment includes a GitDeploymentSource with commit author info,
        history events should record the git author rather than the service account.
        """
        namespace = "history_git_author_test"

        data = await deploy_and_wait(
            client,
            DeploymentSpec(
                namespace=namespace,
                nodes=[default_hard_hats],
                source=GitDeploymentSource(
                    repository="github.com/org/repo",
                    branch="main",
                    commit_sha="abc123",
                    commit_author_email="alice@example.com",
                    commit_author_name="Alice Smith",
                ),
            ),
        )
        assert data["status"] == DeploymentStatus.SUCCESS.value

        response = await client.get(
            f"/history/node/{namespace}.default.hard_hats/",
        )
        assert response.status_code == 200
        history = response.json()
        create_events = [h for h in history if h["activity_type"] == "create"]
        assert len(create_events) >= 1
        assert create_events[0]["user"] == "alice@example.com"

    @pytest.mark.asyncio
    async def test_git_author_email_used_when_no_name(
        self,
        client,
        default_hard_hats,
    ):
        """When commit_author_name is absent, history falls back to commit_author_email."""
        namespace = "history_git_author_email_test"

        data = await deploy_and_wait(
            client,
            DeploymentSpec(
                namespace=namespace,
                nodes=[default_hard_hats],
                source=GitDeploymentSource(
                    repository="github.com/org/repo",
                    branch="main",
                    commit_author_email="alice@example.com",
                ),
            ),
        )
        assert data["status"] == DeploymentStatus.SUCCESS.value

        response = await client.get(
            f"/history/node/{namespace}.default.hard_hats/",
        )
        assert response.status_code == 200
        history = response.json()
        create_events = [h for h in history if h["activity_type"] == "create"]
        assert len(create_events) >= 1
        assert create_events[0]["user"] == "alice@example.com"

    @pytest.mark.asyncio
    async def test_service_account_used_when_no_git_author(
        self,
        client,
        default_hard_hats,
    ):
        """When no git author is present, history records the authenticated user."""
        namespace = "history_no_git_author_test"

        data = await deploy_and_wait(
            client,
            DeploymentSpec(
                namespace=namespace,
                nodes=[default_hard_hats],
                source=GitDeploymentSource(
                    repository="github.com/org/repo",
                    branch="main",
                    commit_sha="abc123",
                    # No commit_author_email or commit_author_name
                ),
            ),
        )
        assert data["status"] == DeploymentStatus.SUCCESS.value

        response = await client.get(
            f"/history/node/{namespace}.default.hard_hats/",
        )
        assert response.status_code == 200
        history = response.json()
        create_events = [h for h in history if h["activity_type"] == "create"]
        assert len(create_events) >= 1
        # Should fall back to the authenticated test user, not a git author
        assert create_events[0]["user"] is not None
        assert "@example.com" not in create_events[0]["user"]


@pytest.mark.xdist_group(name="deployments")
class TestDeploymentColumnOrdering:
    """Tests for column ordering in deployments"""

    @pytest.mark.asyncio
    async def test_deployment_preserves_column_order(self, client):
        """
        Test that column order is preserved for both source specs and
        inferred columns from transform queries.
        """
        namespace = "column_order_test"

        # Create a source with columns in non-alphabetical order
        source_spec = SourceSpec(
            name="test_source",
            description="Test source",
            catalog="default",
            schema="test_schema",
            table="test_table",
            columns=[
                ColumnSpec(name="z_column", type="string"),
                ColumnSpec(name="a_column", type="int"),
                ColumnSpec(name="m_column", type="timestamp"),
                ColumnSpec(name="b_column", type="float"),
            ],
        )

        # Create a transform that reorders the columns
        transform_spec = TransformSpec(
            name="test_transform",
            description="Test transform for column ordering",
            query="""
                SELECT
                    m_column,
                    b_column,
                    z_column,
                    a_column
                FROM ${prefix}test_source
            """,
        )

        # Deploy both nodes
        data = await deploy_and_wait(
            client,
            DeploymentSpec(
                namespace=namespace,
                nodes=[source_spec, transform_spec],
            ),
        )
        assert data["status"] == DeploymentStatus.SUCCESS.value

        # Verify source column order is preserved
        response = await client.get(f"/nodes/{namespace}.test_source/")
        assert response.status_code == 200
        source_columns = [col["name"] for col in response.json()["columns"]]
        assert source_columns == ["z_column", "a_column", "m_column", "b_column"]

        # Verify transform column order matches the SELECT projection
        response = await client.get(f"/nodes/{namespace}.test_transform/")
        assert response.status_code == 200
        transform_columns = [col["name"] for col in response.json()["columns"]]
        assert transform_columns == ["m_column", "b_column", "z_column", "a_column"]


@pytest.mark.xdist_group(name="deployments")
class TestGitOnlyNamespaceDeployments:
    """Tests for git_only namespace deployment verification."""

    @pytest.mark.asyncio
    async def test_git_only_deployment_no_source(self, client):
        """Test that git_only namespace rejects deployment without source."""
        root_namespace = "git_only_no_source_root"
        namespace = "git_only_no_source"

        # Create git root namespace
        await client.post(f"/namespaces/{root_namespace}")
        await client.patch(
            f"/namespaces/{root_namespace}/git",
            json={
                "github_repo_path": "myorg/myrepo",
            },
        )

        # Create branch namespace and set git_only=True
        await client.post(f"/namespaces/{namespace}")
        await client.patch(
            f"/namespaces/{namespace}/git",
            json={
                "parent_namespace": root_namespace,
                "git_branch": "main",
                "git_only": True,
            },
        )

        # Try to deploy without source
        source_spec = SourceSpec(
            name="test_source",
            description="Test source",
            catalog="default",
            schema="test",
            table="test_table",
            columns=[ColumnSpec(name="id", type="int")],
        )

        response = await client.post(
            "/deployments",
            json=DeploymentSpec(
                namespace=namespace,
                nodes=[source_spec],
                # No source field
            ).model_dump(),
        )
        assert response.status_code == 422
        assert "git-only" in response.json()["message"]
        assert "must include a git source" in response.json()["message"]

    @pytest.mark.asyncio
    async def test_git_only_deployment_wrong_source_type(self, client):
        """Test that git_only namespace rejects deployment with local source type."""
        root_namespace = "git_only_wrong_type_root"
        namespace = "git_only_wrong_type"

        # Create git root namespace
        await client.post(f"/namespaces/{root_namespace}")
        await client.patch(
            f"/namespaces/{root_namespace}/git",
            json={
                "github_repo_path": "myorg/myrepo",
            },
        )

        # Create branch namespace and set git_only=True
        await client.post(f"/namespaces/{namespace}")
        await client.patch(
            f"/namespaces/{namespace}/git",
            json={
                "parent_namespace": root_namespace,
                "git_branch": "main",
                "git_only": True,
            },
        )

        source_spec = SourceSpec(
            name="test_source",
            description="Test source",
            catalog="default",
            schema="test",
            table="test_table",
            columns=[ColumnSpec(name="id", type="int")],
        )

        response = await client.post(
            "/deployments",
            json=DeploymentSpec(
                namespace=namespace,
                nodes=[source_spec],
                source=LocalDeploymentSource(
                    hostname="localhost",
                    reason="testing",
                ),
            ).model_dump(),
        )
        assert response.status_code == 422
        assert "git-only" in response.json()["message"]
        assert "source.type='git'" in response.json()["message"]

    @pytest.mark.asyncio
    async def test_git_only_deployment_no_commit_sha(self, client):
        """Test that git_only namespace rejects deployment without commit_sha."""
        root_namespace = "git_only_no_sha_root"
        namespace = "git_only_no_sha"

        # Create git root namespace
        await client.post(f"/namespaces/{root_namespace}")
        await client.patch(
            f"/namespaces/{root_namespace}/git",
            json={
                "github_repo_path": "myorg/myrepo",
            },
        )

        # Create branch namespace and set git_only=True
        await client.post(f"/namespaces/{namespace}")
        await client.patch(
            f"/namespaces/{namespace}/git",
            json={
                "parent_namespace": root_namespace,
                "git_branch": "main",
                "git_only": True,
            },
        )

        source_spec = SourceSpec(
            name="test_source",
            description="Test source",
            catalog="default",
            schema="test",
            table="test_table",
            columns=[ColumnSpec(name="id", type="int")],
        )

        response = await client.post(
            "/deployments",
            json=DeploymentSpec(
                namespace=namespace,
                nodes=[source_spec],
                source=GitDeploymentSource(
                    repository="myorg/myrepo",
                    branch="main",
                    # No commit_sha
                ),
            ).model_dump(),
        )
        assert response.status_code == 422
        assert "git-only" in response.json()["message"]
        assert "commit_sha" in response.json()["message"]

    @pytest.mark.asyncio
    async def test_git_only_deployment_invalid_commit(self, client):
        """Test that git_only namespace rejects deployment with invalid commit."""
        root_namespace = "git_only_invalid_commit_root"
        namespace = "git_only_invalid_commit"

        # Create git root namespace
        await client.post(f"/namespaces/{root_namespace}")
        await client.patch(
            f"/namespaces/{root_namespace}/git",
            json={
                "github_repo_path": "myorg/myrepo",
            },
        )

        # Create branch namespace and set git_only=True
        await client.post(f"/namespaces/{namespace}")
        await client.patch(
            f"/namespaces/{namespace}/git",
            json={
                "parent_namespace": root_namespace,
                "git_branch": "main",
                "git_only": True,
            },
        )

        source_spec = SourceSpec(
            name="test_source",
            description="Test source",
            catalog="default",
            schema="test",
            table="test_table",
            columns=[ColumnSpec(name="id", type="int")],
        )

        # Mock GitHubService to return False for verify_commit
        with patch(
            "datajunction_server.api.deployments.GitHubService",
        ) as mock_github_class:
            mock_github = MagicMock()
            mock_github.verify_commit = AsyncMock(return_value=False)
            mock_github_class.return_value = mock_github

            response = await client.post(
                "/deployments",
                json=DeploymentSpec(
                    namespace=namespace,
                    nodes=[source_spec],
                    source=GitDeploymentSource(
                        repository="myorg/myrepo",
                        branch="main",
                        commit_sha="nonexistent123",
                    ),
                ).model_dump(),
            )

            assert response.status_code == 422
            assert "not found" in response.json()["message"]
            mock_github.verify_commit.assert_called_once_with(
                repo_path="myorg/myrepo",
                commit_sha="nonexistent123",
            )

    @pytest.mark.asyncio
    async def test_git_only_deployment_github_error(self, client):
        """Test that git_only namespace handles GitHub API errors gracefully."""
        root_namespace = "git_only_github_error_root"
        namespace = "git_only_github_error"

        # Create git root namespace
        await client.post(f"/namespaces/{root_namespace}")
        await client.patch(
            f"/namespaces/{root_namespace}/git",
            json={
                "github_repo_path": "myorg/myrepo",
            },
        )

        # Create branch namespace and set git_only=True
        await client.post(f"/namespaces/{namespace}")
        await client.patch(
            f"/namespaces/{namespace}/git",
            json={
                "parent_namespace": root_namespace,
                "git_branch": "main",
                "git_only": True,
            },
        )

        source_spec = SourceSpec(
            name="test_source",
            description="Test source",
            catalog="default",
            schema="test",
            table="test_table",
            columns=[ColumnSpec(name="id", type="int")],
        )

        # Mock GitHubService.verify_commit to raise GitHubServiceError
        with patch(
            "datajunction_server.api.deployments.GitHubService",
        ) as mock_github_class:
            mock_github = MagicMock()
            mock_github.verify_commit = AsyncMock(
                side_effect=GitHubServiceError(
                    "API rate limit exceeded",
                    http_status_code=500,
                    github_status=403,
                ),
            )
            mock_github_class.return_value = mock_github

            response = await client.post(
                "/deployments",
                json=DeploymentSpec(
                    namespace=namespace,
                    nodes=[source_spec],
                    source=GitDeploymentSource(
                        repository="myorg/myrepo",
                        branch="main",
                        commit_sha="abc123def456",
                    ),
                ).model_dump(),
            )

            assert response.status_code == 422
            assert (
                response.json()["message"]
                == "Failed to verify commit: API rate limit exceeded"
            )
            mock_github.verify_commit.assert_called_once_with(
                repo_path="myorg/myrepo",
                commit_sha="abc123def456",
            )

    @pytest.mark.asyncio
    async def test_git_only_deployment_success(self, client):
        """Test successful deployment to git_only namespace with valid commit."""
        root_namespace = "git_only_success_root"
        namespace = "git_only_success"

        # Create git root namespace
        await client.post(f"/namespaces/{root_namespace}")
        await client.patch(
            f"/namespaces/{root_namespace}/git",
            json={
                "github_repo_path": "myorg/myrepo",
            },
        )

        # Create branch namespace and set git_only=True
        await client.post(f"/namespaces/{namespace}")
        await client.patch(
            f"/namespaces/{namespace}/git",
            json={
                "parent_namespace": root_namespace,
                "git_branch": "main",
                "git_only": True,
            },
        )

        source_spec = SourceSpec(
            name="test_source",
            description="Test source",
            catalog="default",
            schema="test",
            table="test_table",
            columns=[ColumnSpec(name="id", type="int")],
        )

        # Mock GitHubService to return True for verify_commit
        with patch(
            "datajunction_server.api.deployments.GitHubService",
        ) as mock_github_class:
            mock_github = MagicMock()
            mock_github.verify_commit = AsyncMock(return_value=True)
            mock_github_class.return_value = mock_github

            data = await deploy_and_wait(
                client,
                DeploymentSpec(
                    namespace=namespace,
                    nodes=[source_spec],
                    source=GitDeploymentSource(
                        repository="myorg/myrepo",
                        branch="main",
                        commit_sha="valid123abc",
                    ),
                ),
            )

            assert data["status"] == DeploymentStatus.SUCCESS.value
            mock_github.verify_commit.assert_called_once_with(
                repo_path="myorg/myrepo",
                commit_sha="valid123abc",
            )

        # Verify node was created
        response = await client.get(f"/nodes/{namespace}.test_source/")
        assert response.status_code == 200

    @pytest.mark.asyncio
    async def test_git_root_namespace_requires_git_source(self, client):
        """Test that a git root namespace (github_repo_path set) auto-locks deployments."""
        root_namespace = "git_root_autolock_deploy"

        # Create git root namespace — no git_only flag needed
        await client.post(f"/namespaces/{root_namespace}")
        await client.patch(
            f"/namespaces/{root_namespace}/git",
            json={"github_repo_path": "myorg/myrepo"},
        )

        source_spec = SourceSpec(
            name="test_source",
            description="Test source",
            catalog="default",
            schema="test",
            table="test_table",
            columns=[ColumnSpec(name="id", type="int")],
        )

        # Deploy without source — should be rejected because namespace is a git root
        response = await client.post(
            "/deployments/",
            json=DeploymentSpec(
                namespace=root_namespace,
                nodes=[source_spec],
                # No source field
            ).model_dump(),
        )
        assert response.status_code == 422
        assert "git-only" in response.json()["message"]
        assert "must include a git source" in response.json()["message"]

    @pytest.mark.asyncio
    async def test_non_git_only_namespace_allows_any_source(self, client):
        """Test that non-git_only namespace accepts deployment without source."""
        namespace = "non_git_only"

        # Create namespace without git_only
        await client.post(f"/namespaces/{namespace}")

        source_spec = SourceSpec(
            name="test_source",
            description="Test source",
            catalog="default",
            schema="test",
            table="test_table",
            columns=[ColumnSpec(name="id", type="int")],
        )

        # Deploy without source - should succeed
        data = await deploy_and_wait(
            client,
            DeploymentSpec(
                namespace=namespace,
                nodes=[source_spec],
                # No source field
            ),
        )

        assert data["status"] == DeploymentStatus.SUCCESS.value

    @pytest.mark.asyncio
    async def test_git_deploy_persists_repo_and_locks(self, client):
        """A CI git deploy (with commit_sha) to a flat namespace persists the repo
        it tracks, making it a repo owner that is UI-locked via is_repo_owner.
        Unlock is a config change (detach the repo), not a git_only toggle."""
        namespace = "git_autolock_flat"

        source_spec = SourceSpec(
            name="s1",
            description="Test source",
            catalog="default",
            schema="test",
            table="t1",
            columns=[ColumnSpec(name="id", type="int")],
        )

        # First git deploy — namespace doesn't exist yet and has no repo, so
        # commit verification is skipped and the deploy succeeds. The repo path
        # gets normalized (github.com/corp/repo -> corp/repo) and persisted.
        data = await deploy_and_wait(
            client,
            DeploymentSpec(
                namespace=namespace,
                nodes=[source_spec],
                source=GitDeploymentSource(
                    repository="github.com/corp/repo",
                    branch="main",
                    commit_sha="abc123",
                ),
            ),
        )
        assert data["status"] == DeploymentStatus.SUCCESS.value

        # Repo + branch persisted → a flat repo owner. No git_only write.
        cfg = (await client.get(f"/namespaces/{namespace}/git")).json()
        assert cfg["github_repo_path"] == "corp/repo"
        assert cfg["git_branch"] == "main"
        assert not cfg["git_only"]

        # Locked via is_repo_owner: a UI node mutation is blocked.
        blocked = await client.delete(f"/nodes/{namespace}.s1/")
        assert blocked.status_code == 422
        assert "git-managed" in blocked.json()["message"]

        # Unlock = detach the repo (config change), not a git_only toggle.
        detach = await client.delete(f"/namespaces/{namespace}/git")
        assert detach.status_code in (200, 204)

        # Now editable again.
        ok = await client.delete(f"/nodes/{namespace}.s1/")
        assert ok.status_code in (200, 201)

    @pytest.mark.parametrize(
        "repository, expected",
        [
            # Already normalized.
            ("corp/repo", "corp/repo"),
            # Bare host-prefixed form (no scheme).
            ("github.com/corp/repo", "corp/repo"),
            # https:// scheme prefix + .git suffix stripped.
            ("https://github.netflix.net/corp/repo.git", "corp/repo"),
            # http:// scheme prefix.
            ("http://github.com/org/repo", "org/repo"),
            # ssh:// scheme prefix.
            ("ssh://git@host/corp/repo.git", "corp/repo"),
            # scp-style git@host:owner/repo (colon normalized to slash).
            ("git@github.netflix.net:corp/repo", "corp/repo"),
            # Single segment (no owner) — returned as-is.
            ("repo", "repo"),
        ],
    )
    def test_normalize_repo_path(self, repository, expected):
        """_normalize_repo_path reduces any repo URL form to owner/repo.

        Covers the scheme-prefix strip (https/http/ssh/git@), the ``.git``
        suffix strip, colon->slash for scp-style URLs, and the single-segment
        fallthrough.
        """
        assert _normalize_repo_path(repository) == expected

    @pytest.mark.asyncio
    async def test_git_only_namespace_no_repo_skips_verification(self, client):
        """A git_only namespace with NO github_repo_path skips commit verification.

        A namespace can be locked (git_only=True) without ever recording a repo
        (e.g. a flat namespace locked via PATCH before its first git deploy).
        There is no repo to verify the commit against, so _verify_git_deployment
        must skip verification and let the git-sourced deploy proceed rather than
        hard-failing.
        """
        namespace = "git_only_no_repo"

        # Lock the namespace WITHOUT configuring a github_repo_path.
        await client.post(f"/namespaces/{namespace}")
        patched = await client.patch(
            f"/namespaces/{namespace}/git",
            json={"git_only": True},
        )
        assert patched.status_code == 200
        cfg = (await client.get(f"/namespaces/{namespace}/git")).json()
        assert cfg["git_only"] is True
        assert cfg["github_repo_path"] is None

        source_spec = SourceSpec(
            name="s1",
            description="Test source",
            catalog="default",
            schema="test",
            table="t1",
            columns=[ColumnSpec(name="id", type="int")],
        )

        # GitHubService should never be constructed since verification is skipped.
        with patch(
            "datajunction_server.api.deployments.GitHubService",
        ) as mock_github_class:
            data = await deploy_and_wait(
                client,
                DeploymentSpec(
                    namespace=namespace,
                    nodes=[source_spec],
                    source=GitDeploymentSource(
                        repository="myorg/myrepo",
                        branch="main",
                        commit_sha="abc123def456",
                    ),
                ),
            )
            assert data["status"] == DeploymentStatus.SUCCESS.value
            mock_github_class.assert_not_called()

        # Still git_only-locked (repo never attached, git_only stays set), so a
        # direct UI node mutation remains blocked.
        blocked = await client.delete(f"/nodes/{namespace}.s1/")
        assert blocked.status_code == 422
        assert "git-managed" in blocked.json()["message"]


@pytest.mark.xdist_group(name="deployments")
class TestDeploymentStatusUpdate:
    """Tests for deployment status update edge cases."""

    @pytest.mark.asyncio
    async def test_update_status_nonexistent_deployment(self, session):
        """Test that update_status handles non-existent deployment gracefully."""

        @asynccontextmanager
        async def mock_session_context():
            yield session

        with patch(
            "datajunction_server.api.deployments.session_context",
            mock_session_context,
        ):
            # This should not raise - just return silently
            await InProcessExecutor.update_status(
                deployment_uuid=str(uuid.uuid4()),
                status=DeploymentStatus.FAILED,
                results=None,
            )
        # If we get here without exception, the test passes

    @pytest.mark.asyncio
    async def test_run_deployment_task_exception_handler(self, session, current_user):
        """When deploy() raises an unexpected exception, the except handler at
        api/deployments.py lines 217-219 catches it and records a FAILED status.
        """
        from contextlib import asynccontextmanager

        from datajunction_server.database.deployment import Deployment
        from datajunction_server.internal.deployment.utils import DeploymentContext

        deployment_id = str(uuid.uuid4())

        # Create a real Deployment record so update_status can find it
        dep = Deployment(
            uuid=deployment_id,
            status=DeploymentStatus.PENDING,
            namespace="test",
            created_by_id=current_user.id,
        )
        session.add(dep)
        await session.flush()

        @asynccontextmanager
        async def mock_session_context():
            yield session

        deployment_spec = DeploymentSpec(namespace="test", nodes=[])
        context = MagicMock(spec=DeploymentContext)

        with (
            patch(
                "datajunction_server.api.deployments.session_context",
                mock_session_context,
            ),
            patch(
                "datajunction_server.api.deployments.deploy",
                side_effect=RuntimeError("unexpected failure"),
            ),
        ):
            executor_instance = InProcessExecutor()
            await executor_instance._run_deployment(
                deployment_id=deployment_id,
                deployment_spec=deployment_spec,
                context=context,
            )

        await session.refresh(dep)
        assert dep.status == DeploymentStatus.FAILED


class TestHistoryUser:
    """Unit tests for DeploymentOrchestrator._history_user."""

    def _make_orchestrator(self, spec, username="service-account"):
        from datajunction_server.internal.deployment.orchestrator import (
            DeploymentOrchestrator,
        )
        from datajunction_server.internal.deployment.utils import DeploymentContext

        user = MagicMock()
        user.username = username
        context = DeploymentContext(current_user=user)
        return DeploymentOrchestrator(
            deployment_spec=spec,
            deployment_id="test-id",
            session=MagicMock(),
            context=context,
        )

    def test_returns_author_email_when_present(self):
        spec = DeploymentSpec(
            namespace="test",
            source=GitDeploymentSource(
                repository="github.com/org/repo",
                commit_author_name="Alice Smith",
                commit_author_email="alice@example.com",
            ),
        )
        orchestrator = self._make_orchestrator(spec)
        assert orchestrator._history_user == "alice@example.com"

    def test_falls_back_to_name_when_no_email(self):
        spec = DeploymentSpec(
            namespace="test",
            source=GitDeploymentSource(
                repository="github.com/org/repo",
                commit_author_name="Alice Smith",
            ),
        )
        orchestrator = self._make_orchestrator(spec)
        assert orchestrator._history_user == "Alice Smith"

    def test_falls_back_to_current_user_when_no_author(self):
        spec = DeploymentSpec(
            namespace="test",
            source=GitDeploymentSource(
                repository="github.com/org/repo",
                branch="main",
            ),
        )
        orchestrator = self._make_orchestrator(spec, username="jenkins")
        assert orchestrator._history_user == "jenkins"

    def test_falls_back_to_current_user_for_local_source(self):
        spec = DeploymentSpec(
            namespace="test",
            source=LocalDeploymentSource(hostname="dev-machine"),
        )
        orchestrator = self._make_orchestrator(spec, username="developer")
        assert orchestrator._history_user == "developer"

    def test_falls_back_to_current_user_when_no_source(self):
        spec = DeploymentSpec(namespace="test")
        orchestrator = self._make_orchestrator(spec, username="admin")
        assert orchestrator._history_user == "admin"


class TestDeploymentRevalidation:
    """
    Deployment write path: _create_node_revision sets NodeRelationship rows
    from node_graph, ensuring each new revision has correct parent links.
    """

    @pytest.mark.asyncio
    async def test_redeploy_creates_new_revision_with_correct_parents(
        self,
        client,
        session,
    ):
        """
        Force-redeploying a spec creates new node revisions via _create_node_revision,
        which derives parents from node_graph. Verify the new revision has the
        correct NodeRelationship row regardless of any corruption on the old revision.
        """
        from sqlalchemy import delete, select

        namespace = "revalidate_parents_test"

        source = SourceSpec(
            name="default.parts",
            catalog="default",
            schema="shop",
            table="parts",
            columns=[
                ColumnSpec(name="part_id", type="int"),
                ColumnSpec(name="name", type="string"),
            ],
        )
        transform = TransformSpec(
            name="default.parts_enriched",
            query="SELECT part_id, name FROM ${prefix}default.parts",
            columns=[
                ColumnSpec(name="part_id", type="int"),
                ColumnSpec(name="name", type="string"),
            ],
        )
        spec = DeploymentSpec(namespace=namespace, nodes=[source, transform])

        # Initial deployment
        data = await deploy_and_wait(client, spec)
        assert data["status"] == DeploymentStatus.SUCCESS.value

        # Fetch the deployed transform and verify it has a parent
        transform_name = f"{namespace}.default.parts_enriched"
        from sqlalchemy.orm import joinedload

        transform_node = (
            await session.execute(
                select(Node)
                .where(Node.name == transform_name)
                .options(joinedload(Node.current)),
            )
        ).scalar_one()
        original_rev_id = transform_node.current.id
        original_rows = (
            await session.execute(
                select(NodeRelationship).where(
                    NodeRelationship.child_id == original_rev_id,
                ),
            )
        ).all()
        assert len(original_rows) == 1, "transform should have one parent after deploy"

        # Corrupt: delete the NodeRelationship rows
        await session.execute(
            delete(NodeRelationship).where(
                NodeRelationship.child_id == original_rev_id,
            ),
        )
        await session.commit()
        assert (
            await session.execute(
                select(NodeRelationship).where(
                    NodeRelationship.child_id == original_rev_id,
                ),
            )
        ).all() == []

        # Re-deploy with force=True so even unchanged nodes get new revisions
        # with correct parent links set by _create_node_revision.
        force_spec = DeploymentSpec(
            namespace=namespace,
            nodes=[source, transform],
            force=True,
        )
        data = await deploy_and_wait(client, force_spec)
        assert data["status"] == DeploymentStatus.SUCCESS.value

        # Re-fetch to get the new current revision created by re-deployment
        await session.refresh(transform_node, ["current"])
        new_rev_id = transform_node.current.id
        restored_rows = (
            await session.execute(
                select(NodeRelationship).where(NodeRelationship.child_id == new_rev_id),
            )
        ).all()
        assert len(restored_rows) == 1, (
            "_create_node_revision should have written correct parent relationships"
        )


@pytest.mark.asyncio
async def test_validate_reference_dimension_link_bad_attribute():
    """
    validate_reference_dimension_link raises when the dimension attribute
    does not exist on the dimension node's columns.
    """
    from datajunction_server.errors import DJInvalidInputException
    from datajunction_server.internal.deployment.orchestrator import (
        validate_reference_dimension_link,
    )

    # Build a reference link pointing to a non-existent column
    link = DimensionReferenceLinkSpec(
        node_column="state",
        dimension="ns.dim_node.nonexistent_col",
    )
    link.namespace = "ns"

    # Build a minimal dim node with columns that do NOT include 'nonexistent_col'
    dim_rev = MagicMock()
    dim_rev.columns = [
        MagicMock(name="id"),
        MagicMock(name="state_short"),
    ]
    # MagicMock(name=...) sets the mock's internal name, not .name attribute
    dim_rev.columns[0].name = "id"
    dim_rev.columns[1].name = "state_short"

    dim_node = MagicMock()
    dim_node.current = dim_rev

    node = MagicMock()
    node.name = "ns.some_node"

    with pytest.raises(DJInvalidInputException, match="nonexistent_col"):
        await validate_reference_dimension_link(link, node, dim_node)


@pytest.mark.asyncio
async def test_validate_reference_dimension_link_good_attribute():
    """
    validate_reference_dimension_link does NOT raise when the dimension
    attribute exists on the dimension node's columns.
    """
    from datajunction_server.internal.deployment.orchestrator import (
        validate_reference_dimension_link,
    )

    link = DimensionReferenceLinkSpec(
        node_column="state",
        dimension="ns.dim_node.state_short",
    )
    link.namespace = "ns"

    dim_rev = MagicMock()
    dim_rev.columns = [MagicMock(), MagicMock()]
    dim_rev.columns[0].name = "id"
    dim_rev.columns[1].name = "state_short"

    dim_node = MagicMock()
    dim_node.current = dim_rev

    node = MagicMock()
    node.name = "ns.some_node"

    # Should not raise
    await validate_reference_dimension_link(link, node, dim_node)


@pytest.mark.xdist_group(name="deployments")
class TestCubeRoleCollisionDeployment:
    """pk_cube role-collision repro via the push path: a fact links one dimension
    twice under two roles and a cube references the same column under both. The
    real push previously crashed with a pk_cube UniqueViolation."""

    @pytest.mark.asyncio
    async def test_deploy_cube_same_column_two_roles(self, client):
        namespace = "pk_cube_role"
        nodes = [
            SourceSpec(
                name="orders_raw",
                description="Raw orders",
                catalog="default",
                schema="roads",
                table="orders_raw",
                columns=[
                    ColumnSpec(name="order_dateint", type="int"),
                    ColumnSpec(name="ship_dateint", type="int"),
                ],
                dimension_links=[],
                owners=["dj"],
            ),
            SourceSpec(
                name="dates_raw",
                description="Raw dates",
                catalog="default",
                schema="roads",
                table="dates_raw",
                columns=[
                    ColumnSpec(name="dateint", type="int"),
                    ColumnSpec(name="monthint", type="int"),
                ],
                dimension_links=[],
                owners=["dj"],
            ),
            DimensionSpec(
                name="dates_d",
                description="Date dimension",
                query="SELECT dateint, monthint FROM ${prefix}dates_raw",
                primary_key=["dateint"],
                dimension_links=[],
                owners=["dj"],
            ),
            TransformSpec(
                name="orders_f",
                description="Orders fact linked to dates twice (order/ship)",
                query="SELECT order_dateint, ship_dateint FROM ${prefix}orders_raw",
                dimension_links=[
                    DimensionJoinLinkSpec(
                        dimension_node="${prefix}dates_d",
                        join_type="left",
                        join_on="${prefix}orders_f.order_dateint = ${prefix}dates_d.dateint",
                        role="order_date",
                    ),
                    DimensionJoinLinkSpec(
                        dimension_node="${prefix}dates_d",
                        join_type="left",
                        join_on="${prefix}orders_f.ship_dateint = ${prefix}dates_d.dateint",
                        role="ship_date",
                    ),
                ],
                owners=["dj"],
            ),
            MetricSpec(
                name="order_count",
                description="Order count",
                query="SELECT COUNT(*) FROM ${prefix}orders_f",
                dimension_links=[],
                owners=["dj"],
            ),
            CubeSpec(
                name="orders_cube",
                display_name="Orders Cube",
                description="Cube referencing dates_d.monthint under both roles",
                metrics=["${prefix}order_count"],
                dimensions=[
                    "${prefix}dates_d.monthint[order_date]",
                    "${prefix}dates_d.monthint[ship_date]",
                ],
                owners=["dj"],
            ),
        ]

        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=nodes),
        )
        # The real push must succeed — previously it raised a pk_cube UniqueViolation.
        assert data["status"] == DeploymentStatus.SUCCESS.value, data

        # Both role-qualified dimensions survive on the deployed cube.
        response = await client.get(f"/cubes/{namespace}.orders_cube/")
        assert response.status_code == 200, response.json()
        cube = response.json()
        assert cube["cube_node_dimensions"] == [
            f"{namespace}.dates_d.monthint[order_date]",
            f"{namespace}.dates_d.monthint[ship_date]",
        ]
        dimension_elements = [
            (elem["node_name"], elem["name"], elem["role"])
            for elem in cube["cube_elements"]
            if elem["type"] == "dimension"
        ]
        assert dimension_elements == [
            (f"{namespace}.dates_d", "monthint", "order_date"),
            (f"{namespace}.dates_d", "monthint", "ship_date"),
        ]


@pytest.mark.xdist_group(name="deployments")
class TestCubeChainedRoleDeployment:
    """A dimension reached over two FK hops carries a chained role
    (``[customer->registration]``).

    ``_batch_load_dimensions`` parses those references with ``FullColumnName``,
    whose role pattern accepts ``-`` and ``>``, so the reference validated fine.
    The deploy loop then re-stripped the role with ``COLUMN_NAME_REGEX``, whose
    role group did not, so ``re.fullmatch`` returned None, the still-role-suffixed
    name never matched a column, and the dimension was dropped from the cube --
    with the deploy reporting SUCCESS, because the two parsers disagreeing meant
    the missing-dimension check upstream saw nothing wrong."""

    def _nodes(self):
        return [
            SourceSpec(
                name="orders_raw",
                description="Raw orders",
                catalog="default",
                schema="roads",
                table="orders_raw",
                columns=[
                    ColumnSpec(name="order_id", type="int"),
                    ColumnSpec(name="customer_id", type="int"),
                ],
                dimension_links=[],
                owners=["dj"],
            ),
            SourceSpec(
                name="customers_raw",
                description="Raw customers",
                catalog="default",
                schema="roads",
                table="customers_raw",
                columns=[
                    ColumnSpec(name="customer_id", type="int"),
                    ColumnSpec(name="registration_dateint", type="int"),
                ],
                dimension_links=[],
                owners=["dj"],
            ),
            SourceSpec(
                name="dates_raw",
                description="Raw dates",
                catalog="default",
                schema="roads",
                table="dates_raw",
                columns=[
                    ColumnSpec(name="dateint", type="int"),
                    ColumnSpec(name="year", type="int"),
                ],
                dimension_links=[],
                owners=["dj"],
            ),
            DimensionSpec(
                name="dates_d",
                description="Date dimension",
                query="SELECT dateint, year FROM ${prefix}dates_raw",
                primary_key=["dateint"],
                dimension_links=[],
                owners=["dj"],
            ),
            DimensionSpec(
                name="customers_d",
                description="Customer dimension, itself linked to dates",
                query=(
                    "SELECT customer_id, registration_dateint "
                    "FROM ${prefix}customers_raw"
                ),
                primary_key=["customer_id"],
                dimension_links=[
                    DimensionJoinLinkSpec(
                        dimension_node="${prefix}dates_d",
                        join_type="left",
                        join_on=(
                            "${prefix}customers_d.registration_dateint = "
                            "${prefix}dates_d.dateint"
                        ),
                        role="registration",
                    ),
                ],
                owners=["dj"],
            ),
            TransformSpec(
                name="orders_f",
                description="Orders fact linked to customers under role `customer`",
                query="SELECT order_id, customer_id FROM ${prefix}orders_raw",
                dimension_links=[
                    DimensionJoinLinkSpec(
                        dimension_node="${prefix}customers_d",
                        join_type="left",
                        join_on=(
                            "${prefix}orders_f.customer_id = "
                            "${prefix}customers_d.customer_id"
                        ),
                        role="customer",
                    ),
                ],
                owners=["dj"],
            ),
            MetricSpec(
                name="order_count",
                description="Order count",
                query="SELECT COUNT(*) FROM ${prefix}orders_f",
                dimension_links=[],
                owners=["dj"],
            ),
            CubeSpec(
                name="orders_cube",
                display_name="Orders Cube",
                description="Cube reaching dates_d over two hops",
                metrics=["${prefix}order_count"],
                dimensions=[
                    "${prefix}customers_d.customer_id[customer]",
                    "${prefix}dates_d.year[customer->registration]",
                ],
                owners=["dj"],
            ),
        ]

    @pytest.mark.asyncio
    async def test_deploy_cube_with_chained_role(self, client):
        namespace = "cube_chained_role"
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=self._nodes()),
        )
        assert data["status"] == DeploymentStatus.SUCCESS.value, data

        # The chained-role dimension must survive the deploy. Previously the cube
        # came back with only the single-hop dimension and no error anywhere.
        response = await client.get(f"/cubes/{namespace}.orders_cube/")
        assert response.status_code == 200, response.json()
        cube = response.json()
        assert cube["cube_node_dimensions"] == [
            f"{namespace}.customers_d.customer_id[customer]",
            f"{namespace}.dates_d.year[customer->registration]",
        ]
        dimension_elements = [
            (elem["node_name"], elem["name"], elem["role"])
            for elem in cube["cube_elements"]
            if elem["type"] == "dimension"
        ]
        assert dimension_elements == [
            (f"{namespace}.customers_d", "customer_id", "customer"),
            (f"{namespace}.dates_d", "year", "customer->registration"),
        ]

    @pytest.mark.asyncio
    async def test_deploy_cube_with_unresolvable_dimension_column_errors(
        self,
        client,
    ):
        """Widening the role group must not mask genuine errors: a chained-role
        reference to a column that does not exist still fails the deploy."""
        namespace = "cube_unresolvable_dim"
        nodes = self._nodes()
        nodes[-1].dimensions = [
            "${prefix}customers_d.customer_id[customer]",
            "${prefix}dates_d.no_such_column[customer->registration]",
        ]
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=nodes),
        )
        cube_result = next(
            r for r in data["results"] if r["name"] == f"{namespace}.orders_cube"
        )
        assert cube_result["status"] in ("invalid", "failed"), cube_result
        assert "no_such_column" in cube_result["message"], cube_result


@pytest.mark.xdist_group(name="deployments")
class TestCubeBareDimRoleAwareDeployment:
    """Deploy-time cube validation must be role-aware.

    A cube that references a dimension attribute BARE (no role) while the
    parent's ONLY link to that dimension is role-played must be rejected at
    deploy time (the bare attribute is not available on every metric), matching
    what a subsequent revalidation would compute. Previously the deploy path
    used a role-agnostic reachability check and accepted the bare reference,
    deploying green and then flipping to INVALID on revalidation.
    """

    def _nodes(self, bare: bool):
        """Fact links dates_d ONLY under role `order_date`; cube references
        dates_d.monthint bare (bare=True) or role-qualified (bare=False)."""
        dim_ref = (
            "${prefix}dates_d.monthint"
            if bare
            else "${prefix}dates_d.monthint[order_date]"
        )
        return [
            SourceSpec(
                name="orders_raw",
                description="Raw orders",
                catalog="default",
                schema="roads",
                table="orders_raw",
                columns=[
                    ColumnSpec(name="order_dateint", type="int"),
                ],
                dimension_links=[],
                owners=["dj"],
            ),
            SourceSpec(
                name="dates_raw",
                description="Raw dates",
                catalog="default",
                schema="roads",
                table="dates_raw",
                columns=[
                    ColumnSpec(name="dateint", type="int"),
                    ColumnSpec(name="monthint", type="int"),
                ],
                dimension_links=[],
                owners=["dj"],
            ),
            DimensionSpec(
                name="dates_d",
                description="Date dimension",
                query="SELECT dateint, monthint FROM ${prefix}dates_raw",
                primary_key=["dateint"],
                dimension_links=[],
                owners=["dj"],
            ),
            TransformSpec(
                name="orders_f",
                description="Orders fact linked to dates ONLY under role order_date",
                query="SELECT order_dateint FROM ${prefix}orders_raw",
                dimension_links=[
                    DimensionJoinLinkSpec(
                        dimension_node="${prefix}dates_d",
                        join_type="left",
                        join_on=(
                            "${prefix}orders_f.order_dateint = ${prefix}dates_d.dateint"
                        ),
                        role="order_date",
                    ),
                ],
                owners=["dj"],
            ),
            MetricSpec(
                name="order_count",
                description="Order count",
                query="SELECT COUNT(*) FROM ${prefix}orders_f",
                dimension_links=[],
                owners=["dj"],
            ),
            CubeSpec(
                name="orders_cube",
                display_name="Orders Cube",
                description="Cube referencing dates_d.monthint",
                metrics=["${prefix}order_count"],
                dimensions=[dim_ref],
                owners=["dj"],
            ),
        ]

    @pytest.mark.asyncio
    async def test_deploy_bare_dim_only_reachable_under_role_is_rejected(
        self,
        client,
    ):
        """Repro: bare `dates_d.monthint` where the only link carries a role
        must be rejected at deploy time (INVALID_DIMENSION)."""
        namespace = "cube_bare_role_reject"
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=self._nodes(bare=True)),
        )
        cube_result = next(
            r for r in data["results"] if r["name"] == f"{namespace}.orders_cube"
        )
        assert cube_result["status"] in ("invalid", "failed"), cube_result
        assert f"{namespace}.dates_d" in cube_result["message"], cube_result
        assert (
            "not reachable" in cube_result["message"]
            or "not available on every metric" in cube_result["message"]
        ), cube_result

        # The deploy-time status must match what revalidation would compute.
        response = await client.get(f"/nodes/{namespace}.orders_cube/")
        assert response.status_code == 200, response.json()
        assert response.json()["status"] == "invalid"

    @pytest.mark.asyncio
    async def test_deploy_role_qualified_dim_passes(self, client):
        """The role-qualified version passes push and revalidates valid."""
        namespace = "cube_role_qualified_ok"
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=self._nodes(bare=False)),
        )
        assert data["status"] == DeploymentStatus.SUCCESS.value, data
        cube_result = next(
            r for r in data["results"] if r["name"] == f"{namespace}.orders_cube"
        )
        assert cube_result["status"] not in ("invalid", "failed"), cube_result

        response = await client.get(f"/nodes/{namespace}.orders_cube/")
        assert response.status_code == 200, response.json()
        assert response.json()["status"] == "valid"


@pytest.mark.xdist_group(name="deployments")
class TestCubeReferenceDimDeployment:
    """Deploy-time cube validation must see reference dimension links.

    A reference link writes no dimensionlink row — it lives on the column, as a
    denormalized copy of the dimension attribute — so the join-path BFS cannot
    find it. A cube sliced by such a dimension used to deploy INVALID ("not
    reachable from parent node(s)") even though it builds correct SQL.
    """

    def _nodes(self):
        """Orders reaches the customer dimension only via a reference link."""
        return [
            SourceSpec(
                name="orders_raw",
                description="Raw orders",
                catalog="default",
                schema="store",
                table="orders_raw",
                columns=[
                    ColumnSpec(name="order_id", type="int"),
                    ColumnSpec(name="customer_country", type="string"),
                ],
                owners=["dj"],
            ),
            SourceSpec(
                name="customers_raw",
                description="Raw customers",
                catalog="default",
                schema="store",
                table="customers_raw",
                columns=[
                    ColumnSpec(name="customer_id", type="int"),
                    ColumnSpec(name="country", type="string"),
                ],
                owners=["dj"],
            ),
            DimensionSpec(
                name="customers_d",
                description="Customer dimension",
                query="SELECT customer_id, country FROM ${prefix}customers_raw",
                primary_key=["customer_id"],
                owners=["dj"],
            ),
            TransformSpec(
                name="orders_f",
                description="Orders fact carrying the customer country inline",
                query="SELECT order_id, customer_country FROM ${prefix}orders_raw",
                dimension_links=[
                    DimensionReferenceLinkSpec(
                        node_column="customer_country",
                        dimension="${prefix}customers_d.country",
                    ),
                ],
                owners=["dj"],
            ),
            MetricSpec(
                name="order_count",
                description="Order count",
                query="SELECT COUNT(*) FROM ${prefix}orders_f",
                owners=["dj"],
            ),
            MetricSpec(
                name="customer_count",
                description="Customer count",
                query="SELECT COUNT(*) FROM ${prefix}customers_d",
                owners=["dj"],
            ),
            CubeSpec(
                name="orders_cube",
                display_name="Orders Cube",
                description="Cube sliced by a reference-linked dimension",
                metrics=["${prefix}order_count", "${prefix}customer_count"],
                dimensions=["${prefix}customers_d.country"],
                owners=["dj"],
            ),
        ]

    @pytest.mark.asyncio
    async def test_deploy_cube_on_reference_linked_dim(self, client):
        """Repro: the reference-linked dimension is reachable, so the cube is valid."""
        namespace = "cube_reference_dim"
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=self._nodes()),
        )
        assert data["status"] == DeploymentStatus.SUCCESS.value, data
        cube_result = next(
            r for r in data["results"] if r["name"] == f"{namespace}.orders_cube"
        )
        assert cube_result["status"] not in ("invalid", "failed"), cube_result

        response = await client.get(f"/nodes/{namespace}.orders_cube/")
        assert response.status_code == 200, response.json()
        assert response.json()["status"] == "valid"


def _hard_hat_deploy_nodes(default_hard_hats, default_us_states, default_us_state):
    """Fact + dimension + link + a COUNT measure metric, for pre-agg deploy tests."""
    hard_hat_facts = TransformSpec(
        name="default.hard_hat_facts",
        node_type=NodeType.TRANSFORM,
        query="SELECT hard_hat_id, state FROM ${prefix}default.hard_hats",
        dimension_links=[
            DimensionJoinLinkSpec(
                dimension_node="${prefix}default.us_state",
                join_type="inner",
                join_on=(
                    "${prefix}default.hard_hat_facts.state = "
                    "${prefix}default.us_state.state_short"
                ),
            ),
        ],
        owners=["dj"],
    )
    count_hard_hats = MetricSpec(
        name="default.count_hard_hats",
        node_type=NodeType.METRIC,
        query="SELECT COUNT(*) FROM ${prefix}default.hard_hat_facts",
        owners=["dj"],
    )
    return [
        default_hard_hats,
        default_us_states,
        default_us_state,
        hard_hat_facts,
        count_hard_hats,
    ]


def _override_query_service(client, columns):
    items = (
        columns.items()
        if isinstance(columns, dict)
        else [(name, "bigint") for name in columns]
    )
    table_columns = [
        SimpleNamespace(name=name, type=type_str) for name, type_str in items
    ]

    async def _fake_columns(*args, **kwargs):
        return table_columns

    mock_qs = MagicMock()
    mock_qs.get_columns_for_table = _fake_columns
    client.app.dependency_overrides[get_query_service_client] = lambda: mock_qs


def _clear_query_service(client):
    client.app.dependency_overrides.pop(get_query_service_client, None)


def _preagg_spec(name, table, metrics, dimensions=None):
    return PreAggSpec(
        name=name,
        metrics=metrics,
        dimensions=dimensions or {"${prefix}default.us_state.state_name": "state_name"},
        catalog="default",
        schema="analytics",
        table=table,
        valid_through_ts=1700000000,
    )


class TestExternalPreAggDeploy:
    """Deploy-time reconciliation of externally-registered pre-aggregations."""

    @pytest.mark.asyncio
    async def test_deploy_preagg_invalid_metric_fails(
        self,
        client,
        default_hard_hats,
        default_us_states,
        default_us_state,
    ):
        """A pre-agg whose measure_columns key is not a valid measure fails the
        whole deploy and rolls it back -- no nodes or pre-aggs are left behind."""
        _override_query_service(
            client,
            {"hard_hat_count": "bigint", "state_name": "string"},
        )
        try:
            bad = _preagg_spec(
                "bad_preagg",
                "hh_bad",
                {"${prefix}default.does_not_exist": "hard_hat_count"},
            )
            data = await deploy_and_wait(
                client,
                DeploymentSpec(
                    namespace="preagg_bad",
                    nodes=_hard_hat_deploy_nodes(
                        default_hard_hats,
                        default_us_states,
                        default_us_state,
                    ),
                    preaggregations=[bad],
                ),
            )
            assert data["status"] == "failed"
            message = data["results"][-1]["message"]
            assert "Failed to reconcile pre-aggregations" in message
            assert "is not a metric node" in message
            # The whole deploy rolled back: the fact node was not created.
            node_resp = await client.get("/nodes/preagg_bad.default.hard_hat_facts")
            assert node_resp.status_code == 404
        finally:
            _clear_query_service(client)

    @pytest.mark.asyncio
    async def test_deploy_preagg_without_query_service(
        self,
        client,
        default_hard_hats,
        default_us_states,
        default_us_state,
    ):
        """Registering pre-aggs needs a query service; without one the deploy
        fails and rolls back rather than silently skipping."""
        client.app.dependency_overrides[get_query_service_client] = lambda: None
        try:
            spec = _preagg_spec(
                "needs_qs",
                "hh_qs",
                {"${prefix}default.count_hard_hats": "hard_hat_count"},
            )
            data = await deploy_and_wait(
                client,
                DeploymentSpec(
                    namespace="preagg_noqs",
                    nodes=_hard_hat_deploy_nodes(
                        default_hard_hats,
                        default_us_states,
                        default_us_state,
                    ),
                    preaggregations=[spec],
                ),
            )
            assert data["status"] == "failed"
            assert (
                "requires a configured query service" in data["results"][-1]["message"]
            )
            # The whole deploy rolled back: the fact node was not created.
            node_resp = await client.get("/nodes/preagg_noqs.default.hard_hat_facts")
            assert node_resp.status_code == 404
        finally:
            _clear_query_service(client)

    @pytest.mark.asyncio
    async def test_dry_run_reports_preagg_reconcile(
        self,
        client,
        default_hard_hats,
        default_us_states,
        default_us_state,
    ):
        """A dry-run reports the planned pre-agg register without persisting it."""
        _override_query_service(
            client,
            {"hard_hat_count": "bigint", "state_name": "string"},
        )
        try:
            nodes = _hard_hat_deploy_nodes(
                default_hard_hats,
                default_us_states,
                default_us_state,
            )
            # Real deploy of the nodes (no pre-agg) so the fact node exists.
            data = await deploy_and_wait(
                client,
                DeploymentSpec(namespace="preagg_dry", nodes=nodes),
            )
            assert data["status"] == "success", data["results"]

            spec = _preagg_spec(
                "dry_preagg",
                "hh_dry",
                {"${prefix}default.count_hard_hats": "hard_hat_count"},
            )

            async def _impact_preagg_results(preaggs, allow_empty=False):
                impact = await client.post(
                    "/deployments/impact",
                    json=DeploymentSpec(
                        namespace="preagg_dry",
                        nodes=nodes,
                        preaggregations=preaggs,
                        allow_empty=allow_empty,
                    ).model_dump(),
                )
                return [
                    r
                    for r in impact.json()["results"]
                    if r["deploy_type"] == "preaggregation"
                ]

            # Not yet registered -> planned CREATE, nothing persisted.
            results = await _impact_preagg_results([spec])
            assert len(results) == 1
            assert results[0]["name"] == "preagg_dry.dry_preagg"
            assert results[0]["operation"] == "create"
            listing = await client.get(
                "/preaggs/",
                params={"node_name": "preagg_dry.default.hard_hat_facts"},
            )
            assert listing.json()["items"] == []

            # Actually register it, then dry-run again.
            data = await deploy_and_wait(
                client,
                DeploymentSpec(
                    namespace="preagg_dry",
                    nodes=nodes,
                    preaggregations=[spec],
                ),
            )
            assert data["status"] == "success", data["results"]

            # Re-declaring it -> planned UPDATE.
            results = await _impact_preagg_results([spec])
            assert [r["operation"] for r in results] == ["update"]

            # Dropping it (allow_empty) -> planned DELETE, still there afterward.
            results = await _impact_preagg_results([], allow_empty=True)
            assert len(results) == 1
            assert results[0]["operation"] == "delete"
            listing = await client.get(
                "/preaggs/",
                params={"node_name": "preagg_dry.default.hard_hat_facts"},
            )
            assert len(listing.json()["items"]) == 1
        finally:
            _clear_query_service(client)

    @pytest.mark.asyncio
    async def test_delete_on_removal_scoped_to_namespace(
        self,
        client,
        default_hard_hats,
        default_us_states,
        default_us_state,
    ):
        """A deploy to one namespace does not remove pre-aggs in another."""
        _override_query_service(
            client,
            {"hard_hat_count": "bigint", "state_name": "string"},
        )
        try:
            nodes = _hard_hat_deploy_nodes(
                default_hard_hats,
                default_us_states,
                default_us_state,
            )
            spec = _preagg_spec(
                "keep_me",
                "hh_keep",
                {"${prefix}default.count_hard_hats": "hard_hat_count"},
            )
            # Namespace A gets a pre-agg.
            data = await deploy_and_wait(
                client,
                DeploymentSpec(
                    namespace="preagg_a",
                    nodes=nodes,
                    preaggregations=[spec],
                ),
            )
            assert data["status"] == "success", data["results"]
            # Namespace B deploys with no pre-aggs.
            data = await deploy_and_wait(
                client,
                DeploymentSpec(namespace="preagg_b", nodes=nodes),
            )
            assert data["status"] == "success", data["results"]
            # A's pre-agg is untouched.
            listing = await client.get(
                "/preaggs/",
                params={"node_name": "preagg_a.default.hard_hat_facts"},
            )
            assert len(listing.json()["items"]) == 1
        finally:
            _clear_query_service(client)

    @pytest.mark.asyncio
    async def test_deploy_updates_existing_preagg(
        self,
        client,
        default_hard_hats,
        default_us_states,
        default_us_state,
    ):
        """Re-deploying the same pre-agg with a different table/column updates it
        in place rather than creating a duplicate."""
        nodes = _hard_hat_deploy_nodes(
            default_hard_hats,
            default_us_states,
            default_us_state,
        )
        try:
            _override_query_service(
                client,
                {"hh_count_v1": "bigint", "state_name": "string"},
            )
            data = await deploy_and_wait(
                client,
                DeploymentSpec(
                    namespace="preagg_upd",
                    nodes=nodes,
                    preaggregations=[
                        _preagg_spec(
                            "hh_agg",
                            "hh_agg_v1",
                            {"${prefix}default.count_hard_hats": "hh_count_v1"},
                        ),
                    ],
                ),
            )
            assert data["status"] == "success", data["results"]

            _override_query_service(
                client,
                {"hh_count_v2": "bigint", "state_name": "string"},
            )
            data = await deploy_and_wait(
                client,
                DeploymentSpec(
                    namespace="preagg_upd",
                    nodes=nodes,
                    preaggregations=[
                        _preagg_spec(
                            "hh_agg",
                            "hh_agg_v2",
                            {"${prefix}default.count_hard_hats": "hh_count_v2"},
                        ),
                    ],
                ),
            )
            assert data["status"] == "success", data["results"]

            listing = await client.get(
                "/preaggs/",
                params={"node_name": "preagg_upd.default.hard_hat_facts"},
            )
            items = listing.json()["items"]
            assert len(items) == 1
            source_columns = {m["source_column"] for m in items[0]["measures"]}
            assert source_columns == {"hh_count_v2"}
        finally:
            _clear_query_service(client)

    @pytest.mark.asyncio
    async def test_preagg_kept_when_none_declared(
        self,
        client,
        default_hard_hats,
        default_us_states,
        default_us_state,
    ):
        """A content-ful deploy that declares no pre-aggs leaves existing
        external pre-aggs intact; allow_empty is required to deregister them."""
        _override_query_service(
            client,
            {"hard_hat_count": "bigint", "state_name": "string"},
        )
        try:
            nodes = _hard_hat_deploy_nodes(
                default_hard_hats,
                default_us_states,
                default_us_state,
            )
            fact_node = "preagg_keep.default.hard_hat_facts"
            data = await deploy_and_wait(
                client,
                DeploymentSpec(
                    namespace="preagg_keep",
                    nodes=nodes,
                    preaggregations=[
                        _preagg_spec(
                            "keep_me2",
                            "hh_keep2",
                            {"${prefix}default.count_hard_hats": "hard_hat_count"},
                        ),
                    ],
                ),
            )
            assert data["status"] == "success", data["results"]

            # Re-deploy the same nodes with NO pre-aggs -> the pre-agg is kept,
            # not silently deregistered.
            data = await deploy_and_wait(
                client,
                DeploymentSpec(namespace="preagg_keep", nodes=nodes),
            )
            assert data["status"] == "success", data["results"]
            listing = await client.get("/preaggs/", params={"node_name": fact_node})
            assert len(listing.json()["items"]) == 1

            # allow_empty opts into the deregistration.
            data = await deploy_and_wait(
                client,
                DeploymentSpec(
                    namespace="preagg_keep",
                    nodes=nodes,
                    allow_empty=True,
                ),
            )
            assert data["status"] == "success", data["results"]
            listing = await client.get("/preaggs/", params={"node_name": fact_node})
            assert listing.json()["items"] == []
        finally:
            _clear_query_service(client)

    @pytest.mark.asyncio
    async def test_preagg_kept_warning_is_surfaced(
        self,
        client,
        default_hard_hats,
        default_us_states,
        default_us_state,
    ):
        """The "left intact" warning is reported by the successful deploy that
        skipped the deregistration -- both as a top-level warning and as a
        per-item pre-aggregation result."""
        _override_query_service(
            client,
            {"hard_hat_count": "bigint", "state_name": "string"},
        )
        try:
            nodes = _hard_hat_deploy_nodes(
                default_hard_hats,
                default_us_states,
                default_us_state,
            )
            data = await deploy_and_wait(
                client,
                DeploymentSpec(
                    namespace="preagg_warn",
                    nodes=nodes,
                    preaggregations=[
                        _preagg_spec(
                            "warn_me",
                            "hh_warn",
                            {"${prefix}default.count_hard_hats": "hard_hat_count"},
                        ),
                    ],
                ),
            )
            assert data["status"] == "success", data["results"]
            assert data["warnings"] == []

            # Re-deploy with no pre-aggs declared: succeeds, keeps the pre-agg,
            # and says so.
            data = await deploy_and_wait(
                client,
                DeploymentSpec(namespace="preagg_warn", nodes=nodes),
            )
            message = (
                "1 external pre-aggregation(s) in namespace 'preagg_warn' were "
                "left intact because the deployment declares none. Re-run with "
                "allow_empty to deregister them."
            )
            assert data["status"] == "success", data["results"]
            assert data["warnings"] == [
                {
                    "code": "INVALID_ARGUMENTS_TO_FUNCTION",
                    "message": message,
                    "debug": None,
                    "context": "",
                },
            ]
            assert [
                result
                for result in data["results"]
                if result["deploy_type"] == "preaggregation"
            ] == [
                {
                    "name": "preagg_warn",
                    "deploy_type": "preaggregation",
                    "status": "warning",
                    "operation": "noop",
                    "message": message,
                    "changed_fields": [],
                },
            ]

            listing = await client.get(
                "/preaggs/",
                params={"node_name": "preagg_warn.default.hard_hat_facts"},
            )
            assert len(listing.json()["items"]) == 1
        finally:
            _clear_query_service(client)

    @pytest.mark.asyncio
    async def test_deploy_preagg_with_dimension_columns(
        self,
        client,
        default_hard_hats,
        default_us_states,
        default_us_state,
    ):
        """dimension_columns on a deployed pre-agg flows through reconcile so the
        renamed physical dimension column is read at query time."""
        _override_query_service(
            client,
            {"hh_state": "string", "hard_hat_count": "bigint"},
        )
        try:
            nodes = _hard_hat_deploy_nodes(
                default_hard_hats,
                default_us_states,
                default_us_state,
            )
            data = await deploy_and_wait(
                client,
                DeploymentSpec(
                    namespace="preagg_dimcol",
                    nodes=nodes,
                    preaggregations=[
                        _preagg_spec(
                            "hh_by_state",
                            "hh_by_state_agg",
                            {"${prefix}default.count_hard_hats": "hard_hat_count"},
                            dimensions={
                                "${prefix}default.us_state.state_name": "hh_state",
                            },
                        ),
                    ],
                ),
            )
            assert data["status"] == "success", data["results"]
            # The registered pre-agg reads the renamed physical dimension column.
            response = await client.get(
                "/sql/measures/v3/",
                params={
                    "metrics": ["preagg_dimcol.default.count_hard_hats"],
                    "dimensions": ["preagg_dimcol.default.us_state.state_name"],
                },
            )
            assert response.status_code == 200
            sql = response.json()["grain_groups"][0]["sql"]
            assert "default.analytics.hh_by_state_agg" in sql
            assert "hh_state" in sql
        finally:
            _clear_query_service(client)


@pytest.mark.xdist_group(name="deployments")
class TestCrossParentRatioDeployment:
    """Cross-parent ratio metric (numerator on a transform, denominator on a
    DIMENSION node) whose base metrics share a dimension ONLY via dimension
    links created in the same deployment.

    Regression for the deploy-time false negative: dimension links are deployed
    after nodes (_deploy_nodes runs before _deploy_links), so at validation time
    get_dimensions sees only each base metric's parent-local columns and the
    shared-dimension intersection is empty. The cross-fact check must not reject
    the derived metric mid-deploy when the pending links establish a common
    dimension. Mirrors the minimal fact ÷ dimension-node repro.
    """

    def _nodes(self):
        return [
            SourceSpec(
                name="fd_fact_raw",
                description="Raw fact rows",
                catalog="default",
                schema="roads",
                table="fd_fact_raw",
                columns=[
                    ColumnSpec(name="account_id", type="bigint"),
                    ColumnSpec(name="dateint", type="int"),
                    ColumnSpec(name="visited", type="int"),
                ],
                dimension_links=[],
                owners=["dj"],
            ),
            SourceSpec(
                name="fd_dim_raw",
                description="Raw dimension rows",
                catalog="default",
                schema="roads",
                table="fd_dim_raw",
                columns=[
                    ColumnSpec(name="account_id", type="bigint"),
                    ColumnSpec(name="dateint", type="int"),
                    ColumnSpec(name="can_stream", type="int"),
                ],
                dimension_links=[],
                owners=["dj"],
            ),
            SourceSpec(
                name="dt_date_raw",
                description="Raw dates",
                catalog="default",
                schema="roads",
                table="dt_date_raw",
                columns=[ColumnSpec(name="dateint", type="int")],
                dimension_links=[],
                owners=["dj"],
            ),
            DimensionSpec(
                name="dt_date_d_v2",
                description="Conformed date dimension",
                query="SELECT dateint FROM ${prefix}dt_date_raw",
                primary_key=["dateint"],
                dimension_links=[],
                owners=["dj"],
            ),
            # Numerator parent: a TRANSFORM, linked to the shared date dim.
            TransformSpec(
                name="fd_fact",
                description="Fact transform",
                query="SELECT account_id, dateint, visited FROM ${prefix}fd_fact_raw",
                dimension_links=[
                    DimensionJoinLinkSpec(
                        dimension_node="${prefix}dt_date_d_v2",
                        join_type="left",
                        join_on="${prefix}fd_fact.dateint = ${prefix}dt_date_d_v2.dateint",
                    ),
                ],
                owners=["dj"],
            ),
            # Denominator parent: a DIMENSION node, linked to the same date dim.
            DimensionSpec(
                name="fd_dim",
                description="Dimension node holding the denominator",
                query="SELECT account_id, dateint, can_stream FROM ${prefix}fd_dim_raw",
                primary_key=["account_id", "dateint"],
                dimension_links=[
                    DimensionJoinLinkSpec(
                        dimension_node="${prefix}dt_date_d_v2",
                        join_type="left",
                        join_on="${prefix}fd_dim.dateint = ${prefix}dt_date_d_v2.dateint",
                    ),
                ],
                owners=["dj"],
            ),
            MetricSpec(
                name="fd_num",
                description="Numerator (parent = transform)",
                query="SELECT SUM(visited) FROM ${prefix}fd_fact",
                dimension_links=[],
                owners=["dj"],
            ),
            MetricSpec(
                name="fd_den",
                description="Denominator (parent = dimension node)",
                query="SELECT SUM(can_stream) FROM ${prefix}fd_dim",
                dimension_links=[],
                owners=["dj"],
            ),
            # Cross-parent ratio: numerator (transform) ÷ denominator (dim node),
            # sharing dt_date_d_v2 only through links deployed in this same batch.
            MetricSpec(
                name="fd_ratio_cross",
                description="fd_num / fd_den",
                query=("SELECT CAST(${prefix}fd_num AS DOUBLE) / ${prefix}fd_den"),
                dimension_links=[],
                owners=["dj"],
            ),
        ]

    @pytest.mark.asyncio
    async def test_cross_parent_ratio_shared_dim_via_pending_links_is_valid(
        self,
        client,
    ):
        namespace = "cross_parent_ratio"
        data = await deploy_and_wait(
            client,
            DeploymentSpec(namespace=namespace, nodes=self._nodes()),
        )
        ratio_result = next(
            r for r in data["results"] if r["name"] == f"{namespace}.fd_ratio_cross"
        )
        # Before the fix this deployed with node status INVALID and the message
        # "[invalid] ... no shared dimensions"; the result status is the deploy
        # outcome ("success"), the node validity is asserted via GET below.
        assert ratio_result["status"] == "success", ratio_result
        assert "invalid" not in ratio_result["message"], ratio_result

        # Deploy-time status must match what a fresh read/revalidation computes.
        response = await client.get(f"/nodes/{namespace}.fd_ratio_cross/")
        assert response.status_code == 200, response.json()
        assert response.json()["status"] == "valid"
