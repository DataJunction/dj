"""Frozen compatibility checks for the default semantic fingerprint version."""

import pytest

from datajunction_server.internal.deployment.fingerprints import (
    SemanticFingerprintGraph,
    build_deployment_fingerprints,
)
from datajunction_server.models.deployment import (
    ColumnSpec,
    CubeSpec,
    DeploymentResult,
    DimensionJoinLinkSpec,
    DimensionSpec,
    MetricSpec,
    SourceSpec,
    TransformSpec,
)
from datajunction_server.models.filter_requirements import FilterRequirement
from datajunction_server.models.semantic_fingerprint import (
    LATEST_SEMANTIC_FINGERPRINT_VERSION,
    SUPPORTED_SEMANTIC_FINGERPRINT_VERSIONS,
    SemanticFingerprint,
)


def _compatibility_specs():
    orders = SourceSpec(
        namespace="gold",
        name="orders",
        catalog="warehouse",
        schema_="sales",
        table="orders",
        columns=[
            ColumnSpec(name="order_id", type="bigint"),
            ColumnSpec(name="customer_id", type="bigint"),
            ColumnSpec(name="amount", type="decimal(12,2)"),
        ],
        primary_key=["order_id"],
    )
    customers = SourceSpec(
        namespace="gold",
        name="customers",
        catalog="warehouse",
        schema_="sales",
        table="customers",
        columns=[
            ColumnSpec(name="customer_id", type="bigint"),
            ColumnSpec(name="country", type="varchar(2)"),
        ],
        primary_key=["customer_id"],
    )
    order_values = TransformSpec(
        namespace="gold",
        name="order_values",
        query=(
            "SELECT order_id, customer_id, amount * 2 AS doubled_amount "
            "FROM ${prefix}orders WHERE amount > 0"
        ),
        columns=[
            ColumnSpec(name="order_id", type="bigint"),
            ColumnSpec(name="customer_id", type="bigint"),
            ColumnSpec(name="doubled_amount", type="decimal(13,2)"),
        ],
    )
    customer_dim = DimensionSpec(
        namespace="gold",
        name="customer_dim",
        query="SELECT customer_id, country FROM ${prefix}customers",
        columns=[
            ColumnSpec(name="customer_id", type="bigint"),
            ColumnSpec(name="country", type="varchar(2)"),
        ],
        primary_key=["customer_id"],
        dimension_links=[
            DimensionJoinLinkSpec(
                dimension_node="${prefix}orders",
                join_on=(
                    "${prefix}customer_dim.customer_id = ${prefix}orders.customer_id"
                ),
                role="purchaser",
            ),
        ],
    )
    gross_revenue = MetricSpec(
        namespace="gold",
        name="gross_revenue",
        query="SELECT SUM(amount) FROM ${prefix}orders",
        required_dimensions=["${prefix}orders.customer_id"],
        fixed_grain=["${prefix}orders.order_id"],
        required_filters=[
            FilterRequirement(
                dimension=["${prefix}orders.customer_id"],
                default="${prefix}orders.customer_id = 42",
            ),
        ],
        recommended_filters=[
            FilterRequirement(dimension=["${prefix}orders.order_id"]),
        ],
        columns=[ColumnSpec(name="gross_revenue", type="decimal(12,2)")],
    )
    adjusted_revenue = MetricSpec(
        namespace="gold",
        name="adjusted_revenue",
        query="SELECT ${prefix}gross_revenue * 1.1",
        required_dimensions=["${prefix}gross_revenue.customer_id"],
    )
    sales_cube = CubeSpec(
        namespace="gold",
        name="sales_cube",
        metrics=["${prefix}gross_revenue", "${prefix}adjusted_revenue"],
        dimensions=["${prefix}customer_dim.country"],
        filters=["${prefix}customer_dim.country = 'US'"],
    )
    return (
        orders,
        customers,
        order_values,
        customer_dim,
        gross_revenue,
        adjusted_revenue,
        sales_cube,
    )


_EXPECTED_DIGESTS = {
    "gold.orders": "37f538dc2e008e9621a0115b02fea1f22987004892608e658bcadeb4b70fdcad",
    "gold.customers": "95990e0df2a15e1b55e70b9fa8d805fb68abcefe03a962679ee4973ee5c3eff4",
    "gold.order_values": "2821f8cf7e20f5b1c2ee4b5222e96343720fb0ee1affc8fe3a66d075e96f0835",
    "gold.customer_dim": "25565975ac69966ce7fe0e90983add84696aa6244744008ab21502a4ea31f68a",
    "gold.gross_revenue": "7ec7bf74c29a4b4634f6aea467d583cd316722597cbe00a01ed071d6c372db89",
    "gold.adjusted_revenue": "c22c30eb50efa6fc8578900a24dfb4c531e278c8f1fba0a2692019f01d6b4b74",
    "gold.sales_cube": "286846d240cb5a040b567821c08327f6d4ee8b1fe79edf70a25056d198342761",
}


def test_default_graph_fingerprints_match_v1_golden_values():
    specs = _compatibility_specs()
    graph = SemanticFingerprintGraph({spec.rendered_name: spec for spec in specs})

    assert LATEST_SEMANTIC_FINGERPRINT_VERSION == 1
    assert 1 in SUPPORTED_SEMANTIC_FINGERPRINT_VERSIONS
    fingerprints = graph.fingerprints()
    assert {
        name: fingerprint.digest
        for name, fingerprint in fingerprints.items()
        if isinstance(fingerprint, SemanticFingerprint)
    } == _EXPECTED_DIGESTS
    assert all(
        isinstance(fingerprint, SemanticFingerprint) and fingerprint.version == 1
        for fingerprint in fingerprints.values()
    )


@pytest.mark.asyncio
async def test_default_deployment_fingerprints_remain_v1():
    specs = _compatibility_specs()
    _, proposed = await build_deployment_fingerprints(
        object(),
        {},
        specs,
        [],
    )

    assert {
        name: fingerprint.digest
        for name, fingerprint in proposed.items()
        if isinstance(fingerprint, SemanticFingerprint)
    } == _EXPECTED_DIGESTS
    assert all(
        isinstance(fingerprint, SemanticFingerprint) and fingerprint.version == 1
        for fingerprint in proposed.values()
    )


def test_deployment_result_serializes_the_default_fingerprint_as_v1():
    fingerprint = SemanticFingerprint(digest=_EXPECTED_DIGESTS["gold.orders"])
    result = DeploymentResult(
        name="gold.orders",
        deploy_type=DeploymentResult.Type.NODE,
        status=DeploymentResult.Status.SUCCESS,
        operation=DeploymentResult.Operation.CREATE,
        change_tier="major",
        semantic_fingerprint=fingerprint,
    )

    assert result.model_dump(mode="json")["semantic_fingerprint"] == {
        "version": 1,
        "digest": _EXPECTED_DIGESTS["gold.orders"],
    }
