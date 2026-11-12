"""Tests for namespace-independent semantic fingerprints (version 2)."""

from unittest.mock import patch

import pytest

from datajunction_server.models.deployment import (
    ColumnSpec,
    CubeSpec,
    DimensionJoinLinkSpec,
    DimensionSpec,
    MetricSpec,
    NodeSpec,
    PartitionSpec,
    SourceSpec,
    TransformSpec,
)
from datajunction_server.models.partition import PartitionType
from datajunction_server.models.semantic_fingerprint import SemanticFingerprint
from datajunction_server.semantic_fingerprints.engine import (
    compose_node_fingerprint,
    local_node_fingerprint,
)
from datajunction_server.semantic_fingerprints.v2 import (
    namespace_free_name,
)
from datajunction_server.semantic_fingerprints.v2 import (
    parse_query as parse_query_cached,
)
from datajunction_server.sql.parsing.backends.antlr4 import parse


def specs(namespace: str) -> dict[str, NodeSpec]:
    return {
        "source": SourceSpec(
            namespace=namespace,
            name="orders",
            catalog="warehouse",
            schema="sales",
            table="orders",
            columns=[ColumnSpec(name="order_id", type="bigint")],
            primary_key=["order_id"],
        ),
        "transform": TransformSpec(
            namespace=namespace,
            name="clean_orders",
            query="SELECT order_id AS id, amount FROM ${prefix}orders WHERE amount > 0",
            dimension_links=[
                DimensionJoinLinkSpec(
                    dimension_node="${prefix}order",
                    join_on="${prefix}clean_orders.id = ${prefix}order.order_id",
                ),
            ],
        ),
        "dimension": DimensionSpec(
            namespace=namespace,
            name="order",
            query="SELECT order_id, status FROM ${prefix}orders",
            primary_key=["order_id"],
        ),
        "metric": MetricSpec(
            namespace=namespace,
            name="total_amount",
            query="SELECT SUM(amount) AS value FROM ${prefix}clean_orders",
            required_dimensions=["${prefix}order.status"],
        ),
        "cube": CubeSpec(
            namespace=namespace,
            name="order_cube",
            metrics=["${prefix}total_amount"],
            dimensions=["${prefix}order.status"],
            filters=["${prefix}order.status != 'cancelled'"],
            columns=[
                ColumnSpec(
                    name="${prefix}order.status",
                    partition=PartitionSpec(type=PartitionType.CATEGORICAL),
                ),
            ],
        ),
    }


def digest(spec: NodeSpec, version: int = 2) -> SemanticFingerprint:
    return local_node_fingerprint(spec, version)


@pytest.mark.parametrize(
    "node_type",
    ["source", "transform", "dimension", "metric", "cube"],
)
def test_fingerprint_is_the_same_in_every_namespace(node_type):
    main = digest(specs("analytics.main")[node_type])
    branch = digest(specs("analytics.feature_branch")[node_type])
    assert main == branch
    assert main.version == 2


@pytest.mark.parametrize("node_type", ["transform", "dimension", "metric", "cube"])
def test_version_1_still_depends_on_the_namespace(node_type):
    main = digest(specs("analytics.main")[node_type], 1)
    branch = digest(specs("analytics.feature_branch")[node_type], 1)
    assert main != branch


def test_fingerprint_matches_when_names_are_already_qualified():
    templated = specs("analytics.main")["metric"]
    qualified = MetricSpec(
        namespace="analytics.main",
        name="total_amount",
        query="SELECT SUM(amount) AS value FROM analytics.main.clean_orders",
        required_dimensions=["analytics.main.order.status"],
    )
    assert digest(templated) == digest(qualified)


def test_spec_loaded_without_a_namespace_matches_its_deployed_form():
    deployed = specs("analytics.main")["metric"]
    loaded = MetricSpec(
        name="analytics.main.total_amount",
        query="SELECT SUM(amount) AS value FROM analytics.main.clean_orders",
        required_dimensions=["analytics.main.order.status"],
    )
    assert loaded.namespace is None
    assert digest(loaded) == digest(deployed)


def test_quoted_values_and_table_names_keep_their_namespace_text():
    def metric(namespace: str) -> MetricSpec:
        return MetricSpec(
            namespace=namespace,
            name="total_amount",
            query=f"SELECT SUM(amount) FROM ${{prefix}}orders WHERE tag = '{namespace}.customer'",
        )

    def source(namespace: str) -> SourceSpec:
        return SourceSpec(
            namespace=namespace,
            name="orders",
            catalog="warehouse",
            schema="sales",
            table=f"{namespace}.orders",
            columns=[ColumnSpec(name="order_id", type="bigint")],
        )

    assert digest(metric("analytics.main")) != digest(metric("analytics.feature"))
    assert digest(source("analytics.main")) != digest(source("analytics.feature"))


def test_filter_values_keep_their_namespace_text():
    def cube(namespace: str) -> CubeSpec:
        return CubeSpec(
            namespace=namespace,
            name="order_cube",
            metrics=["${prefix}total_amount"],
            dimensions=["${prefix}order.status"],
            filters=[f"${{prefix}}order.status = '{namespace}.open'"],
        )

    assert digest(cube("analytics.main")) != digest(cube("analytics.feature"))


def test_names_in_other_namespaces_are_kept():
    def metric(namespace: str, other: str) -> MetricSpec:
        return MetricSpec(
            namespace=namespace,
            name="total_amount",
            query=f"SELECT SUM(amount) AS value FROM {other}.orders",
        )

    assert digest(metric("analytics.main", "common.dim")) == digest(
        metric("analytics.feature_branch", "common.dim"),
    )
    assert digest(metric("analytics.main", "common.dim")) != digest(
        metric("analytics.main", "common.other"),
    )


def test_similar_looking_namespaces_are_not_rewritten():
    def metric(other: str) -> MetricSpec:
        return MetricSpec(
            namespace="analytics",
            name="total_amount",
            query=f"SELECT SUM(amount) AS value FROM {other}.orders",
        )

    assert digest(metric("my_analytics")) != digest(metric("my_reporting"))
    assert digest(metric("analytics")) != digest(metric("analytics_v2"))


def test_real_changes_still_change_the_fingerprint():
    base = specs("analytics.main")
    changed = specs("analytics.main")
    changed["transform"] = TransformSpec(
        namespace="analytics.main",
        name="clean_orders",
        query="SELECT order_id AS id, amount FROM ${prefix}orders WHERE amount > 10",
        dimension_links=base["transform"].dimension_links,
    )
    assert digest(base["transform"]) != digest(changed["transform"])


def test_parent_fingerprints_do_not_depend_on_the_namespace():
    def metric_digest(namespace: str) -> SemanticFingerprint:
        built = specs(namespace)
        parent = digest(built["transform"])
        return compose_node_fingerprint(
            built["metric"],
            2,
            parent_fingerprints=[parent],
        )

    assert metric_digest("analytics.main") == metric_digest("analytics.feature_branch")


def test_parent_versions_must_match():
    with pytest.raises(ValueError, match="Parent fingerprint version does not match"):
        compose_node_fingerprint(
            specs("analytics.main")["metric"],
            2,
            parent_fingerprints=[SemanticFingerprint(version=1, digest="a" * 64)],
        )


def test_fingerprint_without_a_namespace():
    metric = MetricSpec(name="total_amount", query="SELECT SUM(amount) FROM orders")
    assert digest(metric) == digest(metric)


def test_query_is_parsed_once_across_repeated_fingerprints():
    metric = specs("analytics.main")["metric"]
    parse_query_cached.cache_clear()
    with patch(
        "datajunction_server.sql.parsing.backends.antlr4.parse",
        wraps=parse,
    ) as parse_query:
        first = digest(metric)
        second = digest(metric)
    assert first == second
    assert parse_query.call_count == 1


def test_names_are_unchanged_without_a_namespace():
    assert namespace_free_name("analytics.main.orders", "") == "analytics.main.orders"


def test_nested_node_loaded_with_its_deploy_root_matches_its_deployed_form():
    deployed = MetricSpec(
        namespace="analytics.main",
        name="sub.total",
        query="SELECT SUM(amount) FROM ${prefix}sub.orders JOIN ${prefix}dates",
    )
    loaded = MetricSpec(
        namespace="analytics.main",
        name="analytics.main.sub.total",
        query="SELECT SUM(amount) FROM analytics.main.sub.orders "
        "JOIN analytics.main.dates",
    )
    other_branch = MetricSpec(
        namespace="analytics.feature",
        name="sub.total",
        query="SELECT SUM(amount) FROM ${prefix}sub.orders JOIN ${prefix}dates",
    )
    assert digest(loaded) == digest(deployed) == digest(other_branch)


@pytest.mark.parametrize(
    "query",
    [
        "SELECT id -- user's column\nFROM ${prefix}orders WHERE tag = 'active'",
        "SELECT id /* user's column */ FROM ${prefix}orders WHERE tag = 'active'",
        'SELECT "user\'s id" AS id FROM ${prefix}orders',
        "SELECT id FROM ${prefix}orders WHERE note = 'it''s' AND tag = 'a'",
        "SELECT id -- see ${prefix}orders\nFROM ${prefix}orders",
    ],
)
def test_comments_and_quotes_do_not_hide_node_names(query):
    def metric(namespace: str) -> MetricSpec:
        return MetricSpec(namespace=namespace, name="m", query=query)

    assert digest(metric("analytics.main")) == digest(metric("analytics.feature"))


def test_values_after_a_comment_with_an_apostrophe_keep_their_text():
    def metric(namespace: str) -> MetricSpec:
        return MetricSpec(
            namespace=namespace,
            name="m",
            query=f"SELECT id -- user's column\nFROM ${{prefix}}orders WHERE tag = '{namespace}.x'",
        )

    assert digest(metric("analytics.main")) != digest(metric("analytics.feature"))
