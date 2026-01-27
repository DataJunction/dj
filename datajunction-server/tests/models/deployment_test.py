from datajunction_server.models.node import NodeMode, NodeType
from datajunction_server.models.deployment import (
    DeploymentSpec,
    DimensionJoinLinkSpec,
    SourceSpec,
    MetricSpec,
    DimensionReferenceLinkSpec,
    TransformSpec,
    ColumnSpec,
    PartitionSpec,
    Granularity,
    PartitionType,
    eq_columns,
    eq_or_fallback,
)
from datajunction_server.models.node import MetricUnit


def test_source_spec():
    source_spec = SourceSpec(
        name="test_source",
        catalog="public",
        schema="test_db",
        table="test_table",
    )
    assert source_spec.rendered_name == "test_source"
    assert source_spec.rendered_query is None


def test_transform_spec():
    transform_spec = TransformSpec(
        namespace="blah",
        name="test_transform",
        query="SELECT * FROM ${prefix}some_table",
        description="A test transform",
    )
    other_transform_spec = TransformSpec(
        name="other_transform",
        query="SELECT * FROM ${prefix}other_table",
        description="Another test transform",
    )
    assert transform_spec.rendered_name == "blah.test_transform"
    assert transform_spec.rendered_query == "SELECT * FROM blah.some_table"
    assert transform_spec.query_ast is not None
    assert transform_spec.__eq__(transform_spec)
    assert not transform_spec.__eq__(object())
    assert not transform_spec.__eq__(other_transform_spec)


def test_metric_spec():
    metric_spec = MetricSpec(
        name="test_metric",
        query="SELECT 1 AS value",
        unit=MetricUnit.DAY,
        description="A test metric",
    )
    other_metric_spec = MetricSpec(
        name="other_metric",
        query="SELECT 2 AS value",
        unit=MetricUnit.DOLLAR,
        description="Another test metric",
    )
    assert metric_spec.rendered_name == "test_metric"
    assert metric_spec.rendered_query == "SELECT 1 AS value"
    assert metric_spec.query_ast is not None
    assert metric_spec.__eq__(metric_spec)
    assert not metric_spec.__eq__(object())
    assert not metric_spec.__eq__(other_metric_spec)


def test_reference_link_spec():
    link_spec = DimensionReferenceLinkSpec(
        role="test_role",
        node_column="dim_name",
        dimension="some.dimension.name",
    )
    assert link_spec != "1"
    assert link_spec != DimensionJoinLinkSpec(
        role="test_role",
        dimension_node="some.dimension.name",
        join_on="dim_name = some.dimension.name.dim_name",
        join_type="left",
    )

    link_spec_no_role = DimensionReferenceLinkSpec(
        dimension="test_dimension_no_role",
        node_column="dim_name",
    )
    assert link_spec_no_role.role is None
    assert link_spec_no_role.dimension == "test_dimension_no_role"
    assert link_spec != link_spec_no_role


def test_deployment_spec():
    spec = DeploymentSpec(
        namespace="test_deployment",
        nodes=[
            SourceSpec(
                name="test_node",
                node_type=NodeType.SOURCE,
                owners=["user1"],
                tags=["tag1"],
                catalog="db",
                schema="schema",
                table="table",
            ),
        ],
    )
    assert spec.nodes[0].name == "test_node"
    assert spec.nodes[0].namespace == "test_deployment"
    assert spec.nodes[0].node_type == NodeType.SOURCE
    assert spec.nodes[0].owners == ["user1"]
    assert spec.nodes[0].tags == ["tag1"]
    assert spec.namespace == "test_deployment"
    assert spec.model_dump() == {
        "namespace": "test_deployment",
        "nodes": [
            {
                "columns": None,
                "custom_metadata": None,
                "description": None,
                "dimension_links": [],
                "display_name": None,
                "mode": NodeMode.PUBLISHED,
                "name": "test_node",
                "node_type": NodeType.SOURCE,
                "owners": ["user1"],
                "tags": ["tag1"],
                "catalog": "db",
                "schema_": "schema",
                "table": "table",
                "primary_key": [],
            },
        ],
        "tags": [],
        "source": None,
    }


def test_column_spec():
    column_spec = ColumnSpec(
        name="col1",
        type="string",
        description="A test column",
        partition=PartitionSpec(
            type=PartitionType.TEMPORAL,
            format="YYYY-MM-DD",
            granularity=Granularity.DAY,
        ),
    )
    other_column_spec = ColumnSpec(
        name="col1",
        type="string",
        description="A test column",
        partition=PartitionSpec(
            type=PartitionType.TEMPORAL,
            format="YYYY-MM-DD",
            granularity=Granularity.DAY,
        ),
    )
    different_column_spec = ColumnSpec(
        name="col2",
        description="A different test column",
        type="integer",
    )
    assert column_spec.name == "col1"
    assert column_spec.partition.type == PartitionType.TEMPORAL
    assert column_spec.partition.format == "YYYY-MM-DD"
    assert column_spec.__eq__(column_spec)
    assert not column_spec.__eq__(object())
    assert column_spec.__eq__(other_column_spec)
    assert not column_spec.__eq__(different_column_spec)
    assert column_spec != "1"


def test_eq_or_fallback_basic():
    # Equal values
    assert eq_or_fallback("x", "x", "fallback")
    # a is None, b equals fallback
    assert eq_or_fallback(None, "fb", "fb")
    # a is None but b != fallback
    assert not eq_or_fallback(None, "other", "fb")
    # a not None, mismatch
    assert not eq_or_fallback("x", "y", "fb")


def test_eq_columns_equal_lists():
    c1 = ColumnSpec(
        name="col1",
        type="string",
        attributes=["primary_key"],
        partition=None,
    )
    c2 = ColumnSpec(
        name="col1",
        type="string",
        attributes=["primary_key"],
        partition=None,
    )
    assert eq_columns([c1], [c2])  # exact match
    assert eq_columns([], [])  # both empty
    assert eq_columns(None, None)  # both None


def test_eq_columns_none_and_special_case():
    # a is None, b has columns with only primary_key attribute and no partition
    b = [
        ColumnSpec(
            name="col1",
            type="string",
            attributes=["primary_key"],
            partition=None,
        ),
        ColumnSpec(
            name="col1",
            type="string",
            attributes=["primary_key"],
            partition=None,
        ),
    ]
    assert eq_columns(None, b)

    # a is empty list behaves like None
    assert eq_columns([], b)


def test_eq_columns_failures():
    # Different attributes (not just primary_key)
    b = [
        ColumnSpec(
            name="col1",
            type="string",
            attributes=["primary_key", "other"],
            partition=None,
        ),
    ]
    assert not eq_columns(None, b)

    # Partition flag set
    b = [
        ColumnSpec(
            name="col1",
            type="string",
            attributes=["primary_key"],
            partition=PartitionSpec(
                type=PartitionType.TEMPORAL,
                format="YYYY-MM-DD",
                granularity=Granularity.DAY,
            ),
        ),
    ]
    assert not eq_columns(None, b)


def test_dimension_join_link_spec_with_default_value():
    """Test DimensionJoinLinkSpec with default_value for NULL handling in LEFT JOINs."""
    link_spec = DimensionJoinLinkSpec(
        dimension_node="some.dimension.users",
        join_type="left",
        join_on="events.user_id = some.dimension.users.id",
        role="user",
        default_value="Unknown",
    )
    assert link_spec.dimension_node == "some.dimension.users"
    assert link_spec.join_type == "left"
    assert link_spec.default_value == "Unknown"
    assert link_spec.role == "user"

    # Test equality includes default_value
    same_link = DimensionJoinLinkSpec(
        dimension_node="some.dimension.users",
        join_type="left",
        join_on="events.user_id = some.dimension.users.id",
        role="user",
        default_value="Unknown",
    )
    assert link_spec == same_link

    # Different default_value should not be equal
    different_default = DimensionJoinLinkSpec(
        dimension_node="some.dimension.users",
        join_type="left",
        join_on="events.user_id = some.dimension.users.id",
        role="user",
        default_value="N/A",
    )
    assert link_spec != different_default

    # No default_value should not be equal to one with default_value
    no_default = DimensionJoinLinkSpec(
        dimension_node="some.dimension.users",
        join_type="left",
        join_on="events.user_id = some.dimension.users.id",
        role="user",
    )
    assert no_default.default_value is None
    assert link_spec != no_default

    # Test hash includes default_value (for use in sets/dicts)
    assert hash(link_spec) == hash(same_link)
    assert hash(link_spec) != hash(different_default)


def test_source_spec_with_dimension_link_default_value():
    """Test SourceSpec with dimension_links including default_value."""
    source_spec = SourceSpec(
        name="events",
        namespace="test",
        catalog="public",
        schema="test_db",
        table="events",
        dimension_links=[
            DimensionJoinLinkSpec(
                dimension_node="${prefix}users",
                join_type="left",
                join_on="events.user_id = users.id",
                default_value="Unknown User",
                namespace="test",
            ),
        ],
    )
    assert len(source_spec.dimension_links) == 1
    assert source_spec.dimension_links[0].default_value == "Unknown User"
    assert source_spec.dimension_links[0].rendered_dimension_node == "test.users"
