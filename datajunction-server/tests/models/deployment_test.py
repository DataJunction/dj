import json
from datetime import date

import pytest

from datajunction_server.errors import (
    DJInvalidDeploymentConfig,
    DJInvalidInputException,
)
from datajunction_server.models.deployment import (
    ChangeTier,
    ColumnSpec,
    CubeSpec,
    CustomMetadataSchemaSpec,
    DeploymentSpec,
    DimensionJoinLinkSpec,
    DimensionReferenceLinkSpec,
    DimensionSpec,
    Granularity,
    MaterializationAction,
    MaterializationSpec,
    MetricSpec,
    NamespaceGitConfig,
    NodeSpec,
    PartitionSpec,
    PartitionType,
    PreAggSpec,
    SourceSpec,
    TagSpec,
    TransformSpec,
    bump_version,
    eq_columns,
    eq_or_fallback,
    fold_change_tiers,
)
from datajunction_server.models.materialization import (
    CoverageSpec,
    MaterializationStrategy,
)
from datajunction_server.models.node import MetricUnit, NodeMode, NodeType


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


def test_metric_spec_accepts_legacy_string_unit():
    """Legacy `unit: dollar` (string) parses into unit_enum, leaves unit_structured None."""
    spec = MetricSpec(name="m", query="SELECT 1", unit="dollar")
    assert spec.unit_enum == MetricUnit.DOLLAR
    assert spec.unit_structured is None
    assert spec.unit == "dollar"


def test_metric_spec_accepts_metric_unit_enum_directly():
    spec = MetricSpec(name="m", query="SELECT 1", unit=MetricUnit.SECOND)
    assert spec.unit_enum == MetricUnit.SECOND
    assert spec.unit_structured is None
    assert spec.unit == "second"


def test_metric_spec_accepts_structured_unit_dict():
    """Structured `unit: {kind: ..., code: ...}` parses into unit_structured."""
    from datajunction_server.models.unit import AtomicUnit, UnitKind

    spec = MetricSpec(
        name="m",
        query="SELECT 1",
        unit={"kind": "currency", "code": "USD"},
    )
    assert spec.unit_enum is None
    assert isinstance(spec.unit_structured, AtomicUnit)
    assert spec.unit_structured.kind == UnitKind.CURRENCY
    assert spec.unit_structured.code == "USD"
    assert spec.unit == {"kind": "currency", "code": "USD"}


def test_metric_spec_accepts_compound_unit_dict():
    """Compound units round-trip through MetricSpec."""
    from datajunction_server.models.unit import CompoundUnit

    spec = MetricSpec(
        name="m",
        query="SELECT 1",
        unit={
            "numerator": {"kind": "count", "code": "clicks"},
            "denominator": {"kind": "count", "code": "impressions"},
        },
    )
    assert isinstance(spec.unit_structured, CompoundUnit)
    assert spec.unit_structured.numerator.code == "clicks"
    assert spec.unit_structured.denominator.code == "impressions"
    assert spec.unit == {
        "numerator": {"kind": "count", "code": "clicks"},
        "denominator": {"kind": "count", "code": "impressions"},
    }


def test_metric_spec_rejects_unknown_legacy_string():
    from datajunction_server.errors import DJInvalidInputException

    with pytest.raises(DJInvalidInputException, match="Invalid metric unit"):
        MetricSpec(name="m", query="SELECT 1", unit="dollars")


def test_metric_spec_rejects_non_string_non_dict_unit():
    from datajunction_server.errors import DJInvalidInputException

    with pytest.raises(DJInvalidInputException, match="must be a string or"):
        MetricSpec(name="m", query="SELECT 1", unit=42)


def test_metric_spec_no_unit_set():
    spec = MetricSpec(name="m", query="SELECT 1")
    assert spec.unit_enum is None
    assert spec.unit_structured is None
    assert spec.unit is None


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

    # Reference link specs must be hashable so link reconciliation can build a
    # set of existing links during deployment (see DeploymentOrchestrator.
    # _deploy_links). Regression: a missing __hash__ made this raise
    # "TypeError: unhashable type: 'DimensionReferenceLinkSpec'".
    equal_link_spec = DimensionReferenceLinkSpec(
        role="test_role",
        node_column="dim_name",
        dimension="some.dimension.name",
    )
    assert link_spec == equal_link_spec
    assert hash(link_spec) == hash(equal_link_spec)
    # Equal specs collapse in a set; distinct specs stay separate.
    assert {link_spec, equal_link_spec} == {link_spec}
    assert len({link_spec, link_spec_no_role}) == 2


def test_deployment_spec():
    spec = DeploymentSpec(
        git_config=NamespaceGitConfig(
            github_repo_path="some/repo",
            git_branch="main",
            git_path="nodes",
            git_only=True,
        ),
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
        "git_config": {
            "git_branch": "main",
            "git_only": True,
            "git_path": "nodes",
            "github_repo_path": "some/repo",
            "parent_namespace": None,
            "default_branch": None,
            "branch_namespace": None,
            "git_root_namespace": None,
        },
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
        "hierarchies": [],
        "preaggregations": [],
        "custom_metadata_schemas": None,
        "source": None,
        "auto_register_sources": True,
        "force": False,
        "allow_empty": False,
        "default_catalog": None,
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


def test_eq_columns_source_column_removal():
    """Column removal in an explicit list (source node, compare_types=True) is detected."""
    col_id = ColumnSpec(name="id", type="int")
    col_val = ColumnSpec(name="important_col", type="string")

    # Removing a column: a has fewer columns than b → not equal
    assert not eq_columns([col_id], [col_id, col_val], compare_types=True)

    # Adding a column: a has more columns than b → not equal
    assert not eq_columns([col_id, col_val], [col_id], compare_types=True)

    # Unspecified (None/[]) is still considered equal (don't compare)
    assert eq_columns(None, [col_id, col_val], compare_types=True)
    assert eq_columns([], [col_id, col_val], compare_types=True)

    # For non-source (compare_types=False), missing columns are NOT flagged
    assert eq_columns([col_id], [col_id, col_val], compare_types=False)


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


def test_dimension_join_link_spec_with_join_cardinality():
    """Test DimensionJoinLinkSpec join_cardinality default, equality, and hashing."""
    from datajunction_server.models.dimensionlink import JoinCardinality

    # Default cardinality is many_to_one (a fact row joins at most one dim row).
    default_link = DimensionJoinLinkSpec(
        dimension_node="some.dimension.users",
        join_type="left",
        join_on="events.user_id = some.dimension.users.id",
    )
    assert default_link.join_cardinality == JoinCardinality.MANY_TO_ONE

    fanout_link = DimensionJoinLinkSpec(
        dimension_node="some.dimension.users",
        join_type="left",
        join_on="events.user_id = some.dimension.users.id",
        join_cardinality=JoinCardinality.ONE_TO_MANY,
    )
    assert fanout_link.join_cardinality == JoinCardinality.ONE_TO_MANY

    # Equality and hashing must take join_cardinality into account.
    same_default = DimensionJoinLinkSpec(
        dimension_node="some.dimension.users",
        join_type="left",
        join_on="events.user_id = some.dimension.users.id",
    )
    assert default_link == same_default
    assert hash(default_link) == hash(same_default)
    assert default_link != fanout_link
    assert hash(default_link) != hash(fanout_link)


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


def test_deployment_spec_force_defaults_to_false():
    """DeploymentSpec.force should default to False."""
    spec = DeploymentSpec(namespace="test", nodes=[])
    assert spec.force is False


def test_deployment_spec_force_can_be_set_true():
    """DeploymentSpec.force=True should be accepted and round-trip via model_dump."""
    spec = DeploymentSpec(namespace="test", nodes=[], force=True)
    assert spec.force is True
    assert spec.model_dump()["force"] is True


def test_deployment_spec_rejects_empty_namespace():
    """An empty namespace must be rejected at the model boundary.

    Regression: previously set_namespaces silently no-op'd on an empty
    namespace, leaving ``${prefix}`` unrendered on each spec. That survived
    all the way to DimensionLink.parse_join_sql, which then failed with a
    cryptic ANTLR "mismatched input '$'" error. Failing here makes the
    real problem visible at the edge.
    """
    with pytest.raises(DJInvalidDeploymentConfig, match="namespace is required"):
        DeploymentSpec(namespace="", nodes=[])

    with pytest.raises(DJInvalidDeploymentConfig, match="namespace is required"):
        DeploymentSpec.model_validate({"namespace": "", "nodes": []})


def test_deployment_spec_propagates_namespace_to_links():
    """set_namespaces must reach dimension_links, not just the node itself.

    Specifically covers the ``${prefix}shared.date_dim``-style link where the
    dim node is in the same deployment namespace — the render only happens
    when link.namespace is set.
    """
    spec = DeploymentSpec(
        namespace="myproject.dev",
        nodes=[
            DimensionSpec(
                name="${prefix}ops.x",
                node_type=NodeType.DIMENSION,
                query="select 1 as a",
                columns=[ColumnSpec(name="a", type="int", attributes=["primary_key"])],
                primary_key=["a"],
                dimension_links=[
                    DimensionJoinLinkSpec(
                        dimension_node="${prefix}shared.date_dim",
                        join_on=("${prefix}ops.x.a = ${prefix}shared.date_dim.day_id"),
                        role="utc_date",
                    ),
                ],
            ),
        ],
    )
    node = spec.nodes[0]
    link = node.dimension_links[0]
    assert node.namespace == "myproject.dev"
    assert link.namespace == "myproject.dev"
    assert node.rendered_name == "myproject.dev.ops.x"
    assert link.rendered_dimension_node == "myproject.dev.shared.date_dim"
    assert link.rendered_join_on == (
        "myproject.dev.ops.x.a = myproject.dev.shared.date_dim.day_id"
    )


def test_cube_spec_metrics_and_dimensions_default_empty():
    """A CubeSpec missing metrics/dimensions parses with empty defaults.

    Both fields used to be required (and ``dimensions`` had a buggy
    ``default_factory=dict``), so a YAML round-trip that dropped empty lists
    would fail to re-parse. Now both default to ``[]`` so the deployment
    proceeds and downstream cube validation can flag the cube as INVALID.
    """
    from datajunction_server.models.deployment import CubeSpec

    cube = CubeSpec(namespace="test", name="empty_cube")
    assert cube.metrics == []
    assert cube.dimensions == []


def test_cube_spec_eq_non_cube_spec():
    """CubeSpec.__eq__ returns False when compared to a non-CubeSpec."""
    from datajunction_server.models.deployment import CubeSpec

    cube = CubeSpec(
        namespace="test",
        name="my_cube",
        metrics=["test.metric"],
        dimensions=[],
    )
    assert cube.__eq__(object()) is False
    assert cube.__eq__("not a cube spec") is False


def test_cube_spec_eq_different_rendered_name():
    """CubeSpec.__eq__ returns False when super().__eq__ fails (different rendered names)."""
    from datajunction_server.models.deployment import CubeSpec

    cube1 = CubeSpec(
        namespace="test",
        name="cube_a",
        metrics=["test.metric"],
        dimensions=[],
    )
    cube2 = CubeSpec(
        namespace="test",
        name="cube_b",
        metrics=["test.metric"],
        dimensions=[],
    )
    assert cube1.__eq__(cube2) is False


def test_deployment_results_property_getter():
    """Deployment.deployment_results deserializes results list into DeploymentResult objects."""
    from datajunction_server.database.deployment import Deployment
    from datajunction_server.models.deployment import (
        DeploymentResult,
        DeploymentSpec,
        DeploymentStatus,
    )

    deployment = Deployment(
        spec=DeploymentSpec(namespace="test").model_dump(),
        status=DeploymentStatus.SUCCESS,
        results=[
            {
                "name": "test_node",
                "deploy_type": "node",
                "status": "success",
                "operation": "create",
                "message": "Created",
            },
        ],
    )
    results = deployment.deployment_results
    assert len(results) == 1
    assert results[0].name == "test_node"
    assert results[0].status == DeploymentResult.Status.SUCCESS


def test_deployment_spec_preserves_explicit_preagg_namespace():
    """
    set_namespaces propagates the deployment namespace onto pre-agg specs that
    don't have one, but leaves an already-namespaced pre-agg spec untouched.
    """
    spec = DeploymentSpec(
        namespace="ns",
        preaggregations=[
            PreAggSpec(
                name="already_scoped",
                namespace="explicit",
                catalog="c",
                schema="s",
                table="t",
            ),
            PreAggSpec(
                name="unscoped",
                catalog="c",
                schema="s",
                table="t",
            ),
        ],
    )
    assert spec.preaggregations[0].namespace == "explicit"
    assert spec.preaggregations[1].namespace == "ns"


def _preagg_spec_dict() -> dict:
    """Each metric and dimension declared with the column that holds it."""
    return {
        "name": "p",
        "namespace": "ns",
        "metrics": {"${prefix}count": "cnt"},
        "dimensions": {
            "${prefix}d.attr": "phys_attr",
            # The physical column matches the DJ column name, and says so anyway.
            "${prefix}d.same": "same",
        },
        "catalog": "c",
        "schema": "s",
        "table": "t",
        "valid_through_ts": 1700000000,
    }


def test_preagg_spec_renders_column_bindings():
    """
    Every reference is prefix-rendered against the namespace and stays bound to
    the physical column that holds it, which is left as-is.
    """
    spec = PreAggSpec.model_validate(_preagg_spec_dict())
    assert spec.rendered_metrics == {"ns.count": "cnt"}
    assert spec.rendered_dimensions == {
        "ns.d.attr": "phys_attr",
        "ns.d.same": "same",
    }


def test_preagg_spec_round_trips_through_model_dump():
    """
    A dumped spec is what crosses the wire from the client, so re-validating a
    dump has to land on an equal spec.
    """
    spec = PreAggSpec.model_validate(_preagg_spec_dict())
    # `namespace` is excluded from the dump (the deployment re-injects it), so
    # the comparison is on the dumped fields.
    assert (
        PreAggSpec.model_validate(spec.model_dump()).model_dump() == spec.model_dump()
    )
    assert spec.model_dump() == {
        "name": "p",
        "metrics": {"${prefix}count": "cnt"},
        "dimensions": {"${prefix}d.attr": "phys_attr", "${prefix}d.same": "same"},
        "catalog": "c",
        "schema_": "s",
        "table": "t",
        "valid_through_ts": 1700000000,
    }


# The three rejections that hold a spec to the map form are shared with
# POST /preaggs/register and asserted for both surfaces in
# tests/models/preagg_binding_test.py.


def test_tag_spec_metadata_aliases():
    """
    A tag's metadata bag may be authored as `tag_metadata`, `metadata` or
    `custom_metadata`; all three land on `tag_metadata`. `display_name` is
    optional (the deployment labelizes the name when it is absent).
    """
    metadata = {"order": 1, "display": {"color": "blue"}}
    for key in ("tag_metadata", "metadata", "custom_metadata"):
        tag_spec = TagSpec.model_validate(
            {
                "name": "inventory",
                "description": "Inventory tag",
                "tag_type": "group",
                key: metadata,
            },
        )
        assert tag_spec.model_dump() == {
            "name": "inventory",
            "display_name": None,
            "description": "Inventory tag",
            "tag_type": "group",
            "tag_metadata": metadata,
        }

    # Legacy tag entries with neither display_name nor metadata still validate
    assert TagSpec.model_validate(
        {"name": "deprecated", "description": "d", "tag_type": "Maintenance"},
    ).model_dump() == {
        "name": "deprecated",
        "display_name": None,
        "description": "d",
        "tag_type": "Maintenance",
        "tag_metadata": None,
    }


def all_node_spec_classes() -> list[type[NodeSpec]]:
    """Every NodeSpec class, including the abstract intermediate ones."""

    def descendants(klass):
        for subclass in klass.__subclasses__():
            yield subclass
            yield from descendants(subclass)

    return [NodeSpec, *descendants(NodeSpec)]


def test_every_spec_field_has_an_explicit_change_tier():
    """
    Every field on every NodeSpec class must be explicitly classified into a
    ChangeTier. This is what turns "someone added a field and forgot to say how
    significant a change to it is" into a red test instead of a silent fallback.
    """
    unclassified = {
        spec_class.__name__: spec_class.unclassified_fields()
        for spec_class in all_node_spec_classes()
        if spec_class.unclassified_fields()
    }
    assert unclassified == {}


def test_change_tier_lookup_walks_the_mro():
    """
    Each spec class declares only the fields it introduces; lookup finds inherited
    classifications, and anything unclassified falls back to MAJOR (fail safe).
    """
    assert CubeSpec.field_change_tier("metrics") == ChangeTier.MAJOR
    assert CubeSpec.field_change_tier("description") == ChangeTier.MINOR
    assert CubeSpec.field_change_tier("name") == ChangeTier.NONE
    assert CubeSpec.field_change_tier("a_field_nobody_classified") == ChangeTier.MAJOR

    assert CubeSpec.field_order_change_tier("metrics") == ChangeTier.MINOR
    assert CubeSpec.field_order_change_tier("dimensions") == ChangeTier.MINOR
    assert CubeSpec.field_order_change_tier("filters") == ChangeTier.NONE
    assert CubeSpec.field_order_change_tier("description") == ChangeTier.NONE

    assert TransformSpec.field_change_tier("query") == ChangeTier.MAJOR
    assert TransformSpec.field_change_tier("primary_key") == ChangeTier.MAJOR
    assert TransformSpec.field_change_tier("tags") == ChangeTier.MINOR
    assert TransformSpec.order_sensitive_fields() == []
    assert CubeSpec.order_sensitive_fields() == ["metrics", "dimensions", "filters"]


def test_fold_change_tiers():
    """The most significant change wins; no changes at all is NONE."""
    assert fold_change_tiers([]) == ChangeTier.NONE
    assert fold_change_tiers([ChangeTier.NONE]) == ChangeTier.NONE
    assert fold_change_tiers([ChangeTier.NONE, ChangeTier.MINOR]) == ChangeTier.MINOR
    assert (
        fold_change_tiers([ChangeTier.MINOR, ChangeTier.MAJOR, ChangeTier.NONE])
        == ChangeTier.MAJOR
    )
    # The tiers are ordered, and spaced so a future tier fits between them.
    assert ChangeTier.NONE < ChangeTier.MINOR < ChangeTier.MAJOR
    assert ChangeTier.MINOR - ChangeTier.NONE > 1


def test_change_tier_classification():
    """The table of decisions, exercised through the shared classifier."""
    assert CubeSpec.change_tier([], []) == ChangeTier.NONE
    # metric / dimension / filter set changes are major
    assert CubeSpec.change_tier(["metrics"]) == ChangeTier.MAJOR
    assert CubeSpec.change_tier(["dimensions"]) == ChangeTier.MAJOR
    assert CubeSpec.change_tier(["filters"]) == ChangeTier.MAJOR
    # reordering metrics or dimensions is minor, reordering filters is nothing
    assert CubeSpec.change_tier([], ["metrics"]) == ChangeTier.MINOR
    assert CubeSpec.change_tier([], ["dimensions"]) == ChangeTier.MINOR
    assert CubeSpec.change_tier([], ["filters"]) == ChangeTier.NONE
    # metadata is minor
    for field in ("description", "display_name", "mode", "custom_metadata"):
        assert CubeSpec.change_tier([field]) == ChangeTier.MINOR
    # a major change alongside a minor one is still major
    assert CubeSpec.change_tier(["description", "filters"]) == ChangeTier.MAJOR
    # query, columns, primary key and dimension links are major
    assert TransformSpec.change_tier(["query"]) == ChangeTier.MAJOR
    assert TransformSpec.change_tier(["columns"]) == ChangeTier.MAJOR
    assert TransformSpec.change_tier(["primary_key"]) == ChangeTier.MAJOR
    assert TransformSpec.change_tier(["dimension_links"]) == ChangeTier.MAJOR


def test_bump_version():
    """NONE keeps the version: an unchanged node must not be given a new one."""
    assert bump_version("v1.3", ChangeTier.NONE) == "v1.3"
    assert bump_version("v1.3", ChangeTier.MINOR) == "v1.4"
    assert bump_version("v1.3", ChangeTier.MAJOR) == "v2.0"


def a_cube(**kwargs) -> CubeSpec:
    """A minimal cube spec, overridable field by field."""
    defaults: dict = {
        "name": "test_cube",
        "metrics": ["ns.a", "ns.b"],
        "dimensions": ["ns.d.one", "ns.d.two"],
    }
    return CubeSpec(**{**defaults, **kwargs})


def test_cube_spec_eq_is_order_sensitive_for_metrics_and_dimensions():
    """
    Metric and dimension ordering sets the cube's column order, so a reorder is a
    real difference the deployment path has to be able to see.
    """
    assert a_cube() == a_cube()
    assert a_cube() != a_cube(metrics=["ns.b", "ns.a"])
    assert a_cube() != a_cube(dimensions=["ns.d.two", "ns.d.one"])
    assert a_cube() != a_cube(metrics=["ns.a"])


def test_cube_spec_eq_treats_filters_as_a_set():
    """Filters are ANDed, so reordering them is cosmetic and not a change."""
    one = a_cube(filters=["x = 1", "y = 2"])
    assert one == a_cube(filters=["y = 2", "x = 1"])
    assert one != a_cube(filters=["x = 1"])
    assert one != a_cube(filters=["x = 1", "y = 3"])


def test_cube_spec_order_diff():
    """
    order_diff names the order-sensitive fields whose contents held still while
    their ordering moved -- the changes diff()'s set comparison cannot see.
    """
    one = a_cube(filters=["x = 1", "y = 2"])
    assert one.order_diff(a_cube(filters=["x = 1", "y = 2"])) == []
    assert one.order_diff(
        a_cube(metrics=["ns.b", "ns.a"], filters=["x = 1", "y = 2"]),
    ) == ["metrics"]
    assert one.order_diff(
        a_cube(dimensions=["ns.d.two", "ns.d.one"], filters=["x = 1", "y = 2"]),
    ) == ["dimensions"]
    assert one.order_diff(a_cube(filters=["y = 2", "x = 1"])) == ["filters"]
    # A set change is not a reorder — diff() reports that one instead.
    assert one.order_diff(a_cube(metrics=["ns.a"], filters=["x = 1", "y = 2"])) == []
    assert one.diff(a_cube(metrics=["ns.a"], filters=["x = 1", "y = 2"])) == ["metrics"]


def test_diff_compares_dicts_by_value():
    """
    A dict field whose keys are unchanged but whose values moved is a change.
    Iterating a dict yields only its keys, so the set comparison lists use missed
    this and `custom_metadata` edits were reported as no change at all.
    """
    one = a_cube(custom_metadata={"tier": "1"})
    assert one.diff(a_cube(custom_metadata={"tier": "2"})) == ["custom_metadata"]
    assert one.diff(a_cube(custom_metadata={"tier": "1"})) == []
    assert a_cube().diff(a_cube(custom_metadata={})) == []


def _partitioned_cube(**materialization) -> CubeSpec:
    """A cube with a temporal partition, optionally carrying a materialization block."""
    return CubeSpec(
        namespace="test",
        name="my_cube",
        metrics=["${prefix}num_orders"],
        dimensions=["${prefix}date_dim.dateint"],
        columns=[
            ColumnSpec(
                name="${prefix}date_dim.dateint",
                partition=PartitionSpec(
                    type=PartitionType.TEMPORAL,
                    granularity=Granularity.DAY,
                    format="yyyyMMdd",
                ),
            ),
        ],
        materialization=MaterializationSpec(**materialization)
        if materialization
        else None,
    )


def test_materialization_spec_defaults():
    """An author supplying only a schedule gets incremental daily-lookback defaults."""
    spec = MaterializationSpec(schedule="0 6 * * *")
    assert spec.model_dump() == {
        "schedule": "0 6 * * *",
        "strategy": MaterializationStrategy.INCREMENTAL_TIME,
        "lookback_window": "1 DAY",
        "retention": "400 DAYS",
        "coverage": None,
        "druid": None,
        "spark": None,
        "platform": None,
    }


def test_materialization_spec_retention_override():
    """An author can widen or narrow how long Druid keeps the ingested data."""
    spec = MaterializationSpec(schedule="0 6 * * *", retention="30 DAYS")
    assert spec.model_dump() == {
        "schedule": "0 6 * * *",
        "strategy": MaterializationStrategy.INCREMENTAL_TIME,
        "lookback_window": "1 DAY",
        "retention": "30 DAYS",
        "coverage": None,
        "druid": None,
        "spark": None,
        "platform": None,
    }


def test_materialization_spec_full_strategy_drops_lookback():
    """
    ``lookback_window`` is meaningless for FULL, so it is normalized away -- otherwise
    two equivalent specs could compare unequal and churn a live workflow on deploy.
    ``retention`` is not: a full rebuild loads the widest span of history there is.
    """
    declared = MaterializationSpec(
        schedule="0 6 * * *",
        strategy=MaterializationStrategy.FULL,
        lookback_window="7 DAY",
    )
    assert declared.model_dump() == {
        "schedule": "0 6 * * *",
        "strategy": MaterializationStrategy.FULL,
        "lookback_window": None,
        "retention": "400 DAYS",
        "coverage": None,
        "druid": None,
        "spark": None,
        "platform": None,
    }
    assert declared == MaterializationSpec(
        schedule="0 6 * * *",
        strategy=MaterializationStrategy.FULL,
    )


def test_materialization_spec_coverage_fixed_span():
    """
    A fixed span is declared as an inclusive ``from``/``to`` pair, and dumps back out
    under the authored ``from`` key rather than the ``from_`` field name.
    """
    spec = MaterializationSpec(
        schedule="0 6 * * *",
        coverage={"from": "2024-01-01", "to": "2024-06-30"},
    )
    assert spec.coverage == CoverageSpec(from_=date(2024, 1, 1), to=date(2024, 6, 30))
    assert spec.model_dump() == {
        "schedule": "0 6 * * *",
        "strategy": MaterializationStrategy.INCREMENTAL_TIME,
        "lookback_window": "1 DAY",
        "retention": "400 DAYS",
        "coverage": {"from": "2024-01-01", "to": "2024-06-30", "window": None},
        "druid": None,
        "spark": None,
        "platform": None,
    }


def test_materialization_spec_coverage_open_ended_span():
    """``to`` is optional: a span with only a ``from`` is ongoing."""
    spec = MaterializationSpec(
        schedule="0 6 * * *",
        coverage={"from": "2024-01-01"},
    )
    assert spec.model_dump(mode="json", exclude_none=True) == {
        "schedule": "0 6 * * *",
        "strategy": "incremental_time",
        "lookback_window": "1 DAY",
        "retention": "400 DAYS",
        "coverage": {"from": "2024-01-01"},
    }


def test_materialization_spec_coverage_rolling_window():
    """The other form is a trailing duration, in the style of ``lookback_window``."""
    spec = MaterializationSpec(
        schedule="0 6 * * *",
        coverage={"window": "800 DAYS"},
    )
    assert spec.coverage == CoverageSpec(window="800 DAYS")
    assert spec.model_dump(mode="json", exclude_none=True) == {
        "schedule": "0 6 * * *",
        "strategy": "incremental_time",
        "lookback_window": "1 DAY",
        "retention": "400 DAYS",
        "coverage": {"window": "800 DAYS"},
    }


def test_materialization_spec_without_coverage():
    """Coverage is optional, and absent means the author has declared no span."""
    assert MaterializationSpec(schedule="0 6 * * *").coverage is None


def test_materialization_spec_empty_coverage_is_absent():
    """
    A ``coverage:`` block declaring neither form declares nothing, so it normalizes to
    absent -- otherwise it would compare unequal to the same spec without the block and
    churn a live workflow on every deploy.
    """
    spec = MaterializationSpec(schedule="0 6 * * *", coverage={})
    assert spec.coverage is None
    assert spec == MaterializationSpec(schedule="0 6 * * *")


def test_materialization_spec_coverage_equality_ignores_spelling():
    """
    Two specs meaning the same span compare equal however they were spelled: dates are
    parsed rather than kept as text, and the window's whitespace is collapsed.
    """
    assert MaterializationSpec(
        schedule="0 6 * * *",
        coverage={"from": "2024-01-01"},
    ) == MaterializationSpec(
        schedule="0 6 * * *",
        coverage=CoverageSpec(from_=date(2024, 1, 1)),
    )
    assert MaterializationSpec(
        schedule="0 6 * * *",
        coverage={"window": "  800   DAYS "},
    ) == MaterializationSpec(schedule="0 6 * * *", coverage={"window": "800 DAYS"})


def test_materialization_spec_coverage_rejects_both_forms():
    """A fixed span and a rolling window say different things about the same cube."""
    with pytest.raises(DJInvalidInputException) as exc_info:
        MaterializationSpec(
            schedule="0 6 * * *",
            coverage={"from": "2024-01-01", "window": "800 DAYS"},
        )
    assert exc_info.value.message == ("Declare a fixed span or a window, not both.")


def test_materialization_spec_coverage_rejects_to_without_from():
    """An end date on its own does not describe a span."""
    with pytest.raises(DJInvalidInputException) as exc_info:
        MaterializationSpec(schedule="0 6 * * *", coverage={"to": "2024-06-30"})
    assert exc_info.value.message == ("Coverage needs a `from` to go with `to`.")


def test_materialization_spec_coverage_rejects_backwards_span():
    """A span that ends before it starts covers nothing."""
    with pytest.raises(DJInvalidInputException) as exc_info:
        MaterializationSpec(
            schedule="0 6 * * *",
            coverage={"from": "2024-06-30", "to": "2024-01-01"},
        )
    assert exc_info.value.message == (
        "Coverage ends before it starts: 2024-01-01 precedes 2024-06-30."
    )


def test_coverage_spec_survives_a_json_round_trip():
    """
    What ``model_dump`` produces is exactly what validates back, which is what lets a
    declared span pass through the JSON materialization config unchanged.
    """
    for coverage in (
        CoverageSpec(from_=date(2024, 1, 1), to=date(2024, 6, 30)),
        CoverageSpec(from_=date(2024, 1, 1)),
        CoverageSpec(window="800 DAYS"),
    ):
        stored = json.loads(json.dumps(coverage.model_dump()))
        assert CoverageSpec.model_validate(stored) == coverage


@pytest.mark.parametrize(
    ("coverage", "span"),
    [
        # A fixed span with both ends is already the answer.
        (
            CoverageSpec(from_=date(2024, 1, 1), to=date(2024, 6, 30)),
            (date(2024, 1, 1), date(2024, 6, 30)),
        ),
        # An ongoing span runs to yesterday, the last full day.
        (
            CoverageSpec(from_=date(2024, 1, 1)),
            (date(2024, 1, 1), date(2026, 8, 22)),
        ),
        # A rolling window counts back from yesterday, both ends included.
        (
            CoverageSpec(window="800 DAYS"),
            (date(2024, 6, 14), date(2026, 8, 22)),
        ),
        (
            CoverageSpec(window="2 weeks"),
            (date(2026, 8, 9), date(2026, 8, 22)),
        ),
        # A window in a unit DJ cannot count in days.
        (CoverageSpec(window="6 MONTHS"), None),
        # A window that reads as no duration at all.
        (CoverageSpec(window="a while"), None),
    ],
)
def test_coverage_spec_span(coverage, span):
    """The days a declared span asks a backfill to run, as of a given day."""
    assert coverage.span(date(2026, 8, 23)) == span


def test_cube_spec_incremental_requires_temporal_partition():
    """An incremental cube with no temporal partition has nothing to increment over."""
    with pytest.raises(DJInvalidDeploymentConfig) as exc_info:
        CubeSpec(
            namespace="test",
            name="my_cube",
            metrics=["${prefix}num_orders"],
            dimensions=["${prefix}date_dim.dateint"],
            materialization=MaterializationSpec(schedule="0 6 * * *"),
        )
    assert exc_info.value.message == (
        "Cube `my_cube` declares an `incremental_time` materialization but no "
        "temporal partition. Add `partition: {type: temporal, ...}` to the cube "
        "column that partitions it, or use `strategy: full`."
    )


def test_cube_spec_full_strategy_needs_no_partition():
    """FULL rebuilds everything, so it carries no partition requirement."""
    spec = CubeSpec(
        namespace="test",
        name="my_cube",
        metrics=["${prefix}num_orders"],
        dimensions=["${prefix}date_dim.dateint"],
        materialization=MaterializationSpec(
            schedule="0 6 * * *",
            strategy=MaterializationStrategy.FULL,
        ),
    )
    assert spec.materialization == MaterializationSpec(
        schedule="0 6 * * *",
        strategy=MaterializationStrategy.FULL,
        lookback_window=None,
    )


def test_cube_spec_partition_satisfies_incremental():
    """The partition is read off the cube's columns, not restated on the block."""
    spec = _partitioned_cube(schedule="0 6 * * *", lookback_window="3 DAY")
    assert spec.materialization == MaterializationSpec(
        schedule="0 6 * * *",
        strategy=MaterializationStrategy.INCREMENTAL_TIME,
        lookback_window="3 DAY",
    )


def test_cube_spec_eq_ignores_materialization():
    """
    Equality decides whether the cube's *definition* changed, which gates a new node
    revision. Rescheduling a build is not a definition change.
    """
    six_am = _partitioned_cube(schedule="0 6 * * *")
    midnight = _partitioned_cube(schedule="0 0 * * *")
    unscheduled = _partitioned_cube()

    assert six_am == midnight
    assert six_am == unscheduled

    # ...while the definition itself still drives inequality.
    renamed = _partitioned_cube(schedule="0 6 * * *")
    renamed.metrics = ["${prefix}num_returns"]
    assert six_am != renamed


def test_materialization_change_tier_is_none():
    """
    `materialization` is explicitly classified, so the "every field is classified"
    test stays green rather than falling back to MAJOR -- and it is classified NONE
    because configuring a materialization has never cut a node revision.

    A materialization-only edit is equal under `__eq__` and so never reaches the
    classifier; this pins the tier that would apply if it ever did.
    """
    assert CubeSpec.has_explicit_change_tier("materialization") is True
    assert CubeSpec.field_change_tier("materialization") == ChangeTier.NONE
    assert CubeSpec.change_tier(["materialization"]) == ChangeTier.NONE

    six_am = _partitioned_cube(schedule="0 6 * * *").rendered_spec()
    assert six_am.diff(_partitioned_cube(schedule="0 0 * * *")) == ["materialization"]
    assert CubeSpec.change_tier(six_am.diff(_partitioned_cube())) == ChangeTier.NONE


def test_cube_spec_without_materialization_omits_it_from_export():
    """Unmaterialized cubes must not gain a `materialization: null` key on export."""
    spec = _partitioned_cube()
    assert spec.materialization is None
    assert "materialization" not in spec.model_dump(mode="json", exclude_none=True)


def test_cube_spec_teardown_survives_serialization():
    """
    The two ways a cube can decline to be materialized have to stay distinguishable
    after a round trip. `model_dump` emits every optional field explicitly, so a
    teardown carried by a key's presence alone would be indistinguishable from every
    cube that never mentioned `materialization:` -- and the round-tripped spec would
    read as a request to tear down the whole namespace.
    """
    torn_down = CubeSpec.model_validate(
        {**_partitioned_cube().model_dump(), "materialization": "none"},
    )
    assert torn_down.materialization == MaterializationAction.NONE
    assert (
        CubeSpec.model_validate(torn_down.model_dump()).materialization
        == MaterializationAction.NONE
    )

    unmanaged = CubeSpec.model_validate(_partitioned_cube().model_dump())
    assert unmanaged.materialization is None
    assert CubeSpec.model_validate(unmanaged.model_dump()).materialization is None

    # `rendered_spec` round-trips through JSON too, and preserved neither before.
    assert torn_down.rendered_spec().materialization == MaterializationAction.NONE
    assert unmanaged.rendered_spec().materialization is None


def test_cube_spec_teardown_sentinel_is_case_insensitive():
    """`None` is what a Python author writes and `NONE` what a YAML one does."""
    for spelling in ("none", "None", "NONE"):
        spec = CubeSpec.model_validate(
            {**_partitioned_cube().model_dump(), "materialization": spelling},
        )
        assert spec.materialization == MaterializationAction.NONE


def test_cube_spec_teardown_needs_no_temporal_partition():
    """
    The partition requirement is a property of an incremental block, so a cube being
    torn down is exempt -- it may well be losing the partition in the same push.
    """
    spec = CubeSpec(
        namespace="test",
        name="my_cube",
        metrics=["${prefix}num_orders"],
        dimensions=["${prefix}date_dim.dateint"],
        materialization=MaterializationAction.NONE,
    )
    assert spec.materialization == MaterializationAction.NONE


def _cube_declaring(materialization) -> CubeSpec:
    """The same partitioned cube, declaring whatever is handed to it."""
    return CubeSpec(
        namespace="test",
        name="my_cube",
        metrics=["${prefix}num_orders"],
        dimensions=["${prefix}date_dim.dateint"],
        columns=[
            ColumnSpec(
                name="${prefix}date_dim.dateint",
                partition=PartitionSpec(
                    type=PartitionType.TEMPORAL,
                    granularity=Granularity.DAY,
                    format="yyyyMMdd",
                ),
            ),
        ],
        materialization=materialization,
    )


def test_cube_spec_accepts_several_materializations():
    """
    A cube legitimately needs two: an incremental build for freshness and a periodic
    full rebuild that corrects late-arriving data and dimension backfills.
    """
    spec = _cube_declaring(
        [
            MaterializationSpec(schedule="59 23 * * *", lookback_window="1 DAY"),
            MaterializationSpec(
                schedule="0 6 * * *",
                strategy=MaterializationStrategy.FULL,
            ),
        ],
    )
    assert spec.declared_materializations == [
        MaterializationSpec(schedule="59 23 * * *", lookback_window="1 DAY"),
        MaterializationSpec(
            schedule="0 6 * * *",
            strategy=MaterializationStrategy.FULL,
            lookback_window=None,
        ),
    ]


def test_cube_spec_scalar_materialization_reads_as_one_block():
    """
    The scalar form is not a legacy shape to be migrated: it stays exactly what it
    was, and only the reading of it is common with the list form.
    """
    spec = _partitioned_cube(schedule="0 6 * * *")
    assert spec.materialization == MaterializationSpec(schedule="0 6 * * *")
    assert spec.declared_materializations == [
        MaterializationSpec(schedule="0 6 * * *"),
    ]


def test_cube_spec_declares_nothing_without_a_block():
    """Neither an absent block nor the teardown sentinel declares anything to build."""
    assert _partitioned_cube().declared_materializations == []
    assert _cube_declaring(MaterializationAction.NONE).declared_materializations == []


def test_cube_spec_rejects_repeated_materialization_strategy():
    """
    Strategy is what matches a declared entry to the materialization it owns, so two
    entries sharing one leave no way to say which is which.
    """
    with pytest.raises(DJInvalidDeploymentConfig) as exc_info:
        _cube_declaring(
            [
                MaterializationSpec(schedule="59 23 * * *"),
                MaterializationSpec(schedule="0 6 * * *"),
            ],
        )
    assert exc_info.value.message == (
        "Cube `my_cube` declares more than one `incremental_time` materialization. "
        "Strategies identify a cube's materializations, so each one may appear at "
        "most once."
    )


def test_cube_spec_rejects_empty_materialization_list():
    """An empty list cannot be told from the teardown sentinel, so it is refused."""
    with pytest.raises(DJInvalidDeploymentConfig) as exc_info:
        _cube_declaring([])
    assert exc_info.value.message == (
        "Cube `my_cube` declares an empty `materialization:` list. Use "
        "`materialization: none` to remove the cube's materialization, or omit the "
        "key to leave it alone."
    )


def test_cube_spec_incremental_entry_in_a_list_still_needs_a_partition():
    """The partition requirement is per entry, not per cube."""
    with pytest.raises(DJInvalidDeploymentConfig) as exc_info:
        CubeSpec(
            namespace="test",
            name="my_cube",
            metrics=["${prefix}num_orders"],
            dimensions=["${prefix}date_dim.dateint"],
            materialization=[
                MaterializationSpec(
                    schedule="0 6 * * *",
                    strategy=MaterializationStrategy.FULL,
                ),
                MaterializationSpec(schedule="59 23 * * *"),
            ],
        )
    assert exc_info.value.message == (
        "Cube `my_cube` declares an `incremental_time` materialization but no "
        "temporal partition. Add `partition: {type: temporal, ...}` to the cube "
        "column that partitions it, or use `strategy: full`."
    )


def test_cube_spec_materialization_shapes_survive_serialization():
    """
    Every shape a cube can declare has to mean the same thing after a round trip, or
    a spec pulled from the server and pushed back would change what the cube does.
    """
    blocks = [
        MaterializationSpec(schedule="59 23 * * *", lookback_window="1 DAY"),
        MaterializationSpec(
            schedule="0 6 * * *",
            strategy=MaterializationStrategy.FULL,
            lookback_window=None,
        ),
    ]
    for declared in (
        blocks,
        blocks[0],
        MaterializationAction.NONE,
        None,
    ):
        spec = _cube_declaring(declared)
        assert CubeSpec.model_validate(spec.model_dump()).materialization == declared
        assert spec.rendered_spec().materialization == declared


def test_cube_spec_eq_ignores_a_list_of_materializations():
    """
    Whichever shape the block is written in, it is not part of the cube's definition
    and must not mint a revision. The diff still names it, at tier NONE.
    """
    one = _cube_declaring(
        [
            MaterializationSpec(schedule="59 23 * * *"),
            MaterializationSpec(
                schedule="0 6 * * *",
                strategy=MaterializationStrategy.FULL,
            ),
        ],
    )
    two = _cube_declaring(MaterializationSpec(schedule="59 23 * * *"))
    assert one == two
    assert one.rendered_spec().diff(two) == ["materialization"]
    assert CubeSpec.change_tier(one.rendered_spec().diff(two)) == ChangeTier.NONE


def test_cube_spec_materialization_diff_ignores_declaration_order():
    """
    A cube pulled from the server lists its materializations in name order, and an
    author is free to write them the other way round. Reading that as a change would
    report an edit on every deploy of an untouched cube.
    """
    blocks = [
        MaterializationSpec(schedule="59 23 * * *", coverage={"window": "800 DAYS"}),
        MaterializationSpec(
            schedule="0 6 * * *",
            strategy=MaterializationStrategy.FULL,
        ),
    ]
    assert (
        _cube_declaring(blocks)
        .rendered_spec()
        .diff(
            _cube_declaring(list(reversed(blocks))),
        )
        == []
    )


def test_a_schema_namespace_defaults_to_the_deployment():
    """Omitting it keeps the existing behaviour: the deploying namespace."""
    spec = DeploymentSpec(
        namespace="shared",
        nodes=[],
        custom_metadata_schemas=[
            CustomMetadataSchemaSpec(key="system", json_schema={"type": "object"}),
        ],
    )
    assert spec.custom_metadata_schemas[0].namespace == "shared"


def test_a_schema_may_be_scoped_to_a_sub_namespace():
    """Narrower than the deployment is how a vocabulary rolls out in stages."""
    spec = DeploymentSpec(
        namespace="shared",
        nodes=[],
        custom_metadata_schemas=[
            CustomMetadataSchemaSpec(
                key="system",
                namespace="shared.conformed",
                json_schema={"type": "object"},
            ),
        ],
    )
    assert spec.custom_metadata_schemas[0].namespace == "shared.conformed"


@pytest.mark.parametrize(
    "outside",
    ["arc", "shared_other", "other.shared", "sharedx"],
)
def test_a_schema_namespace_outside_the_deployment_is_rejected(outside):
    """
    A manifest may scope a schema narrower than itself, never wider or sideways --
    otherwise one repo governs another repo's nodes. `shared_other` and `sharedx`
    are the prefix trap: they start with the namespace but are not beneath it.
    """
    with pytest.raises(DJInvalidDeploymentConfig) as exc_info:
        DeploymentSpec(
            namespace="shared",
            nodes=[],
            custom_metadata_schemas=[
                CustomMetadataSchemaSpec(
                    key="system",
                    namespace=outside,
                    json_schema={"type": "object"},
                ),
            ],
        )
    assert "not 'shared' or beneath it" in str(exc_info.value)
