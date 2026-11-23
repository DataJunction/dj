"""The v2 local contracts omit presentation metadata and retain data inputs."""

import pytest

from datajunction_server.models.deployment import (
    ColumnSpec,
    CubeSpec,
    DimensionJoinLinkSpec,
    DimensionReferenceLinkSpec,
    DimensionSpec,
    MetricSpec,
    SourceSpec,
    TransformSpec,
)
from datajunction_server.models.filter_requirements import FilterRequirement
from datajunction_server.semantic_fingerprints.v2_dependencies import (
    _dimension_link_contract,
    _effective_filter_requirements,
    _node_contract,
    _output_column_contract,
)


def _specs():
    return [
        SourceSpec(
            name="ns.source",
            catalog="warehouse",
            schema_="sales",
            table="orders",
            columns=[ColumnSpec(name="id", type="bigint")],
            primary_key=["id"],
        ),
        DimensionSpec(
            name="ns.dimension",
            query="SELECT id FROM ns.source",
            columns=[ColumnSpec(name="id", type="bigint")],
            primary_key=["id"],
        ),
        TransformSpec(
            name="ns.transform",
            query="SELECT id FROM ns.source",
            columns=[ColumnSpec(name="id", type="bigint")],
        ),
        MetricSpec(
            name="ns.metric",
            query="SELECT SUM(id) AS total FROM ns.source",
            columns=[ColumnSpec(name="total", type="bigint")],
            required_dimensions=["ns.source.id"],
        ),
        CubeSpec(
            name="ns.cube",
            metrics=["ns.metric"],
            dimensions=["ns.dimension.id"],
            filters=["ns.dimension.id > 0"],
            columns=[ColumnSpec(name="total", type="bigint")],
        ),
    ]


def _contract(spec):
    return _node_contract(spec, tuple(spec.columns or ()))


def _with_presentation_metadata(spec, label):
    columns = [
        column.model_copy(
            update={
                "description": f"{label} column description",
                "display_name": f"{label} column label",
                "attributes": [label],
            },
        )
        for column in (spec.columns or [])
    ]
    return spec.model_copy(
        update={
            "description": f"{label} description",
            "display_name": f"{label} display name",
            "owners": [label],
            "tags": [label],
            "custom_metadata": {label: {"value": label}},
            "columns": columns,
        },
    )


def test_node_and_column_presentation_metadata_are_excluded_for_each_type():
    for spec in _specs():
        before = _contract(spec)
        after = _contract(_with_presentation_metadata(spec, "edited"))

        assert before == after, spec.node_type


def test_each_node_type_retains_a_data_bearing_contract_change():
    for spec in _specs():
        if isinstance(spec, SourceSpec):
            changed = spec.model_copy(update={"table": "orders_v2"})
        elif isinstance(spec, DimensionSpec):
            changed = spec.model_copy(update={"primary_key": ["other_id"]})
        elif isinstance(spec, TransformSpec):
            changed = spec.model_copy(
                update={"columns": [ColumnSpec(name="id", type="varchar")]},
            )
        elif isinstance(spec, MetricSpec):
            changed = spec.model_copy(
                update={"required_dimensions": ["ns.dimension.id"]},
            )
        elif isinstance(spec, CubeSpec):
            changed = spec.model_copy(
                update={"filters": ["ns.dimension.id < 0"]},
            )
        else:
            raise TypeError(f"Unhandled spec type: {type(spec).__name__}")

        assert _contract(spec) != _contract(changed), spec.node_type


def test_join_cardinality_is_warning_only_while_join_predicate_is_semantic():
    link = DimensionJoinLinkSpec(
        dimension_node="ns.dimension",
        join_on="ns.source.id = ns.dimension.id",
    )
    source = _specs()[0].model_copy(update={"dimension_links": [link]})
    changed_cardinality = source.model_copy(
        update={
            "dimension_links": [
                link.model_copy(update={"join_cardinality": "one_to_many"}),
            ],
        },
    )
    changed_predicate = source.model_copy(
        update={
            "dimension_links": [
                link.model_copy(
                    update={"join_on": "ns.source.id + 1 = ns.dimension.id"}
                ),
            ],
        },
    )

    assert _contract(source) == _contract(changed_cardinality)
    assert _contract(source) != _contract(changed_predicate)


def test_missing_output_column_has_a_stable_fallback_contract():
    assert _output_column_contract("amount", None) == {
        "name": "amount",
        "type": None,
        "partition": None,
    }
    assert _output_column_contract(
        "amount",
        ColumnSpec(name="amount", type="bigint"),
    ) == {
        "name": "amount",
        "type": "bigint",
        "partition": None,
    }


def test_effective_filter_defaults_are_normalized_and_deduplicated():
    shared = FilterRequirement(
        dimension=["ns.dimension.name", "ns.dimension.id", "ns.dimension.name"],
        default="ns.dimension.id IS NOT NULL",
    )
    metric = MetricSpec(
        name="ns.metric",
        query="SELECT SUM(id) FROM ns.source",
        required_filters=[
            shared,
            FilterRequirement(dimension=["ns.dimension.id"], default=""),
        ],
        recommended_filters=[
            FilterRequirement(
                dimension=["ns.dimension.id", "ns.dimension.name"],
                default="ns.dimension.id IS NOT NULL",
            ),
            FilterRequirement(
                dimension=["ns.dimension.name"],
                default="ns.dimension.name IS NOT NULL",
            ),
        ],
    )

    assert _effective_filter_requirements(metric) == [
        FilterRequirement(
            dimension=["ns.dimension.id", "ns.dimension.name"],
            default="ns.dimension.id IS NOT NULL",
        ),
        FilterRequirement(
            dimension=["ns.dimension.name"],
            default="ns.dimension.name IS NOT NULL",
        ),
    ]


def test_reference_link_contract_records_attribute_and_rejects_unknown_links():
    link = DimensionReferenceLinkSpec(
        node_column="dimension_name",
        dimension="ns.dimension.name",
        role="owner",
    )

    assert _dimension_link_contract(link) == {
        "type": "reference",
        "role": "owner",
        "dimension_node": "ns.dimension",
        "dimension_attribute": "name",
        "node_column": "dimension_name",
    }
    with pytest.raises(TypeError, match="Unsupported dimension link: object"):
        _dimension_link_contract(object())


def test_node_contract_can_omit_columns_and_dimension_links():
    source = _specs()[0].model_copy(
        update={
            "dimension_links": [
                DimensionJoinLinkSpec(
                    dimension_node="ns.dimension",
                    join_on="ns.source.id = ns.dimension.id",
                ),
            ],
        },
    )

    contract = _node_contract(source, include_dimension_links=False)

    assert contract["table"] == "orders"
    assert "columns" not in contract
    assert "dimension_links" not in contract
