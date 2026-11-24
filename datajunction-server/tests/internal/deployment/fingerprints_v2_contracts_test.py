"""The v2 local contracts omit presentation metadata and retain data inputs."""

import pytest

from datajunction_server.models.deployment import (
    ColumnSpec,
    CubeSpec,
    DimensionJoinLinkSpec,
    DimensionSpec,
    MetricSpec,
    SourceSpec,
    TransformSpec,
)
from datajunction_server.semantic_fingerprints.v2_dependencies import _node_contract


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
                    update={"join_on": "ns.source.id + 1 = ns.dimension.id"},
                ),
            ],
        },
    )

    assert _contract(source) == _contract(changed_cardinality)
    assert _contract(source) != _contract(changed_predicate)


@pytest.mark.parametrize(
    ("query", "equivalent"),
    [
        ("SELECT SUM(id) FROM ns.source", True),
        ("SELECT SUM(s.id) FROM ns.source s", True),
        ("SELECT SUM(s.id) FROM ns.source s JOIN ns.other o ON s.id = o.id", False),
        ("SELECT SUM(s.id) FROM ns.source s JOIN ns.source t ON s.id = t.id", False),
        ("WITH t AS (SELECT id FROM ns.source) SELECT SUM(id) FROM t", False),
        ("SELECT SUM(id) FROM (SELECT id FROM ns.source) t", False),
        ("SELECT ns.source", False),
        ("SELECT (", False),
        ("", False),
    ],
)
def test_required_dimensions_normalize_only_single_input_references(query, equivalent):
    metric = MetricSpec(name="ns.metric", query=query, required_dimensions=["id", "z"])
    qualified = metric.model_copy(
        update={"required_dimensions": ["z", "ns.source.id", "ns.source.id"]},
    )

    assert (_contract(metric) == _contract(qualified)) is equivalent
    assert _contract(qualified)["required_dimensions"] == [
        "id" if equivalent else "ns.source.id",
        "z",
    ]


def test_required_dimensions_preserve_join_providers_and_roles():
    metric = MetricSpec(
        name="ns.metric",
        query=(
            "SELECT SUM(o.amount) FROM ns.orders o "
            "JOIN ns.customers c ON o.customer_id = c.id"
        ),
        required_dimensions=["ns.orders.id"],
    )
    customer = metric.model_copy(update={"required_dimensions": ["ns.customers.id"]})
    assert _contract(metric) != _contract(customer)

    metric = _specs()[3]
    employee = metric.model_copy(
        update={"required_dimensions": ["ns.source.id[employee]"]},
    )
    manager = metric.model_copy(
        update={"required_dimensions": ["ns.source.id[manager]"]},
    )
    assert _contract(employee)["required_dimensions"] == ["ns.source.id[employee]"]
    assert _contract(employee) != _contract(manager)


@pytest.mark.parametrize(
    ("before", "after", "equivalent"),
    [
        (["b", "a", "a"], ["a", "b"], True),
        (None, [], False),
        (["a"], ["b"], False),
    ],
)
def test_fixed_grain_ignores_order_and_duplicates_but_retains_meaning(
    before,
    after,
    equivalent,
):
    metric = _specs()[3].model_copy(update={"fixed_grain": before})
    changed = metric.model_copy(update={"fixed_grain": after})
    assert (_contract(metric) == _contract(changed)) is equivalent
