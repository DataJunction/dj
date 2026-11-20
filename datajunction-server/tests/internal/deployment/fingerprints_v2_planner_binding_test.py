"""SQL ancestry alone cannot establish a configured dimension binding."""

import asyncio

from datajunction_server.models.deployment import (
    ColumnSpec,
    CubeSpec,
    DimensionJoinLinkSpec,
    DimensionSpec,
    MetricSpec,
    SourceSpec,
    TransformSpec,
)
from datajunction_server.semantic_fingerprints.v2 import (
    SemanticGraphSnapshot,
    compare_components,
)
from datajunction_server.semantic_fingerprints.v2_configured import (
    compute_configured_v2_fingerprints,
)


def _evaluate(key="manager_id", dimension="ns.manager.name"):
    employee = SourceSpec(
        name="ns.employee",
        catalog="warehouse",
        schema_="payroll",
        table="employee",
        columns=[
            ColumnSpec(name="employee_id", type="bigint"),
            ColumnSpec(name="manager_id", type="bigint"),
            ColumnSpec(name="salary", type="double"),
        ],
        dimension_links=[
            DimensionJoinLinkSpec(
                dimension_node="ns.manager",
                role="manager",
                join_on=f"ns.employee.{key} = ns.manager.employee_id",
            ),
        ],
    )
    manager_rows = SourceSpec(
        name="ns.manager_rows",
        catalog="warehouse",
        schema_="payroll",
        table="manager",
        columns=[
            ColumnSpec(name="employee_id", type="bigint"),
            ColumnSpec(name="name", type="string"),
        ],
    )
    manager = DimensionSpec(
        name="ns.manager",
        query="SELECT employee_id, name FROM ns.manager_rows",
        columns=manager_rows.columns,
        primary_key=["employee_id"],
    )
    payroll = TransformSpec(
        name="ns.payroll",
        query=(
            "SELECT e.employee_id, e.salary FROM ns.employee e "
            "LEFT JOIN ns.manager m ON e.employee_id = m.employee_id"
        ),
        columns=[employee.columns[0], employee.columns[2]],
        primary_key=["employee_id"],
    )
    total = MetricSpec(
        name="ns.total",
        query="SELECT SUM(salary) FROM ns.payroll",
        required_dimensions=[dimension],
    )
    cube = CubeSpec(name="ns.cube", metrics=["ns.total"], dimensions=[dimension])
    snapshot = SemanticGraphSnapshot(
        {
            node.name: node
            for node in (employee, manager_rows, manager, payroll, total, cube)
        },
    )
    return asyncio.run(compute_configured_v2_fingerprints(snapshot))


def test_unresolved_ancestor_grouping_is_indeterminate():
    before = _evaluate()
    after = _evaluate(key="employee_id")

    for name in ("ns.total", "ns.cube"):
        assert before.nodes[name].full.digest is None
        assert "planner_dimension_binding_unavailable" in before.nodes[name].reasons
        assert compare_components(before.nodes[name].full, after.nodes[name].full) == (
            "indeterminate"
        )


def test_role_bound_grouping_retains_the_join_key():
    before = _evaluate(dimension="ns.manager.name[manager]")
    after = _evaluate(key="employee_id", dimension="ns.manager.name[manager]")

    for name in ("ns.total", "ns.cube"):
        assert before.nodes[name].full.digest
        assert after.nodes[name].full.digest
        assert compare_components(before.nodes[name].full, after.nodes[name].full) == (
            "changed"
        )


def test_parent_local_grouping_ignores_an_unused_link():
    before = _evaluate(dimension="ns.payroll.employee_id")
    after = _evaluate(key="employee_id", dimension="ns.payroll.employee_id")

    for name in ("ns.total", "ns.cube"):
        assert before.nodes[name].full.digest
        assert compare_components(before.nodes[name].full, after.nodes[name].full) == (
            "unchanged"
        )
