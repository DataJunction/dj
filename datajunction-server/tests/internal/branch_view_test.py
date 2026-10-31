"""
Tests that a default branch node, seen through a branch, matches a real copy.
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import AsyncClient
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.database.node import Node
from datajunction_server.internal.branch_view import branch_view
from datajunction_server.models.filter_requirements import FilterRequirement
from datajunction_server.models.node import NodeOutput, NodeRevisionOutput
from datajunction_server.models.node_type import NodeNameOutput, NodeType
from datajunction_server.models.reaggregate import (
    DimensionReaggregateRule,
    ReaggregateSpec,
    ReaggregationFunction,
)

# Values a copy gets fresh or does not carry over.
FRESH = {
    "id",
    "node_id",
    "version",
    "current_version",
    "created_at",
    "updated_at",
    "created_by",
    "availability",
    "git_info",
}


async def make_branch(client: AsyncClient) -> None:
    """
    Branches `foo.bar` into `foo.feature_x`, copying its nodes.
    """
    await client.patch(
        "/namespaces/foo.bar/git",
        json={"github_repo_path": "myorg/myrepo", "git_branch": "main"},
    )
    with patch("datajunction_server.api.branches.GitHubService") as github_class:
        github = MagicMock()
        github.create_branch = AsyncMock(
            return_value={"ref": "refs/heads/feature-x", "object": {"sha": "abc"}},
        )
        github_class.return_value = github
        response = await client.post(
            "/namespaces/foo.bar/branches",
            json={"branch_name": "feature-x"},
        )
    assert response.status_code == 201


async def load_node(session: AsyncSession, name: str) -> Node:
    """
    Loads a node with everything its outputs need.
    """
    return (
        (
            await session.execute(
                select(Node)
                .where(Node.name == name)
                .options(*NodeOutput.load_options()),
            )
        )
        .unique()
        .scalar_one()
    )


async def load_output(session: AsyncSession, name: str) -> NodeOutput:
    """
    Loads a node the way the node endpoint does.
    """
    return NodeOutput.model_validate(await load_node(session, name))


def comparable(output: NodeOutput | NodeRevisionOutput) -> dict:
    """
    The fields a copy is expected to match.
    """
    return {
        key: value
        for key, value in output.model_dump(mode="json").items()
        if key not in FRESH
    }


@pytest.mark.asyncio
async def test_branch_view_matches_copy(
    client_with_namespaced_roads: AsyncClient,
    session: AsyncSession,
):
    """
    Every node in `foo.bar`, viewed as `foo.feature_x`, equals its real copy.
    """
    await make_branch(client_with_namespaced_roads)
    names = (
        (
            await session.execute(
                select(Node.name)
                .where(Node.name.like("foo.bar.%"), Node.deactivated_at.is_(None))
                .order_by(Node.name),
            )
        )
        .scalars()
        .all()
    )
    assert names
    views = {}
    copies = {}
    for name in names:
        copy_name = name.replace("foo.bar.", "foo.feature_x.")
        main_node = await load_node(session, name)
        copied_node = await load_node(session, copy_name)
        for kind, model, main_source, copied_source in (
            ("node", NodeOutput, main_node, copied_node),
            ("revision", NodeRevisionOutput, main_node.current, copied_node.current),
        ):
            views[kind, name] = comparable(
                branch_view(
                    model.model_validate(main_source),
                    "foo.bar",
                    "foo.feature_x",
                ),
            )
            copies[kind, name] = comparable(model.model_validate(copied_source))
    differing = {
        key: {
            field: repr((views[key][field], copies[key][field]))[:600]
            for field in views[key]
            if views[key][field] != copies[key][field]
        }
        for key in views
    }
    assert {key: diff for key, diff in differing.items() if diff} == {}


@pytest.mark.asyncio
async def test_branch_view_metric_fields(
    client_with_namespaced_roads: AsyncClient,
    session: AsyncSession,
):
    """
    Metric grain, filters and reaggregation rules follow the namespace.
    """
    metric = await load_output(session, "foo.bar.num_repair_orders")
    assert metric.type == NodeType.METRIC
    metric.fixed_grain = ["foo.bar.hard_hat.state", "other.dim.col"]
    metric.required_filters = [
        FilterRequirement(dimension=("foo.bar.hard_hat.state", "other.dim.col")),
    ]
    metric.recommended_filters = [
        FilterRequirement(
            dimension=("foo.bar.hard_hat.city",),
            default="foo.bar.hard_hat.city = 'foo.bar.x'",
        ),
    ]
    metric.reaggregate = ReaggregateSpec(
        rules=[
            DimensionReaggregateRule(
                dimension="foo.bar.hard_hat.state",
                fn=ReaggregationFunction.SUM,
            ),
        ],
    )
    view = branch_view(metric, "foo.bar", "foo.feature_x")
    assert view.fixed_grain == ["foo.feature_x.hard_hat.state", "other.dim.col"]
    assert view.required_filters == [
        FilterRequirement(
            dimension=("foo.feature_x.hard_hat.state", "other.dim.col"),
        ),
    ]
    assert view.recommended_filters == [
        FilterRequirement(
            dimension=("foo.feature_x.hard_hat.city",),
            default="foo.feature_x.hard_hat.city = 'foo.bar.x'",
        ),
    ]
    assert view.reaggregate == ReaggregateSpec(
        rules=[
            DimensionReaggregateRule(
                dimension="foo.feature_x.hard_hat.state",
                fn=ReaggregationFunction.SUM,
            ),
        ],
    )
    assert metric.fixed_grain == ["foo.bar.hard_hat.state", "other.dim.col"]


@pytest.mark.asyncio
async def test_branch_view_cube_fields(
    client_with_namespaced_roads: AsyncClient,
    session: AsyncSession,
):
    """
    Cube columns and filters follow the namespace, and the input is unchanged.
    """
    cube = await load_output(session, "foo.bar.num_repair_orders")
    cube.type = NodeType.CUBE
    cube.query = None
    cube.cube_filters = ["foo.bar.hard_hat.state = 'CA'"]
    cube.columns[0].name = "foo.bar.num_repair_orders"
    view = branch_view(cube, "foo.bar", "foo.feature_x")
    assert view.cube_filters == ["foo.feature_x.hard_hat.state = 'CA'"]
    assert view.columns[0].name == "foo.feature_x.num_repair_orders"
    assert cube.cube_filters == ["foo.bar.hard_hat.state = 'CA'"]
    assert cube.columns[0].name == "foo.bar.num_repair_orders"


@pytest.mark.asyncio
async def test_branch_view_leaves_other_namespaces(
    client_with_namespaced_roads: AsyncClient,
    session: AsyncSession,
):
    """
    A node outside the source namespace keeps its names.
    """
    node = await load_output(session, "foo.bar.repair_orders")
    view = branch_view(node, "other.ns", "foo.feature_x")
    assert view.model_dump() == node.model_dump()


@pytest.mark.asyncio
async def test_branch_view_column_dimension_and_empty_query(
    client_with_namespaced_roads: AsyncClient,
    session: AsyncSession,
):
    """
    A column's linked dimension is renamed, and a missing query stays missing.
    """
    node = await load_output(session, "foo.bar.num_repair_orders")
    node.query = None
    node.columns[0].dimension = NodeNameOutput(name="foo.bar.hard_hat")
    view = branch_view(node, "foo.bar", "foo.feature_x")
    assert view.query is None
    assert view.columns[0].dimension == NodeNameOutput(name="foo.feature_x.hard_hat")
