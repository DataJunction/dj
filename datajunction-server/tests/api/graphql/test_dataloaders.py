"""Tests for GraphQL DataLoaders."""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from datajunction_server.api.graphql.dataloaders import (
    InstrumentedDataLoader,
    batch_load_extracted_measures,
    batch_load_git_info,
    batch_load_nodes,
    batch_load_nodes_by_name_only,
    create_git_info_loader,
    create_node_by_name_loader,
)
from datajunction_server.instrumentation.provider import (
    MetricsProvider,
    get_metrics_provider,
    set_metrics_provider,
)


@pytest.mark.asyncio
@patch("datajunction_server.api.graphql.dataloaders.session_context")
@patch("datajunction_server.api.graphql.dataloaders.DBNode.find_by")
@patch("datajunction_server.api.graphql.dataloaders.load_node_options")
async def test_batch_load_nodes_with_fields(
    mock_load_options,
    mock_find_by,
    mock_session_context,
):
    """Test batch_load_nodes with specific fields requested"""
    # Setup mocks
    mock_session = AsyncMock()
    mock_session_context.return_value.__aenter__.return_value = mock_session

    mock_node1 = MagicMock()
    mock_node1.name = "default.node1"
    mock_node2 = MagicMock()
    mock_node2.name = "default.node2"

    mock_find_by.return_value = [mock_node1, mock_node2]
    mock_load_options.return_value = ["option1", "option2"]

    mock_request = MagicMock()

    # Test with multiple nodes and different field selections
    keys = [
        ("default.node1", {"name": None, "current": {"displayName": None}}),
        ("default.node2", {"name": None, "type": None}),
    ]

    result = await batch_load_nodes(keys, mock_request)

    # Verify all_fields was merged correctly
    expected_all_fields = {
        "name": None,
        "current": {"displayName": None},
        "type": None,
    }
    mock_load_options.assert_called_once_with(expected_all_fields)

    # Verify find_by was called with correct parameters
    mock_find_by.assert_called_once_with(
        mock_session,
        names=["default.node1", "default.node2"],
        options=["option1", "option2"],
    )

    # Verify results are in correct order
    assert len(result) == 2
    assert result[0] == mock_node1
    assert result[1] == mock_node2


@pytest.mark.asyncio
@patch("datajunction_server.api.graphql.dataloaders.session_context")
@patch("datajunction_server.api.graphql.dataloaders.DBNode.find_by")
@patch("datajunction_server.api.graphql.dataloaders.load_node_options")
async def test_batch_load_nodes_with_no_fields(
    mock_load_options,
    mock_find_by,
    mock_session_context,
):
    """Test batch_load_nodes with no fields (None) in all keys"""
    mock_session = AsyncMock()
    mock_session_context.return_value.__aenter__.return_value = mock_session

    mock_node1 = MagicMock()
    mock_node1.name = "default.node1"

    mock_find_by.return_value = [mock_node1]

    mock_request = MagicMock()

    # All keys have None for fields
    keys = [
        ("default.node1", None),
        ("default.node2", None),
    ]

    result = await batch_load_nodes(keys, mock_request)

    # Verify load_node_options was NOT called since all_fields is empty
    mock_load_options.assert_not_called()

    # Verify find_by was called with empty options
    mock_find_by.assert_called_once_with(
        mock_session,
        names=["default.node1", "default.node2"],
        options=[],
    )

    # Verify result contains the found node and None for the missing one
    assert len(result) == 2
    assert result[0] == mock_node1
    assert result[1] is None


@pytest.mark.asyncio
@patch("datajunction_server.api.graphql.dataloaders.session_context")
@patch("datajunction_server.api.graphql.dataloaders.DBNode.find_by")
@patch("datajunction_server.api.graphql.dataloaders.load_node_options")
async def test_batch_load_nodes_missing_nodes(
    mock_load_options,
    mock_find_by,
    mock_session_context,
):
    """Test batch_load_nodes returns None for missing nodes"""
    mock_session = AsyncMock()
    mock_session_context.return_value.__aenter__.return_value = mock_session

    # Only node1 exists in DB
    mock_node1 = MagicMock()
    mock_node1.name = "default.node1"

    mock_find_by.return_value = [mock_node1]
    mock_load_options.return_value = []

    mock_request = MagicMock()

    # Request 3 nodes but only 1 exists
    keys = [
        ("default.node1", {"name": None}),
        ("default.missing_node", {"name": None}),
        ("default.another_missing", {"name": None}),
    ]

    result = await batch_load_nodes(keys, mock_request)

    # Verify results are in correct order with None for missing nodes
    assert len(result) == 3
    assert result[0] == mock_node1
    assert result[1] is None
    assert result[2] is None


@pytest.mark.asyncio
@patch("datajunction_server.api.graphql.dataloaders.session_context")
@patch("datajunction_server.api.graphql.dataloaders.DBNode.find_by")
@patch("datajunction_server.api.graphql.dataloaders.load_node_options")
async def test_batch_load_nodes_ordering_preservation(
    mock_load_options,
    mock_find_by,
    mock_session_context,
):
    """Test batch_load_nodes preserves the order of requested keys"""
    mock_session = AsyncMock()
    mock_session_context.return_value.__aenter__.return_value = mock_session

    mock_node1 = MagicMock()
    mock_node1.name = "default.aaa"
    mock_node2 = MagicMock()
    mock_node2.name = "default.zzz"
    mock_node3 = MagicMock()
    mock_node3.name = "default.mmm"

    # Return nodes in different order than requested
    mock_find_by.return_value = [mock_node2, mock_node1, mock_node3]
    mock_load_options.return_value = []

    mock_request = MagicMock()

    # Request in specific order
    keys = [
        ("default.zzz", {"name": None}),
        ("default.aaa", {"name": None}),
        ("default.mmm", {"name": None}),
    ]

    result = await batch_load_nodes(keys, mock_request)

    # Verify results match the requested order (not DB return order)
    assert len(result) == 3
    assert result[0].name == "default.zzz"
    assert result[1].name == "default.aaa"
    assert result[2].name == "default.mmm"


@pytest.mark.asyncio
@patch("datajunction_server.api.graphql.dataloaders.session_context")
@patch("datajunction_server.api.graphql.dataloaders.DBNode.find_by")
@patch("datajunction_server.api.graphql.dataloaders.load_node_options")
async def test_batch_load_nodes_by_name_only(
    mock_load_options,
    mock_find_by,
    mock_session_context,
):
    """Test batch_load_nodes_by_name_only with default fields"""
    mock_session = AsyncMock()
    mock_session_context.return_value.__aenter__.return_value = mock_session

    mock_node1 = MagicMock()
    mock_node1.name = "default.node1"
    mock_node2 = MagicMock()
    mock_node2.name = "default.node2"

    mock_find_by.return_value = [mock_node1, mock_node2]
    mock_load_options.return_value = ["default_options"]

    mock_request = MagicMock()

    names = ["default.node1", "default.node2"]

    result = await batch_load_nodes_by_name_only(names, mock_request)

    # Verify load_node_options was called with default fields
    mock_load_options.assert_called_once_with({"name": None, "current": {"name": None}})

    # Verify find_by was called correctly
    mock_find_by.assert_called_once_with(
        mock_session,
        names=names,
        options=["default_options"],
    )

    # Verify results
    assert len(result) == 2
    assert result[0] == mock_node1
    assert result[1] == mock_node2


@pytest.mark.asyncio
@patch("datajunction_server.api.graphql.dataloaders.session_context")
@patch("datajunction_server.api.graphql.dataloaders.DBNode.find_by")
@patch("datajunction_server.api.graphql.dataloaders.load_node_options")
async def test_batch_load_nodes_by_name_only_with_missing(
    mock_load_options,
    mock_find_by,
    mock_session_context,
):
    """Test batch_load_nodes_by_name_only returns None for missing nodes"""
    mock_session = AsyncMock()
    mock_session_context.return_value.__aenter__.return_value = mock_session

    mock_node1 = MagicMock()
    mock_node1.name = "default.node1"

    mock_find_by.return_value = [mock_node1]
    mock_load_options.return_value = []

    mock_request = MagicMock()

    names = ["default.node1", "default.missing", "default.node3"]

    result = await batch_load_nodes_by_name_only(names, mock_request)

    # Verify results with None for missing
    assert len(result) == 3
    assert result[0] == mock_node1
    assert result[1] is None
    assert result[2] is None


def test_create_node_by_name_loader():
    """create_node_by_name_loader returns an InstrumentedDataLoader named 'node_by_name'."""
    from datajunction_server.api.graphql.dataloaders import InstrumentedDataLoader

    mock_request = MagicMock()
    result = create_node_by_name_loader(mock_request)

    assert isinstance(result, InstrumentedDataLoader)
    assert result._loader_name == "node_by_name"


@pytest.mark.asyncio
@patch("datajunction_server.api.graphql.dataloaders.session_context")
async def test_batch_load_git_info_with_fk_hop(mock_session_context):
    """Test batch_load_git_info when a branch namespace has a parent_namespace
    outside the string hierarchy, triggering the FK hop query."""
    mock_session = AsyncMock()
    mock_session_context.return_value.__aenter__.return_value = mock_session

    # Create namespace mocks:
    # "project" is the git root (has github_repo_path)
    # "project.feature" is the branch namespace (has git_branch, parent_namespace
    #   points to "other_root" which is outside the string ancestors)
    # "other_root" is the FK hop target (has github_repo_path)
    ns_project = MagicMock()
    ns_project.namespace = "project"
    ns_project.git_branch = None
    ns_project.github_repo_path = None
    ns_project.parent_namespace = None

    ns_feature = MagicMock()
    ns_feature.namespace = "project.feature"
    ns_feature.git_branch = "feature-branch"
    ns_feature.parent_namespace = "other_root"  # outside string hierarchy
    ns_feature.github_repo_path = None

    ns_other_root = MagicMock()
    ns_other_root.namespace = "other_root"
    ns_other_root.github_repo_path = "owner/repo"
    ns_other_root.git_branch = None
    ns_other_root.default_branch = "main"
    ns_other_root.git_path = "/path"
    ns_other_root.git_only = False
    ns_other_root.parent_namespace = None

    # First query returns project + project.feature (not other_root)
    # Second query (FK hop) returns other_root
    mock_result_1 = MagicMock()
    mock_result_1.scalars.return_value.all.return_value = [ns_project, ns_feature]
    mock_result_2 = MagicMock()
    mock_result_2.scalars.return_value.all.return_value = [ns_other_root]
    mock_session.execute.side_effect = [mock_result_1, mock_result_2]

    mock_request = MagicMock()
    result = await batch_load_git_info(
        ["project.feature.cubes"],
        mock_request,
    )

    # Two queries should have been executed (ancestors + FK hop)
    assert mock_session.execute.call_count == 2
    assert len(result) == 1
    assert result[0] is not None
    assert result[0]["repo"] == "owner/repo"
    assert result[0]["branch"] == "feature-branch"


@pytest.mark.asyncio
@patch("datajunction_server.api.graphql.dataloaders.session_context")
async def test_batch_load_git_info_no_fk_hop(mock_session_context):
    """Test batch_load_git_info when no FK hop is needed (single query)."""
    mock_session = AsyncMock()
    mock_session_context.return_value.__aenter__.return_value = mock_session

    ns_root = MagicMock()
    ns_root.namespace = "project"
    ns_root.git_branch = "main"
    ns_root.github_repo_path = "owner/repo"
    ns_root.parent_namespace = None
    ns_root.default_branch = "main"
    ns_root.git_path = "/"
    ns_root.git_only = False

    mock_result = MagicMock()
    mock_result.scalars.return_value.all.return_value = [ns_root]
    mock_session.execute.return_value = mock_result

    mock_request = MagicMock()
    result = await batch_load_git_info(["project"], mock_request)

    # Only one query — no FK hop needed
    assert mock_session.execute.call_count == 1
    assert len(result) == 1
    assert result[0]["repo"] == "owner/repo"


@pytest.mark.asyncio
@patch("datajunction_server.api.graphql.dataloaders.session_context")
async def test_batch_load_git_info_no_git_branch(mock_session_context):
    """Test batch_load_git_info when no namespace has git_branch set."""
    mock_session = AsyncMock()
    mock_session_context.return_value.__aenter__.return_value = mock_session

    ns_root = MagicMock()
    ns_root.namespace = "project"
    ns_root.git_branch = None
    ns_root.github_repo_path = None
    ns_root.parent_namespace = None

    mock_result = MagicMock()
    mock_result.scalars.return_value.all.return_value = [ns_root]
    mock_session.execute.return_value = mock_result

    mock_request = MagicMock()
    result = await batch_load_git_info(["project"], mock_request)

    # Only one query — no branch found, no FK hop
    assert mock_session.execute.call_count == 1
    assert len(result) == 1
    assert result[0] is None


def test_create_git_info_loader():
    """create_git_info_loader returns an InstrumentedDataLoader named 'git_info'."""
    from datajunction_server.api.graphql.dataloaders import InstrumentedDataLoader

    mock_request = MagicMock()
    result = create_git_info_loader(mock_request)

    assert isinstance(result, InstrumentedDataLoader)
    assert result._loader_name == "git_info"


@pytest.mark.asyncio
@patch("datajunction_server.api.graphql.dataloaders.find_upstream_node_names")
@patch("datajunction_server.api.graphql.dataloaders.session_context")
async def test_batch_load_extracted_measures_missing_ids(
    mock_session_context,
    mock_find_upstream,
):
    """Batch loader returns None for nr_ids that don't resolve to a metric node.

    Exercises the `if all_names:` False branch (no ancestor set, so the bulk
    Node load is skipped) and the `metric_node is None` early-continue that
    appends None to the results — both defensive paths for nr_ids that
    vanished between GraphQL resolution and the batch query.
    """
    mock_session = AsyncMock()
    mock_session_context.return_value.__aenter__.return_value = mock_session

    # nr_stmt returns no rows -> nr_to_name is empty
    nr_result = MagicMock()
    nr_result.all.return_value = []
    mock_session.execute.return_value = nr_result

    # find_upstream_node_names returns empty when called with []
    mock_find_upstream.return_value = (set(), {})

    result = await batch_load_extracted_measures([999999], MagicMock())

    assert result == [None]
    # Only the nr_stmt query ran; the bulk Node load was skipped.
    assert mock_session.execute.call_count == 1


# ---------------------------------------------------------------------------
# InstrumentedDataLoader: hit/miss + batch metrics
# ---------------------------------------------------------------------------


class _SpyProvider(MetricsProvider):
    def __init__(self) -> None:
        self.counters: list[tuple] = []

    def counter(self, name, value=1, tags=None):
        self.counters.append((name, value, tags))

    def gauge(self, name, value, tags=None):  # pragma: no cover
        pass

    def timer(self, name, value_ms, tags=None):  # pragma: no cover
        pass


@pytest.fixture
def spy_metrics():
    original = get_metrics_provider()
    spy = _SpyProvider()
    set_metrics_provider(spy)
    yield spy
    set_metrics_provider(original)


@pytest.mark.asyncio
async def test_instrumented_dataloader_misses_then_hit(spy_metrics):
    """First load is a miss + batch fetch; second load of the same key is a cache hit."""

    async def load_fn(keys):
        return [f"v:{k}" for k in keys]

    loader = InstrumentedDataLoader("test_loader", load_fn=load_fn)

    assert await loader.load("a") == "v:a"
    # Strawberry caches resolved futures, so a second load is a hit.
    assert await loader.load("a") == "v:a"

    miss_counts = [
        c for c in spy_metrics.counters if c[0] == "dj.graphql.dataloader.misses"
    ]
    hit_counts = [
        c for c in spy_metrics.counters if c[0] == "dj.graphql.dataloader.hits"
    ]
    batch_counts = [
        c for c in spy_metrics.counters if c[0] == "dj.graphql.dataloader.batches"
    ]
    item_counts = [
        c for c in spy_metrics.counters if c[0] == "dj.graphql.dataloader.items"
    ]

    assert miss_counts == [
        ("dj.graphql.dataloader.misses", 1, {"loader": "test_loader"}),
    ]
    assert hit_counts == [
        ("dj.graphql.dataloader.hits", 1, {"loader": "test_loader"}),
    ]
    assert batch_counts == [
        ("dj.graphql.dataloader.batches", 1, {"loader": "test_loader"}),
    ]
    assert item_counts == [
        ("dj.graphql.dataloader.items", 1, {"loader": "test_loader"}),
    ]


@pytest.mark.asyncio
async def test_instrumented_dataloader_batches_distinct_keys(spy_metrics):
    """Concurrent loads of distinct keys are batched into a single fetch sized by len(keys)."""
    import asyncio

    async def load_fn(keys):
        return [f"v:{k}" for k in keys]

    loader = InstrumentedDataLoader("batched", load_fn=load_fn)

    results = await asyncio.gather(
        loader.load("a"),
        loader.load("b"),
        loader.load("c"),
    )
    assert results == ["v:a", "v:b", "v:c"]

    item_counts = [
        c for c in spy_metrics.counters if c[0] == "dj.graphql.dataloader.items"
    ]
    batch_counts = [
        c for c in spy_metrics.counters if c[0] == "dj.graphql.dataloader.batches"
    ]
    assert batch_counts == [
        ("dj.graphql.dataloader.batches", 1, {"loader": "batched"}),
    ]
    assert item_counts == [
        ("dj.graphql.dataloader.items", 3, {"loader": "batched"}),
    ]
