import pytest
from datajunction_server.database.queryrequest import (
    VersionedNodeKey,
    VersionedQueryKey,
)
from datajunction_server.database.node import Node


def test_str_with_version():
    key = VersionedNodeKey(name="some.node.name", version="v1")
    assert str(key) == "some.node.name@v1"


def test_str_without_version():
    key = VersionedNodeKey(name="some.node.name")
    assert str(key) == "some.node.name"


def test_eq_same_node_key():
    k1 = VersionedNodeKey(name="some.node.name", version="v1")
    k2 = VersionedNodeKey(name="some.node.name", version="v1")
    assert k1 == k2


def test_eq_different_node_key():
    k1 = VersionedNodeKey(name="some.node.name", version="v1")
    k2 = VersionedNodeKey(name="some.node.name", version="v2")
    assert k1 != k2


def test_eq_node_key_vs_str_with_version():
    k1 = VersionedNodeKey(name="some.node.name", version="v1")
    assert k1 == "some.node.name@v1"


def test_eq_node_key_vs_str_without_version():
    k1 = VersionedNodeKey(name="some.node.name")
    assert k1 == "some.node.name"


def test_eq_node_key_vs_str_mismatch():
    k1 = VersionedNodeKey(name="some.node.name", version="v1")
    assert k1 != "some.node.name@v2"
    assert k1 == "some.node.name@v1"
    assert k1 != "some.node.name"


def test_eq_with_non_string_non_node():
    k1 = VersionedNodeKey(name="some.node.name", version="v1")
    assert k1 != 123


def test_hash():
    k1 = VersionedNodeKey(name="some.node.name", version="v1")
    k2 = VersionedNodeKey(name="some.node.name", version="v1")
    k3 = VersionedNodeKey(name="some.node.name", version="v2")

    assert hash(k1) == hash(k2)
    assert hash(k1) != hash(k3)


def test_parse_with_version():
    parsed = VersionedNodeKey.parse("some.node.name@v1")
    assert parsed.name == "some.node.name"
    assert parsed.version == "v1"


def test_parse_without_version():
    parsed = VersionedNodeKey.parse("some.node.name")
    assert parsed.name == "some.node.name"
    assert parsed.version is None


def test_from_node():
    node = Node(name="some.node.name", current_version="v42")
    key = VersionedNodeKey.from_node(node)
    assert key.name == "some.node.name"
    assert key.version == "v42"


@pytest.mark.asyncio
async def test_versioning_nodes(module__session, module__client_with_roads):
    """
    Test versioning nodes and dimensions
    """
    versioned_nodes = await VersionedQueryKey.version_nodes(
        module__session,
        ["default.avg_repair_price", "default.num_repair_orders"],
    )
    expected_nodes = [
        VersionedNodeKey(name="default.avg_repair_price", version="v1.0"),
        VersionedNodeKey(name="default.num_repair_orders", version="v1.0"),
    ]
    assert versioned_nodes[0] == expected_nodes
    assert versioned_nodes[1] == [
        VersionedNodeKey(name="default.repair_orders_fact", version="v1.0"),
    ]
    versioned_nodes = await VersionedQueryKey.version_nodes(
        module__session,
        ["default.num_repair_orders", "default.avg_repair_price"],
    )
    assert versioned_nodes[0] == expected_nodes

    dimensions = ["default.dispatcher.company_name", "default.hard_hat.state"]
    versioned_dimensions = await VersionedQueryKey.version_dimensions(
        module__session,
        dimensions,
        current_node=versioned_nodes[0],
    )
    assert versioned_dimensions == [
        VersionedNodeKey(name="default.dispatcher.company_name", version="v1.0"),
        VersionedNodeKey(name="default.hard_hat.state", version="v1.0"),
    ]


@pytest.mark.asyncio
async def test_versioning_filters(module__session, module__client_with_roads):
    filters = ["default.hard_hat.state = 'CA'", "default.hard_hat.state = 'NY'"]
    versioned_filters = await VersionedQueryKey.version_filters(
        module__session,
        filters,
    )
    assert len(versioned_filters) == 2
    assert versioned_filters[0] == "default.hard_hat.state@v1.0 = 'CA'"
    assert versioned_filters[1] == "default.hard_hat.state@v1.0 = 'NY'"

    filters = [
        "default.hard_hat.state = 'CA' OR default.hard_hat.state = 'AB'",
        "default.hard_hat.state IN ('A', 'B')",
    ]
    versioned_filters = await VersionedQueryKey.version_filters(
        module__session,
        filters,
    )
    assert (
        versioned_filters[0]
        == "default.hard_hat.state@v1.0 = 'CA' OR default.hard_hat.state@v1.0 = 'AB'"
    )
    assert versioned_filters[1] == "default.hard_hat.state@v1.0 IN ('A', 'B')"


@pytest.mark.asyncio
async def test_versioning_orderby(module__session, module__client_with_roads):
    orderby = ["default.hard_hat.state DESC", "default.dispatcher.company_name ASC"]
    versioned_orderby = await VersionedQueryKey.version_orderby(
        module__session,
        orderby,
    )
    assert versioned_orderby[0] == "default.hard_hat.state@v1.0 DESC"
    assert versioned_orderby[1] == "default.dispatcher.company_name@v1.0 ASC"


@pytest.mark.asyncio
async def test_version_query_request(module__session, module__client_with_roads):
    versioned_query_request = await VersionedQueryKey.version_query_request(
        session=module__session,
        nodes=["default.avg_repair_price", "default.num_repair_orders"],
        dimensions=["default.dispatcher.company_name", "default.hard_hat.state"],
        filters=[
            "default.hard_hat.state = 'CA'",
            "default.hard_hat.state = 'NY'",
            "default.avg_repair_price > 20",
        ],
        orderby=[
            "default.avg_repair_price DESC",
            "default.dispatcher.company_name ASC",
        ],
    )
    assert versioned_query_request == VersionedQueryKey(
        nodes=[
            VersionedNodeKey(name="default.avg_repair_price", version="v1.0"),
            VersionedNodeKey(name="default.num_repair_orders", version="v1.0"),
        ],
        parents=[VersionedNodeKey(name="default.repair_orders_fact", version="v1.0")],
        dimensions=[
            VersionedNodeKey(name="default.dispatcher.company_name", version="v1.0"),
            VersionedNodeKey(name="default.hard_hat.state", version="v1.0"),
        ],
        filters=[
            "default.hard_hat.state@v1.0 = 'CA'",
            "default.hard_hat.state@v1.0 = 'NY'",  # dimension
            "default.avg_repair_price@v1.0 > 20",  # metric
        ],
        orderby=[
            "default.avg_repair_price@v1.0 DESC",  # metric
            "default.dispatcher.company_name@v1.0 ASC",  # dimension
        ],
    )


@pytest.mark.asyncio
async def test_version_query_request_missing_nodes(
    module__session,
    module__client_with_roads,
):
    versioned_query_request = await VersionedQueryKey.version_query_request(
        session=module__session,
        nodes=["default.avg_repair_price", "default.num_repair_orders"],
        dimensions=["default.dispatcher.company_name", "default.hard_hat.state"],
        filters=[
            "default.hard_hat.state = 'NY'",
            "default.bogus.bad > 10",
        ],
        orderby=[
            "default.avg_repair_price DESC",
        ],
    )
    assert versioned_query_request == VersionedQueryKey(
        nodes=[
            VersionedNodeKey(name="default.avg_repair_price", version="v1.0"),
            VersionedNodeKey(name="default.num_repair_orders", version="v1.0"),
        ],
        parents=[VersionedNodeKey(name="default.repair_orders_fact", version="v1.0")],
        dimensions=[
            VersionedNodeKey(name="default.dispatcher.company_name", version="v1.0"),
            VersionedNodeKey(name="default.hard_hat.state", version="v1.0"),
        ],
        filters=[
            "default.hard_hat.state@v1.0 = 'NY'",
            "default.bogus.bad > 10",
        ],
        orderby=[
            "default.avg_repair_price@v1.0 DESC",
        ],
    )


@pytest.mark.asyncio
async def test_version_query_request_filter_on_dim_role(
    module__session,
    module__client_with_roads,
):
    versioned_query_request = await VersionedQueryKey.version_query_request(
        session=module__session,
        nodes=["default.avg_repair_price", "default.num_repair_orders"],
        dimensions=["default.dispatcher.company_name", "default.hard_hat.state"],
        filters=[
            "default.hard_hat.state[stuff] = 'NY'",
        ],
        orderby=[],
    )
    assert versioned_query_request == VersionedQueryKey(
        nodes=[
            VersionedNodeKey(name="default.avg_repair_price", version="v1.0"),
            VersionedNodeKey(name="default.num_repair_orders", version="v1.0"),
        ],
        parents=[VersionedNodeKey(name="default.repair_orders_fact", version="v1.0")],
        dimensions=[
            VersionedNodeKey(name="default.dispatcher.company_name", version="v1.0"),
            VersionedNodeKey(name="default.hard_hat.state", version="v1.0"),
        ],
        filters=["default.hard_hat.state[stuff]@v1.0 = 'NY'"],
        orderby=[],
    )
