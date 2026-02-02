"""
Tests for source table metadata API endpoints.
"""

from http import HTTPStatus
import pytest
import pytest_asyncio
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.database.node import Node, NodeRevision
from datajunction_server.database.user import User
from datajunction_server.models.node_type import NodeType


@pytest_asyncio.fixture
async def source_node(session: AsyncSession, current_user: User) -> Node:
    """
    Create a test source node.
    """
    node = Node(
        name="test.source_metadata_table",
        type=NodeType.SOURCE,
        current_version="1",
        created_by_id=current_user.id,
    )
    node_rev = NodeRevision(
        node=node,
        version="1",
        name=node.name,
        type=node.type,
        created_by_id=current_user.id,
    )
    session.add(node_rev)
    await session.commit()
    await session.refresh(node)
    return node


@pytest_asyncio.fixture
async def transform_node(session: AsyncSession, current_user: User) -> Node:
    """
    Create a test transform node (non-source).
    """
    node = Node(
        name="test.transform_node",
        type=NodeType.TRANSFORM,
        current_version="1",
        created_by_id=current_user.id,
    )
    node_rev = NodeRevision(
        node=node,
        version="1",
        name=node.name,
        type=node.type,
        query="SELECT 1 as col",
        created_by_id=current_user.id,
    )
    session.add(node_rev)
    await session.commit()
    await session.refresh(node)
    return node


@pytest.mark.asyncio
class TestSetSourceTableMetadata:
    """
    Tests for POST /sources/{name}/table-metadata endpoint.
    """

    async def test_create_table_metadata(
        self,
        client: AsyncClient,
        source_node: Node,
    ):
        """
        Test successfully creating table metadata for a source node.
        """
        metadata_input = {
            "total_size_bytes": 1000000,
            "total_row_count": 50000,
            "total_partitions": 30,
            "earliest_partition_value": "20240101",
            "latest_partition_value": "20240130",
            "freshness_timestamp": 1706745600,
            "ttl_days": 365,
        }

        response = await client.post(
            f"/sources/{source_node.name}/table-metadata",
            json=metadata_input,
        )
        assert response.status_code == 200

        data = response.json()
        assert data["total_size_bytes"] == 1000000
        assert data["total_row_count"] == 50000
        assert data["total_partitions"] == 30
        assert data["earliest_partition_value"] == "20240101"
        assert data["latest_partition_value"] == "20240130"
        assert data["freshness_timestamp"] == 1706745600
        assert data["ttl_days"] == 365
        assert "updated_at" in data

    async def test_update_existing_table_metadata(
        self,
        client: AsyncClient,
        source_node: Node,
    ):
        """
        Test successfully updating (upserting) existing table metadata.
        """
        # Create initial metadata
        initial_metadata = {
            "total_size_bytes": 1000000,
            "total_row_count": 50000,
            "total_partitions": 30,
            "earliest_partition_value": "20240101",
            "latest_partition_value": "20240130",
        }

        response = await client.post(
            f"/sources/{source_node.name}/table-metadata",
            json=initial_metadata,
        )
        assert response.status_code == 200
        # initial_data = response.json()

        # Update with new values
        updated_metadata = {
            "total_size_bytes": 2000000,
            "total_row_count": 100000,
            "total_partitions": 60,
            "earliest_partition_value": "20240101",
            "latest_partition_value": "20240229",
            "freshness_timestamp": 1709251200,
            "ttl_days": 730,
        }

        response = await client.post(
            f"/sources/{source_node.name}/table-metadata",
            json=updated_metadata,
        )
        assert response.status_code == 200

        data = response.json()
        assert data["total_size_bytes"] == 2000000
        assert data["total_row_count"] == 100000
        assert data["total_partitions"] == 60
        assert data["freshness_timestamp"] == 1709251200
        assert data["ttl_days"] == 730

    async def test_create_with_optional_fields_none(
        self,
        client: AsyncClient,
        source_node: Node,
    ):
        """
        Test creating metadata with optional fields omitted or null.
        """
        metadata_input = {
            "total_size_bytes": 1000000,
            "total_row_count": 50000,
            "total_partitions": 30,
            "earliest_partition_value": "20240101",
            "latest_partition_value": "20240130",
        }

        response = await client.post(
            f"/sources/{source_node.name}/table-metadata",
            json=metadata_input,
        )
        assert response.status_code == 200

        data = response.json()
        assert data["freshness_timestamp"] is None
        assert data["ttl_days"] is None

    async def test_node_not_found(
        self,
        client: AsyncClient,
    ):
        """
        Test error when node doesn't exist.
        """
        metadata_input = {
            "total_size_bytes": 1000000,
            "total_row_count": 50000,
            "total_partitions": 30,
        }

        response = await client.post(
            "/sources/nonexistent.node/table-metadata",
            json=metadata_input,
        )
        assert response.status_code == 404
        data = response.json()
        assert "does not exist" in data["message"]

    async def test_node_is_not_source_type(
        self,
        client: AsyncClient,
        transform_node: Node,
    ):
        """
        Test error when node is not a SOURCE type.
        """
        metadata_input = {
            "total_size_bytes": 1000000,
            "total_row_count": 50000,
            "total_partitions": 30,
        }

        response = await client.post(
            f"/sources/{transform_node.name}/table-metadata",
            json=metadata_input,
        )
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        data = response.json()
        assert "Only source nodes can have table metadata" in data["message"]
        assert "transform" in data["message"].lower()

    async def test_validation_negative_values(
        self,
        client: AsyncClient,
        source_node: Node,
    ):
        """
        Test validation rejects negative values.
        """
        # Negative total_size_bytes
        response = await client.post(
            f"/sources/{source_node.name}/table-metadata",
            json={
                "total_size_bytes": -1000,
                "total_row_count": 50000,
                "total_partitions": 30,
            },
        )
        assert response.status_code == 422

        # Negative total_row_count
        response = await client.post(
            f"/sources/{source_node.name}/table-metadata",
            json={
                "total_size_bytes": 1000000,
                "total_row_count": -50000,
                "total_partitions": 30,
            },
        )
        assert response.status_code == 422

        # Negative ttl_days
        response = await client.post(
            f"/sources/{source_node.name}/table-metadata",
            json={
                "total_size_bytes": 1000000,
                "total_row_count": 50000,
                "total_partitions": 30,
                "ttl_days": -100,
            },
        )
        assert response.status_code == 422

    async def test_validation_missing_required_fields(
        self,
        client: AsyncClient,
        source_node: Node,
    ):
        """
        Test validation rejects missing required fields.
        """
        # Missing total_size_bytes
        response = await client.post(
            f"/sources/{source_node.name}/table-metadata",
            json={
                "total_row_count": 50000,
                "total_partitions": 30,
            },
        )
        assert response.status_code == 422


@pytest.mark.asyncio
class TestGetSourceTableMetadata:
    """
    Tests for GET /sources/{name}/table-metadata endpoint.
    """

    async def test_get_existing_metadata(
        self,
        client: AsyncClient,
        source_node: Node,
    ):
        """
        Test successfully retrieving table metadata.
        """
        # Create metadata first
        metadata_input = {
            "total_size_bytes": 1000000,
            "total_row_count": 50000,
            "total_partitions": 30,
            "earliest_partition_value": "20240101",
            "latest_partition_value": "20240130",
            "freshness_timestamp": 1706745600,
            "ttl_days": 365,
        }

        await client.post(
            f"/sources/{source_node.name}/table-metadata",
            json=metadata_input,
        )

        # Retrieve it
        response = await client.get(
            f"/sources/{source_node.name}/table-metadata",
        )
        assert response.status_code == 200

        data = response.json()
        assert data["total_size_bytes"] == 1000000
        assert data["total_row_count"] == 50000
        assert data["total_partitions"] == 30
        assert data["earliest_partition_value"] == "20240101"
        assert data["latest_partition_value"] == "20240130"
        assert data["freshness_timestamp"] == 1706745600
        assert data["ttl_days"] == 365

    async def test_node_not_found(
        self,
        client: AsyncClient,
    ):
        """
        Test error when node doesn't exist.
        """
        response = await client.get(
            "/sources/nonexistent.node/table-metadata",
        )
        assert response.status_code == 404
        data = response.json()
        assert "does not exist" in data["message"]

    async def test_no_metadata_exists(
        self,
        client: AsyncClient,
        source_node: Node,
    ):
        """
        Test error when node has no metadata yet.
        """
        response = await client.get(
            f"/sources/{source_node.name}/table-metadata",
        )
        assert response.status_code == 404
        data = response.json()
        assert "No table metadata found" in data["message"]


@pytest.mark.asyncio
class TestUpsertPartitionMetadata:
    """
    Tests for POST /sources/{name}/partition-metadata endpoint.
    """

    @pytest_asyncio.fixture
    async def source_node_with_table_metadata(
        self,
        client: AsyncClient,
        source_node: Node,
    ) -> Node:
        """Create source node with table metadata."""
        metadata_input = {
            "total_size_bytes": 1000000,
            "total_row_count": 50000,
            "total_partitions": 30,
            "earliest_partition_value": "20240101",
            "latest_partition_value": "20240130",
        }
        await client.post(
            f"/sources/{source_node.name}/table-metadata",
            json=metadata_input,
        )
        return source_node

    async def test_insert_new_partitions(
        self,
        client: AsyncClient,
        source_node_with_table_metadata: Node,
    ):
        """
        Test successfully upserting new partition metadata.
        """
        node = source_node_with_table_metadata
        partition_input = {
            "partitions": [
                {
                    "partition_value": "20240101",
                    "size_bytes": 50000,
                    "row_count": 1000,
                },
                {
                    "partition_value": "20240102",
                    "size_bytes": 60000,
                    "row_count": 1200,
                },
            ],
        }

        response = await client.post(
            f"/sources/{node.name}/partition-metadata",
            json=partition_input,
        )
        assert response.status_code == 201

        data = response.json()
        assert "Upserted 2 partition(s)" in data["message"]

    async def test_update_existing_partitions(
        self,
        client: AsyncClient,
        source_node_with_table_metadata: Node,
    ):
        """
        Test updating existing partitions (upsert behavior).
        """
        node = source_node_with_table_metadata

        # Insert initial partitions
        initial_input = {
            "partitions": [
                {
                    "partition_value": "20240101",
                    "size_bytes": 50000,
                    "row_count": 1000,
                },
            ],
        }
        await client.post(
            f"/sources/{node.name}/partition-metadata",
            json=initial_input,
        )

        # Update with new values
        updated_input = {
            "partitions": [
                {
                    "partition_value": "20240101",
                    "size_bytes": 55000,
                    "row_count": 1100,
                },
            ],
        }
        response = await client.post(
            f"/sources/{node.name}/partition-metadata",
            json=updated_input,
        )
        assert response.status_code == 201

    async def test_mixed_insert_and_update(
        self,
        client: AsyncClient,
        source_node_with_table_metadata: Node,
    ):
        """
        Test mixed insert and update in single request.
        """
        node = source_node_with_table_metadata

        # Insert initial partition
        await client.post(
            f"/sources/{node.name}/partition-metadata",
            json={
                "partitions": [
                    {
                        "partition_value": "20240101",
                        "size_bytes": 50000,
                        "row_count": 1000,
                    },
                ],
            },
        )

        # Mixed: update 20240101, insert 20240102
        response = await client.post(
            f"/sources/{node.name}/partition-metadata",
            json={
                "partitions": [
                    {
                        "partition_value": "20240101",
                        "size_bytes": 55000,
                        "row_count": 1100,
                    },
                    {
                        "partition_value": "20240102",
                        "size_bytes": 60000,
                        "row_count": 1200,
                    },
                ],
            },
        )
        assert response.status_code == 201
        data = response.json()
        assert "Upserted 2 partition(s)" in data["message"]

    async def test_empty_partition_list(
        self,
        client: AsyncClient,
        source_node_with_table_metadata: Node,
    ):
        """
        Test that empty partition list is handled.
        """
        node = source_node_with_table_metadata
        response = await client.post(
            f"/sources/{node.name}/partition-metadata",
            json={"partitions": []},
        )
        assert response.status_code == 201
        data = response.json()
        assert "Upserted 0 partition(s)" in data["message"]

    async def test_node_not_found(
        self,
        client: AsyncClient,
    ):
        """
        Test error when node doesn't exist.
        """
        response = await client.post(
            "/sources/nonexistent.node/partition-metadata",
            json={
                "partitions": [
                    {
                        "partition_value": "20240101",
                        "size_bytes": 50000,
                        "row_count": 1000,
                    },
                ],
            },
        )
        assert response.status_code == 404

    async def test_node_is_not_source_type(
        self,
        client: AsyncClient,
        transform_node: Node,
    ):
        """
        Test error when node is not a SOURCE type.
        """
        response = await client.post(
            f"/sources/{transform_node.name}/partition-metadata",
            json={
                "partitions": [
                    {
                        "partition_value": "20240101",
                        "size_bytes": 50000,
                        "row_count": 1000,
                    },
                ],
            },
        )
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY
        data = response.json()
        assert "Only source nodes can have partition metadata" in data["message"]

    async def test_table_metadata_does_not_exist(
        self,
        client: AsyncClient,
        source_node: Node,
    ):
        """
        Test error when table metadata doesn't exist (must be created first).
        """
        response = await client.post(
            f"/sources/{source_node.name}/partition-metadata",
            json={
                "partitions": [
                    {
                        "partition_value": "20240101",
                        "size_bytes": 50000,
                        "row_count": 1000,
                    },
                ],
            },
        )
        assert response.status_code == 404
        data = response.json()
        assert "No table metadata found" in data["message"]
        assert "Please set table metadata first" in data["message"]

    async def test_validation_negative_values(
        self,
        client: AsyncClient,
        source_node_with_table_metadata: Node,
    ):
        """
        Test validation rejects negative values.
        """
        node = source_node_with_table_metadata

        # Negative size_bytes
        response = await client.post(
            f"/sources/{node.name}/partition-metadata",
            json={
                "partitions": [
                    {
                        "partition_value": "20240101",
                        "size_bytes": -50000,
                        "row_count": 1000,
                    },
                ],
            },
        )
        assert response.status_code == 422

        # Negative row_count
        response = await client.post(
            f"/sources/{node.name}/partition-metadata",
            json={
                "partitions": [
                    {
                        "partition_value": "20240101",
                        "size_bytes": 50000,
                        "row_count": -1000,
                    },
                ],
            },
        )
        assert response.status_code == 422


@pytest.mark.asyncio
class TestGetPartitionMetadata:
    """
    Tests for GET /sources/{name}/partition-metadata endpoint.
    """

    @pytest_asyncio.fixture
    async def source_node_with_partitions(
        self,
        client: AsyncClient,
        source_node: Node,
    ) -> Node:
        """Create source node with table metadata and partitions."""
        # Create table metadata
        await client.post(
            f"/sources/{source_node.name}/table-metadata",
            json={
                "total_size_bytes": 1000000,
                "total_row_count": 50000,
                "total_partitions": 10,
                "earliest_partition_value": "20240101",
                "latest_partition_value": "20240110",
            },
        )

        # Add partitions
        partitions = [
            {
                "partition_value": f"2024010{i}",
                "size_bytes": 50000 + i * 1000,
                "row_count": 1000 + i * 100,
            }
            for i in range(1, 10)
        ]
        partitions.append(
            {
                "partition_value": "20240110",
                "size_bytes": 60000,
                "row_count": 2000,
            },
        )

        await client.post(
            f"/sources/{source_node.name}/partition-metadata",
            json={"partitions": partitions},
        )
        return source_node

    async def test_get_all_partitions(
        self,
        client: AsyncClient,
        source_node_with_partitions: Node,
    ):
        """
        Test getting all partitions without filters.
        """
        node = source_node_with_partitions
        response = await client.get(
            f"/sources/{node.name}/partition-metadata",
        )
        assert response.status_code == 200

        data = response.json()
        assert len(data) == 10
        # Verify descending order
        assert data[0]["partition_value"] == "20240110"
        assert data[9]["partition_value"] == "20240101"

    async def test_filter_by_from_partition_only(
        self,
        client: AsyncClient,
        source_node_with_partitions: Node,
    ):
        """
        Test filtering by from_partition only.
        """
        node = source_node_with_partitions
        response = await client.get(
            f"/sources/{node.name}/partition-metadata",
            params={"from_partition": "20240105"},
        )
        assert response.status_code == 200

        data = response.json()
        assert len(data) == 6
        assert data[0]["partition_value"] == "20240110"
        assert data[-1]["partition_value"] == "20240105"

    async def test_filter_by_to_partition_only(
        self,
        client: AsyncClient,
        source_node_with_partitions: Node,
    ):
        """
        Test filtering by to_partition only.
        """
        node = source_node_with_partitions
        response = await client.get(
            f"/sources/{node.name}/partition-metadata",
            params={"to_partition": "20240105"},
        )
        assert response.status_code == 200

        data = response.json()
        assert len(data) == 5
        assert data[0]["partition_value"] == "20240105"
        assert data[-1]["partition_value"] == "20240101"

    async def test_filter_by_both_partitions(
        self,
        client: AsyncClient,
        source_node_with_partitions: Node,
    ):
        """
        Test filtering by both from_partition and to_partition.
        """
        node = source_node_with_partitions
        response = await client.get(
            f"/sources/{node.name}/partition-metadata",
            params={
                "from_partition": "20240103",
                "to_partition": "20240107",
            },
        )
        assert response.status_code == 200

        data = response.json()
        assert len(data) == 5
        assert data[0]["partition_value"] == "20240107"
        assert data[-1]["partition_value"] == "20240103"

    async def test_node_not_found(
        self,
        client: AsyncClient,
    ):
        """
        Test error when node doesn't exist.
        """
        response = await client.get(
            "/sources/nonexistent.node/partition-metadata",
        )
        assert response.status_code == 404

    async def test_table_metadata_not_found(
        self,
        client: AsyncClient,
        source_node: Node,
    ):
        """
        Test error when table metadata doesn't exist.
        """
        response = await client.get(
            f"/sources/{source_node.name}/partition-metadata",
        )
        assert response.status_code == 404
        data = response.json()
        assert "No table metadata found" in data["message"]

    async def test_empty_list_when_no_partitions_match(
        self,
        client: AsyncClient,
        source_node_with_partitions: Node,
    ):
        """
        Test empty list returned when no partitions match filters.
        """
        node = source_node_with_partitions
        response = await client.get(
            f"/sources/{node.name}/partition-metadata",
            params={"from_partition": "20240201"},
        )
        assert response.status_code == 200
        data = response.json()
        assert data == []


@pytest.mark.asyncio
class TestDeletePartitionMetadata:
    """
    Tests for DELETE /sources/{name}/partition-metadata endpoint.
    """

    @pytest_asyncio.fixture
    async def source_node_with_partitions(
        self,
        client: AsyncClient,
        source_node: Node,
    ) -> Node:
        """Create source node with table metadata and partitions."""
        # Create table metadata
        await client.post(
            f"/sources/{source_node.name}/table-metadata",
            json={
                "total_size_bytes": 1000000,
                "total_row_count": 50000,
                "total_partitions": 10,
                "earliest_partition_value": "20240101",
                "latest_partition_value": "20240110",
            },
        )

        # Add partitions
        partitions = [
            {
                "partition_value": f"2024010{i}",
                "size_bytes": 50000 + i * 1000,
                "row_count": 1000 + i * 100,
            }
            for i in range(1, 10)
        ]
        partitions.append(
            {
                "partition_value": "20240110",
                "size_bytes": 60000,
                "row_count": 2000,
            },
        )

        await client.post(
            f"/sources/{source_node.name}/partition-metadata",
            json={"partitions": partitions},
        )
        return source_node

    async def test_delete_all_partitions(
        self,
        client: AsyncClient,
        source_node_with_partitions: Node,
    ):
        """
        Test deleting all partitions without filters.
        """
        node = source_node_with_partitions
        response = await client.delete(
            f"/sources/{node.name}/partition-metadata",
        )
        assert response.status_code == 200

        data = response.json()
        assert "Deleted 10 partition(s)" in data["message"]

        # Verify all partitions deleted
        response = await client.get(
            f"/sources/{node.name}/partition-metadata",
        )
        assert response.status_code == 200
        assert response.json() == []

    async def test_delete_by_from_partition_only(
        self,
        client: AsyncClient,
        source_node_with_partitions: Node,
    ):
        """
        Test deleting by from_partition only.
        """
        node = source_node_with_partitions
        response = await client.delete(
            f"/sources/{node.name}/partition-metadata",
            params={"from_partition": "20240105"},
        )
        assert response.status_code == 200

        data = response.json()
        assert "Deleted 6 partition(s)" in data["message"]

        # Verify correct partitions remain
        response = await client.get(
            f"/sources/{node.name}/partition-metadata",
        )
        remaining = response.json()
        assert len(remaining) == 4

    async def test_delete_by_to_partition_only(
        self,
        client: AsyncClient,
        source_node_with_partitions: Node,
    ):
        """
        Test deleting by to_partition only.
        """
        node = source_node_with_partitions
        response = await client.delete(
            f"/sources/{node.name}/partition-metadata",
            params={"to_partition": "20240105"},
        )
        assert response.status_code == 200

        data = response.json()
        assert "Deleted 5 partition(s)" in data["message"]

    async def test_delete_by_range(
        self,
        client: AsyncClient,
        source_node_with_partitions: Node,
    ):
        """
        Test deleting by range (from_partition and to_partition).
        """
        node = source_node_with_partitions
        response = await client.delete(
            f"/sources/{node.name}/partition-metadata",
            params={
                "from_partition": "20240103",
                "to_partition": "20240107",
            },
        )
        assert response.status_code == 200

        data = response.json()
        assert "Deleted 5 partition(s)" in data["message"]

    async def test_node_not_found(
        self,
        client: AsyncClient,
    ):
        """
        Test error when node doesn't exist.
        """
        response = await client.delete(
            "/sources/nonexistent.node/partition-metadata",
        )
        assert response.status_code == 404

    async def test_node_is_not_source_type(
        self,
        client: AsyncClient,
        transform_node: Node,
    ):
        """
        Test error when node is not a SOURCE type.
        """
        response = await client.delete(
            f"/sources/{transform_node.name}/partition-metadata",
        )
        assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY

    async def test_table_metadata_not_found(
        self,
        client: AsyncClient,
        source_node: Node,
    ):
        """
        Test error when table metadata doesn't exist.
        """
        response = await client.delete(
            f"/sources/{source_node.name}/partition-metadata",
        )
        assert response.status_code == 404
        data = response.json()
        assert "No table metadata found" in data["message"]

    async def test_zero_deleted_when_no_match(
        self,
        client: AsyncClient,
        source_node_with_partitions: Node,
    ):
        """
        Test that zero count returned when no partitions match range.
        """
        node = source_node_with_partitions
        response = await client.delete(
            f"/sources/{node.name}/partition-metadata",
            params={"from_partition": "20240201"},
        )
        assert response.status_code == 200
        data = response.json()
        assert "Deleted 0 partition(s)" in data["message"]


@pytest.mark.asyncio
class TestIntegrationScenarios:
    """
    Integration tests covering end-to-end workflows and edge cases.
    """

    async def test_full_workflow(
        self,
        client: AsyncClient,
        source_node: Node,
    ):
        """
        Test complete workflow: POST table metadata → POST partitions → GET both → DELETE partitions.
        """
        # Step 1: Create table metadata
        table_response = await client.post(
            f"/sources/{source_node.name}/table-metadata",
            json={
                "total_size_bytes": 1000000,
                "total_row_count": 50000,
                "total_partitions": 3,
                "earliest_partition_value": "20240101",
                "latest_partition_value": "20240103",
            },
        )
        assert table_response.status_code == 200

        # Step 2: Add partition metadata
        partition_response = await client.post(
            f"/sources/{source_node.name}/partition-metadata",
            json={
                "partitions": [
                    {
                        "partition_value": "20240101",
                        "size_bytes": 300000,
                        "row_count": 15000,
                    },
                    {
                        "partition_value": "20240102",
                        "size_bytes": 350000,
                        "row_count": 17500,
                    },
                    {
                        "partition_value": "20240103",
                        "size_bytes": 350000,
                        "row_count": 17500,
                    },
                ],
            },
        )
        assert partition_response.status_code == 201

        # Step 3: Get table metadata
        table_get_response = await client.get(
            f"/sources/{source_node.name}/table-metadata",
        )
        assert table_get_response.status_code == 200
        table_data = table_get_response.json()
        assert table_data["total_size_bytes"] == 1000000

        # Step 4: Get partition metadata
        partition_get_response = await client.get(
            f"/sources/{source_node.name}/partition-metadata",
        )
        assert partition_get_response.status_code == 200
        partition_data = partition_get_response.json()
        assert len(partition_data) == 3

        # Step 5: Delete specific partitions
        delete_response = await client.delete(
            f"/sources/{source_node.name}/partition-metadata",
            params={"from_partition": "20240102"},
        )
        assert delete_response.status_code == 200
        delete_data = delete_response.json()
        assert "Deleted 2 partition(s)" in delete_data["message"]

        # Step 6: Verify only one partition remains
        final_get_response = await client.get(
            f"/sources/{source_node.name}/partition-metadata",
        )
        final_data = final_get_response.json()
        assert len(final_data) == 1
        assert final_data[0]["partition_value"] == "20240101"

    async def test_update_workflow(
        self,
        client: AsyncClient,
        source_node: Node,
    ):
        """
        Test update workflow: POST metadata twice, verify upsert behavior.
        """
        # First POST
        await client.post(
            f"/sources/{source_node.name}/table-metadata",
            json={
                "total_size_bytes": 1000000,
                "total_row_count": 50000,
                "total_partitions": 30,
            },
        )

        # Second POST (update)
        response = await client.post(
            f"/sources/{source_node.name}/table-metadata",
            json={
                "total_size_bytes": 2000000,
                "total_row_count": 100000,
                "total_partitions": 60,
            },
        )
        assert response.status_code == 200

        # Verify updated values
        get_response = await client.get(
            f"/sources/{source_node.name}/table-metadata",
        )
        data = get_response.json()
        assert data["total_size_bytes"] == 2000000
        assert data["total_row_count"] == 100000

    async def test_partition_requires_table_metadata_first(
        self,
        client: AsyncClient,
        source_node: Node,
    ):
        """
        Test that partition metadata requires table metadata to exist first.
        """
        # Try to add partition metadata without table metadata
        response = await client.post(
            f"/sources/{source_node.name}/partition-metadata",
            json={
                "partitions": [
                    {
                        "partition_value": "20240101",
                        "size_bytes": 50000,
                        "row_count": 1000,
                    },
                ],
            },
        )
        assert response.status_code == 404
        data = response.json()
        assert "Please set table metadata first" in data["message"]

    async def test_cascade_delete_with_node(
        self,
        session: AsyncSession,
        client: AsyncClient,
        source_node: Node,
    ):
        """
        Test CASCADE delete: when node is deleted, metadata is also deleted.
        """
        # Create table and partition metadata
        await client.post(
            f"/sources/{source_node.name}/table-metadata",
            json={
                "total_size_bytes": 1000000,
                "total_row_count": 50000,
                "total_partitions": 2,
            },
        )
        await client.post(
            f"/sources/{source_node.name}/partition-metadata",
            json={
                "partitions": [
                    {
                        "partition_value": "20240101",
                        "size_bytes": 50000,
                        "row_count": 1000,
                    },
                ],
            },
        )

        # Delete the node
        from sqlalchemy import delete

        from datajunction_server.database.node import Node

        await session.execute(
            delete(Node).where(Node.id == source_node.id),
        )
        await session.commit()

        # Verify metadata endpoints return 404
        response = await client.get(
            f"/sources/{source_node.name}/table-metadata",
        )
        assert response.status_code == 404

    async def test_large_partition_batch(
        self,
        client: AsyncClient,
        source_node: Node,
    ):
        """
        Test inserting a large batch of partitions.
        """
        # Create table metadata
        await client.post(
            f"/sources/{source_node.name}/table-metadata",
            json={
                "total_size_bytes": 10000000,
                "total_row_count": 500000,
                "total_partitions": 100,
            },
        )

        # Create 100 partitions
        partitions = [
            {
                "partition_value": f"202401{i:02d}",
                "size_bytes": 100000 + i * 1000,
                "row_count": 5000 + i * 100,
            }
            for i in range(1, 101)
        ]

        response = await client.post(
            f"/sources/{source_node.name}/partition-metadata",
            json={"partitions": partitions},
        )
        assert response.status_code == 201
        data = response.json()
        assert "Upserted 100 partition(s)" in data["message"]

        # Verify all were inserted
        get_response = await client.get(
            f"/sources/{source_node.name}/partition-metadata",
        )
        assert len(get_response.json()) == 100
