"""
Tests for source table metadata internal functions.
"""

import pytest
import pytest_asyncio
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.database.node import Node, NodeRevision
from datajunction_server.database.source_metadata import (
    SourcePartitionMetadata,
    SourceTableMetadata,
)
from datajunction_server.database.user import User
from datajunction_server.internal.source_metadata import (
    delete_partition_metadata_range,
    get_partition_metadata_range,
    get_source_table_metadata,
    upsert_partition_metadata,
    upsert_source_table_metadata,
)
from datajunction_server.models.node_type import NodeType
from datajunction_server.models.source_metadata import (
    PartitionMetadataItem,
    SourceTableMetadataInput,
)


@pytest_asyncio.fixture
async def source_node(session: AsyncSession, current_user: User) -> Node:
    """
    Create a test source node.
    """
    node = Node(
        name="test.source_table",
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


@pytest.mark.asyncio
class TestUpsertSourceTableMetadata:
    """
    Tests for upsert_source_table_metadata function.
    """

    async def test_insert_new_table_metadata(
        self,
        session: AsyncSession,
        source_node: Node,
    ):
        """
        Test inserting new table metadata for a node.
        """
        metadata_input = SourceTableMetadataInput(
            total_size_bytes=1000000,
            total_row_count=50000,
            total_partitions=30,
            earliest_partition_value="20240101",
            latest_partition_value="20240130",
            freshness_timestamp=1706745600,
            ttl_days=365,
        )

        result = await upsert_source_table_metadata(
            session=session,
            node_id=source_node.id,
            metadata=metadata_input,
        )

        assert result.id is not None
        assert result.node_id == source_node.id
        assert result.total_size_bytes == 1000000
        assert result.total_row_count == 50000
        assert result.total_partitions == 30
        assert result.earliest_partition_value == "20240101"
        assert result.latest_partition_value == "20240130"
        assert result.freshness_timestamp == 1706745600
        assert result.ttl_days == 365
        assert result.updated_at is not None

    async def test_update_existing_table_metadata(
        self,
        session: AsyncSession,
        source_node: Node,
    ):
        """
        Test updating existing table metadata (upsert behavior).
        """
        # Insert initial metadata
        initial_metadata = SourceTableMetadataInput(
            total_size_bytes=1000000,
            total_row_count=50000,
            total_partitions=30,
            earliest_partition_value="20240101",
            latest_partition_value="20240130",
            freshness_timestamp=1706745600,
            ttl_days=365,
        )

        initial = await upsert_source_table_metadata(
            session=session,
            node_id=source_node.id,
            metadata=initial_metadata,
        )
        await session.commit()

        initial_id = initial.id
        initial_updated_at = initial.updated_at

        # Update with new values
        updated_metadata = SourceTableMetadataInput(
            total_size_bytes=2000000,
            total_row_count=100000,
            total_partitions=60,
            earliest_partition_value="20240101",
            latest_partition_value="20240229",
            freshness_timestamp=1709251200,
            ttl_days=730,
        )

        result = await upsert_source_table_metadata(
            session=session,
            node_id=source_node.id,
            metadata=updated_metadata,
        )
        await session.commit()

        # Verify it's the same record (update, not insert)
        assert result.id == initial_id
        assert result.node_id == source_node.id

        # Verify all fields were updated
        assert result.total_size_bytes == 2000000
        assert result.total_row_count == 100000
        assert result.total_partitions == 60
        assert result.earliest_partition_value == "20240101"
        assert result.latest_partition_value == "20240229"
        assert result.freshness_timestamp == 1709251200
        assert result.ttl_days == 730

        # Verify updated_at changed
        assert result.updated_at >= initial_updated_at

    async def test_insert_with_optional_fields_none(
        self,
        session: AsyncSession,
        source_node: Node,
    ):
        """
        Test inserting metadata with optional fields as None.
        """
        metadata_input = SourceTableMetadataInput(
            total_size_bytes=1000000,
            total_row_count=50000,
            total_partitions=30,
            earliest_partition_value="20240101",
            latest_partition_value="20240130",
            freshness_timestamp=None,
            ttl_days=None,
        )

        result = await upsert_source_table_metadata(
            session=session,
            node_id=source_node.id,
            metadata=metadata_input,
        )

        assert result.freshness_timestamp is None
        assert result.ttl_days is None
        assert result.total_size_bytes == 1000000

    async def test_update_clears_optional_fields(
        self,
        session: AsyncSession,
        source_node: Node,
    ):
        """
        Test that updating can clear optional fields.
        """
        # Insert with optional fields populated
        initial_metadata = SourceTableMetadataInput(
            total_size_bytes=1000000,
            total_row_count=50000,
            total_partitions=30,
            earliest_partition_value="20240101",
            latest_partition_value="20240130",
            freshness_timestamp=1706745600,
            ttl_days=365,
        )

        await upsert_source_table_metadata(
            session=session,
            node_id=source_node.id,
            metadata=initial_metadata,
        )
        await session.commit()

        # Update with optional fields as None
        updated_metadata = SourceTableMetadataInput(
            total_size_bytes=2000000,
            total_row_count=100000,
            total_partitions=60,
            earliest_partition_value="20240101",
            latest_partition_value="20240229",
            freshness_timestamp=None,
            ttl_days=None,
        )

        result = await upsert_source_table_metadata(
            session=session,
            node_id=source_node.id,
            metadata=updated_metadata,
        )

        assert result.freshness_timestamp is None
        assert result.ttl_days is None


@pytest.mark.asyncio
class TestUpsertPartitionMetadata:
    """
    Tests for upsert_partition_metadata function.
    """

    @pytest_asyncio.fixture
    async def table_metadata(
        self,
        session: AsyncSession,
        source_node: Node,
    ) -> SourceTableMetadata:
        """Create table metadata for partition tests."""
        metadata_input = SourceTableMetadataInput(
            total_size_bytes=1000000,
            total_row_count=50000,
            total_partitions=30,
            earliest_partition_value="20240101",
            latest_partition_value="20240130",
        )
        result = await upsert_source_table_metadata(
            session=session,
            node_id=source_node.id,
            metadata=metadata_input,
        )
        await session.commit()
        return result

    async def test_insert_new_partitions(
        self,
        session: AsyncSession,
        table_metadata: SourceTableMetadata,
    ):
        """
        Test inserting new partition metadata.
        """
        partitions = [
            PartitionMetadataItem(
                partition_value="20240101",
                size_bytes=50000,
                row_count=1000,
            ),
            PartitionMetadataItem(
                partition_value="20240102",
                size_bytes=60000,
                row_count=1200,
            ),
            PartitionMetadataItem(
                partition_value="20240103",
                size_bytes=55000,
                row_count=1100,
            ),
        ]

        await upsert_partition_metadata(
            session=session,
            source_table_metadata_id=table_metadata.id,
            partition_stats=partitions,
        )
        await session.commit()

        # Verify partitions were inserted
        result = await session.execute(
            select(SourcePartitionMetadata)
            .where(
                SourcePartitionMetadata.source_table_metadata_id == table_metadata.id,
            )
            .order_by(SourcePartitionMetadata.partition_value),
        )
        stored_partitions = list(result.scalars().all())

        assert len(stored_partitions) == 3
        assert stored_partitions[0].partition_value == "20240101"
        assert stored_partitions[0].size_bytes == 50000
        assert stored_partitions[0].row_count == 1000
        assert stored_partitions[1].partition_value == "20240102"
        assert stored_partitions[2].partition_value == "20240103"

    async def test_update_existing_partitions(
        self,
        session: AsyncSession,
        table_metadata: SourceTableMetadata,
    ):
        """
        Test updating existing partition metadata (upsert behavior).
        """
        # Insert initial partitions
        initial_partitions = [
            PartitionMetadataItem(
                partition_value="20240101",
                size_bytes=50000,
                row_count=1000,
            ),
            PartitionMetadataItem(
                partition_value="20240102",
                size_bytes=60000,
                row_count=1200,
            ),
        ]

        await upsert_partition_metadata(
            session=session,
            source_table_metadata_id=table_metadata.id,
            partition_stats=initial_partitions,
        )
        await session.commit()

        # Update with new values
        updated_partitions = [
            PartitionMetadataItem(
                partition_value="20240101",
                size_bytes=55000,
                row_count=1100,
            ),
            PartitionMetadataItem(
                partition_value="20240102",
                size_bytes=65000,
                row_count=1300,
            ),
        ]

        await upsert_partition_metadata(
            session=session,
            source_table_metadata_id=table_metadata.id,
            partition_stats=updated_partitions,
        )
        await session.commit()

        # Verify partitions were updated
        result = await session.execute(
            select(SourcePartitionMetadata)
            .where(
                SourcePartitionMetadata.source_table_metadata_id == table_metadata.id,
            )
            .order_by(SourcePartitionMetadata.partition_value),
        )
        stored_partitions = list(result.scalars().all())

        assert len(stored_partitions) == 2
        assert stored_partitions[0].partition_value == "20240101"
        assert stored_partitions[0].size_bytes == 55000
        assert stored_partitions[0].row_count == 1100
        assert stored_partitions[1].partition_value == "20240102"
        assert stored_partitions[1].size_bytes == 65000
        assert stored_partitions[1].row_count == 1300

    async def test_mixed_insert_and_update(
        self,
        session: AsyncSession,
        table_metadata: SourceTableMetadata,
    ):
        """
        Test mixed insert and update in same batch.
        """
        # Insert initial partition
        initial_partitions = [
            PartitionMetadataItem(
                partition_value="20240101",
                size_bytes=50000,
                row_count=1000,
            ),
        ]

        await upsert_partition_metadata(
            session=session,
            source_table_metadata_id=table_metadata.id,
            partition_stats=initial_partitions,
        )
        await session.commit()

        # Mixed: update 20240101, insert 20240102 and 20240103
        mixed_partitions = [
            PartitionMetadataItem(
                partition_value="20240101",
                size_bytes=55000,
                row_count=1100,
            ),
            PartitionMetadataItem(
                partition_value="20240102",
                size_bytes=60000,
                row_count=1200,
            ),
            PartitionMetadataItem(
                partition_value="20240103",
                size_bytes=65000,
                row_count=1300,
            ),
        ]

        await upsert_partition_metadata(
            session=session,
            source_table_metadata_id=table_metadata.id,
            partition_stats=mixed_partitions,
        )
        await session.commit()

        # Verify all partitions
        result = await session.execute(
            select(SourcePartitionMetadata)
            .where(
                SourcePartitionMetadata.source_table_metadata_id == table_metadata.id,
            )
            .order_by(SourcePartitionMetadata.partition_value),
        )
        stored_partitions = list(result.scalars().all())

        assert len(stored_partitions) == 3
        assert stored_partitions[0].partition_value == "20240101"
        assert stored_partitions[0].size_bytes == 55000
        assert stored_partitions[1].partition_value == "20240102"
        assert stored_partitions[2].partition_value == "20240103"

    async def test_empty_partition_list(
        self,
        session: AsyncSession,
        table_metadata: SourceTableMetadata,
    ):
        """
        Test that empty partition list is handled gracefully.
        """
        await upsert_partition_metadata(
            session=session,
            source_table_metadata_id=table_metadata.id,
            partition_stats=[],
        )
        await session.commit()

        # Verify no partitions were inserted
        result = await session.execute(
            select(SourcePartitionMetadata).where(
                SourcePartitionMetadata.source_table_metadata_id == table_metadata.id,
            ),
        )
        stored_partitions = list(result.scalars().all())
        assert len(stored_partitions) == 0


@pytest.mark.asyncio
class TestGetSourceTableMetadata:
    """
    Tests for get_source_table_metadata function.
    """

    async def test_retrieve_existing_metadata(
        self,
        session: AsyncSession,
        source_node: Node,
    ):
        """
        Test retrieving existing metadata by node_id.
        """
        # Insert metadata
        metadata_input = SourceTableMetadataInput(
            total_size_bytes=1000000,
            total_row_count=50000,
            total_partitions=30,
            earliest_partition_value="20240101",
            latest_partition_value="20240130",
            freshness_timestamp=1706745600,
            ttl_days=365,
        )

        inserted = await upsert_source_table_metadata(
            session=session,
            node_id=source_node.id,
            metadata=metadata_input,
        )
        await session.commit()

        # Retrieve it
        result = await get_source_table_metadata(
            session=session,
            node_id=source_node.id,
        )

        assert result is not None
        assert result.id == inserted.id
        assert result.node_id == source_node.id
        assert result.total_size_bytes == 1000000
        assert result.total_row_count == 50000

    async def test_return_none_when_not_exists(
        self,
        session: AsyncSession,
        source_node: Node,
    ):
        """
        Test that None is returned when no metadata exists.
        """
        result = await get_source_table_metadata(
            session=session,
            node_id=source_node.id,
        )

        assert result is None


@pytest.mark.asyncio
class TestGetPartitionMetadataRange:
    """
    Tests for get_partition_metadata_range function.
    """

    @pytest_asyncio.fixture
    async def table_metadata_with_partitions(
        self,
        session: AsyncSession,
        source_node: Node,
    ) -> SourceTableMetadata:
        """Create table metadata with partition data."""
        metadata_input = SourceTableMetadataInput(
            total_size_bytes=1000000,
            total_row_count=50000,
            total_partitions=10,
            earliest_partition_value="20240101",
            latest_partition_value="20240110",
        )
        table_metadata = await upsert_source_table_metadata(
            session=session,
            node_id=source_node.id,
            metadata=metadata_input,
        )
        await session.commit()

        # Add partitions
        partitions = [
            PartitionMetadataItem(
                partition_value=f"2024010{i}",
                size_bytes=50000 + i * 1000,
                row_count=1000 + i * 100,
            )
            for i in range(1, 10)  # 20240101 to 20240109
        ]
        partitions.append(
            PartitionMetadataItem(
                partition_value="20240110",
                size_bytes=60000,
                row_count=2000,
            ),
        )

        await upsert_partition_metadata(
            session=session,
            source_table_metadata_id=table_metadata.id,
            partition_stats=partitions,
        )
        await session.commit()
        return table_metadata

    async def test_get_all_partitions(
        self,
        session: AsyncSession,
        table_metadata_with_partitions: SourceTableMetadata,
    ):
        """
        Test getting all partitions without filters.
        """
        result = await get_partition_metadata_range(
            session=session,
            source_table_metadata_id=table_metadata_with_partitions.id,
        )

        assert len(result) == 10
        # Verify descending order
        assert result[0].partition_value == "20240110"
        assert result[9].partition_value == "20240101"

    async def test_filter_by_from_partition_only(
        self,
        session: AsyncSession,
        table_metadata_with_partitions: SourceTableMetadata,
    ):
        """
        Test filtering by from_partition only.
        """
        result = await get_partition_metadata_range(
            session=session,
            source_table_metadata_id=table_metadata_with_partitions.id,
            from_partition="20240105",
        )

        assert len(result) == 6
        assert result[0].partition_value == "20240110"
        assert result[-1].partition_value == "20240105"

    async def test_filter_by_to_partition_only(
        self,
        session: AsyncSession,
        table_metadata_with_partitions: SourceTableMetadata,
    ):
        """
        Test filtering by to_partition only.
        """
        result = await get_partition_metadata_range(
            session=session,
            source_table_metadata_id=table_metadata_with_partitions.id,
            to_partition="20240105",
        )

        assert len(result) == 5
        assert result[0].partition_value == "20240105"
        assert result[-1].partition_value == "20240101"

    async def test_filter_by_both_partitions(
        self,
        session: AsyncSession,
        table_metadata_with_partitions: SourceTableMetadata,
    ):
        """
        Test filtering by both from_partition and to_partition.
        """
        result = await get_partition_metadata_range(
            session=session,
            source_table_metadata_id=table_metadata_with_partitions.id,
            from_partition="20240103",
            to_partition="20240107",
        )

        assert len(result) == 5
        assert result[0].partition_value == "20240107"
        assert result[-1].partition_value == "20240103"

    async def test_return_empty_when_no_partitions_exist(
        self,
        session: AsyncSession,
        source_node: Node,
    ):
        """
        Test that empty list is returned when no partitions exist.
        """
        # Create table metadata without partitions
        metadata_input = SourceTableMetadataInput(
            total_size_bytes=1000000,
            total_row_count=50000,
            total_partitions=0,
        )
        table_metadata = await upsert_source_table_metadata(
            session=session,
            node_id=source_node.id,
            metadata=metadata_input,
        )
        await session.commit()

        result = await get_partition_metadata_range(
            session=session,
            source_table_metadata_id=table_metadata.id,
        )

        assert result == []

    async def test_return_empty_when_no_partitions_match(
        self,
        session: AsyncSession,
        table_metadata_with_partitions: SourceTableMetadata,
    ):
        """
        Test that empty list is returned when no partitions match filters.
        """
        result = await get_partition_metadata_range(
            session=session,
            source_table_metadata_id=table_metadata_with_partitions.id,
            from_partition="20240201",
        )

        assert result == []


@pytest.mark.asyncio
class TestDeletePartitionMetadataRange:
    """
    Tests for delete_partition_metadata_range function.
    """

    @pytest_asyncio.fixture
    async def table_metadata_with_partitions(
        self,
        session: AsyncSession,
        source_node: Node,
    ) -> SourceTableMetadata:
        """Create table metadata with partition data."""
        metadata_input = SourceTableMetadataInput(
            total_size_bytes=1000000,
            total_row_count=50000,
            total_partitions=10,
            earliest_partition_value="20240101",
            latest_partition_value="20240110",
        )
        table_metadata = await upsert_source_table_metadata(
            session=session,
            node_id=source_node.id,
            metadata=metadata_input,
        )
        await session.commit()

        # Add partitions
        partitions = [
            PartitionMetadataItem(
                partition_value=f"2024010{i}",
                size_bytes=50000 + i * 1000,
                row_count=1000 + i * 100,
            )
            for i in range(1, 10)
        ]
        partitions.append(
            PartitionMetadataItem(
                partition_value="20240110",
                size_bytes=60000,
                row_count=2000,
            ),
        )

        await upsert_partition_metadata(
            session=session,
            source_table_metadata_id=table_metadata.id,
            partition_stats=partitions,
        )
        await session.commit()
        return table_metadata

    async def test_delete_all_partitions(
        self,
        session: AsyncSession,
        table_metadata_with_partitions: SourceTableMetadata,
    ):
        """
        Test deleting all partitions without filters.
        """
        deleted_count = await delete_partition_metadata_range(
            session=session,
            source_table_metadata_id=table_metadata_with_partitions.id,
        )
        await session.commit()

        assert deleted_count == 10

        # Verify all partitions deleted
        result = await get_partition_metadata_range(
            session=session,
            source_table_metadata_id=table_metadata_with_partitions.id,
        )
        assert len(result) == 0

    async def test_delete_by_from_partition_only(
        self,
        session: AsyncSession,
        table_metadata_with_partitions: SourceTableMetadata,
    ):
        """
        Test deleting by from_partition only.
        """
        deleted_count = await delete_partition_metadata_range(
            session=session,
            source_table_metadata_id=table_metadata_with_partitions.id,
            from_partition="20240105",
        )
        await session.commit()

        assert deleted_count == 6

        # Verify correct partitions remain
        result = await get_partition_metadata_range(
            session=session,
            source_table_metadata_id=table_metadata_with_partitions.id,
        )
        assert len(result) == 4
        assert result[0].partition_value == "20240104"
        assert result[-1].partition_value == "20240101"

    async def test_delete_by_to_partition_only(
        self,
        session: AsyncSession,
        table_metadata_with_partitions: SourceTableMetadata,
    ):
        """
        Test deleting by to_partition only.
        """
        deleted_count = await delete_partition_metadata_range(
            session=session,
            source_table_metadata_id=table_metadata_with_partitions.id,
            to_partition="20240105",
        )
        await session.commit()

        assert deleted_count == 5

        # Verify correct partitions remain
        result = await get_partition_metadata_range(
            session=session,
            source_table_metadata_id=table_metadata_with_partitions.id,
        )
        assert len(result) == 5
        assert result[0].partition_value == "20240110"
        assert result[-1].partition_value == "20240106"

    async def test_delete_by_range(
        self,
        session: AsyncSession,
        table_metadata_with_partitions: SourceTableMetadata,
    ):
        """
        Test deleting by range (from_partition and to_partition).
        """
        deleted_count = await delete_partition_metadata_range(
            session=session,
            source_table_metadata_id=table_metadata_with_partitions.id,
            from_partition="20240103",
            to_partition="20240107",
        )
        await session.commit()

        assert deleted_count == 5

        # Verify correct partitions remain
        result = await get_partition_metadata_range(
            session=session,
            source_table_metadata_id=table_metadata_with_partitions.id,
        )
        assert len(result) == 5
        # Should have 20240101, 20240102, 20240108, 20240109, 20240110
        partition_values = {p.partition_value for p in result}
        assert partition_values == {
            "20240101",
            "20240102",
            "20240108",
            "20240109",
            "20240110",
        }

    async def test_return_zero_when_no_partitions_match(
        self,
        session: AsyncSession,
        table_metadata_with_partitions: SourceTableMetadata,
    ):
        """
        Test that zero is returned when no partitions match range.
        """
        deleted_count = await delete_partition_metadata_range(
            session=session,
            source_table_metadata_id=table_metadata_with_partitions.id,
            from_partition="20240201",
        )
        await session.commit()

        assert deleted_count == 0

        # Verify all partitions remain
        result = await get_partition_metadata_range(
            session=session,
            source_table_metadata_id=table_metadata_with_partitions.id,
        )
        assert len(result) == 10
