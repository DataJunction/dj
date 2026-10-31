"""
A node revision with no catalog returns a null catalog.

Wrapping the missing catalog in the GraphQL `Catalog` type produced an object
with a null name, so selecting `catalog { name }` failed with "Cannot return
null for non-nullable field Catalog.name" instead of returning null.
"""

import pytest
import sqlalchemy as sa
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.database.node import NodeRevision


@pytest.mark.asyncio
async def test_node_without_a_catalog_has_a_null_catalog(
    client_with_roads: AsyncClient,
    session: AsyncSession,
) -> None:
    await session.execute(
        sa.update(NodeRevision)
        .where(NodeRevision.name == "default.repair_type")
        .values(catalog_id=None),
    )
    await session.commit()

    for query in [
        """
        {
            findNodes(names: ["default.repair_type"]) {
                current { catalog { name } }
            }
        }
        """,
        """
        {
            findNodesPaginated(names: ["default.repair_type"]) {
                edges { node { current { catalog { name } } } }
            }
        }
        """,
    ]:
        body = (await client_with_roads.post("/graphql", json={"query": query})).json()
        assert "errors" not in body, body
        data = body["data"]
        current = (
            data["findNodes"][0]["current"]
            if "findNodes" in data
            else data["findNodesPaginated"]["edges"][0]["node"]["current"]
        )
        assert current["catalog"] is None


@pytest.mark.asyncio
async def test_node_with_a_catalog_still_returns_it(
    client_with_roads: AsyncClient,
) -> None:
    query = """
    {
        findNodes(names: ["default.repair_type"]) {
            current { catalog { name } }
        }
    }
    """
    body = (await client_with_roads.post("/graphql", json={"query": query})).json()
    assert body["data"]["findNodes"][0]["current"]["catalog"] == {"name": "default"}
