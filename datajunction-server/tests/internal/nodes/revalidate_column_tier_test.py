"""
The version tier `revalidate_node` gives a node whose columns moved.

Any column difference used to earn a major bump. That is wrong for the additive
cases -- nothing written before a column existed can be referencing it -- and it
matters more now that downstream cubes inherit their upstream's tier, since a
major bump rebuilds a cube's materialized table. A type change stays major:
everything reading the column is reading a different type than it was written
against.

The column differences are induced by editing the stored columns directly, the
same trick the existing revalidate tests use, because they are differences
between what is *stored* on the revision and what the validator recomputes --
which no API payload can produce on its own.
"""

import pytest
from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

NODE = "default.tier_node"
COLUMNS = [("repair_order_id", "int"), ("price", "float")]


async def _create_node(client: AsyncClient) -> None:
    response = await client.post(
        "/nodes/transform/",
        json={
            "name": NODE,
            "description": "A transform whose columns the tests move",
            "mode": "published",
            "query": "SELECT repair_order_id, price FROM default.repair_order_details",
        },
    )
    assert response.status_code == 201, response.text
    assert response.json()["version"] == "v1.0"
    assert [(col["name"], col["type"]) for col in response.json()["columns"]] == COLUMNS


async def _revalidate(client: AsyncClient) -> None:
    response = await client.post(f"/nodes/{NODE}/validate/")
    assert response.status_code == 200, response.text
    assert response.json()["status"] == "valid"


async def _node(client: AsyncClient) -> tuple[str, list[tuple[str, str]]]:
    response = await client.get(f"/nodes/{NODE}")
    assert response.status_code == 200, response.text
    return (
        response.json()["version"],
        [(col["name"], col["type"]) for col in response.json()["columns"]],
    )


async def _edit_stored_columns(session: AsyncSession, statement: str) -> None:
    await session.execute(
        text(
            f"""
            {statement}
            WHERE name = 'price' AND node_revision_id IN (
                SELECT id FROM noderevision WHERE name = :name
            )
            """,
        ),
        {"name": NODE},
    )
    await session.commit()
    session.expire_all()


@pytest.mark.asyncio
async def test_added_column_is_a_minor_bump(
    client_with_roads: AsyncClient,
    session: AsyncSession,
):
    """A column the revision does not have yet is additive, so it earns v1.1."""
    await _create_node(client_with_roads)
    await _edit_stored_columns(session, 'DELETE FROM "column"')

    await _revalidate(client_with_roads)

    assert await _node(client_with_roads) == ("v1.1", COLUMNS)


@pytest.mark.asyncio
async def test_column_type_change_is_a_major_bump(
    client_with_roads: AsyncClient,
    session: AsyncSession,
):
    """A column whose type moved breaks everything reading it, so it earns v2.0."""
    await _create_node(client_with_roads)
    await _edit_stored_columns(session, "UPDATE \"column\" SET type = 'int'")

    await _revalidate(client_with_roads)

    assert await _node(client_with_roads) == ("v2.0", COLUMNS)


@pytest.mark.asyncio
async def test_backfilled_column_order_is_a_minor_bump(
    client_with_roads: AsyncClient,
    session: AsyncSession,
):
    """
    A missing `order` is DJ filling in metadata it should already have stored. It
    changes no name, no type and no value, so it sits with the additive cases.
    """
    await _create_node(client_with_roads)
    await _edit_stored_columns(session, 'UPDATE "column" SET "order" = NULL')

    await _revalidate(client_with_roads)

    assert await _node(client_with_roads) == ("v1.1", COLUMNS)
