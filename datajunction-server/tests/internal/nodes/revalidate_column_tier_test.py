"""
The version tier `revalidate_node` gives a node whose columns moved.

Any column difference used to earn a major bump. That is wrong for the additive
cases -- nothing written before a column existed can be referencing it -- and it
matters more now that downstream cubes inherit their upstream's tier, since a
major bump rebuilds a cube's materialized table. A type change stays major:
everything reading the column is reading a different type than it was written
against, and so is a removal, which breaks anything that referenced it.

A missing column `order` is not a change at all: it is DJ's own bookkeeping,
backfilled in place with no new revision.

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


async def _stored_order(session: AsyncSession) -> list[tuple[str, int | None]]:
    """(name, order) for the current revision's stored columns, by name."""
    session.expire_all()
    rows = await session.execute(
        text(
            """
            SELECT c.name, c."order"
            FROM "column" c
            JOIN noderevision nr ON nr.id = c.node_revision_id
            JOIN node n ON n.id = nr.node_id AND n.current_version = nr.version
            WHERE nr.name = :name
            ORDER BY c.name
            """,
        ),
        {"name": NODE},
    )
    return [(name, order) for name, order in rows.all()]


async def _revalidate_events(client: AsyncClient) -> list[dict]:
    response = await client.get(f"/history?node={NODE}")
    assert response.status_code == 200, response.text
    return [
        entry["details"]
        for entry in response.json()
        if entry["activity_type"] == "update"
    ]


async def _edit_stored_columns(
    session: AsyncSession,
    statement: str,
    column: str = "price",
) -> None:
    await session.execute(
        text(
            f"""
            {statement}
            WHERE name = :column AND node_revision_id IN (
                SELECT id FROM noderevision WHERE name = :name
            )
            """,
        ),
        {"name": NODE, "column": column},
    )
    await session.commit()
    session.expire_all()


async def _clear_all_orders(session: AsyncSession) -> None:
    """What a revision written before the `order` field existed looks like."""
    await session.execute(
        text(
            """
            UPDATE "column" SET "order" = NULL
            WHERE node_revision_id IN (
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
async def test_removed_column_is_a_major_bump(
    client_with_roads: AsyncClient,
    session: AsyncSession,
):
    """
    A column the query no longer produces is breaking for anything that referenced
    it, and must not survive onto the new revision advertising a value the node
    cannot supply.
    """
    await _create_node(client_with_roads)
    # A column stored on the revision that the query does not produce -- what a
    # node whose query stopped selecting a field looks like from the validator's
    # side. Cloned from an existing row so every non-null column is populated.
    await session.execute(
        text(
            """
            INSERT INTO "column" (name, type, node_revision_id, "order")
            SELECT 'ghost', c.type, c.node_revision_id, 99
            FROM "column" c
            JOIN noderevision nr ON nr.id = c.node_revision_id
            WHERE nr.name = :name AND c.name = 'price'
            """,
        ),
        {"name": NODE},
    )
    await session.commit()
    session.expire_all()

    await _revalidate(client_with_roads)

    assert await _node(client_with_roads) == ("v2.0", COLUMNS)


@pytest.mark.asyncio
async def test_backfilled_column_order_creates_no_revision(
    client_with_roads: AsyncClient,
    session: AsyncSession,
):
    """
    A missing `order` is DJ filling in metadata it should already have stored. No
    query changed and no name, type or value moved, so there is no new revision --
    and so nothing downstream is rebuilt for it. The value is written to the
    current revision in place.
    """
    await _create_node(client_with_roads)
    await _clear_all_orders(session)
    assert await _stored_order(session) == [("price", None), ("repair_order_id", None)]

    await _revalidate(client_with_roads)

    assert await _node(client_with_roads) == ("v1.0", COLUMNS)
    assert await _stored_order(session) == [("price", 1), ("repair_order_id", 0)]


@pytest.mark.asyncio
async def test_backfilled_column_order_still_writes_a_history_event(
    client_with_roads: AsyncClient,
    session: AsyncSession,
):
    """
    Losing the version bump must not mean losing the record that DJ touched the
    row: a column that changes position needs an explanation somewhere.
    """
    await _create_node(client_with_roads)
    await _clear_all_orders(session)

    await _revalidate(client_with_roads)

    assert await _revalidate_events(client_with_roads) == [
        {
            "version": "v1.0",
            "reason": "column order backfill",
            "order_fixed": ["repair_order_id", "price"],
        },
    ]


@pytest.mark.asyncio
async def test_backfill_leaves_orders_that_are_already_set(
    client_with_roads: AsyncClient,
    session: AsyncSession,
):
    """
    Only the missing values are filled. A partially-ordered revision is the case
    where the backfill genuinely moves a column: `price` with no order sorts behind
    every ordered column, and filling it in puts it back at the position the query
    always projected it in.
    """
    await _create_node(client_with_roads)
    await _edit_stored_columns(session, 'UPDATE "column" SET "order" = NULL')
    assert await _stored_order(session) == [("price", None), ("repair_order_id", 0)]

    await _revalidate(client_with_roads)

    assert await _node(client_with_roads) == ("v1.0", COLUMNS)
    assert await _stored_order(session) == [("price", 1), ("repair_order_id", 0)]
