"""
Dependency for getting the postgres pool and running backend DB queries
"""

# pylint: disable=too-many-arguments
from datetime import datetime
from typing import List
from uuid import UUID

from fastapi import Request
from psycopg import sql
from psycopg_pool import AsyncConnectionPool

from djqs.exceptions import DJDatabaseError


async def get_postgres_pool(request: Request) -> AsyncConnectionPool:
    """
    Get the postgres pool from the app instance
    """
    app = request.app
    return app.state.pool


class DBQuery:
    """
    Metadata DB queries using the psycopg composition utility
    """

    def __init__(self):
        self._reset()

    def _reset(self):
        self.selects: List = []
        self.inserts: List = []

    def get_query(self, query_id: UUID):
        """
        Get metadata about a query
        """
        self.selects.append(
            sql.SQL(
                """
                SELECT id, catalog_name, engine_name, engine_version, submitted_query,
                       async_, executed_query, scheduled, started, finished, state, progress
                FROM query
                WHERE id = {query_id}
                """,
            ).format(query_id=sql.Literal(query_id)),
        )
        return self

    def save_query(
        self,
        query_id: UUID,
        catalog_name: str = "",
        engine_name: str = "",
        engine_version: str = "",
        submitted_query: str = "",
        async_: bool = False,
        state: str = "",
        progress: float = 0.0,
        executed_query: str = None,
        scheduled: datetime = None,
        started: datetime = None,
        finished: datetime = None,
    ):
        """
        Save metadata about a query
        """
        self.inserts.append(
            sql.SQL(
                """
                INSERT INTO query (id, catalog_name, engine_name, engine_version,
                                   submitted_query, async_, executed_query, scheduled,
                                   started, finished, state, progress)
                VALUES ({query_id}, {catalog_name}, {engine_name}, {engine_version},
                        {submitted_query}, {async_}, {executed_query}, {scheduled},
                        {started}, {finished}, {state}, {progress})
                ON CONFLICT (id) DO UPDATE SET
                    catalog_name = EXCLUDED.catalog_name,
                    engine_name = EXCLUDED.engine_name,
                    engine_version = EXCLUDED.engine_version,
                    submitted_query = EXCLUDED.submitted_query,
                    async_ = EXCLUDED.async_,
                    executed_query = EXCLUDED.executed_query,
                    scheduled = EXCLUDED.scheduled,
                    started = EXCLUDED.started,
                    finished = EXCLUDED.finished,
                    state = EXCLUDED.state,
                    progress = EXCLUDED.progress
                RETURNING *
                """,
            ).format(
                query_id=sql.Literal(query_id),
                catalog_name=sql.Literal(catalog_name),
                engine_name=sql.Literal(engine_name),
                engine_version=sql.Literal(engine_version),
                submitted_query=sql.Literal(submitted_query),
                async_=sql.Literal(async_),
                executed_query=sql.Literal(executed_query),
                scheduled=sql.Literal(scheduled),
                started=sql.Literal(started),
                finished=sql.Literal(finished),
                state=sql.Literal(state),
                progress=sql.Literal(progress),
            ),
        )
        return self

    async def execute(self, conn):
        """
        Submit all statements to the backend DB, multiple statements are submitted together
        """
        if not self.selects and not self.inserts:  # pragma: no cover
            return

        async with conn.cursor() as cur:
            results = []
            if len(self.inserts) > 1:  # pragma: no cover
                async with conn.transaction():
                    for statement in self.inserts:
                        await cur.execute(statement)
                results.append(await cur.fetchall())

            if len(self.inserts) == 1:
                await cur.execute(self.inserts[0])
                if cur.rowcount == 0:  # pragma: no cover
                    raise DJDatabaseError(
                        "Insert statement resulted in no records being inserted",
                    )
                results.append((await cur.fetchone()))
            if self.selects:
                for statement in self.selects:
                    await cur.execute(statement)
                    results.append(await cur.fetchall())
            await conn.commit()
            self._reset()
            return results
