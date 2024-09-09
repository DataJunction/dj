"""
Dependency for getting the postgres pool and running backend DB queries
"""
from typing import List

from fastapi import Request, App
from psycopg import sql
from psycopg_pool import AsyncConnectionPool

from djqs.exceptions import DJDatabaseError

async def get_postgres_pool(request: Request) -> AsyncConnectionPool:
    """
    Get the postgres pool from the app instance
    """
    app = request.app
    return app.router.lifespan_context

class DBQuery:
    """
    Metadata DB queries using the psycopg composition utility
    """

    def __init__(self):
        self._reset()

    def _reset(self):
        self.selects: List = []
        self.inserts: List = []

    def get_query(self, foo):
        """
        Get metadata about a query
        """
        self.selects.append(
            sql.SQL(
                """
                SELECT ...
                """,
            ).format(foo=foo),
        )
        return self
    
    def save_query(self, foo):
        """
        Save metadata about a query
        """
        self.inserts.append(
            sql.SQL(
                """
                SELECT ...
                """,
            ).format(foo=foo)
        )
        return self
    
    async def execute(self, conn, fetch_results=True):
        """
        Submit all statements to the backend DB, multiple statements are submitted together
        """
        if not self.selects and not self.inserts:
            return

        async with conn.cursor() as cur:
            results = []
            if len(self.inserts) > 1:
                async with conn.transaction():
                    for statement in self.inserts:
                        await cur.execute(statement)
                results.append(await cur.fetchall())

            if len(self.inserts) == 1:
                await cur.execute(self.inserts[0])
                if cur.rowcount == 0:
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
