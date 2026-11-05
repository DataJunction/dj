"""Transaction-scoped advisory locks."""

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

_NAMESPACE_BOUNDARY_LOCK_ID = 0x444A4E53424E4459


async def advisory_xact_lock(
    session: AsyncSession,
    key: str,
    shared: bool = False,
) -> None:
    """Hold a Postgres advisory lock on ``key`` until the transaction ends."""
    if session.get_bind().dialect.name != "postgresql":
        return
    lock = "pg_advisory_xact_lock_shared" if shared else "pg_advisory_xact_lock"
    await session.execute(text(f"SELECT {lock}(hashtext(:key))"), {"key": key})


async def lock_namespace_boundary_lifecycle(session: AsyncSession) -> None:
    """Serialize namespace boundary creation."""
    if session.get_bind().dialect.name == "postgresql":
        await session.execute(
            text("SELECT pg_advisory_xact_lock(:lock_id)"),
            {"lock_id": _NAMESPACE_BOUNDARY_LOCK_ID},
        )
