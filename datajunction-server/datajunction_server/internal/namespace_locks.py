"""Transaction locks for namespace policy lifecycle changes."""

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

_NAMESPACE_BOUNDARY_LOCK_ID = 0x444A4E53424E4459


async def lock_namespace_boundary_lifecycle(session: AsyncSession) -> None:
    """Serialize boundary provisioning and destructive namespace lifecycle changes."""
    if session.get_bind().dialect.name == "postgresql":
        await session.execute(
            text("SELECT pg_advisory_xact_lock(:lock_id)"),
            {"lock_id": _NAMESPACE_BOUNDARY_LOCK_ID},
        )
