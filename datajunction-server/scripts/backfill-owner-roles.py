"""
Backfill owner roles for existing namespaces based on History.

This script creates owner roles for namespaces that existed before
auto-role creation was implemented. It looks up the original creator from
the History table and creates the appropriate owner role.

Note: Node-level owner roles are NOT auto-created. Fine-grained node
permissions should be set up manually when needed. Namespace ownership
provides implicit MANAGE access to all nodes within that namespace.

Usage:
    # Dry run (see what would be created)
    python scripts/backfill-owner-roles.py --dry-run

    # Backfill all namespaces
    python scripts/backfill-owner-roles.py

    # Backfill specific namespace pattern
    python scripts/backfill-owner-roles.py --pattern "finance.*"
"""

import argparse
import asyncio
from typing import Optional

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from datajunction_server.database.history import History
from datajunction_server.database.rbac import Role, RoleAssignment, RoleScope
from datajunction_server.database.user import User
from datajunction_server.internal.history import ActivityType, EntityType
from datajunction_server.models.access import ResourceAction, ResourceType
from datajunction_server.utils import get_settings

settings = get_settings()


async def get_namespace_creators(
    session: AsyncSession,
    pattern: Optional[str] = None,
) -> list[tuple[str, str]]:
    """
    Get namespace names and their creators from History.

    Returns list of (namespace, username) tuples.
    """
    query = (
        sa.select(History.entity_name, History.user)
        .where(
            History.entity_type == EntityType.NAMESPACE,
            History.activity_type == ActivityType.CREATE,
            History.entity_name.isnot(None),
            History.user.isnot(None),
        )
        .distinct(History.entity_name)
        .order_by(History.entity_name, History.created_at)
    )

    if pattern:
        if pattern.endswith("*"):
            prefix = pattern.rstrip("*").rstrip(".")
            query = query.where(
                sa.or_(
                    History.entity_name == prefix,
                    History.entity_name.like(f"{prefix}.%"),
                ),
            )
        else:
            query = query.where(History.entity_name == pattern)

    result = await session.execute(query)
    return [(row.entity_name, row.user) for row in result.all()]


async def role_exists(session: AsyncSession, role_name: str) -> bool:
    """Check if a role already exists."""
    result = await session.execute(
        sa.select(Role.id).where(Role.name == role_name, Role.deleted_at.is_(None)),
    )
    return result.scalar_one_or_none() is not None


async def get_user_by_username(
    session: AsyncSession,
    username: str,
) -> Optional[User]:
    """Get user by username."""
    result = await session.execute(sa.select(User).where(User.username == username))
    return result.scalar_one_or_none()


async def create_owner_role(
    session: AsyncSession,
    namespace: str,
    owner: User,
    dry_run: bool = False,
) -> bool:
    """
    Create an owner role for a namespace.

    Returns True if role was created, False if it already exists.
    """
    role_name = f"{namespace}-owner"

    if await role_exists(session, role_name):
        return False

    if dry_run:
        print(
            f"  [DRY RUN] Would create role '{role_name}' for user '{owner.username}'",
        )
        return True

    # Create the role
    role = Role(
        name=role_name,
        description=f"Owner role for namespace {namespace}",
        created_by_id=owner.id,
    )
    session.add(role)
    await session.flush()

    # Add MANAGE scope on the namespace
    scope = RoleScope(
        role_id=role.id,
        action=ResourceAction.MANAGE,
        scope_type=ResourceType.NAMESPACE,
        scope_value=namespace,
    )
    session.add(scope)

    # Assign to owner
    assignment = RoleAssignment(
        principal_id=owner.id,
        role_id=role.id,
        granted_by_id=owner.id,
    )
    session.add(assignment)

    print(f"  Created role '{role_name}' assigned to '{owner.username}'")
    return True


async def backfill_namespace_roles(
    session: AsyncSession,
    pattern: Optional[str] = None,
    dry_run: bool = False,
) -> tuple[int, int]:
    """
    Backfill owner roles for namespaces.

    Returns (created_count, skipped_count).
    """
    print("\n=== Backfilling Namespace Owner Roles ===")

    creators = await get_namespace_creators(session, pattern)
    print(f"Found {len(creators)} namespace creation events in History")

    created = 0
    skipped = 0

    for namespace, username in creators:
        user = await get_user_by_username(session, username)
        if not user:
            print(f"  Skipping '{namespace}': user '{username}' not found")
            skipped += 1
            continue

        if await create_owner_role(session, namespace, user, dry_run):
            created += 1
        else:
            print(f"  Skipping '{namespace}': role already exists")
            skipped += 1

    return created, skipped


async def main(
    dry_run: bool = False,
    pattern: Optional[str] = None,
):
    """Main backfill function."""
    print("=" * 60)
    print("RBAC Namespace Owner Role Backfill Script")
    print("=" * 60)

    if dry_run:
        print("\n*** DRY RUN MODE - No changes will be made ***\n")

    engine = create_async_engine(settings.writer_db.uri)
    async_session = sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

    async with async_session() as session:
        async with session.begin():
            created, skipped = await backfill_namespace_roles(
                session,
                pattern,
                dry_run,
            )

            if not dry_run:
                await session.commit()

            print("\n" + "=" * 60)
            print("Summary")
            print("=" * 60)
            print(f"Namespaces: {created} roles created, {skipped} skipped")

            if dry_run:
                print("\n*** DRY RUN - No changes were made ***")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Backfill owner roles for existing namespaces",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be created without making changes",
    )
    parser.add_argument(
        "--pattern",
        type=str,
        help="Only backfill namespaces matching pattern (e.g., 'finance.*')",
    )

    args = parser.parse_args()

    asyncio.run(
        main(
            dry_run=args.dry_run,
            pattern=args.pattern,
        ),
    )
