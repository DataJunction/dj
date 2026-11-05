"""Overlapping deployments queue behind each other instead of colliding."""

import asyncio
import uuid
from unittest.mock import MagicMock

import pytest
from sqlalchemy import func, select, text

from datajunction_server.database.custom_metadata_schema import CustomMetadataSchema
from datajunction_server.database.tag import Tag
from datajunction_server.internal.custom_metadata import upsert_schema_specs
from datajunction_server.internal.deployment.orchestrator import DeploymentOrchestrator
from datajunction_server.internal.deployment.utils import DeploymentContext
from datajunction_server.models.deployment import (
    CustomMetadataSchemaSpec,
    DeploymentSpec,
    TagSpec,
)
from tests.conftest import patch_session_contexts

NAMESPACE_LOCK = "SELECT pg_try_advisory_xact_lock(hashtext(:key))"
SHARED_LOCK = "SELECT pg_advisory_xact_lock_shared(hashtext(:key))"
EXCLUSIVE_LOCK = "SELECT pg_advisory_xact_lock(hashtext(:key))"


def _orchestrator(session, current_user, namespace, dry_run=False, tags=None):
    context = MagicMock(spec=DeploymentContext)
    context.current_user = current_user
    return DeploymentOrchestrator(
        deployment_spec=DeploymentSpec(namespace=namespace, nodes=[], tags=tags or []),
        deployment_id=str(uuid.uuid4()),
        session=session,
        context=context,
        dry_run=dry_run,
    )


@pytest.mark.asyncio
async def test_concurrent_schema_writes_do_not_collide(
    session,
    session_factory,
    current_user,
):
    """A second deployment declaring the same schema waits for the first.

    Without the lock it misses the first's uncommitted row, inserts its own, and
    fails on `uq_custommetadataschema_key_scope` once the first commits.
    """
    specs = [CustomMetadataSchemaSpec(key="grain", json_schema={"type": "string"})]
    other = await session_factory()
    async with other:
        await upsert_schema_specs(
            session,
            namespace="team_a",
            specs=specs,
            current_user_id=current_user.id,
        )
        await session.flush()  # the first deployment's row, still uncommitted

        async def second_deployment():
            await upsert_schema_specs(
                other,
                namespace="team_a",
                specs=specs,
                current_user_id=current_user.id,
            )
            await other.commit()

        task = asyncio.create_task(second_deployment())
        await asyncio.sleep(1)  # let it reach the lock, or the unique index
        assert not task.done()
        await session.commit()
        await asyncio.wait_for(task, timeout=30)

    active = await session.scalar(
        select(func.count())
        .select_from(CustomMetadataSchema)
        .where(
            CustomMetadataSchema.key == "grain",
            CustomMetadataSchema.deactivated_at.is_(None),
        ),
    )
    assert active == 1


@pytest.mark.asyncio
async def test_deployments_to_one_namespace_run_one_at_a_time(
    session,
    session_factory,
    current_user,
):
    """A deployment waits while another holds its namespace's lock."""
    holder = await session_factory()
    async with holder:
        await holder.execute(text(NAMESPACE_LOCK), {"key": "dj:deploy:team_a"})

        with patch_session_contexts(session_factory):
            deployment = asyncio.create_task(
                _orchestrator(session, current_user, "team_a").execute(),
            )
            await asyncio.sleep(1)
            assert not deployment.done()

            await holder.rollback()  # releases the lock
            await asyncio.wait_for(deployment, timeout=30)


@pytest.mark.asyncio
async def test_deployments_to_other_namespaces_do_not_wait(
    session,
    session_factory,
    current_user,
):
    """The lock is per namespace, so unrelated deployments are not slowed."""
    holder = await session_factory()
    async with holder:
        await holder.execute(text(NAMESPACE_LOCK), {"key": "dj:deploy:team_a"})
        with patch_session_contexts(session_factory):
            await asyncio.wait_for(
                _orchestrator(session, current_user, "team_b").execute(),
                timeout=30,
            )
        await holder.rollback()


@pytest.mark.asyncio
async def test_child_deployment_waits_for_a_parent_deployment(
    session,
    session_factory,
    current_user,
):
    """A deployment into team.sub waits while one into team is running."""
    holder = await session_factory()
    async with holder:
        await holder.execute(text(EXCLUSIVE_LOCK), {"key": "dj:deploy:team"})

        with patch_session_contexts(session_factory):
            deployment = asyncio.create_task(
                _orchestrator(session, current_user, "team.sub").execute(),
            )
            await asyncio.sleep(1)
            assert not deployment.done()

            await holder.rollback()
            await asyncio.wait_for(deployment, timeout=30)


@pytest.mark.asyncio
async def test_parent_deployment_waits_for_a_child_deployment(
    session,
    session_factory,
    current_user,
):
    """A deployment into team waits while one into team.sub is running."""
    holder = await session_factory()
    async with holder:
        # What a deployment into team.sub holds: team shared, team.sub exclusive.
        await holder.execute(text(SHARED_LOCK), {"key": "dj:deploy:team"})
        await holder.execute(text(EXCLUSIVE_LOCK), {"key": "dj:deploy:team.sub"})

        with patch_session_contexts(session_factory):
            deployment = asyncio.create_task(
                _orchestrator(session, current_user, "team").execute(),
            )
            await asyncio.sleep(1)
            assert not deployment.done()

            await holder.rollback()
            await asyncio.wait_for(deployment, timeout=30)


@pytest.mark.asyncio
async def test_sibling_deployments_do_not_wait(
    session,
    session_factory,
    current_user,
):
    """Deployments into team.a and team.b share team but not each other's lock."""
    holder = await session_factory()
    async with holder:
        await holder.execute(text(SHARED_LOCK), {"key": "dj:deploy:team"})
        await holder.execute(text(EXCLUSIVE_LOCK), {"key": "dj:deploy:team.a"})

        with patch_session_contexts(session_factory):
            await asyncio.wait_for(
                _orchestrator(session, current_user, "team.b").execute(),
                timeout=30,
            )
        await holder.rollback()


@pytest.mark.asyncio
async def test_dry_runs_do_not_wait_for_each_other(
    session,
    session_factory,
    current_user,
):
    """A dry run only needs the namespace lock shared, so previews run side by side."""
    holder = await session_factory()
    async with holder:
        await holder.execute(text(SHARED_LOCK), {"key": "dj:deploy:team_a"})
        with patch_session_contexts(session_factory):
            await asyncio.wait_for(
                _orchestrator(session, current_user, "team_a", dry_run=True).execute(),
                timeout=30,
            )
        await holder.rollback()


@pytest.mark.asyncio
async def test_real_deployment_waits_for_a_dry_run(
    session,
    session_factory,
    current_user,
):
    """A real deployment takes the namespace lock exclusively, so a dry run holds it off."""
    holder = await session_factory()
    async with holder:
        await holder.execute(text(SHARED_LOCK), {"key": "dj:deploy:team_a"})

        with patch_session_contexts(session_factory):
            deployment = asyncio.create_task(
                _orchestrator(session, current_user, "team_a").execute(),
            )
            await asyncio.sleep(1)
            assert not deployment.done()

            await holder.rollback()
            await asyncio.wait_for(deployment, timeout=30)


@pytest.mark.asyncio
async def test_concurrent_tag_creation_does_not_collide(
    session,
    session_factory,
    current_user,
):
    """A deployment creating a tag another one just created skips it and moves on.

    Without the conflict handling it inserts a second row for the name and fails on
    the unique constraint once the first deployment commits.
    """
    other = await session_factory()
    async with other:
        other.add(
            Tag(
                name="shared_tag",
                tag_type="general",
                description="",
                display_name="Shared tag",
                tag_metadata={},
                created_by_id=current_user.id,
            ),
        )
        await other.flush()  # the other deployment's tag, still uncommitted

        with patch_session_contexts(session_factory):
            deployment = asyncio.create_task(
                _orchestrator(
                    session,
                    current_user,
                    "team_a",
                    tags=[TagSpec(name="shared_tag", tag_type="general")],
                ).execute(),
            )
            await asyncio.sleep(1)  # it waits on the unique index
            assert not deployment.done()

            await other.commit()
            await asyncio.wait_for(deployment, timeout=30)

    count = await session.scalar(
        select(func.count()).select_from(Tag).where(Tag.name == "shared_tag"),
    )
    assert count == 1


@pytest.mark.asyncio
async def test_losing_tag_creator_still_applies_its_declared_settings(
    session,
    session_factory,
    current_user,
):
    """The deployment that loses the tag-creation race updates it to its manifest."""
    other = await session_factory()
    async with other:
        other.add(
            Tag(
                name="raced_tag",
                tag_type="general",
                description="winner",
                display_name="Raced tag",
                tag_metadata={},
                created_by_id=current_user.id,
            ),
        )
        await other.flush()

        with patch_session_contexts(session_factory):
            deployment = asyncio.create_task(
                _orchestrator(
                    session,
                    current_user,
                    "team_a",
                    tags=[
                        TagSpec(
                            name="raced_tag",
                            tag_type="general",
                            description="loser",
                        ),
                    ],
                ).execute(),
            )
            await asyncio.sleep(1)
            await other.commit()
            await asyncio.wait_for(deployment, timeout=30)

    session.expire_all()
    description = await session.scalar(
        select(Tag.description).where(Tag.name == "raced_tag"),
    )
    assert description == "loser"


@pytest.mark.asyncio
async def test_new_tags_are_plainly_added_off_postgres(current_user):
    """Without Postgres's ON CONFLICT, new tags are just added to the session."""
    session = MagicMock()
    session.get_bind.return_value.dialect.name = "sqlite"
    orchestrator = _orchestrator(session, current_user, "team_a")
    tag = Tag(name="t", tag_type="general", description="", display_name="T")

    created = await orchestrator._insert_new_tags({"t": tag}, tag_options=[])

    assert created == ["t"]
    session.add_all.assert_called_once()


@pytest.mark.asyncio
async def test_schema_rows_are_written_in_lock_order(
    session,
    current_user,
    monkeypatch,
):
    """Specs listed as [b, a] still write a before b, so shared holders cannot cross."""
    from datajunction_server.internal import custom_metadata

    written = []
    real = custom_metadata.upsert_schema_row

    async def spy(session, *, key, **kwargs):
        written.append(key)
        return await real(session, key=key, **kwargs)

    monkeypatch.setattr(custom_metadata, "upsert_schema_row", spy)
    await upsert_schema_specs(
        session,
        namespace="team_a",
        specs=[
            CustomMetadataSchemaSpec(key="b", json_schema={"type": "string"}),
            CustomMetadataSchemaSpec(key="a", json_schema={"type": "string"}),
        ],
        current_user_id=current_user.id,
        shared_lock=True,
    )
    assert written == ["a", "b"]


@pytest.mark.asyncio
@pytest.mark.parametrize("dry_run", [False, True])
async def test_deployment_releases_its_namespace_lock(
    session,
    session_factory,
    current_user,
    dry_run,
):
    """A dry run (SAVEPOINT rollback) and a wet run (commit) both free the lock."""
    with patch_session_contexts(session_factory):
        await _orchestrator(
            session,
            current_user,
            "team_a",
            dry_run=dry_run,
        ).execute()

    other = await session_factory()
    async with other:
        got_lock = await other.scalar(
            text(NAMESPACE_LOCK),
            {"key": "dj:deploy:team_a"},
        )
    assert got_lock is True
