"""Tests for tag type claims — namespace ownership of a tag vocabulary."""

from unittest.mock import MagicMock

import pytest
from sqlalchemy import select

from datajunction_server.database.namespace import NodeNamespace
from datajunction_server.database.tag_type_claim import TagTypeClaim
from datajunction_server.errors import (
    DJAlreadyExistsException,
    DJInvalidDeploymentConfig,
)
from datajunction_server.internal.deployment.orchestrator import DeploymentOrchestrator
from datajunction_server.internal.deployment.utils import DeploymentContext
from datajunction_server.internal.tag_type_claims import (
    resolve_claim_namespace,
    upsert_tag_type_claims,
)
from datajunction_server.models.deployment import (
    DeploymentSpec,
    TagSpec,
    TagTypeClaimSpec,
)


async def claim_for(session, tag_type: str) -> TagTypeClaim | None:
    """The stored claim for a tag type, if any."""
    return (
        await session.execute(
            select(TagTypeClaim).where(TagTypeClaim.tag_type == tag_type),
        )
    ).scalar_one_or_none()


async def add_namespace(session, namespace: str, **kwargs) -> None:
    """Register a namespace so claim resolution can walk it."""
    session.add(NodeNamespace(namespace=namespace, **kwargs))
    await session.flush()


def orchestrator_for(spec: DeploymentSpec, session, current_user):
    """An orchestrator wired to a deployment spec, as the deploy API builds one."""
    context = MagicMock(spec=DeploymentContext)
    context.current_user = current_user
    return DeploymentOrchestrator(
        deployment_spec=spec,
        deployment_id=f"{spec.namespace}-claims",
        session=session,
        context=context,
    )


@pytest.mark.asyncio
async def test_a_manifest_records_a_claim(session, current_user):
    """A declared tag type is claimed for the deploying namespace."""
    spec = DeploymentSpec(
        namespace="taxonomy",
        nodes=[],
        managed_tag_types=[TagTypeClaimSpec(tag_type="domain")],
    )
    await orchestrator_for(spec, session, current_user)._setup_deployment_resources()

    claim = await claim_for(session, "domain")
    assert claim.namespace == "taxonomy"
    assert claim.created_by_id == current_user.id


@pytest.mark.asyncio
async def test_a_second_namespace_cannot_claim_the_same_type(session, current_user):
    """The conflict names the type and the namespace already holding it."""
    await upsert_tag_type_claims(
        session,
        "taxonomy",
        [TagTypeClaimSpec(tag_type="topic", namespace="taxonomy")],
        current_user_id=current_user.id,
    )
    with pytest.raises(DJAlreadyExistsException) as exc_info:
        await upsert_tag_type_claims(
            session,
            "analytics",
            [TagTypeClaimSpec(tag_type="topic", namespace="analytics")],
            current_user_id=current_user.id,
        )
    assert "`topic`" in str(exc_info.value)
    assert "`taxonomy`" in str(exc_info.value)
    assert (await claim_for(session, "topic")).namespace == "taxonomy"


@pytest.mark.asyncio
async def test_dropping_the_declaration_releases_the_claim(session, current_user):
    """Otherwise a vocabulary could never move to another repo."""
    await upsert_tag_type_claims(
        session,
        "taxonomy",
        [TagTypeClaimSpec(tag_type="topic", namespace="taxonomy")],
        current_user_id=current_user.id,
    )
    await upsert_tag_type_claims(
        session,
        "taxonomy",
        [],
        current_user_id=current_user.id,
    )
    assert await claim_for(session, "topic") is None

    await upsert_tag_type_claims(
        session,
        "analytics",
        [TagTypeClaimSpec(tag_type="topic", namespace="analytics")],
        current_user_id=current_user.id,
    )
    assert (await claim_for(session, "topic")).namespace == "analytics"


@pytest.mark.asyncio
async def test_redeclaring_a_claim_keeps_it(session, current_user):
    """A re-deploy of the same manifest updates the claim in place."""
    specs = [
        TagTypeClaimSpec(tag_type="domain", namespace="taxonomy"),
        TagTypeClaimSpec(tag_type="topic", namespace="taxonomy"),
    ]
    await upsert_tag_type_claims(
        session,
        "taxonomy",
        specs,
        current_user_id=current_user.id,
    )
    claim_id = (await claim_for(session, "domain")).id
    await upsert_tag_type_claims(
        session,
        "taxonomy",
        specs,
        current_user_id=current_user.id,
    )
    assert (await claim_for(session, "domain")).id == claim_id

    # Dropping one declaration releases only that type.
    await upsert_tag_type_claims(
        session,
        "taxonomy",
        specs[:1],
        current_user_id=current_user.id,
    )
    assert (await claim_for(session, "domain")).id == claim_id
    assert await claim_for(session, "topic") is None


@pytest.mark.asyncio
async def test_a_claim_may_be_scoped_to_a_sub_namespace(session, current_user):
    """A narrower rollout stores the sub-namespace, not the deploying root."""
    await add_namespace(session, "taxonomy")
    await add_namespace(session, "taxonomy.curated")
    await upsert_tag_type_claims(
        session,
        "taxonomy",
        [TagTypeClaimSpec(tag_type="domain", namespace="taxonomy.curated")],
        current_user_id=current_user.id,
    )
    assert (await claim_for(session, "domain")).namespace == "taxonomy.curated"


@pytest.mark.asyncio
async def test_a_sideways_claim_is_rejected():
    """The spec validator refuses a namespace the deployment does not contain."""
    with pytest.raises(DJInvalidDeploymentConfig) as exc_info:
        DeploymentSpec(
            namespace="taxonomy",
            nodes=[],
            managed_tag_types=[
                TagTypeClaimSpec(tag_type="domain", namespace="analytics"),
            ],
        )
    assert "not 'taxonomy' or beneath it" in str(exc_info.value)


@pytest.mark.asyncio
async def test_a_branch_namespace_claims_for_its_parent(session, current_user):
    """A branch's claim is recorded against its parent, so deleting the branch
    does not drop the vocabulary."""
    await add_namespace(session, "taxonomy")
    await add_namespace(session, "taxonomy.somebranch", parent_namespace="taxonomy")
    await add_namespace(
        session,
        "taxonomy.somebranch.curated",
        parent_namespace=None,
    )

    assert await resolve_claim_namespace(session, "taxonomy.somebranch") == "taxonomy"
    await upsert_tag_type_claims(
        session,
        "taxonomy.somebranch",
        [TagTypeClaimSpec(tag_type="domain", namespace="taxonomy.somebranch")],
        current_user_id=current_user.id,
    )
    assert (await claim_for(session, "domain")).namespace == "taxonomy"

    # A namespace under the branch resolves through the branch to the same owner.
    assert (
        await resolve_claim_namespace(session, "taxonomy.somebranch.curated")
        == "taxonomy"
    )


@pytest.mark.asyncio
async def test_resolution_stops_at_a_governed_boundary(session, current_user):
    """A governed boundary is its own authority, so the walk does not escape it."""
    await add_namespace(session, "governed", is_governed_boundary=True)
    await add_namespace(
        session,
        "governed.inner",
        parent_namespace="governed",
        is_governed_boundary=True,
    )
    assert await resolve_claim_namespace(session, "governed.inner") == "governed.inner"


@pytest.mark.asyncio
async def test_resolution_survives_a_parent_cycle(session):
    """A cycle in parent links must not spin: resolution stops once it repeats."""
    await add_namespace(session, "loop_a")
    await add_namespace(session, "loop_b", parent_namespace="loop_a")
    row = await NodeNamespace.get(session, "loop_a")
    row.parent_namespace = "loop_b"
    await session.flush()

    assert await resolve_claim_namespace(session, "loop_b") in {"loop_a", "loop_b"}


@pytest.mark.asyncio
async def test_a_deploy_cannot_create_a_tag_of_a_claimed_type(session, current_user):
    """Only the claiming namespace's deployment may add to its vocabulary."""
    await upsert_tag_type_claims(
        session,
        "taxonomy",
        [TagTypeClaimSpec(tag_type="domain", namespace="taxonomy")],
        current_user_id=current_user.id,
    )
    spec = DeploymentSpec(
        namespace="analytics",
        nodes=[],
        tags=[TagSpec(name="payments", tag_type="domain")],
    )
    orchestrator = orchestrator_for(spec, session, current_user)
    await orchestrator._setup_deployment_resources()
    assert "claimed by namespace `taxonomy`" in orchestrator.errors[0].message

    # The claiming namespace's own deploy creates tags of the type.
    spec = DeploymentSpec(
        namespace="taxonomy",
        nodes=[],
        tags=[TagSpec(name="billing", tag_type="domain")],
    )
    orchestrator = orchestrator_for(spec, session, current_user)
    await orchestrator._setup_deployment_resources()
    assert orchestrator.errors == []
    assert "billing" in orchestrator.registry.tags


@pytest.mark.asyncio
async def test_a_claim_does_not_invalidate_existing_tags(session, current_user):
    """Recording a claim is not a migration: tags already deployed still update."""
    spec = DeploymentSpec(
        namespace="analytics",
        nodes=[],
        tags=[TagSpec(name="payments", tag_type="domain", description="before")],
    )
    await orchestrator_for(spec, session, current_user)._setup_deployment_resources()
    await upsert_tag_type_claims(
        session,
        "taxonomy",
        [TagTypeClaimSpec(tag_type="domain", namespace="taxonomy")],
        current_user_id=current_user.id,
    )

    spec = DeploymentSpec(
        namespace="analytics",
        nodes=[],
        tags=[TagSpec(name="payments", tag_type="domain", description="after")],
    )
    orchestrator = orchestrator_for(spec, session, current_user)
    await orchestrator._setup_deployment_resources()
    assert orchestrator.errors == []
    assert orchestrator.registry.tags["payments"].description == "after"


@pytest.mark.asyncio
async def test_a_manifest_without_claims_touches_nothing(session, current_user):
    """Omitting the section leaves existing claims alone."""
    await upsert_tag_type_claims(
        session,
        "taxonomy",
        [TagTypeClaimSpec(tag_type="domain", namespace="taxonomy")],
        current_user_id=current_user.id,
    )
    spec = DeploymentSpec(namespace="taxonomy", nodes=[])
    await orchestrator_for(spec, session, current_user)._setup_deployment_resources()
    assert (await claim_for(session, "domain")).namespace == "taxonomy"
