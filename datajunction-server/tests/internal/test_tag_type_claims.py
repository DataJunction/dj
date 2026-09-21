"""Tests for tag type claims — namespace ownership of a tag vocabulary."""

import logging
from datetime import UTC, datetime
from unittest.mock import MagicMock

import pytest
from sqlalchemy import select

from datajunction_server.database.namespace import NodeNamespace
from datajunction_server.database.node import Node
from datajunction_server.database.tag import Tag, TagNodeRelationship
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
    DeploymentResult,
    DeploymentSpec,
    MetricSpec,
    TagSpec,
    TagTypeClaimSpec,
)
from datajunction_server.models.node_type import NodeType

ORCHESTRATOR_LOGGER = "datajunction_server.internal.deployment.orchestrator"


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


async def add_tag(session, current_user, name: str, tag_type: str) -> Tag:
    """A tag already stored on the server, as an earlier deploy would leave it."""
    tag = Tag(
        name=name,
        tag_type=tag_type,
        display_name=name,
        created_by_id=current_user.id,
    )
    session.add(tag)
    await session.flush()
    return tag


async def attach_tag(session, current_user, tag: Tag, node_name: str) -> Node:
    """A node in some other namespace carrying the tag."""
    node = Node(
        name=node_name,
        type=NodeType.METRIC,
        namespace=node_name.rsplit(".", 1)[0],
        current_version="v1.0",
        created_by_id=current_user.id,
    )
    session.add(node)
    await session.flush()
    session.add(TagNodeRelationship(tag_id=tag.id, node_id=node.id))
    await session.flush()
    return node


def results_for(results, name: str) -> list:
    """The deployment results reported against a tag."""
    return [result for result in results if result.name == name]


@pytest.mark.asyncio
async def test_reconcile_deletes_a_tag_the_manifest_dropped(session, current_user):
    """The manifest is the full list of tags of a type it claims."""
    await add_tag(session, current_user, "payments", "domain")
    await add_tag(session, current_user, "retired", "domain")
    spec = DeploymentSpec(
        namespace="taxonomy",
        nodes=[],
        tags=[TagSpec(name="payments", tag_type="domain")],
        managed_tag_types=[TagTypeClaimSpec(tag_type="domain")],
    )
    orchestrator = orchestrator_for(spec, session, current_user)
    await orchestrator._setup_deployment_resources()
    results = await orchestrator._reconcile_claimed_tags()

    remaining = await Tag.find_tags(session, tag_types=["domain"])
    assert [tag.name for tag in remaining] == ["payments"]
    assert [
        (result.status, result.operation) for result in results_for(results, "retired")
    ] == [(DeploymentResult.Status.SUCCESS, DeploymentResult.Operation.DELETE)]


@pytest.mark.asyncio
async def test_reconcile_logs_what_it_touched(session, current_user, caplog):
    """The deploy log names each tag created, deleted, or kept."""
    await add_tag(session, current_user, "retired", "domain")
    kept = await add_tag(session, current_user, "in_use", "domain")
    await attach_tag(session, current_user, kept, "analytics.revenue")
    spec = DeploymentSpec(
        namespace="taxonomy",
        nodes=[],
        tags=[TagSpec(name="payments", tag_type="domain")],
        managed_tag_types=[TagTypeClaimSpec(tag_type="domain")],
    )
    orchestrator = orchestrator_for(spec, session, current_user)
    with caplog.at_level(logging.INFO, logger=ORCHESTRATOR_LOGGER):
        await orchestrator._setup_deployment_resources()
        await orchestrator._reconcile_claimed_tags()

    messages = [record.getMessage() for record in caplog.records]
    assert "Creating tag `payments` of type `domain`" in messages
    assert "Tags in taxonomy: created 1, updated 0, unchanged 0" in messages
    assert "Reconciling tags of claimed type(s) domain for taxonomy" in messages
    assert "Deleting tag `retired` of claimed type `domain`" in messages
    assert "Keeping tag `in_use`: still on 1 active node(s)" in messages
    assert "Reconciled claimed tags: deleted 1, kept 1 still in use" in messages


@pytest.mark.asyncio
async def test_reconcile_keeps_a_tag_an_active_node_still_holds(session, current_user):
    """
    The nodes holding a claimed tag usually live in namespaces the deploying repo
    cannot edit, so the deploy reports the blocked deletion instead of failing.
    """
    tag = await add_tag(session, current_user, "retired", "domain")
    await attach_tag(session, current_user, tag, "analytics.revenue")
    spec = DeploymentSpec(
        namespace="taxonomy",
        nodes=[],
        tags=[TagSpec(name="payments", tag_type="domain")],
        managed_tag_types=[TagTypeClaimSpec(tag_type="domain")],
    )
    orchestrator = orchestrator_for(spec, session, current_user)
    await orchestrator._setup_deployment_resources()
    results = await orchestrator._reconcile_claimed_tags()

    assert {tag.name for tag in await Tag.find_tags(session, tag_types=["domain"])} == {
        "payments",
        "retired",
    }
    (result,) = results_for(results, "retired")
    assert result.status == DeploymentResult.Status.WARNING
    assert result.operation == DeploymentResult.Operation.NOOP
    assert result.message == (
        "Tag 'retired' is no longer declared but is still attached to 1 node(s): "
        "analytics.revenue. Remove the tag from these nodes to delete it."
    )


@pytest.mark.asyncio
async def test_a_long_list_of_holders_is_truncated(session, current_user):
    """A widely-used tag names the first few holders rather than all of them."""
    tag = await add_tag(session, current_user, "retired", "domain")
    for index in range(6):
        await attach_tag(session, current_user, tag, f"analytics.metric_{index}")
    spec = DeploymentSpec(
        namespace="taxonomy",
        nodes=[],
        tags=[TagSpec(name="payments", tag_type="domain")],
        managed_tag_types=[TagTypeClaimSpec(tag_type="domain")],
    )
    orchestrator = orchestrator_for(spec, session, current_user)
    await orchestrator._setup_deployment_resources()
    results = await orchestrator._reconcile_claimed_tags()

    (result,) = results_for(results, "retired")
    assert result.message == (
        "Tag 'retired' is no longer declared but is still attached to 6 node(s): "
        "analytics.metric_0, analytics.metric_1, analytics.metric_2, "
        "analytics.metric_3, analytics.metric_4 and 1 more. Remove the tag from "
        "these nodes to delete it."
    )


@pytest.mark.asyncio
async def test_a_tag_only_a_deleted_node_held_is_deleted(session, current_user):
    """A deactivated node does not keep a dropped tag alive."""
    tag = await add_tag(session, current_user, "retired", "domain")
    node = await attach_tag(session, current_user, tag, "analytics.revenue")
    node.deactivated_at = datetime.now(UTC)
    await session.flush()
    spec = DeploymentSpec(
        namespace="taxonomy",
        nodes=[],
        tags=[TagSpec(name="payments", tag_type="domain")],
        managed_tag_types=[TagTypeClaimSpec(tag_type="domain")],
    )
    orchestrator = orchestrator_for(spec, session, current_user)
    await orchestrator._setup_deployment_resources()
    results = await orchestrator._reconcile_claimed_tags()

    assert [tag.name for tag in await Tag.find_tags(session, tag_types=["domain"])] == [
        "payments",
    ]
    assert [result.operation for result in results_for(results, "retired")] == [
        DeploymentResult.Operation.DELETE,
    ]


@pytest.mark.asyncio
async def test_reconcile_leaves_unclaimed_types_alone(session, current_user):
    """A claim on one type says nothing about tags of another."""
    await add_tag(session, current_user, "quarterly", "group")
    spec = DeploymentSpec(
        namespace="taxonomy",
        nodes=[],
        tags=[TagSpec(name="payments", tag_type="domain")],
        managed_tag_types=[TagTypeClaimSpec(tag_type="domain")],
    )
    orchestrator = orchestrator_for(spec, session, current_user)
    await orchestrator._setup_deployment_resources()
    results = await orchestrator._reconcile_claimed_tags()

    assert [tag.name for tag in await Tag.find_tags(session, tag_types=["group"])] == [
        "quarterly",
    ]
    assert results_for(results, "quarterly") == []


@pytest.mark.asyncio
async def test_a_tag_a_deployed_node_uses_survives(session, current_user):
    """A node's tag counts as declared even with no tag spec of its own."""
    await add_tag(session, current_user, "payments", "domain")
    spec = DeploymentSpec(
        namespace="taxonomy",
        nodes=[
            MetricSpec(
                name="revenue",
                query="SELECT SUM(amount) FROM taxonomy.fct",
                tags=["payments"],
            ),
        ],
        managed_tag_types=[TagTypeClaimSpec(tag_type="domain")],
    )
    orchestrator = orchestrator_for(spec, session, current_user)
    await orchestrator._setup_deployment_resources()
    results = await orchestrator._reconcile_claimed_tags()
    assert results == []

    assert [tag.name for tag in await Tag.find_tags(session, tag_types=["domain"])] == [
        "payments",
    ]


@pytest.mark.asyncio
async def test_a_branch_deploy_does_not_reconcile(session, current_user):
    """
    A branch's claim resolves to its parent, so the branch is not the holder and
    a stale checkout of it cannot delete the parent's vocabulary.
    """
    await add_namespace(session, "taxonomy")
    await add_namespace(session, "taxonomy.somebranch", parent_namespace="taxonomy")
    await add_tag(session, current_user, "retired", "domain")
    spec = DeploymentSpec(
        namespace="taxonomy.somebranch",
        nodes=[],
        tags=[TagSpec(name="payments", tag_type="domain")],
        managed_tag_types=[TagTypeClaimSpec(tag_type="domain")],
    )
    orchestrator = orchestrator_for(spec, session, current_user)
    await orchestrator._setup_deployment_resources()
    results = await orchestrator._reconcile_claimed_tags()
    assert results == []

    assert (await claim_for(session, "domain")).namespace == "taxonomy"
    assert {tag.name for tag in await Tag.find_tags(session, tag_types=["domain"])} == {
        "payments",
        "retired",
    }


@pytest.mark.asyncio
async def test_wiping_a_vocabulary_needs_allow_empty(session, current_user):
    """
    A manifest that claims a type but declares no tags is usually a partial push,
    not a request to drop the whole vocabulary.
    """
    await add_tag(session, current_user, "payments", "domain")
    fields = dict(
        namespace="taxonomy",
        nodes=[],
        managed_tag_types=[TagTypeClaimSpec(tag_type="domain")],
    )
    orchestrator = orchestrator_for(DeploymentSpec(**fields), session, current_user)
    await orchestrator._setup_deployment_resources()
    results = await orchestrator._reconcile_claimed_tags()

    assert [tag.name for tag in await Tag.find_tags(session, tag_types=["domain"])] == [
        "payments",
    ]
    (result,) = results_for(results, "taxonomy")
    assert result.status == DeploymentResult.Status.WARNING
    assert result.message == (
        "1 tag(s) of claimed type(s) domain were left intact because the "
        "deployment declares no tags. Re-run with allow_empty to delete them."
    )
    assert [warning.message for warning in orchestrator.warnings] == [result.message]

    # The opt-in drops them.
    orchestrator = orchestrator_for(
        DeploymentSpec(**fields, allow_empty=True),
        session,
        current_user,
    )
    await orchestrator._setup_deployment_resources()
    results = await orchestrator._reconcile_claimed_tags()
    assert await Tag.find_tags(session, tag_types=["domain"]) == []


@pytest.mark.asyncio
async def test_a_manifest_without_claims_reconciles_nothing(session, current_user):
    """Omitting the section leaves every tag alone, claimed or not."""
    await upsert_tag_type_claims(
        session,
        "taxonomy",
        [TagTypeClaimSpec(tag_type="domain", namespace="taxonomy")],
        current_user_id=current_user.id,
    )
    await add_tag(session, current_user, "retired", "domain")
    spec = DeploymentSpec(namespace="taxonomy", nodes=[])
    orchestrator = orchestrator_for(spec, session, current_user)
    results = await orchestrator._reconcile_claimed_tags()
    assert results == []

    assert [tag.name for tag in await Tag.find_tags(session, tag_types=["domain"])] == [
        "retired",
    ]
