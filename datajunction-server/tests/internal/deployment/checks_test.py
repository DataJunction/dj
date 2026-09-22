"""Governance checks wired into a deployment: projection, loading, evaluation."""

from unittest.mock import MagicMock

import pytest

from datajunction_server.errors import DJInvalidDeploymentConfig
from datajunction_server.internal.checks.context import custom_metadata
from datajunction_server.internal.checks.manifest import resolve_rulesets
from datajunction_server.internal.custom_metadata import upsert_schema_specs
from datajunction_server.internal.deployment.checks import (
    DeclaredSchemas,
    build_fixtures,
    project_column,
    project_link,
    project_node,
    resolve_declared_schemas,
    resolve_tag_types,
    unsupported_ruleset_guards,
)
from datajunction_server.internal.deployment.orchestrator import (
    DeploymentOrchestrator,
    DeploymentPlan,
)
from datajunction_server.internal.deployment.utils import DeploymentContext
from datajunction_server.models.deployment import (
    CheckVerdict,
    ColumnSpec,
    DeploymentCheckSpec,
    DeploymentResult,
    DeploymentRulesetSpec,
    DeploymentSpec,
    DimensionJoinLinkSpec,
    DimensionReferenceLinkSpec,
    DimensionSpec,
    MetricSpec,
    NodeSpec,
    RulesetVerdict,
    TagSpec,
    TransformSpec,
)
from datajunction_server.models.node_type import NodeType

NAMESPACE = "checks_demo"


def transform(name: str, **kwargs) -> TransformSpec:
    return TransformSpec(
        name=name,
        namespace=NAMESPACE,
        query="SELECT 1 AS one",
        **kwargs,
    )


def make_orchestrator(
    session,
    current_user,
    checks,
    rulesets=None,
    nodes=None,
    tags=None,
):
    spec = DeploymentSpec(
        namespace=NAMESPACE,
        nodes=nodes or [],
        checks=checks,
        rulesets=rulesets,
        tags=tags or [],
    )
    context = MagicMock(spec=DeploymentContext)
    context.current_user = current_user
    return DeploymentOrchestrator(
        deployment_spec=spec,
        deployment_id="checks-test",
        session=session,
        context=context,
    )


def make_plan(
    to_deploy=(),
    to_skip=(),
    to_delete=(),
    existing_specs=None,
    node_graph=None,
) -> DeploymentPlan:
    specs = list(to_deploy) + list(to_skip) + list(to_delete)
    return DeploymentPlan(
        to_deploy=list(to_deploy),
        to_skip=list(to_skip),
        to_delete=list(to_delete),
        existing_specs=dict(existing_specs or {}),
        node_graph=node_graph or {spec.rendered_name: [] for spec in specs},
        external_deps=set(),
    )


def check_rows(orchestrator) -> list[DeploymentResult]:
    return [
        result
        for result in orchestrator.deployed_results
        if result.deploy_type == DeploymentResult.Type.CHECK
    ]


def verdicts(orchestrator) -> dict[str, CheckVerdict]:
    """Check name -> verdict, for the only node checked."""
    (results,) = orchestrator.check_results
    return {verdict.check: verdict.verdict for verdict in results.checks}


def ruleset_verdicts(orchestrator) -> dict[str, RulesetVerdict]:
    """Ruleset name -> verdict, for the only node checked."""
    (results,) = orchestrator.check_results
    return {verdict.ruleset: verdict.verdict for verdict in results.rulesets}


def all_ruleset_verdicts(orchestrator) -> list[tuple[str, str, RulesetVerdict]]:
    """(node, ruleset, verdict) across every node checked."""
    return [
        (results.node, verdict.ruleset, verdict.verdict)
        for results in orchestrator.check_results
        for verdict in results.rulesets
    ]


@pytest.mark.asyncio
async def test_no_checks_declared_changes_nothing(session, current_user):
    """A manifest with no `checks` block reports exactly what it did before."""
    orchestrator = make_orchestrator(session, current_user, checks=None)
    await orchestrator._run_governance_checks(make_plan(to_deploy=[transform("one")]))
    assert orchestrator.deployed_results == []
    assert orchestrator.errors == []
    assert orchestrator.warnings == []


@pytest.mark.asyncio
async def test_passing_check_reports_a_success_row(session, current_user):
    orchestrator = make_orchestrator(
        session,
        current_user,
        checks=[
            DeploymentCheckSpec(
                name="demo.owner_present",
                condition="size(node.owners) >= 1",
                gate="warn",
                description="Every node has an owner.",
            ),
        ],
    )
    await orchestrator._run_governance_checks(
        make_plan(to_deploy=[transform("one", owners=["someone"])]),
    )
    (row,) = check_rows(orchestrator)
    assert row.name == f"{NAMESPACE}.one"
    assert row.status == DeploymentResult.Status.SUCCESS
    assert verdicts(orchestrator) == {"demo.owner_present": CheckVerdict.PASSED}
    assert orchestrator.errors == []
    assert orchestrator.warnings == []


@pytest.mark.asyncio
async def test_failing_warn_check_warns_without_blocking(session, current_user):
    orchestrator = make_orchestrator(
        session,
        current_user,
        checks=[
            DeploymentCheckSpec(
                name="demo.owner_present",
                condition="size(node.owners) >= 1",
                gate="warn",
            ),
        ],
    )
    await orchestrator._run_governance_checks(
        make_plan(to_deploy=[transform("one")]),
    )
    (row,) = check_rows(orchestrator)
    assert row.status == DeploymentResult.Status.WARNING
    assert orchestrator.errors == []
    assert "demo.owner_present" in orchestrator.warnings[0].message


@pytest.mark.asyncio
async def test_failing_block_check_fails_the_deploy(session, current_user):
    orchestrator = make_orchestrator(
        session,
        current_user,
        checks=[
            DeploymentCheckSpec(
                name="demo.description_present",
                condition="node.description != ''",
                gate="block",
            ),
        ],
    )
    with pytest.raises(DJInvalidDeploymentConfig) as excinfo:
        await orchestrator._run_governance_checks(
            make_plan(to_deploy=[transform("one")]),
        )
    assert "blocked by 1 failing check" in str(excinfo.value)
    (row,) = check_rows(orchestrator)
    assert row.status == DeploymentResult.Status.FAILED
    assert "demo.description_present" in orchestrator.errors[0].message


@pytest.mark.asyncio
async def test_block_on_regression_blocks_a_check_that_used_to_pass(
    session,
    current_user,
):
    """The node had an owner and no longer does, so the gate closes."""
    orchestrator = make_orchestrator(
        session,
        current_user,
        checks=[
            DeploymentCheckSpec(
                name="demo.owner_present",
                condition="size(node.owners) >= 1",
                gate="block_on_regression",
            ),
        ],
    )
    plan = make_plan(
        to_deploy=[transform("one")],
        existing_specs={f"{NAMESPACE}.one": transform("one", owners=["someone"])},
    )
    with pytest.raises(DJInvalidDeploymentConfig):
        await orchestrator._run_governance_checks(plan)
    assert check_rows(orchestrator)[0].status == DeploymentResult.Status.FAILED


@pytest.mark.asyncio
async def test_block_on_regression_allows_a_check_that_already_failed(
    session,
    current_user,
):
    """The deployed node had no owner either: not a regression, so only a warning."""
    orchestrator = make_orchestrator(
        session,
        current_user,
        checks=[
            DeploymentCheckSpec(
                name="demo.owner_present",
                condition="size(node.owners) >= 1",
                gate="block_on_regression",
            ),
        ],
    )
    plan = make_plan(
        to_deploy=[transform("one")],
        existing_specs={f"{NAMESPACE}.one": transform("one")},
    )
    await orchestrator._run_governance_checks(plan)
    assert check_rows(orchestrator)[0].status == DeploymentResult.Status.WARNING
    assert orchestrator.errors == []


@pytest.mark.asyncio
async def test_a_skipped_check_is_recorded_as_skipped(session, current_user):
    """
    `when` excluded the node, so the check asserted nothing about it -- which is
    not the same as having no verdict at all.
    """
    orchestrator = make_orchestrator(
        session,
        current_user,
        checks=[
            DeploymentCheckSpec(
                name="demo.primary_key_set",
                when="node.node_type == 'metric'",
                condition="size(node.primary_key) >= 1",
                gate="block",
            ),
        ],
    )
    await orchestrator._run_governance_checks(
        make_plan(to_deploy=[transform("one")]),
    )
    assert verdicts(orchestrator) == {"demo.primary_key_set": CheckVerdict.SKIPPED}
    (row,) = check_rows(orchestrator)
    assert row.status == DeploymentResult.Status.SKIPPED
    assert orchestrator.errors == []


@pytest.mark.asyncio
async def test_malformed_check_fails_the_deploy(session, current_user):
    orchestrator = make_orchestrator(
        session,
        current_user,
        checks=[
            DeploymentCheckSpec(
                name="demo.unknown_binding",
                condition="size(entity.owners) >= 1",
                gate="warn",
            ),
        ],
    )
    with pytest.raises(DJInvalidDeploymentConfig) as excinfo:
        await orchestrator._run_governance_checks(
            make_plan(to_deploy=[transform("one")]),
        )
    assert "Invalid governance checks" in str(excinfo.value)
    message = orchestrator.errors[0].message
    assert "demo.unknown_binding" in message
    assert "`condition`" in message
    assert check_rows(orchestrator) == []


@pytest.mark.asyncio
async def test_ruleset_when_guard_is_refused(session, current_user):
    """Scoping a ruleset is not implemented, and under-enforcing it silently is worse."""
    orchestrator = make_orchestrator(
        session,
        current_user,
        checks=[
            DeploymentCheckSpec(
                name="demo.owner_present",
                condition="size(node.owners) >= 1",
                gate="warn",
            ),
        ],
        rulesets=[
            DeploymentRulesetSpec(
                name="strict",
                when="node.node_type == 'metric'",
                checks=["demo.owner_present"],
            ),
        ],
    )
    with pytest.raises(DJInvalidDeploymentConfig):
        await orchestrator._run_governance_checks(
            make_plan(to_deploy=[transform("one")]),
        )
    assert "not yet supported" in orchestrator.errors[0].message


@pytest.mark.asyncio
async def test_a_ruleset_passes_when_every_member_check_passed(session, current_user):
    orchestrator = make_orchestrator(
        session,
        current_user,
        checks=[
            DeploymentCheckSpec(
                name="demo.owner_present",
                condition="size(node.owners) >= 1",
                gate="warn",
            ),
        ],
        rulesets=[
            DeploymentRulesetSpec(name="baseline", checks=["demo.owner_present"]),
        ],
    )
    await orchestrator._run_governance_checks(
        make_plan(to_deploy=[transform("one", owners=["someone"])]),
    )
    assert check_rows(orchestrator)[0].status == DeploymentResult.Status.SUCCESS
    (results,) = orchestrator.check_results
    assert results.node == f"{NAMESPACE}.one"
    assert ruleset_verdicts(orchestrator) == {"baseline": RulesetVerdict.PASSED}


@pytest.mark.asyncio
async def test_a_ruleset_fails_when_a_member_check_failed(session, current_user):
    """The gate is warn, so the deploy still runs; the roll-up still says failed."""
    orchestrator = make_orchestrator(
        session,
        current_user,
        checks=[
            DeploymentCheckSpec(
                name="demo.owner_present",
                condition="size(node.owners) >= 1",
                gate="warn",
            ),
            DeploymentCheckSpec(
                name="demo.description_present",
                condition="node.description != ''",
                gate="warn",
            ),
        ],
        rulesets=[
            DeploymentRulesetSpec(
                name="baseline",
                checks=["demo.owner_present", "demo.description_present"],
            ),
        ],
    )
    await orchestrator._run_governance_checks(
        make_plan(to_deploy=[transform("one", owners=["someone"])]),
    )
    assert ruleset_verdicts(orchestrator) == {"baseline": RulesetVerdict.FAILED}
    assert verdicts(orchestrator) == {
        "demo.owner_present": CheckVerdict.PASSED,
        "demo.description_present": CheckVerdict.FAILED,
    }


@pytest.mark.asyncio
async def test_a_skipped_member_does_not_fail_its_ruleset(session, current_user):
    """A check whose `when` excluded the node asserted nothing about it."""
    orchestrator = make_orchestrator(
        session,
        current_user,
        checks=[
            DeploymentCheckSpec(
                name="demo.owner_present",
                condition="size(node.owners) >= 1",
                gate="warn",
            ),
            DeploymentCheckSpec(
                name="demo.primary_key_set",
                when="node.node_type == 'metric'",
                condition="size(node.primary_key) >= 1",
                gate="warn",
            ),
        ],
        rulesets=[
            DeploymentRulesetSpec(
                name="baseline",
                checks=["demo.owner_present", "demo.primary_key_set"],
            ),
        ],
    )
    await orchestrator._run_governance_checks(
        make_plan(to_deploy=[transform("one", owners=["someone"])]),
    )
    assert ruleset_verdicts(orchestrator) == {"baseline": RulesetVerdict.PASSED}
    assert verdicts(orchestrator)["demo.primary_key_set"] == CheckVerdict.SKIPPED


@pytest.mark.asyncio
async def test_a_ruleset_is_not_applicable_when_every_member_was_skipped(
    session,
    current_user,
):
    """Nothing asserted is not the same as everything satisfied."""
    orchestrator = make_orchestrator(
        session,
        current_user,
        checks=[
            DeploymentCheckSpec(
                name="demo.primary_key_set",
                when="node.node_type == 'metric'",
                condition="size(node.primary_key) >= 1",
                gate="warn",
            ),
        ],
        rulesets=[
            DeploymentRulesetSpec(name="baseline", checks=["demo.primary_key_set"]),
        ],
    )
    await orchestrator._run_governance_checks(
        make_plan(to_deploy=[transform("one")]),
    )
    assert check_rows(orchestrator)[0].status == DeploymentResult.Status.SKIPPED
    assert ruleset_verdicts(orchestrator) == {
        "baseline": RulesetVerdict.NOT_APPLICABLE,
    }


@pytest.mark.asyncio
async def test_a_nested_ruleset_rolls_up_what_it_includes(session, current_user):
    """Passing `certified` means passing everything `baseline` asserts, too."""
    orchestrator = make_orchestrator(
        session,
        current_user,
        checks=[
            DeploymentCheckSpec(
                name="demo.owner_present",
                condition="size(node.owners) >= 1",
                gate="warn",
            ),
            DeploymentCheckSpec(
                name="demo.description_present",
                condition="node.description != ''",
                gate="warn",
            ),
        ],
        rulesets=[
            DeploymentRulesetSpec(name="baseline", checks=["demo.owner_present"]),
            DeploymentRulesetSpec(
                name="certified",
                includes=["baseline"],
                checks=["demo.description_present"],
            ),
        ],
    )
    await orchestrator._run_governance_checks(
        make_plan(to_deploy=[transform("one", owners=["someone"])]),
    )
    assert ruleset_verdicts(orchestrator) == {
        "baseline": RulesetVerdict.PASSED,
        "certified": RulesetVerdict.FAILED,
    }


@pytest.mark.asyncio
async def test_every_node_gets_a_verdict_for_every_ruleset(session, current_user):
    orchestrator = make_orchestrator(
        session,
        current_user,
        checks=[
            DeploymentCheckSpec(
                name="demo.owner_present",
                condition="size(node.owners) >= 1",
                gate="warn",
            ),
        ],
        rulesets=[
            DeploymentRulesetSpec(name="baseline", checks=["demo.owner_present"]),
        ],
    )
    await orchestrator._run_governance_checks(
        make_plan(
            to_deploy=[transform("one", owners=["someone"]), transform("two")],
        ),
    )
    assert all_ruleset_verdicts(orchestrator) == [
        (f"{NAMESPACE}.one", "baseline", RulesetVerdict.PASSED),
        (f"{NAMESPACE}.two", "baseline", RulesetVerdict.FAILED),
    ]


@pytest.mark.asyncio
async def test_a_ruleset_verdict_never_blocks_the_deploy(session, current_user):
    """Blocking is the member checks' gates; the roll-up only reports."""
    orchestrator = make_orchestrator(
        session,
        current_user,
        checks=[
            DeploymentCheckSpec(
                name="demo.owner_present",
                condition="size(node.owners) >= 1",
                gate="warn",
            ),
        ],
        rulesets=[
            DeploymentRulesetSpec(name="baseline", checks=["demo.owner_present"]),
        ],
    )
    await orchestrator._run_governance_checks(make_plan(to_deploy=[transform("one")]))
    assert ruleset_verdicts(orchestrator) == {"baseline": RulesetVerdict.FAILED}
    assert orchestrator.errors == []


@pytest.mark.asyncio
async def test_a_removed_node_is_checked_from_its_deployed_spec(session, current_user):
    """Deletions have no manifest spec, so the existing one is what gets checked."""
    existing = transform("one", owners=["someone"])
    orchestrator = make_orchestrator(
        session,
        current_user,
        checks=[
            DeploymentCheckSpec(
                name="demo.removal_reviewed",
                when="change.kind == 'delete'",
                condition="size(node.owners) >= 1",
                gate="warn",
            ),
        ],
    )
    await orchestrator._run_governance_checks(
        make_plan(
            to_delete=[existing],
            existing_specs={f"{NAMESPACE}.one": existing},
        ),
    )
    (row,) = check_rows(orchestrator)
    assert row.status == DeploymentResult.Status.SUCCESS


@pytest.mark.asyncio
async def test_unchanged_nodes_are_checked_too(session, current_user):
    """A newly declared check governs the graph, not only the nodes that changed."""
    unchanged = transform("one")
    orchestrator = make_orchestrator(
        session,
        current_user,
        checks=[
            DeploymentCheckSpec(
                name="demo.owner_present",
                condition="size(node.owners) >= 1",
                gate="warn",
            ),
        ],
    )
    await orchestrator._run_governance_checks(
        make_plan(
            to_skip=[unchanged],
            existing_specs={f"{NAMESPACE}.one": unchanged},
        ),
    )
    assert check_rows(orchestrator)[0].status == DeploymentResult.Status.WARNING


@pytest.mark.asyncio
async def test_dependencies_prefer_the_spec_being_deployed(session, current_user):
    """An upstream deployed in this batch is read as it will be, not as it is."""
    upstream_now = transform("parent", custom_metadata={"sample": {"color": "green"}})
    upstream_before = transform("parent", custom_metadata={"sample": {"color": "red"}})
    child = transform("child")
    orchestrator = make_orchestrator(
        session,
        current_user,
        checks=[
            DeploymentCheckSpec(
                name="demo.parent_color",
                when="size(dependencies) >= 1",
                condition=(
                    "dependencies.all(parent, "
                    "parent.custom_metadata.sample.color == 'green')"
                ),
                gate="block",
            ),
        ],
    )
    await upsert_schema_specs(
        session,
        namespace=NAMESPACE,
        specs=[_sample_schema(["color"])],
        current_user_id=current_user.id,
    )
    plan = make_plan(
        to_deploy=[upstream_now, child],
        existing_specs={f"{NAMESPACE}.parent": upstream_before},
        node_graph={
            f"{NAMESPACE}.parent": [],
            f"{NAMESPACE}.child": [f"{NAMESPACE}.parent"],
        },
    )
    await orchestrator._run_governance_checks(plan)
    statuses = {row.name: row.status for row in check_rows(orchestrator)}
    assert statuses[f"{NAMESPACE}.child"] == DeploymentResult.Status.SUCCESS


def _sample_schema(properties, node_type=None):
    from datajunction_server.models.deployment import CustomMetadataSchemaSpec

    return CustomMetadataSchemaSpec(
        key="sample",
        node_type=node_type,
        json_schema={
            "type": "object",
            "properties": {name: {"type": "string"} for name in properties},
        },
    )


@pytest.mark.asyncio
async def test_declared_properties_union_across_node_types(session, current_user):
    """
    A key's schema can differ by node type; the checks compile once, so the
    property names are unioned and the ones that do not apply read as null.
    """
    await upsert_schema_specs(
        session,
        namespace=NAMESPACE,
        specs=[
            _sample_schema(["color"], node_type=NodeType.DIMENSION),
            _sample_schema(["shape"], node_type=NodeType.METRIC),
        ],
        current_user_id=current_user.id,
    )
    declared = await resolve_declared_schemas(
        session,
        NAMESPACE,
        [NodeType.DIMENSION, NodeType.METRIC],
    )
    assert declared.properties == {"sample": ("color", "shape")}

    orchestrator = make_orchestrator(
        session,
        current_user,
        checks=[
            DeploymentCheckSpec(
                name="demo.shape_unset",
                condition="node.custom_metadata.sample.shape == null",
                gate="warn",
            ),
        ],
    )
    dimension = DimensionSpec(
        name="a_dimension",
        namespace=NAMESPACE,
        query="SELECT 1 AS one",
        primary_key=["one"],
    )
    metric = MetricSpec(
        name="a_metric",
        namespace=NAMESPACE,
        query="SELECT COUNT(1) FROM one",
        custom_metadata={"sample": {"shape": "round"}},
    )
    await orchestrator._run_governance_checks(
        make_plan(to_deploy=[dimension, metric]),
    )
    statuses = {row.name: row.status for row in check_rows(orchestrator)}
    assert statuses[f"{NAMESPACE}.a_dimension"] == DeploymentResult.Status.SUCCESS
    assert statuses[f"{NAMESPACE}.a_metric"] == DeploymentResult.Status.WARNING


@pytest.mark.asyncio
async def test_numeric_properties_survive_the_fixtures(session, current_user):
    """A fixture value is typed from its schema, so a numeric check still loads."""
    from datajunction_server.models.deployment import CustomMetadataSchemaSpec

    await upsert_schema_specs(
        session,
        namespace=NAMESPACE,
        specs=[
            CustomMetadataSchemaSpec(
                key="sample",
                json_schema={
                    "type": "object",
                    "properties": {"size": {"type": "integer"}},
                },
            ),
        ],
        current_user_id=current_user.id,
    )
    orchestrator = make_orchestrator(
        session,
        current_user,
        checks=[
            DeploymentCheckSpec(
                name="demo.size_set",
                condition=(
                    "node.custom_metadata.sample.size != null && "
                    "node.custom_metadata.sample.size >= 1"
                ),
                gate="warn",
            ),
        ],
    )
    await orchestrator._run_governance_checks(
        make_plan(
            to_deploy=[transform("one", custom_metadata={"sample": {"size": 3}})],
        ),
    )
    assert check_rows(orchestrator)[0].status == DeploymentResult.Status.SUCCESS


def test_an_absent_node_projects_the_same_keys_as_a_real_one():
    """`previous` is projected like `node`, so a check reads either one."""
    declared = {"sample": ("color",)}
    assert (
        project_node(None, declared, {}).keys()
        == project_node(
            transform("one"),
            declared,
            {},
        ).keys()
    )
    assert project_node(None, declared, {})["custom_metadata"] == custom_metadata(
        None,
        declared,
    )


def test_fixtures_cover_both_an_empty_and_a_populated_entity():
    declared = DeclaredSchemas(
        properties={"sample": ("color",)},
        placeholders={"sample": {"color": "placeholder"}},
    )
    bare, populated = build_fixtures(declared, [NodeType.TRANSFORM])
    assert bare["node"]["custom_metadata"]["sample"]["color"] is None
    assert populated["node"]["custom_metadata"]["sample"]["color"] == "placeholder"
    assert bare["change"] == {"kind": "create"}
    assert populated["change"] == {"kind": "delete"}


def test_only_guarded_rulesets_are_refused():
    rulesets = resolve_rulesets(
        [
            DeploymentRulesetSpec(name="baseline", checks=["demo.owner_present"]),
            DeploymentRulesetSpec(name="strict", when="node.node_type == 'metric'"),
        ],
    )
    (refused,) = unsupported_ruleset_guards(rulesets)
    assert refused.check == "ruleset strict"
    assert refused.clause == "when"


def test_every_node_spec_field_is_projected():
    """
    A new field on any node spec becomes visible to checks without anyone
    remembering to add it here.
    """
    declared: set[str] = set()
    pending = [NodeSpec]
    while pending:
        cls = pending.pop()
        declared |= set(cls.model_fields)
        pending.extend(cls.__subclasses__())

    projected = set(project_node(None, {}, {}))
    assert declared - projected == set()


def test_a_subclass_without_a_field_still_projects_it_empty():
    """
    A metric has no dimension links, and a list field must project as a list:
    `.all()` on the empty string would fail where it should trivially pass.
    """
    metric = project_node(
        MetricSpec(name="m", node_type="metric", query="SELECT 1"),
        {},
        {},
    )
    dimension = project_node(
        DimensionSpec(name="d", node_type="dimension", query="SELECT 1"),
        {},
        {},
    )
    assert set(metric) == set(dimension) == set(project_node(None, {}, {}))
    assert metric["dimension_links"] == []
    assert metric["catalog"] == ""
    # A number defaults to null, not "": `'' >= 1` has no overload, so a
    # comparison would fail rather than answer.
    assert metric["significant_digits"] is None


def test_a_node_projects_its_own_namespace_not_the_manifests():
    """
    The spec's `namespace` is the manifest's root, so a nested node has to read
    its namespace off the rendered name.
    """
    spec = TransformSpec(name="sub.deep.orders", namespace="top", query="SELECT 1")
    projected = project_node(spec, {}, {})
    assert projected["name"] == "top.sub.deep.orders"
    assert projected["namespace"] == "top.sub.deep"
    assert project_node(None, {}, {})["namespace"] == ""


def _linked(*links) -> DimensionSpec:
    return DimensionSpec(
        name="d",
        node_type="dimension",
        query="SELECT 1",
        dimension_links=list(links),
    )


def test_both_kinds_of_dimension_link_project_the_same_keys():
    """
    A reference link declares no join fields. Without the shared key set, a
    check reading `l.join_type` would error on the first reference link it met
    rather than reading it empty.
    """
    reference = DimensionReferenceLinkSpec(node_column="c", dimension="a.b")
    join = DimensionJoinLinkSpec(dimension_node="a.b", node_column="c")
    projected = project_node(_linked(reference, join), {}, {})["dimension_links"]
    assert set(projected[0]) == set(projected[1])
    assert projected[0]["join_type"] == ""
    assert projected[1]["join_type"] == "left"


def test_a_link_names_its_target_node_the_same_way():
    """
    A reference link's `dimension` is `<node>.<column>` while a join link's
    `dimension_node` is the node, so only `dimension_node` compares across the
    two kinds.
    """
    reference = DimensionReferenceLinkSpec(node_column="c", dimension="a.b.column")
    join = DimensionJoinLinkSpec(dimension_node="a.b", node_column="c")
    assert project_link(reference)["dimension_node"] == "a.b"
    assert project_link(join)["dimension_node"] == "a.b"
    assert project_link(reference)["dimension"] == "a.b.column"


def test_tags_project_with_their_type():
    spec = transform("one", tags=["sweet", "unknown"])
    tags = project_node(spec, {}, {"sweet": "flavor"})["tags"]
    assert tags == [
        {"name": "sweet", "tag_type": "flavor"},
        # A tag whose type the deploy could not resolve still reads as empty
        # rather than as a missing key.
        {"name": "unknown", "tag_type": ""},
    ]


@pytest.mark.asyncio
async def test_a_tag_defined_by_this_deploy_supplies_its_type(session):
    # Checks run before tags are written, so the manifest is the only source.
    resolved = await resolve_tag_types(
        session,
        [TagSpec(name="sweet", tag_type="flavor")],
        ["sweet"],
    )
    assert resolved == {"sweet": "flavor"}


@pytest.mark.asyncio
async def test_a_check_can_require_a_tag_of_a_given_type(session, current_user):
    orchestrator = make_orchestrator(
        session,
        current_user,
        checks=[
            DeploymentCheckSpec(
                name="demo.flavor_tagged",
                condition="node.tags.exists(t, t.tag_type == 'flavor')",
                gate="warn",
            ),
        ],
        tags=[TagSpec(name="sweet", tag_type="flavor")],
    )
    await orchestrator._run_governance_checks(
        make_plan(
            to_deploy=[transform("tagged", tags=["sweet"]), transform("bare")],
        ),
    )
    by_name = {row.name: row.status for row in check_rows(orchestrator)}
    assert by_name[f"{NAMESPACE}.tagged"] == DeploymentResult.Status.SUCCESS
    assert by_name[f"{NAMESPACE}.bare"] == DeploymentResult.Status.WARNING


@pytest.mark.asyncio
async def test_one_row_per_node_however_many_checks_ran(session, current_user):
    """
    A node gets a single summary row, so a large deploy does not multiply its
    results by the number of checks.
    """
    orchestrator = make_orchestrator(
        session,
        current_user,
        checks=[
            DeploymentCheckSpec(
                name="demo.owner_present",
                condition="size(node.owners) >= 1",
                gate="warn",
            ),
            DeploymentCheckSpec(
                name="demo.described",
                condition="node.description != ''",
                gate="warn",
            ),
            DeploymentCheckSpec(
                name="demo.metrics_only",
                when="node.node_type == 'metric'",
                condition="size(node.primary_key) >= 1",
                gate="warn",
            ),
        ],
        rulesets=[
            DeploymentRulesetSpec(
                name="baseline",
                checks=["demo.owner_present", "demo.described"],
            ),
        ],
    )
    await orchestrator._run_governance_checks(
        make_plan(
            to_deploy=[
                transform("one", owners=["someone"], description="Described."),
                transform("two"),
            ],
        ),
    )
    assert [(row.name, row.status) for row in check_rows(orchestrator)] == [
        (f"{NAMESPACE}.one", DeploymentResult.Status.SUCCESS),
        (f"{NAMESPACE}.two", DeploymentResult.Status.WARNING),
    ]
    good, bad = orchestrator.check_results
    # The guarded check applied to neither node, and says so on both.
    assert {v.check: v.verdict for v in good.checks} == {
        "demo.owner_present": CheckVerdict.PASSED,
        "demo.described": CheckVerdict.PASSED,
        "demo.metrics_only": CheckVerdict.SKIPPED,
    }
    assert {v.check: v.verdict for v in bad.checks} == {
        "demo.owner_present": CheckVerdict.FAILED,
        "demo.described": CheckVerdict.FAILED,
        "demo.metrics_only": CheckVerdict.SKIPPED,
    }
    assert bad.checks[0].gate == "warn"
    assert [v.verdict for v in bad.rulesets] == [RulesetVerdict.FAILED]


@pytest.mark.asyncio
async def test_the_summary_row_names_the_failing_checks(session, current_user):
    orchestrator = make_orchestrator(
        session,
        current_user,
        checks=[
            DeploymentCheckSpec(
                name="demo.owner_present",
                condition="size(node.owners) >= 1",
                gate="warn",
            ),
            DeploymentCheckSpec(
                name="demo.described",
                condition="node.description != ''",
                gate="warn",
            ),
        ],
    )
    await orchestrator._run_governance_checks(
        make_plan(to_deploy=[transform("one", owners=["someone"])]),
    )
    (row,) = check_rows(orchestrator)
    assert row.message == "1 of 2 check(s) failed: demo.described."


@pytest.mark.asyncio
async def test_previous_reads_the_upstreams_it_was_deployed_with(
    session,
    current_user,
):
    """
    This deploy repoints `child` from `old_parent` to `new_parent`. The check
    reads `dependencies`, so the prior verdict has to be taken against the
    upstream the node actually had.
    """
    old_parent = transform("old_parent", description="Described.")
    new_parent = transform("new_parent")
    child = TransformSpec(
        name="child",
        namespace=NAMESPACE,
        query=f"SELECT 1 FROM {NAMESPACE}.new_parent",
    )
    deployed_child = TransformSpec(
        name="child",
        namespace=NAMESPACE,
        query=f"SELECT 1 FROM {NAMESPACE}.old_parent",
    )
    orchestrator = make_orchestrator(
        session,
        current_user,
        checks=[
            DeploymentCheckSpec(
                name="demo.parents_described",
                condition="dependencies.all(d, d.description != '')",
                gate="block_on_regression",
            ),
        ],
    )
    plan = make_plan(
        to_deploy=[child, new_parent],
        existing_specs={
            f"{NAMESPACE}.child": deployed_child,
            f"{NAMESPACE}.old_parent": old_parent,
        },
        node_graph={
            f"{NAMESPACE}.child": [f"{NAMESPACE}.new_parent"],
            f"{NAMESPACE}.new_parent": [],
        },
    )
    with pytest.raises(DJInvalidDeploymentConfig):
        await orchestrator._run_governance_checks(plan)
    # It passed against `old_parent`, which is described, so repointing it at an
    # undescribed parent is a regression and the gate closes.
    by_node = {
        results.node: {verdict.check: verdict.verdict for verdict in results.checks}
        for results in orchestrator.check_results
    }
    assert by_node[f"{NAMESPACE}.child"] == {
        "demo.parents_described": CheckVerdict.FAILED,
    }
    assert "demo.parents_described" in orchestrator.errors[0].message


def test_a_column_carries_every_field_the_spec_declares():
    """
    `order` is excluded from the dump, so without reading it off the spec a
    check naming it would hit a missing key rather than an empty value.
    """
    spec = DimensionSpec(
        name="d",
        node_type="dimension",
        query="SELECT 1",
        columns=[ColumnSpec(name="first", type="string", order=2)],
    )
    (column,) = project_node(spec, {}, {})["columns"]
    assert set(column) == set(ColumnSpec.model_fields)
    assert column["order"] == 2
    assert project_column(ColumnSpec(name="c", type="string"))["order"] is None


def test_a_metrics_columns_are_projected():
    """
    MetricSpec excludes `columns` from the dump, so a column rule used to read
    an empty list on every metric and pass vacuously.
    """
    spec = MetricSpec(
        name="m",
        node_type="metric",
        query="SELECT 1",
        columns=[ColumnSpec(name="first", type="string")],
    )
    assert [column["name"] for column in project_node(spec, {}, {})["columns"]] == [
        "first",
    ]
