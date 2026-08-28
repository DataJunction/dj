"""
Tests for internal namespace functions
"""

from datetime import UTC, datetime

import pytest
from ruamel.yaml import YAML
from ruamel.yaml.comments import CommentedMap, CommentedSeq
from sqlalchemy import select

from datajunction_server.database.history import History
from datajunction_server.database.namespace import NodeNamespace
from datajunction_server.database.rbac import Role, RoleAssignment
from datajunction_server.database.user import OAuthProvider, PrincipalKind, User
from datajunction_server.errors import DJAlreadyExistsException, DJInvalidInputException
from datajunction_server.internal.history import ActivityType, EntityType
from datajunction_server.internal.namespace_locks import (
    lock_namespace_boundary_lifecycle,
)
from datajunction_server.internal.namespaces import (
    _merge_list_with_key,
    _merge_yaml_preserving_comments,
    create_or_reactivate_namespace,
    node_spec_to_yaml,
    provision_namespace_boundary,
)
from datajunction_server.models.access import ResourceAction, ResourceType
from datajunction_server.models.deployment import MetricSpec, TransformSpec
from datajunction_server.models.namespace import NamespaceWriteStatus
from datajunction_server.models.node_type import NodeType


def _principal(username: str, kind: PrincipalKind) -> User:
    return User(
        username=username,
        password=None,
        email=None,
        name=username,
        oauth_provider=OAuthProvider.BASIC,
        kind=kind,
    )


async def _save_history(event, session):
    """No-op history recorder."""


async def _create_namespace(
    session,
    user: User,
    namespace: str,
    patterns: list[str],
    save_history=_save_history,
):
    return await create_or_reactivate_namespace(
        namespace,
        include_parents=False,
        session=session,
        current_user=user,
        save_history=save_history,
        creator_owned_namespace_patterns=patterns,
    )


async def test_lock_namespace_boundary_lifecycle(mocker):
    session = mocker.MagicMock()
    session.execute = mocker.AsyncMock()

    session.get_bind.return_value.dialect.name = "sqlite"
    await lock_namespace_boundary_lifecycle(session)
    session.execute.assert_not_awaited()

    session.get_bind.return_value.dialect.name = "postgresql"
    await lock_namespace_boundary_lifecycle(session)
    session.execute.assert_awaited_once()


async def test_create_or_reactivate_namespace_reports_already_exists(
    session,
    current_user: User,
):
    """
    An existing namespace is reported back, not raised.

    The register_table / register_view callers discard the result and only need
    the namespace to exist, so raising here would make registering into any
    existing namespace fail.
    """

    namespace = "already_exists_ns"
    patterns = ["other", "personal.*"]
    created = await _create_namespace(session, current_user, namespace, patterns)
    duplicate = await _create_namespace(session, current_user, namespace, patterns)

    assert created.status == NamespaceWriteStatus.CREATED
    assert duplicate.status == NamespaceWriteStatus.ALREADY_EXISTS
    assert await Role.get_by_name(session, f"namespace:{namespace}:owners") is None


async def test_create_namespace_assigns_matching_creator_as_owner(
    session,
    current_user: User,
):
    namespace = "personal.alice"
    result = await _create_namespace(
        session,
        current_user,
        namespace,
        [namespace],
    )

    role = await Role.get_by_name(session, f"namespace:{namespace}:owners")
    boundary = await NodeNamespace.get(session, namespace)
    assert result.status == NamespaceWriteStatus.CREATED
    assert role is not None
    assert boundary is not None
    assert boundary.is_governed_boundary
    assert {
        (scope.action, scope.scope_type, scope.scope_value) for scope in role.scopes
    } == {
        (ResourceAction.MANAGE, ResourceType.NAMESPACE, namespace),
        (ResourceAction.MANAGE, ResourceType.NAMESPACE, f"{namespace}.*"),
        (ResourceAction.MANAGE, ResourceType.NODE, f"{namespace}.*"),
    }
    assert await RoleAssignment.find(
        session,
        principal_id=current_user.id,
        role_id=role.id,
    )
    assignment_name = f"{current_user.username}:{role.name}"
    history = set(
        (
            await session.execute(
                select(History.entity_type, History.entity_name).where(
                    History.entity_name.in_([role.name, assignment_name]),
                ),
            )
        ).all(),
    )
    assert history == {
        (EntityType.ROLE, role.name),
        (EntityType.ROLE_ASSIGNMENT, assignment_name),
    }


async def test_creator_ownership_respects_policy_boundaries(
    session,
    current_user: User,
):
    await _create_namespace(
        session,
        current_user,
        "personal.alice",
        ["personal.*"],
    )
    owner_role = await Role.get_by_name(session, "namespace:personal.alice:owners")
    assert owner_role is not None
    owner_role.name = "renamed-personal-owner"
    await session.commit()
    deployer = _principal("personal-deployer", PrincipalKind.SERVICE_ACCOUNT)
    session.add(deployer)
    await session.commit()
    await _create_namespace(
        session,
        deployer,
        "personal.alice.project",
        ["personal.*"],
    )
    assert (
        await Role.get_by_name(
            session,
            "namespace:personal.alice.project:owners",
        )
        is None
    )

    with pytest.raises(DJInvalidInputException, match="Only user principals"):
        await _create_namespace(
            session,
            deployer,
            "personal.service",
            ["personal.*"],
        )
    assert (
        await NodeNamespace.get(
            session,
            "personal.service",
            raise_if_not_exists=False,
        )
        is None
    )


async def test_creator_owner_assignment_is_atomic_with_namespace(
    session,
    current_user: User,
):
    async def fail_to_save_history(event, session):
        raise RuntimeError("history unavailable")

    with pytest.raises(RuntimeError, match="history unavailable"):
        await _create_namespace(
            session,
            current_user,
            "personal.atomic",
            ["personal.*"],
            save_history=fail_to_save_history,
        )
    await session.rollback()

    assert await session.get(NodeNamespace, "personal.atomic") is None
    assert await Role.get_by_name(session, "namespace:personal.atomic:owners") is None


async def test_creator_owned_namespace_rejects_owner_role_conflict(
    session,
    current_user: User,
):
    session.add(
        Role(
            name="namespace:personal.conflict:owners",
            created_by_id=current_user.id,
        ),
    )
    await session.commit()

    with pytest.raises(DJAlreadyExistsException, match="already exists"):
        await _create_namespace(
            session,
            current_user,
            "personal.conflict",
            ["personal.*"],
        )


async def test_provision_namespace_boundary_creates_owner_and_deployer_roles(
    session,
    current_user: User,
):
    owner_group = _principal("analytics-owners", PrincipalKind.GROUP)
    deployer = _principal("analytics-deployer", PrincipalKind.SERVICE_ACCOUNT)
    session.add_all([owner_group, deployer])
    await session.commit()

    result = await provision_namespace_boundary(
        session=session,
        namespace="analytics",
        current_user=current_user,
        owner_group=owner_group.username,
        deployer_service_accounts=[deployer.username],
    )

    assert result.namespace == "analytics"
    assert result.owner_role == "namespace:analytics:owners"
    assert result.deployer_role == "namespace:analytics:deployers"
    namespace = await NodeNamespace.get(session, "analytics")
    assert namespace is not None
    assert namespace.is_governed_boundary

    owner_role = await Role.get_by_name(session, result.owner_role)
    deployer_role = await Role.get_by_name(session, result.deployer_role)
    assert owner_role is not None
    assert deployer_role is not None
    assert {
        (scope.action, scope.scope_type, scope.scope_value)
        for scope in owner_role.scopes
    } == {
        (ResourceAction.MANAGE, ResourceType.NAMESPACE, "analytics"),
        (ResourceAction.MANAGE, ResourceType.NAMESPACE, "analytics.*"),
        (ResourceAction.MANAGE, ResourceType.NODE, "analytics.*"),
    }
    assert {
        (scope.action, scope.scope_type, scope.scope_value)
        for scope in deployer_role.scopes
    } == {
        (ResourceAction.DELETE, ResourceType.NAMESPACE, "analytics"),
        (ResourceAction.DELETE, ResourceType.NAMESPACE, "analytics.*"),
        (ResourceAction.DELETE, ResourceType.NODE, "analytics.*"),
    }
    assert await RoleAssignment.find(
        session,
        principal_id=owner_group.id,
        role_id=owner_role.id,
    )
    assert await RoleAssignment.find(
        session,
        principal_id=deployer.id,
        role_id=deployer_role.id,
    )


async def test_provision_namespace_boundary_records_rbac_history(
    session,
    current_user: User,
):
    owner_group = _principal("history-owners", PrincipalKind.GROUP)
    deployer = _principal("history-deployer", PrincipalKind.SERVICE_ACCOUNT)
    session.add_all([owner_group, deployer])
    await session.commit()

    result = await provision_namespace_boundary(
        session=session,
        namespace="history_boundary",
        current_user=current_user,
        owner_group=owner_group.username,
        deployer_service_accounts=[deployer.username],
    )
    assert result.deployer_role is not None

    owner_assignment_name = f"{owner_group.username}:{result.owner_role}"
    deployer_assignment_name = f"{deployer.username}:{result.deployer_role}"
    entity_names = {
        result.owner_role,
        result.deployer_role,
        owner_assignment_name,
        deployer_assignment_name,
    }
    history = (
        (
            await session.execute(
                select(History).where(History.entity_name.in_(entity_names)),
            )
        )
        .scalars()
        .all()
    )

    assert {
        (event.entity_type, event.entity_name, event.activity_type) for event in history
    } == {
        (EntityType.ROLE, result.owner_role, ActivityType.CREATE),
        (EntityType.ROLE, result.deployer_role, ActivityType.CREATE),
        (EntityType.ROLE_ASSIGNMENT, owner_assignment_name, ActivityType.CREATE),
        (EntityType.ROLE_ASSIGNMENT, deployer_assignment_name, ActivityType.CREATE),
    }
    role_history = next(
        event for event in history if event.entity_name == result.owner_role
    )
    assert {
        (scope["action"], scope["scope_type"], scope["scope_value"])
        for scope in role_history.post["scopes"]
    } == {
        ("manage", "namespace", "history_boundary"),
        ("manage", "namespace", "history_boundary.*"),
        ("manage", "node", "history_boundary.*"),
    }


async def test_provision_namespace_boundary_rejects_non_service_deployer(
    session,
    current_user: User,
):
    owner_group = _principal("governed-owners", PrincipalKind.GROUP)
    session.add(owner_group)
    await session.commit()

    with pytest.raises(DJInvalidInputException):
        await provision_namespace_boundary(
            session=session,
            namespace="governed",
            current_user=current_user,
            owner_group=owner_group.username,
            deployer_service_accounts=[current_user.username],
        )

    assert (
        await NodeNamespace.get(
            session,
            "governed",
            raise_if_not_exists=False,
        )
        is None
    )


async def test_provision_namespace_boundary_rejects_invalid_owner_and_conflicts(
    session,
    current_user: User,
):
    owner_group = _principal("boundary-owners", PrincipalKind.GROUP)
    session.add(owner_group)
    await session.commit()

    with pytest.raises(DJInvalidInputException, match="must be a group"):
        await provision_namespace_boundary(
            session=session,
            namespace="invalid_owner",
            current_user=current_user,
            owner_group=current_user.username,
            deployer_service_accounts=[],
        )
    with pytest.raises(DJInvalidInputException, match="must be unique"):
        await provision_namespace_boundary(
            session=session,
            namespace="duplicate_principal",
            current_user=current_user,
            owner_group=owner_group.username,
            deployer_service_accounts=[owner_group.username],
        )

    session.add(NodeNamespace(namespace="existing_boundary"))
    session.add(
        Role(
            name="namespace:conflicting_role:owners",
            created_by_id=current_user.id,
        ),
    )
    await session.commit()

    with pytest.raises(DJAlreadyExistsException, match="already exists"):
        await provision_namespace_boundary(
            session=session,
            namespace="existing_boundary",
            current_user=current_user,
            owner_group=owner_group.username,
            deployer_service_accounts=[],
        )
    with pytest.raises(DJAlreadyExistsException, match="already exists"):
        await provision_namespace_boundary(
            session=session,
            namespace="conflicting_role",
            current_user=current_user,
            owner_group=owner_group.username,
            deployer_service_accounts=[],
        )


async def test_provision_rejects_deactivated_namespace_and_preserves_policy(
    session,
    current_user: User,
):
    owner_group = _principal("restore-owners", PrincipalKind.GROUP)
    session.add(owner_group)
    await session.commit()
    result = await provision_namespace_boundary(
        session=session,
        namespace="restore_boundary",
        current_user=current_user,
        owner_group=owner_group.username,
        deployer_service_accounts=[],
    )
    namespace = await NodeNamespace.get(session, result.namespace)
    assert namespace is not None
    namespace.deactivated_at = datetime.now(UTC)
    await session.commit()

    with pytest.raises(DJAlreadyExistsException, match="Restore it through"):
        await provision_namespace_boundary(
            session=session,
            namespace=result.namespace,
            current_user=current_user,
            owner_group=owner_group.username,
            deployer_service_accounts=[],
        )

    owner_role = await Role.get_by_name(session, result.owner_role)
    assert owner_role is not None
    assert namespace.is_governed_boundary
    assert await RoleAssignment.find(
        session,
        principal_id=owner_group.id,
        role_id=owner_role.id,
    )


@pytest.mark.parametrize(
    ("existing_namespace", "requested_namespace"),
    [
        ("governed_root", "governed_root.child"),
        ("governed_root.child", "governed_root"),
    ],
)
async def test_provision_namespace_boundary_rejects_overlapping_boundary(
    session,
    current_user: User,
    existing_namespace: str,
    requested_namespace: str,
):
    owner_group = _principal("overlap-owners", PrincipalKind.GROUP)
    session.add(owner_group)
    await session.commit()
    result = await provision_namespace_boundary(
        session=session,
        namespace=existing_namespace,
        current_user=current_user,
        owner_group=owner_group.username,
        deployer_service_accounts=[],
    )
    owner_role = await Role.get_by_name(session, result.owner_role)
    assert owner_role is not None
    owner_role.name = f"renamed-{existing_namespace}-owners"
    await session.commit()

    with pytest.raises(DJInvalidInputException, match="overlaps governed boundary"):
        await provision_namespace_boundary(
            session=session,
            namespace=requested_namespace,
            current_user=current_user,
            owner_group=owner_group.username,
            deployer_service_accounts=[],
        )

    assert (
        await NodeNamespace.get(
            session,
            requested_namespace,
            raise_if_not_exists=False,
        )
        is None
    )


async def test_provision_namespace_boundary_rejects_child_under_git_root(
    session,
    current_user: User,
):
    owner_group = _principal("git-root-owners", PrincipalKind.GROUP)
    session.add_all(
        [
            owner_group,
            NodeNamespace(
                namespace="git_root",
                github_repo_path="org/repo",
            ),
        ],
    )
    await session.commit()

    with pytest.raises(DJInvalidInputException, match="Create a new branch"):
        await provision_namespace_boundary(
            session=session,
            namespace="git_root.child",
            current_user=current_user,
            owner_group=owner_group.username,
            deployer_service_accounts=[],
        )

    assert (
        await NodeNamespace.get(
            session,
            "git_root.child",
            raise_if_not_exists=False,
        )
        is None
    )


class TestMergeListWithKey:
    """Tests for _merge_list_with_key function"""

    def test_merge_yaml_list_preserves_attribute_order_when_unchanged(self):
        """Test that attribute order is preserved when the attribute set hasn't changed"""
        from ruamel.yaml import YAML

        yaml = YAML()

        # Create existing list by parsing YAML to get proper CommentedMap structure
        existing_yaml = """
- name: col1
  type: int
  attributes:
    - primary_key
    - dimension
"""
        existing = yaml.load(existing_yaml)

        # Create new list with same attributes but different order
        new_yaml = """
- name: col1
  type: int
  attributes:
    - dimension
    - primary_key
"""
        new_list = yaml.load(new_yaml)

        result = _merge_list_with_key(existing, new_list, "name")

        # Should preserve the original attribute order since the set is unchanged
        assert result[0]["attributes"] == ["primary_key", "dimension"]

    def test_merge_yaml_list_updates_attributes_when_changed(self):
        """Test that attributes are updated when the set changes"""
        # Create existing list with specific attribute order
        existing = CommentedSeq(
            [
                CommentedMap(
                    {
                        "name": "col1",
                        "type": "int",
                        "attributes": ["primary_key", "dimension"],
                    },
                ),
            ],
        )

        # Create new list with different attributes
        new_list = CommentedSeq(
            [
                CommentedMap(
                    {
                        "name": "col1",
                        "type": "int",
                        "attributes": ["primary_key"],  # Removed "dimension"
                    },
                ),
            ],
        )

        result = _merge_list_with_key(existing, new_list, "name")

        # Should update to new attributes since the set changed
        assert result[0]["attributes"] == ["primary_key"]

    def test_merge_yaml_list_handles_non_list_attributes(self):
        """Test that non-list attributes in existing item don't cause issues"""
        # Create existing list with non-list attributes value
        existing = CommentedSeq(
            [
                CommentedMap(
                    {
                        "name": "col1",
                        "type": "int",
                        "attributes": "not_a_list",  # Not a list
                    },
                ),
            ],
        )

        # Create new list with proper list attributes
        new_list = CommentedSeq(
            [
                CommentedMap(
                    {
                        "name": "col1",
                        "type": "int",
                        "attributes": ["primary_key"],
                    },
                ),
            ],
        )

        result = _merge_list_with_key(existing, new_list, "name")

        # Should update since existing wasn't a list
        assert result[0]["attributes"] == ["primary_key"]

    def test_merge_yaml_list_adds_attributes_when_missing_in_existing(self):
        """Test that attributes are added when they don't exist in existing item"""
        # Create existing list without attributes
        existing = CommentedSeq(
            [
                CommentedMap(
                    {
                        "name": "col1",
                        "type": "int",
                    },
                ),
            ],
        )

        # Create new list with attributes
        new_list = CommentedSeq(
            [
                CommentedMap(
                    {
                        "name": "col1",
                        "type": "int",
                        "attributes": ["primary_key"],
                    },
                ),
            ],
        )

        result = _merge_list_with_key(existing, new_list, "name")

        # Should add the new attributes
        assert result[0]["attributes"] == ["primary_key"]

    def test_merge_via_yaml_preserving_comments_with_unchanged_attributes(self):
        """Test attribute order preservation through the full YAML merge flow"""
        yaml = YAML()

        # Create existing YAML with columns that have specific attribute order
        existing_yaml = """
name: test_node
type: transform
columns:
  - name: col1
    type: int
    attributes:
      - primary_key
      - dimension
  - name: col2
    type: string
"""
        existing = yaml.load(existing_yaml)

        # Create new YAML with same attributes but different order
        new_yaml = """
name: test_node
type: transform
columns:
  - name: col1
    type: int
    attributes:
      - dimension
      - primary_key
  - name: col2
    type: string
"""
        new_data = yaml.load(new_yaml)

        # Merge the YAML structures
        result = _merge_yaml_preserving_comments(existing, new_data, yaml)

        # Should preserve the original attribute order since the set is unchanged
        assert result["columns"][0]["attributes"] == ["primary_key", "dimension"]

    def test_merge_yaml_preserves_cube_metrics_dimensions_order(self):
        """Test that cube metrics and dimensions order is preserved from existing YAML"""
        yaml = YAML()

        # Create existing cube YAML with specific order
        existing_yaml = """
node_type: cube
name: my_cube
description: A cube
metrics:
  - metric_z
  - metric_a
  - metric_m
dimensions:
  - dim_y
  - dim_b
  - dim_x
"""
        existing = yaml.load(existing_yaml)

        # Create new YAML with same items but different order (simulating DB order)
        new_yaml = """
node_type: cube
name: my_cube
description: A cube updated
metrics:
  - metric_a
  - metric_m
  - metric_z
dimensions:
  - dim_b
  - dim_x
  - dim_y
"""
        new_data = yaml.load(new_yaml)

        # Merge the YAML structures
        result = _merge_yaml_preserving_comments(existing, new_data, yaml)

        # Should preserve the original order from existing YAML
        assert result["metrics"] == ["metric_z", "metric_a", "metric_m"]
        assert result["dimensions"] == ["dim_y", "dim_b", "dim_x"]
        # Description should be updated though
        assert result["description"] == "A cube updated"

    def test_merge_yaml_cube_adds_new_metrics_at_end(self):
        """Test that new metrics/dimensions are added at the end when preserving order"""
        yaml = YAML()

        # Create existing cube YAML
        existing_yaml = """
node_type: cube
name: my_cube
metrics:
  - metric_a
  - metric_b
dimensions:
  - dim_x
"""
        existing = yaml.load(existing_yaml)

        # Create new YAML with additional items
        new_yaml = """
node_type: cube
name: my_cube
metrics:
  - metric_a
  - metric_b
  - metric_c
dimensions:
  - dim_x
  - dim_y
"""
        new_data = yaml.load(new_yaml)

        # Merge the YAML structures
        result = _merge_yaml_preserving_comments(existing, new_data, yaml)

        # Should preserve existing order and add new items at end
        assert result["metrics"] == ["metric_a", "metric_b", "metric_c"]
        assert result["dimensions"] == ["dim_x", "dim_y"]

    def test_merge_yaml_cube_removes_deleted_metrics(self):
        """Test that removed metrics/dimensions are not included in result"""
        yaml = YAML()

        # Create existing cube YAML
        existing_yaml = """
node_type: cube
name: my_cube
metrics:
  - metric_a
  - metric_b
  - metric_c
dimensions:
  - dim_x
  - dim_y
"""
        existing = yaml.load(existing_yaml)

        # Create new YAML with some items removed
        new_yaml = """
node_type: cube
name: my_cube
metrics:
  - metric_a
  - metric_c
dimensions:
  - dim_x
"""
        new_data = yaml.load(new_yaml)

        # Merge the YAML structures
        result = _merge_yaml_preserving_comments(existing, new_data, yaml)

        # Should only include items that are in new data, in original order
        assert result["metrics"] == ["metric_a", "metric_c"]
        assert result["dimensions"] == ["dim_x"]


class TestNodeSpecToYaml:
    """Tests for node_spec_to_yaml formatting and determinism"""

    def test_owners_are_sorted_on_fresh_dump(self):
        """owners are sorted alphabetically when there is no existing YAML to merge"""
        spec = MetricSpec(
            name="ns.metrics.revenue",
            node_type=NodeType.METRIC,
            owners=["zara@netflix.com", "alice@netflix.com", "bob@netflix.com"],
            query="SELECT SUM(rev) FROM ns.transforms.t",
        )
        assert node_spec_to_yaml(spec).splitlines() == [
            "name: ns.metrics.revenue",
            "node_type: metric",
            "owners:",
            "  - alice@netflix.com",
            "  - bob@netflix.com",
            "  - zara@netflix.com",
            "mode: published",
            "query: SELECT SUM(rev) FROM ns.transforms.t",
        ]

    def test_owners_preserve_order_on_merge(self):
        """owners preserve existing file order when merging with existing YAML"""
        existing_yaml = (
            "name: ns.metrics.revenue\n"
            "node_type: metric\n"
            "owners:\n"
            "  - zara@netflix.com\n"
            "  - alice@netflix.com\n"
            "mode: published\n"
            "query: SELECT SUM(rev) FROM ns.transforms.t\n"
        )
        spec = MetricSpec(
            name="ns.metrics.revenue",
            node_type=NodeType.METRIC,
            owners=["alice@netflix.com", "zara@netflix.com"],
            query="SELECT SUM(rev) FROM ns.transforms.t",
        )
        result = node_spec_to_yaml(spec, existing_yaml=existing_yaml)
        owners_lines = [line for line in result.splitlines() if "@netflix.com" in line]
        assert owners_lines == ["  - zara@netflix.com", "  - alice@netflix.com"]

    def test_tags_are_sorted_on_fresh_dump(self):
        """tags are sorted alphabetically when there is no existing YAML to merge"""
        spec = MetricSpec(
            name="ns.metrics.revenue",
            node_type=NodeType.METRIC,
            tags=["ratio_metric", "core", "finance"],
            query="SELECT SUM(rev) FROM ns.transforms.t",
        )
        assert node_spec_to_yaml(spec).splitlines() == [
            "name: ns.metrics.revenue",
            "node_type: metric",
            "tags:",
            "  - core",
            "  - finance",
            "  - ratio_metric",
            "mode: published",
            "query: SELECT SUM(rev) FROM ns.transforms.t",
        ]

    def test_tags_preserve_order_on_merge(self):
        """tags preserve existing file order when merging with existing YAML"""
        existing_yaml = (
            "name: ns.metrics.revenue\n"
            "node_type: metric\n"
            "tags:\n"
            "  - ratio_metric\n"
            "  - core\n"
            "mode: published\n"
            "query: SELECT SUM(rev) FROM ns.transforms.t\n"
        )
        spec = MetricSpec(
            name="ns.metrics.revenue",
            node_type=NodeType.METRIC,
            tags=["core", "ratio_metric"],
            query="SELECT SUM(rev) FROM ns.transforms.t",
        )
        result = node_spec_to_yaml(spec, existing_yaml=existing_yaml)
        tag_lines = [line for line in result.splitlines() if "  - " in line]
        assert tag_lines == ["  - ratio_metric", "  - core"]

    def test_metric_with_legacy_string_unit_round_trips(self):
        """Metric authored with legacy `unit: dollar` emits `unit: dollar` on export."""
        spec = MetricSpec(
            name="ns.metrics.revenue",
            node_type=NodeType.METRIC,
            unit="dollar",
            query="SELECT SUM(amount) FROM ns.transforms.t",
        )
        output = node_spec_to_yaml(spec)
        assert "unit: dollar" in output
        # Not a structured dict in the legacy case.
        assert "kind:" not in output

    def test_metric_with_structured_unit_emits_at_spec_level(self):
        """Structured `unit:` at the metric spec level emits as a nested dict."""
        spec = MetricSpec(
            name="ns.metrics.revenue_eur",
            node_type=NodeType.METRIC,
            unit={"kind": "currency", "code": "EUR"},
            query="SELECT SUM(amount_eur) FROM ns.transforms.t",
        )
        output = node_spec_to_yaml(spec)
        # Emitted at spec level, not on a columns block.
        assert "unit:" in output
        assert "kind: currency" in output
        assert "code: EUR" in output

    def test_metric_with_compound_unit_emits_nested(self):
        spec = MetricSpec(
            name="ns.metrics.qps",
            node_type=NodeType.METRIC,
            unit={
                "numerator": {"kind": "count"},
                "denominator": {"kind": "time", "code": "s"},
            },
            query="SELECT 1",
        )
        output = node_spec_to_yaml(spec)
        assert "numerator:" in output
        assert "denominator:" in output
        assert "kind: time" in output
        assert "code: s" in output

    def test_metric_export_suppresses_per_column_unit(self):
        """A metric's columns[].unit must NOT appear in YAML export — the metric
        emits its unit at the spec top level instead."""
        spec = MetricSpec(
            name="ns.metrics.revenue",
            node_type=NodeType.METRIC,
            unit={"kind": "currency", "code": "USD"},
            query="SELECT SUM(amount) FROM ns.transforms.t",
            columns=[
                {
                    "name": "revenue",
                    "unit": {"kind": "currency", "code": "USD"},
                },
            ],
        )
        output = node_spec_to_yaml(spec)
        # Spec-level unit is present...
        assert "kind: currency" in output
        # ...but columns block (if present) does not contain a duplicate unit
        if "columns:" in output:
            # Find columns block and inspect its body
            columns_idx = output.find("columns:")
            columns_block = output[columns_idx:]
            # The metric-level `unit:` line lives before `columns:` — so any
            # unit-related text after `columns:` would be a per-column emit.
            assert "unit:" not in columns_block

    def test_column_with_unit_is_exported(self):
        """A column whose only customization is a unit must still be exported.

        `_has_column_customizations` recognizes `unit` alongside display_name,
        description, attributes, and partition; without that, a unit-only
        column would be treated as "unmodified" and silently dropped.
        """
        spec = TransformSpec(
            name="ns.transforms.t",
            node_type=NodeType.TRANSFORM,
            query="SELECT revenue FROM ns.source.s",
            columns=[
                {
                    "name": "revenue",
                    "unit": {"kind": "currency", "code": "USD"},
                },
            ],
        )
        lines = node_spec_to_yaml(spec).splitlines()
        assert "columns:" in lines
        assert "  - name: revenue" in lines
        # Unit appears as a nested mapping; check key + values are present
        unit_idx = next(i for i, line in enumerate(lines) if line.strip() == "unit:")
        # The following lines should be the kind/code mapping
        assert "kind: currency" in lines[unit_idx + 1]
        assert "code: USD" in lines[unit_idx + 2]

    def test_column_without_unit_is_not_exported_with_noise(self):
        """A column with no customizations (including no unit) is omitted from YAML.

        The new `unit` field must not introduce `unit: null` lines on every
        column when no unit is set.
        """
        spec = TransformSpec(
            name="ns.transforms.t",
            node_type=NodeType.TRANSFORM,
            query="SELECT id FROM ns.source.s",
            columns=[{"name": "id"}],  # no unit, no customizations
        )
        output = node_spec_to_yaml(spec)
        assert "unit:" not in output
        assert "columns:" not in output  # column itself filtered out

    def test_compound_unit_round_trips(self):
        """Compound units serialize cleanly and the structure is preserved."""
        spec = TransformSpec(
            name="ns.transforms.t",
            node_type=NodeType.TRANSFORM,
            query="SELECT qps FROM ns.source.s",
            columns=[
                {
                    "name": "qps",
                    "unit": {
                        "numerator": {"kind": "count"},
                        "denominator": {"kind": "time", "code": "s"},
                    },
                },
            ],
        )
        output = node_spec_to_yaml(spec)
        assert "numerator:" in output
        assert "denominator:" in output
        assert "kind: time" in output
        assert "code: s" in output

    def test_column_attributes_are_sorted(self):
        """column attributes are sorted alphabetically regardless of input order"""
        spec = TransformSpec(
            name="ns.transforms.t",
            node_type=NodeType.TRANSFORM,
            query="SELECT id FROM ns.source.s",
            columns=[
                {
                    "name": "id",
                    "display_name": "ID",
                    "attributes": ["dimension", "primary_key"],
                },
            ],
        )
        assert node_spec_to_yaml(spec).splitlines() == [
            "name: ns.transforms.t",
            "node_type: transform",
            "mode: published",
            "columns:",
            "  - name: id",
            "    display_name: ID",
            "    attributes:",
            "      - dimension",
            "      - primary_key",
            "query: SELECT id FROM ns.source.s",
        ]

    def test_output_is_deterministic(self):
        """calling node_spec_to_yaml twice with the same spec gives identical output"""
        spec = MetricSpec(
            name="ns.metrics.revenue",
            node_type=NodeType.METRIC,
            owners=["zara@netflix.com", "alice@netflix.com"],
            tags=["ratio_metric", "core"],
            query="SELECT SUM(rev) FROM ns.transforms.t",
        )
        assert node_spec_to_yaml(spec) == node_spec_to_yaml(spec)

    def test_no_yaml_document_start_marker(self):
        """output does not start with --- (explicit_start=False)"""
        spec = MetricSpec(
            name="ns.metrics.revenue",
            node_type=NodeType.METRIC,
            query="SELECT SUM(rev) FROM ns.transforms.t",
        )
        assert node_spec_to_yaml(spec).splitlines() == [
            "name: ns.metrics.revenue",
            "node_type: metric",
            "mode: published",
            "query: SELECT SUM(rev) FROM ns.transforms.t",
        ]

    def test_metric_reaggregate_exports_dimension_rules(self):
        """metric reaggregate exports the rule list without null future fields"""
        spec = MetricSpec(
            name="ns.metrics.daily_balance",
            node_type=NodeType.METRIC,
            query="SELECT SUM(balance) FROM ns.transforms.accounts",
            reaggregate={
                "rules": [
                    {
                        "dimension": "${prefix}v3.date.date_id[order]",
                        "fn": "last_value",
                    },
                ],
            },
        )
        assert node_spec_to_yaml(spec).splitlines() == [
            "name: ns.metrics.daily_balance",
            "node_type: metric",
            "mode: published",
            "query: SELECT SUM(balance) FROM ns.transforms.accounts",
            "reaggregate:",
            "  rules:",
            "    - dimension: ${prefix}v3.date.date_id[order]",
            "      fn: last_value",
        ]

    def test_multiline_query_uses_literal_block_style(self):
        """multiline queries are serialized with |- literal block style"""
        spec = MetricSpec(
            name="ns.metrics.revenue",
            node_type=NodeType.METRIC,
            query="SELECT SUM(rev)\nFROM ns.transforms.t",
        )
        assert node_spec_to_yaml(spec).splitlines() == [
            "name: ns.metrics.revenue",
            "node_type: metric",
            "mode: published",
            "query: |-",
            "  SELECT SUM(rev)",
            "  FROM ns.transforms.t",
        ]

    def test_short_lists_use_inline_style(self):
        """short lists (owners, tags) use inline [a, b] style after yamlfix"""
        spec = MetricSpec(
            name="ns.metrics.revenue",
            node_type=NodeType.METRIC,
            owners=["alice@netflix.com"],
            tags=["core"],
            query="SELECT SUM(rev) FROM ns.transforms.t",
        )
        assert node_spec_to_yaml(spec).splitlines() == [
            "name: ns.metrics.revenue",
            "node_type: metric",
            "owners:",
            "  - alice@netflix.com",
            "tags:",
            "  - core",
            "mode: published",
            "query: SELECT SUM(rev) FROM ns.transforms.t",
        ]
