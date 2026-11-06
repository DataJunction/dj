from http import HTTPStatus

import pytest
from sqlalchemy import select

from datajunction_server.database.namespace import NodeNamespace
from datajunction_server.database.rbac import Role, RoleAssignment, RoleScope
from datajunction_server.internal.access.authorization import RBACAuthorizationService
from datajunction_server.models.access import ResourceAction, ResourceType
from tests.authz import VALIDATOR_AUTH_SERVICE


@pytest.mark.parametrize("boundary_at_branch", [False, True])
@pytest.mark.parametrize("grant", ["write", "delete", "manage"])
async def test_branch_deletion_requires_manage_for_governed_boundaries(
    client,
    session,
    current_user,
    mocker,
    boundary_at_branch,
    grant,
):
    root = "branchguard"
    branch = f"{root}.feature"
    boundary = branch if boundary_at_branch else f"{branch}.metrics"
    session.add(NodeNamespace(namespace=root, github_repo_path="example/repo"))
    await session.flush()
    session.add(
        NodeNamespace(
            namespace=branch,
            parent_namespace=root,
            git_branch="feature",
            is_governed_boundary=boundary_at_branch,
        ),
    )
    if not boundary_at_branch:
        session.add(NodeNamespace(namespace=boundary, is_governed_boundary=True))
    role = Role(name="branch-deleter", created_by_id=current_user.id)
    session.add(role)
    await session.flush()
    session.add(
        RoleAssignment(
            principal_id=current_user.id,
            role_id=role.id,
            granted_by_id=current_user.id,
        ),
    )
    session.add_all(
        RoleScope(
            role_id=role.id,
            action=action,
            scope_type=ResourceType.NAMESPACE,
            scope_value=scope,
        )
        for scope, action in [
            (root, ResourceAction.WRITE),
            (f"{root}.*", ResourceAction(grant)),
        ]
    )
    await session.commit()

    service_settings = mocker.patch(
        "datajunction_server.internal.access.authorization.service.settings",
    )
    service_settings.default_access_policy = "permissive"
    service_settings.restrictive_scopes = []
    mocker.patch(
        "datajunction_server.internal.access.authorization.context.settings",
    ).default_access_role = None
    mocker.patch(VALIDATOR_AUTH_SERVICE, lambda: RBACAuthorizationService())
    github = mocker.patch("datajunction_server.api.branches.GitHubService").return_value
    github.delete_branch = mocker.AsyncMock()

    response = await client.delete(f"/namespaces/{root}/branches/{branch}")

    remaining = set((await session.scalars(select(NodeNamespace.namespace))).all())
    assert root in remaining
    if grant == "manage":
        assert response.status_code == HTTPStatus.OK, response.text
        assert branch not in remaining
        assert boundary not in remaining
        github.delete_branch.assert_awaited_once_with(
            repo_path="example/repo",
            branch="feature",
        )
    else:
        assert response.status_code == HTTPStatus.FORBIDDEN, response.text
        assert {branch, boundary} <= remaining
        github.delete_branch.assert_not_awaited()
