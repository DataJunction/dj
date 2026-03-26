"""
Tests for the namespaces GraphQL query.
"""

import pytest
from httpx import AsyncClient

NAMESPACES_QUERY = """
{
    listNamespaces {
        namespace
        numNodes
        git {
            __typename
            ... on GitRootConfig {
                repo
                path
                defaultBranch
            }
            ... on GitBranchConfig {
                branch
                gitOnly
                parentNamespace
                root {
                    repo
                    path
                    defaultBranch
                }
            }
        }
    }
}
"""


@pytest.mark.asyncio
async def test_list_namespaces_no_git(
    client: AsyncClient,
) -> None:
    """
    Non-git namespaces have git=null.
    """
    await client.post("/namespaces/foo/")
    await client.post("/namespaces/foo.bar/")

    response = await client.post("/graphql", json={"query": NAMESPACES_QUERY})
    assert response.status_code == 200
    data = response.json()["data"]["listNamespaces"]
    namespaces = {ns["namespace"]: ns for ns in data}

    assert "foo" in namespaces
    assert "foo.bar" in namespaces
    assert namespaces["foo"]["numNodes"] == 0
    assert namespaces["foo"]["git"] is None


@pytest.mark.asyncio
async def test_list_namespaces_git_root(
    client: AsyncClient,
) -> None:
    """
    Git root namespaces have git.__typename == GitRootConfig.
    """
    await client.post("/namespaces/myproject/")
    await client.patch(
        "/namespaces/myproject/git",
        json={
            "github_repo_path": "owner/repo",
            "git_path": "definitions/",
            "default_branch": "main",
        },
    )

    response = await client.post("/graphql", json={"query": NAMESPACES_QUERY})
    assert response.status_code == 200
    data = response.json()["data"]["listNamespaces"]
    namespaces = {ns["namespace"]: ns for ns in data}

    assert "myproject" in namespaces
    ns = namespaces["myproject"]
    assert ns["git"]["__typename"] == "GitRootConfig"
    assert ns["git"]["repo"] == "owner/repo"
    assert ns["git"]["path"] == "definitions/"
    assert ns["git"]["defaultBranch"] == "main"


@pytest.mark.asyncio
async def test_list_namespaces_git_branch(
    client: AsyncClient,
) -> None:
    """
    Branch namespaces have git.__typename == GitBranchConfig with root embedded.
    """
    await client.post("/namespaces/myproject/")
    await client.patch(
        "/namespaces/myproject/git",
        json={
            "github_repo_path": "owner/repo",
            "git_path": "definitions/",
            "default_branch": "main",
        },
    )
    await client.post("/namespaces/myproject.feature_x/")
    await client.patch(
        "/namespaces/myproject.feature_x/git",
        json={
            "git_branch": "feature-x",
            "parent_namespace": "myproject",
        },
    )

    response = await client.post("/graphql", json={"query": NAMESPACES_QUERY})
    assert response.status_code == 200
    data = response.json()["data"]["listNamespaces"]
    namespaces = {ns["namespace"]: ns for ns in data}

    assert "myproject.feature_x" in namespaces
    ns = namespaces["myproject.feature_x"]
    assert ns["git"]["__typename"] == "GitBranchConfig"
    assert ns["git"]["branch"] == "feature-x"
    assert ns["git"]["gitOnly"] is False
    assert ns["git"]["parentNamespace"] == "myproject"
    assert ns["git"]["root"]["repo"] == "owner/repo"
    assert ns["git"]["root"]["path"] == "definitions/"
    assert ns["git"]["root"]["defaultBranch"] == "main"
