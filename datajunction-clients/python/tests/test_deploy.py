import io
from pathlib import Path
import time
from unittest import mock
import pytest
from unittest.mock import MagicMock
from datajunction.deployment import DeploymentService
from datajunction.exceptions import DJClientException, DJDeploymentFailure
import yaml
from rich.console import Console


def test_clean_dict_removes_nones_and_empty():
    dirty = {
        "a": None,
        "b": [],
        "c": {},
        "d": {"x": None, "y": {"z": []}, "k": "keep"},
        "e": [1, 2],
    }
    cleaned = DeploymentService.clean_dict(dirty)
    assert cleaned == {"d": {"k": "keep"}, "e": [1, 2]}


def test_filter_node_for_export_removes_columns_without_customizations():
    """Columns without meaningful customizations should be removed."""
    node = {
        "name": "test.node",
        "query": "SELECT * FROM foo",
        "columns": [
            # Should be kept: has custom display_name
            {"name": "user_id", "type": "INT", "display_name": "User ID"},
            # Should be removed: display_name same as name
            {"name": "created_at", "type": "TIMESTAMP", "display_name": "created_at"},
            # Should be kept: has attributes
            {"name": "id", "type": "BIGINT", "attributes": ["primary_key"]},
            # Should be removed: no customizations
            {"name": "plain_col", "type": "VARCHAR"},
            # Should be kept: has description
            {"name": "desc_col", "type": "TEXT", "description": "A useful column"},
        ],
    }

    filtered = DeploymentService.filter_node_for_export(node)

    # Only columns with customizations should remain
    assert len(filtered["columns"]) == 3

    # Type should be excluded from all columns
    for col in filtered["columns"]:
        assert "type" not in col

    # Check correct columns were kept
    col_names = [c["name"] for c in filtered["columns"]]
    assert "user_id" in col_names
    assert "id" in col_names
    assert "desc_col" in col_names
    assert "created_at" not in col_names
    assert "plain_col" not in col_names


def test_filter_node_for_export_removes_columns_key_when_empty():
    """If no columns have customizations, the columns key should be removed."""
    node = {
        "name": "test.node",
        "query": "SELECT * FROM foo",
        "columns": [
            {"name": "a", "type": "INT"},
            {"name": "b", "type": "VARCHAR", "display_name": "b"},  # same as name
        ],
    }

    filtered = DeploymentService.filter_node_for_export(node)

    assert "columns" not in filtered


def test_filter_node_for_export_always_removes_columns_for_cubes():
    """Cube columns should always be removed - they're inferred from metrics/dimensions."""
    node = {
        "name": "test.cube",
        "node_type": "cube",
        "metrics": ["test.metric1", "test.metric2"],
        "dimensions": ["test.dim1"],
        "columns": [
            {"name": "metric1", "type": "BIGINT"},
            {"name": "dim1", "type": "VARCHAR", "display_name": "Dimension 1"},
        ],
    }

    filtered = DeploymentService.filter_node_for_export(node)

    # Columns should be removed regardless of customizations
    assert "columns" not in filtered
    # Other fields should remain
    assert filtered["metrics"] == ["test.metric1", "test.metric2"]
    assert filtered["dimensions"] == ["test.dim1"]


def test_pull_writes_yaml_files(tmp_path):
    # fake client returning a minimal deployment spec
    client = MagicMock()
    client._export_namespace_spec.return_value = {
        "namespace": "foo.bar",
        "nodes": [
            {"name": "foo.bar.baz", "query": "SELECT 1"},
            {"name": "foo.bar.qux", "query": "SELECT 2"},
        ],
    }
    svc = DeploymentService(client)

    svc.pull("foo.bar", tmp_path)

    # project-level yaml
    project_yaml = yaml.safe_load((tmp_path / "dj.yaml").read_text())
    assert project_yaml["namespace"] == "foo.bar"

    # node files
    baz_file = tmp_path / "foo" / "bar" / "baz.yaml"
    assert baz_file.exists()
    assert yaml.safe_load(baz_file.read_text())["query"] == "SELECT 1"

    qux_file = tmp_path / "foo" / "bar" / "qux.yaml"
    assert qux_file.exists()


def test_pull_raises_if_target_not_empty(tmp_path):
    (tmp_path / "something.txt").write_text("not empty")
    client = MagicMock()
    svc = DeploymentService(client)
    with pytest.raises(DJClientException):
        svc.pull("ns", tmp_path)


def test_build_table_has_expected_columns():
    tbl = DeploymentService.build_table(
        "abc-123",
        {
            "namespace": "some.namespace",
            "status": "success",
            "results": [
                {
                    "deploy_type": "node",
                    "name": "some.random.node",
                    "operation": "create",
                    "status": "success",
                    "message": "ok",
                },
            ],
        },
    )
    cols = [c.header for c in tbl.columns]
    assert cols == ["Type", "Name", "Operation", "Status", "Message"]
    row_values = [col._cells[0] for col in tbl.columns]
    assert row_values == [
        "node",
        "some.random.node",
        "create",
        "[bold green]success[/bold green]",
        "[gray]ok[/gray]",
    ]


def test_reconstruct_deployment_spec(tmp_path):
    # set up a fake exported project
    (tmp_path / "dj.yaml").write_text(
        yaml.safe_dump({"namespace": "foo", "tags": ["t1"]}),
    )
    node_dir = tmp_path / "foo"
    node_dir.mkdir()
    node_file = node_dir / "bar.yaml"
    node_file.write_text(yaml.safe_dump({"name": "foo.bar", "query": "SELECT 1"}))

    svc = DeploymentService(MagicMock())
    spec = svc._reconstruct_deployment_spec(tmp_path)
    assert spec["namespace"] == "foo"
    assert spec["tags"] == ["t1"]
    assert spec["nodes"][0]["name"] == "foo.bar"


@pytest.mark.timeout(2)
def test_push_waits_until_success(monkeypatch, tmp_path):
    # Create a fake project structure so _reconstruct_deployment_spec returns something
    (tmp_path / "dj.yaml").write_text(yaml.safe_dump({"namespace": "foo"}))
    (tmp_path / "foo.yaml").write_text(yaml.safe_dump({"name": "foo.bar"}))

    # Fake client that returns "pending" once then "success"
    client = MagicMock()
    responses = [
        {"uuid": "123", "status": "pending", "results": [], "namespace": "foo"},
        {"uuid": "123", "status": "success", "results": [], "namespace": "foo"},
    ]
    client.deploy.return_value = responses[0]
    client.check_deployment.side_effect = responses[1:]

    svc = DeploymentService(client, console=Console(file=io.StringIO()))
    monkeypatch.setattr(time, "sleep", lambda _: None)

    svc.push(tmp_path)  # should not raise

    client.deploy.assert_called_once()
    client.check_deployment.assert_called()


def test_push_times_out(monkeypatch, tmp_path):
    # minimal project structure so _reconstruct_deployment_spec works
    (tmp_path / "dj.yaml").write_text(yaml.safe_dump({"namespace": "foo"}))
    (tmp_path / "foo.yaml").write_text(yaml.safe_dump({"name": "foo.bar"}))

    # Fake client: deploy returns a uuid, check_deployment always 'pending'
    client = MagicMock()
    client.deploy.return_value = {
        "uuid": "123",
        "status": "pending",
        "results": [],
        "namespace": "foo",
    }
    client.check_deployment.return_value = {
        "uuid": "123",
        "status": "pending",
        "results": [],
        "namespace": "foo",
    }

    svc = DeploymentService(client, console=Console(file=io.StringIO()))

    # Patch time.sleep to skip waiting
    monkeypatch.setattr(time, "sleep", lambda _: None)

    # Simulate time moving past the timeout on the second call
    start_time = 1_000_000
    times = [start_time, start_time + 301]  # second call is > 5 minutes later
    monkeypatch.setattr(time, "time", lambda: times.pop(0))

    with pytest.raises(DJClientException, match="Deployment timed out"):
        svc.push(tmp_path)

    client.deploy.assert_called_once()
    client.check_deployment.assert_called()


@pytest.mark.timeout(2)
def test_push_raises_on_failed_deployment(monkeypatch, tmp_path):
    (tmp_path / "dj.yaml").write_text(yaml.safe_dump({"namespace": "foo"}))
    (tmp_path / "foo.yaml").write_text(yaml.safe_dump({"name": "foo.bar"}))

    failed_results = [
        {
            "deploy_type": "node",
            "name": "foo.bar",
            "operation": "create",
            "status": "failed",
            "message": "Column `x` does not exist",
        },
    ]
    client = MagicMock()
    client.deploy.return_value = {
        "uuid": "456",
        "status": "failed",
        "results": failed_results,
        "namespace": "foo",
    }

    svc = DeploymentService(client, console=Console(file=io.StringIO()))
    monkeypatch.setattr(time, "sleep", lambda _: None)

    with pytest.raises(DJDeploymentFailure) as exc_info:
        svc.push(tmp_path)

    assert "foo" in exc_info.value.message
    assert len(exc_info.value.errors) == 1
    assert exc_info.value.errors[0]["name"] == "foo.bar"


@pytest.mark.timeout(2)
def test_push_raises_after_polling_to_failure(monkeypatch, tmp_path):
    (tmp_path / "dj.yaml").write_text(yaml.safe_dump({"namespace": "ns"}))
    (tmp_path / "ns.yaml").write_text(yaml.safe_dump({"name": "ns.node"}))

    client = MagicMock()
    client.deploy.return_value = {
        "uuid": "789",
        "status": "pending",
        "results": [],
        "namespace": "ns",
    }
    client.check_deployment.side_effect = [
        {"uuid": "789", "status": "pending", "results": [], "namespace": "ns"},
        {
            "uuid": "789",
            "status": "failed",
            "results": [
                {
                    "deploy_type": "general",
                    "name": "DJInvalidDeploymentConfig",
                    "operation": "unknown",
                    "status": "failed",
                    "message": "Missing dependencies: source.foo",
                },
            ],
            "namespace": "ns",
        },
    ]

    svc = DeploymentService(client, console=Console(file=io.StringIO()))
    monkeypatch.setattr(time, "sleep", lambda _: None)

    with pytest.raises(DJDeploymentFailure) as exc_info:
        svc.push(tmp_path)

    assert exc_info.value.errors[0]["message"] == "Missing dependencies: source.foo"
    assert client.check_deployment.call_count == 2


def test_read_project_yaml_returns_empty(tmp_path: Path):
    """
    Verify that when dj.yaml is missing, _read_project_yaml returns an empty dict.
    """
    # Create a directory without dj.yaml
    project_dir = tmp_path
    client = mock.MagicMock()
    svc = DeploymentService(client, console=Console(file=io.StringIO()))
    result = svc._read_project_yaml(project_dir)
    assert result == {}


class TestBuildDeploymentSource:
    """Tests for _build_deployment_source method."""

    def test_no_git_env_vars_returns_local_source(self, monkeypatch):
        """When no DJ_DEPLOY_REPO is set, returns local source with hostname."""
        # Ensure none of the git-related env vars are set
        monkeypatch.delenv("DJ_DEPLOY_REPO", raising=False)
        monkeypatch.delenv("DJ_DEPLOY_BRANCH", raising=False)
        monkeypatch.delenv("DJ_DEPLOY_COMMIT", raising=False)
        monkeypatch.delenv("DJ_DEPLOY_CI_SYSTEM", raising=False)
        monkeypatch.delenv("DJ_DEPLOY_CI_RUN_URL", raising=False)

        result = DeploymentService._build_deployment_source()

        # Now we always track local deploys
        assert result is not None
        assert result["type"] == "local"
        assert "hostname" in result

    def test_git_source_with_all_fields(self, monkeypatch):
        """When DJ_DEPLOY_REPO is set, returns git source with all fields."""
        monkeypatch.setenv("DJ_DEPLOY_REPO", "github.com/org/repo")
        monkeypatch.setenv("DJ_DEPLOY_BRANCH", "main")
        monkeypatch.setenv("DJ_DEPLOY_COMMIT", "abc123")
        monkeypatch.setenv("DJ_DEPLOY_CI_SYSTEM", "github-actions")
        monkeypatch.setenv(
            "DJ_DEPLOY_CI_RUN_URL",
            "https://github.com/org/repo/actions/runs/123",
        )

        result = DeploymentService._build_deployment_source()

        assert result is not None
        assert result["type"] == "git"
        assert result["repository"] == "github.com/org/repo"
        assert result["branch"] == "main"
        assert result["commit_sha"] == "abc123"
        assert result["ci_system"] == "github-actions"
        assert result["ci_run_url"] == "https://github.com/org/repo/actions/runs/123"

    def test_git_source_with_only_repo(self, monkeypatch):
        """When only DJ_DEPLOY_REPO is set, other fields are omitted."""
        monkeypatch.setenv("DJ_DEPLOY_REPO", "github.com/org/repo")
        monkeypatch.delenv("DJ_DEPLOY_BRANCH", raising=False)
        monkeypatch.delenv("DJ_DEPLOY_COMMIT", raising=False)
        monkeypatch.delenv("DJ_DEPLOY_CI_SYSTEM", raising=False)
        monkeypatch.delenv("DJ_DEPLOY_CI_RUN_URL", raising=False)

        result = DeploymentService._build_deployment_source()

        assert result is not None
        assert result["type"] == "git"
        assert result["repository"] == "github.com/org/repo"
        assert "branch" not in result
        assert "commit_sha" not in result
        assert "ci_system" not in result
        assert "ci_run_url" not in result

    def test_local_source_when_track_local_true(self, monkeypatch):
        """When DJ_DEPLOY_TRACK_LOCAL=true, returns local source."""
        monkeypatch.delenv("DJ_DEPLOY_REPO", raising=False)
        monkeypatch.setenv("DJ_DEPLOY_TRACK_LOCAL", "true")
        monkeypatch.setenv("DJ_DEPLOY_REASON", "testing locally")

        result = DeploymentService._build_deployment_source()

        assert result is not None
        assert result["type"] == "local"
        assert "hostname" in result  # should be set to socket.gethostname()
        assert result["reason"] == "testing locally"

    def test_local_source_track_local_case_insensitive(self, monkeypatch):
        """DJ_DEPLOY_TRACK_LOCAL should be case-insensitive."""
        monkeypatch.delenv("DJ_DEPLOY_REPO", raising=False)
        monkeypatch.setenv("DJ_DEPLOY_TRACK_LOCAL", "TRUE")
        monkeypatch.delenv("DJ_DEPLOY_REASON", raising=False)

        result = DeploymentService._build_deployment_source()

        assert result is not None
        assert result["type"] == "local"

    def test_git_takes_precedence_over_local(self, monkeypatch):
        """When both DJ_DEPLOY_REPO and DJ_DEPLOY_TRACK_LOCAL are set, git wins."""
        monkeypatch.setenv("DJ_DEPLOY_REPO", "github.com/org/repo")
        monkeypatch.setenv("DJ_DEPLOY_TRACK_LOCAL", "true")

        result = DeploymentService._build_deployment_source()

        assert result is not None
        assert result["type"] == "git"

    def test_reconstruct_deployment_spec_includes_source(self, tmp_path, monkeypatch):
        """_reconstruct_deployment_spec should include source when env vars are set."""
        # Set up project files
        (tmp_path / "dj.yaml").write_text(yaml.safe_dump({"namespace": "test"}))
        (tmp_path / "node.yaml").write_text(yaml.safe_dump({"name": "test.node"}))

        # Set env vars for git source
        monkeypatch.setenv("DJ_DEPLOY_REPO", "github.com/test/repo")
        monkeypatch.setenv("DJ_DEPLOY_BRANCH", "feature-branch")

        svc = DeploymentService(MagicMock())
        spec = svc._reconstruct_deployment_spec(tmp_path)

        assert "source" in spec
        assert spec["source"]["type"] == "git"
        assert spec["source"]["repository"] == "github.com/test/repo"
        assert spec["source"]["branch"] == "feature-branch"

    def test_reconstruct_deployment_spec_local_source_without_repo_env_var(
        self,
        tmp_path,
        monkeypatch,
    ):
        """_reconstruct_deployment_spec should include local source when no repo env var."""
        # Set up project files
        (tmp_path / "dj.yaml").write_text(yaml.safe_dump({"namespace": "test"}))
        (tmp_path / "node.yaml").write_text(yaml.safe_dump({"name": "test.node"}))

        # Ensure no git env vars are set
        monkeypatch.delenv("DJ_DEPLOY_REPO", raising=False)

        svc = DeploymentService(MagicMock())
        spec = svc._reconstruct_deployment_spec(tmp_path)

        # Now we always track local deploys
        assert "source" in spec
        assert spec["source"]["type"] == "local"
        assert "hostname" in spec["source"]


class TestGetImpact:
    """Tests for the get_impact method."""

    def test_get_impact_calls_api(self, tmp_path, monkeypatch):
        """get_impact should call the deployment impact API."""
        # Set up project files
        (tmp_path / "dj.yaml").write_text(yaml.safe_dump({"namespace": "test.ns"}))
        (tmp_path / "node.yaml").write_text(
            yaml.safe_dump({"name": "test.ns.my_node", "node_type": "source"}),
        )

        # Ensure no git env vars are set for cleaner test
        monkeypatch.delenv("DJ_DEPLOY_REPO", raising=False)

        # Create mock client
        mock_client = MagicMock()
        mock_client.get_deployment_impact.return_value = {
            "namespace": "test.ns",
            "changes": [],
            "create_count": 0,
            "update_count": 0,
            "delete_count": 0,
            "skip_count": 1,
            "downstream_impacts": [],
            "will_invalidate_count": 0,
            "may_affect_count": 0,
            "warnings": [],
        }

        svc = DeploymentService(mock_client)
        result = svc.get_impact(tmp_path)

        # Verify the API was called
        mock_client.get_deployment_impact.assert_called_once()
        call_args = mock_client.get_deployment_impact.call_args[0][0]
        assert call_args["namespace"] == "test.ns"
        assert "nodes" in call_args

        # Verify the result is returned
        assert result["namespace"] == "test.ns"
        assert result["skip_count"] == 1

    def test_get_impact_with_namespace_override(self, tmp_path, monkeypatch):
        """get_impact should respect namespace override."""
        # Set up project files
        (tmp_path / "dj.yaml").write_text(yaml.safe_dump({"namespace": "original.ns"}))
        (tmp_path / "node.yaml").write_text(yaml.safe_dump({"name": "test.node"}))

        monkeypatch.delenv("DJ_DEPLOY_REPO", raising=False)

        mock_client = MagicMock()
        mock_client.get_deployment_impact.return_value = {"namespace": "override.ns"}

        svc = DeploymentService(mock_client)
        svc.get_impact(tmp_path, namespace="override.ns")

        # Verify the namespace was overridden in the API call
        call_args = mock_client.get_deployment_impact.call_args[0][0]
        assert call_args["namespace"] == "override.ns"
