import io
from pathlib import Path
import time
from unittest import mock
import pytest
from unittest.mock import MagicMock
from datajunction.deployment import DeploymentService
from datajunction.exceptions import DJClientException
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

    def test_no_env_vars_returns_none(self, monkeypatch):
        """When no DJ_DEPLOY_* env vars are set, returns None."""
        # Ensure none of the relevant env vars are set
        monkeypatch.delenv("DJ_DEPLOY_REPO", raising=False)
        monkeypatch.delenv("DJ_DEPLOY_BRANCH", raising=False)
        monkeypatch.delenv("DJ_DEPLOY_COMMIT", raising=False)
        monkeypatch.delenv("DJ_DEPLOY_CI_SYSTEM", raising=False)
        monkeypatch.delenv("DJ_DEPLOY_CI_RUN_URL", raising=False)
        monkeypatch.delenv("DJ_DEPLOY_TRACK_LOCAL", raising=False)

        result = DeploymentService._build_deployment_source()
        assert result is None

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

    def test_reconstruct_deployment_spec_no_source_without_env_vars(
        self,
        tmp_path,
        monkeypatch,
    ):
        """_reconstruct_deployment_spec should not include source when no env vars."""
        # Set up project files
        (tmp_path / "dj.yaml").write_text(yaml.safe_dump({"namespace": "test"}))
        (tmp_path / "node.yaml").write_text(yaml.safe_dump({"name": "test.node"}))

        # Ensure no env vars are set
        monkeypatch.delenv("DJ_DEPLOY_REPO", raising=False)
        monkeypatch.delenv("DJ_DEPLOY_TRACK_LOCAL", raising=False)

        svc = DeploymentService(MagicMock())
        spec = svc._reconstruct_deployment_spec(tmp_path)

        assert "source" not in spec
