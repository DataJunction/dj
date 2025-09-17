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
