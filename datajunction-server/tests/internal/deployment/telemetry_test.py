"""Deployment execution metrics must include unsuccessful attempts."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.internal.deployment.orchestrator import (
    DeploymentExecuteResult,
    DeploymentOrchestrator,
)
from datajunction_server.models.deployment import (
    DeploymentResult,
    DeploymentSpec,
    GitDeploymentSource,
    LocalDeploymentSource,
)

MODULE = "datajunction_server.internal.deployment.orchestrator"


@pytest.fixture
def orchestrator():
    instance = DeploymentOrchestrator(
        deployment_spec=DeploymentSpec(namespace="telemetry", nodes=[]),
        deployment_id="deployment-id",
        session=MagicMock(spec=AsyncSession),
        context=MagicMock(),
    )
    instance._plan_and_execute = AsyncMock()
    instance._apply_cube_materialization_swaps = AsyncMock()
    instance._stop_deleted_node_materializations = MagicMock(return_value=[])
    return instance


@pytest.mark.parametrize("dry_run", [False, True])
@pytest.mark.parametrize(
    "source",
    [None, LocalDeploymentSource(), GitDeploymentSource(repository="org/repo")],
)
@pytest.mark.parametrize("result_status", list(DeploymentResult.Status))
async def test_execution_outcome(orchestrator, dry_run, source, result_status):
    orchestrator.dry_run = dry_run
    orchestrator.deployment_spec.source = source
    result = DeploymentExecuteResult(
        results=[
            DeploymentResult(
                name="node",
                deploy_type=DeploymentResult.Type.NODE,
                operation=DeploymentResult.Operation.CREATE,
                status=result_status,
            ),
        ],
        downstream_impacts=[],
    )
    orchestrator._plan_and_execute.return_value = result

    with patch(f"{MODULE}.get_metrics_provider") as provider:
        assert await orchestrator.execute() is result

    tags = {
        "namespace": "telemetry",
        "dry_run": str(dry_run),
        "source_type": source.type.value if source else "local",
        "status": "failed"
        if result_status == DeploymentResult.Status.FAILED
        else "success",
    }
    provider.return_value.counter.assert_called_once_with(
        "dj.deployments.execute",
        tags=tags,
    )
    name, elapsed_ms, timer_tags = provider.return_value.timer.call_args.args
    assert name == "dj.deployments.execute_latency_ms"
    assert elapsed_ms >= 0
    assert timer_tags == tags
    provider.return_value.timer.assert_called_once()
    if dry_run:
        orchestrator.session.commit.assert_not_awaited()
        orchestrator._apply_cube_materialization_swaps.assert_not_awaited()
    else:
        orchestrator.session.commit.assert_awaited_once()
        orchestrator._apply_cube_materialization_swaps.assert_awaited_once()


@pytest.mark.parametrize("stage", ["plan", "commit", "materialization"])
@pytest.mark.parametrize("error_type", [ValueError, asyncio.CancelledError])
async def test_failed_execution_is_timed_and_propagated(
    orchestrator,
    stage,
    error_type,
):
    orchestrator._plan_and_execute.return_value = DeploymentExecuteResult(
        results=[],
        downstream_impacts=[],
    )
    error = error_type("deployment stopped")
    target = {
        "plan": orchestrator._plan_and_execute,
        "commit": orchestrator.session.commit,
        "materialization": orchestrator._apply_cube_materialization_swaps,
    }[stage]
    target.side_effect = error

    with patch(f"{MODULE}.get_metrics_provider") as provider:
        with pytest.raises(error_type) as raised:
            await orchestrator.execute()

    assert raised.value is error
    tags = {
        "namespace": "telemetry",
        "dry_run": "False",
        "source_type": "local",
        "status": "cancelled" if error_type is asyncio.CancelledError else "failed",
    }
    provider.return_value.counter.assert_called_once_with(
        "dj.deployments.execute",
        tags=tags,
    )
    provider.return_value.timer.assert_called_once()
    name, elapsed_ms, timer_tags = provider.return_value.timer.call_args.args
    assert (name, timer_tags) == ("dj.deployments.execute_latency_ms", tags)
    assert elapsed_ms >= 0


@pytest.mark.parametrize("metric_stage", ["provider", "timer", "counter"])
@pytest.mark.parametrize("error_type", [None, ValueError, asyncio.CancelledError])
async def test_metrics_failure_preserves_deployment_outcome(
    orchestrator,
    caplog,
    metric_stage,
    error_type,
):
    result = DeploymentExecuteResult(results=[], downstream_impacts=[])
    error = error_type("deployment stopped") if error_type else None
    orchestrator._execute = AsyncMock(return_value=result, side_effect=error)
    metrics_error = RuntimeError("metrics unavailable")

    with patch(f"{MODULE}.get_metrics_provider") as provider:
        target = {
            "provider": provider,
            "timer": provider.return_value.timer,
            "counter": provider.return_value.counter,
        }[metric_stage]
        target.side_effect = metrics_error

        if error_type:
            with pytest.raises(error_type) as raised:
                await orchestrator.execute()
            assert raised.value is error
        else:
            assert await orchestrator.execute() is result

    assert "Failed to record deployment execution metrics" in caplog.text
    assert any(record.exc_info[1] is metrics_error for record in caplog.records)
