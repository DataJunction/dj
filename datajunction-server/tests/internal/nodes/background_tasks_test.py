"""
Tests for background task entry points.

Most of these swallow exceptions rather than propagating them to Starlette's
ServerErrorMiddleware (which would log the generic "Exception in ASGI
application" instead of anything actionable). `derive_frozen_measures` is the
exception: it re-raises a real failure instead of returning a silently
"successful" empty measures list, which relies on `save_column_level_lineage`
always being scheduled before it -- `BackgroundTasks` runs queued tasks
sequentially and stops at the first one that raises.
"""

import logging
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from httpx import AsyncClient

from datajunction_server.internal.materializations import (
    schedule_materialization_jobs_bg,
)
from datajunction_server.internal.nodes import (
    derive_frozen_measures,
    propagate_update_downstream,
    save_column_level_lineage,
)


@pytest.mark.asyncio
async def test_propagate_update_downstream_swallows_exceptions(caplog):
    with patch(
        "datajunction_server.internal.nodes.session_context",
    ) as mock_ctx:
        mock_session = AsyncMock()
        mock_ctx.return_value.__aenter__ = AsyncMock(return_value=mock_session)
        mock_ctx.return_value.__aexit__ = AsyncMock(return_value=False)

        with (
            patch(
                "datajunction_server.internal.nodes._propagate_update_downstream",
                side_effect=RuntimeError("boom"),
            ),
            caplog.at_level(
                logging.ERROR,
                logger="datajunction_server.internal.nodes",
            ),
        ):
            await propagate_update_downstream(
                node=MagicMock(name="test.node"),
                current_user=MagicMock(),
                save_history=MagicMock(),
            )

    assert any("propagating update" in r.message.lower() for r in caplog.records)


@pytest.mark.asyncio
async def test_derive_frozen_measures_reraises_instead_of_swallowing(caplog):
    """
    A failure inside `_derive_frozen_measures_impl` -- extract() rejecting
    the metric, or a real bug -- must not vanish as a silent, successful
    empty measures list. It propagates instead of being swallowed.
    """
    with patch(
        "datajunction_server.internal.nodes.session_context",
    ) as mock_ctx:
        mock_session = AsyncMock()
        mock_ctx.return_value.__aenter__ = AsyncMock(return_value=mock_session)
        mock_ctx.return_value.__aexit__ = AsyncMock(return_value=False)

        with (
            patch(
                "datajunction_server.internal.nodes._background_write_allowed",
                return_value=True,
            ),
            patch(
                "datajunction_server.internal.nodes._derive_frozen_measures_impl",
                side_effect=RuntimeError("boom"),
            ),
            caplog.at_level(
                logging.ERROR,
                logger="datajunction_server.internal.nodes",
            ),
        ):
            with pytest.raises(RuntimeError, match="boom"):
                await derive_frozen_measures(
                    node_revision_id=99,
                    current_user=MagicMock(),
                    access_target=MagicMock(),
                )

    assert any("deriving frozen measures" in r.message.lower() for r in caplog.records)


@pytest.mark.asyncio
async def test_lineage_is_scheduled_before_frozen_measures(
    client_with_roads: AsyncClient,
    mocker,
):
    """
    `BackgroundTasks` (`starlette.background.BackgroundTasks.__call__`) runs
    its queued tasks sequentially and stops at the first one that raises.
    Since `derive_frozen_measures` is now allowed to raise on a real bug,
    `save_column_level_lineage` must be queued *before* it when a metric is
    created -- otherwise a decomposition bug would also silently drop
    lineage for that metric, not just its frozen measures.
    """
    order: list[str] = []

    async def _record_lineage(*args, **kwargs):
        order.append("lineage")

    async def _record_derive(*args, **kwargs):
        order.append("derive")
        return []

    mocker.patch(
        "datajunction_server.internal.nodes.save_column_level_lineage",
        side_effect=_record_lineage,
    )
    mocker.patch(
        "datajunction_server.internal.nodes.derive_frozen_measures",
        side_effect=_record_derive,
    )

    response = await client_with_roads.post(
        "/nodes/metric/",
        json={
            "name": "default.bg_task_order_metric",
            "description": "Exercises background task scheduling order",
            "query": "SELECT COUNT(repair_order_id) FROM default.repair_orders",
            "mode": "published",
        },
    )
    assert response.status_code == 201

    assert order == ["lineage", "derive"]


@pytest.mark.asyncio
async def test_save_column_level_lineage_swallows_exceptions(caplog):
    with patch(
        "datajunction_server.internal.nodes.session_context",
    ) as mock_ctx:
        mock_session = AsyncMock()
        mock_ctx.return_value.__aenter__ = AsyncMock(return_value=mock_session)
        mock_ctx.return_value.__aexit__ = AsyncMock(return_value=False)
        mock_session.execute.side_effect = RuntimeError("boom")

        with (
            patch(
                "datajunction_server.internal.nodes._background_write_allowed",
                return_value=True,
            ),
            caplog.at_level(
                logging.ERROR,
                logger="datajunction_server.internal.nodes",
            ),
        ):
            await save_column_level_lineage(
                node_revision_id=99,
                current_user=MagicMock(),
                access_target=MagicMock(),
            )

    # The exception must be folded into the message itself (not just exc_info),
    # so backends that retain only the formatted message stay diagnosable.
    assert any("column-level lineage" in r.message.lower() for r in caplog.records)
    assert any("boom" in r.message for r in caplog.records)
    assert any("RuntimeError" in r.message for r in caplog.records)


@pytest.mark.asyncio
async def test_schedule_materialization_jobs_bg_swallows_exceptions(caplog):
    with patch(
        "datajunction_server.internal.materializations.session_context",
    ) as mock_ctx:
        mock_session = AsyncMock()
        mock_ctx.return_value.__aenter__ = AsyncMock(return_value=mock_session)
        mock_ctx.return_value.__aexit__ = AsyncMock(return_value=False)

        with patch(
            "datajunction_server.internal.materializations.schedule_materialization_jobs",
            side_effect=RuntimeError("boom"),
        ):
            with caplog.at_level(
                logging.ERROR,
                logger="datajunction_server.internal.materializations",
            ):
                await schedule_materialization_jobs_bg(
                    node_revision_id=99,
                    materialization_names=["mat1"],
                    query_service_client=MagicMock(),
                )

    assert any(
        "scheduling materialization" in r.message.lower() for r in caplog.records
    )
