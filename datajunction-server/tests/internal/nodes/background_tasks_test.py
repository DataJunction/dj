"""
Tests that background task entry points swallow exceptions rather than
propagating them to Starlette's ServerErrorMiddleware (which would log the
generic "Exception in ASGI application" instead of anything actionable).
"""

import logging
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from datajunction_server.internal.nodes import (
    derive_frozen_measures,
    propagate_update_downstream,
    save_column_level_lineage,
)
from datajunction_server.internal.materializations import (
    schedule_materialization_jobs_bg,
)


@pytest.mark.asyncio
async def test_propagate_update_downstream_swallows_exceptions(caplog):
    with patch(
        "datajunction_server.internal.nodes.session_context",
    ) as mock_ctx:
        mock_session = AsyncMock()
        mock_ctx.return_value.__aenter__ = AsyncMock(return_value=mock_session)
        mock_ctx.return_value.__aexit__ = AsyncMock(return_value=False)

        with patch(
            "datajunction_server.internal.nodes._propagate_update_downstream",
            side_effect=RuntimeError("boom"),
        ):
            with caplog.at_level(
                logging.ERROR,
                logger="datajunction_server.internal.nodes",
            ):
                await propagate_update_downstream(
                    "test.node",
                    "v1.0",
                    current_user=MagicMock(),
                    save_history=MagicMock(),
                )

    assert any("propagating update" in r.message.lower() for r in caplog.records)


@pytest.mark.asyncio
async def test_derive_frozen_measures_swallows_exceptions(caplog):
    with patch(
        "datajunction_server.internal.nodes.session_context",
    ) as mock_ctx:
        mock_session = AsyncMock()
        mock_ctx.return_value.__aenter__ = AsyncMock(return_value=mock_session)
        mock_ctx.return_value.__aexit__ = AsyncMock(return_value=False)

        with patch(
            "datajunction_server.internal.nodes._derive_frozen_measures_impl",
            side_effect=RuntimeError("boom"),
        ):
            with caplog.at_level(
                logging.ERROR,
                logger="datajunction_server.internal.nodes",
            ):
                result = await derive_frozen_measures(node_revision_id=99)

    assert result == []
    assert any("deriving frozen measures" in r.message.lower() for r in caplog.records)


@pytest.mark.asyncio
async def test_save_column_level_lineage_swallows_exceptions(caplog):
    with patch(
        "datajunction_server.internal.nodes.session_context",
    ) as mock_ctx:
        mock_session = AsyncMock()
        mock_ctx.return_value.__aenter__ = AsyncMock(return_value=mock_session)
        mock_ctx.return_value.__aexit__ = AsyncMock(return_value=False)
        mock_session.execute.side_effect = RuntimeError("boom")

        with caplog.at_level(
            logging.ERROR,
            logger="datajunction_server.internal.nodes",
        ):
            await save_column_level_lineage(node_revision_id=99)

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
