"""
Tests for building nodes and extracting dependencies
"""

from unittest.mock import patch
import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.construction.utils import get_dj_node
from datajunction_server.errors import DJErrorException
from datajunction_server.models.node_type import NodeType


@pytest.mark.asyncio
async def test_get_dj_node_raise_unknown_node_exception(session: AsyncSession):
    """
    Test raising an unknown node exception when calling get_dj_node
    """
    with patch(
        "datajunction_server.construction.utils.session_context",
    ) as mock_session_context:
        mock_session_context.return_value = session
        with pytest.raises(DJErrorException) as exc_info:
            await get_dj_node("foobar")

        assert "No node" in str(exc_info.value)

        with pytest.raises(DJErrorException) as exc_info:
            await get_dj_node(
                "foobar",
                kinds={NodeType.METRIC, NodeType.DIMENSION},
            )

        assert "dimension" in str(exc_info.value)
        assert "metric" in str(exc_info.value)
        assert "source" not in str(exc_info.value)
        assert "transform" not in str(exc_info.value)

        with pytest.raises(DJErrorException) as exc_info:
            # test that the event_type raises because it's a dimension and not a transform
            await get_dj_node("event_type", kinds={NodeType.TRANSFORM})

        assert "No node `event_type` exists of kind transform" in str(exc_info.value)

        # test that the event_type raises because it's a dimension and not a transform
        with pytest.raises(DJErrorException) as exc_info:
            await get_dj_node("event_type", kinds={NodeType.TRANSFORM})

        assert "No node `event_type` exists of kind transform" in str(
            exc_info.value,
        )
