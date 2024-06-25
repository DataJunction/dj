"""
Tests for the data API.
"""
import pytest

from datajunction_server.api.main import app
from datajunction_server.internal.access.authorization import validate_access
from datajunction_server.models import access


class TestDataAccessControl:  # pylint: disable=too-few-public-methods
    """
    Test the data access control.
    """

    @pytest.mark.asyncio
    async def test_get_metric_data_unauthorized(
        self,
        module__client_with_basic,
    ) -> None:
        """
        Test retrieving data for a metric
        """

        def validate_access_override():
            def _validate_access(access_control: access.AccessControl):
                access_control.deny_all()

            return _validate_access

        app.dependency_overrides[validate_access] = validate_access_override
        response = await module__client_with_basic.get("/data/basic.num_comments/")
        data = response.json()
        assert data["message"] == (
            "Authorization of User `dj` for this request failed."
            "\nThe following requests were denied:\nread:node/basic.num_comments."
        )
        assert response.status_code == 403
        app.dependency_overrides.clear()
