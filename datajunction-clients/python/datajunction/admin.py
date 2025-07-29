"""DataJunction admin client module."""

import logging
from typing import Optional

from datajunction.builder import DJBuilder
from datajunction.exceptions import DJClientException


_logger = logging.getLogger(__name__)


class DJAdmin(DJBuilder):  # pylint: disable=too-many-public-methods
    """
    Client class for DJ system administration.
    """

    #
    # Data Catalogs
    #
    def get_catalog(self, name: str) -> dict:
        """
        Get catalog by name.
        """
        response = self._session.get(f"/catalogs/{name}/", timeout=self._timeout)
        return response.json()

    def add_catalog(self, name: str, skip_if_exists: bool = False) -> None:
        """
        Add a catalog.
        """
        try:
            response = self._session.post(
                "/catalogs/",
                json={"name": f"{name}"},
                timeout=self._timeout,
            )
        except DJClientException as exc:  # pragma: no cover
            if skip_if_exists and "already exists" in str(exc):
                _logger.info(
                    "Catalog `%s` already exists, skipping creation.",
                    name,
                )
                return
            raise exc

        if not response.status_code < 400:
            raise DJClientException(
                f"Adding catalog `{name}` failed: {response.json()}",
            )  # pragma: no cover

    #
    # Database Engines
    #
    def get_engine(self, name: str, version: str) -> dict:
        """
        Get engine by name.
        """
        response = self._session.get(
            f"/engines/{name}/{version}",
            timeout=self._timeout,
        )
        return response.json()

    def add_engine(
        self,
        name: str,
        version: str,
        uri: Optional[str] = None,
        dialect: Optional[str] = None,
        skip_if_exists: bool = False,
    ) -> None:
        """
        Add an engine.
        """
        params = {
            "name": f"{name}",
            "version": f"{version}",
        }
        if uri:  # pragma: no cover
            params["uri"] = f"{uri}"
        if dialect:  # pragma: no cover
            params["dialect"] = f"{dialect}"

        try:
            response = self._session.post(
                "/engines/",
                json=params,
                timeout=self._timeout,
            )
        except DJClientException as exc:  # pragma: no cover
            if skip_if_exists and "already exists" in str(exc):
                return
            raise exc

        if not response.status_code < 400:
            raise DJClientException(
                f"Adding engine failed: {response.json()}",
            )  # pragma: no cover

    def link_engine_to_catalog(
        self,
        engine: str,
        version: str,
        catalog: str,
    ) -> None:
        """
        Add/link a particular engine to a particular catalog.
        """
        response = self._session.post(
            f"/catalogs/{catalog}/engines/",
            json=[
                {
                    "name": f"{engine}",
                    "version": f"{version}",
                },
            ],
            timeout=self._timeout,
        )
        json_response = response.json()
        if not response.status_code < 400:
            raise DJClientException(
                f"Linking engine (name: {engine}, version: {version}) "
                f"to catalog `{catalog}` failed: {json_response}",
            )  # pragma: no cover
