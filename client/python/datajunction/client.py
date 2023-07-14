"""DataJunction main client module."""

from datajunction.base import _DJClient


class DJReader(_DJClient):
    """
    Client class to consume basic DJ services: metrics and dimensions.
    """


class DJWriter(DJReader):
    """
    Client class to manage all your DJ dag: nodes, attributes, namespaces, tags.
    """
