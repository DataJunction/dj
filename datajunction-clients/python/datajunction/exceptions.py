"""DJ client exceptions"""


class DJClientException(Exception):
    """
    Base class for client errors.
    """


class DJNamespaceAlreadyExists(DJClientException):
    """
    Raised when a namespace to be created already exists.
    """


class DJNodeAlreadyExists(DJClientException):
    """
    Raised when a node to be created already exists.
    """

    def __init__(self, node_name: str, *args) -> None:
        self.message = f"Node `{node_name}` already exists."
        super().__init__(self.message, *args)
