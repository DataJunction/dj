"""DJ client exceptions"""
from typing import List


class DJClientException(Exception):
    """
    Base class for client errors.
    """


class DJNamespaceAlreadyExists(DJClientException):
    """
    Raised when a namespace to be created already exists.
    """


class DJTagAlreadyExists(DJClientException):
    """
    Raised when a tag to be created already exists.
    """

    def __init__(self, tag_name: str, *args) -> None:
        self.message = f"Tag `{tag_name}` already exists."
        super().__init__(self.message, *args)


class DJNodeAlreadyExists(DJClientException):
    """
    Raised when a node to be created already exists.
    """

    def __init__(self, node_name: str, *args) -> None:
        self.message = f"Node `{node_name}` already exists."
        super().__init__(self.message, *args)


class DJDeploymentFailure(DJClientException):
    """
    Raised when a deployment of a project includes any errors
    """

    def __init__(self, project_name: str, errors: List[dict], *args) -> None:
        self.errors = errors
        self.message = f"Some failures while deploying project `{project_name}`"
        super().__init__(self.message, *args)
