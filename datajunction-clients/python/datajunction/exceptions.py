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

    def __init__(self, ns_name: str, *args) -> None:
        self.message = f"Namespace `{ns_name}` already exists."
        super().__init__(self.message, *args)


class DJTagAlreadyExists(DJClientException):
    """
    Raised when a tag to be created already exists.
    """

    def __init__(self, tag_name: str, *args) -> None:
        self.message = f"Tag `{tag_name}` already exists."
        super().__init__(self.message, *args)


class DJTagDoesNotExist(DJClientException):
    """
    Raised when a referenced tag does not exist.
    """

    def __init__(self, tag_name: str, *args) -> None:
        self.message = f"Tag `{tag_name}` does not exist."
        super().__init__(self.message, *args)


class DJDeploymentFailure(DJClientException):
    """
    Raised when a deployment of a project includes any errors
    """

    def __init__(self, project_name: str, errors: List[dict], *args) -> None:
        self.errors = errors
        self.message = f"Some failures while deploying project `{project_name}`"
        super().__init__(self.message, *args)


class DJTableAlreadyRegistered(DJClientException):
    """
    Raised when a table is already regsistered in DJ.
    """

    def __init__(self, catalog: str, schema: str, table: str, *args) -> None:
        self.message = f"Table `{catalog}.{schema}.{table}` is already registered."
        super().__init__(self.message, *args)


class DJViewAlreadyRegistered(DJClientException):
    """
    Raised when a view is already regsistered in DJ.
    """

    def __init__(self, catalog: str, schema: str, view: str, *args) -> None:
        self.message = (
            f"View `{catalog}.{schema}.{view}` is already registered, "
            "use replace=True to force a refresh."
        )
        super().__init__(self.message, *args)
