"""DJ client exceptions"""


class DJClientException(Exception):
    """
    Base class for client errors.
    """


class DJNamespaceAlreadyExists(DJClientException):
    """
    Raised when a namespace to be created already exists.
    """
