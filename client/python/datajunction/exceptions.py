"""DJ client exceptions"""


class DJClientException(Exception):
    """
    Base class for errors.
    """


class DJNamespaceAlreadyExists(DJClientException):
    """
    Raised when a namespace to be created already exists.
    """
