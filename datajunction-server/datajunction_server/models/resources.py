from enum import Enum


class ResourceType(Enum):
    """
    Types of resources
    """

    NODE = "node"
    NAMESPACE = "namespace"


class ResourceRequestVerb(Enum):
    """
    Types of actions for a request
    """

    BROWSE = "browse"
    READ = "read"
    WRITE = "write"
    USE = "use"
    EXECUTE = "execute"
    DELETE = "delete"
