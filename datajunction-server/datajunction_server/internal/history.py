"""Enums for history events"""

from datajunction_server.enum import StrEnum


class ActivityType(StrEnum):
    """
    An activity type
    """

    CREATE = "create"
    DELETE = "delete"
    RESTORE = "restore"
    UPDATE = "update"
    REFRESH = "refresh"
    TAG = "tag"
    SET_ATTRIBUTE = "set_attribute"
    STATUS_CHANGE = "status_change"


class EntityType(StrEnum):
    """
    An entity type for which activity can occur
    """

    ATTRIBUTE = "attribute"
    AVAILABILITY = "availability"
    BACKFILL = "backfill"
    CATALOG = "catalog"
    COLUMN_ATTRIBUTE = "column_attribute"
    DEPENDENCY = "dependency"
    ENGINE = "engine"
    LINK = "link"
    MATERIALIZATION = "materialization"
    NAMESPACE = "namespace"
    NODE = "node"
    PARTITION = "partition"
    QUERY = "query"
    TAG = "tag"
