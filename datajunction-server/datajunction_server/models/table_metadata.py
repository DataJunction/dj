"""Generic table metadata returned by a query service client."""

from pydantic import BaseModel

from datajunction_server.database.column import Column


class TableOwner(BaseModel):
    """
    The principal that owns a warehouse table.

    ``username`` is what DJ keys on, and the query service decides it: DJ
    usernames are not universally emails (the GitHub auth path uses a login,
    basic auth uses an arbitrary name), so the mapping from a warehouse
    identity to a DJ username belongs to whichever query service knows its own
    catalog's identity model.

    Not necessarily a person -- a Snowflake owner is a role, and a BigQuery one
    can be a group or a service account.
    """

    username: str
    email: str | None = None
    display_name: str = ""


class TableMetadata(BaseModel):
    """
    What a query service can tell us about a table.

    ``owner`` is None whenever it cannot be determined -- no owner on the table,
    a query client with no ownership source, or a query service too old to
    report one. Callers must treat None as "no ownership information", never as
    "no owner".
    """

    model_config = {"arbitrary_types_allowed": True}

    columns: list[Column] = []
    owner: TableOwner | None = None
    partitions: list[str] = []
    # The table's own comment/description. Every major catalog has one
    # (Snowflake COMMENT, BigQuery description, Iceberg/Hive comment). Per-column
    # descriptions ride along on ``columns``, which already has the field.
    description: str | None = None
