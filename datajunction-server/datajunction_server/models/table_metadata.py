"""Generic table metadata returned by a query service client."""

from pydantic import BaseModel

from datajunction_server.database.column import Column


class TableOwner(BaseModel):
    """A resolved, current human owner of a warehouse table."""

    email: str
    full_name: str = ""


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
    broken_ref: bool | None = None
    valid_through: str | None = None
