"""
Principals: the kinds of thing that can own something in DJ, and the small models
that name one.

Kept free of any import beyond pydantic and DJ's own `StrEnum`. These models used to
live in `models/user.py`, which reaches the database package for the rest of what it
declares -- and the database package cannot be imported from the models the
query-service payloads are built out of, since `service_clients` imports those and
`utils` imports `service_clients`. Here, a payload can name a principal without
dragging any of that in. `models/user.py` and `database/user.py` re-export what they
used to declare, so nothing else has to change where it imports from.
"""

from pydantic import BaseModel, ConfigDict

from datajunction_server.enum import StrEnum


class PrincipalKind(StrEnum):
    """
    Principal kinds: users, service accounts, and groups
    """

    USER = "user"
    SERVICE_ACCOUNT = "service_account"
    GROUP = "group"


class UserNameOnly(BaseModel):
    """
    Username only
    """

    username: str

    model_config = ConfigDict(from_attributes=True)


class PrincipalRef(UserNameOnly):
    """
    A principal by name, together with what kind of principal it is.

    A name on its own is enough wherever it is only shown, or looked up again in DJ.
    It is not enough for a consumer that has to route on the answer: DJ's owners are
    individuals, service accounts and groups alike, and a group handle is not an
    address the way a person's username is.
    """

    kind: PrincipalKind = PrincipalKind.USER
