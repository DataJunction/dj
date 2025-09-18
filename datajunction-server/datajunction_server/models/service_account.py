from pydantic import BaseModel
from datajunction_server.typing import UTCDatetime


class ServiceAccountCreate(BaseModel):
    """
    Payload to create a service account
    """

    name: str


class ServiceAccountCreateResponse(BaseModel):
    """
    Response payload for creating a service account
    """

    id: int
    name: str
    client_id: str
    client_secret: str  # returned once


class ServiceAccountOutput(BaseModel):
    """
    Response payload for creating a service account
    """

    id: int
    name: str
    client_id: str
    created_at: UTCDatetime


class TokenResponse(BaseModel):
    """
    Response payload for service account login
    """

    token: str
    token_type: str
    expires_in: int
