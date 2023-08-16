"""
A secure API router for routes that require authentication
"""
from http import HTTPStatus
from sqlmodel import Session
from typing import Any, Optional

from fastapi import APIRouter, Depends, Request
from fastapi.security import HTTPBearer
from fastapi.security.utils import get_authorization_scheme_param

from datajunction_server.internal.authentication.basic import get_user
from datajunction_server.internal.authentication.tokens import decode_token
from datajunction_server.utils import get_settings, get_session
from datajunction_server.errors import DJException, DJError, ErrorCode
from datajunction_server.constants import DJ_AUTH_COOKIE

class DJHTTPBearer(HTTPBearer):
    """
    A custom HTTPBearer that accepts a cookie or bearer token
    """
    async def __call__(
        self, request: Request, session: Session = Depends(get_session)
    ) -> None:
        # First check for a JWT sent in a cookie
        jwt = request.cookies.get(DJ_AUTH_COOKIE)
        if jwt:
            data = await decode_token(jwt)
            request.state.user = get_user(
                username=data["username"],
                session=session
            )
            return

        authorization: str = request.headers.get("Authorization")
        scheme, credentials = get_authorization_scheme_param(authorization)
        if not (authorization and scheme and credentials):
            if self.auto_error:
                raise DJException(
                    http_status_code=HTTPStatus.FORBIDDEN,
                    errors=[
                        DJError(
                            message="Not authenticated",
                            code=ErrorCode.AUTHENTICATION_ERROR,
                        )
                    ]
                )
            else:  # pragma: no cover
                return
        if scheme.lower() != "bearer":
            if self.auto_error:
                raise DJException(
                    http_status_code=HTTPStatus.FORBIDDEN,
                    errors=[
                        DJError(
                            message="Invalid authentication credentials",
                            code=ErrorCode.AUTHENTICATION_ERROR,
                        )
                    ]
                )
            else:  # pragma: no cover
                return
        data = await decode_token(credentials)
        request.state.user = get_user(username=data["username"], session=session)
        return

class SecureAPIRouter(APIRouter):

    def __init__(self, *args: Any, **kwargs: Any):
        settings = get_settings()
        return super().__init__(
            *args,
            dependencies=[Depends(DJHTTPBearer())] if settings.secret else [],
            **kwargs,
        )
