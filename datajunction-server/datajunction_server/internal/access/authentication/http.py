"""
A secure API router for routes that require authentication
"""

from http import HTTPStatus
from typing import Any, Callable

from fastapi import APIRouter, Depends
from fastapi.security import HTTPBearer
from fastapi.security.utils import get_authorization_scheme_param
from fastapi.types import DecoratedCallable
from jose.exceptions import JWEError, JWTError
from sqlalchemy.ext.asyncio import AsyncSession
from starlette.requests import Request

from datajunction_server.constants import AUTH_COOKIE
from datajunction_server.errors import DJAuthenticationException, DJError, ErrorCode
from datajunction_server.internal.access.authentication.basic import get_user
from datajunction_server.internal.access.authentication.tokens import decode_token
from datajunction_server.utils import get_session, get_settings


class DJHTTPBearer(HTTPBearer):
    """
    A custom HTTPBearer that accepts a cookie or bearer token
    """

    async def __call__(
        self,
        request: Request,
        session: AsyncSession = Depends(get_session),
    ) -> None:
        # First check for a JWT sent in a cookie
        jwt = request.cookies.get(AUTH_COOKIE)
        if jwt:
            try:
                jwt_data = decode_token(jwt)
            except (JWEError, JWTError) as exc:
                raise DJAuthenticationException(
                    http_status_code=HTTPStatus.UNAUTHORIZED,
                    errors=[
                        DJError(
                            message="Cannot decode authorization token",
                            code=ErrorCode.AUTHENTICATION_ERROR,
                        ),
                    ],
                ) from exc
            request.state.user = await get_user(
                username=jwt_data["username"],
                session=session,
            )
            return

        authorization: str = request.headers.get("Authorization")
        scheme, credentials = get_authorization_scheme_param(authorization)
        if not (authorization and scheme and credentials):
            if self.auto_error:
                raise DJAuthenticationException(
                    http_status_code=HTTPStatus.FORBIDDEN,
                    errors=[
                        DJError(
                            message="Not authenticated",
                            code=ErrorCode.AUTHENTICATION_ERROR,
                        ),
                    ],
                )
            return  # pragma: no cover
        if scheme.lower() != "bearer":
            if self.auto_error:
                raise DJAuthenticationException(
                    http_status_code=HTTPStatus.FORBIDDEN,
                    errors=[
                        DJError(
                            message="Invalid authentication credentials",
                            code=ErrorCode.AUTHENTICATION_ERROR,
                        ),
                    ],
                )
            return  # pragma: no cover
        jwt_data = decode_token(credentials)
        request.state.user = await get_user(
            username=jwt_data["username"],
            session=session,
        )
        return


class TrailingSlashAPIRouter(APIRouter):
    """
    A base APIRouter that handles trailing slashes
    """

    def api_route(
        self,
        path: str,
        *,
        include_in_schema: bool = True,
        **kwargs: Any,
    ) -> Callable[[DecoratedCallable], DecoratedCallable]:
        """
        For any given API route path, we always add both the path without the trailing slash
        and the path with the trailing slash, ensuring that we can serve both types of calls.
        This solution is pulled from https://github.com/tiangolo/fastapi/discussions/7298
        """
        if path.endswith("/"):
            path = path[:-1]

        add_path = super().api_route(
            path,
            include_in_schema=include_in_schema,
            **kwargs,
        )

        path_with_trailing_slash = path + "/"
        add_trailing_slash_path = super().api_route(
            path_with_trailing_slash,
            include_in_schema=False,
            **kwargs,
        )

        def decorator(func: DecoratedCallable) -> DecoratedCallable:
            add_trailing_slash_path(func)
            return add_path(func)

        return decorator


class SecureAPIRouter(TrailingSlashAPIRouter):
    """
    A fastapi APIRouter with a DJHTTPBearer dependency
    """

    def __init__(self, *args: Any, **kwargs: Any):
        settings = get_settings()
        super().__init__(
            *args,
            dependencies=[Depends(DJHTTPBearer())] if settings.secret else [],
            **kwargs,
        )
