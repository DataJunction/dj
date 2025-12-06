"""
Service Account related API endpoints
"""

from datetime import timedelta
import logging
import secrets
import uuid

from fastapi import APIRouter, Depends, Form
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.database.user import OAuthProvider, PrincipalKind, User
from datajunction_server.errors import DJAuthenticationException, DJError, ErrorCode
from datajunction_server.internal.access.authentication.http import SecureAPIRouter
from datajunction_server.internal.access.authentication.basic import (
    get_password_hash,
    validate_password_hash,
)
from datajunction_server.internal.access.authentication.tokens import create_token
from datajunction_server.models.service_account import (
    ServiceAccountCreate,
    ServiceAccountCreateResponse,
    ServiceAccountOutput,
    TokenResponse,
)
from datajunction_server.utils import (
    Settings,
    get_current_user,
    get_session,
    get_settings,
)

secure_router = SecureAPIRouter(tags=["Service Accounts"])
router = APIRouter(tags=["Service Accounts"])
logger = logging.getLogger(__name__)


@secure_router.post("/service-accounts", response_model=ServiceAccountCreateResponse)
async def create_service_account(
    payload: ServiceAccountCreate,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    """
    Create a new service account
    """
    if current_user.kind != PrincipalKind.USER:
        raise DJAuthenticationException(
            errors=[
                DJError(
                    message="Only users can create service accounts",
                    code=ErrorCode.AUTHENTICATION_ERROR,
                ),
            ],
        )

    logger.info("User %s is creating a service account", current_user.username)

    client_secret = secrets.token_urlsafe(32)
    service_account = User(
        name=payload.name,
        username=str(uuid.uuid4()),
        created_by_id=current_user.id,
        password=get_password_hash(client_secret),
        kind=PrincipalKind.SERVICE_ACCOUNT,
        oauth_provider=OAuthProvider.BASIC,
    )
    session.add(service_account)
    await session.commit()
    await session.refresh(service_account)

    logger.info(
        "Service account %s created by user %s",
        service_account.username,
        current_user.username,
    )
    return ServiceAccountCreateResponse(
        id=service_account.id,
        name=service_account.name,
        client_id=service_account.username,
        client_secret=client_secret,
    )


@secure_router.get("/service-accounts", response_model=list[ServiceAccountOutput])
async def list_service_accounts(
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
) -> list[ServiceAccountOutput]:
    """
    List service accounts for the current user
    """
    service_accounts = await User.get_service_accounts_for_user_id(
        session,
        current_user.id,
    )
    return [
        ServiceAccountOutput(
            id=service_account.id,
            name=service_account.name,
            client_id=service_account.username,
            created_at=service_account.created_at,
        )
        for service_account in service_accounts
    ]


@secure_router.delete("/service-accounts/{client_id}")
async def delete_service_account(
    client_id: str,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_current_user),
):
    """
    Delete a service account
    """
    service_account = await User.get_by_username(session, client_id)
    if not service_account:
        raise DJAuthenticationException(
            errors=[
                DJError(
                    message=f"Service account `{client_id}` not found",
                    code=ErrorCode.USER_NOT_FOUND,
                ),
            ],
        )

    if service_account.kind != PrincipalKind.SERVICE_ACCOUNT:
        raise DJAuthenticationException(
            errors=[
                DJError(
                    message="Not a service account",
                    code=ErrorCode.AUTHENTICATION_ERROR,
                ),
            ],
        )

    if service_account.created_by_id != current_user.id:
        raise DJAuthenticationException(
            errors=[
                DJError(
                    message="You can only delete service accounts you created",
                    code=ErrorCode.AUTHENTICATION_ERROR,
                ),
            ],
        )

    logger.info(
        "User %s is deleting service account %s",
        current_user.username,
        service_account.username,
    )

    await session.delete(service_account)
    await session.commit()

    return {"message": f"Service account `{client_id}` deleted"}


@router.post("/service-accounts/token", response_model=TokenResponse)
async def service_account_token(
    client_id: str = Form(...),
    client_secret: str = Form(...),
    session: AsyncSession = Depends(get_session),
    settings: Settings = Depends(get_settings),
) -> TokenResponse:
    """
    Get an authentication token for a service account
    """
    service_account = await User.get_by_username(session, client_id)
    if not service_account:
        raise DJAuthenticationException(
            errors=[
                DJError(
                    message=f"Service account `{client_id}` not found",
                    code=ErrorCode.USER_NOT_FOUND,
                ),
            ],
        )
    if service_account.kind != PrincipalKind.SERVICE_ACCOUNT:
        raise DJAuthenticationException(
            errors=[
                DJError(
                    message="Not a service account",
                    code=ErrorCode.INVALID_LOGIN_CREDENTIALS,
                ),
            ],
        )

    if not validate_password_hash(client_secret, service_account.password):
        raise DJAuthenticationException(
            errors=[
                DJError(
                    message="Invalid service account credentials",
                    code=ErrorCode.INVALID_LOGIN_CREDENTIALS,
                ),
            ],
        )

    expire_delta = timedelta(seconds=settings.service_account_token_expire)
    token = create_token(
        data={"username": service_account.username},
        secret=settings.secret,
        iss=settings.url,
        expires_delta=expire_delta,
    )
    return TokenResponse(
        token=token,
        token_type="bearer",
        expires_in=int(expire_delta.total_seconds()),
    )
