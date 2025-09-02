"""
Service Account related API endpoints
"""

from datetime import timedelta
import secrets
import uuid

from fastapi import APIRouter, Depends, Form
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.database.user import OAuthProvider, User
from datajunction_server.errors import DJAuthenticationException, DJError, ErrorCode
from datajunction_server.internal.access.authentication.http import SecureAPIRouter
from datajunction_server.internal.access.authentication.basic import (
    get_password_hash,
    validate_password_hash,
)
from datajunction_server.internal.access.authentication.tokens import create_token
from datajunction_server.models.service_account import (
    ServiceAccountCreate,
    ServiceAccountResponse,
    TokenResponse,
)
from datajunction_server.utils import (
    Settings,
    get_and_update_current_user,
    get_session,
    get_settings,
)

secure_router = SecureAPIRouter(tags=["Service Accounts"])
router = APIRouter(tags=["Service Accounts"])


@secure_router.post("/service_account", response_model=ServiceAccountResponse)
async def create_service_account(
    payload: ServiceAccountCreate,
    session: AsyncSession = Depends(get_session),
    current_user: User = Depends(get_and_update_current_user),
):
    """
    Create a new service account
    """
    client_secret = secrets.token_urlsafe(32)
    service_account = User(
        name=payload.name,
        username=str(uuid.uuid4()),
        created_by_user_id=current_user.id,
        password=get_password_hash(client_secret),
        is_service_account=True,
        oauth_provider=OAuthProvider.BASIC,
    )
    session.add(service_account)
    await session.commit()
    await session.refresh(service_account)

    return ServiceAccountResponse(
        client_id=service_account.username,
        client_secret=client_secret,
        id=service_account.id,
    )


@router.post("/service_account/token", response_model=TokenResponse)
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
                    message=f"Client ID {client_id} not found",
                    code=ErrorCode.USER_NOT_FOUND,
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
