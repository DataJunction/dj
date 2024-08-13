"""
Basic OAuth Authentication Router
"""
from datetime import timedelta
import asyncio
from http import HTTPStatus
import os
from alembic.config import Config
from alembic import command

from fastapi import APIRouter, Depends, Form, HTTPException, Header
from fastapi.responses import JSONResponse, Response
from fastapi.security import OAuth2PasswordRequestForm
from sqlalchemy import select,text
from sqlalchemy.ext.asyncio import AsyncSession

from datajunction_server.constants import AUTH_COOKIE, LOGGED_IN_FLAG_COOKIE
from datajunction_server.database.user import OAuthProvider, User
from datajunction_server.errors import DJError, DJException, ErrorCode
from datajunction_server.internal.access.authentication.basic import (
    get_password_hash,
    validate_user_password,
)
from datajunction_server.internal.access.authentication.tokens import create_token
from datajunction_server.utils import Settings, get_session, get_settings
from fastapi import Request

router = APIRouter(tags=["Basic OAuth2"])


@router.post("/basic/user/")
async def create_a_user(
    email: str = Form(),
    username: str = Form(),
    password: str = Form(),
    session: AsyncSession = Depends(get_session),
) -> JSONResponse:
    """
    Create a new user
    """
    user_result = await session.execute(select(User).where(User.username == username))
    if user_result.scalar_one_or_none():
        raise DJException(
            http_status_code=HTTPStatus.CONFLICT,
            errors=[
                DJError(
                    code=ErrorCode.ALREADY_EXISTS,
                    message=f"User {username} already exists.",
                ),
            ],
        )
    new_user = User(
        email=email,
        username=username,
        password=get_password_hash(password),
        oauth_provider=OAuthProvider.BASIC,
    )
    session.add(new_user)
    await session.commit()
    await session.refresh(new_user)
    return JSONResponse(
        content={"message": "User successfully created"},
        status_code=HTTPStatus.CREATED,
    )

async def run_alembic_upgrade():
    # Set the path to the Alembic configuration file
    alembic_ini_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'alembic.ini'))

    # Set the path to the Alembic scripts directory
    script_location = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..', '..', 'alembic'))

    # Check if paths exist
    if not os.path.isfile(alembic_ini_path):
        raise FileNotFoundError(f"Config file not found: {alembic_ini_path}")
    if not os.path.isdir(script_location):
        raise FileNotFoundError(f"Script directory not found: {script_location}")

    # Load the Alembic configuration
    alembic_cfg = Config(alembic_ini_path)
    alembic_cfg.set_main_option('script_location', script_location)

    # Run the Alembic upgrade command
    try:
        # This might need to be run in a thread if it’s blocking
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, lambda: command.upgrade(alembic_cfg, 'head'))
    except Exception as e:
        print(f"Error during upgrade: {e}")
        raise

@router.post("/schema/register/")
async def schema_register(
    request: Request,
    session: AsyncSession = Depends(get_session),
) -> JSONResponse:
    """
    Create a new schema if it does not exist.
    """
    try:
        body  = await request.json()
        schema = body.get("schema")
        print("schema body is",schema)
        if not schema:
            raise HTTPException(status_code=400, detail="Schema in body is required")

        # Check if schema already exists
        query = text(f"SELECT schema_name FROM information_schema.schemata WHERE schema_name = :schema_name")
        result = await session.execute(query, {"schema_name": schema})
        if result.fetchone():
            raise HTTPException(status_code=400, detail="Schema already exists")

        # Create schema
        await session.execute(text(f"CREATE SCHEMA {schema}"))
        await session.commit()


        await run_alembic_upgrade()

        return JSONResponse(
            content={"message": "Schema successfully created and migrations applied"},
            status_code=HTTPStatus.CREATED,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
@router.post("/basic/login/")
async def login(
    form_data: OAuth2PasswordRequestForm = Depends(),
    session: AsyncSession = Depends(get_session),
    settings: Settings = Depends(get_settings),
):
    """
    Get a JWT token and set it as an HTTP only cookie
    """
    print("requesting login - ")
    user = await validate_user_password(
        username=form_data.username,
        password=form_data.password,
        session=session,
    )
    response = Response(status_code=HTTPStatus.OK)
    response.set_cookie(
        AUTH_COOKIE,
        create_token(
            {"username": user.username},
            secret=settings.secret,
            iss=settings.url,
            expires_delta=timedelta(days=365),
        ),
        httponly=True,
    )
    response.set_cookie(
        LOGGED_IN_FLAG_COOKIE,
        "true",
    )
    return response


@router.post("/logout/")
def logout():
    """
    Logout a user by deleting the auth cookie
    """
    response = Response(status_code=HTTPStatus.OK)
    response.delete_cookie(AUTH_COOKIE, httponly=True)
    response.delete_cookie(LOGGED_IN_FLAG_COOKIE)
    return response
