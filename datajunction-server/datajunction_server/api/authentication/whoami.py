"""
Router for getting the current active user
"""
from fastapi import APIRouter, Depends, Request
from fastapi.security import HTTPBearer

from datajunction_server.models.user import UserOutput

router = APIRouter(tags=["Who am I?"], dependencies=[Depends(HTTPBearer())])


@router.get("/basic/whoami/", response_model=UserOutput)
async def get_current_user(request: Request) -> UserOutput:
    """
    Returns the current authenticated user
    """
    return request.state.user
