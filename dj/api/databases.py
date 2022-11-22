"""
Database related APIs.
"""

import logging
from typing import List

from fastapi import APIRouter, Depends
from sqlmodel import Session, select

from dj.models.database import Database
from dj.utils import get_session

_logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/databases/", response_model=List[Database])
def read_databases(*, session: Session = Depends(get_session)) -> List[Database]:
    """
    List the available databases.
    """
    return session.exec(select(Database)).all()
