"""
Run a DJ server.
"""

import logging
from typing import List

from fastapi import APIRouter, Depends
from sqlmodel import Session, select

from datajunction.models.database import Database
from datajunction.utils import get_session

_logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/databases/", response_model=List[Database])
def read_databases(*, session: Session = Depends(get_session)) -> List[Database]:
    """
    List the available databases.
    """
    databases = session.exec(select(Database)).all()
    return databases
