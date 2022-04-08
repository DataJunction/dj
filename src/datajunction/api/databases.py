"""
Database related APIs.
"""

import logging
from typing import List

from fastapi import APIRouter, Depends, Request
from sqlalchemy.engine.url import URL
from sqlmodel import Session, select

from datajunction.models.database import Database
from datajunction.utils import DJ_DATABASE_ID, get_session

_logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/databases/", response_model=List[Database])
def read_databases(
    *, request: Request, session: Session = Depends(get_session)
) -> List[Database]:
    """
    List the available databases.
    """
    native_database = Database(
        id=DJ_DATABASE_ID,
        name="dj",
        description="Native DJ",
        URI=str(
            URL(
                "dj",
                host=request.url.hostname,
                port=request.url.port,
                database=str(DJ_DATABASE_ID),
            ),
        ),
        read_only=True,
    )

    databases = session.exec(select(Database)).all()
    databases.append(native_database)

    return databases
