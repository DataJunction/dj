import json
import logging
from typing import cast
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from sqlalchemy.ext.asyncio import AsyncSession
from datajunction_server.utils import get_session_manager
from graphql import parse, OperationType, GraphQLError

logger = logging.getLogger(__name__)


def is_mutation(body_bytes: bytes) -> bool:
    """
    Return True if the GraphQL query contains a mutation, False otherwise.
    """
    if not body_bytes:
        return False
    try:
        payload = json.loads(body_bytes)
        query = payload.get("query")
        if not query:
            return False
        document = parse(query)
        return any(
            getattr(defn, "operation", None) == OperationType.MUTATION
            for defn in document.definitions
        )
    except (GraphQLError, json.JSONDecodeError) as exc:
        logger.exception("Failed to parse GraphQL query: %s", str(exc))
        return False  # pragma: no cover
    except Exception as exc:  # pragma: no cover
        logger.exception(  # pragma: no cover
            "Exception handling GraphQL query: %s",
            str(exc),
        )
        return False


class GraphQLSessionMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        """
        Middleware to manage database sessions for GraphQL requests. This ensures that
        a database session is created for each GraphQL request, and that it's committed
        or rolled back as appropriate.
        """
        if request.url.path.startswith("/graphql"):
            body_bytes = await request.body()

            # Restore request body for downstream usage
            async def receive():  # pragma: no cover
                return {"type": "http.request", "body": body_bytes}

            request._receive = receive  # type: ignore

            # Set up the database session based on whether it's a mutation or not
            session = (
                cast(AsyncSession, get_session_manager().writer_session)
                if is_mutation(body_bytes)
                else cast(AsyncSession, get_session_manager().reader_session)
            )
            request.state.db = session  # Attach to request so context can access it
            try:
                response = await call_next(request)
                await session.commit()
                return response
            except Exception:
                await session.rollback()
                raise
            finally:
                await session.close()
        else:
            return await call_next(request)
