import json
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from datajunction_server.utils import get_session_manager
from graphql import parse

def is_mutation_operation(body_bytes: bytes) -> bool:
    """
    Check if the provided GraphQL query text contains a mutation operation.
    """
    try:
        payload = json.loads(body_bytes)
        query_text = payload.get("query", "").lower() if payload else ""
        document = parse(query_text)
        for definition in document.definitions:
            if hasattr(definition, "operation") and definition.operation == "mutation":
                return True
        return False
    except Exception:
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
            if is_mutation_operation(body_bytes):
                session = get_session_manager().writer_session()
            else:
                session = get_session_manager().reader_session()
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
