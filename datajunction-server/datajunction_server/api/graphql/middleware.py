from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from datajunction_server.utils import get_session_manager


class GraphQLSessionMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        """
        Middleware to manage database sessions for GraphQL requests. This ensures that
        a database session is created for each GraphQL request, and that it's committed
        or rolled back as appropriate.
        """
        if request.url.path.startswith("/graphql"):
            session = get_session_manager().session()
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
