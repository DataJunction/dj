"""
Rate limiting middleware and dependencies for DataJunction server.

Uses DJ's cache abstraction to track request counts per user and enforce
per-endpoint rate limits. Works with ANY cache backend (Redis, KVDAL,
in-memory, etc.) via the cachelib interface.

For GraphQL endpoints, implements query-aware rate limiting by hashing the
actual GraphQL query, so simple and complex queries are tracked separately.
"""

import hashlib
import json
import logging
import time
from typing import Optional, Tuple, List

from fastapi import Request, HTTPException, Depends
from starlette.status import HTTP_429_TOO_MANY_REQUESTS

from datajunction_server.database.user import User
from datajunction_server.utils import get_settings
from datajunction_server.internal.caching.interface import Cache
from datajunction_server.internal.caching.cachelib_cache import get_cache

_logger = logging.getLogger(__name__)
settings = get_settings()


async def get_graphql_query_identifier(request: Request) -> Optional[str]:
    """
    Extract and hash the GraphQL query from the request body.

    This allows query-aware rate limiting where different GraphQL queries
    are tracked separately (simple queries vs complex queries).

    Args:
        request: FastAPI request object

    Returns:
        Hash of the GraphQL query, or None if not a GraphQL request or parsing failed
    """
    if not request.url.path.startswith("/graphql"):
        return None

    try:
        # Read the request body
        body = await request.body()

        # GraphQL requests can be GET or POST
        if request.method == "POST":
            # Parse JSON body
            data = json.loads(body) if body else {}
            query = data.get("query", "")
        elif request.method == "GET":
            # Query is in URL params for GET requests
            query = request.query_params.get("query", "")
        else:
            return None

        if not query:
            return None

        # Create a hash of the query (normalize whitespace first)
        normalized_query = " ".join(query.split())
        query_hash = hashlib.sha256(normalized_query.encode()).hexdigest()[:16]

        return f"gql:{query_hash}"

    except Exception as e:
        _logger.warning(f"Failed to extract GraphQL query: {e}")
        return None


class RateLimiter:
    """
    Cache-backed rate limiter for per-user, per-endpoint rate limiting.

    Works with any cache backend (Redis, KVDAL, in-memory) via the
    cachelib interface.
    """

    def __init__(
        self,
        requests_per_second: int,
        window_seconds: int = 1,
    ):
        """
        Initialize rate limiter.

        Args:
            requests_per_second: Maximum requests allowed per second
            window_seconds: Time window for rate limiting (default: 1 second)
        """
        self.requests_per_second = requests_per_second
        self.window_seconds = window_seconds
        self.max_requests = requests_per_second * window_seconds

    async def check_rate_limit(
        self,
        user: User,
        endpoint: str,
        cache: Optional[Cache],
        request: Optional[Request] = None,
    ) -> Tuple[bool, Optional[int]]:
        """
        Check if request should be rate limited using the cache abstraction.

        Args:
            user: Authenticated user making the request
            endpoint: API endpoint being accessed (can include query identifier for GraphQL)
            cache: Cache instance (from get_cache dependency)
            request: Optional request object for GraphQL query extraction

        Returns:
            Tuple of (is_allowed, retry_after_seconds)
        """
        # If cache is not available, allow all requests
        if cache is None:
            _logger.warning(
                "Rate limiting disabled: Cache not configured. "
                "Configure cache backend to enable rate limiting.",
            )
            return True, None

        # Create a unique key for this user + endpoint combination
        # For GraphQL, endpoint includes the query hash for query-aware limiting
        # Format: rate_limit:{user_id}:{endpoint} or rate_limit:{user_id}:/graphql:gql:{hash}
        user_id = user.username or user.email or "anonymous"
        cache_key = f"rate_limit:{user_id}:{endpoint}"

        current_time = int(time.time())
        window_start = current_time - self.window_seconds

        try:
            # Get existing timestamps from cache (list of request timestamps)
            timestamps: List[int] = cache.get(cache_key) or []

            # Remove timestamps outside the current window
            valid_timestamps = [ts for ts in timestamps if ts >= window_start]

            # Check if rate limit exceeded
            if len(valid_timestamps) >= self.max_requests:
                # Calculate retry-after time
                oldest_timestamp = min(valid_timestamps)
                retry_after = oldest_timestamp + self.window_seconds - current_time
                retry_after = max(1, retry_after)  # At least 1 second

                _logger.warning(
                    f"Rate limit exceeded for user={user_id}, endpoint={endpoint}, "
                    f"count={len(valid_timestamps)}, limit={self.max_requests}",
                )
                return False, retry_after

            # Add current request timestamp
            valid_timestamps.append(current_time)

            # Save back to cache with TTL
            cache.set(
                cache_key,
                valid_timestamps,
                timeout=self.window_seconds * 2,  # Keep for 2x window for safety
            )

            return True, None

        except Exception as e:  # pragma: no cover
            # On cache errors, fail open (allow the request)
            _logger.error(f"Rate limiting error: {e}. Allowing request.")
            return True, None


# Default rate limiters for different endpoint categories
DEFAULT_RATE_LIMITER = RateLimiter(
    requests_per_second=settings.rate_limit_default_rps,
    window_seconds=settings.rate_limit_window_seconds,
)

EXPENSIVE_RATE_LIMITER = RateLimiter(
    requests_per_second=settings.rate_limit_expensive_rps,
    window_seconds=settings.rate_limit_window_seconds,
)

STANDARD_RATE_LIMITER = RateLimiter(
    requests_per_second=settings.rate_limit_standard_rps,
    window_seconds=settings.rate_limit_window_seconds,
)


def get_rate_limiter_for_endpoint(path: str, method: str) -> RateLimiter:
    """
    Get the appropriate rate limiter for an endpoint based on path and method.

    Args:
        path: Request path (e.g., "/data/my_metric/", "/graphql:gql:abc123")
        method: HTTP method (e.g., "GET", "POST")

    Returns:
        RateLimiter instance configured for this endpoint
    """
    # Extract base path (remove query identifier if present)
    base_path = path.split(":")[0]

    # Expensive endpoints that hit the database heavily
    if base_path.startswith("/sql/") or base_path.startswith("/data/"):
        return EXPENSIVE_RATE_LIMITER

    # GraphQL queries - now with query-aware rate limiting
    # Each unique GraphQL query is tracked separately
    if base_path.startswith("/graphql"):
        return EXPENSIVE_RATE_LIMITER

    # Node refresh operations
    if "/refresh" in base_path:
        return EXPENSIVE_RATE_LIMITER

    # Standard read operations (GET requests to most endpoints)
    if method == "GET":
        return STANDARD_RATE_LIMITER

    # Default for everything else
    return DEFAULT_RATE_LIMITER


async def enforce_rate_limit(
    request: Request,
    cache: Cache = Depends(get_cache),
) -> None:
    """
    Dependency that enforces rate limiting on authenticated requests.

    This should be used AFTER authentication (DJHTTPBearer) so that
    request.state.user is already populated.

    For GraphQL endpoints, implements query-aware rate limiting by hashing
    the GraphQL query. This means:
    - Simple queries: tracked separately from complex queries
    - Same query repeated: counted together
    - Different queries: counted separately

    Args:
        request: FastAPI request object (with request.state.user set by auth)
        cache: Cache instance from dependency injection

    Raises:
        HTTPException: 429 Too Many Requests if rate limit exceeded
    """
    # Get current user from request state (set by DJHTTPBearer)
    current_user = getattr(request.state, "user", None)
    if current_user is None:
        # If no user in request state, skip rate limiting
        # (this shouldn't happen on SecureAPIRouter endpoints, but fail open)
        _logger.warning("Rate limiting skipped: no user in request.state")
        return

    # For GraphQL, create a query-aware endpoint identifier
    endpoint_key = request.url.path
    if request.url.path.startswith("/graphql"):
        query_id = await get_graphql_query_identifier(request)
        if query_id:
            endpoint_key = f"{request.url.path}:{query_id}"
            # Store body back so route handler can read it
            # FastAPI will check _body before reading the stream
            if not hasattr(request, "_body"):
                request._body = await request.body()

    # Get the appropriate rate limiter for this endpoint
    rate_limiter = get_rate_limiter_for_endpoint(
        path=endpoint_key,
        method=request.method,
    )

    # Check rate limit
    is_allowed, retry_after = await rate_limiter.check_rate_limit(
        user=current_user,
        endpoint=endpoint_key,
        cache=cache,
        request=request,
    )

    if not is_allowed:
        _logger.warning(
            f"Rate limit exceeded: user={current_user.username or current_user.email}, "
            f"endpoint={endpoint_key}, method={request.method}",
        )
        raise HTTPException(
            status_code=HTTP_429_TOO_MANY_REQUESTS,
            detail={
                "message": "Rate limit exceeded. Please slow down your requests.",
                "retry_after": retry_after,
            },
            headers={"Retry-After": str(retry_after)} if retry_after else {},
        )
