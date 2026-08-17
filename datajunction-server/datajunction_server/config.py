"""
Configuration for the datajunction server.
"""

# :w
import urllib.parse
from datetime import timedelta
from pathlib import Path
from typing import Any

from cachelib.base import BaseCache
from cachelib.file import FileSystemCache
from cachelib.redis import RedisCache
from celery import Celery
from pydantic import BaseModel, Field, field_validator
from pydantic_settings import BaseSettings

from datajunction_server.naming import is_valid_namespace, parse_scope_pattern

RESTRICTIVE_SCOPE_ACTIONS = {"read", "write", "execute", "delete", "manage"}
RESTRICTIVE_SCOPE_TYPES = {"node", "namespace"}


class DatabaseConfig(BaseModel):
    """
    Metadata database configuration.
    """

    uri: str
    pool_size: int = 20
    max_overflow: int = 100
    pool_timeout: int = 10
    pool_recycle: int = 300
    connect_timeout: int = 5
    pool_pre_ping: bool = True
    echo: bool = False
    keepalives: int = 1
    keepalives_idle: int = 30
    keepalives_interval: int = 10
    keepalives_count: int = 5


class QueryClientConfig(BaseModel):
    """
    Configuration for query service clients.

    Set via environment variables using double-underscore delimiters, e.g.::

        QUERY_CLIENT__TYPE=bigquery
        QUERY_CLIENT__CONNECTION__PROJECT=my-gcp-project

    Supported client types and their required connection parameters:

    **http** (default)::

        QUERY_CLIENT__TYPE=http
        QUERY_CLIENT__CONNECTION__URI=http://djqs:8001

    **snowflake**::

        QUERY_CLIENT__TYPE=snowflake
        QUERY_CLIENT__CONNECTION__ACCOUNT=my-account
        QUERY_CLIENT__CONNECTION__USER=my-user
        QUERY_CLIENT__CONNECTION__PASSWORD=my-password

    **bigquery**::

        QUERY_CLIENT__TYPE=bigquery
        QUERY_CLIENT__CONNECTION__PROJECT=my-gcp-project
        # Optional: path to a service account JSON key file
        QUERY_CLIENT__CONNECTION__CREDENTIALS_PATH=/path/to/service-account.json
        # Optional: BigQuery location (e.g. US, EU)
        QUERY_CLIENT__CONNECTION__LOCATION=US

    When multiple DJ catalogs map to different GCP projects, set the engine URI
    on the DJ engine to ``bigquery://my-gcp-project`` — the project is resolved
    from the engine URI first, falling back to ``PROJECT`` above.
    """

    # Type of query client: 'http', 'snowflake', 'bigquery', 'databricks', 'trino', etc.
    type: str = "http"

    # Connection parameters (varies by client type)
    connection: dict[str, Any] = Field(default_factory=dict)

    # Number of retries for failed requests (mainly for HTTP client)
    retries: int = 0


class SeedSetup(BaseModel):
    # A "default" catalog for nodes that are pure SQL and don't belong in any
    # particular catalog. This typically applies to on-the-fly user-defined dimensions.
    virtual_catalog_name: str = "default"

    # Server-level default catalog for non-source nodes when catalog can't be
    # inferred from upstream parents. Takes precedence over virtual catalog.
    # Configurable via env var SEED_SETUP__DEFAULT_CATALOG_NAME.
    default_catalog_name: str | None = None

    # A "DJ System" catalog that contains all system tables modeled in DJ
    system_catalog_name: str = "dj_metadata"

    # The engine for DJ's postgres metadata db
    system_engine_name: str = "dj_system"

    # The namespace for system tables modeled in DJ
    system_namespace: str = "system.dj"


class Settings(BaseSettings):  # pragma: no cover
    """
    DataJunction configuration.
    """

    model_config = {"env_nested_delimiter": "__"}  # Enables nesting like WRITER_DB__URI

    name: str = "DJ server"
    description: str = "A DataJunction metrics layer"
    url: str = "http://localhost:8000/"

    # A list of hostnames that are allowed to make cross-site HTTP requests
    cors_origin_whitelist: list[str] = ["http://localhost:3000"]

    # Config for the metadata database, with support for writer and reader clusters
    # `writer_db` is the primary database used for write operations
    # [optional] `reader_db` is used for read operations and defaults to `writer_db`
    # if no dedicated read replica is configured.
    writer_db: DatabaseConfig = DatabaseConfig(
        uri="postgresql+psycopg://dj:dj@postgres_metadata:5432/dj",
    )
    reader_db: DatabaseConfig = writer_db

    # Directory where the repository lives. This should have 2 subdirectories, "nodes" and
    # "databases".
    repository: Path = Path(".")

    # Where to store the results from queries.
    results_backend: BaseCache = FileSystemCache("/tmp/dj", default_timeout=0)

    # Cache for paginating results and potentially other things.
    redis_cache: str | None = None
    paginating_timeout: timedelta = timedelta(minutes=5)

    # Configure Celery for async requests. If not configured async queries will be
    # executed using FastAPI's ``BackgroundTasks``.
    celery_broker: str | None = None

    # How long to wait when pinging databases to find out the fastest online database.
    do_ping_timeout: timedelta = timedelta(seconds=5)

    # Query service url (only used with "http" query client config)
    # TODO: once the `QueryClientConfig` is proven out, this can be removed.
    query_service: str | None = None

    # Query client configuration
    query_client: QueryClientConfig = Field(default_factory=QueryClientConfig)

    # The namespace where source nodes for registered tables should exist
    source_node_namespace: str | None = "source"

    # This specifies what the DJ_LOGICAL_TIMESTAMP() macro should be replaced with.
    # This defaults to an Airflow compatible value, but other examples include:
    #   ${dj_logical_timestamp}
    #   {{ dj_logical_timestamp }}
    #   $dj_logical_timestamp
    dj_logical_timestamp_format: str | None = "${dj_logical_timestamp}"

    # Prefix applied to Druid datasource names built by ``build_druid_spec``.
    # All DJ envs share a single Druid cluster; the prefix env-tags datasources
    # so test/prod cubes with the same definition don't collide. Default is the
    # prod value; set ``DRUID_DATASOURCE_PREFIX=dj_test__`` in the test deploy.
    druid_datasource_prefix: str = "dj__"

    # DJ UI host, used for OAuth redirection
    frontend_host: str | None = "http://localhost:3000"

    # Enabled transpilation plugin names
    transpilation_plugins: list[str] = ["default", "sqlglot"]

    # 128 bit DJ secret, used to encrypt passwords and JSON web tokens
    secret: str = "a-fake-secretkey"

    # GitHub OAuth application client ID
    github_oauth_client_id: str | None = None

    # GitHub OAuth application client secret
    github_oauth_client_secret: str | None = None

    # Google OAuth application client ID
    google_oauth_client_id: str | None = None

    # Google OAuth application client secret
    google_oauth_client_secret: str | None = None

    # Google OAuth application client secret file
    google_oauth_client_secret_file: str | None = None

    # Interval in seconds for which to expire service account tokens
    service_account_token_expire: int = 3600 * 24 * 30

    # Group membership provider
    # Options: "postgres" (uses group_members table), "static" (no membership),
    # or a custom implementation of the GroupMembershipProvider interface
    group_membership_provider: str = "postgres"

    # Authorization configuration
    # Provider for authorization checks:
    # - "rbac": Role-based access control (default)
    # - "passthrough": Always approve (testing/development)
    # - Custom implementations can be plugged in
    authorization_provider: str = "rbac"

    # Default access policy when no explicit RBAC rule exists:
    # - "permissive": Allow by default
    # - "restrictive": Deny by default
    default_access_policy: str = "permissive"  # or "restrictive"

    # Optional role name whose scopes are evaluated as a fallback when no
    # explicit grant matches. Lets a deployment express graceful defaults such
    # as "everyone gets read on *" without flipping the whole policy to
    # permissive. Applied before the default_access_policy fallback.
    default_access_role: str | None = None

    # Rules that require an explicit principal, group, or service-account grant.
    # Entries use "action:scope_type:scope_value", for example
    # "write:node:finance.*". Actions are exact, so configure each governed
    # mutating action and every resource shape separately.
    restrictive_scopes: list[str] = Field(default_factory=list)

    # Require configured break-glass admins before serving requests.
    # Restrictive default access also enables this check automatically.
    # RBAC_ADMIN_USERS uses JSON list syntax, for example ["admin-user"].
    rbac_require_admin: bool = False
    rbac_admin_users: list[str] = Field(default_factory=list)

    @field_validator("restrictive_scopes")
    @classmethod
    def validate_restrictive_scopes(cls, values: list[str]) -> list[str]:
        """Reject malformed policy rules while loading settings."""
        for value in values:
            parts = value.split(":")
            if len(parts) != 3:
                raise ValueError(
                    "restrictive scope must be 'action:scope_type:scope_value'",
                )
            action, scope_type, scope_value = parts
            if (
                action not in RESTRICTIVE_SCOPE_ACTIONS
                or scope_type not in RESTRICTIVE_SCOPE_TYPES
            ):
                raise ValueError(
                    "restrictive scope action and scope type must be supported values",
                )
            if (
                scope_value != scope_value.strip()
                or parse_scope_pattern(scope_value) is None
            ):
                raise ValueError(
                    "restrictive scope value must be '*', an exact scope, "
                    "or a subtree ending in '.*'",
                )
        return values

    # Exact namespaces or subtrees where a first human creator becomes the owner.
    # CREATOR_OWNED_NAMESPACE_PATTERNS uses JSON list syntax, for example
    # ["personal.*", "scratch"].
    creator_owned_namespace_patterns: list[str] = Field(default_factory=list)

    @field_validator("creator_owned_namespace_patterns")
    @classmethod
    def validate_creator_owned_namespace_patterns(
        cls,
        patterns: list[str],
    ) -> list[str]:
        """Require valid namespace patterns and reject global creator ownership."""
        for pattern in patterns:
            parsed = parse_scope_pattern(pattern)
            if (
                pattern != pattern.strip()
                or parsed is None
                or parsed[0] == "global"
                or not is_valid_namespace(parsed[1])
            ):
                raise ValueError(
                    f"`{pattern}` must be an exact namespace or a subtree ending in `.*`",
                )
        return patterns

    # Interval in seconds with which to expire caching of any indexes
    index_cache_expire: int = 60

    # Cache expiration for SQL endpoints
    query_cache_timeout: int = 86400 * 300

    # Maximum number of concurrent background cache refreshes.
    # Caps how many SQL rebuilds run simultaneously to avoid DB connection spikes.
    query_cache_max_concurrent_refreshes: int = 3

    # Maximum amount of nodes to return for requests to list all nodes
    node_list_max: int = 10000

    # Pre-aggregation output location
    # Used when generating combined SQL that references pre-agg tables
    preagg_catalog: str = "default"
    preagg_schema: str = "dj_preaggs"

    # Freshness gating for pre-aggregations. When enabled, a pre-aggregation is
    # only allowed to answer a query if the temporal range its table covers
    # (`min_temporal_partition`/`max_temporal_partition`, falling back to
    # `valid_through_ts`) contains the range the query asks for. Off by default:
    # enabling it can route queries away from pre-aggs that serve them today.
    preagg_freshness_gating: bool = False

    # Wall-clock staleness budget, in seconds, applied only when a query has no
    # discoverable upper bound on the pre-agg's temporal partition (such a query
    # implicitly asks for data through the present). When None, unbounded
    # queries are never rejected for staleness. Ignored unless
    # `preagg_freshness_gating` is on.
    preagg_max_staleness_seconds: int | None = None

    # Cube view output location
    # Used when generating CREATE OR REPLACE VIEW DDL for cube views
    view_catalog: str = "default"
    view_schema: str = "dj_views"

    # GitHub API configuration for git-backed branch management
    # API URL (defaults to github.com, override for GitHub Enterprise)
    github_api_url: str = "https://api.github.com"

    # Option 1: Simple PAT auth (recommended for OSS)
    # Set GITHUB_SERVICE_TOKEN to a Personal Access Token or fine-grained token
    github_service_token: str | None = None

    # Option 2: GitHub App auth (for internal/enterprise deployments)
    # Set all three to use GitHub App authentication instead of a PAT
    github_app_id: str | None = None
    github_app_private_key: str | None = None  # PEM-encoded private key
    github_app_installation_id: str | None = None

    @property
    def celery(self) -> Celery:
        """
        Return Celery app.
        """
        return Celery(__name__, broker=self.celery_broker)

    @property
    def cache(self) -> BaseCache | None:
        """
        Configure the Redis cache.
        """
        if self.redis_cache is None:
            return None

        parsed = urllib.parse.urlparse(self.redis_cache)
        return RedisCache(
            host=parsed.hostname,
            port=parsed.port,
            password=parsed.password,
            db=parsed.path.strip("/"),
        )

    seed_setup: SeedSetup = SeedSetup()

    @property
    def effective_reader_concurrency(self) -> int:
        return max(1, self.reader_db.pool_size // 2)

    @property
    def effective_writer_concurrency(self) -> int:
        return max(1, self.writer_db.pool_size // 2)
