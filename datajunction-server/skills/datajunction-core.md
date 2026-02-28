# DataJunction Core

> **Skill**: `datajunction-core`
> **Purpose**: Foundational DJ concepts - always activate this with other DJ skills
> **Keywords**: DataJunction, DJ, semantic layer, star schema, dimension links

---

## What is DataJunction?

DataJunction is a semantic layer that provides a unified interface to query metrics across data sources using dimensional modeling (star schema).

**Key capabilities:**
- Define metrics once, query anywhere
- Automatic join path discovery via dimension links
- Multi-engine SQL generation (Trino, Spark, Druid, DuckDB, etc.)
- Lineage tracking and impact analysis
- Materialization for performance optimization

---

## Connecting to DataJunction

To interact with DataJunction, you'll make authenticated HTTP requests to the DJ API server.

**IMPORTANT**: Before proceeding, you need the DJ server URL. If the user hasn't provided it yet, ask them:
- "What's your DJ server URL?"
- Common values: `http://localhost:8000` (local), `https://dj.company.com` (production)

### Setup (Development)

```bash
# Set your DJ server URL and credentials
export DJ_URL=http://localhost:8000
export DJ_USER=dj
export DJ_PWD=dj

# Test the connection (health endpoint is public, no auth needed)
curl $DJ_URL/health
```

### Authentication

**DataJunction requires authentication for most operations** (creating nodes, querying, etc.). Only a few endpoints like `/health` and `/docs` are public.

DJ uses **JWT token authentication**. You must first login to get a token, then use that token in subsequent requests.

#### Step 1: Login to Get Token

```bash
# Create DJ config directory if it doesn't exist
mkdir -p ~/.dj

# Login and save cookies
curl -X POST $DJ_URL/basic/login/ \
  -d "username=$DJ_USER&password=$DJ_PWD" \
  -c ~/.dj/cookies.txt

# Response: {"message": "Logged in successfully"}
# Token is saved in ~/.dj/cookies.txt as an HTTP-only cookie
```

#### Step 2: Make Authenticated Requests

**Option A: Using Cookies** (simpler, recommended for CLI):
```bash
# Use the cookie file with -b flag
curl -b ~/.dj/cookies.txt -X GET $DJ_URL/nodes/
```

**Option B: Using Bearer Token** (for programmatic access):
```bash
# Extract token from cookie file (if needed for API clients)
TOKEN=$(grep djSession ~/.dj/cookies.txt | cut -f7)

# Use Bearer token in Authorization header
curl -H "Authorization: Bearer $TOKEN" -X GET $DJ_URL/nodes/
```

### Connection Tips

- **Login once, reuse token**: Login saves a token valid for 365 days (default)
- **Cookie file**: `~/.dj/cookies.txt` persists across sessions
- **Always login first**: You need a valid JWT token before making authenticated requests
- **Reuse cookies**: Use `-b ~/.dj/cookies.txt` in all subsequent requests
- **Token expiration**: If token expires, just login again to refresh
- **Always use `$DJ_URL`**: Makes connection URL configurable
- **Test connectivity**: Check `/health` endpoint first (no auth needed)
- **Default local server**: `http://localhost:8000` (when running DJ locally)
- **Default credentials**: `DJ_USER=dj`, `DJ_PWD=dj` (local development)
- **Headers**: Most POST/PATCH requests need `-H 'Content-Type: application/json'`

### Complete Example Workflow

```bash
# 1. Set up environment
export DJ_URL=http://localhost:8000
export DJ_USER=dj
export DJ_PWD=dj

# 2. First-time setup: Login and save token (only needed once)
mkdir -p ~/.dj
curl -X POST $DJ_URL/basic/login/ \
  -d "username=$DJ_USER&password=$DJ_PWD" \
  -c ~/.dj/cookies.txt

# 3. Make authenticated requests using cookies
curl -b ~/.dj/cookies.txt -X GET $DJ_URL/nodes/

# 4. Create a metric
curl -b ~/.dj/cookies.txt -X POST $DJ_URL/nodes/metric/ \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "finance.revenue",
    "query": "SELECT SUM(amount) as revenue FROM finance.transactions",
    "description": "Total revenue",
    "mode": "published"
  }'

# 5. Generate SQL
curl -b ~/.dj/cookies.txt -X POST $DJ_URL/sql/metrics/v3 \
  -H 'Content-Type: application/json' \
  -d '{
    "metrics": ["finance.revenue"],
    "dimensions": ["common.date.dateint"],
    "filters": ["common.date.dateint >= 20240101"]
  }'
```

**Note for Deployment-Specific Setups**: If you're using DataJunction at an organization with custom authentication or infrastructure, your deployment may provide custom connection instructions. Check with your platform team for the correct DJ server URL and authentication method.

---

## Node Types

DataJunction organizes entities into different node types:

| Node Type | Description | Example |
|-----------|-------------|---------|
| **Source** | Physical table or view in your data warehouse | `catalog.finance.transactions_table` |
| **Transform** | SQL transformation of other nodes (cleaning, filtering, joining) | `events.clean.user_events` |
| **Dimension** | Entity with attributes (users, products, dates, regions) | `core.users`, `core.dates` |
| **Metric** | Aggregated measure (revenue, count, ratios) | `finance.total_revenue` |
| **Cube** | Pre-computed metric combinations for performance | `finance.revenue_cube` |

---

## Star Schema & Dimensional Modeling

DJ uses normalized star schema modeling where **dimension links** connect fact tables to dimension tables, and dimensional entities to other dimensions:

```
[Fact Table: transactions]
    ├─ dimension_link: user_id → [Dimension: users]
    │   ├─ Attributes: country, signup_date, tier
    │   │  └─ dimension_link: signup_date → [Dimension: date]
    │   │      └─ Attributes: month, quarter, year
    │   └─ Attributes: country, signup_date, tier
    ├─ dimension_link: product_id → [Dimension: products]
    │   └─ Attributes: category, price, brand
    └─ dimension_link: date → [Dimension: dates]
        └─ Attributes: month, quarter, year

[Metric: revenue]
    ├─ Defined on: transactions
    └─ Auto-inherits dimension links from transactions
        ├─ users.country (via user_id)
        ├─ products.category (via product_id)
        └─ dates.month (via date)
```

**Key insight**: When you define dimension links on a node, any metric built on that node automatically inherits access to all dimension attributes. DJ automatically discovers and generates the necessary joins across the entire dimensions graph.

---

## Dimension Links

Dimension links define how nodes join to dimensions. They enable automatic join path discovery.

### Where to Define Them

Dimension links can be defined on **three node types**:

#### 1. Source Nodes
When the source table cleanly represents a semantic entity.

#### 2. Transform Nodes (Clean Semantic Representation)
When source tables don't accurately represent your semantic entity - use transforms to clean data first.

#### 3. Dimension Nodes (Build Dimensional Graph)
Dimensions can link to other dimensions, creating a rich dimensional graph.

**Example dimensional graph:**
```
transactions → users → countries → regions
```

Metrics on `transactions` can now group by:
- `users.country_code` (1 hop)
- `countries.country_name` (2 hops via users → countries)
- `regions.population` (3 hops via users → countries → regions)

---

## Dimension Links with Roles

When a node references the same dimension node multiple times, use **roles** to disambiguate:

```json
{
  "dimension_links": [
    {
      "dimension": "core.users",
      "join_on": "orders.buyer_id = core.users.user_id",
      "role": "buyer"
    },
    {
      "dimension": "core.users",
      "join_on": "orders.seller_id = core.users.user_id",
      "role": "seller"
    }
  ]
}
```

**Query with**: `buyer.country` vs `seller.country`

DJ uses the role prefix to generate the correct joins.

---

## Node Status & Validity

DJ tracks TWO separate state systems:

### 1. Mode (User-Controlled Lifecycle)

Users control this via the `mode` field:

- **`draft`**: Work in progress
  - Use for development/testing
  - Can be invalid SQL - that's okay while drafting

- **`published`**: Production-ready
  - Should be valid and tested
  - Indicates that it's ready for use

### 2. Status (System-Controlled Validation)

DJ automatically validates nodes and sets status:

- **`valid`**: Passes all validation checks
  - SQL is syntactically correct
  - Referenced nodes exist
  - Metadata is correct
  - Dimension links are valid

- **`invalid`**: Failed validation
  - SQL syntax error
  - References non-existent node
  - Circular dependency
  - Dimension link broken

**Key insight**: `mode` and `status` are independent:

| Mode | Status | Meaning |
|------|--------|---------|
| `draft` | `valid` | Good draft, not yet published |
| `draft` | `invalid` | Work in progress, has errors |
| `published` | `valid` | Production-ready ✅ |
| `published` | `invalid` | Was working, now broken (upstream change) ⚠️ |

---

## API Endpoints - Use V3!

⚠️ **Important**: Always use V3 endpoints for SQL generation.

### ✅ Correct Endpoints (Use These)

```bash
# Generate metric SQL
POST /sql/metrics/v3

# Generate measures SQL (for pre-aggregation/materialization)
POST /sql/measures/v3
```

### ❌ Deprecated Endpoints (Don't Use)

- `GET /sql` - Old, less powerful
- `GET /sql/v1` - Deprecated
- `GET /sql/v2` - Use v3 instead

**V3 advantages:**
- Supports complex cube queries
- Better filter handling
- Measure-level pre-aggregation
- Improved join path discovery

---

## Common Patterns

### Naming Convention

Use fully qualified names with namespace:

```
namespace.node_name
```

**Examples:**
- ✅ `finance.total_revenue`
- ✅ `common.dimensions.users`
- ✅ `clean.user_events`
- ❌ `revenue` (missing namespace)

### Namespace Organization

Namespaces are organized by business area: e.g., finance, growth, product etc.

**Common conventions:**
- `common.dimensions.*` - Shared dimensions (users, dates, regions)
- `finance.*` - Financial metrics & facts
- `growth.*` - User engagement & activation
- `product.*` - Product usage & features
- `source.*` - Raw source tables

### Workflow: Draft → Validate → Test → Publish

```bash
# 1. Create in draft mode
POST /nodes/metric
{"name": "finance.new_metric", "mode": "draft", ...}

# 2. Validate
GET /nodes/finance.new_metric/validate

# 3. Test SQL generation
POST /sql/metrics/v3
{"metrics": ["finance.new_metric"], ...}

# 4. Publish
PATCH /nodes/finance.new_metric
{"mode": "published"}
```

---

## Key Concepts Summary

| Concept | Description |
|---------|-------------|
| **Node** | Any entity in DJ (source, dimension, metric, etc.) |
| **Dimension Link** | Defines how nodes join to dimensions |
| **Dimensional Graph** | Network of dimensions linked to each other |
| **Star Schema** | Fact tables at center, dimension tables radiate out |
| **Auto-Join** | DJ automatically finds join paths via dimension links |
| **Mode** | User-controlled: draft vs published |
| **Status** | System-controlled: valid vs invalid |
| **Namespace** | Logical grouping (finance, core, growth) |

---

## When to Use Other Skills

- **Building nodes, metrics, cubes?** → Activate `datajunction-builder`
- **Querying metrics, generating SQL?** → Activate `datajunction-consumer`
- **Working in specific namespace?** → Activate namespace skill (e.g., `dj-finance`)

---

## Additional Resources

- **DJ Documentation**: https://datajunction.io
- **GitHub**: https://github.com/DataJunction/dj
- **API Docs**: {server_url}/docs (Swagger UI)
- **GraphQL Playground**: {server_url}/graphql
