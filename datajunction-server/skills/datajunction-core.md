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

DJ uses normalized star schema modeling where **dimension links** connect fact tables to dimension tables:

```
[Fact Table: transactions]
    ├─ dimension_link: user_id → [Dimension: users]
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

**Key insight**: When you define dimension links on a node, any metric built on that node automatically inherits access to all dimension attributes. DJ automatically discovers and generates the necessary joins.

---

## Dimension Links

Dimension links define how nodes join to dimensions. They enable automatic join path discovery.

### Where to Define Them

Dimension links can be defined on **three node types**:

#### 1. Source Nodes (Most Common)
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
  - Not queryable via SQL endpoints
  - Use for development/testing
  - Can be invalid SQL - that's okay while drafting

- **`published`**: Production-ready
  - Queryable via SQL endpoints
  - Should be valid and tested
  - Visible to all users

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
- ✅ `core.users`
- ✅ `clean.user_events`
- ❌ `revenue` (missing namespace)

### Namespace Organization

**Common conventions:**
- `core.*` - Shared dimensions (users, dates, regions)
- `finance.*` - Financial metrics & facts
- `growth.*` - User engagement & activation
- `product.*` - Product usage & features
- `raw.*` - Raw source tables
- `clean.*` - Cleaned transforms

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
