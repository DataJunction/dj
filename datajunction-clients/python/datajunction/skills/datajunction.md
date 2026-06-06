---
name: datajunction
description: |
  Activate this skill whenever working with DataJunction (DJ) semantic layer.
  Core concepts and shared vocabulary used by every DJ workflow. For
  querying metrics, invoke `datajunction-query`. For creating or modifying
  nodes (API or repo-backed), invoke `datajunction-build`.
  Keywords:
  - DataJunction, DJ
  - semantic layer
  - dimension link, dimension links
  - star schema
  - node types
  - source, transform, dimension, metric, cube
  - mode, status, valid, invalid, draft, published
  - namespace
user-invocable: false
---

# DataJunction

## What is DataJunction?

DataJunction is a semantic layer that provides a unified interface to query metrics across data sources using dimensional modeling (star schema).

**Key capabilities:**
- Define metrics once, query anywhere
- Automatic join path discovery via dimension links
- Multi-engine SQL generation (Trino, Spark, Druid, DuckDB, etc.)
- Lineage tracking and impact analysis
- Materialization for performance optimization

**Companion skills:**
- `datajunction-query` — discovery, SQL generation, fetching data (consumer workflows)
- `datajunction-build` — creating metrics/dimensions/transforms/cubes, query-to-DJ decomposition, repo-backed YAML workflow (author workflows)

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

**Basic structure:**
```yaml
dimension_links:
  - dimension: common.dimensions.users
    join_type: left              # Optional: left, right, inner (default: left)
    join_on: finance.transactions.user_id = common.dimensions.users.user_id
```

**Join types:**
- `left` - Left outer join (default, most common)
- `right` - Right outer join
- `inner` - Inner join (only matching rows)

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

## SQL Query Constraints

**SELECT * is NOT supported**
- Always explicitly list columns in queries
- DJ needs to know exact columns for type inference and validation

```sql
-- ❌ NOT ALLOWED
SELECT * FROM finance.transactions

-- ✅ CORRECT
SELECT transaction_id, user_id, amount_usd, transaction_date
FROM finance.transactions
```

**Column types are auto-inferred from queries**
- DJ analyzes your SQL and infers column types automatically
- No need to manually specify types in YAML

**Use CAST() to control types**
- If you need a specific type, use explicit CAST in your query

```sql
-- Force specific type
SELECT CAST(user_id AS bigint), CAST(revenue AS decimal(18,2))
FROM finance.transactions
```

For *metric*-specific constraints (no WHERE clauses, single-expression rule, cross-metric composition), see the `datajunction-build` skill.

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
| **Repo-Backed** | Namespace definitions stored in git as YAML |
| **Branch Namespace** | DJ namespace pointing to specific git branch |

---

## Additional Resources

- **DJ Documentation**: https://datajunction.io
- **GitHub**: https://github.com/DataJunction/dj
- **API Docs**: {server_url}/docs (Swagger UI)
- **GraphQL Playground**: {server_url}/graphql
