---
name: datajunction-api
description: |
  Activate this skill when authoring DataJunction (DJ) nodes via the REST
  API directly (curl, HTTP clients) — typically for exploration, ad-hoc
  prototyping, or namespaces that aren't repo-backed. For modeling
  decisions and the decomposition workflow, invoke `datajunction-model`.
  For repo-backed YAML authoring (the production path), invoke
  `datajunction-repo`. For concepts, invoke `datajunction`.
  Keywords:
  - DJ API, REST API, curl
  - POST nodes/metric, POST nodes/dimension
  - API approach, direct API changes
  - prototyping, exploration
user-invocable: false
---

# DataJunction Direct API Authoring

Direct REST API authoring for DJ nodes. Use this skill for quick exploration, prototyping, or working in namespaces that don't use the repo-backed workflow.

For the modeling work upstream of any authoring (decomposition, naming, ratio decomposition, etc.), see `datajunction-model`. For the production-path equivalent of these patterns in YAML, see `datajunction-repo`.

## When to Use the API Approach

**✅ MUST use repo workflow instead** (`datajunction-repo`) for:
- Namespaces configured as repo-backed and read-only (`git_only: true`) — direct API changes are rejected

**✅ Should use repo workflow** for:
- Production changes (review required)
- Multi-node changes (related metrics/dimensions)
- Team environments (multiple contributors)
- Audit-trail requirements
- Complex refactoring

**✅ API workflow is appropriate** for:
- Quick exploration and prototyping
- Ad-hoc analysis
- Single-user, non-production namespaces
- Temporary metrics
- Non-production experiments
- Only when the target namespace is NOT read-only repo-backed

---

## Checking if a Namespace Is Repo-Backed

Before authoring via API, verify the target namespace allows it.

**Best approach — `get_node_details` MCP tool** (`datajunction-query` skill):

```
get_node_details(name="finance.total_revenue")
```

The response will include git repository information:
```
Git Repository:
  Repo: owner/dj-finance
  Branch: main
  Default Branch: main
  → This namespace is repo-backed (use git workflow for changes)
```

**Alternative — REST API** (shows read-only status):
```bash
curl -b ~/.dj/cookies.txt -X GET $DJ_URL/namespaces/finance/git

# Response:
{
  "github_repo_path": "owner/dj-finance",
  "git_branch": "main",
  "default_branch": "main",
  "git_path": "nodes/",
  "git_only": true    ← If true, namespace is read-only (API changes blocked)
}
```

**Decision tree:**
- **If git info is present AND `git_only: true`**: MUST use repo workflow (API changes will fail)
- **If git info is present AND `git_only: false`**: Can use either workflow
- **If git info is null**: Use API workflow (direct POST/PATCH)

---

## Creating a Metric

### Metric Structure

```sql
SELECT <aggregation_expression> AS <metric_name>
FROM <single_node>
```

Metrics select a **single expression** from a **single source, transform, or dimension node**. They cannot contain WHERE clauses — use CASE WHEN instead. See `datajunction-model` for the modeling rationale.

### Metric Metadata Fields

**Required:**
- `name` — Fully qualified metric name (e.g., `finance.total_revenue`)
- `query` — SQL aggregation expression

**Recommended:**
- `description` — Human-readable description
- `metric_metadata.direction` — `higher_is_better` / `lower_is_better` / `neutral`
- `metric_metadata.unit` — `dollar` / `unitless` (**⚠️ NOT `count`** — server rejects)
- `mode` — `draft` / `published`
- `required_dimensions` — Dimensions required for this metric to make sense
- `owners` — List of email addresses (prefer team emails)

### Examples

**COUNT**:
```bash
curl -b ~/.dj/cookies.txt -X POST $DJ_URL/nodes/metric/ \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "finance.num_transactions",
    "description": "Total number of transactions",
    "query": "SELECT COUNT(transaction_id) AS num_transactions FROM finance.transactions",
    "owners": ["data-platform-team@company.com"],
    "mode": "published"
  }'
```

**SUM**:
```bash
curl -b ~/.dj/cookies.txt -X POST $DJ_URL/nodes/metric/ \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "finance.total_revenue",
    "description": "Total revenue from all transactions",
    "query": "SELECT SUM(amount_usd) AS total_revenue FROM finance.transactions",
    "metric_metadata": {
      "direction": "higher_is_better",
      "unit": "dollar"
    },
    "owners": ["finance-data-team@company.com"],
    "mode": "published"
  }'
```

**Conditional aggregation** (CASE WHEN, not WHERE):
```bash
curl -b ~/.dj/cookies.txt -X POST $DJ_URL/nodes/metric/ \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "finance.completed_revenue",
    "description": "Revenue from completed non-refund transactions",
    "query": "
      SELECT SUM(
        CASE
          WHEN status = '\''completed'\'' AND refund_flag = false
          THEN amount_usd
          ELSE 0
        END
      ) AS completed_revenue
      FROM finance.transactions
    ",
    "metric_metadata": {
      "direction": "higher_is_better",
      "unit": "dollar"
    },
    "owners": ["finance-data-team@company.com"],
    "mode": "published"
  }'
```

**Ratio over base metrics** (decompose first, then derive — see `datajunction-model`):
```bash
# Step 1: create the base metrics (one curl each)
curl -X POST $DJ_URL/nodes/metric/ -d '{
  "name": "finance.clicks",
  "query": "SELECT COUNT_IF(event = '\''click'\'') FROM finance.events",
  "owners": ["marketing@company.com"],
  "mode": "published"
}'

curl -X POST $DJ_URL/nodes/metric/ -d '{
  "name": "finance.impressions",
  "query": "SELECT COUNT_IF(event = '\''impression'\'') FROM finance.events",
  "owners": ["marketing@company.com"],
  "mode": "published"
}'

# Step 2: derived ratio metric referencing the base metrics
curl -X POST $DJ_URL/nodes/metric/ -d '{
  "name": "finance.conversion_rate",
  "description": "Click-through rate as percentage",
  "query": "SELECT finance.clicks * 100.0 / NULLIF(finance.impressions, 0)",
  "metric_metadata": {
    "direction": "higher_is_better",
    "unit": "unitless"
  },
  "owners": ["marketing@company.com"],
  "mode": "published"
}'
```

DJ automatically handles divide-by-zero, but `NULLIF()` is extra safety.

---

## Creating Other Node Types via API

Same pattern as metrics — POST JSON to the appropriate endpoint:

- `POST /nodes/source/` — source nodes (catalog/schema/table refs)
- `POST /nodes/dimension/` — dimension nodes
- `POST /nodes/transform/` — transform nodes
- `POST /nodes/cube/` — cubes (metric + dimension combinations)

For the YAML-equivalent shapes of each, see `datajunction-repo` — the field set is the same, just expressed in JSON instead of YAML.

---

## Updating and Deleting Nodes

**Update** (PATCH):
```bash
curl -b ~/.dj/cookies.txt -X PATCH $DJ_URL/nodes/finance.total_revenue/ \
  -H 'Content-Type: application/json' \
  -d '{"description": "Updated description"}'
```

**Deactivate** (soft delete):
```bash
curl -b ~/.dj/cookies.txt -X DELETE $DJ_URL/nodes/finance.total_revenue/
```

Deactivated nodes can be revived. For hard-delete (irreversible), use the `dj` CLI: `dj delete-node finance.total_revenue --hard`.
