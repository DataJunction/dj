# DataJunction Repo Workflow

> **Skill**: `datajunction-repo-workflow`
> **Purpose**: Git-backed namespace development with YAML node definitions
> **Requires**: `datajunction-core`, `datajunction-builder` (activate together for concepts)
> **Keywords**: git workflow, repo-backed namespace, YAML nodes, branch development, gitops, pull request, version control

---

## Overview

DataJunction supports **repo-backed namespaces** where node definitions are stored as YAML files in a git repository. This enables:

- ✅ **Version control** for your semantic layer
- ✅ **Pull request review workflows** for changes
- ✅ **Branch-based development** (feature branches, environments)
- ✅ **Declarative configuration** (infrastructure as code)
- ✅ **Audit trail** of all changes
- ✅ **Team collaboration** with code review

---

## Two Ways to Contribute to DJ

**IMPORTANT**: Always validate with the user how they want to contribute before making changes.

### Option 1: Direct API Changes (Immediate)

**How it works:**
- POST JSON to DJ API endpoints
- Changes take effect immediately in DJ
- No git operations needed

**Good for:**
- Quick iterations and exploration
- Ad-hoc analysis
- Namespaces without repo backing

**Example:**
```bash
curl -b ~/.dj/cookies.txt -X POST $DJ_URL/nodes/metric/ \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "finance.revenue",
    "query": "SELECT SUM(amount) FROM transactions",
    "mode": "published"
  }'
```

### Option 2: Repo-Backed Workflow (Version Controlled)

**How it works:**
- Write YAML node definitions in git repo
- Create feature branches for changes
- Submit pull requests for review
- Merge to default branch
- DJ syncs changes automatically

**Good for:**
- Production changes requiring review
- Team collaboration
- Maintaining history and audit trail
- Multi-step changes across many nodes

**Example workflow:**
1. Create branch: `finance.feature-new-metrics`
2. Add YAML file: `nodes/metrics/revenue.yaml`
3. Commit and push
4. Create PR for review
5. Merge → DJ syncs automatically

---

## Checking if a Namespace is Repo-Backed

Before making changes, check if the namespace is backed by a git repository:

```bash
# Get namespace info
curl -b ~/.dj/cookies.txt -X GET $DJ_URL/namespaces/finance

# Response includes:
{
  "namespace": "finance",
  "repo_url": "https://github.com/myorg/dj-finance.git",  # ← Repo-backed!
  "default_branch": "main",
  ...
}
```

**If `repo_url` is present**: Use repo workflow (this skill)
**If `repo_url` is null**: Use API workflow (datajunction-builder skill)

---

## Setting Up Repo-Backed Namespaces

### Namespace Configuration

To configure a namespace as repo-backed:

```bash
curl -b ~/.dj/cookies.txt -X POST $DJ_URL/namespaces/ \
  -H 'Content-Type: application/json' \
  -d '{
    "namespace": "finance",
    "repo_url": "https://github.com/myorg/dj-finance.git",
    "default_branch": "main"
  }'
```

**Configuration fields:**
- `namespace`: Namespace name (e.g., `finance`)
- `repo_url`: Git repository URL
- `default_branch`: Default branch (usually `main` or `master`)

### Repository Structure

The repository should follow this structure:

```
dj-finance/
├── nodes/
│   ├── sources/
│   │   ├── transactions.yaml
│   │   └── users.yaml
│   ├── dimensions/
│   │   ├── date.yaml
│   │   └── user.yaml
│   ├── metrics/
│   │   ├── revenue.yaml
│   │   └── user_count.yaml
│   └── transforms/
│       └── clean_transactions.yaml
├── cubes/
│   └── revenue_cube.yaml
└── README.md
```

---

## Branch-Based Development

### Understanding Branch-Based Namespaces

When you create a branch in a repo-backed namespace, DJ creates a **corresponding namespace** that points to that branch.

**Naming convention:**
```
{namespace}.{branch_name}
```

**Examples:**
- `finance.main` → points to `main` branch of finance repo
- `finance.feature-new-metrics` → points to `feature-new-metrics` branch
- `finance.staging` → points to `staging` branch

### Creating a Branch

**Option A: Via DJ API**
```bash
curl -b ~/.dj/cookies.txt -X POST $DJ_URL/namespaces/finance/branches \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "feature-new-metrics",
    "from_branch": "main"
  }'

# This creates:
# 1. Git branch: feature-new-metrics (from main)
# 2. DJ namespace: finance.feature-new-metrics
```

**Option B: Manually in Git**
```bash
cd dj-finance/
git checkout -b feature-new-metrics
git push origin feature-new-metrics

# Then tell DJ about it (DJ may auto-discover branches)
```

### Working in a Branch

Once you have a branch namespace (e.g., `finance.feature-new-metrics`):

1. **Clone the repo** (if not already done)
   ```bash
   git clone https://github.com/myorg/dj-finance.git
   cd dj-finance
   ```

2. **Checkout your feature branch**
   ```bash
   git checkout feature-new-metrics
   ```

3. **Create/edit YAML files** (see YAML format below)
   ```bash
   vi nodes/metrics/new_revenue_metric.yaml
   ```

4. **Commit and push**
   ```bash
   git add nodes/metrics/new_revenue_metric.yaml
   git commit -m "Add new revenue metric"
   git push origin feature-new-metrics
   ```

5. **DJ syncs automatically** - changes appear in `finance.feature-new-metrics` namespace

6. **Create PR to merge** to default branch
   ```bash
   # Via GitHub UI or:
   gh pr create --title "Add new revenue metric" --base main
   ```

7. **Merge PR** → changes sync to `finance.main` namespace

---

## YAML Node Definitions

All node types can be defined as YAML files in the repository.

### Source Node YAML

```yaml
# nodes/sources/transactions.yaml
name: finance.transactions
description: Raw transaction data from payment system
type: source
catalog: prod_catalog
schema_: finance
table: transactions_table

columns:
  - name: transaction_id
    type: bigint
  - name: user_id
    type: bigint
  - name: amount_usd
    type: decimal(10,2)
  - name: transaction_date
    type: date
  - name: status
    type: string

dimension_links:
  - dimension: common.dimensions.users
    join_on: finance.transactions.user_id = common.dimensions.users.user_id

  - dimension: common.dimensions.date
    join_on: finance.transactions.transaction_date = common.dimensions.date.dateint

mode: published
```

### Dimension Node YAML

```yaml
# nodes/dimensions/user.yaml
name: finance.user
description: User dimension with attributes
type: dimension
query: |
  SELECT
    user_id,
    username,
    email,
    country_code,
    signup_date,
    tier
  FROM finance.users

primary_key:
  - user_id

dimension_links:
  - dimension: common.dimensions.date
    join_on: finance.user.signup_date = common.dimensions.date.dateint

  - dimension: common.dimensions.country
    join_on: finance.user.country_code = common.dimensions.country.country_code

mode: published
```

### Metric Node YAML

```yaml
# nodes/metrics/revenue.yaml
name: finance.total_revenue
description: Total revenue from completed transactions
type: metric
query: |
  SELECT
    SUM(
      CASE
        WHEN status = 'completed' AND refund_flag = false
        THEN amount_usd
        ELSE 0
      END
    ) AS total_revenue
  FROM finance.transactions

required_dimensions:
  - common.dimensions.date.dateint

metric_metadata:
  direction: higher_is_better
  unit: dollar

mode: published
```

**Important metric rules:**
- ❌ **No WHERE clauses** in metric queries (use CASE WHEN instead)
- ✅ **Use CASE WHEN** for conditional aggregation
- ✅ **Include required_dimensions** for time-based metrics
- ✅ **Add metric_metadata** for direction and unit

### Transform Node YAML

```yaml
# nodes/transforms/clean_transactions.yaml
name: finance.clean_transactions
description: Cleaned transaction data with standardized status
type: transform
query: |
  SELECT
    transaction_id,
    user_id,
    amount_usd,
    transaction_date,
    CASE
      WHEN status IN ('complete', 'completed', 'success') THEN 'completed'
      WHEN status IN ('fail', 'failed', 'error') THEN 'failed'
      ELSE status
    END AS status_clean,
    refund_flag
  FROM finance.transactions

dimension_links:
  - dimension: common.dimensions.users
    join_on: finance.clean_transactions.user_id = common.dimensions.users.user_id

  - dimension: common.dimensions.date
    join_on: finance.clean_transactions.transaction_date = common.dimensions.date.dateint

mode: published
```

### Cube YAML

```yaml
# cubes/revenue_cube.yaml
name: finance.revenue_cube
description: Pre-computed revenue metrics by date and region
metrics:
  - finance.total_revenue
  - finance.avg_transaction_value

dimensions:
  - common.dimensions.date.dateint
  - common.dimensions.date.month
  - common.dimensions.users.country_code

mode: published
```

---

## Complete Workflow Example

### Scenario: Add a new metric to the finance namespace

**Step 1: Check if finance is repo-backed**
```bash
curl -b ~/.dj/cookies.txt -X GET $DJ_URL/namespaces/finance

# Response shows:
# "repo_url": "https://github.com/myorg/dj-finance.git"
# → Use repo workflow!
```

**Step 2: Create feature branch**
```bash
# Via DJ API
curl -b ~/.dj/cookies.txt -X POST $DJ_URL/namespaces/finance/branches \
  -H 'Content-Type: application/json' \
  -d '{
    "name": "feature-add-churn-metric",
    "from_branch": "main"
  }'

# Creates namespace: finance.feature-add-churn-metric
```

**Step 3: Clone repo and checkout branch**
```bash
git clone https://github.com/myorg/dj-finance.git
cd dj-finance
git checkout feature-add-churn-metric
```

**Step 4: Create metric YAML file**
```bash
cat > nodes/metrics/churn_rate.yaml <<EOF
name: finance.churn_rate
description: Monthly user churn rate
type: metric
query: |
  SELECT
    CAST(SUM(CASE WHEN churned = true THEN 1 ELSE 0 END) AS DOUBLE) /
    NULLIF(COUNT(DISTINCT user_id), 0) AS churn_rate
  FROM finance.user_activity

required_dimensions:
  - common.dimensions.date.month

metric_metadata:
  direction: lower_is_better
  unit: percentage

mode: published
EOF
```

**Step 5: Commit and push**
```bash
git add nodes/metrics/churn_rate.yaml
git commit -m "Add monthly churn rate metric"
git push origin feature-add-churn-metric
```

**Step 6: DJ syncs automatically**
```bash
# Verify metric exists in branch namespace
curl -b ~/.dj/cookies.txt -X GET $DJ_URL/nodes/finance.churn_rate

# Query it in the branch namespace
curl -b ~/.dj/cookies.txt -X POST $DJ_URL/sql/metrics/v3 \
  -H 'Content-Type: application/json' \
  -d '{
    "metrics": ["finance.churn_rate"],
    "dimensions": ["common.dimensions.date.month"],
    "filters": ["common.dimensions.date.year = 2024"]
  }'
```

**Step 7: Create PR for review**
```bash
gh pr create \
  --title "Add monthly churn rate metric" \
  --body "Adds a new metric to track monthly user churn rate" \
  --base main
```

**Step 8: Get review, merge PR**
```bash
# After approval, merge via GitHub UI or:
gh pr merge --squash
```

**Step 9: Changes sync to production**
```bash
# Metric now available in finance.main (production)
curl -b ~/.dj/cookies.txt -X GET $DJ_URL/nodes/finance.churn_rate
```

---

## Syncing Changes

### How DJ Syncs from Git

DJ monitors repo-backed namespaces and syncs changes automatically:

1. **Push to branch** → DJ detects changes
2. **Parse YAML files** → Validate node definitions
3. **Update DJ catalog** → Create/update nodes
4. **Trigger revalidation** → Check node status

### Manual Sync (if needed)

If automatic sync doesn't work:

```bash
curl -b ~/.dj/cookies.txt -X POST $DJ_URL/namespaces/finance/sync \
  -H 'Content-Type: application/json' \
  -d '{"branch": "feature-add-churn-metric"}'
```

### Checking Sync Status

```bash
# Get namespace sync info
curl -b ~/.dj/cookies.txt -X GET $DJ_URL/namespaces/finance/sync-status

# Response includes:
{
  "last_sync": "2024-01-15T10:30:00Z",
  "status": "success",
  "branch": "main",
  "commit": "abc123def456"
}
```

---

## Best Practices

### When to Use Repo Workflow

✅ **Use repo workflow for:**
- Production changes (require review)
- Multi-node changes (related metrics/dimensions)
- Team environments (multiple contributors)
- Changes requiring audit trail
- Complex refactoring

### When to Use API Workflow

✅ **Use API workflow for:**
- Quick exploration and prototyping
- Ad-hoc analysis
- Single-user namespaces
- Temporary metrics
- Non-production experiments

### Workflow Tips

1. **Always create feature branches** - never commit directly to default branch
2. **Use descriptive branch names** - `feature-add-revenue-metrics` not `fix-stuff`
3. **Write clear commit messages** - explain the "why" not just the "what"
4. **Keep PRs focused** - one logical change per PR
5. **Test in branch namespace** - validate metrics work before merging
6. **Use draft mode first** - set `mode: draft` while developing, `published` when ready
7. **Document in YAML** - use `description` fields thoroughly

### Directory Organization

**Recommended structure:**
```
nodes/
├── sources/        # Raw data tables
├── dimensions/     # Dimensional entities
├── transforms/     # Data cleaning/prep
└── metrics/        # Business metrics
    ├── finance/    # Sub-organize by area if needed
    ├── growth/
    └── product/
```

---

## Validation and Testing

### Validate Before Committing

```bash
# Check if YAML is valid
yamllint nodes/metrics/new_metric.yaml

# Test query syntax locally (if possible)
dj validate nodes/metrics/new_metric.yaml
```

### Test in Branch Namespace

After pushing to feature branch:

```bash
# 1. Check node status
curl -b ~/.dj/cookies.txt -X GET $DJ_URL/nodes/finance.new_metric

# 2. Validate node
curl -b ~/.dj/cookies.txt -X GET $DJ_URL/nodes/finance.new_metric/validate

# 3. Test query
curl -b ~/.dj/cookies.txt -X POST $DJ_URL/sql/metrics/v3 \
  -H 'Content-Type: application/json' \
  -d '{
    "metrics": ["finance.new_metric"],
    "dimensions": ["common.dimensions.date.dateint"],
    "filters": ["common.dimensions.date.dateint >= 20240101"],
    "limit": 10
  }'
```

### CI/CD Integration

Example GitHub Actions workflow:

```yaml
# .github/workflows/validate-dj-nodes.yml
name: Validate DJ Nodes

on:
  pull_request:
    branches: [main]

jobs:
  validate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2

      - name: Validate YAML syntax
        run: |
          yamllint nodes/

      - name: Validate DJ nodes
        run: |
          # Login to DJ
          TOKEN=$(curl -X POST $DJ_URL/basic/login/ \
            -d "username=$DJ_USER&password=$DJ_PWD" | jq -r .token)

          # Validate each changed node
          for file in $(git diff --name-only origin/main...HEAD | grep '.yaml$'); do
            curl -H "Authorization: Bearer $TOKEN" \
              -X POST $DJ_URL/validate \
              -d @$file
          done
```

---

## Troubleshooting

### Sync Not Working

```bash
# Check sync status
curl -b ~/.dj/cookies.txt -X GET $DJ_URL/namespaces/finance/sync-status

# Manually trigger sync
curl -b ~/.dj/cookies.txt -X POST $DJ_URL/namespaces/finance/sync

# Check logs
curl -b ~/.dj/cookies.txt -X GET $DJ_URL/namespaces/finance/sync-logs
```

### YAML Validation Errors

```bash
# Get detailed validation errors
curl -b ~/.dj/cookies.txt -X GET $DJ_URL/nodes/finance.problematic_metric/validate

# Common issues:
# - Missing required fields (name, type, query)
# - Invalid node type
# - Malformed YAML syntax
# - Invalid dimension link references
```

### Branch Not Appearing

```bash
# List all namespace branches
curl -b ~/.dj/cookies.txt -X GET $DJ_URL/namespaces/finance/branches

# Create missing branch namespace
curl -b ~/.dj/cookies.txt -X POST $DJ_URL/namespaces/finance/branches \
  -H 'Content-Type: application/json' \
  -d '{"name": "my-branch", "from_branch": "main"}'
```

---

## Summary: Decision Flow

```
User wants to create/modify a node
    ↓
Check if namespace is repo-backed
    ↓
┌─────────────────────────┬──────────────────────────┐
│   Repo-backed?          │   Not repo-backed?       │
│   (repo_url present)    │   (repo_url is null)     │
└────────┬────────────────┴──────────┬───────────────┘
         ↓                           ↓
    Use Repo Workflow           Use API Workflow
    (this skill)                (builder skill)
         ↓                           ↓
    1. Create branch            1. POST JSON to API
    2. Write YAML files         2. Changes immediate
    3. Commit & push            3. Done!
    4. Create PR
    5. Review & merge
    6. Auto-sync to DJ
```

**Always ask the user**: "Would you like to use the repo workflow (with PR review) or make direct API changes?"

---

## Related Skills

- **datajunction-core**: Understanding DJ concepts (dimension links, node types)
- **datajunction-builder**: API-based node creation (for non-repo namespaces)
- **datajunction-consumer**: Querying metrics (works with both workflows)
