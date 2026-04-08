---
weight: 3
title: "Git Integration"
---

DJ provides full Git integration for managing your node definitions. Link a namespace to a repository, then create branches, commit changes, open pull requests, and merge—all from the DJ UI.

## Overview

Link any namespace to a Git repository to enable version-controlled workflows. Once linked, you choose how to work:

- **Git as source of truth**: Make the namespace read-only so all changes must come from Git commits (recommended for production)
- **UI-driven development**: Create branches, edit nodes in the UI, commit your changes, and open PRs to merge them back

## Recommended Workflow

This section walks through a typical setup where your production namespace is linked to the `main` branch and protected from direct edits.

### 1. Configure Your Production Namespace

Link your production namespace (e.g., `demo.metrics.main`) to your Git repository's `main` branch with **git-only** enabled. This makes the namespace read-only -- all changes must flow through Git.

| Setting | Value |
|---------|-------|
| Repository | `myorg/dj-definitions` |
| Branch | `main` |
| Path | `nodes/` |
| Git-only | `true` |

With this configuration, direct UI edits on the `demo.metrics.main` namespace are prohibited. Changes can only be deployed by merging commits to the `main` branch in the `myorg/dj-definitions` repository.

### 2. Create a Branch to Make Changes

When you want to add or modify nodes:

1. Navigate to your production namespace
2. Click **Create Branch**
3. Enter a branch name (e.g., `add-revenue-metrics`)

This creates:
- A new Git branch from `main`
- A new DJ namespace (e.g., `demo.metrics.add_revenue_metrics`)
- Copies of all nodes from the production namespace

### 3. Make Changes in the Branch Namespace

In your new branch namespace, you can freely:
- Create new nodes
- Edit existing nodes
- Delete nodes
- Test queries against your changes

All changes are isolated to this branch -- your production namespace is unaffected.

### 4. Commit and Create a PR

Once you're satisfied with your changes:

1. Click **Commit** to push your changes to the Git branch
2. Click **Create PR** to open a pull request against `main`
3. Enter a title and description for your PR

### 5. Review and Merge

Complete the deployment cycle:

1. Review the PR in GitHub (code review, CI checks, etc.)
2. Merge the PR to `main`
3. Your changes are automatically deployed to the production namespace
4. Delete the branch namespace when done

## CI/CD Integration

Automate deployments when changes are merged to your main branch.

### Example: GitHub Actions

Create `.github/workflows/deploy-dj.yml`:

```yaml
name: Deploy DJ Definitions

on:
  push:
    branches: [main]
    paths:
      - 'nodes/**'

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Install DJ Client
        run: pip install datajunction

      - name: Deploy to DJ
        env:
          DJ_URL: ${{ secrets.DJ_URL }}
          DJ_TOKEN: ${{ secrets.DJ_TOKEN }}
        run: |
          dj push ./nodes --namespace demo.metrics.main
```

## Best Practices

1. Use git-only for production to prevent accidental UI changes to production namespaces

2. Create branch namespaces for changes rather than editing production directly

3. Use the same `git_path` (e.g., `nodes/`) across namespaces pointing to the same repo

4. Leverage PR reviews to catch issues before they reach production

5. Automate deployments with CI/CD to ensure consistent, repeatable deployments

## Python Client

All Git operations can also be performed programmatically using the Python client.

### Setup

```python
from datajunction import DJBuilder

dj = DJBuilder("http://localhost:8000")
```

### Configure a Git Root Namespace

Initialize a namespace as a Git root to enable branch creation:

```python
dj.init_git_root(
    namespace="demo.metrics",
    github_repo_path="myorg/dj-definitions",
    default_branch="main",
    git_path="nodes/",      # optional: subdirectory for node definitions
    git_only=True,          # optional: block UI edits, require changes via Git
)
```

### Create and Manage Branches

Create a branch namespace for development:

```python
# Create a new branch (creates both Git branch and DJ namespace)
branch_ns = dj.create_branch("demo.metrics", "add-revenue-metrics")
# Returns namespace: demo.metrics.add_revenue_metrics

# List all branches under a root namespace
branches = dj.list_branches("demo.metrics")
for branch in branches:
    print(f"{branch.namespace} -> {branch.git_branch}")

# Delete a branch (removes both DJ namespace and Git branch)
dj.delete_branch("demo.metrics", "add-revenue-metrics")

# Delete only the DJ namespace, keep the Git branch
dj.delete_branch("demo.metrics", "add-revenue-metrics", delete_git_branch=False)
```

### Inspect and Clear Git Config

```python
# Get the current Git configuration for a namespace
config = dj.get_git_config("demo.metrics")
print(f"Repo: {config.github_repo_path}")
print(f"Branch: {config.default_branch}")
print(f"Path: {config.git_path}")
print(f"Git-only: {config.git_only}")

# Remove Git configuration from a namespace
dj.clear_git_config("demo.metrics")
```

## CLI

The same Git operations are available via the DJ command-line tool.

### Configure a Git Root Namespace

```bash
# Initialize a namespace as a git root
dj git init demo.metrics \
    --repo myorg/dj-definitions \
    --default-branch main \
    --git-path nodes/ \
    --git-only

# View git configuration
dj git show demo.metrics

# Clear git configuration
dj git clear demo.metrics
```

### Create and Manage Branches

```bash
# Create a branch namespace
dj branch create demo.metrics add-revenue-metrics

# List all branches under a root namespace
dj branch list demo.metrics

# Delete a branch (removes both DJ namespace and Git branch)
dj branch delete demo.metrics add-revenue-metrics

# Delete only the DJ namespace, keep the Git branch
dj branch delete demo.metrics add-revenue-metrics --keep-git-branch
```

### JSON Output

Both `dj git show` and `dj branch list` support JSON output:

```bash
dj git show demo.metrics --format json
dj branch list demo.metrics --format json
```

## Server Setup

This section covers prerequisites for administrators setting up Git integration.

### Prerequisites

1. **GitHub Repository**: A repository to store your YAML node definitions
2. **GitHub Token**: A personal access token or GitHub App token with repo access, configured in your DJ server
3. **YAML Project Structure**: Node definitions in YAML format (see [YAML Projects](../yaml))
