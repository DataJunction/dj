---
weight: 3
title: "Git Integration"
---

DJ namespaces can be connected to GitHub repositories, enabling version-controlled workflows for your node definitions. This integration supports both strict git-as-source-of-truth setups and more flexible UI-editable workflows with git sync.

## Overview

Each namespace can be mapped to a specific GitHub repository, branch, and path. This mapping enables:

- **Version control** for all node definitions
- **Code review** through pull requests
- **CI/CD integration** for automated deployments
- **Branch-based development** for isolated changes

### Namespace-to-Git Mapping

A namespace's git configuration consists of:

| Field | Description | Example |
|-------|-------------|---------|
| Repository | GitHub repo in `owner/repo` format | `myorg/dj-definitions` |
| Branch | Git branch for this namespace | `main` |
| Path | Directory within the repo for YAML files | `nodes/` |
| Git-only | Whether UI editing is disabled | `true` / `false` |

Multiple namespaces can point to the same repository but different branches:

```
analytics.prod  → myorg/dj-definitions (main)     [git-only: true]
analytics.staging → myorg/dj-definitions (staging) [git-only: true]
analytics.dev   → myorg/dj-definitions (dev)      [git-only: false]
```

## Prerequisites

1. **GitHub Repository**: A repository to store your YAML node definitions
2. **GitHub Token**: A personal access token or GitHub App token with repo access, configured in your DJ server
3. **YAML Project Structure**: Node definitions in YAML format (see [YAML Projects](../yaml))

## Configuring Git for a Namespace

### Via the UI

1. Navigate to any namespace page
2. Click the **Configure Git** button in the namespace header
3. Fill in the configuration:
   - **Repository**: `owner/repo` format (e.g., `myorg/dj-definitions`)
   - **Branch**: The git branch for this namespace (e.g., `main`)
   - **Path**: Subdirectory for YAML files (e.g., `nodes/`)
   - **Git-only**: Enable to prevent UI edits (recommended for production)
4. Click **Save Settings**

### Via the API

```bash
curl -X PATCH "https://your-dj-server/namespaces/analytics.prod/git" \
  -H "Content-Type: application/json" \
  -d '{
    "github_repo_path": "myorg/dj-definitions",
    "git_branch": "main",
    "git_path": "nodes/",
    "git_only": true
  }'
```

## Git-only Mode

When **git-only** is enabled for a namespace:

- UI editing is disabled (no Edit buttons)
- Node creation/modification must go through git
- This is the recommended setting for production namespaces

### Workflow

1. **Edit YAML files** in your local repository
2. **Create a pull request** for review
3. **Merge** after approval
4. **CI/CD deploys** changes using `dj push`

This ensures all changes are reviewed and auditable.

## Editable Mode with Git Sync

When **git-only** is disabled, you can make changes through the UI and sync them to git:

1. **Make changes** in the DJ UI (create/edit nodes)
2. **Click "Sync to Git"** in the namespace header
3. Enter a commit message describing your changes
4. Changes are committed to the configured branch

This mode is useful for:
- Development and exploration
- Quick iterations before formalizing in git
- Teams transitioning to git-based workflows

### Creating Pull Requests from the UI

For branch namespaces (see below), you can create PRs directly from the UI:

1. Make your changes in the branch namespace
2. Click **Create PR** in the namespace header
3. Enter a title and description
4. The UI automatically syncs your changes and creates the PR

## Branch-based Development

For larger changes, you can create isolated branch namespaces:

### Creating a Branch

1. Navigate to your main namespace (e.g., `analytics.prod`)
2. Click **Create Branch**
3. Enter a branch name (e.g., `feature-new-metrics`)

This creates:
- A new git branch from the parent's branch
- A new namespace (e.g., `analytics.feature_new_metrics`)
- Copies of all nodes from the parent namespace

### Development Workflow

1. **Create a branch** from your production namespace
2. **Make changes** in the branch namespace (UI or YAML)
3. **Test** your changes in isolation
4. **Create a PR** to merge back to the parent branch
5. **Review and merge** the PR in GitHub
6. **Delete the branch** namespace when done

### Deleting a Branch

1. Navigate to the branch namespace
2. Click **Delete Branch**
3. Optionally delete the git branch as well

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
          dj push ./nodes --namespace analytics.prod
```

### Environment-specific Deployments

You can set up different workflows for different environments:

```yaml
# Deploy to staging on push to staging branch
on:
  push:
    branches: [staging]
# ...
run: dj push ./nodes --namespace analytics.staging

# Deploy to prod on push to main branch
on:
  push:
    branches: [main]
# ...
run: dj push ./nodes --namespace analytics.prod
```

## Best Practices

1. **Use git-only for production**: Prevent accidental UI changes to production namespaces

2. **Branch for features**: Create branch namespaces for significant changes rather than editing production directly

3. **Consistent paths**: Use the same `git_path` (e.g., `nodes/`) across namespaces pointing to the same repo

4. **Review before merge**: Leverage PR reviews to catch issues before they reach production

5. **Automate deployments**: Use CI/CD to ensure consistent, repeatable deployments

6. **Backup with `dj pull`**: Periodically export your namespaces to ensure you have local backups:
   ```bash
   dj pull analytics.prod ./backups/prod-$(date +%Y%m%d)
   ```
