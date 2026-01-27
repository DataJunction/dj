---
weight: 2
title: "YAML Projects"
---

DJ entities can be managed through YAML definitions. This is a versatile feature that enables change review and more holistic testing before deploying to production. Using source-controlled YAML definitions provide a more structured approach to development, allowing you to review and audit changes.

{{< alert icon="👉" >}}
You can anchor your project to a specific namespace in DJ, and use YAML files to define all nodes in that namespace. While not required, this approach promotes a cleaner and more organized setup.
{{< /alert >}}

## Setup Guide

### From Existing

If you've already started developing DJ entities through the UI or a different client, you can export the existing entities to a YAML project to get started quickly.

1. **Export**: Use the DJ CLI (installed when you install the DJ Python client) to export your DJ entities to a YAML project. This snippet will export the `default` namespace to a YAML project in the `./example_project` directory:
```sh
dj pull default ./example_project
```

2. **Validation**: Once you've made changes to the YAML files, you can validate those changes with:
```sh
dj push ./example_project --dryrun
```

3. **Deployment**: When satisfied, you can deploy the changes like this:
```sh
dj push ./example_project --namespace <optional namespace>
```

### From Scratch

You can also create a YAML project from scratch.

1. Create a `dj.yaml` file with project metadata. See additional details on [project metadata fields below](#project-metadata-fields). Example:
```
namespace: projects.roads
tags:
  - name: deprecated
    description: This node is deprecated
    tag_type: Maintenance
```
2. Create YAML files that represent each node. Here is an example for the `roads.date_dim` node, in file `./roads/date_dim.yaml`:
```
name: ${prefix}date_dim
display_name: Date
description: Date dimension
query: |
  SELECT
    dateint,
    month,
    year,
    day
  FROM ${prefix}roads.date
primary_key:
  - dateint
```
#### Project Metadata Fields

| Field | Required? | Description |
| ---- | ---- | ---- |
| `namespace` | Yes | The DJ namespace for this YAML project |
| `tags` | No | Used to define any tags that are used by nodes in the project |


#### Node YAML Fields Overview

{{< alert icon="👉" >}}
The **node name is derived** from the directory structure and file name. For example, for the prefix `projects.roads` and file `./baz/boom.source.yaml`, the node name becomes `projects.roads.baz.boom`.
{{< /alert >}}

##### Source Node YAML
| Field | Required? | Description |
| ---- | ---- | ---- |
| `table` | Yes | The physical table of this source node |
| `columns` | No | The columns of this source node, will be derived from the table if not provided  |
| `display_name` | No | The display name of the source node |
| `description` | No | Description of the node |
| `tags` | No | A list of tags for this node |
| `primary_key` | No | A list of columns that make up the primary key of this node |
| `dimension_links` | No | A list of dimension links, if any. See [details](#dimension-link-yaml). |

##### Transform / Dimension Node YAML
| Field | Required? | Description |
| ---- | ---- | ---- |
| `display_name` | No | The display name of the node |
| `description` | No | Description of the node |
| `query` | Yes | The SQL query for the node |
| `columns` | No | Optional column-level settings (like `attributes` or `partition`) |
| `tags` | No | A list of tags for this node |
| `primary_key` | No | A list of columns that make up the primary key of this node |
| `dimension_links` | No | A list of dimension links, if any. See [details](#dimension-link-yaml). |

##### Metric Node YAML
| Field | Required? | Description |
| ---- | ---- | ---- |
| `display_name` | No | The display name of the node |
| `description` | No | Description of the node |
| `query` | Yes | The SQL query for the node |
| `columns` | No | Optional column-level settings (like `attributes` or `partition`) |
| `tags` | No | A list of tags for this node |
| `required_dimensions` | No | A list of required dimensions for this metric |
| `direction` | No | Direction of this metric (one of `higher_is_better`, `lower_is_better`, or `neutral`) |
| `unit` | No | The unit of this metric |

##### Cube Node YAML
| Field | Required? | Description |
| ---- | ---- | ---- |
| `display_name` | No | The display name of the node |
| `description` | No | Description of the node |
| `metrics` | Yes | The metrics in the cube |
| `dimensions` | Yes | The dimensions in the cube |
| `columns` | No | Optional column-level settings (like `attributes` or `partition`) |
| `tags` | No | A list of tags for this node |

##### Dimension Link YAML

There are two types of dimension links, join links and reference links. The fields available for each of the two are slightly different. Here's an example of a node with both:
```
description: Hard hat dimension
display_name: Local Hard Hats
query: ...
primary_key: ...
dimension_links:
  - type: join
    node_column: state_id
    dimension_node: ${prefix}roads.us_state
    default_value: Unknown  # Optional: fallback for NULL values from LEFT JOIN
  - type: reference
    node_column: birth_date
    dimension: ${prefix}roads.date_dim.dateint
    role: birth_date
```

| Join Link Fields | Required?  | Description |
| ---- | ---- | ---- | ---- |
| `type` | Yes  | Must be `join` |
| `dimension_node` | Yes | The dimension node being linked to |
| `node_column` | No  | The column on this node that is being linked from |
| `join_on` | No | A custom join on SQL clause |
| `join_type` | No | The type of join (one of `left`, `right`, `inner`, `full`, `cross`). Defaults to `left`. |
| `role` | No | The role this dimension represents |
| `default_value` | No | A fallback value for NULL results from LEFT/RIGHT joins. When set, dimension columns are wrapped in `COALESCE(column, 'default_value')`. |

| Reference Link Fields | Required?  | Description |
| ---- | ---- | ---- | ---- |
| `type` | Yes  | Must be `reference` |
| `node_column` | Yes  | The column on this node that is being linked from |
| `dimension` | Yes | The dimension attribute being linked to |
| `role` | No | The role this dimension represents |

##### Columns YAML

The `columns` section can be included if additional column-level settings are needed on the node.

**Attributes**

Column-level attributes like `dimension` can be configured like this:

```
columns:
- name: is_clicked
  attributes:
  - dimension
```

**Display Name**

A column can be given a custom display name like this:

```
columns:
- name: is_clicked
  display_name: Clicked?
```

**Partitions**

Partition columns can be configured like this:

```
columns:
- name: utc_date
  partition:
    format: yyyyMMdd
    granularity: day
    type_: temporal
```

## YAML Deployment

### Deployment Orchestration

The DJ CLI's deployment command (`dj push`) leverages backend APIs designed for fast, atomic deployments with full tracking. This orchestration system handles the complexity of dependency management, status tracking, and transactional deployment while providing you with real-time feedback through the CLI.

#### Topological Sorting

The deployment system automatically analyzes dependencies between nodes and deploys them in the correct order. You do not need to manually specify deployment priority, as DJ will:

- Detect dependencies between nodes through SQL parsing to build a dependency graph
- Ensure dependent nodes are deployed after their dependencies

#### Real-Time Status Tracking

Each deployment is assigned a unique UUID and tracked in real-time. The deployment process provides:

- Live progress updates in the terminal
- Detailed status for each node being deployed
- Rich formatted output showing deployment progress and errors

#### Deployment Phases

The deployment orchestrator executes deployments in distinct phases:

1. **Setup Phase**: Validates resources (tags, namespaces, catalogs)
2. **Node Deployment**: Deploys nodes in topologically sorted order, with each independent layer of nodes deployed atomically.
3. **Dimension Links**: Creates dimension links between nodes
4. **Cubes**: Deploys all cube nodes in a single transaction, after the dimensional graph is complete
5. **Cleanup**: Handles any nodes marked for deletion

### Supported CLI Commands

#### `dj push` - Streamlined Deployment

The `dj push` command provides a streamlined deployment experience:

```sh
# Push all YAML files in a directory
dj push ./my-project

# Override the namespace specified in dj.yaml
dj push ./my-project --namespace production.analytics

# Example with real-time output
dj push ./example_project --namespace my.namespace
```

#### `dj pull` - Export to YAML

Export existing nodes from a namespace to YAML files:

```sh
# Export all nodes from a namespace
dj pull default ./exported-nodes

# Export production namespace for backup
dj pull production.metrics ./backups/production-$(date +%Y%m%d)
```

### Deployment Workflow Examples

#### Complete Development Workflow

```sh
# 1. Export existing namespace to get started
dj pull production.analytics ./my-project

# 2. Make changes to YAML files
# ... edit files ...

# 3. Validate changes with dry run
dj deploy ./my-project --dryrun

# 4. Deploy to development namespace first
dj push ./my-project --namespace development.analytics

# 5. After testing, deploy to production
dj push ./my-project --namespace production.analytics
```

#### Real-Time Deployment Output

When you run `dj push`, you'll see output like this:

```
Pushing project from: ./my-project

┌─────────────────────────────────────────────────────────────┐
│                      Deployment Status                      │
├──────────────────────┬─────────────┬────────────────────────┤
│ UUID                 │ Status      │ Progress               │
├──────────────────────┼─────────────┼────────────────────────┤
│ abc123-def456-...    │ RUNNING     │ Deploying nodes...     │
└──────────────────────┴─────────────┴────────────────────────┘

Deployment finished: SUCCESS
```

### Error Handling and Troubleshooting

#### Common Deployment Issues

1. **Dependency Errors**: If nodes have circular dependencies or missing dependencies, the deployment will fail with clear error messages identifying the problematic nodes.

2. **Validation Failures**: SQL validation errors, invalid column references, or schema mismatches will be reported during the validation phase.

#### Best Practices

- **Test in Development**: Always deploy to a development namespace first and complete verification.
- **Use Dry Runs**: Validate changes with `--dryrun` before actual deployment
- **Backup Existing Namespaces**: Use `dj pull` to backup production namespaces before major changes
- **Monitor Dependencies**: Be aware of dependencies between nodes when making structural changes
- **Namespace Isolation**: Use separate namespaces for different environments (dev, staging, production)
