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

If you've already started developing DJ entities through the UI or a different client, you can export the existing entities to a YAML project to get started quickly. Note that this process only supports exporting a single namespace at a time.

1. **Export**: Use the DJ Python client to export your DJ entities to a YAML project. This snippet will export to a YAML project in your local directory `./example_project`:
```python
from datajunction import DJBuilder, Project
dj = DJBuilder()
Project.pull(
    client=dj,
    namespace="default",
    target_path="./example_project",
    ignore_existing_files=True,
)
project = Project.load("./example_project")
compiled_project = project.compile()
```

2. **Validation**: Once you've made changes to the YAML files, you can validate those changes with:
```
compiled_project.validate(client=dj)
```

3. **Deployment**: When satisfied, you can deploy the changes like this:
```
compiled_project.deploy(client=dj)
```

### From Scratch

You can also create a YAML project from scratch.

1. Create a `dj.yaml` file with project metadata. See additional details on [project metadata fields below](#project-metadata-fields). Example:
```
name: Roads Project
description: Roads example project
prefix: projects.roads
mode: published
tags:
  - name: deprecated
    description: This node is deprecated
    tag_type: Maintenance
build:
  priority:
    - roads.date
    - roads.repair_order_details
    - roads.contractors
    - roads.hard_hats
```
2. Create YAML files that represent each node. Use file infixes to define node types, like `foo.dimension.yaml` or `foo.transform.yaml`). Here is an example for the `roads.date_dim` node, in file `./roads/
```
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
display_name: Date
```
#### Project Metadata Fields

| Field | Required? | Description |
| ---- | ---- | ---- |
| `name` | Yes | Name of the YAML project |
| `description` | Yes | Description of the YAML project |
| `prefix` | Yes | This is set to a DJ namespace. Node names are derived from the directory and file structure. For example, for the prefix `projects.roads` and file `./baz/boom.source.yaml`, the node name becomes `projects.roads.baz.boom`. |
| `mode` | No | Whether the project is published or draft (defaults to published) |
| `tags` | No | Used to define any tags that are used by nodes in the project |
| `build.priority` | No | Used to control the ordering of node deployment |


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
| `query` | Yes | The SQL query for the node |
| `display_name` | No | The display name of the node |
| `description` | No | Description of the node |
| `tags` | No | A list of tags for this node |
| `primary_key` | No | A list of columns that make up the primary key of this node |
| `dimension_links` | No | A list of dimension links, if any. See [details](#dimension-link-yaml). |

##### Metric Node YAML
| Field | Required? | Description |
| ---- | ---- | ---- |
| `query` | Yes | The SQL query for the node |
| `display_name` | No | The display name of the node |
| `description` | No | Description of the node |
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

| Reference Link Fields | Required?  | Description |
| ---- | ---- | ---- | ---- |
| `type` | Yes  | Must be `reference` |
| `node_column` | Yes  | The column on this node that is being linked from |
| `dimension` | Yes | The dimension attribute being linked to |
| `role` | No | The role this dimension represents |
