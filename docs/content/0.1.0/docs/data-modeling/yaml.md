# YAML

The DJ client enables managing DJ entities through YAML definitions, a versatile feature that enables change review and testing entities more holistically before deploying to production. Using source-controlled YAML files provide a more structured deployment approach, ensuring all changes are versioned, reviewed, and auditable.

{{< alert icon="👉" >}}
You can anchor your YAML project to a specific namespace in DJ, and use YAML files to define all nodes in that namespace. While not required, this approach promotes a cleaner and more organized setup.
{{< /alert >}}

## Setup Guide

### From Existing

1. **Export**: If you've already started developing DJ entities through the UI or a different client, you can export the entities in a namespace to a YAML project like this:
```
from datajunction import DJBuilder, Project
dj = DJBuilder()
Project.pull(client=dj, namespace="default", target_path="./example_project", ignore_existing_files=True)
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

1. Create a `dj.yaml` file with project metadata. Example:
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
**Fields Overview:**
   * `prefix`: This is set to a DJ namespace. Node names are derived from the directory and file structure. For example, for the prefix `projects.roads` and file `./baz/boom.source.yaml`, the node name becomes `projects.roads.baz.boom`.
   * `tags`: Define any tags that are used by nodes in the project
2. Create a YAML file that represents each node. Use file infixes to define node types, like `foo.dimension.yaml` or `foo.transform.yaml`). Here is an example for the `roads.date_dim` node, in file `./roads/
```
description: This is a source for information on outstanding and fulfilled repair orders
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
3. 