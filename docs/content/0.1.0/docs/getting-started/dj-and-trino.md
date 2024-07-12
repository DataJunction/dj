---
title: "DataJunction + Trino Quickstart"
draft: false
images: []
menu:
  docs:
    parent: ""
weight: 70
toc: true
---

This tutorial will guide you through the process of setting up DataJunction, a powerful metrics platform, to work with
[Trino](https://trino.io/download) as the query compute engine. By the end of this tutorial, you'll have a functional
DataJunction instance layered on top of a local dockerized Trino deployment, and you'll be able to register tables,
define dimensions, create metrics, and more.

## Prerequisites

- Docker installed on your system.
- Basic knowledge of SQL and REST APIs.

### Clone the DataJunction Repository

First, clone the DataJunction GitHub repository to your local machine:

```bash
git clone https://github.com/DataJunction/dj.git
cd dj
```

### Start the Docker Environment

Use Docker Compose to start the environment, including the Trino container:

```bash
docker compose --profile demo --profile trino up
```

{{< alert icon="❗" >}}
The `--profile demo` flag is required to launch the [DataJunction query qervice](../../deploying-dj/query-service/)
container and the `--profile trino` flag is required to launch the container running the
[official Trino docker image](https://hub.docker.com/r/trinodb/trino).
{{< /alert >}}

### Access the DataJunction UI

Once the containers are running, navigate to [http://localhost:3000](http://localhost:3000) in your web browser to
access the DataJunction UI.

Create a new user using basic auth by clicking the `Sign Up` link.

![Login](/images/login-screenshot.png)

Create a user using any email, username, and password (don't worry, all of this data will only be stored in
the local postgres instance). In the screenshot below, the **email**, **username**, and **password** were set to
`dj@datajunction.io`, `datajunction`, and `datajunction`respectively.

![Signup](/images/signup-screenshot.png)

After clicking the sign up button, you'll be automatically logged in and will be sent to the Explore page.

### Verify Trino Tables

Before we start modeling in the DJ UI, let's verify that the tpch tables are available in the Trino instance.
Run the following command to open the Trino CLI:

```bash
docker exec -it dj-trino trino --catalog tpch --schema sf1
```

In the Trino CLI, run the following query to list the tables in the `sf1` schema:

```sql
SHOW TABLES;
```

| tables      |
|-------------|
| customer    |
| lineitem    |
| nation      |
| orders      |
| part        |
| partsupp    |
| region      |
| supplier    |

### Understand the DataJunction Query Service Configuration

The file `./datajunction-query/config.djqs.yml` contains the configuration for the Trino engine and the `tpch` catalog.
It contains the necessary settings for the query service to connect to the Trino deployment.
```yaml
engines:
  - name: trino
    version: 451
    type: sqlalchemy
    uri: trino://trino-coordinator:8080/tpch/sf1
    extra_params:
      http_scheme: http
      user: admin
catalogs:
  - name: tpch
    engines:
      - trino
```

This configuration defines an engine implementation named `trino` (the name can be anything) that uses the `sqlalchemy`
type engine which supports any URI and passes in `extra_params` as connection arguments. In the demo query service
container, the [trino python client](https://github.com/trinodb/trino-python-client) is installed which includes the
sqlalchemy engine implementation for Trino.

The `tpch` catalog is then defined and the `trino` engine is attached to it.

### Register Tables as Source Nodes

In the DataJunction UI, you can start registering tables as source nodes:

1. Go to the DataJunction UI at [http://localhost:3000](http://localhost:3000)
2. Hover over the `+ Add Node` dropdown and select `Register Table`.
3. Input each of the tables using `tpch` as the catalog and `sf1` as the schema. They will be automatically registered
in the `source` namespace.

Navigate to the `source->tpch->sf1` namespace in the DataJunction UI. You should see all of the tables you just registered.

![source.tpch.sf1](/images/tpch-explore-view.png)

### Define Dimension Nodes

Next, define the dimension nodes based on the TPCH schema. Although dimensions can include arbitrary SQL, for
simplicity, we will select all columns from the source nodes:

1. Go to the Explore page.
2. Next to "Namespaces," click the pencil icon and create a new namespace called `tpch_dimensions`.
3. Hover over "+ Add Node" and select "Dimension".
4. For namespace, choose `tpch_dimensions`. Start by creating the `customer` dimension by selecting all columns from the `source.tpch.sf1.customer` source node.

Repeat this process for the other dimension tables.

### Define Dimension Links

Now, define the relationships between dimensions, referred to as dimension linking:

1. Go to the Columns tab for each source node.
2. For example, in the `source.tpch.sf1.orders` source node, go to the Columns tab.
3. For the `custkey` column, click the pencil under Linked Dimension and select `tpch_dimensions.customer` from the dropdown.

Continue linking dimensions for the relevant columns. Refer to the provided ERD diagram for guidance.

![source.tpch.sf1](/images/tpch-erd.png)

### Create a Metric

Create a simple metric to count orders:

1. Go to the Explore page and create a namespace called `tpch_metrics`.
2. Hover over "+ Add Node" and select "Metric".
3. Create a metric called `num_orders` with the upstream node `source.tpch.sf1.orders` and the expression `COUNT(orderkey)`.

![tpch_metrics.num_orders](/images/tpch_metrics.num_orders.png)

You can also validate the metric directly in the UI to make sure that DataJunction can generate a proper query to
retrieve the metric data.

1. Go to the metric page and click the Validate tab.
2. Click Run Query to confirm that the metric retrieves data from the compute engine. You should see a single row with a number in the query result.

In the Validate tab, use the "Group By" dropdown to group the metric by discovered dimension primary keys, such as
Customer, Nation, and Region dimensions.

### Retrieving Data from the REST API

The REST API is one of the most common mechanisms to retrieve SQL or data for a combination of metrics and dimensions.
To try out the REST API, navigate to the Swagger UI at [http://localhost:8000/docs](http://localhost:8000/docs).

1. Go to the `/metrics/{name}` endpoint and request the `tpch_metrics.num_orders` metric. Review the available dimension attributes.
2. Use the `/data` endpoint to get data for the number of orders per customer, displaying the market segment attribute from the customer dimension. Add `tpch_metrics.num_orders` as a metric and `tpch_dimensions.customer.mktsegment` as a dimension.
3. If you only want to retrieve the SQL query without actually executing it against the compute engine, you can use the `/sql` endpoint which has a request specification almost identical to `/data`.

After running the query by calling the `/data` endpoint, you should see the results for the number of orders grouped
by each market segment.
