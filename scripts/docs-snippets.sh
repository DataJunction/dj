# Creating a namespace
printf "\n"
curl -X POST http://localhost:8000/namespaces/default/

# Creating a catalog
printf "\n"
curl -X POST http://localhost:8000/catalogs/ \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
  "name": "warehouse"
}'

# Creating an engine
printf "\n"
curl -X 'POST' \
  'http://localhost:8000/engines/' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
  "name": "duckdb",
  "version": "0.7.1",
  "dialect": "spark"
}'

# Attaching an engine to a catalog
printf "\n"
curl -X 'POST' \
  'http://localhost:8000/catalogs/warehouse/engines/' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '[{
  "name": "duckdb",
  "version": "0.7.1"
}]'

# Creating a source node
printf "\n"
curl -X POST http://localhost:8000/nodes/source/ \
-H 'Content-Type: application/json' \
-d '{
    "name": "default.repair_orders",
    "description": "Repair orders",
    "mode": "published",
    "catalog": "warehouse",
    "schema_": "roads",
    "table": "repair_orders",
    "columns": [
        {"name": "repair_order_id", "type": "int"},
        {"name": "municipality_id", "type": "string"},
        {"name": "hard_hat_id", "type": "int"},
        {"name": "order_date", "type": "timestamp"},
        {"name": "required_date", "type": "timestamp"},
        {"name": "dispatched_date", "type": "timestamp"},
        {"name": "dispatcher_id", "type": "int"}
    ]
}'

# Creating a transform node
printf "\n"
curl -X POST http://localhost:8000/nodes/transform/ \
-H 'Content-Type: application/json' \
-d '{
    "name": "default.repair_orders_w_dispatchers",
    "description": "Repair orders that have a dispatcher",
    "mode": "published",
    "query": "SELECT repair_order_id, municipality_id, hard_hat_id, dispatcher_id FROM default.repair_orders WHERE dispatcher_id IS NOT NULL"
}'

# Creating a dimension node (part 1, creating a source first)
printf "\n"
curl -X 'POST' \
  'http://localhost:8000/nodes/source/' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/json' \
  -d '{
        "columns": [
            {"name": "dispatcher_id", "type": "int"},
            {"name": "company_name", "type": "string"},
            {"name": "phone", "type": "string"}
        ],
        "description": "Contact list for dispatchers",
        "mode": "published",
        "name": "default.dispatchers",
        "catalog": "warehouse",
        "schema_": "roads",
        "table": "dispatchers"
  }'

# Creating a dimension node (part 2, creating the dimension node)
printf "\n"
curl -X POST http://localhost:8000/nodes/dimension/ \
-H 'Content-Type: application/json' \
-d '{
    "name": "default.all_dispatchers",
    "description": "All dispatchers",
    "mode": "published",
    "query": "SELECT dispatcher_id, company_name, phone FROM default.dispatchers",
    "primary_key": ["dispatcher_id"]
}'

# Creating a dimension node (part 3, linking a dimension to a column)
printf "\n"
curl -X 'POST' \
  'http://localhost:8000/nodes/default.repair_orders/columns/dispatcher_id/?dimension=default.all_dispatchers&dimension_column=dispatcher_id' \
  -H 'accept: application/json'

# Creating a metric node
printf "\n"
curl -X POST http://localhost:8000/nodes/metric/ \
-H 'Content-Type: application/json' \
-d '{
    "name": "default.num_repair_orders",
    "description": "Number of repair orders",
    "mode": "published",
    "query": "SELECT count(repair_order_id) as num_repair_orders FROM default.repair_orders"
}'

# Creating a cube
printf "\n"
curl -X POST http://localhost:8000/nodes/cube/ \
-H 'Content-Type: application/json' \
-d '{
    "name": "default.repairs_cube",
    "mode": "published",
    "display_name": "Repairs for each company",
    "description": "Cube of the number of repair orders grouped by dispatcher companies",
    "metrics": [
        "default.num_repair_orders"
    ],
    "dimensions": [
        "default.all_dispatchers.company_name"
    ],
    "filters": ["default.all_dispatchers.company_name IS NOT NULL"],
}'

# Get SQL for metrics
printf "\n"
curl -X 'GET' \
  'http://localhost:8000/sql/?metrics=default.num_repair_orders&dimensions=default.all_dispatchers.company_name&filters=default.all_dispatchers.company_name%20IS%20NOT%20NULL' \
  -H 'accept: application/json'

# Get data for metrics
printf "\n"
curl -X 'GET' \
  'http://localhost:8000/data/?metrics=default.num_repair_orders&dimensions=default.all_dispatchers.company_name&filters=default.all_dispatchers.company_name%20IS%20NOT%20NULL' \
  -H 'accept: application/json'

printf "\n"
