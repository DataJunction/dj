const { DJClient } = require("datajunction")

const dj = new DJClient("http://localhost:8000")

Promise.resolve().then(
    () => {
        return dj.namespaces.create("default").then(data => console.log(data))
    }
).then(
    () => {
        return dj.catalogs.create({"name": "warehouse"}).then(data => console.log(data))
    }
).then(
    () => {
        return dj.engines.create({
            name: "duckdb",
            version: "0.7.1",
            dialect: "spark",
        }).then(data => console.log(data))
    }
).then(
    () => {
        return dj.catalogs.addEngine("warehouse", "duckdb", "0.7.1").then(data => console.log(data))
    }
).then(
    () => {
        return dj.sources.create({
            name: "default.repair_orders",
            description: "Repair orders",
            mode: "published",
            catalog: "warehouse",
            schema_: "roads",
            table: "repair_orders",
            columns: [
                {name: "repair_order_id", type: "int"},
                {name: "municipality_id", type: "string"},
                {name: "hard_hat_id", type: "int"},
                {name: "order_date", type: "timestamp"},
                {name: "required_date", type: "timestamp"},
                {name: "dispatched_date", type: "timestamp"},
                {name: "dispatcher_id", type: "int"}
            ]
        }).then(data => console.log(data))
    }
).then(
    () => {
        return dj.transforms.create(
            {
                name: "default.repair_orders_w_dispatchers",
                mode: "published",
                description: "Repair orders that have a dispatcher",
                query: `
                    SELECT
                    repair_order_id,
                    municipality_id,
                    hard_hat_id,
                    dispatcher_id
                    FROM default.repair_orders
                    WHERE dispatcher_id IS NOT NULL
                `
            }
        ).then(data => console.log(data))
    }
).then(
    () => {
        return dj.sources.create(
            {
                name: "default.dispatchers",
                mode: "published",
                description: "Contact list for dispatchers",
                catalog: "warehouse",
                schema_: "roads",
                table: "dispatchers",
                columns: [
                    {name: "dispatcher_id", type: "int"},
                    {name: "company_name", type: "string"},
                    {name: "phone", type: "string"}
                ]
          }
        ).then(data => console.log(data))
    }
).then(
    () => {
        return dj.dimensions.create(
            {
                name: "default.all_dispatchers",
                mode: "published",
                description: "All dispatchers",
                query: `
                    SELECT
                    dispatcher_id,
                    company_name,
                    phone
                    FROM default.dispatchers
                `,
                primary_key: ["dispatcher_id"]
            }
        ).then(data => console.log(data))
    }
).then(
    () => {
        return dj.dimensions.link("default.repair_orders", "dispatcher_id", "default.all_dispatchers", "dispatcher_id").then(data => console.log(data))
    }
).then(
    () => {
        return dj.metrics.create(
            {
                name: "default.num_repair_orders",
                description: "Number of repair orders",
                mode: "published",
                query: `
                    SELECT
                    count(repair_order_id) as num_repair_orders
                    FROM default.repair_orders
                `
            }
        ).then(data => console.log(data))
    }
).then(
    () => {
        return dj.cubes.create(
            {
                name: "default.repairs_cube",
                mode: "published",
                display_name: "Repairs for each company",
                description: "Cube of the number of repair orders grouped by dispatcher companies",
                metrics: [
                    "default.num_repair_orders"
                ],
                dimensions: [
                    "default.all_dispatchers.company_name"
                ],
                filters: ["default.all_dispatchers.company_name IS NOT NULL"]
            }
        ).then(data => console.log(data))
    }
).then(
    () => {
        return dj.sql.get(
            metrics=["default.num_repair_orders"],
            dimensions=["default.all_dispatchers.company_name"],
            filters=["default.all_dispatchers.company_name IS NOT NULL"]
        ).then(data => console.log(data))
    }
).then(
    () => {
        return dj.data.get(
            metrics=["default.num_repair_orders"],
            dimensions=["default.all_dispatchers.company_name"],
            filters=["default.all_dispatchers.company_name IS NOT NULL"]
        ).then(data => console.log(data))
    }
)
