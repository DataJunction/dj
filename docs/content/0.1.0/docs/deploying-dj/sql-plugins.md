---
weight: 50
title: SQL Plugins
---

DataJunction has a flexible SQL transpilation plugin system that enables you to add support for transpiling SQL queries across any SQL dialects. By default, support is enabled for Spark SQL, Trino, and Druid. You can also add custom transpilation plugins by following the instructions below.

**1. Create the Plugin Class**

Subclass `SQLTranspilationPlugin`, and register it with the dialect(s) it supports using the @dialect_plugin decorator:

```python
@dialect_plugin("customdb")
class CustomDBTranspilationPlugin(SQLTranspilationPlugin):
    package_name: str = "customdb-sql"

    def transpile_sql(
        self,
        query: str,
        *,
        input_dialect: Optional[Dialect] = None,
        output_dialect: Optional[Dialect] = None,
    ) -> str:
        import customdb_sql  # Ensure the library is installed

        # Ex: use the custom library’s transpilation interface
        return customdb_sql.transpile(query, source=input_dialect.name, target=output_dialect.name)
```

**2. Update Configuration**

Modify your settings to activate the new plugin:
```toml
sql_transpilation_plugins = ["default", "sqlglot", "customdb-sql"]
```

**3. Include Any Required Dependencies**

Make sure the transpilation library for your dialect (e.g., customdb-sql) is listed in your requirements file and installed in the deployment setup.
