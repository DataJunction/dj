---
weight: 35
title: Query Service
---

The query service is responsible for the execution of data retrieval queries against the underlying 
data sources. While it doesn't generate the queries themselves, it ensures that the executed queries 
are efficient, secure, and adhere to the necessary protocols and standards.

Key Features:
* **Query Execution:** Executes SQL queries provided by the primary DJ backend.
* **Query Tracking:** Tracks the results of each query for debugging and optimization purposes.
* **Caching:** Maintains a cache of frequently accessed data to reduce the load on the database and speed up query execution.
* **Security Layer Integration:** Ensures that all data requests are compliant with security and data privacy protocols.
