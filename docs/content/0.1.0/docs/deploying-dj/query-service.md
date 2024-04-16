---
weight: 35
title: Query Service
mermaid: true
---

{{< mermaid class="bg-light text-center" >}}
flowchart LR
	classDef coreAPI stroke:#000,stroke-width:2px,color:#000,font-size:22px
    classDef UI stroke:#000,stroke-width:2px,color:#000,font-size:22px
    classDef db stroke:#000,stroke-width:2px,color:#000,font-size:22px
    classDef reflection stroke:#000,stroke-width:2px,color:#000,font-size:22px
    classDef query fill:#ffec99,stroke:#000,stroke-width:2px,color:#000,font-size:22px
    classDef clients stroke:#000,stroke-width:2px,color:#000,font-size:22px

    subgraph DataLayer["Data Layer"]
        direction TB
        DataWarehouse["Data Warehouse"]
        Scheduler["Scheduler"]
    end

    subgraph "DataJunction"
        direction TB
    
        CoreAPI("<br>&nbsp;&nbsp;Core API&nbsp;&nbsp;<br><br>"):::coreAPI
        MetadataDB["Metadata DB<br>(Postgres)"]:::db
        QueryService["<br>&nbsp;&nbsp;Query&nbsp;&nbsp;<br>&nbsp;&nbsp;Service&nbsp;&nbsp;<br><br>"]:::query
        ReflectionService["Reflection<br>Service"]:::reflection
        
        subgraph Clients["Client Libraries"]
            PythonClient["Python Client"]:::clients
            JSClient["JS Client"]:::clients
        end

        UI("<br>&nbsp;&nbsp;DJ UI&nbsp;&nbsp;<br><br>"):::UI
    end

    subgraph "Presentation Layer"
        direction TB
        JupyterNotebooks["Jupyter Notebooks"]
        Streamlit["Streamlit"]
        ReactApps["React Apps"]
        Tableau["Tableau"]
    end

    CoreAPI <--> MetadataDB
    CoreAPI <--> UI

    CoreAPI --> PythonClient["Python Client"]
    CoreAPI --> JSClient["JS Client"]

    DataWarehouse --> QueryService
    Scheduler --> QueryService
    ReflectionService --> CoreAPI
    QueryService <--> CoreAPI
    DataWarehouse --> ReflectionService

    PythonClient --> JupyterNotebooks["Jupyter Notebooks"]
    PythonClient --> Streamlit["Streamlit"]
    JSClient --> ReactApps["React Apps"]
{{< /mermaid >}}

The query service is responsible for the execution of data retrieval queries against the underlying 
data sources. While it doesn't generate the queries themselves, it ensures that the executed queries 
are efficient, secure, and adhere to the necessary protocols and standards.

Key Features:
* **Query Execution:** Executes SQL queries provided by the primary DJ backend.
* **Query Tracking:** Tracks the results of each query for debugging and optimization purposes.
* **Caching:** Maintains a cache of frequently accessed data to reduce the load on the database and speed up query execution.
* **Security Layer Integration:** Ensures that all data requests are compliant with security and data privacy protocols.
