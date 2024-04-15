---
weight: 40
title: Reflection Service
mermaid: true
---

{{< mermaid class="bg-light text-center" >}}
flowchart LR
	classDef coreAPI stroke:#000,stroke-width:2px,color:#000,font-size:22px
    classDef UI stroke:#000,stroke-width:2px,color:#000,font-size:22px
    classDef db stroke:#000,stroke-width:2px,color:#000,font-size:22px
    classDef reflection fill:#ffec99,stroke:#000,stroke-width:2px,color:#000,font-size:22px
    classDef query stroke:#000,stroke-width:2px,color:#000,font-size:22px
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
        QueryService["Query<br>Service"]:::query
        ReflectionService["<br>&nbsp;&nbsp;Reflection&nbsp;&nbsp;<br>Service&nbsp;<br><br>"]:::reflection
        
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

The reflection service polls the DJ core service for all nodes with associated tables, whether source
tables or materialized tables. For each node, it refreshes the node's schema based on the associated
table's schema that it retrieves from the query service. It also retrieves the available partitions and
the valid through timestamp of these tables and reflects them accordingly to DJ core.

This service uses a celery beat scheduler, with a configurable polling interval that defaults to once per
hour and async tasks for each node's reflection.
