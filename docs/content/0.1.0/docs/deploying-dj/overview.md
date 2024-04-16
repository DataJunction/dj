---
weight: 1
title: Overview
mermaid: true
---
## The Components of a DJ Deployment

A full DJ deployment consists of the following services:
* Core API
* UI
* Postgres DB (for storing metadata)
* [Query Service](../query-service)
* [Reflection Service](../reflection-service)
* Materialization Service (optional)

{{< mermaid class="bg-light text-center" >}}
flowchart LR
	classDef coreAPI fill:#ffc9c9,stroke:#000,stroke-width:2px,color:#000,font-size:22px
    classDef UI fill:#99e9f2,stroke:#000,stroke-width:2px,color:#000,font-size:22px
    classDef db fill:#b2f2bb,stroke:#000,stroke-width:2px,color:#000,font-size:22px
    classDef reflection fill:#d0bfff,stroke:#000,stroke-width:2px,color:#000,font-size:22px
    classDef query fill:#ffec99,stroke:#000,stroke-width:2px,color:#000,font-size:22px
    classDef clients fill:#e3fafc,stroke:#000,stroke-width:2px,color:#000,font-size:22px

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

