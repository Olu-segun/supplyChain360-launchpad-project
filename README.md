# SupplyChain360 Unified Data Platform
----
## 📌 Project Overview
SupplyChain360 is a fast‑growing retail distribution company in the United States. The business faces operational inefficiencies such as frequent stockouts, overstocked warehouses, and delivery delays. Data fragmentation across multiple systems (store location, warehouses, suppliers, shipments, sales, warehouse and product) makes it difficult for leadership to gain timely insights.

This project builds a Unified Supply Chain Data Platform to centralize operational data, improve analytics, and enable better decision‑making in inventory planning, supplier performance monitoring, shipment tracking, and demand forecasting.
### The platform enables:
- 📦 **Inventory optimization**
- 🚚 **Shipment tracking**
- 🏭 **Warehouse efficiency analysis**
- 🤝 **Supplier performance monitoring**
----
## 🧠 Business Problem
SupplyChain360 currently struggles with:
- 📉 Frequent stockouts of high-demand products
- 📦 Overstocking of low-demand inventory
- 🚚 Delayed shipments
- ⏱️ Lack of real-time insights
## 🗂️ Data Fragmentation
Operational data is scattered across multiple systems:
- 🗄️ S3 (CSV/JSON files)
- 🛢️ PostgreSQL transactional database
- 📊 Google Sheets
---
## 📊 Architecture Diagram

<img src="architecture_diagram.png" alt="Architecture Diagram" width="600">

---
## 📂 Project Folder Structure
- **Airflow** → Orchestration layer
- **dbt** → Data transformation layer
- **Ingestion Layer** → Data extraction (S3, Postgres, Google Sheets)
- **Snowflake SQL** → Warehouse setup scripts
- **Terraform** → Infrastructure as Code
- **GitHub Actions** → CI/CD automation
```
supplychain360/
│
├── .github/
│   └── workflows/
│       └── supplychain360_ci_cd.yml
│
├── terraform/
│   ├── main.tf
│   ├── variables.tf
│   └── backend.tf
│
├── airflow/
│   ├── dags/
│   │   ├── task.py
│   │   └── supplychain360_dag.py
│   └── plugins/
│       └── config/
│
├── ingestion_layer/
│   ├── s3_ingestion.py
│   ├── postgres_ingestion.py
│   └── sheets_ingestion.py
│
├── dbt/
│   ├── models/
│   │   ├── staging/
│   │   └── marts/
│   ├── seeds/
│   ├── tests/
│   ├── profiles.yml
│   └── dbt_project.yml
│
├── snowflake_setup_sql/
│   ├── 01_setup_database.sql
│   ├── 02_roles_and_permissions.sql
│   ├── 03_storage_integration.sql
│   └── 04_stage_and_tables.sql
│
├── utils/
│   └── credentials.py
│
├── docker-compose.yaml
├── Dockerfile
├── requirements.txt
├── README.md
└── architecture.png
```

----
## ⚙️ Tech Stack
| Layer            | Tool           |
| ---------------- | -------------- |
| Orchestration    | Apache Airflow |
| Storage          | AWS S3         |
| Data Warehouse   | Snowflake      |
| Transformation   | dbt            |
| Infrastructure   | Terraform      |
| Containerization | Docker         |
| CI/CD            | GitHub Actions |

## 📊 Data Sources
| Source              | Type          | Frequency                     |
| ------------------- | ------------- | ------------------------------|                 
| Product Catalog     | CSV (S3)      | Static dataset (Rarely change)|
| Store Locations     | Google Sheets | Static dataset (Rarely change)|
| Suppliers           | CSV (S3)      | Static dataset (Rarely change)|
| Warehouses          | CSV (S3)      | Static dataset (Rarely change)|
| Inventory Snapshots | CSV (S3)      | Generate Daily                |
| Shipment Logs       | JSON (S3)     | Generate Daily                |
| Sales Transactions  | PostgreSQL    | Generate Daily                |

## 🔄 Pipeline Workflow
Airflow orchestrates:
- Extract data from all sources
- Load raw data into S3 (Parquet format)
- Ingest data into Snowflake (RAW schema)
- Transform data using dbt
- Run data quality checks
- Build analytical models

## 🧱 Data Modeling
Layers:
- RAW → Unprocessed data
- STAGING → Cleaned & standardized
- MARTS → Business-ready models
Example Models:
- fact_sales
- dim_products
- dim_suppliers
- fact_shipments
## 🧹 Data Cleaning
- Standardized column names
- Removed duplicates
- Handled null values
- Enforced referential integrity
## 🔁 Pipeline Features
- Idempotent ingestion
- Incremental loading
- Retry mechanisms
- Failure alerts
- Partitioned data

## 🐳 Containerization
The entire pipeline is containerized using Docker.

`docker build -t supplychain360 .`

## ⚙️ CI/CD Pipeline
GitHub Actions handles:
- Code linting
- Formatting checks
- Docker image build
- Push to container registry

## ☁️ Infrastructure (Terraform)
- S3 buckets
- Snowflake resources
- IAM roles
- Remote state backend

## 🚀 How to Run
1. Clone repo

    `git clone https://github.com/Olu-segun/supplyChain360-launchpad-project`

2. Set environment variables

    `cp .env.example .env`

3. Start Airflow

    `docker-compose up`

4. Run dbt

    `cd dbt`

    `dbt build`

## 📈 Business Insights 
- Products causing most stockouts
- Suppliers with late deliveries
- Warehouse inefficiencies
- Demand trends by region

## 🎯 Outcome
This platform enables:
- Real-time decision-making
- Reduced stockouts
- Optimized inventory
- Improved delivery performance


