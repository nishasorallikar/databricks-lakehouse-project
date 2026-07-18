# 🌟 Databricks Lakehouse Medallion Pipeline

[![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)](https://databricks.com/)
[![Apache Spark](https://img.shields.io/badge/Apache_Spark-E25A28?style=for-the-badge&logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-00ADF2?style=for-the-badge&logo=deltalake&logoColor=white)](https://delta.io/)
[![Python](https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)

An end-to-end ELT (Extract, Load, Transform) data engineering project built on the **Databricks Lakehouse Platform**. This pipeline ingests transactional and master data from multiple source systems (**CRM** and **ERP**) and processes it through a standard three-layer **Medallion Architecture** (Bronze ➡️ Silver ➡️ Gold) to build a star-schema analytical model using PySpark, SQL, and Delta Lake.

---

## 🏗️ Architecture Overview

The pipeline organizes data into three distinct processing layers to guarantee data quality, lineage, and performance:

![Databricks Lakehouse Medallion Architecture](docs/images/architecture.jpg)


1. **Bronze Layer (Raw Ingestion)**: Ingests raw source CSV files directly from Databricks Unity Catalog Volumes into Delta tables in the `bronze` schema without modifying the schema or values.
2. **Silver Layer (Cleanse & Standardize)**: Cleans, filters, casts, and normalizes data. Handles missing values, trims whitespace, standardizes casing, and sets proper data types.
3. **Gold Layer (Analytical Star Schema)**: Enriches and aggregates data to create a reporting-ready dimensional model consisting of Dimension and Fact tables, ready for BI tools like Power BI or Tableau.

---

## 📂 Repository Directory Structure

```directory
.
├── Bronze/
│   ├── Bronze_layer_basic.ipynb      # Step-by-step raw CSV file load to Bronze Delta tables
│   └── Bronze_layer_improved.ipynb   # Configuration-driven dynamic ingestion pipeline
├── Silver/
│   ├── crm/
│   │   ├── silver_crm_cust_info.ipynb      # Cleanses and transforms CRM customer data
│   │   ├── silver_crm_prd_info.ipynb       # Standardizes CRM product master records
│   │   └── silver_crm_sales_details.ipynb  # Processes CRM sales order transactions
│   ├── erp/
│   │   ├── silver_erp_cust_az12.ipynb      # Processes ERP customer master data
│   │   ├── silver_erp_loc_a101.ipynb       # Standardizes customer location records
│   │   └── silver_erp_px_cat_g1v2.ipynb    # Processes product category master lists
│   └── silver_orchestration.ipynb          # Orchestrates all Silver notebook executions
├── Gold/
│   ├── gold_dim_customers.ipynb      # Merges CRM & ERP customers to create dim_customers
│   ├── gold_dim_products.ipynb       # Joins CRM products with ERP categories for dim_products
│   ├── gold_fact_sales.ipynb         # Builds transactional fact table (fact_sales)
│   └── gold_orchestration.ipynb      # Orchestrates all Gold notebook executions
└── README.md                         # Project documentation
```

---

## 🔧 Medallion Layer Pipeline Details

### 1. Bronze Layer (Ingestion)
* **Ingestion Source**: Files are uploaded to UC Volumes.
  * CRM path: `/Volumes/workspace/bronze/source_systems/source_crm/`
  * ERP path: `/Volumes/workspace/bronze/source_systems/source_erp/`
* **Execution Options**:
  * **Basic Approach** (`Bronze/Bronze_layer_basic.ipynb`): Sequential manual reading and table-saving logic.
  * **Improved Approach** (`Bronze/Bronze_layer_improved.ipynb`): Dynamic metadata-driven pipeline that loops through a configuration list mapping sources, file paths, and target tables.
* **Output**: Delta tables written to `workspace.bronze.*`.

### 2. Silver Layer (Standardization)
The Silver layer cleanses and normalizes the data:
* **String Cleaning**: Trims all leading and trailing whitespace on text fields.
* **Format Correction**: Standardizes dates, IDs, and null representations (e.g., mapping `'n/a'` to default values).
* **Source Cleansing**:
  * `crm_customers`: Resolves string trims and outputs to `silver.crm_customers`.
  * `crm_products`: Cleans product names and handles start/end dates.
  * `crm_sales`: Extracts dates, quantifies figures, and normalizes column formats.
  * `erp_customers`: Formats customer birthdates and matches codes.
  * `erp_customer_location`: Standardizes locations and country columns.
  * `erp_product_category`: Standardizes product hierarchy and subcategory mapping.
* **Orchestration**: Managed by `Silver/silver_orchestration.ipynb` using `dbutils.notebook.run`.

### 3. Gold Layer (Dimensional Modeling)
Models the cleansed datasets into an analytical star schema:
* **`dim_customers`**: Combines CRM customer info with ERP customer codes and locations to output customer keys, demographics, and geographics.
* **`dim_products`**: Integrates CRM product specifications with ERP category catalogs, providing surrogate product keys.
* **`fact_sales`**: Joins CRM sales orders with the surrogate dimension keys (`customer_key`, `product_key`) and extracts key transaction metrics (quantity, unit price, total sales amount).
* **Orchestration**: Managed by `Gold/gold_orchestration.ipynb`.

---

## 🚀 Setting Up & Executing the Pipeline

### Prerequisites
1. **Databricks Workspace**: Access to a Databricks environment.
2. **Unity Catalog**: A Unity Catalog enabled metastore with a catalog named `workspace` (or modify catalog references in notebooks as needed).
3. **Database Schemas**: Create schemas for the medallion layers:
   ```sql
   CREATE SCHEMA IF NOT EXISTS workspace.bronze;
   CREATE SCHEMA IF NOT EXISTS workspace.silver;
   CREATE SCHEMA IF NOT EXISTS workspace.gold;
   ```
4. **Unity Catalog Volumes**: Create volumes for raw file staging:
   ```sql
   CREATE VOLUME workspace.bronze.source_systems;
   ```
   *Within the volume, ensure subdirectories `source_crm/` and `source_erp/` contain the raw CSV files (e.g. `cust_info.csv`, `CUST_AZ12.csv`, etc.).*

### Execution Order
Run the layers sequentially or schedule them as a Databricks Job workflow:
1. **Run Ingestion**: Run `Bronze/Bronze_layer_improved.ipynb` to ingest CSV files into Bronze tables.
2. **Orchestrate Silver**: Run `Silver/silver_orchestration.ipynb` to clean and write all Silver tables.
3. **Orchestrate Gold**: Run `Gold/gold_orchestration.ipynb` to construct the Dimensional Model.

---

## 📊 Analytics Schema Representation

The dimensional model generated in the Gold layer is structured for optimized analytical querying:

```
                  +-----------------------+
                  |     dim_customers     |
                  +-----------------------+
                  | customer_key (PK)     |<-----+
                  | customer_id           |      |
                  | customer_number       |      |
                  | first_name            |      |
                  | last_name             |      |
                  | country               |      |
                  | marital_status        |      |
                  | gender                |      |
                  | birthdate             |      |
                  | create_date           |      |
                  +-----------------------+      |
                                                 |
                                                 |
                  +-----------------------+      |
                  |      fact_sales       |      |
                  +-----------------------+      |
                  | order_number          |      |
                  | customer_key (FK)     |------+
                  | product_key (FK)      |------+
                  | order_date            |      |
                  | ship_date             |      |
                  | due_date              |      |
                  | quantity              |      |
                  | price                 |      |
                  | sales_amount          |      |
                  +-----------------------+      |
                                                 |
                                                 |
                  +-----------------------+      |
                  |     dim_products      |      |
                  +-----------------------+      |
                  | product_key (PK)      |<-----+
                  | product_id            |
                  | product_number        |
                  | product_name          |
                  | category_id           |
                  | category              |
                  | subcategory           |
                  | maintenance_flag      |
                  | product_line          |
                  | start_date            |
                  +-----------------------+
```

---

## 🛠️ Built With
* [Databricks Workflows](https://docs.databricks.com/workflows/index.html) - Pipeline scheduling & orchestration.
* [Delta Lake](https://delta.io/) - Acid Transactions, Time Travel, Schema Enforcement.
* [Unity Catalog](https://www.databricks.com/product/unity-catalog) - Data governance and volume management.
* [PySpark SQL](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/index.html) - Data transformation and processing engine.
