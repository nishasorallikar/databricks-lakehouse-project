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

### 🟫 1. Bronze Layer: Ingestion & Raw Staging

The entry point of the pipeline ingests external files from Unity Catalog (UC) Volumes. It acts as an append-only archive preserving the history of raw datasets.

> [!NOTE]
> **Raw Source Data Locations**
> * **📁 CRM System Volume Path:** `/Volumes/workspace/bronze/source_systems/source_crm/`
> * **📁 ERP System Volume Path:** `/Volumes/workspace/bronze/source_systems/source_erp/`

#### 🔄 Ingestion Execution Strategies

| Strategy | Notebook | Mechanism | Key Benefits |
| :--- | :--- | :--- | :--- |
| 📜 **Basic Approach** | 📓 [`Bronze_layer_basic.ipynb`](Bronze/Bronze_layer_basic.ipynb) | 📥 Explicit file-by-file loading via PySpark APIs | 🟢 Easy to debug and troubleshoot<br>🟢 Clean separation of individual scripts |
| ⚡ **Improved (Recommended)** | 📓 [`Bronze_layer_improved.ipynb`](Bronze/Bronze_layer_improved.ipynb) | ⚙️ Configuration-driven dynamic looping using metadata configs | 🚀 Highly scalable and dynamic<br>🛠️ Low maintenance<br>➕ Add new tables via list updates |

*All raw outputs are saved as Delta tables under `workspace.bronze.*` with no transformations applied.*

---

### ⬜ 2. Silver Layer: Cleanse & Standardize

This layer is responsible for data cleaning, type casting, schema reinforcement, and normalizing across CRM and ERP data sources.

#### ⚙️ Data Cleansing Principles
* 🧹 **String Trimming**: Automatic cleanup of leading/trailing whitespace on all string columns.
* 📅 **Date Standardization**: Normalization of varying date patterns (e.g. ERP birthdates) to standard SQL dates.
* 👥 **Null Value Mitigation**: Handling invalid input columns (e.g., mapping gender `'n/a'` values using backup source keys).

#### 📊 Source Transformation Catalog

| Source System | Raw Table (Bronze) | Standardized Table (Silver) | Transformations Applied | Notebook |
| :---: | :--- | :--- | :--- | :--- |
| **🏢 CRM** | 📥 `crm_cust_info` | 🧹 `crm_customers` | ✂️ Trims text fields<br>🚫 Drops duplicate entries<br>🧼 Normalizes values | 📓 [`silver_crm_cust_info.ipynb`](Silver/crm/silver_crm_cust_info.ipynb) |
| **🏢 CRM** | 📥 `crm_prd_info` | 🧹 `crm_products` | 🏷️ Extracts names<br>💳 Casts unit costs to double<br>⏳ Maps active product dates | 📓 [`silver_crm_prd_info.ipynb`](Silver/crm/silver_crm_prd_info.ipynb) |
| **🏢 CRM** | 📥 `crm_sales_details` | 🧹 `crm_sales` | 💵 Type casts currency fields<br>🔢 Validates quantity > 0<br>🧼 Standardizes formats | 📓 [`silver_crm_sales_details.ipynb`](Silver/crm/silver_crm_sales_details.ipynb) |
| **🏭 ERP** | 📥 `erp_cust_az12` | 🧹 `erp_customers` | 📅 Formats birthdates to standard YYYY-MM-DD<br>🆔 Aligns customer codes | 📓 [`silver_erp_cust_az12.ipynb`](Silver/erp/silver_erp_cust_az12.ipynb) |
| **🏭 ERP** | 📥 `erp_loc_a101` | 🧹 `erp_customer_location` | 📍 Standardizes spatial locations<br>🗺️ Normalizes country names | 📓 [`silver_erp_loc_a101.ipynb`](Silver/erp/silver_erp_loc_a101.ipynb) |
| **🏭 ERP** | 📥 `erp_px_cat_g1v2` | 🧹 `erp_product_category` | 🗂️ Standardizes product categories & subcategories | 📓 [`silver_erp_px_cat_g1v2.ipynb`](Silver/erp/silver_erp_px_cat_g1v2.ipynb) |

> [!TIP]
> **Orchestration Tool**: Use [`silver_orchestration.ipynb`](Silver/silver_orchestration.ipynb) to trigger all six notebooks sequentially using `dbutils.notebook.run`.

---

### 🟨 3. Gold Layer: Analytical Star Schema

Transforms standardized Silver tables into a business-level Dimensional Model optimized for analytics, reporting, and BI consumption.

#### 🌟 Star Schema Components

| Table Type | Table Name | Source Input Tables | Modeling Logic & Key Enhancements | Notebook |
| :--- | :--- | :--- | :--- | :--- |
| **💎 Dimension** | 👤 `dim_customers` | 📋 `silver.crm_customers`<br>📍 `silver.erp_customers`<br>🗺️ `silver.erp_customer_location` | 🔗 Joins CRM demographic data with ERP location details.<br>🔑 Generates standard surrogate keys (`customer_key`) using row numbers. | 📓 [`gold_dim_customers.ipynb`](Gold/gold_dim_customers.ipynb) |
| **💎 Dimension** | 📦 `dim_products` | 📋 `silver.crm_products`<br>🗂️ `silver.erp_product_category` | 🔗 Enriches CRM products with ERP categories/subcategories.<br>⏳ Filters out inactive/historical product revisions. | 📓 [`gold_dim_products.ipynb`](Gold/gold_dim_products.ipynb) |
| **📊 Fact** | 💰 `fact_sales` | 📋 `silver.crm_sales`<br>📦 `gold.dim_products`<br>👤 `gold.dim_customers` | 🔗 Joins transaction details with surrogate dimension keys (`customer_key`, `product_key`).<br>📈 Computes business metric metrics (gross sales amount). | 📓 [`gold_fact_sales.ipynb`](Gold/gold_fact_sales.ipynb) |

> [!TIP]
> **Orchestration Tool**: Run [`gold_orchestration.ipynb`](Gold/gold_orchestration.ipynb) to execute the dimension and fact notebooks sequentially in the correct dependency order.


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

Run the layers sequentially or schedule them as a unified Databricks Job workflow:

```mermaid
graph LR
    %% Theme styling definitions for execution steps
    classDef bronzeStep fill:#ffe0b2,stroke:#ff9800,stroke-width:2px,color:#e65100,font-weight:bold;
    classDef silverStep fill:#e0f7fa,stroke:#00bcd4,stroke-width:2px,color:#006064,font-weight:bold;
    classDef goldStep fill:#e8f5e9,stroke:#4caf50,stroke-width:2px,color:#1b5e20,font-weight:bold;
    
    %% Execution Nodes
    step1["🟫 Step 1: Raw Ingestion<br/>📓 Bronze_layer_improved.ipynb"]:::bronzeStep
    step2["⬜ Step 2: Silver Orchestration<br/>📓 silver_orchestration.ipynb"]:::silverStep
    step3["🟨 Step 3: Gold Orchestration<br/>📓 gold_orchestration.ipynb"]:::goldStep

    %% Workflow connection
    step1 -->|1. Ingests Raw CSVs| step2
    step2 -->|2. Cleanses & Standardizes| step3
```


---

## 📊 Analytics Schema Representation

The dimensional model generated in the Gold layer is structured for optimized analytical querying:

### 🔗 Star Schema Key Relationships

```mermaid
%%{init: {
  'theme': 'base',
  'themeVariables': {
    'primaryColor': '#e3f2fd',
    'primaryTextColor': '#0d47a1',
    'primaryBorderColor': '#1e88e5',
    'lineColor': '#8e24aa',
    'attributeBackgroundColor': '#ffffff',
    'attributeTextColor': '#1f2937',
    'keyColor': '#e53935'
  }
}}%%
erDiagram
    dim_customers ||--o{ fact_sales : "customer_key"
    dim_products ||--o{ fact_sales : "product_key"

    dim_customers {
        int customer_key PK
        string customer_id
        string customer_number
    }

    dim_products {
        int product_key PK
        string product_id
        string product_number
    }

    fact_sales {
        string order_number
        int customer_key FK
        int product_key FK
    }
```


### 📋 Detailed Table Schemas

<details open>
<summary><b>👤 Dimension: <code>dim_customers</code></b></summary>

* **Description:** Stores customer demographic profiles consolidated from CRM and ERP systems.
* **Fields:**
  * 🔑 **`customer_key`** `INT` (PK) - Surrogate key generated via row number sequence.
  * 🆔 **`customer_id`** `VARCHAR` - Unique CRM customer identifier.
  * 🔢 **`customer_number`** `VARCHAR` - Standardized customer code for cross-system mapping.
  * 👤 **`first_name`** / **`last_name`** `VARCHAR` - Standardized and trimmed names.
  * 🌍 **`country`** `VARCHAR` - Country location mapped from ERP.
  * 💍 **`marital_status`** `VARCHAR` - Marital status from CRM.
  * 🚻 **`gender`** `VARCHAR` - Gender (imputed from ERP if missing in CRM).
  * 📅 **`birthdate`** `DATE` - Birthdate (YYYY-MM-DD).
  * 📅 **`create_date`** `DATE` - Account creation timestamp.

</details>

<details open>
<summary><b>📦 Dimension: <code>dim_products</code></b></summary>

* **Description:** Stores product catalog data enriched with category hierarchies.
* **Fields:**
  * 🔑 **`product_key`** `INT` (PK) - Surrogate key generated via row number sequence.
  * 🆔 **`product_id`** `VARCHAR` - Unique CRM product identifier.
  * 🔢 **`product_number`** `VARCHAR` - Standardized product serial number.
  * 📦 **`product_name`** `VARCHAR` - Standardized product name.
  * 🏷️ **`category_id`** `VARCHAR` - Category identifier.
  * 🗂️ **`category`** / **`subcategory`** `VARCHAR` - ERP category mapping.
  * 🔧 **`maintenance_flag`** `VARCHAR` - Product maintenance status flag.
  * 📈 **`product_line`** `VARCHAR` - Active product line classification.
  * 📅 **`start_date`** `DATE` - Product record activation date.

</details>

<details open>
<summary><b>📊 Fact Table: <code>fact_sales</code></b></summary>

* **Description:** Captures transactional sales records mapped to customer and product dimensions.
* **Fields:**
  * 🧾 **`order_number`** `VARCHAR` - Unique sales transaction identifier.
  * 🔗 **`customer_key`** `INT` (FK) - Reference linking to `dim_customers`.
  * 🔗 **`product_key`** `INT` (FK) - Reference linking to `dim_products`.
  * 📅 **`order_date`** `DATE` - Date of order.
  * 📅 **`ship_date`** `DATE` - Date of shipment.
  * 📅 **`due_date`** `DATE` - Payment due date.
  * 🔢 **`quantity`** `INT` - Units purchased.
  * 💵 **`price`** `DECIMAL` - Transactional unit price.
  * 💰 **`sales_amount`** `DECIMAL` - Computed gross sales amount (`quantity` * `price`).

</details>









---

## 🛠️ Built With
* [![Databricks](https://img.shields.io/badge/Databricks_Workflows-FF3621?style=flat-square&logo=databricks&logoColor=white)](https://docs.databricks.com/workflows/index.html) &nbsp;•&nbsp; Pipeline scheduling & orchestration.
* [![Delta Lake](https://img.shields.io/badge/Delta_Lake-00ADF2?style=flat-square&logo=deltalake&logoColor=white)](https://delta.io/) &nbsp;•&nbsp; Acid Transactions, Time Travel, Schema Enforcement.
* [![Unity Catalog](https://img.shields.io/badge/Unity_Catalog-FF3621?style=flat-square&logo=databricks&logoColor=white)](https://www.databricks.com/product/unity-catalog) &nbsp;•&nbsp; Data governance and volume management.
* [![Apache Spark](https://img.shields.io/badge/PySpark_SQL-E25A28?style=flat-square&logo=apachespark&logoColor=white)](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/index.html) &nbsp;•&nbsp; Data transformation and processing engine.

