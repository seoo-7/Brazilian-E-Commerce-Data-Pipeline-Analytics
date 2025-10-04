# E-Commerce Data Pipeline - Technical Documentation

## Project Overview

This project implements a comprehensive end-to-end data pipeline for analyzing Brazilian e-commerce data from Olist. The pipeline orchestrates data ingestion, transformation, and analytics using modern data engineering tools, following a medallion architecture (Bronze, Silver, Gold layers) and culminating in interactive Power BI dashboards.

---

## Technology Stack

| Category | Technology | Version | Purpose |
|----------|-----------|---------|---------|
| **Orchestration** | Apache Airflow | 2.8.3 | Workflow scheduling and monitoring |
| **Data Processing** | Apache Spark (PySpark) | 3.5.3 | Distributed data processing |
| **Database** | PostgreSQL | 13 | Data warehouse storage |
| **Containerization** | Docker & Docker Compose | - | Application deployment |
| **Programming** | Python | 3.x | Scripting and automation |
| **Visualization** | Power BI | - | Business intelligence dashboards |
| **Database Management** | pgAdmin4 | - | Database administration |
| **Message Broker** | Redis | Latest | Airflow task queue backend |
| **Runtime** | OpenJDK | 17 | Java runtime for Spark |
| **JDBC Driver** | PostgreSQL JDBC | 42.6.0 | Spark-PostgreSQL connectivity |

---

## Data Pipeline Architecture

### Medallion Architecture

The pipeline follows a three-tier medallion architecture:

```
┌─────────────────────────────────────────────────────────────┐
│                    BRONZE LAYER (Raw Data)                   │
│  • CSV ingestion from Kaggle dataset                        │
│  • 9 raw tables with minimal transformation                 │
│  • Schema inference and type detection                      │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│                   SILVER LAYER (Cleaned Data)                │
│  • Data cleansing and standardization                       │
│  • Column renaming with business-friendly aliases           │
│  • Duplicate removal and data quality filters               │
│  • 8 transformed tables ready for analytics                 │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│              GOLD LAYER (Star Schema - Analytics Ready)      │
│  • 9 Dimension tables                                        │
│  • 3 Fact tables (Sales, Orders, Reviews)                   │
│  • Optimized for Power BI reporting                         │
└─────────────────────────────────────────────────────────────┘
```

### Data Model

**Dimension Tables:**
- `dim_date` - Date hierarchy (Year, Quarter, Month, Week, Day)
- `dim_time` - Hour-level time periods (24-hour, business hours, peak shopping)
- `dim_customers` - Customer geography and segments
- `dim_products` - Product categories, dimensions, weights
- `dim_sellers` - Seller locations and regions
- `dim_geography` - Brazilian zip codes, cities, states, coordinates
- `dim_order_status` - Order status classifications
- `dim_payment_types` - Payment method categories
- `dim_review_scores` - Review ratings (1-5) with descriptions

**Fact Tables:**
- `fact_sales` - Order item level grain (Sales Amount, Freight Value, Quantity)
- `fact_orders` - Order level grain (Payment Metrics, Delivery Timelines)
- `fact_reviews` - Review level grain (Review Scores, Comment Analysis)

---

## Data Flow

### End-to-End Pipeline Flow

```
┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│ CSV Files    │───▶│ Bronze Layer │───▶│ Silver Layer │
│ (Kaggle)     │    │ (PostgreSQL) │    │ (PostgreSQL) │
└──────────────┘    └──────────────┘    └──────────────┘
                                               │
                                               ▼
                    ┌──────────────────────────────────┐
                    │      Quality Checks              │
                    │  • Null value validation         │
                    │  • Data integrity checks         │
                    └──────────────┬───────────────────┘
                                   │
                                   ▼
                    ┌──────────────────────────────────┐
                    │      Gold Layer                  │
                    │  • Star schema modeling          │
                    │  • Dimension tables              │
                    │  • Fact tables                   │
                    └──────────────┬───────────────────┘
                                   │
                                   ▼
                    ┌──────────────────────────────────┐
                    │      Power BI Dashboards         │
                    │  • Sales Analytics               │
                    │  • Orders & Logistics            │
                    └──────────────────────────────────┘
```

### Source Dataset

**Brazilian E-Commerce Public Dataset by Olist** (Kaggle)
- **Records:** ~100,000 orders (2016-2018)
- **Tables:** 9 CSV files
  - `olist_customers_dataset.csv`
  - `olist_geolocation_dataset.csv`
  - `olist_order_items_dataset.csv`
  - `olist_order_payments_dataset.csv`
  - `olist_order_reviews_dataset.csv`
  - `olist_orders_dataset.csv`
  - `olist_products_dataset.csv`
  - `olist_sellers_dataset.csv`
  - `product_category_name_translation.csv`

---

## Pipeline Components

### 1. Bronze Layer - Data Ingestion (`ingestion.py`)

**Purpose:** Load raw CSV data into PostgreSQL with minimal transformation

**Process:**
- Reads 9 CSV files from `/opt/airflow/data/` directory
- Infers schema automatically using Spark
- Loads data into PostgreSQL `bronze` schema via JDBC
- Uses overwrite mode for idempotent execution

**Key Features:**
```python
# Automatic schema inference
df = spark.read.option('header', 'true').option('inferschema','true').csv(csv_path)

# JDBC connection to PostgreSQL
df.write.format('jdbc')\
    .option('url','jdbc:postgresql://ecommerce-postgres:5432/airflow')\
    .option('dbtable',f'"bronze".{table_name}')\
    .mode('overwrite')\
    .save()
```

**Output:** 9 raw tables in `bronze` schema

---

### 2. Silver Layer - Data Transformation (`transformation.py`)

**Purpose:** Cleanse, standardize, and prepare data for analytics

**Transformation Rules:**

#### General Transformations
- Column renaming with business-friendly aliases (e.g., `customer_id` → `Cus_ID`)
- Text formatting using `initcap()` for city names
- Duplicate removal based on `Order_ID`

#### Table-Specific Transformations

**Customers:**
- Standardize city names (initcap)
- Preserve state codes

**Orders:**
- Rename all date/timestamp columns
- Standardize order status (initcap)

**Geolocation:**
- Format city names (initcap)
- Maintain coordinates precision

**Order Items:**
- Remove duplicates by Order_ID
- Preserve pricing and freight values

**Order Payments:**
- Filter out 'not_defined' payment types
- Remove duplicate orders

**Order Reviews:**
- **Data Quality Filters:**
  - Review ID must be exactly 32 characters
  - Review score must be between 1-5
  - Remove special characters from comments (except: a-z, A-Z, 0-9, spaces, .,!?)
  - Validate date format (YYYY-MM-DD)
- Remove duplicate reviews per order

**Products:**
- Format category names (initcap, replace underscores with spaces)
- Preserve dimensional measurements

**Sellers:**
- Standardize city names (initcap)

**Output:** 8 cleaned tables in `silver` schema

---

### 3. Gold Layer - Star Schema Modeling (`reporting.py`)

**Purpose:** Create analytics-ready star schema optimized for Power BI

#### Dimension Tables Creation

**dim_date:**
- Date range derived from order dates
- Attributes: Date_SK, Year, Month, Quarter, Week, Day, DayName, MonthName
- Business flags: IsWeekend, IsWeekday, IsMonday, IsHoliday
- Fiscal year support
- Power BI compatible date format

**dim_time:**
- Hourly granularity (0-23 hours)
- 12/24 hour formats with AM/PM
- Time periods: Morning (6-11), Afternoon (12-17), Evening (18-21), Night (22-5)
- Business hours classification (9-17)
- Peak shopping hours (18-22)

**dim_customers:**
- Customer geography (Zip Code, City, State)
- Regional segmentation based on zip code ranges

**dim_products:**
- Product categories and dimensions
- Weight classifications: Light (<500g), Medium (500-2000g), Heavy (>2000g)
- Volume calculation (Height × Width × Length)

**dim_sellers:**
- Seller locations and regions
- Regional classification

**dim_geography:**
- Brazilian geographic data
- Regions: Southeast, Northeast, South, Center-West, North
- Latitude/Longitude coordinates
- Deduplicated by zip code

**dim_order_status:**
- Order status categories: Completed, In Progress, Failed
- Status mappings

**dim_payment_types:**
- Payment categories: Card, Bank Transfer, Voucher, Other

**dim_review_scores:**
- Score descriptions: Very Poor (1) to Excellent (5)
- Score categories: Negative (1-2), Neutral (3), Positive (4-5)

#### Fact Tables Creation

**fact_sales (Grain: Order Item Level):**
- Measures: Sales Amount, Freight Value, Quantity, Order Payment Value
- Keys: Product_ID, Customer_ID, Seller_ID, Payment_Type, Order_Status
- Date/Time keys for temporal analysis
- One record per order item

**fact_orders (Grain: Order Level):**
- Aggregated metrics:
  - Items count and total value
  - Total freight value
  - Payment installments and methods count
- Delivery timeline metrics:
  - Approval days
  - Total delivery days
- Timestamps for lifecycle analysis
- One record per order

**fact_reviews (Grain: Review Level):**
- Review scores and comments
- Comment analysis:
  - Comment length
  - Has comment flag
  - Has title flag
- Response time calculation (days)
- One record per review

#### Performance Optimizations

**Spark Configuration:**
```python
# Adaptive query execution
spark.sql.adaptive.enabled = true
spark.sql.adaptive.coalescePartitions.enabled = true
spark.sql.adaptive.skewJoin.enabled = true

# Optimized partitions and memory
spark.sql.shuffle.partitions = 20
spark.executor.memory = 2g
spark.driver.memory = 2g

# Serialization and Arrow optimization
spark.serializer = KryoSerializer
spark.sql.execution.arrow.pyspark.enabled = true
```

**Write Strategy:**
- Batch size: 1000 records
- Retry logic: 3 attempts
- Garbage collection between fact table writes
- Isolation level: NONE for performance

**Output:** 9 dimension tables + 3 fact tables in `gold` schema

---

## JDBC Spark Configuration

### PostgreSQL JDBC Driver

**Driver Details:**
- **Version:** 42.6.0
- **Location:** `/opt/airflow/spark_jobs/postgresql-42.6.0.jar`
- **Purpose:** Enable Spark-PostgreSQL connectivity

### Connection Configuration

```python
# Spark Session Configuration
spark = SparkSession.builder\
    .config('spark.jars', '/opt/airflow/spark_jobs/postgresql-42.6.0.jar')\
    .getOrCreate()

# JDBC Read
df = spark.read.format('jdbc')\
    .option('url', 'jdbc:postgresql://ecommerce-postgres:5432/airflow')\
    .option('dbtable', 'schema.table_name')\
    .option('user', 'airflow')\
    .option('password', 'airflow')\
    .option('driver', 'org.postgresql.Driver')\
    .load()

# JDBC Write
df.write.format('jdbc')\
    .option('url', 'jdbc:postgresql://ecommerce-postgres:5432/airflow')\
    .option('dbtable', 'schema.table_name')\
    .option('user', 'airflow')\
    .option('password', 'airflow')\
    .option('driver', 'org.postgresql.Driver')\
    .option('batchsize', '1000')\
    .mode('overwrite')\
    .save()
```

### Network Configuration

- **Container Name:** `ecommerce-postgres`
- **Internal Port:** 5432
- **External Port:** 5433 (host machine)
- **Database:** airflow
- **Network:** ecommerce-analytics-network

---

## Airflow Orchestration

### DAG Configuration

**DAG Name:** `E-commerce_dag`

**Schedule:** Every 30 minutes (`timedelta(minutes=30)`)

**Execution Parameters:**
- Start Date: September 23, 2025
- Catchup: False (no backfilling)
- Timeout: 60 minutes
- Retries: 1 (with 30-minute delay)

### Task Dependencies

```
create_bronze_schema
    ↓
ingest_data
    ↓
    ├─→ ingest_email (notification)
    └─→ create_silver_schema
            ↓
        transform_data
            ↓
        quality_checks
            ↓
            ├─→ transform_email (notification)
            └─→ create_gold_schema
                    ↓
                load_to_gold
                    ↓
                gold_email (notification)

                
```
![alt text](<Airflow Pipeline.png>)


### Task Details

| Task ID | Type | Purpose | Priority |
|---------|------|---------|----------|
| `create_bronze_schema` | PostgresOperator | Create bronze schema | 10 |
| `ingest_data` | SparkSubmitOperator | Load CSV to bronze | 9 |
| `ingest_email` | EmailOperator | Notify ingestion completion | 8 |
| `create_silver_schema` | PostgresOperator | Create silver schema | 7 |
| `transform_data` | SparkSubmitOperator | Transform to silver | 6 |
| `quality_checks` | PythonOperator | Validate data quality | 5 |
| `transform_email` | EmailOperator | Notify transformation completion | 4 |
| `create_gold_schema` | PostgresOperator | Create gold schema | 3 |
| `load_to_gold` | SparkSubmitOperator | Create star schema | 2 |
| `gold_email` | EmailOperator | Notify gold layer completion | 1 |

### Spark Submit Configuration

```python
SparkSubmitOperator(
    application='/opt/airflow/spark_jobs/script.py',
    conn_id='spark_default',
    conf={
        "spark.master": "local",
        "spark.app.name": "Ecommerce-Job-Name",
        "spark.sql.adaptive.enabled": "true"
    },
    jars='/opt/airflow/spark_jobs/postgresql-42.6.0.jar',
    driver_class_path='/opt/airflow/spark_jobs/postgresql-42.6.0.jar'
)
```

### Email Notifications

**Configuration:**
- **SMTP Server:** Gmail (smtp.gmail.com:587)
- **Protocol:** STARTTLS
- **Recipient:** sayedyasserrady@gmail.com

**Notification Points:**
1. After data ingestion (Bronze layer completion)
2. After transformation and quality checks (Silver layer completion)
3. After gold layer creation (Analytics-ready)

---

## Quality Checks

### Automated Data Validation

**Function:** `quality_checks()` (Python Operator)

**Hook:** PostgresHook with connection ID `postgres_conn`

### Validation Rules

#### 1. Order Items Quality Check

```sql
SELECT
    COUNT(*) as total_records,
    SUM(CASE WHEN "Ord_ID" IS NULL THEN 1 ELSE 0 END) as null_ord_id,
    SUM(CASE WHEN "Prod_ID" IS NULL THEN 1 ELSE 0 END) as null_prod_id
FROM silver.order_items;
```

**Criteria:**
- No null `Ord_ID` values
- No null `Prod_ID` values
- Must have records in table

#### 2. Order Reviews Quality Check

```sql
SELECT
    COUNT(*) as total_records,
    SUM(CASE WHEN "Rev_ID" IS NULL THEN 1 ELSE 0 END) as null_rev_id,
    SUM(CASE WHEN LENGTH("Rev_ID") != 32 THEN 1 ELSE 0 END) as invalid_length_rev_id
FROM silver.order_reviews;
```

**Criteria:**
- No null `Rev_ID` values
- All `Rev_ID` must be exactly 32 characters
- Must have records in table

### Failure Handling

- **Pass:** Pipeline continues to gold layer
- **Fail:** Pipeline stops, error message logged, email notification sent
- **Logging:** All check results logged to Airflow task logs

---

## Power BI Dashboard

### Dashboard 1: Sales Analytics

![alt text](Orders
.png)

**KPI Cards:**
- Total Payments (with Previous Year, YoY Difference, YoY %)
- Total Sales (with Previous Year, YoY Difference, YoY %)
- Total Customers (with Previous Year, YoY Difference, YoY %)
- Total Products (with Previous Year, YoY Difference, YoY %)

**Visualizations:**

1. **Gauge Chart**
   - Metric: Average Daily Sales
   - Purpose: Track daily performance against targets

2. **Decomposition Tree**
   - Hierarchy: Sales → Region → Products → City
   - Purpose: Drill-down analysis of sales drivers

3. **Stacked Column Chart**
   - X-axis: Months (current year)
   - Y-axis: Sales
   - Data Labels: YoY percentage change
   - Purpose: Monthly sales trend analysis

4. **Stacked Bar Chart**
   - Top 10 products by sales
   - Data Labels: YoY percentage change
   - Purpose: Product performance ranking

5. **Line Chart**
   - X-axis: Days in month
   - Y-axis: Sales
   - Purpose: Daily sales pattern within month

6. **Table**
   - Columns: Day, Time Period (24-hour breakdown), Sales
   - Purpose: Hourly sales analysis

7. **Donut Chart**
   - Dimension: Review Score (1-5)
   - Purpose: Customer satisfaction distribution

**Slicers:**
- Year
- Month
- Day

---

### Dashboard 2: Orders & Logistics
![alt text](Orders.png)
**KPI Cards:**
- Total Orders (with Previous Year, YoY Difference, YoY %)
- Total Shipping (with Previous Year, YoY Difference, YoY %)

**Metric Cards:**
- Average Order Value
- Average Ship Cost
- Average Daily Orders
- Average Ship Date
- Number of Cities

**Visualizations:**

1. **Stacked Column Chart**
   - X-axis: Months (current year)
   - Y-axis: Orders
   - Data Labels: YoY percentage change
   - Purpose: Monthly order trend

2. **Bar Chart**
   - Top 3 cities by Average Daily Orders
   - Metric: % of Total
   - Purpose: Identify key markets

3. **Shape Map**
   - Geography: South America
   - Purpose: Geographic distribution visualization

4. **Bar Chart**
   - Cities performance by orders
   - Data Labels: YoY percentage change
   - Purpose: City-level performance analysis

5. **Comprehensive Table**
   - Columns: Payments, Sales, Shipping, Orders, Avg Daily Order, Avg Order Value, Avg Delivery Daily, YoY %
   - Purpose: Detailed metrics overview

6. **Donut Chart 1: Orders by Payment Type**
   - Dimension: Payment methods (credit card, boleto, voucher, etc.)

7. **Donut Chart 2: Orders by Region**
   - Dimension: Brazilian regions (Southeast, Northeast, South, etc.)

8. **Donut Chart 3: Orders by Time Period**
   - Dimension: Morning, Afternoon, Evening, Night
   - Purpose: Temporal ordering patterns

**Slicers:**
- Year
- Month
- Score Category (Positive, Neutral, Negative)
- Region

### Data Source Connection

**Connection Type:** DirectQuery to PostgreSQL

**Database:** 
- Host: localhost
- Port: 5433
- Database: airflow
- Schema: gold

**Tables Used:**
- All 9 dimension tables
- All 3 fact tables

---

## Docker Deployment

### Container Architecture

```
┌─────────────────────────────────────────────────────────┐
│                   Docker Network                         │
│              ecommerce-analytics-network                 │
│                                                          │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐ │
│  │   Redis      │  │  PostgreSQL  │  │   pgAdmin4   │ │
│  │   :6379      │  │   :5432      │  │    :80       │ │
│  └──────────────┘  └──────────────┘  └──────────────┘ │
│                                                          │
│  ┌──────────────────────────────────────────────────┐  │
│  │          Airflow Services                        │  │
│  │  ┌─────────────┐  ┌─────────────┐               │  │
│  │  │ Webserver   │  │  Scheduler  │               │  │
│  │  │   :8080     │  │             │               │  │
│  │  └─────────────┘  └─────────────┘               │  │
│  │                                                   │  │
│  │  • Spark 3.5.3 installed                        │  │
│  │  • Java 17 (OpenJDK)                            │  │
│  │  • PySpark + dependencies                       │  │
│  └──────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────┘
```

### Dockerfile

**Base Image:** `apache/airflow:2.8.3`

**System Dependencies:**
- OpenJDK 17
- Procps
- Wget

**Spark Installation:**
- Version: 3.5.3 with Hadoop 3
- Download from Apache archives
- Extracted to `/opt/spark`

**Python Dependencies:**
- apache-airflow-providers-postgres
- requests
- pandas
- psycopg2-binary
- pyspark==3.5.3
- findspark

**Environment Variables:**
```bash
JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
SPARK_HOME=/opt/spark
PATH=$SPARK_HOME/bin:$JAVA_HOME/bin:$PATH
```

### Docker Compose Services

#### 1. ecommerce-redis
- **Image:** redis:latest
- **Port:** 6379
- **Purpose:** Celery result backend
- **Healthcheck:** redis-cli ping

#### 2. ecommerce-postgres
- **Image:** postgres:13
- **Port:** 5433 (external), 5432 (internal)
- **Credentials:**
  - User: airflow
  - Password: airflow
  - Database: airflow
- **Volumes:**
  - `./pgdata:/var/lib/postgresql/data`
  - `./data:/data`
- **Healthcheck:** pg_isready

#### 3. airflow-init
- **Purpose:** Initialize Airflow database and create admin user
- **Credentials:**
  - Username: airflow
  - Password: airflow
  - Role: Admin

#### 4. airflow-webserver
- **Port:** 8080
- **Purpose:** Web UI for monitoring and management
- **Command:** webserver

#### 5. airflow-scheduler
- **Purpose:** Schedule and execute DAG tasks
- **Command:** scheduler

#### 6. pgadmin
- **Image:** dpage/pgadmin4
- **Port:** 5050
- **Credentials:**
  - Email: pgadmin@example.com
  - Password: pgadmin
- **Volume:** `./pgadmin_data:/var/lib/pgadmin`

