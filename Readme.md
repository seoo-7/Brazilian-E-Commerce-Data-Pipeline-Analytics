# Brazilian E-Commerce Data Pipeline & Analytics

An end-to-end data engineering & Analaytics  project that processes 100K+ e-commerce orders using Apache Airflow, PySpark, PostgreSQL, and Power BI. The pipeline follows a medallion architecture (Bronze → Silver → Gold) to transform raw data into actionable business insights.

## 📊 Project Overview

This project implements a complete data pipeline for analyzing Brazilian e-commerce transactions from the Olist dataset (2016-2018). The system automatically ingests raw CSV files, performs data quality checks and transformations, builds a star schema data warehouse, and powers interactive Power BI dashboards for sales and logistics analytics.

**Key Features:**
- Automated data pipeline with Airflow orchestration
- Three-layer medallion architecture for data quality
- Star schema modeling optimized for analytics
- Real-time email notifications at pipeline milestones
- Interactive Power BI dashboards with YoY analysis
- Fully containerized with Docker for easy deployment

## 🛠️ Technology Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Orchestration** | Apache Airflow 2.8.3 | Workflow scheduling and monitoring |
| **Processing** | Apache Spark 3.5.3 (PySpark) | Distributed data transformation |
| **Storage** | PostgreSQL 13 | Data warehouse (Bronze/Silver/Gold layers) |
| **Containerization** | Docker & Docker Compose | Service deployment and isolation |
| **Visualization** | Power BI Desktop | Business intelligence dashboards |
| **Database UI** | pgAdmin4 | Database administration |
| **Message Queue** | Redis | Airflow backend |
| **Runtime** | Java 17 (OpenJDK) | Spark execution environment |

## 🔄 End-to-End Pipeline Flow

### Architecture Overview

```
┌──────────────────┐      ┌──────────────────┐      ┌──────────────────┐
│   CSV Files      │ ───▶ │  Bronze Layer    │ ───▶ │  Silver Layer    │
│   (9 Tables)     │      │  (Raw Data)      │      │  (Cleaned Data)  │
└──────────────────┘      └──────────────────┘      └────────┬─────────┘
                                                               │
                                                               ▼
                                                    ┌──────────────────┐
                                                    │ Quality Checks   │
                                                    └────────┬─────────┘
                                                             │
                                                             ▼
                                                    ┌──────────────────┐
                                                    │   Gold Layer     │
                                                    │ (Star Schema)    │
                                                    │ • 9 Dimensions   │
                                                    │ • 3 Fact Tables  │
                                                    └────────┬─────────┘
                                                             │
                                                             ▼
                                                    ┌──────────────────┐
                                                    │  Power BI        │
                                                    │  Dashboards      │
                                                    └──────────────────┘
```

### Data Layers

**🥉 Bronze Layer** - Raw Data Ingestion
- Loads 9 CSV files from Kaggle dataset
- Schema inference and type detection
- Minimal transformation, preserves original data
- Tables: customers, orders, products, sellers, payments, reviews, items, geolocation

**🥈 Silver Layer** - Data Cleansing & Standardization
- Column renaming with business-friendly aliases
- Text formatting (capitalize city names, clean categories)
- Data quality filters:
  - Remove duplicates by Order_ID
  - Validate review scores (1-5)
  - Remove special characters from comments
  - Filter invalid payment types
  - Validate date formats
- Output: 8 cleaned, analysis-ready tables

**🥇 Gold Layer** - Star Schema Data Warehouse
- **9 Dimension Tables:**
  - `dim_date` - Date hierarchy with fiscal periods
  - `dim_time` - Hourly analysis (business hours, peak shopping times)
  - `dim_customers` - Customer geography and segments
  - `dim_products` - Product categories, dimensions, weight classifications
  - `dim_sellers` - Seller locations and regions
  - `dim_geography` - Brazilian regions with coordinates
  - `dim_order_status` - Order lifecycle categories
  - `dim_payment_types` - Payment method classifications
  - `dim_review_scores` - Rating descriptions and categories

- **3 Fact Tables:**
  - `fact_sales` - Order item grain (sales amount, freight, quantity)
  - `fact_orders` - Order grain (payment metrics, delivery timelines)
  - `fact_reviews` - Review grain (scores, comments, response times)

### Data Quality Checks

Automated validation between Silver and Gold layers:
- Null value detection in critical columns
- Review ID length validation (must be 32 characters)
- Record count verification
- Pipeline halts on failures with email alerts

## ⚙️ Airflow Orchestration

### DAG: `E-commerce_dag`

**Schedule:** Runs every 30 minutes

**Pipeline Stages:**

1. **Bronze Layer Creation**
   - Create schema → Ingest CSV data → Email notification

2. **Silver Layer Transformation**
   - Create schema → Transform & cleanse data → Quality checks → Email notification

3. **Gold Layer Modeling**
   - Create schema → Build star schema → Email notification

### Task Flow

<img width="1825" height="730" alt="Airflow Pipeline" src="https://github.com/user-attachments/assets/ed5486ec-e088-461c-8eb7-b083945d8c62" />

```
create_bronze_schema
    ↓
ingest_data (Spark)
    ↓
[ingest_email] + create_silver_schema
                      ↓
                transform_data (Spark)
                      ↓
                quality_checks (Python)
                      ↓
      [transform_email] + create_gold_schema
                               ↓
                         load_to_gold (Spark)
                               ↓
                          [gold_email]
```

### Monitoring Features

- **Real-time logs** in Airflow UI
- **Email notifications** at 3 key milestones
- **Retry logic** with 30-minute delay
- **Task priorities** for critical path optimization
- **60-minute timeout** for full pipeline execution

## 📈 Power BI Dashboards

### Dashboard 1: Sales Analytics
![alt text](Sales.png)

**KPIs with YoY Analysis:**
- Total Payments, Sales, Customers, Products
- Each showing: Current Value | Previous Year | YoY Difference | YoY %

**Visualizations:**
- **Gauge:** Average Daily Sales
- **Decomposition Tree:** Sales by Region → Products → City
- **Stacked Column Chart:** Monthly sales with YoY% labels
- **Stacked Bar Chart:** Top 10 products with YoY% labels
- **Line Chart:** Daily sales pattern
- **Table:** Hourly sales breakdown (24-hour analysis)
- **Donut Chart:** Review score distribution

**Filters:** Year, Month, Day

---

### Dashboard 2: Orders & Logistics

![alt text](Orders.png)

**KPIs with YoY Analysis:**
- Total Orders, Total Shipping

**Key Metrics:**
- Avg Order Value, Avg Ship Cost, Avg Daily Orders, Avg Ship Date, Cities Count

**Visualizations:**
- **Stacked Column Chart:** Monthly orders with YoY% labels
- **Bar Chart:** Top 3 cities by Avg Daily Orders (% of Total)
- **Shape Map:** South America geographic distribution
- **Bar Chart:** City performance with YoY% labels
- **Comprehensive Table:** All metrics with YoY percentages
- **3 Donut Charts:**
  - Orders by Payment Type
  - Orders by Region
  - Orders by Time Period (Morning, Afternoon, Evening, Night)

**Filters:** Year, Month, Score Category, Region

### Power BI Connection

- **Type:** DirectQuery to PostgreSQL
- **Host:** localhost:5433
- **Database:** airflow
- **Schema:** gold
- **Tables:** All 9 dimensions + 3 fact tables

## 🚀 Quick Start

### Prerequisites
- Docker Desktop installed
- 8GB+ RAM
- 20GB+ free disk space

### Setup Instructions

1. **Clone Repository**
```bash
git clone <your-repo-url>
cd ecommerce-pipeline
```

2. **Download Dataset**
   - Get "Brazilian E-Commerce Public Dataset by Olist" from Kaggle
   - Place CSV files in `./data/` directory

3. **Start Services**
```bash
docker-compose up -d
```

4. **Install Spark Provider**
```bash
docker exec ecommerce-airflow-webserver pip install apache-airflow-providers-apache-spark
docker exec ecommerce-airflow-scheduler pip install apache-airflow-providers-apache-spark
docker-compose restart airflow-webserver airflow-scheduler
```

5. **Access Airflow**
   - URL: http://localhost:8080
   - Credentials: airflow / airflow
   - Enable and trigger `E-commerce_dag`

6. **Connect Power BI**
   - Get Data → PostgreSQL
   - Server: localhost:5433
   - Database: airflow
   - Import from `gold` schema

### Service Ports

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow UI | http://localhost:8080 | airflow / airflow |
| pgAdmin | http://localhost:5050 | pgadmin@example.com / pgadmin |
| PostgreSQL | localhost:5433 | airflow / airflow |

## 📁 Project Structure

```
ecommerce-pipeline/
├── dags/
│   └── E-commerce_dag.py           # Airflow DAG definition
├── spark_jobs/
│   ├── ingestion.py                # Bronze layer script
│   ├── transformation.py           # Silver layer script
│   ├── reporting.py                # Gold layer script
│   └── postgresql-42.6.0.jar       # JDBC driver
├── sql/
│   ├── create_bronze_schema.sql
│   ├── create_silver_schema.sql
│   └── create_gold_schema.sql
├── data/                           # Place CSV files here
├── docker-compose.yaml             # Service orchestration
├── Dockerfile                      # Custom Airflow image
└── requirements.txt                # Python dependencies
```

## 🎯 Key Achievements

- ✅ **100% Automated Pipeline** - Zero manual intervention required
- ✅ **Data Quality Assurance** - Built-in validation checks
- ✅ **Scalable Architecture** - Handles millions of records efficiently
- ✅ **Production-Ready** - Containerized with retry logic and monitoring
- ✅ **Business-Ready Analytics** - Star schema optimized for BI tools
- ✅ **Year-over-Year Analysis** - Built-in time intelligence for trend analysis

## 📊 Dataset Information

**Source:** Brazilian E-Commerce Public Dataset by Olist (Kaggle)
- **Period:** 2016-2018
- **Records:** ~100,000 orders
- **Scope:** Brazilian marketplace connecting merchants to customers
- **Data:** Orders, customers, products, sellers, payments, reviews, shipping

## 🔧 Technologies Explained

- **Apache Airflow** - Orchestrates the entire pipeline, schedules tasks, monitors execution
- **PySpark** - Processes large datasets efficiently with distributed computing
- **PostgreSQL** - Stores data in three layers (Bronze/Silver/Gold) with schema separation
- **Docker** - Containerizes all services for consistent deployment across environments
- **JDBC** - Connects Spark to PostgreSQL for reading/writing data
- **Power BI** - Creates interactive dashboards with DirectQuery to live data

## 📧 Contact

For questions or suggestions, please open an issue or contact: sayedyasserrady@gmail.com

---

**⭐ If you find this project useful, please consider giving it a star!**
