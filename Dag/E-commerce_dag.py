from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator  # Updated import
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator  # Updated import
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.email import EmailOperator
import logging


def quality_checks():
    ''' Function to perform data quality checks'''

    logger = logging.getLogger(__name__)
    check_passed = True
    message = []

    try:
        hook = PostgresHook(postgres_conn_id='postgres_conn')

        # Check 1: Order Items Quality
        null_checks_orders = '''
            SELECT
                COUNT(*) as total_records,
                SUM(CASE WHEN "Ord_ID" IS NULL THEN 1 ELSE 0 END) as null_ord_id,
                SUM(CASE WHEN "Prod_ID" IS NULL THEN 1 ELSE 0 END) as null_prod_id
            FROM silver.order_items;
        '''

        # Check 2: Order Reviews Quality
        clean_check_reviews = '''
            SELECT
                COUNT(*) as total_records,
                SUM(CASE WHEN "Rev_ID" IS NULL THEN 1 ELSE 0 END) as null_rev_id,
                SUM(CASE WHEN LENGTH("Rev_ID") != 32 THEN 1 ELSE 0 END) as invalid_length_rev_id
            FROM silver.order_reviews;
        '''

        # Execute queries
        results_orders = hook.get_first(null_checks_orders)
        results_reviews = hook.get_first(clean_check_reviews)

        logger.info(f"Order items results: {results_orders}")
        logger.info(f"Order reviews results: {results_reviews}")

        if results_orders is None or results_reviews is None:
            raise ValueError("Quality check queries returned no results")

        # Validate order_items
        total_orders, null_ord_id, null_prod_id = results_orders
        if total_orders == 0:
            message.append("WARNING: No records found in order_items table")
        elif null_ord_id > 0 or null_prod_id > 0:
            check_passed = False
            message.append(f"FAILED: order_items - {null_ord_id} null Ord_IDs, {null_prod_id} null Prod_IDs")
        else:
            message.append("PASSED: order_items - No null values in critical columns")

        # Validate order_reviews
        total_reviews, null_rev_id, invalid_length_rev_id = results_reviews
        if total_reviews == 0:
            message.append("WARNING: No records found in order_reviews table")
        elif null_rev_id > 0 or invalid_length_rev_id > 0:
            check_passed = False
            message.append(f"FAILED: order_reviews - {null_rev_id} null Rev_IDs, {invalid_length_rev_id} invalid length Rev_IDs")
        else:
            message.append("PASSED: order_reviews - All Rev_IDs are valid")

        # Final decision
        if not check_passed:
            error_message = "Data quality checks failed:\n" + "\n".join(message)
            logger.error(error_message)
            raise ValueError(error_message)
        else:
            success_message = "All data quality checks passed:\n" + "\n".join(message)
            logger.info(success_message)
            print(success_message)

    except Exception as e:
        logger.error(f"Error during quality checks: {str(e)}")
        raise

# DAG definition
with DAG(
    'E-commerce_dag',
    description='E-commerce data pipeline DAG',
    start_date=datetime(2025, 9, 23),
    schedule_interval=timedelta(minutes=30),
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=30),
    }
) as dag:

    # Task 1: Create bronze schema
    create_bronze_schema = PostgresOperator(
        task_id='create_bronze_schema',
        postgres_conn_id='postgres_conn',
        sql='sql/create_bronze_schema.sql',
        pool='default_pool',
        priority_weight=10
    )

    # Task 2: Data ingestion using Spark
    ingest_data = SparkSubmitOperator(
        task_id='ingest_data',
        application='/opt/airflow/spark_jobs/ingestion.py',
        conn_id='spark_default',
        verbose=False,
        pool='default_pool',
        priority_weight=9,
        conf={
            "spark.master": "local",
            "spark.app.name": "Ecommerce-Data-Ingestion",
            "spark.sql.adaptive.enabled": "true"
        },
        jars='/opt/airflow/spark_jobs/postgresql-42.6.0.jar',
        driver_class_path='/opt/airflow/spark_jobs/postgresql-42.6.0.jar'
    )

    # Task 3: Send email after ingestion
    ingest_email = EmailOperator(
        task_id='ingest_email',
        to='sayedyasserrady@gmail.com',
        subject='E-commerce Data Ingestion Completed',
        html_content='''<h1>E-commerce Data Ingestion Completed Successfully</h1>
                        <p>The data ingestion process has been completed and the data is now available in the bronze schema.</p>''',
        pool='default_pool',
        priority_weight=8
    )

    # Task 4: Create silver schema
    create_silver_schema = PostgresOperator(
        task_id='create_silver_schema',
        postgres_conn_id='postgres_conn',
        sql='sql/create_silver_schema.sql',
        pool='default_pool',
        priority_weight=7
    )

    # Task 5: Data transformation using Spark
    transform_data = SparkSubmitOperator(
        task_id='transform_data',
        application='/opt/airflow/spark_jobs/transformation.py',
        conn_id='spark_default',
        verbose=False,
        pool='default_pool',
        priority_weight=6,
        conf={
            "spark.master": "local",
            "spark.app.name": "Ecommerce-Data-Transformation",
            "spark.sql.adaptive.enabled": "true"
        },
        jars='/opt/airflow/spark_jobs/postgresql-42.6.0.jar',
        driver_class_path='/opt/airflow/spark_jobs/postgresql-42.6.0.jar'
    )

    # Task 6: Quality checks
    quality_checks_task = PythonOperator(
        task_id='quality_checks',
        python_callable=quality_checks,
        pool='default_pool',
        priority_weight=5
    )

    # Task 7: Send email after transformation and quality checks
    transform_email = EmailOperator(
        task_id='transform_email',
        to='sayedyasserrady@gmail.com',
        subject='E-commerce Data Transformation and Quality Checks Completed',
        html_content='''<h1>E-commerce Data Transformation and Quality Checks Completed Successfully</h1>
                        <p>The data transformation process has been completed and the data is now available in the silver schema.</p>
                        <p>Data quality checks have been performed successfully.</p>''',
        pool='default_pool',
        priority_weight=4
    )

    # task 8 create gold schema for reporting
    create_gold_schema = PostgresOperator(
        task_id='create_gold_schema',
        postgres_conn_id='postgres_conn',
        sql='sql/create_gold_schema.sql',
        pool='default_pool',
        priority_weight=3
    )

    # task 9 load to gold layer
    load_to_gold = SparkSubmitOperator(
        task_id='load_to_gold',
        application='/opt/airflow/spark_jobs/reporting.py',
        conn_id='spark_default',
        verbose=False,
        pool='default_pool',
        priority_weight=2,
        conf={
            "spark.master": "local",
            "spark.app.name": "Ecommerce-Load-to-Gold",
            "spark.sql.adaptive.enabled": "true"
        },
        jars='/opt/airflow/spark_jobs/postgresql-42.6.0.jar',
        driver_class_path='/opt/airflow/spark_jobs/postgresql-42.6.0.jar'
    )
    # task 10 send email after loading to gold
    gold_email = EmailOperator(
        task_id='gold_email',
        to='sayedyasserrady@gmail.com',
        subject='E-commerce Data Loaded to Gold Layer Successfully',
        html_content='''<h1>E-commerce Data Loaded to Gold Layer Successfully</h1>
                        <p>The data has been successfully loaded into the gold layer and is ready for reporting.</p>''',
        pool='default_pool',
        priority_weight=1
    )



    # Proper dependency chain:
    
    create_bronze_schema >> ingest_data >> [ingest_email, create_silver_schema] 
    create_silver_schema >> transform_data >> quality_checks_task >> [ transform_email , create_gold_schema ]
    create_gold_schema >> load_to_gold >> gold_email  