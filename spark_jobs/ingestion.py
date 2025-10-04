from pyspark.sql import SparkSession

def ingest_csv_to_postgres(csv_path , table_name ,schema_name='bronze' ):
    # Initialize Spark session
    spark = SparkSession.builder\
    .appName('Ecommerce Data bronze')\
    .config('spark.jars', '/opt/airflow/spark_jobs/postgresql-42.6.0.jar')\
    .config("spark.log.level", "WARN") \
    .getOrCreate()
    
    # Read CSV file into DataFrame
    df = spark.read.option('header', 'true').option('inferschema','true').csv(csv_path)

    # Write DataFrame to PostgreSQL
    df.write.format('jdbc')\
        .option('url','jdbc:postgresql://ecommerce-postgres:5432/airflow')\
        .option('dbtable',f'"{schema_name}".{table_name}')\
        .option('user', 'airflow')\
        .option('password', 'airflow')\
        .option('driver', 'org.postgresql.Driver')\
        .mode('overwrite')\
        .save()
    
    spark.stop()
    print(f'Data bronze Completed {csv_path} to {schema_name}.{table_name}')

tables = {
        'customers': 'olist_customers_dataset.csv',
        'geolocation': 'olist_geolocation_dataset.csv',
        'order_items': 'olist_order_items_dataset.csv',
        'order_payments': 'olist_order_payments_dataset.csv',
        'order_reviews': 'olist_order_reviews_dataset.csv',
        'orders': 'olist_orders_dataset.csv',
        'products': 'olist_products_dataset.csv',
        'sellers': 'olist_sellers_dataset.csv',
        'product_category_name_translation': 'product_category_name_translation.csv'
    }
    
if __name__ == '__main__':
    for key , value  in tables.items():
        ingest_csv_to_postgres(f'/opt/airflow/data/{value}', key)