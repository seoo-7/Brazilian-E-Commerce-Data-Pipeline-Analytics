from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


def create_spark_session():
    return SparkSession.builder \
        .appName('Ecommerce Data Transformaton')\
        .config('spark.jars', '/opt/airflow/spark_jobs/postgresql-42.6.0.jar')\
        .config("spark.log.level", "WARN") \
        .getOrCreate()

def read_tables(spark, table_name, schema_name='bronze'):
    '''Read tables from Postgres'''
    return spark.read.format('jdbc')\
        .option('url','jdbc:postgresql://ecommerce-postgres:5432/airflow')\
        .option('dbtable',f'"{schema_name}".{table_name}')\
        .option('user', 'airflow')\
        .option('password', 'airflow')\
        .option('driver', 'org.postgresql.Driver')\
        .load()
    
def transform(spark):  
    ''' Transform all tables in ingestion layer'''

    ## Customers table transformation
    customers = read_tables(spark, 'customers')
    customers_df = customers.select(
                col('customer_id').alias('Cus_ID'),
                col('customer_unique_id').alias('Cus_Unique_ID'),
                col('customer_zip_code_prefix').alias('Cus_Zip_Code_Prefix'),
                initcap(col('customer_city')).alias('Cus_City'),
                col('customer_state')
    )

       
    ## orders table transformation
    orders = read_tables(spark, 'orders')
    orders_df = orders.select(
            col('order_id').alias('Ord_ID'),
            col('customer_id').alias('Cus_ID'),
            initcap(col('order_status')).alias('Ord_Status'),
            col('order_purchase_timestamp').alias('Ord_Purchase_Time'),
            col('order_approved_at').alias('Ord_Approved_At'),
            col('order_delivered_carrier_date').alias('Ord_Delivered_Carrier_Date'),
            col('order_delivered_customer_date').alias('Ord_Delivered_Customer_Date'),
            col('order_estimated_delivery_date').alias('Ord_Estimated_Delivery_Date')
                                                        
        )
    ## geolocation table transformation
    geolocation = read_tables(spark, 'geolocation')
    geolocation_df = geolocation.select(
            col('geolocation_zip_code_prefix').alias('Geo_Zip_Code_Prefix'),
            col('geolocation_lat').alias('Geo_Lat'),
            col('geolocation_lng').alias('Geo_Lng'),
            initcap(col('geolocation_city')).alias('Geo_City'),
            col('geolocation_state').alias('Geo_State')
        )
    
    ## order items table transformation
    order_items = read_tables(spark, 'order_items')
    order_items_df = order_items.select(
            col('order_id').alias('Ord_ID'),
            col('order_item_id').alias('Ord_Item_ID'),
            col('product_id').alias('Prod_ID'),
            col('seller_id').alias('Sell_ID'),
            col('shipping_limit_date').alias('Shipping_Limit_Date'),
            col('price').alias('Price'),
            col('freight_value').alias('Freight_Value')
        ).dropDuplicates(['Ord_ID']) 
    
    ## order payments table transformation
    order_payments = read_tables(spark, 'order_payments')
    order_payments_df = order_payments.select(
            col('order_id').alias('Ord_ID'),
            col('payment_sequential').alias('Payment_Sequential'),
            col('payment_type').alias('Payment_Type'),
            col('payment_installments').alias('Payment_Installments'),
            col('payment_value').alias('Payment_Value')
        ).dropDuplicates(['Ord_ID']) \
        .filter(col('Payment_Type') != 'not_defined')
        
    ## order reviews table transformation
    order_reviews = read_tables(spark, 'order_reviews')
    order_reviews_df = order_reviews.select(
            col('review_id').alias('Rev_ID'),
            col('order_id').alias('Ord_ID'),
            col('review_score').alias('Rev_Score'),
            col('review_comment_title').alias('Rev_Comment_Title'),
            col('review_comment_message').alias('Rev_Comment_Message'),
            col('review_creation_date').alias('Rev_Creation_Date'),
            col('review_answer_timestamp').alias('Rev_Answer_Timestamp')
        ).dropDuplicates(['Ord_ID']) \
        .filter(length(col('Rev_ID')) == 32)   \
        .filter(col('Rev_Score').between(1,5))\
        .filter(~col('Rev_Comment_Message').rlike(r'[^a-zA-Z0-9\s.,!?]'))\
        .filter(~col('Rev_Comment_Title').rlike(r'[^a-zA-Z0-9\s.,!?]'))\
        .filter(col('Rev_Creation_Date').rlike(r'^\d{4}-\d{2}-\d{2}')) 
    

    ## products table transformation
    products = read_tables(spark, 'products')
    products_df = products.select(
            col('product_id').alias('Prod_ID'),
            initcap(regexp_replace(col('product_category_name'), '_', ' ')).alias('Prod_Category_Name'),
            col('product_photos_qty').alias('Prod_Photos_Qty'),
            col('product_weight_g').alias('Prod_Weight_G'),
            col('product_length_cm').alias('Prod_Length_CM'),
            col('product_height_cm').alias('Prod_Height_CM'),
            col('product_width_cm').alias('Prod_Width_CM')
        )

    ## sellers table transformation
    sellers = read_tables(spark, 'sellers')
    sellers_df = sellers.select(
            col('seller_id').alias('Sell_ID'),
            col('seller_zip_code_prefix').alias('Sell_Zip_Code_Prefix'),
            initcap(col('seller_city')).alias('Sell_City'),
            col('seller_state').alias('Sell_State')
        )

    return {  
        'customers': customers_df,
        'orders': orders_df,
        'geolocation': geolocation_df,
        'order_items': order_items_df,
        'order_payments': order_payments_df,
        'order_reviews': order_reviews_df,
        'products': products_df,
        'sellers': sellers_df
    }

def load_to_postgres(df, table_name):
    '''Load DataFrame to PostgreSQL'''
    df.write.format('jdbc')\
        .option('url','jdbc:postgresql://ecommerce-postgres:5432/airflow')\
        .option('dbtable',f'"silver".{table_name}')\
        .option('user', 'airflow')\
        .option('password', 'airflow')\
        .option('driver', 'org.postgresql.Driver')\
        .mode('overwrite')\
        .save()

    print(f'✅ Loaded {table_name} to silver schema')

if __name__ == '__main__':
    spark = create_spark_session()
    
    try:
        # Transform all tables in ingestion layer
        transformed_tables = transform(spark)  
        # Load transformed tables to PostgreSQL
        for table_name, df in transformed_tables.items():
            load_to_postgres(df, table_name)
        
        print("✅ All tables successfully transformed and loaded to silver schema")
        
    except Exception as e:
        print(f" Error in transformation process: {str(e)}")
        raise e
    finally:
        spark.stop()