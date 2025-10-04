from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import timedelta
from pyspark.sql.window import Window

def create_spark_session():
    return SparkSession.builder\
        .appName('Ecommerce Reporting Layer')\
        .config('spark.jars', '/opt/airflow/spark_jobs/postgresql-42.6.0.jar')\
        .config("spark.log.level", "WARN") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "20") \
        .config("spark.sql.autoBroadcastJoinThreshold", "52428800") \
        .config("spark.memory.fraction", "0.8") \
        .config("spark.memory.storageFraction", "0.3") \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.memory", "2g") \
        .config("spark.network.timeout", "800s") \
        .config("spark.sql.adaptive.logLevel", "WARN") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .getOrCreate()

def read_silver_table(spark, table_name):
    """Read silver tables using JDBC with error handling"""
    try:
        df = spark.read.format('jdbc') \
            .option('url', 'jdbc:postgresql://ecommerce-postgres:5432/airflow') \
            .option('dbtable', f'silver.{table_name}') \
            .option('user', 'airflow') \
            .option('password', 'airflow') \
            .option('driver', 'org.postgresql.Driver') \
            .load()
        print(f"✅ Successfully loaded {table_name} via JDBC")
        return df
    except Exception as e:
        print(f"❌ Failed to load silver.{table_name}: {e}")
        raise

def dimension_tables(spark):
    '''Create complete dimension tables following the specified data model'''
    
    # Load source data
    orders_df = read_silver_table(spark, 'orders')
    customers_df = read_silver_table(spark, 'customers')
    products_df = read_silver_table(spark, 'products')
    sellers_df = read_silver_table(spark, 'sellers')
    geolocation_df = read_silver_table(spark, 'geolocation')
    order_payments_df = read_silver_table(spark, 'order_payments')
    order_reviews_df = read_silver_table(spark, 'order_reviews')
    
    # Dim_Date (Critical for Time Intelligence) - Power BI Compatible
    date_range = orders_df.agg(
        min("Ord_Purchase_Time").alias("min_date"),
        max("Ord_Purchase_Time").alias("max_date")
    ).collect()[0]
    
    start_date = date_range['min_date'].date()
    end_date = date_range['max_date'].date()
    days_diff = (end_date - start_date).days + 1
    
    date_df = spark.range(days_diff).select(
        date_add(lit(start_date), col("id").cast("int")).alias("date")
    )
    
    dim_date = date_df.select(
        date_format(col("date"), "yyyyMMdd").cast("int").alias("Date_SK"),
        col("date").cast("date").alias("Date"),  # Ensure pure date type
        year("date").alias("Year"),
        month("date").alias("Month"),
        date_format("date", "MMMM").alias("MonthName"),        
        quarter("date").alias("Quarter"),
        concat(lit("Q"), quarter("date")).alias("QuarterName"),        
        weekofyear("date").alias("Week"),
        dayofmonth("date").alias("Day"), 
        date_format("date", "EEEE").alias("DayName"),
        # ISO week date for better Power BI compatibility  
        concat(year("date"), lit("-W"), lpad(weekofyear("date"), 2, "0")).alias("YearWeek"),
        # Month-Year for reporting
        date_format("date", "yyyy-MM").alias("YearMonth"),       
        when(dayofweek("date").isin(1, 7), True).otherwise(False).alias("IsWeekend"),
        # Add more business date flags
        when(dayofweek("date") == 2, True).otherwise(False).alias("IsMonday"),
        when(dayofweek("date").isin(2, 3, 4, 5, 6), True).otherwise(False).alias("IsWeekday"),
        lit(False).alias("IsHoliday"),
        lit("").alias("HolidayName"),
        # Add fiscal year support (assuming calendar year = fiscal year)
        year("date").alias("FiscalYear"),
        quarter("date").alias("FiscalQuarter")
    ).distinct()  # Ensure no duplicate dates

    # Dim_Time (Hour-level granularity for time-based analysis)
    time_df = spark.range(24).select(
        col("id").cast("int").alias("hour")
    )
    
    dim_time = time_df.select(
        col("hour").alias("Time_SK"),  # 0-23 as surrogate key
        col("hour").alias("Hour_24"),  # 24-hour format
        when(col("hour") == 0, 12)
        .when(col("hour") <= 12, col("hour"))
        .otherwise(col("hour") - 12).alias("Hour_12"),  # 12-hour format
        when(col("hour") < 12, "AM").otherwise("PM").alias("AM_PM"),
        # Time periods for business analysis
        when(col("hour").between(6, 11), "Morning")
        .when(col("hour").between(12, 17), "Afternoon") 
        .when(col("hour").between(18, 21), "Evening")
        .otherwise("Night").alias("Time_Period"),
        # Business hours classification
        when(col("hour").between(9, 17), True).otherwise(False).alias("Is_Business_Hours"),
        when(col("hour").between(18, 22), True).otherwise(False).alias("Is_Peak_Shopping"),
        # Formatted display
        concat(
            when(col("hour") == 0, "12")
            .when(col("hour") <= 12, format_string("%02d", col("hour")))
            .otherwise(format_string("%02d", col("hour") - 12)),
            lit(":00 "),
            when(col("hour") < 12, "AM").otherwise("PM")
        ).alias("Time_Display")
    )

    # Dim_Customers (Customer Geography, Customer Segments)
    dim_customers = customers_df.select(
        monotonically_increasing_id().alias('Customer_SK'),
        col('Cus_ID').alias('Customer_ID'),  # Natural Key
        col('Cus_Unique_ID').alias('Customer_Unique_ID'),
        col('Cus_Zip_Code_Prefix').alias('Customer_Zip_Code'),
        col('Cus_City').alias('Customer_City'),
        col('customer_state').alias('Customer_State'),
        # Add customer segmentation logic
        when(col('Cus_Zip_Code_Prefix').between(1000, 19999), "Southeast")
        .when(col('Cus_Zip_Code_Prefix').between(20000, 39999), "Northeast") 
        .when(col('Cus_Zip_Code_Prefix').between(40000, 69999), "Southeast")
        .when(col('Cus_Zip_Code_Prefix').between(70000, 99999), "Other")
        .otherwise("Unknown").alias('Customer_Region')
    )

    # Dim_Products (Product Categories, Dimensions, Weights)
    dim_products = products_df.select(
        monotonically_increasing_id().alias('Product_SK'),
        col('Prod_ID').alias('Product_ID'),  # Natural Key
        col('Prod_Category_Name').alias('Product_Category'),
        col('Prod_Weight_G').alias('Product_Weight_G'),
        col('Prod_Height_CM').alias('Product_Height_CM'),
        col('Prod_Width_CM').alias('Product_Width_CM'),
        col('Prod_Length_CM').alias('Product_Length_CM'),
        # Add product classification
        when(col('Prod_Weight_G') < 500, "Light")
        .when(col('Prod_Weight_G') < 2000, "Medium")
        .when(col('Prod_Weight_G') >= 2000, "Heavy")
        .otherwise("Unknown").alias('Product_Weight_Category'),
        # Volume calculation
        (col('Prod_Height_CM') * col('Prod_Width_CM') * col('Prod_Length_CM')).alias('Product_Volume_CM3')
    )

    # Dim_Sellers (Seller Locations, Performance Tiers)
    dim_sellers = sellers_df.select(
        monotonically_increasing_id().alias('Seller_SK'),
        col('Sell_ID').alias('Seller_ID'),  # Natural Key
        col('Sell_Zip_Code_Prefix').alias('Seller_Zip_Code'),
        col('Sell_City').alias('Seller_City'),
        col('Sell_State').alias('Seller_State'),
        # Add seller region classification
        when(col('Sell_Zip_Code_Prefix').between(1000, 19999), "Southeast")
        .when(col('Sell_Zip_Code_Prefix').between(20000, 39999), "Northeast") 
        .when(col('Sell_Zip_Code_Prefix').between(40000, 69999), "Southeast")
        .when(col('Sell_Zip_Code_Prefix').between(70000, 99999), "Other")
        .otherwise("Unknown").alias('Seller_Region')
    )

    # Dim_Geography (Zip codes, Cities, States, Coordinates)
    dim_geography = geolocation_df.select(
        monotonically_increasing_id().alias('Geography_SK'), 
        col('Geo_Zip_Code_Prefix').alias('Zip_Code'),
        col('Geo_City').alias('City'),
        col('Geo_State').alias('State'),
        col('Geo_Lat').alias('Latitude'),
        col('Geo_Lng').alias('Longitude'),
        # Add geographic classifications
        when(col('Geo_State').isin('SP', 'RJ', 'MG', 'ES'), "Southeast")
        .when(col('Geo_State').isin('BA', 'SE', 'PE', 'AL', 'PB', 'RN', 'CE', 'PI', 'MA'), "Northeast")
        .when(col('Geo_State').isin('PR', 'SC', 'RS'), "South")
        .when(col('Geo_State').isin('GO', 'DF', 'MT', 'MS'), "Center-West")
        .when(col('Geo_State').isin('AM', 'RR', 'AP', 'PA', 'TO', 'RO', 'AC'), "North")
        .otherwise("Unknown").alias('Region')
    ).dropDuplicates(['Zip_Code'])

    # Dim_Order_Status (delivered, shipped, canceled, etc.)
    dim_order_status = orders_df.select(
        col('Ord_Status').alias('Order_Status')
    ).distinct().select(
        monotonically_increasing_id().alias('Order_Status_SK'),
        col('Order_Status'),
        # Add status category
        when(col('Order_Status').isin('delivered'), "Completed")
        .when(col('Order_Status').isin('shipped', 'processing'), "In Progress")
        .when(col('Order_Status').isin('canceled', 'unavailable'), "Failed")
        .otherwise("Other").alias('Status_Category')
    )

    # Dim_Payment_Types (credit_card, boleto, voucher, etc.)
    dim_payment_types = order_payments_df.select(
        col('Payment_Type')
    ).distinct().select(
        monotonically_increasing_id().alias('Payment_Type_SK'),
        col('Payment_Type'),
        # Add payment category
        when(col('Payment_Type').isin('credit_card', 'debit_card'), "Card")
        .when(col('Payment_Type').isin('boleto'), "Bank Transfer")
        .when(col('Payment_Type').isin('voucher'), "Voucher")
        .otherwise("Other").alias('Payment_Category')
    )

    # Dim_Review_Scores (1-5 rating scale with descriptions)
    dim_review_scores = order_reviews_df.select(
        col('Rev_Score').cast("int").alias('Review_Score')
    ).distinct().select(
        monotonically_increasing_id().alias('Review_Score_SK'),
        col('Review_Score'),
        # Add score descriptions
        when(col('Review_Score') == 1, "Very Poor")
        .when(col('Review_Score') == 2, "Poor")
        .when(col('Review_Score') == 3, "Average")
        .when(col('Review_Score') == 4, "Good")
        .when(col('Review_Score') == 5, "Excellent")
        .otherwise("Unknown").alias('Score_Description'),
        # Add score category
        when(col('Review_Score').isin(1, 2), "Negative")
        .when(col('Review_Score') == 3, "Neutral")
        .when(col('Review_Score').isin(4, 5), "Positive")
        .otherwise("Unknown").alias('Score_Category')
    )
    
    return {
        'dim_date': dim_date,
        'dim_time': dim_time,
        'dim_customers': dim_customers,
        'dim_products': dim_products,
        'dim_sellers': dim_sellers,
        'dim_geography': dim_geography,
        'dim_order_status': dim_order_status,
        'dim_payment_types': dim_payment_types,
        'dim_review_scores': dim_review_scores
    }

def create_fact_sales(spark):
    """Create Fact_Sales (Grain: Order Item Level) - Sales Amount, Freight Value, Quantity"""
    try:
        print("Creating fact_sales table at order item grain level...")
        
        # Load source tables
        order_items_df = read_silver_table(spark, "order_items")
        orders_df = read_silver_table(spark, "orders") 
        order_payments_df = read_silver_table(spark, "order_payments")
        
        # Aggregate payments by order to get total payment per order
        payments_agg = order_payments_df.groupBy("Ord_ID").agg(
            sum("Payment_Value").alias("Order_Payment_Value"),
            first("Payment_Type").alias("Primary_Payment_Type")
        )
        
        # Create fact sales with business keys for joins
        fact_sales = order_items_df.alias("oi") \
            .join(orders_df.alias("o"), "Ord_ID") \
            .join(payments_agg.alias("p"), "Ord_ID") \
            .select(
                # Surrogate key
                monotonically_increasing_id().alias("Sales_SK"),
                # Natural keys for dimension joins
                col("oi.Ord_ID").alias("Order_ID"),
                col("oi.Ord_Item_ID").alias("Order_Item_ID"), 
                col("oi.Prod_ID").alias("Product_ID"),
                col("o.Cus_ID").alias("Customer_ID"),
                col("oi.Sell_ID").alias("Seller_ID"),
                col("p.Primary_Payment_Type").alias("Payment_Type"),
                col("o.Ord_Status").alias("Order_Status"),
                date_format(col("o.Ord_Purchase_Time"), "yyyyMMdd").cast("int").alias("Order_Date_SK"),
                hour(col("o.Ord_Purchase_Time")).alias("Order_Time_SK"),  # Hour for time dimension
                # Measures
                col("oi.Price").alias("Sales_Amount"),
                col("oi.Freight_Value").alias("Freight_Value"),
                lit(1).alias("Quantity"),  # Assuming 1 item per order item
                col("p.Order_Payment_Value").alias("Order_Payment_Value"),
                # Timestamps
                col("o.Ord_Purchase_Time").alias("Order_Timestamp"),
                current_timestamp().alias("Load_Timestamp")
            )
        
        print("Fact sales table created successfully!")
        return fact_sales
        
    except Exception as e:
        print(f"💥 Error creating fact_sales: {str(e)}")
        raise e

def create_fact_orders(spark):
    """Create Fact_Orders (Grain: Order Level) - Order Status, Payment Metrics, Delivery Timelines"""
    try:
        print("Creating fact_orders table at order grain level...")
        
        orders_df = read_silver_table(spark, "orders")
        order_payments_df = read_silver_table(spark, "order_payments")
        order_items_df = read_silver_table(spark, "order_items")
        
        # Aggregate payments by order
        payments_agg = order_payments_df.groupBy("Ord_ID").agg(
            sum("Payment_Value").alias("Total_Payment_Value"),
            sum("Payment_Installments").alias("Total_Installments"),
            count("Payment_Type").alias("Payment_Methods_Count"),
            first("Payment_Type").alias("Primary_Payment_Type")
        )
        
        # Aggregate order items by order
        items_agg = order_items_df.groupBy("Ord_ID").agg(
            count("Ord_Item_ID").alias("Items_Count"),
            sum("Price").alias("Items_Total_Value"),
            sum("Freight_Value").alias("Total_Freight_Value")
        )
        
        # Create fact orders
        fact_orders = orders_df.alias("o") \
            .join(payments_agg.alias("p"), "Ord_ID") \
            .join(items_agg.alias("i"), "Ord_ID") \
            .select(
                # Surrogate key
                monotonically_increasing_id().alias("Order_SK"),
                # Natural keys for dimension joins
                col("o.Ord_ID").alias("Order_ID"),
                col("o.Cus_ID").alias("Customer_ID"),
                col("p.Primary_Payment_Type").alias("Payment_Type"),
                col("o.Ord_Status").alias("Order_Status"),
                date_format(col("o.Ord_Purchase_Time"), "yyyyMMdd").cast("int").alias("Order_Date_SK"),
                hour(col("o.Ord_Purchase_Time")).alias("Order_Time_SK"),  # Hour for time dimension
                # Order metrics
                col("i.Items_Count").alias("Order_Items_Count"),
                col("i.Items_Total_Value").alias("Order_Items_Value"),
                col("i.Total_Freight_Value").alias("Order_Freight_Value"),
                # Payment metrics
                col("p.Total_Payment_Value").alias("Total_Payment_Value"),
                col("p.Total_Installments").alias("Total_Installments"), 
                col("p.Payment_Methods_Count").alias("Payment_Methods_Count"),
                # Delivery timeline metrics
                col("o.Ord_Purchase_Time").alias("Order_Timestamp"),
                col("o.Ord_Approved_At").alias("Approved_Timestamp"),
                col("o.Ord_Delivered_Carrier_Date").alias("Carrier_Delivery_Date"),
                col("o.Ord_Delivered_Customer_Date").alias("Customer_Delivery_Date"),
                col("o.Ord_Estimated_Delivery_Date").alias("Estimated_Delivery_Date"),
                # Calculate delivery time metrics
                datediff(col("o.Ord_Approved_At"), col("o.Ord_Purchase_Time")).alias("Approval_Days"),
                datediff(col("o.Ord_Delivered_Customer_Date"), col("o.Ord_Purchase_Time")).alias("Total_Delivery_Days"),
                # Load timestamp
                current_timestamp().alias("Load_Timestamp")
            )
        
        print("Fact orders table created successfully!")
        return fact_orders
        
    except Exception as e:
        print(f"💥 Error creating fact_orders: {str(e)}")
        raise e

def create_fact_reviews(spark):
    """Create Fact_Reviews (Grain: Review Level) - Review Scores, Comment Analysis"""
    try:
        print("Creating fact_reviews table at review grain level...")
        
        order_reviews_df = read_silver_table(spark, "order_reviews")
        orders_df = read_silver_table(spark, "orders")
        
        # Create fact reviews
        fact_reviews = order_reviews_df.alias("r") \
            .join(orders_df.alias("o"), "Ord_ID") \
            .select(
                # Surrogate key
                monotonically_increasing_id().alias("Review_SK"),
                # Natural keys for dimension joins
                col("r.Rev_ID").alias("Review_ID"),
                col("r.Ord_ID").alias("Order_ID"),
                col("o.Cus_ID").alias("Customer_ID"),
                col("r.Rev_Score").cast("int").alias("Review_Score"),
                date_format(col("r.Rev_Creation_Date"), "yyyyMMdd").cast("int").alias("Review_Date_SK"),
                hour(col("r.Rev_Creation_Date")).alias("Review_Time_SK"),  # Hour for time dimension
                # Review measures
                col("r.Rev_Comment_Title").alias("Review_Title"),
                col("r.Rev_Comment_Message").alias("Review_Message"),
                # Comment analysis metrics
                length(col("r.Rev_Comment_Message")).alias("Comment_Length"),
                when(col("r.Rev_Comment_Message").isNull() | (col("r.Rev_Comment_Message") == ""), 0)
                .otherwise(1).alias("Has_Comment"),
                when(col("r.Rev_Comment_Title").isNull() | (col("r.Rev_Comment_Title") == ""), 0)
                .otherwise(1).alias("Has_Title"),
                # Timestamps
                col("r.Rev_Creation_Date").alias("Review_Creation_Date"),
                col("r.Rev_Answer_Timestamp").alias("Review_Answer_Date"),
                # Calculate response time
                datediff(col("r.Rev_Answer_Timestamp"), col("r.Rev_Creation_Date")).alias("Response_Days"),
                # Load timestamp
                current_timestamp().alias("Load_Timestamp")
            )
        
        print("Fact reviews table created successfully!")
        return fact_reviews
        
    except Exception as e:
        print(f"💥 Error creating fact_reviews: {str(e)}")
        raise e

def write_table_with_retry(df, table_name, max_retries=3):
    """Write table with retry logic and smaller batch size"""
    for attempt in range(max_retries):
        try:
            df.write.format('jdbc') \
                .option('url','jdbc:postgresql://ecommerce-postgres:5432/airflow') \
                .option('dbtable', f'"gold".{table_name}') \
                .option('user', 'airflow') \
                .option('password', 'airflow') \
                .option('driver', 'org.postgresql.Driver') \
                .option('batchsize', '1000') \
                .option('isolationLevel', 'NONE') \
                .mode('overwrite') \
                .save()
            print(f"✅ Successfully wrote {table_name}")
            return True
        except Exception as e:
            print(f"❌ Attempt {attempt + 1} failed for {table_name}: {str(e)}")
            if attempt == max_retries - 1:
                raise e
            continue

def create_reporting_tables(spark):
    '''Main function to create complete data warehouse'''
    
    print("Creating dimension tables...")
    dimensions = dimension_tables(spark)
    
    # Write dimensions first
    print("Writing dimension tables...")
    for dim_name, dim_df in dimensions.items():
        print(f"📊 Writing {dim_name}...")
        write_table_with_retry(dim_df, dim_name)
    
    # Force garbage collection
    spark.sparkContext._jvm.System.gc()
    
    # Create and write fact tables
    print("Creating and writing fact_sales...")
    fact_sales = create_fact_sales(spark)
    write_table_with_retry(fact_sales, 'fact_sales')
    
    spark.sparkContext._jvm.System.gc()
    
    print("Creating and writing fact_orders...")  
    fact_orders = create_fact_orders(spark)
    write_table_with_retry(fact_orders, 'fact_orders')
    
    spark.sparkContext._jvm.System.gc()
    
    print("Creating and writing fact_reviews...")
    fact_reviews = create_fact_reviews(spark)
    write_table_with_retry(fact_reviews, 'fact_reviews')
    
    return True

# Main execution with error handling
if __name__ == '__main__':
    spark = None
    try:
        spark = create_spark_session()
        print("Starting gold layer creation...")
        
        success = create_reporting_tables(spark)
        
        if success:
            print("🎉 Successfully created complete data warehouse in gold layer!")
            print("📌 Created 9 dimension tables (including separate date and time) and 3 fact tables")
            print("📌 All tables follow proper star schema with natural keys for joins")
            print("📊 Ready for business intelligence and analytics!")
            print("⏰ Time dimension enables hour-level analysis for customer behavior patterns")
        
    except Exception as e:
        print(f"💥 Error in gold layer creation: {str(e)}")
        raise e
        
    finally:
        if spark:
            spark.stop()
            print("Spark session stopped.")