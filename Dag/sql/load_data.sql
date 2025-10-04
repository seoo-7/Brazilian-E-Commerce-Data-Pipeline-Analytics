TRUNCATE TABLE sales;
TRUNCATE TABLE sales;
COPY sales (
    region, 
    country, 
    item_type, 
    sales_channel, 
    order_priority, 
    order_date, 
    order_id, 
    ship_date, 
    units_sold, 
    unit_price, 
    unit_cost, 
    total_revenue, 
    total_cost, 
    total_profit
) 
FROM '/data/100 Sales Records.csv' DELIMITER ',' CSV HEADER;