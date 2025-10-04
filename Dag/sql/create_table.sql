CREATE TABLE IF NOT EXISTS SALES (
    Sales_ID SERIAL PRIMARY KEY ,
    Region VARCHAR(100),
    Country VARCHAR(50) ,
    Item_Type VARCHAR(30) ,
    Sales_Channel VARCHAR(10) ,
    Order_Priority VARCHAR(30),
    Order_Date DATE ,
    Order_ID BIGINT  ,
    Ship_Date DATE,
    Units_Sold DECIMAL(10,2),
    Unit_Price DECIMAL(10,2),
    Unit_Cost DECIMAL(10,2) ,
    Total_Revenue DECIMAL(10,2) ,
    Total_Cost DECIMAL(10,2) ,
    Total_Profit DECIMAL(10,2),
    Created_At TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)