-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS cdc;
DROP TABLE IF EXISTS cdc.OrdersSilver;
DROP TABLE IF EXISTS cdc.OrdersGold;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS cdc.OrdersSilver(
  OrderID int,
  UnitPrice int,
  Quantity int,
  Customer string
)
USING DELTA
TBLPROPERTIES (delta.enableChangeDataFeed = true)
LOCATION 'gs://deongcp-data-bucket/schema_evolution/raw/OrdersSilver'

-- COMMAND ----------

describe history cdc.OrdersSilver

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS cdc.OrdersGold(
  OrderID int,
  OrdersTotal int,
  Customer string
)
using DELTA
LOCATION 'gs://deongcp-data-bucket/schema_evolution/raw/OrdersGold'

-- COMMAND ----------

describe history cdc.OrdersGold

-- COMMAND ----------

INSERT INTO cdc.OrdersSilver
SELECT 1 OrderID, 96 as UnitPrice, 5 as Quantity, "A" as Customer
UNION
SELECT 2 OrderID, 450 as UnitPrice, 10 as Quantity, "B" as Customer
UNION
SELECT 3 OrderID, 134 as UnitPrice, 7 as Quantity, "C" as Customer
UNION
SELECT 4 OrderID, 847 as UnitPrice, 8 as Quantity, "D" as Customer
UNION
SELECT 5 OrderID, 189 as UnitPrice, 15 as Quantity, "E" as Customer;
SELECT * FROM cdc.OrdersSilver

-- COMMAND ----------

INSERT INTO cdc.OrdersGold
SELECT OrderID, UnitPrice * Quantity AS OrderTotal, Customer FROM cdc.OrdersSilver;
SELECT * FROM cdc.OrdersGold

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Change Data Capture

-- COMMAND ----------

describe history cdc.OrdersSilver

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW latest_version as 
SELECT * 
  FROM
      (SELECT *, rank() over (PARTITION BY OrderID order by _commit_version desc) as rank
      from table_changes('cdc.OrdersSilver',2) 
      where _change_type != 'update_preimage')
  WHERE rank = 1

-- COMMAND ----------

select * from latest_version

-- COMMAND ----------

UPDATE cdc.OrdersSilver SET Quantity = 50 WHERE OrderID = 1;
DELETE FROM cdc.OrdersSilver WHERE Customer = 'D';
INSERT INTO cdc.OrdersSilver SELECT 6 OrderID, 100 as UnitPrice, 10 as Quantity, "F" as Customer;

-- COMMAND ----------

SELECT * FROM table_changes('cdc.OrdersSilver', 0) order by _commit_timestamp;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ALTER TABLE OrdersSilver SET TBLPROPERTIES (delta.enableChangeDataFeed=true)

-- COMMAND ----------

select * from latest_version order by _commit_timestamp;

-- COMMAND ----------

MERGE INTO cdc.OrdersGold og USING latest_version os ON og.OrderID = os.OrderID
WHEN MATCHED AND os._change_type='update_postimage' THEN UPDATE SET OrdersTotal = os.UnitPrice * os.Quantity
WHEN MATCHED AND os._change_type='delete' THEN DELETE
WHEN NOT MATCHED THEN INSERT (OrderID, OrdersTotal, Customer) VALUES (os.OrderID, os.UnitPrice * os.Quantity, os.Customer)

-- COMMAND ----------

select * from OrdersGold

-- COMMAND ----------

-- version as ints or longs e.g. changes from version 0 to 10
SELECT * FROM table_changes('OrdersSilver', 0, 10)

-- COMMAND ----------

-- timestamp as string formatted timestamps
SELECT * FROM table_changes('OrdersSilver', '2023-11-30 20:50:24', '2023-11-30 20:54:24')

-- COMMAND ----------

-- providing only the startingVersion/timestamp
SELECT * FROM table_changes('OrdersSilver', 0)

-- COMMAND ----------

-- database/schema names inside the string for table name, with backticks for escaping dots and special characters
SELECT * FROM table_changes('cdc.`OrdersSilver`', '2023-11-30 20:51:24' , '2023-11-30 20:54:24')

-- COMMAND ----------

-- path based tables
SELECT * FROM table_changes_by_path('gs://deongcp-data-bucket/schema_evolution/raw/OrdersSilver', '2023-11-30 00:00:00')

-- COMMAND ----------

DROP TABLE cdc.OrdersSilver;
DROP TABLE cdc.OrdersGold;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC https://www.mssqltips.com/sqlservertip/6781/real-time-iot-analytics-apache-sparks-structured-streaming-databricks-delta-lake/
-- MAGIC

-- COMMAND ----------


