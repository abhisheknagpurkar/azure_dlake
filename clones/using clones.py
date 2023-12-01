# Databricks notebook source
display(dbutils.fs.ls('dbfs:/databricks-datasets/nyctaxi/tables/'))
display(dbutils.fs.ls('dbfs:/databricks-datasets/nyctaxi/'))
display(dbutils.fs.ls('dbfs:/databricks-datasets/nyctaxi/tripdata/yellow/'))

# COMMAND ----------

df_trips = spark.read.csv('dbfs:/databricks-datasets/nyctaxi/tripdata/yellow/yellow_tripdata_2019-12.csv.gz', inferSchema=True, header=True)

# COMMAND ----------

display(df_trips)

# COMMAND ----------

# MAGIC %md
# MAGIC df = spark.table("delta.`/databricks-datasets/nyctaxi/tables/nyctaxi_yellow`")
# MAGIC display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC use default;

# COMMAND ----------

# MAGIC %md
# MAGIC select count(*) from nyctaxi;

# COMMAND ----------

df_trips.write.format('delta').mode('overwrite').saveAsTable('nyctrips')

# COMMAND ----------

# MAGIC %md
# MAGIC use catalog samples
# MAGIC use database nyctaxi

# COMMAND ----------

# MAGIC %sql
# MAGIC show table extended like 'trips'

# COMMAND ----------

# MAGIC %md
# MAGIC df.write.format('delta').mode('overwrite').save('gs://deongcp-data-bucket/clones/raw/delta/nyctaxi')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from trips

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists nyctaxi.trips_shallow_clone;
# MAGIC shallow clone nyctaxi.trips
# MAGIC location 'gs://deongcp-data-bucket/clones/raw/delta/trips_delta_Shallow_clone'

# COMMAND ----------


