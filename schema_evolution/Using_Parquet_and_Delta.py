# Databricks notebook source
df1 = spark.createDataFrame([(100, 2019), (101, 2019)], ['newCol1', 'Year'])
display(df1)

# COMMAND ----------

parquetpath = "gs://deongcp-data-bucket/schema_evolution/raw/parquet"

# Write the data frame to the specified parquet path and show the contents of the parquet file
(
    df1.write.mode('overwrite').format('parquet').save(parquetpath)
)
spark.read.parquet(parquetpath).show()

# COMMAND ----------

df2 = spark.createDataFrame([(200, 300), (201, 301)], ['newCol2', 'newCol3'])
display(df2)

# COMMAND ----------

df2.write.mode('append').parquet(parquetpath)

# COMMAND ----------

spark.read.parquet(parquetpath).show()

# COMMAND ----------

# MAGIC %md
# MAGIC spark.read.option('badRecordsPath', 'gs://deongcp-data-bucket/schema_evolution/raw/parquet/badRecords').parquet(parquetpath).show()

# COMMAND ----------

# MAGIC %md
# MAGIC spark.read.option('mode', 'PERMISSIVE').parquet(parquetpath).show()

# COMMAND ----------

# MAGIC %md
# MAGIC spark.read.option('mode', 'DROPMALFORMED').parquet(parquetpath).show()

# COMMAND ----------

# MAGIC %md
# MAGIC spark.read.option('mode', 'FAILFAST').parquet(parquetpath).show()

# COMMAND ----------

# MAGIC %md
# MAGIC df = spark.read.parquet('gs://deongcp-data-bucket/schema_evolution/raw/parquet', enforceSchema=True, columnNameOfCorruptRecord="CORRUPTED")
# MAGIC df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Using Delta Format

# COMMAND ----------

deltapath = 'gs://deongcp-data-bucket/schema_evolution/raw/deltaformat'

# COMMAND ----------

df1.write.mode('overwrite').format('delta').save(deltapath)

# COMMAND ----------

spark.read.format('delta').load(deltapath).show()

# COMMAND ----------

df2.write.format('delta').mode('append').option('mergeSchema', 'true').save(deltapath)

# COMMAND ----------

spark.read.format('delta').load(deltapath).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### mergeSchema is not supported when table access control is enabled (as it elevates a request that requires MODIFY to one that requires ALL PRIVILEGES) and mergeSchema cannot be used with INSERT INTO or .write.insertInto().

# COMMAND ----------

df3 = spark.createDataFrame([(102, 302), (103, 303)], ['newCol4', 'newCol5'])
display(df3)

# COMMAND ----------

df3.write.format('delta').mode('overwrite').option('mergeSchema', 'true').save(deltapath)
spark.read.format('delta').load(deltapath).show()

# COMMAND ----------

df4 = spark.createDataFrame([(104, 304), (105, 305)], ['newCol5', 'newCol7'])
display(df4)

# COMMAND ----------

df4.write.format('delta').option('overwriteSchema', 'true').mode('overwrite').save(deltapath)
spark.read.format('delta').load(deltapath).show()

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history 'gs://deongcp-data-bucket/schema_evolution/raw/deltaformat'

# COMMAND ----------

spark.read.format('delta').option('versionAsOF', '').load(deltapath).show()

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables

# COMMAND ----------

from delta.tables import *
deltaTable = DeltaTable.forPath(spark, 'gs://deongcp-data-bucket/schema_evolution/raw/deltaformat')
deltaTable.restoreToVersion(2)

# COMMAND ----------

spark.read.format('delta').load(deltapath).show()

# COMMAND ----------


