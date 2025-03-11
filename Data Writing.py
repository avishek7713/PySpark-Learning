# Databricks notebook source
# reading the previous data

df = spark.read.format('csv')\
            .option("inferSchema", "true")\
                .option("header", "true")\
                    .load("/FileStore/tables/BigMart_Sales.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ### CSV

# COMMAND ----------

# APPEND
df.write.format('csv')\
        .mode('append')\
            .save('/FileStore/tables/CSV/data.csv')

# COMMAND ----------

# OVERWRITE

df.write.format('csv')\
        .mode('overwrite')\
            .save('/FileStore/tables/CSV/data.csv')


# COMMAND ----------

# ERROR

df.write.format('csv')\
        .mode('error')\
            .save('/FileStore/tables/CSV/data.csv')

# COMMAND ----------

# IGNORE

df.write.format('csv')\
        .mode('ignore')\
            .save('/FileStore/tables/CSV/data.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC ### PARQUET

# COMMAND ----------

df.write.format('parquet')\
        .mode('append')\
            .save('/FileStore/tables/PARQUET/data.parquet')

# COMMAND ----------

# MAGIC %md
# MAGIC ### DELTA

# COMMAND ----------

df.write.format('delta')\
        .mode('append')\
            .save('/FileStore/tables/DELTA/data.delta')

# COMMAND ----------

# MAGIC %md
# MAGIC Uses the Transactional logs to store the metadata know as Delta Logs.

# COMMAND ----------

# MAGIC %md
# MAGIC ### CREATING TABLE

# COMMAND ----------

df.write.format('parquet')\
        .mode('append')\
            .saveAsTable('my_table')

# COMMAND ----------

# MAGIC %md
# MAGIC The table is created at default location in databricks which is managed by databricks itself- called Managed Table. When **deleting the table - the data is also deleted.**

# COMMAND ----------

# MAGIC %md
# MAGIC #### External Table
# MAGIC The table is stored at a user given location and managed by the user. When **deleting the table - only the schema is deleted - not the data.**
