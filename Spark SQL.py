# Databricks notebook source
# MAGIC %md
# MAGIC #### Reading the data

# COMMAND ----------

df = spark.read.format('csv')\
            .option("inferSchema", "true")\
                .option("header", "true")\
                    .load("/FileStore/tables/BigMart_Sales.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC **To use SQL in spark, we need to create a Temporary View out of the dataframe**

# COMMAND ----------

# MAGIC %md
# MAGIC ## TEMP_VIEW

# COMMAND ----------

df.createTempView('MyView')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM MyView WHERE Item_Fat_Content = 'LF'

# COMMAND ----------

# MAGIC %md
# MAGIC ### CREATING A DATAFRAME USING SQL

# COMMAND ----------

df_sql = spark.sql("SELECT * FROM MyView WHERE Item_Fat_Content = 'LF'")

# COMMAND ----------

display(df_sql)

# COMMAND ----------


