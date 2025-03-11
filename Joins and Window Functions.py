# Databricks notebook source
df = spark.read.format('csv')\
            .option("inferSchema", "true")\
                .option("header", "true")\
                    .load("/FileStore/tables/BigMart_Sales.csv")

# COMMAND ----------

display(df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC # JOINS

# COMMAND ----------

# MAGIC %md
# MAGIC ### a. INNER JOIN

# COMMAND ----------

data1 = [
    ('1', 'gaur', 'd01'),
    ('2', 'kit', 'd02'),
    ('3', 'sam', 'd03'),
    ('4', 'tim', 'd03'),
    ('5', 'aman', 'd05')
]
schema1 = 'emp_id STRING, emp_name STRING, dept_id STRING'
df1 = spark.createDataFrame(data1, schema1)

data2 = [
    ('HR', 'd01'),
    ('Marketing', 'd02'),
    ('Accounts', 'd03'),
    ('IT', 'd04'),
    ('Finance', 'd05')
]
schema2 = 'department STRING, dept_id STRING'
df2 = spark.createDataFrame(data2, schema2)

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

df1.join(df2, df1['dept_id'] == df2['dept_id'], 'inner').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### b. Left Join

# COMMAND ----------

data1 = [
    ('1', 'gaur', 'd01'),
    ('2', 'kit', 'd02'),
    ('3', 'sam', 'd03'),
    ('4', 'tim', 'd03'),
    ('5', 'aman', 'd05'),
    ('6', 'joy', 'd06') # added to show the left join still shows this record with null value for department name
]
schema1 = 'emp_id STRING, emp_name STRING, dept_id STRING'
df1 = spark.createDataFrame(data1, schema1)

data2 = [
    ('HR', 'd01'),
    ('Marketing', 'd02'),
    ('Accounts', 'd03'),
    ('IT', 'd04'),
    ('Finance', 'd05')
]
schema2 = 'dept_name STRING, dept_id STRING'
df2 = spark.createDataFrame(data2, schema2)

# COMMAND ----------

df1.join(df2, df1['dept_id'] == df2['dept_id'], 'left').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### c. Right Join
# MAGIC This join is exactly opposite to the left join

# COMMAND ----------

df1.join(df2, df1['dept_id'] == df2['dept_id'], 'right').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### d. Anti Join
# MAGIC Whenever we want to fetch data from one dataframe not available in another

# COMMAND ----------

df1.join(df2, df1['dept_id'] == df2['dept_id'], 'anti').display()

# COMMAND ----------

# MAGIC %md
# MAGIC # WINDOW FUNCTIONS

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, rank, dense_rank, percent_rank, ntile, cume_dist

# COMMAND ----------

# MAGIC %md
# MAGIC ### a. ROW_NUMBER()

# COMMAND ----------

df = df.withColumn('rowCol', row_number().over(Window.orderBy('Item_Identifier')))

# COMMAND ----------

df.select('Item_Identifier', 'rowCol').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### b. Rank()

# COMMAND ----------

df = df.withColumn(
    'rankCol',
    rank().over(Window.orderBy(col('Item_Identifier')))
)

# COMMAND ----------

df.select('Item_Identifier', 'rowCol', 'rankCol').display()

# COMMAND ----------

# MAGIC %md
# MAGIC Here, we can see that, the rank() window function numbers similar records the same and skips the sequence.

# COMMAND ----------

# MAGIC %md
# MAGIC ### c. Dense_Rank()

# COMMAND ----------

df = df.withColumn(
    'dense_rank_col',
    dense_rank().over(Window.orderBy(col('Item_Identifier')))
)

# COMMAND ----------

df.select('Item_Identifier', 'rowCol', 'rankCol', 'dense_rank_col').display()

# COMMAND ----------

# MAGIC %md
# MAGIC Here, we can see that, the dense_rank() window function numbers similar records the same and doesn't skip the sequence.

# COMMAND ----------

# MAGIC %md
# MAGIC ### d. cume_sum()

# COMMAND ----------

from pyspark.sql.functions import cast, sum
from pyspark.sql.types import DoubleType

# COMMAND ----------

df = df.withColumn('Item_MRP', df['Item_MRP'].cast(DoubleType()))

# COMMAND ----------

df.withColumn('CumSum', sum('Item_MRP').over(Window.orderBy('Item_Type'))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC Here, we can see that the result is the sum for every Item_Type - the total sum, not the moving sum.

# COMMAND ----------

df.withColumn('CumSum', sum('Item_MRP').over(Window.orderBy('Item_Type').rowsBetween(Window.unboundedPreceding, Window.currentRow))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC Here, we can see now that the result is the moving sum of the records for every Item_Type. This is possible by using the Frame clause inside of the over() method using the rowsBetween() method to specify the range of rows required.

# COMMAND ----------

df.withColumn('CumSum', sum('Item_MRP').over(Window.orderBy('Item_Type').rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC Here, using the upper range as unbounded following - the sum is the range between first and last record, no matter what the Item_Type is.

# COMMAND ----------

# MAGIC %md
# MAGIC # USER DEFINED FUNCTIONS (UDF)

# COMMAND ----------

def myFunc(x):
    return x * x
    
my_udf = udf(myFunc)

# COMMAND ----------

df.withColumn('myCol', my_udf('Item_MRP')).display()
