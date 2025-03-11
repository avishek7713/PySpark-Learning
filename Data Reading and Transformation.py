# Databricks notebook source
# MAGIC %md
# MAGIC # 1. DATA READING

# COMMAND ----------

# To view the Filestore ,,,,,, used to verify the path
dbutils.fs.ls("FileStore/tables")

# COMMAND ----------

# MAGIC %md
# MAGIC ### a. Reading csv file

# COMMAND ----------

# Reading the BigMart_Sales-1.csv file into a dataframe
df = spark.read.format('csv').\
            option('inferSchema', 'true').\
                option('header', 'true').\
                    load('/FileStore/tables/BigMart_Sales.csv') # loading the file from the filestore


# COMMAND ----------

# Displaying the dataframe
display(df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### b. Reading JSON file

# COMMAND ----------

# Reading the json file and storing into a dataframe
df_json = spark.read.format('json')\
                .option('inferSchema', 'true')\
                    .option('header', 'true')\
                        .option('multiline', 'false')\
                            .load('/FileStore/tables/drivers.json')

# COMMAND ----------

# displaying the dataframe
display(df_json.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. SCHEMA DEFINITION

# COMMAND ----------

# MAGIC %md
# MAGIC ### a. DDL Schema

# COMMAND ----------

df.printSchema() # checking the schema of the dataframe

# COMMAND ----------

# MAGIC %md
# MAGIC **Changing the schema of Item_Weight from DOUBLE to STRING**

# COMMAND ----------

ddl_schema = '''
    Item_Identifier STRING,
    Item_Weight STRING, 
    Item_Fat_Content STRING,
    Item_Visibility DOUBLE,
    Item_Type STRING,
    Item_MRP DOUBLE,
    Outlet_Identifier STRING,
    Outlet_Establishment_Year INT,
    Outlet_Size STRING,
    Outlet_Location_Type STRING,
    Outlet_Type STRING,
    Item_Outlet_Sales DOUBLE
'''

# COMMAND ----------

df_ddl_schema = spark.read.format('csv') \
                        .schema(ddl_schema) \
                            .option('header', 'true') \
                                .load('/FileStore/tables/BigMart_Sales-1.csv')

# COMMAND ----------

display(df_ddl_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC We can see that the schema type of Item_Weight column has been changed to String from Double

# COMMAND ----------

# MAGIC %md
# MAGIC ### b. StructType() Schema

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# COMMAND ----------

struct_schema = StructType([
    StructField('Item_Identifier', StringType(), False), 
    StructField('Item_Weight', DoubleType(), True),
    StructField('Item_Fat_Content', StringType(), True),
    StructField('Item_Visibility', StringType(), True),
    StructField('Item_MRP', StringType(), True),
    StructField('Outlet_Identifier', StringType(), False),
    StructField('Outlet_Establishment_Year', StringType(), True),
    StructField('Outlet_Size', StringType(), True),
    StructField('Outlet_Location_Type', StringType(), True),
    StructField('Outlet_Type', StringType(), True),
    StructField('Item_Outlet_Sales', StringType(), True)
])

# COMMAND ----------

df_struct = spark.read.format('csv') \
                        .schema(struct_schema) \
                            .option('header', 'true') \
                                .load('/FileStore/tables/BigMart_Sales-1.csv')

# COMMAND ----------

display(df_struct.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC We can see again that the schema of Item_Weight column has been changed to double

# COMMAND ----------

# MAGIC %md
# MAGIC # 3. SELECT

# COMMAND ----------

df_select = df.select('Item_Identifier', 'Item_Weight', 'Item_Fat_Content')
display(df_select.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC Can also be done using the col() method

# COMMAND ----------

from pyspark.sql.functions import col

df_select = df.select(col('Item_Identifier'), col('Item_Weight'), col('Item_Fat_Content'))
display(df_select.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC We can see that both the methods give the same output, but col() method is used often with other transformations, so widely used.

# COMMAND ----------

# MAGIC %md
# MAGIC # 4. ALIAS

# COMMAND ----------

# MAGIC %md
# MAGIC We can use alias() method to rename a column, here, we can only do it using the col() method. 
# MAGIC The alias() method changes the column name temporarily, specifically to select or transform the dataframe

# COMMAND ----------

df.select(col('Item_Identifier').alias('Item_ID')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # 5. Filter

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenario-**1** - Filter the data with Item_Fat_Content = Regular

# COMMAND ----------

df.filter(col('Item_Fat_Content') == 'Regular').display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenario-**2** -- Filter the data with Item_Type = Soft Drinks and Item_Weight < 10

# COMMAND ----------

df.filter((col('Item_Type') == "Soft Drinks") & (col('Item_Weight') < 10.0)).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Scenario-**3** - Fetch the data with Tier in (Tier 1 or Tier 2) of Outlet_Location_Type and Outlet_Size is Null

# COMMAND ----------

# using the isin() method to check if the type matches and isNull() method to filter the null values
df.filter((col('Outlet_Location_Type').isin('Tier 1', 'Tier 2')) & (col('Outlet_Size').isNull())).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # 6. withColumnRenamed()

# COMMAND ----------

# MAGIC %md
# MAGIC The withColumnRenamed() method permanently changes the name of the column and returns an updated dataframe

# COMMAND ----------

df_renamed = df.withColumnRenamed('Item_Weight', 'Item_Wt')
display(df_renamed)

# COMMAND ----------

# MAGIC %md
# MAGIC # 7. withColumn()

# COMMAND ----------

# MAGIC %md
# MAGIC The most important method used for data transformation.

# COMMAND ----------

# MAGIC %md
# MAGIC #### a. Scenario-1 ---- Creating a new column and assigning a constant value

# COMMAND ----------

from pyspark.sql.functions import lit

# creating a new column and assigning a constant value
df = df.withColumn('flag', lit('new'))
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### b. Scenario-2 ---- Creating a new column based on a transformation

# COMMAND ----------

# multiply Item_Wt and Item_MRP

df.withColumn('product', col('Item_Weight') * col('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### c. Scenario-3 ---- Modifying a column and replacing its value

# COMMAND ----------

from pyspark.sql.functions import regexp_replace, regexp_extract

df.withColumn('Item_Fat_Content', regexp_replace(col('Item_Fat_Content'), "Regular", "Reg"))\
    .withColumn('Item_Fat_Content', regexp_replace(col('Item_Fat_Content'), "Low Fat", "Lf")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # 8. Type Casting

# COMMAND ----------

df.withColumn('Item_Weight', col('Item_Weight').cast(StringType())).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # 9. sort (orderBy)

# COMMAND ----------

df.sort(col('Item_Weight').desc()).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### OR

# COMMAND ----------

df.sort(['Item_Weight', 'Item_Visibility'], ascending = [0, 1]).display() # 0 for false and 1 for true

# COMMAND ----------

# MAGIC %md
# MAGIC # 10. limit

# COMMAND ----------

display(df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC # 11. Drop

# COMMAND ----------

# MAGIC %md
# MAGIC Used to drop the columns

# COMMAND ----------

df.drop('Item_Weight', 'Item_Fat_Content').display()

# COMMAND ----------

# MAGIC %md
# MAGIC # 12. drop_duplicates

# COMMAND ----------

# MAGIC %md
# MAGIC **Dropping all the rows having duplicates in the entire df**

# COMMAND ----------

df.dropDuplicates().display() # also called d-dup

# COMMAND ----------

# MAGIC %md
# MAGIC ### OR

# COMMAND ----------

df.distinct().display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Dropping only the duplicates from a particular column**

# COMMAND ----------

df.drop_duplicates(subset= ['Item_Identifier']).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### OR

# COMMAND ----------

# MAGIC %md
# MAGIC # 13. union and union byname

# COMMAND ----------

# MAGIC %md
# MAGIC **Preparing Dataframes**

# COMMAND ----------

data1 = [
    ('1', 'kad'),
    ('2', 'sid')
]
schema1 = 'id STRING, name STRING'

df1 = spark.createDataFrame(data1, schema1)

data2 = [
    ('3', 'rahul'),
    ('4', 'jas')
]
schema2 = 'id STRING, name STRING'

df2 = spark.createDataFrame(data2, schema2)

# COMMAND ----------

df1.display()

# COMMAND ----------

df2.display()

# COMMAND ----------

df1.union(df2).display()

# COMMAND ----------

data1 = [
    ('kad', '1'),
    ('sid', '2')
]
schema1 = 'name STRING, id STRING'
df1 = spark.createDataFrame(data1, schema1)
df1.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **union**

# COMMAND ----------

df1.union(df2).display()

# COMMAND ----------

# MAGIC %md
# MAGIC We can see that the data is messed up by union - to solve the issue, we can use unionByName()

# COMMAND ----------

# MAGIC %md
# MAGIC **unionByName**

# COMMAND ----------

df1.unionByName(df2).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # 14. String Functions

# COMMAND ----------

from pyspark.sql.functions import initcap, upper, lower

# COMMAND ----------

# MAGIC %md
# MAGIC ### a. upper()

# COMMAND ----------

df.select(upper(col('Item_Type')).alias('UpperCase')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### b. lower()

# COMMAND ----------

df.select(lower(col('Item_Type')).alias('LowerCase')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### c. initcap()

# COMMAND ----------

df.select(initcap(col('Item_Type')).alias('InitialCapital')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # 15. Date Functions

# COMMAND ----------

from pyspark.sql.functions import current_date, date_add, date_sub, datediff, date_format, date_trunc

# COMMAND ----------

# MAGIC %md
# MAGIC ### a. current_date()

# COMMAND ----------

df = df.withColumn('curr_date', current_date())
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### b. date_add()

# COMMAND ----------

df = df.withColumn('week_after', date_add('curr_date', 7))
display(df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### c. date_sub()

# COMMAND ----------

df = df.withColumn("week_before", date_sub('curr_date', 7))
display(df.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC **It can also be done using the date_add() method**

# COMMAND ----------

df.withColumn('before', date_add('curr_date', -7)).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### d. datediff()

# COMMAND ----------

df.withColumn('days_passed', datediff('curr_date', 'week_before')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### e. date_format()

# COMMAND ----------

df.withColumn('week_before', date_format('week_before', 'dd-MM-yyyy')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # 16. Handling Nulls

# COMMAND ----------

# MAGIC %md
# MAGIC ### a. Dropping nulls

# COMMAND ----------

# MAGIC %md
# MAGIC **Dropping all records having null in all the columns**

# COMMAND ----------

df.dropna('all').display() 

# COMMAND ----------

# MAGIC %md
# MAGIC **Dropping any record having null in any column**

# COMMAND ----------

df.dropna('any').display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Dropping the records having nulls in a subset column**

# COMMAND ----------

df.dropna(subset = ['Outlet_Size']).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### b. Filling nulls

# COMMAND ----------

# MAGIC %md
# MAGIC **Replacing all null values with 0**

# COMMAND ----------

df.fillna(0).display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Replacing the null with "Not Available" in Outlet_Size column - using subset to define the column**

# COMMAND ----------

df.fillna('Not Available', subset= ['Outlet_Size']).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # 17. Split and Indexing

# COMMAND ----------

from pyspark.sql.functions import split

# COMMAND ----------

df.withColumn('Type', split(col("Outlet_Type"), " ")[1])\
    .withColumn('Outlet_Type', split(col("Outlet_Type"), " ")[0]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC Here, we can see that the Outlet_Type column has been split and a new column called "Type" has been created containing the second index item of the splitted list, and the Outlet_Type column only contains the first item.

# COMMAND ----------

# MAGIC %md
# MAGIC # 18. explode

# COMMAND ----------

from pyspark.sql.functions import explode

# COMMAND ----------

# MAGIC %md
# MAGIC It is used along with the split() method. The split method splits the records into items in a list, and explode() basically explodes every splitted item in a separate row with the same record but different item from the list.

# COMMAND ----------

df.withColumn('Outlet_Type', explode(split(col('Outlet_Type'), " "))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # 19. array_contains 

# COMMAND ----------

from pyspark.sql.functions import array_contains

# COMMAND ----------

# MAGIC %md
# MAGIC Works with the split column or with the list.

# COMMAND ----------

df.withColumn('Type1_flag', array_contains((split('Outlet_Type', " ")), "Type1")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # 20. Group_By

# COMMAND ----------

from pyspark.sql.functions import sum, avg

# COMMAND ----------

df.groupBy('Item_Type', 'Outlet_Size').agg((sum('Item_MRP').alias('Total_MRP')), avg('Item_MRP').alias('Avg_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # 21. collect_list

# COMMAND ----------

from pyspark.sql.functions import collect_list, collect_set

# COMMAND ----------

# MAGIC %md
# MAGIC Alternate to group_concat in sql

# COMMAND ----------

data = [
    ('user1', 'book1'),
    ('user1', 'book2'),
    ('user2', 'book2'),
    ('user2', 'book4'),
    ('user3', 'book1')
]

schema = 'user STRING, book STRING'
df_book = spark.createDataFrame(data, schema)
display(df_book)

# COMMAND ----------

df_book.groupBy('user').agg(collect_list('book')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC We can see that all the books holded by the users have been listed.

# COMMAND ----------

df_book.groupBy('book').agg(collect_list('user')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC Here, we can see that the users have been grouped per book.

# COMMAND ----------

# MAGIC %md
# MAGIC # 22. Pivot

# COMMAND ----------

df.groupBy('Item_Type').pivot('Outlet_Size').agg(avg('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # 23. WHEN/OTHERWISE

# COMMAND ----------

from pyspark.sql.functions import when

# COMMAND ----------

# MAGIC %md
# MAGIC ### a. Scenario-1

# COMMAND ----------

df = df.withColumn('veg_flag', when(col("Item_Type") == "Meat", "Non-Veg").otherwise('Veg')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### b. Scenario-2

# COMMAND ----------

df.withColumn
