# Databricks notebook source
# MAGIC %md 
# MAGIC
# MAGIC ### Data read JSON

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data bricks - Data Read

# COMMAND ----------

df_json = spark.read.format('json').option("infraSchema",True)\
         .option("header",True)\
         .option("inferSchema",True)\
         .load("/Volumes/workspace/learning_upload/learn_upload_file/drivers.json")

# COMMAND ----------

df_json.display()

# COMMAND ----------

# dbutils.fs.ls("/Volumes/workspace/learning_upload/learn_upload_file/")
dbutils.fs.ls("/Volumes/workspace/learning_upload/learn_upload_file/")

# COMMAND ----------

# spark.read.format('csv').option("header", "true").option("inferSchema", "true").load("/Volumes/workspace/learning_upload/learn_upload_file/").display()
spark.read.csv(
    "/Volumes/workspace/learning_upload/learn_upload_file/",
    header=True,
    inferSchema=True
).display()

# COMMAND ----------

df = spark.read.format('csv').option("header",True).option("inferSchema",True).load("/Volumes/workspace/learning_upload/learn_upload_file/BigMart Sales.csv")

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Schema - auto

# COMMAND ----------

df.printSchema()

# COMMAND ----------

my_ddl_schema = '''
                   Item_Identifier string,
                   Item_Weight DOUBLE,
                   Item_Fat_Content STRING, 
                   Item_Visibility DOUBLE,
                   Item_Type STRING,
                   Item_MRP DOUBLE,
                   Outlet_Identifier STRING,
                   Outlet_Establishment_Year INTEGER,      
                   Outlet_Size STRING,
                   Outlet_Location_Type STRING,
                   Outlet_Type STRING,
                   Item_Outlet_Sales DOUBLE   
                '''   
                   

# COMMAND ----------

# df = spark.read.format('csv')\
#     .schema(my_ddl_schema)\
#         .option("header",True)\
#         .load("/Volumes/workspace/learning_upload/learn_upload_file/BigMart Sales.csv")
df = spark.read.csv("/Volumes/workspace/learning_upload/learn_upload_file/BigMart Sales.csv",header=True,schema=my_ddl_schema)
df.printSchema()
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### StructType method

# COMMAND ----------

from pyspark.sql.types import*
from pyspark.sql.functions import*

# COMMAND ----------

my_structType_sch = StructType([
                         StructField('Item_Identifier', StringType(), True),
                         StructField('Item_Weight', FloatType(), True),
                         StructField('Item_Fat_Content', StringType(), True),
                         StructField('Item_Visibility', StringType(), True),
                         StructField('Item_Type', StringType(), True),
                         StructField('Item_MRP', StringType(), True),
                         StructField('Outlet_Identifier', StringType(), True),
                         StructField('Outlet_Establishment_Year', StringType(), True),
                         StructField('Outlet_Size', StringType(), True),
                         StructField('Outlet_Location_Type', StringType(), True),
                         StructField('Outlet_Type', StringType(), True),
                         StructField('Item_Outlet_Sales', StringType(), True)
                       ])


# COMMAND ----------

df_StructType = spark.read.format('csv')\
    .schema(my_structType_sch)\
        .option("header",True)\
        .load("/Volumes/workspace/learning_upload/learn_upload_file/BigMart Sales.csv")


# COMMAND ----------

df_StructType.printSchema()


# COMMAND ----------


df_StructType.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### SELECT

# COMMAND ----------

df.display()

# COMMAND ----------

from pyspark.sql.functions import *

df.select(col('Item_Identifier'),col('Item_Weight'),col('Item_Fat_Content')).display()


# COMMAND ----------

# MAGIC %md
# MAGIC ### alias

# COMMAND ----------

# #df.select(col('Item_Identifier').alias('Item_ID')).display()
# df.select(col('Item_MRP').alias('Item_rate')).show()

cols = [col(c) for c in df.columns if c!="Item_Type"]

cols = [col("Item_Type").alias("Item_Type")]+cols

df.select(*cols).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### FILETR/WHERE

# COMMAND ----------

#sceneario 1
df.filter(col('Item_Fat_Content') == 'Regular').display()
#scenario 2
# df.filter(col('Fat_Content') == 'Low Fat').display()'))

# COMMAND ----------

# # df.printSchema()
# from pyspark.sql.types import *

# schema = StructType([
#     StructField("Item_Weight", FloatType(), True)
# ])

# df = spark.read.csv(
#     "/Volumes/workspace/learning_upload/learn_upload_file/BigMart Sales.csv",
#     header=True,
#     schema=schema)
df.printSchema()
df.display()

# COMMAND ----------

#scenario 2

df.filter( (col('Item_Type') == 'Soft Drinks') & (col('Item_Weight') < 10)).display()

# COMMAND ----------

df.display()

# COMMAND ----------

# #scenario 3
# # df.filter( (col('Outlet_Location_Type').isin(['Tier 1','Tier 2'])) & (col('Outlet_Size').isNull())).display()
# from pyspark.sql.functions import round as rd
# df_new= df.withColumn("Item_Weight", rd(col('Item_Weight'),2))
# df_new.display()

# COMMAND ----------

df_new.display()

# COMMAND ----------

df.withColumnRenamed('Item_Weight','Item_Weight_new').display()

# COMMAND ----------

df.withColumnRenamed('Item_Weight','Item_Weight_New').display()

# COMMAND ----------

from pyspark.sql.functions import*
from pyspark.sql.types import*
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### create new column

# COMMAND ----------

df= df.withColumn('New_col_flag', lit('new_const_val'))

# COMMAND ----------

df.display()

# COMMAND ----------

df= df.withColumn('Multiplied_column',col('Item_Weight')*col('Item_MRP'))
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Modifying the column

# COMMAND ----------

# df.display()

df.withColumn('Item_Fat_Content', regexp_replace(col('Item_Fat_Content'),"Regular","Reg"))\
    .withColumn('Item_Fat_Content', regexp_replace(col('Item_Fat_Content'),"Low Fat","Lf")).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### type casting - cast()

# COMMAND ----------

df = df.withColumn('Item_Weight',col('Item_Weight').cast(StringType()))

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### sort/orderby

# COMMAND ----------

df.sort(col('Item_Weight').desc()).display()

# COMMAND ----------

df.sort(col('Item_Visibility').asc()).display()

# COMMAND ----------

df.sort('Item_Weight','Item_Visibility',ascending=[0,0]).display()

# COMMAND ----------

df.sort(['Item_Weight','Item_Visibility'],ascending=[0,1]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### limit

# COMMAND ----------

df.limit(9).display()

# COMMAND ----------

df.withColumn('Newcol',lit('x')).display()

# COMMAND ----------

df.drop('Newcol')
df.display()

# COMMAND ----------

df.drop('Newcol','Item_Visibility').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### drop duplicates

# COMMAND ----------

df.dropDuplicates().display()

# COMMAND ----------

df.dropDuplicates(['Item_Type']).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### creating data from using spark

# COMMAND ----------

data1 = [('1','Deek'),('2','Raji')]
schema1= 'id string, name string'
df1 = spark.createDataFrame(data=data1, schema = schema1)
df1.display()

# COMMAND ----------

data2 = [('sagar','3'),('Deepa','4')]
schema2= 'name string,id string'
df2 = spark.createDataFrame(data2, schema2)
df2.display()

# COMMAND ----------

df1.union(df2).display()
df1.union(df2).distinct().display()
df1.union(df2).distinct().count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### union by name

# COMMAND ----------

dff= df1.unionByName(df2).display()
df1.unionByName(df2).distinct().display()
df1.unionByName(df2).distinct().count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### string functions

# COMMAND ----------

df.select(initcap('Item_Type')).display()
df.select(upper('Item_Type')).display()
df.select(lower('Item_Type')).display()