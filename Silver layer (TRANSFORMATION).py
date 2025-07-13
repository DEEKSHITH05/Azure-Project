# Databricks notebook source
# MAGIC %md
# MAGIC # SILVER LAYER
# MAGIC ### TRANSFORMATION

# COMMAND ----------

# MAGIC %md
# MAGIC #### Data access - Set up using Service APP

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.advwstoragedatalake.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.advwstoragedatalake.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.advwstoragedatalake.dfs.core.windows.net", "748b4dff-058f-4bdc-9f1c-2bb0b0fdc618")
spark.conf.set("fs.azure.account.oauth2.client.secret.advwstoragedatalake.dfs.core.windows.net","T-08Q~qfwfWvFocQMwQmmICYKowh5qksp45bUbe_")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.advwstoragedatalake.dfs.core.windows.net", "https://login.microsoftonline.com/68a8c9a5-324a-4760-8a90-b12ffa50209c/oauth2/token")

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Data Loading

# COMMAND ----------

df_cal = spark.read.format("csv").option("header",True).option("inferSchema",True).load('abfss://bronze@advwstoragedatalake.dfs.core.windows.net/AdventureWorks_Calendar/')

df_cus = spark.read.format("csv").option("header",True).option("inferSchema",True).load('abfss://bronze@advwstoragedatalake.dfs.core.windows.net/AdventureWorks_Customers/')

df_cus = spark.read.format("csv").option("header",True).option("inferSchema",True).load('abfss://bronze@advwstoragedatalake.dfs.core.windows.net/AdventureWorks_Customers/')

df_prod_cat = spark.read.format("csv").option("header",True).option("inferSchema",True).load('abfss://bronze@advwstoragedatalake.dfs.core.windows.net/AdventureWorks_Product_Categories/')

df_prodts = spark.read.format("csv").option("header",True).option("inferSchema",True).load('abfss://bronze@advwstoragedatalake.dfs.core.windows.net/AdventureWorks_Products/')

df_Adwretun = spark.read.format("csv").option("header",True).option("inferSchema",True).load('abfss://bronze@advwstoragedatalake.dfs.core.windows.net/AdventureWorks_Returns/')

df_adwsales =spark.read.format("csv").option("header",True).option("inferSchema",True).load('abfss://bronze@advwstoragedatalake.dfs.core.windows.net/AdventureWorks_Sales*/')

#df_adwsales_2016 =spark.read.format("csv").option("header",True).option("inferSchema",True).load('abfss://bronze@advwstoragedatalake.dfs.core.windows.net/AdventureWorks_Sales_2016/')

#df_adwsales_2017 =spark.read.format("csv").option("header",True).option("inferSchema",True).load('abfss://bronze@advwstoragedatalake.dfs.core.windows.net/AdventureWorks_Sales_2017/')

df_adwteritory = spark.read.format("csv").option("header",True).option("inferSchema",True).load('abfss://bronze@advwstoragedatalake.dfs.core.windows.net/AdventureWorks_Territories/')

df_prodsubcat = spark.read.format("csv").option("header",True).option("inferSchema",True).load('abfss://bronze@advwstoragedatalake.dfs.core.windows.net/Product_Subcategories/')


# COMMAND ----------

df_cus = spark.read.format("csv").option("header",True).option("inferSchema",True).load('abfss://bronze@advwstoragedatalake.dfs.core.windows.net/AdventureWorks_Customers/')


# COMMAND ----------

#df_adwsales.tail(10).display()

display(df_adwsales.tail(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transaformation starts!!! calender data

# COMMAND ----------

df_cal.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation begins - df_cal

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df_cal = df_cal.withColumn('Month', month(col('Date')))
df_cal = df_cal.withColumn('Year', year(col('Date')))

df_cal.display()    

# COMMAND ----------

df_cal.withColumn('day', weekday(col('Date'))).display()

# COMMAND ----------

df_cal.write.format('parquet').mode('overwrite').save('abfss://silver@advwstoragedatalake.dfs.core.windows.net/AdventureWorks_Calendar/')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation - Customer

# COMMAND ----------

df_cus.display()

# COMMAND ----------

df_cus.withColumn('fullname',concat(col('Prefix'),lit(' '),col('FirstName'),lit(' '),col('LastName'))).display()

# COMMAND ----------


df_cus = df_cus.withColumn('fullName', initcap(concat_ws(' ', col('Prefix'), col('FirstName'), col('LastName'))))


# COMMAND ----------

df_cus.display()

# COMMAND ----------

df_cus.write.format('parquet').mode('overwrite').save('abfss://silver@advwstoragedatalake.dfs.core.windows.net/AdventureWorks_Customers/')

# COMMAND ----------

# MAGIC %md
# MAGIC ###Transaformation product categories

# COMMAND ----------

df_prod_cat.display()

# COMMAND ----------

df_prod_cat.write.format('parquet').mode('overwrite').save('abfss://silver@advwstoragedatalake.dfs.core.windows.net/AdventureWorks_Product_Categories/')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation Products 

# COMMAND ----------

df_prodts.display()

# COMMAND ----------



# COMMAND ----------

df_prodts = df_prodts.withColumn('ProductSKU',split(col('ProductSKU'),'-')[0])\
    .withColumn('ProductName',split(col('ProductName'),' ')[0])

# COMMAND ----------

df_prodts.display()

# COMMAND ----------

df_prodts.write.format('parquet').mode('overwrite').save('abfss://silver@advwstoragedatalake.dfs.core.windows.net/AdventureWorks_Products/')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transaformation Returns

# COMMAND ----------

df_Adwretun.display()

# COMMAND ----------

df_Adwretun.write.format('parquet').mode('overwrite').save('abfss://silver@advwstoragedatalake.dfs.core.windows.net/AdventureWorks_Returns/')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformation Teritories

# COMMAND ----------

df_adwteritory.display()

# COMMAND ----------

df_adwteritory.write.format('parquet').mode('overwrite').save('abfss://silver@advwstoragedatalake.dfs.core.windows.net/AdventureWorks_Territories/')

# COMMAND ----------

# MAGIC %md
# MAGIC Transformation SALES

# COMMAND ----------

df_adwsales =spark.read.format("csv").option("header",True).option("inferSchema",True).load('abfss://bronze@advwstoragedatalake.dfs.core.windows.net/AdventureWorks_Sales*/')

# COMMAND ----------

df_adwsales.display()

# COMMAND ----------

df_adwsales = df_adwsales.withColumn('StockDate', to_timestamp(col('StockDate'))); display(df_adwsales)

# COMMAND ----------

from pyspark.sql.functions import regexp_replace

# COMMAND ----------

df_adwsales = df_adwsales.withColumn('OrderNumber', regexp_replace(col('OrderNumber'), 'S', 'T'))

# COMMAND ----------

df_adwsales.display()

# COMMAND ----------

df_adwsales = df_adwsales.withColumn('Multiper', col('OrderLineItem') * col('OrderQuantity'))

# COMMAND ----------

df_adwsales.write.format('parquet').mode('overwrite').save('abfss://silver@advwstoragedatalake.dfs.core.windows.net/AdventureWorks_Sales/')

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Sales analysis

# COMMAND ----------

df_adwsales.groupBy('OrderDate').agg(count('OrderNumber').alias('TotalSales')).display()

# COMMAND ----------

df_prod_cat.display()

# COMMAND ----------

df_prodsubcat.display()

# COMMAND ----------

df_prodsubcat.write.format('parquet').mode('overwrite').save('abfss://silver@advwstoragedatalake.dfs.core.windows.net/Product_Subcategories/')