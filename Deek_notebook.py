# Databricks notebook source
dbutils.fs.ls("/Volumes/workspace/learning_upload/learn_upload_file/")

# COMMAND ----------

df = spark.read.format('csv').option("header",True).option("inferSchema",True).load("/Volumes/workspace/learning_upload/learn_upload_file/BigMart Sales.csv")
df.display()
df.printSchema()

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

df.filter( (col('Item_Type') == 'Soft Drinks') & (col('Item_Weight') < 10)).display()

# COMMAND ----------

df.withColumn('Tets',regexp_replace(col('Item_Fat_Content'), 'Low Fat','Lf')).display()

# COMMAND ----------

#df.sort('Item_Weight','Item_Outlet_Sales',ascending=[0,0]).display()
df.sort(['Item_Weight','Item_Outlet_Sales'],ascending=[0,0]).display()
# df.groupBy('Item_Type').agg(sum('Item_Outlet_Sales')).display()'

# COMMAND ----------

# MAGIC %md
# MAGIC ###DATE FUNCTIONS

# COMMAND ----------


df=df.withColumn('curr_date',current_date())
df.display()

# COMMAND ----------

df = df.withColumn('week_after', date_add('curr_date', 7))
df.display()

# COMMAND ----------

df = df.withColumn('week_before',date_add('curr_date',-7))
df.display()

# COMMAND ----------


df.drop('Date_colum_test').drop('Postoneweek').drop('lasteweekdate').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### DATE DIFF

# COMMAND ----------

df= df.withColumn('datediff',datediff('curr_date','week_before'))
df.display()
df.drop('Interval').display()


# COMMAND ----------

# MAGIC %md
# MAGIC ### DATE FORMAT 

# COMMAND ----------

df = df.withColumn('week_after',date_format('curr_date','MM-dd-yy'))
df.display()


# COMMAND ----------

df= df.drop('Date_colum_test').drop('Postoneweek').drop('lasteweekdate')
df.display()

# COMMAND ----------

df.fillna('novalue').display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### HANDLING NULL

# COMMAND ----------

df.dropna('all').display()

# COMMAND ----------

# df.dropna('any').display()
df.fillna('novalue').display()

# COMMAND ----------

# df.dropna(subset=['Outlet_Size']).display()
# # df.dropna(how='all').display()
df.dropna(how='any').display()

# COMMAND ----------

df.display()

# COMMAND ----------

df.fillna('novalue').display()

# COMMAND ----------

df.display()

# COMMAND ----------

df.fillna('Not available',subset=['Outlet_Size']).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### ADVANCE FUNTIONS
# MAGIC ### SPLIT AND INDEXING - IMP 

# COMMAND ----------

df.withColumn('Outlet_Type',split('Outlet_Type',' ')).display()

# COMMAND ----------

df.withColumn('Outlet_Type',split('Outlet_Type',' ')[0]).display()
df.withColumn('Outlet_Type',split('Outlet_Type',' ')[1]).display()

# COMMAND ----------

dff = df.withColumn('Outlet_Type', split('Outlet_Type',' '))
dff.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### EXPLODE

# COMMAND ----------

dff.withColumn('Outlet_Type', explode('Outlet_Type')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### ARRAY_CONTAINS

# COMMAND ----------

dff.withColumn('Outlet_Type_flag',array_contains('Outlet_Type','Type1')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###GROUP BY (AGG)

# COMMAND ----------

df.groupBy('Item_Type').count().display()
df.groupBy('Item_Type').agg(sum('Item_MRP')).display()

# COMMAND ----------

df.groupBy('Item_Type').agg(avg('Item_MRP')).display()

# COMMAND ----------

df.groupBy('Item_Type','Outlet_Size').agg(avg('Item_MRP').alias('TOTALMRPP')).orderBy(desc('TOTALMRPP')).display()

# COMMAND ----------

df.groupBy('Item_Type','Outlet_Size').agg(sum('Item_MRP').alias('total')).agg(avg('total').alias('TOTALMRPP')).orderBy(desc('TOTALMRPP')).display()

# COMMAND ----------

df.groupBy('Item_Type','Outlet_Size').agg(sum('Item_MRP'),avg('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###COLLECT LIST

# COMMAND ----------

dff.display()

# COMMAND ----------

dfn = dff.withColumn('Outlet_Type',explode('Outlet_Type'))
dfn.display()

# COMMAND ----------

dfn.groupBy('Item_Identifier').agg(collect_list('Outlet_Type')).display()
# dfn.groupBy('Outlet_Type').agg(collect_set('Outlet_Type')).display()

# COMMAND ----------

data1 = [('user1','b1'),('user1','b2'),('user2','b3'),('user2','b3')]
schema1 = 'col1 STRING, col2 STRING'
df1 = spark.createDataFrame(data1,schema1)
df1.display()




# COMMAND ----------

df1.groupBy('col1').agg(collect_list('col2')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###PIVOT

# COMMAND ----------

dff2= df.fillna('nullede')
dff2.display()

# COMMAND ----------

dff2.groupBy('Item_Type').pivot('Outlet_Size').agg(avg('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### WHEN OTHERWISE

# COMMAND ----------

dff3=df.withColumn('VegorNonveg',when(col('Item_Type')=='Meat','Non-veg').otherwise('veg'))

# COMMAND ----------

dff3.withColumn('veg_flag_exp',when((col('VegorNonveg') == 'veg') & (col('Item_MRP')<100),'vegkammibele')\
    .when((col('VegorNonveg') == 'veg') & (col('Item_MRP')>100),'vegjaastibele').otherwise('nonveg')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### JOINS
# MAGIC #### INNER JOIN

# COMMAND ----------

data1 =[(1,'deek','d001'),(2,'deek2','d002'),(3,'deek3','d003'),(4,'deek4','d004'),(5,'deek','d005'),(6,'deek','d006'),(7,'deek','d007'),(8,'deek','d008'),(9,'deek','d009'),(10,'deek','d010')]
schema1 = ('empid string,name string,dpid string')
df1 = spark.createDataFrame(data1,schema1)

data2 =[('deek','d001'),('deek','d002'),('deek','d003')]
schema2 = ('dept string,dpid string')
df2 = spark.createDataFrame(data2,schema2)
df1.join(df2,df1['dpid']==df2['dpid']).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ####left join

# COMMAND ----------

data1 =[(1,'deek','d001'),(2,'deek2','d002'),(3,'deek3','d003'),(4,'deek4','d004'),(5,'deek','d005'),(6,'deek','d006'),(7,'deek','d007'),(8,'deek','d008'),(9,'deek','d009'),(10,'deek','d010')]
schema1 = ('empid string,name string,dpid string')
df1 = spark.createDataFrame(data1,schema1)

data2 =[('deek','d001'),('deek','d002'),('deek','d003')]
schema2 = ('dept string,dpid string')
df2 = spark.createDataFrame(data2,schema2)
df1.join(df2,df1['dpid']==df2['dpid'],'left').display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### right join

# COMMAND ----------

data1 =[(1,'deek','d001'),(2,'deek2','d002')]
schema1 = ('empid string,name string,dpid string')
df1 = spark.createDataFrame(data1,schema1)

data2 =[('deek','d001'),('deek','d002'),('deek','d003')]
schema2 = ('dept string,dpid string')
df2 = spark.createDataFrame(data2,schema2)
df1.join(df2,df1['dpid']==df2['dpid'],'right').display()


# COMMAND ----------

# MAGIC %md
# MAGIC #### FULL JOIN

# COMMAND ----------

data1 =[(1,'deek','d001'),(2,'deek2','d002'),(3,'deek3','d003'),(4,'deek4','d004'),(5,'deek','d005'),(6,'deek','d006'),(7,'deek','d007'),(8,'deek','d008'),(9,'deek','d009')]
schema1 = ('empid string,name string,dpid string')
df1 = spark.createDataFrame(data1,schema1)

data2 =[('deek','d001'),('deek','d002'),('deek','d003')]
schema2 = ('dept string,dpid string')
df2 = spark.createDataFrame(data2,schema2)
df1.join(df2,df1['dpid']==df2['dpid'],'full').display()


# COMMAND ----------

# MAGIC %md
# MAGIC ###ANTI JOIN

# COMMAND ----------

data1 =[(1,'deek','d001'),(2,'deek2','d002'),(3,'deek3','d003'),(4,'deek4','d004'),(5,'deek','d005'),(6,'deek','d006'),(7,'deek','d007'),(8,'deek','d008'),(9,'deek','d009')]
schema1 = ('empid string,name string,dpid string')
df1 = spark.createDataFrame(data1,schema1)

data2 =[('deek','d001'),('deek','d002'),('deek','d003')]
schema2 = ('dept string,dpid string')
df2 = spark.createDataFrame(data2,schema2)
df1.join(df2,df1['dpid']==df2['dpid'],'anti').display()


# COMMAND ----------

df.display()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### window funtion 

# COMMAND ----------

# MAGIC %md
# MAGIC #### row_number()

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

df.withColumn('newrownum',row_number().over(Window.orderBy('Item_Identifier'))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###RANK()

# COMMAND ----------

df.withColumn('RANKrownum',rank().over(Window.orderBy('Item_Identifier'))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### DENSE RANK

# COMMAND ----------

df.withColumn('denseRANKrownum',dense_rank().over(Window.orderBy(col('Item_Identifier').desc()))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### cumilative sum

# COMMAND ----------

df.withColumn('cumsum',sum(col('Item_MRP')).over(Window.orderBy(col('Item_Type')).rowsBetween(Window.unboundedPreceding, 0))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### USER DEFINED FUNCTIONS

# COMMAND ----------

def myfun(x):
    return x//2

my_udf = udf(myfun)

df.withColumn('test_ufd',my_udf('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###DATA WRITING 

# COMMAND ----------

# MAGIC %md
# MAGIC #### CSV

# COMMAND ----------

df.write.format('csv').save("/Volumes/workspace/learning_upload/learn_upload_file/data.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC #### append

# COMMAND ----------

df.write.format('csv').mode('append').save("/Volumes/workspace/learning_upload/learn_upload_file/data.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC #### over write mode

# COMMAND ----------

df.write.format('csv').mode('overwrite').save("/Volumes/workspace/learning_upload/learn_upload_file/data.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC #### error mode

# COMMAND ----------

df.write.format('csv').mode('error').save("/Volumes/workspace/learning_upload/learn_upload_file/data.csv")
#df.write.format('csv').mode('ignore').save("/Volumes/workspace/learning_upload/learn_upload_file/data.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC #### ignore mode

# COMMAND ----------

df.write.format('csv').mode('ignore').save("/Volumes/workspace/learning_upload/learn_upload_file/data.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ### PARQUET FILE FORMAT

# COMMAND ----------

df.write.format('parquet').mode('overwrite').save("/Volumes/workspace/learning_upload/learn_upload_file/data.csv")
##df.write.format('csv').mode('overwrite').save("/Volumes/workspace/learning_upload/learn_upload_file/data.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ###table

# COMMAND ----------

df.write.format('delta').mode('overwrite').saveAsTable("MY_table")

# COMMAND ----------

# MAGIC %md
# MAGIC ### SPARK SQL!!!!!

# COMMAND ----------

# MAGIC %md
# MAGIC #### CREATE NEW VEIW FOR SQL

# COMMAND ----------

df.display()

# COMMAND ----------

df.createTempView("MY_table")





# COMMAND ----------

# MAGIC %sql
# MAGIC select * from My_table;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from My_table where Item_Fat_Content != 'Low Fat';

# COMMAND ----------

df.describe().display()

# COMMAND ----------

df.display()

# COMMAND ----------

#df.sort(['Item_Identifier','Item_Weight'],ascending=['Ture','False']).display()
df.sort(['Item_Identifier','Item_Weight'],ascending=[True,False]).display()

# COMMAND ----------

#df.dropDuplicates(subset=['Item_Identifier','Item_Weight']).display()
df.drop('Item_Weight').display()


# COMMAND ----------

df.fillna(0).display()