from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Spark Playground').getOrCreate()
from pyspark.sql.functions import *
from pyspark.sql.window import Window

df1 = spark.read.format('csv').option('header', 'true').option('inferSchema', 'true').load('/samples/customers.csv')
df2 = spark.read.format('csv').option('header', 'true').option('inferSchema', 'true').load('/samples/sales.csv')

df1_dropped = df1.dropna() 
df2_dropped = df2.dropna() 

#1. Read sales data -> clean nulls -> calculate daily sales
df2_dropped.groupBy("sale_date").sum("total_amount").show()

#2. Read customer data -> clean invalid rows -> city-wise revenue
df1_dropped.join(df2_dropped, on="customer_id").groupBy("city").sum("total_amount").show()

#3. Find repeat customers (>2 orders)
df2_dropped.groupBy("customer_id").count().filter(col("count")>2).show()

#4. Find highest spending customer in each city
spend=df1_dropped.join(df2_dropped, on="customer_id").groupBy("city", "customer_id") .agg(sum("total_amount").alias("total_spend"))
wind=Window.partitionBy("city").orderBy(col("total_spend").desc())
spend.withColumn("rank", rank().over(wind)).filter(col("rank") == 1).show()

#5. Build final reporting table with customer, city, total spend, order count
df1_dropped.join(df2_dropped, on="customer_id").groupBy("customer_id","city").agg(sum("total_amount"),count("sale_id")).show()
