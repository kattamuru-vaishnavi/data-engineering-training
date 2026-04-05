from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Spark Playground').getOrCreate()
from pyspark.sql.functions import col, sum, count, desc, when, concat_ws

df1 = spark.read.format('csv').option('header', 'true').load('/samples/customers.csv')
df2 = spark.read.format('csv').option('header', 'true').load('/samples/sales.csv')

# Remove null keys
df1_clean = df1.dropna(subset=["customer_id"])
df2_clean = df2.dropna(subset=["customer_id"])

# Remove duplicates
df1_clean = df1_clean.dropDuplicates()
df2_clean = df2_clean.dropDuplicates()

# Fix data types
df2_clean = df2_clean.withColumn("total_amount", col("total_amount").cast("double")) \
                     .withColumn("quantity", col("quantity").cast("int"))

# Remove invalid values
df2_clean = df2_clean.filter(col("total_amount") > 0)

#1
df2_clean.groupBy("sale_date").sum("total_amount").show()

#2
df2_clean.join(df1_clean, "customer_id").groupBy("city").sum("total_amount").show()

#3
df2_clean.groupBy("customer_id").sum("total_amount").orderBy(desc("sum(total_amount)")).limit(5).show()

#4
df2_clean.groupBy("customer_id").agg(count("sale_id").alias("order_count")).filter(col("order_count") > 1).show()

#5
segmented = df2_clean.groupBy("customer_id").agg(sum("total_amount").alias("total_spend"))\
.withColumn("segment",when(col("total_spend") > 10000, "Gold").when((col("total_spend") >= 5000) & (col("total_spend") <= 10000), "Silver")\
            .otherwise("Bronze"))
segmented.show()

#6
df1_final = df1_clean.withColumn("customer_name", concat_ws(" ", col("first_name"), col("last_name")))
order_counts = df2_clean.groupBy("customer_id") .agg(count("sale_id").alias("order_count"))
final_df = df1_final.join(segmented, "customer_id").join(order_counts, "customer_id")
final_df.select(
    "customer_name",
    "city",
    "total_spend",
    "order_count",
    "segment"
).show()

#7
final_df.write.mode('overwrite').csv('/samples/output/report')
