from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('Spark Playground').getOrCreate()


data = [
    (1, "Ravi", "Hyderabad", 25),
    (2, None, "Chennai", 32),
    (None, "Arun", "Hyderabad", 28),
    (4, "Meena", None, 30),
    (4, "Meena", None, 30),
    (5, "John", "Bangalore", -5)
]

columns = ["customer_id", "name", "city", "age"]

df = spark.createDataFrame(data, columns)
df.show()

#1 Identify data issues (nulls, duplicates, invalid values)
df.groupBy(columns).count().filter("count > 1").show()

df.filter("age < 0").show()

#2 Clean data (remove null keys, handle missing values, remove duplicates, filter invalid age)
from pyspark.sql.functions import col
df_clean = df.filter(col("customer_id").isNotNull())

df_clean = df_clean.fillna({
    "name": "Unknown",
    "city": "Unknown"
})

df_clean = df_clean.dropDuplicates()

df_clean = df_clean.filter(col("age") > 0)
df_clean.show()

#3 Validate cleaning (row counts before and after)
print("no.of rows before cleaning:", df.count())
print("no.of rows after cleaning:", df_clean.count())

#4 Perform aggregation (customers per city)
df_clean.groupBy("city").count().show()
