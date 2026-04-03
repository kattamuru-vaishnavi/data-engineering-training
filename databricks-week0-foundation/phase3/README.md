## SQL to PySpark Phase 3 – Final ETL & Pipeline (Practice Pack)

# Objective
Move from writing isolated queries to thinking like a data engineer. This phase focuses on data ingestion,
cleaning, transformation, and pipeline building using Spark Playground sample datasets.

# Core ETL Concept
Every data engineering workflow follows ETL: 
Extract (read data), 
Transform (clean, filter, join, aggregate), 
Load (save or display final output). 

# Hands-on Ingestion Tasks
1. Read a CSV file from /samples/
2. Inspect schema using show() and printSchema()
3. Identify missing values
4. Clean data using dropna() or fillna()
5. Filter invalid records
6. Read JSON and Parquet sample files

# Business Pipeline Exercises
1. Read sales data -> clean nulls -> calculate daily sales
2. Read customer data -> clean invalid rows -> city-wise revenue
3. Find repeat customers (>2 orders)
4. Find highest spending customer in each city
5. Build final reporting table with customer, city, total spend, order count

#  
In this phase, I learned how to build a complete ETL pipeline using PySpark. 
I worked on reading data from CSV files, inspecting schemas, and cleaning the data by handling null and invalid values.

I performed transformations such as joins, aggregations, and filtering to generate insights like daily sales,
city-wise revenue, repeat customers, and top customers per city.
