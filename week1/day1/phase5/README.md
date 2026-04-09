#  Phase 5 – Databricks + Olist: End-to-End Data Engineering Pipeline

##  Project Overview

This project focuses on building an end-to-end data engineering pipeline using Databricks and the Olist Brazilian E-commerce dataset. The objective is to simulate a real-world data workflow involving data ingestion, validation, transformation, and advanced analytics using PySpark.

---

##  Dataset

* Source: Olist Brazilian E-commerce Dataset (Kaggle)
* Contains multiple related tables such as:

  * Customers
  * Orders
  * Order Items
  * Payments
  * Products

These tables were used to model a real-world e-commerce system.

---


## 📊 Analytical Tasks

### 🔹 Task 1: Top 3 Customers per City

* Calculated total spend per customer
* Used window function (`RANK`) partitioned by city

### 🔹 Task 2: Running Total of Sales

* Aggregated daily sales
* Used cumulative sum with window function

### 🔹 Task 3: Top Products per Category

* Joined fact table with product dimension
* Aggregated total sales per product
* Applied `DENSE_RANK` within each category

### 🔹 Task 4: Customer Lifetime Value (CLV)

* Calculated total spend per customer across all orders

### 🔹 Task 5: Customer Segmentation

* Segmented customers into:

  * Gold (>10000)
  * Silver (5000–10000)
  * Bronze (<5000)
* Grouped by segment to analyze distribution

### 🔹 Task 6: Final Reporting Table

* Combined key metrics into a single dataset:

  * customer_id
  * city
  * total_spend
  * segment
  * total_orders


---

## Reflection

### What challenges did you face in setup?

I faced some issues while uploading all the files correctly into Databricks and making sure the file paths were correct. 
Initially, I also got errors because I didn’t run the notebook cells in order. 
Understanding how different tables are connected was also a bit confusing at first.

---

### How did you validate joins?

I validated joins by checking row counts before and after joining tables. 
I also used left anti joins to see if there were any unmatched records. 
Additionally, I checked for null values in important columns to make sure the joins worked properly.

---

### How did window functions help?

Window functions helped me to perform calculations like ranking and running totals without grouping the data completely.
For example, I used them to find top customers per city and top products per category. It made the analysis easier and more efficient.

---

### Can you explain your pipeline?

First, I loaded all the datasets into Databricks. Then I cleaned and validated the data by checking for nulls and duplicates. 
After that, I created a fact table by joining relevant tables. I performed different analytical tasks using aggregations and window functions. 
Finally, I combined the results into a reporting table that can be used for analysis.
