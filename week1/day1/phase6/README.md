#  Phase 6 – Spark Playground Exit Sprint (Advanced Practice Lab)

##  Objective

Build fluency and confidence in PySpark by practicing joins, window functions, date operations, and
complete pipeline execution before transitioning to Databricks.


---


##  Practice Sets

###  Practice Set A – Join Drills

* Perform **Inner Join** between orders and customers (valid records)
* Perform **Left Join** and identify null values
* Use **Left Anti Join** to find invalid foreign keys
* Compare row counts across different joins

---

###  Practice Set B – Window Functions

* Find **Top 3 customers per city** using ranking
* Calculate **running total of sales**
* Rank customers by **total spend**
* Use **LAG function** to find previous order

---

###  Practice Set C – Date Analysis

* Extract **month** from order date
* Perform **monthly sales aggregation**
* Calculate **date differences** between orders
* Perform **trend analysis by month**

---

###  Practice Set D – Timed Pipeline (60–75 mins)

* Load dataset
* Clean invalid records (nulls, negative values)
* Validate referential integrity
* Join datasets
* Apply aggregations and window functions
* Save final output

---
##  Reflection Questions

### 1. Which task took the most time?

Window functions and debugging joins took more time.

### 2. What mistakes did you make?

I used wrong column names, got null values after joins, and had confusion in window functions (partition and order).

### 3. How did you debug issues?

I used `printSchema()` and `show()` to check data, fixed column names, and verified join conditions.

### 4. Can you now build pipeline independently?

Yes, I can build a basic pipeline with cleaning, joins, aggregations, and window functions.

### 5. What needs improvement?

I need to improve speed, better understand window functions, and handle complex joins.

