##  SQL to PySpark – Phase 2 (Bridge Pack)

##  Objective
In this phase, I bridged the gap between basic SQL and real-world PySpark transformations.  
I worked with sample datasets, performed data cleaning, and used joins and aggregations.

---

##  What I Learned

- Joins 
- Aggregations (SUM, AVG, COUNT)
- Grouping data (GROUP BY)
- Sorting results (ORDER BY)
- Handling missing values using PySpark

---

##  Data Cleaning

Removed rows with missing `customer_id`:


customers = customers.dropna(subset=["customer_id"])
orders = orders.dropna(subset=["customer_id"])


---

##  Problems Solved (SQL → PySpark)

1. Find the total order amount for each customer
2. Find the top 3 customers based on total spending
3. Find customers who have not placed any orders
4. Calculate total revenue for each city
5. Find the average order amount per customer
6. Find customers who have placed more than one order
7. Sort customers based on total spending in descending order

---

##  Approach

* First wrote queries in **SQL**
* Then converted them into **PySpark DataFrame code**
* Executed both and compared results
* Focused on understanding how SQL logic maps to PySpark transformations

---

##  Files in this Folder

* `phase2_problem_statement.pdf` → Contains all problem statements
* `sql_queries.sql` → SQL solutions for all problems
* `pyspark_notebook_phase2.py` → PySpark solutions
* `outputs/` → Output screenshots

---

##  Outcome

This phase helped me:

* Understand real-world data transformations
* Gain confidence in writing PySpark code from SQL logic
* Work with joins and aggregations in Spark efficiently
