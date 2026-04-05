#  Phase 4 – Mini Project: Business Pipeline & Analytics

##  Objective

Built an end-to-end data pipeline using PySpark to transform raw sales and customer data into meaningful business insights. This project focuses on data cleaning, transformation, aggregation, and reporting.

---

##  Pipeline Steps


###  Data Cleaning

* Removed rows with null `customer_id`
  
* Dropped duplicate records
  
* Filtered invalid values (e.g., negative amounts)
  
* Ensured correct column data types

###  Transformations & Analysis

Task 1: Daily Sales → Output: date, total_sales

Task 2: City-wise Revenue → Output: city, total_revenue

Task 3: Top 5 Customers → Output: customer_name, total_spend

Task 4: Repeat Customers (>1 order) → Output: customer_id, order_count

Task 5: Customer Segmentation → Create business logic: total_spend > 10000 → Gold 5000–10000 →
Silver <5000 → Bronze Output: customer_name, total_spend, segment

Task 6: Final Reporting Table → Combine all relevant insights into a final table. Output should include:
customer_name, city, total_spend, order_count, segment

Task 7: Save Output → final_df.write.mode('overwrite').csv('/samples/output/report')

---



##  Reflection

###  Why is cleaning done before joining tables?

Cleaning ensures accurate joins by removing invalid or inconsistent data, preventing incorrect aggregations and duplication.

###  What would go wrong if null keys are not removed?

* Joins may fail or produce missing data
* Aggregations become incorrect
* Data integrity is compromised

###  How did you decide join order?

Started with **fact table (transactions)** and joined with **dimension table (customers)** to enrich data efficiently.

###  Which step was most difficult and why?

Handling joins and ensuring correct aggregations without duplication required careful planning.

###  How is SQL logic similar to PySpark?

* `GROUP BY` → `groupBy()`
* `JOIN` → `join()`
* `CASE WHEN` → `when()`
* Overall logic remains same, syntax differs

###  What challenges will appear with large data?

* Memory constraints
* Data skew
* Slow joins and shuffles
* Need for partitioning and optimization

###   Can you explain your pipeline in simple steps?

1. Load data
2. Clean data
3. Aggregate and analyze
4. Join results
5. Generate final report
6. Save output


