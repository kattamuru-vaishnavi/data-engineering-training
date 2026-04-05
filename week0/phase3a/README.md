# Phase 3A – Data Quality & Cleaning Challenge

## Objective
Work with intentionally messy data and apply cleaning techniques before building a pipeline.

This phase helps understand why data cleaning is critical in real-world data engineering.


## Dataset (PySpark)

```python
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
````

---

## Tasks Performed

### 1. Identify Data Issues


### 2. Data Cleaning

### 3. Validation

### 4. Aggregation


---


## Reflection Questions

### What happens if cleaning is skipped?

Duplicate data increases counts and gives wrong results.

Null and invalid values cause errors in analysis.

### Which issue impacted results most?

Invalid values like negative age affected accuracy.

### How would this affect business decisions?

Wrong customer insights.

Leads to wrong customer insights and poor decisions.

Marketing and analytics become unreliable.

### Cleaning Checklist

Remove null primary keys

Handle missing values

Remove duplicates

Validate data ranges

Compare data before and after cleaning
