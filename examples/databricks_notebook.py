# Databricks notebook source
# This file can be imported as a Databricks notebook or run locally with a
# local SparkSession.  Each cell is separated by # COMMAND ----------

# COMMAND ----------
# MAGIC %md
# MAGIC # Data Quality Checker — Example
# MAGIC
# MAGIC Demonstrates `DQChecker` between two pipeline layers (bronze → silver).

# COMMAND ----------

from pyspark.sql import SparkSession, Row
from pyspark.sql import functions as F

from dq_checker import DQChecker

# When running locally, create a SparkSession.
# In Databricks, `spark` is already available.
try:
    spark
except NameError:
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("dq_checker_example")
        .getOrCreate()
    )

# COMMAND ----------
# MAGIC %md ## 1. Build a sample "silver" orders DataFrame

# COMMAND ----------

orders_data = [
    Row(customer_id=1,    order_id=1001, product="Widget A", amount=49.99,  order_date="2024-03-01"),
    Row(customer_id=2,    order_id=1002, product="Widget B", amount=129.00, order_date="2024-03-01"),
    Row(customer_id=3,    order_id=1003, product="Widget A", amount=49.99,  order_date="2024-03-02"),
    Row(customer_id=None, order_id=1004, product="Widget C", amount=75.50,  order_date="2024-03-02"),  # null customer
    Row(customer_id=5,    order_id=1005, product=None,       amount=None,   order_date="2024-03-03"),  # nulls
    Row(customer_id=2,    order_id=1002, product="Widget B", amount=129.00, order_date="2024-03-01"),  # duplicate
]

orders_df = spark.createDataFrame(orders_data)
orders_df.show()

# COMMAND ----------
# MAGIC %md ## 2. Run data quality checks

# COMMAND ----------

results = (
    DQChecker(orders_df)
    # No more than 1 % nulls allowed on critical identity columns
    .check_nulls(columns=["customer_id", "order_id"], threshold=0.01)
    # Nulls on non-critical columns are tolerated up to 20 %
    .check_nulls(columns=["product", "amount"], threshold=0.20)
    # (customer_id, order_id) must be unique — acts as PK
    .check_duplicates(key_columns=["customer_id", "order_id"])
    .run()
)

# COMMAND ----------
# MAGIC %md ## 3. View summary

# COMMAND ----------

results.summary()

# COMMAND ----------
# MAGIC %md ## 4. Gate the pipeline on failures

# COMMAND ----------

if results.has_failures():
    print("Pipeline halted — data quality issues detected:\n")
    for check in results.failed_checks():
        print(f"  • {check['message']}")
    # In a real pipeline you would raise here or write a quarantine table:
    # raise ValueError("Data quality gate failed")
else:
    print("All checks passed — proceeding to next pipeline layer.")

# COMMAND ----------
# MAGIC %md ## 5. Clean DataFrame — all checks pass

# COMMAND ----------

clean_data = [
    Row(customer_id=1, order_id=2001, product="Gadget X", amount=199.99, order_date="2024-03-10"),
    Row(customer_id=2, order_id=2002, product="Gadget Y", amount=299.99, order_date="2024-03-10"),
    Row(customer_id=3, order_id=2003, product="Gadget Z", amount=149.99, order_date="2024-03-11"),
]

clean_df = spark.createDataFrame(clean_data)

clean_results = (
    DQChecker(clean_df)
    .check_nulls(columns=["customer_id", "order_id", "product", "amount"], threshold=0.0)
    .check_duplicates(key_columns=["customer_id", "order_id"])
    .run()
)

clean_results.summary()
assert not clean_results.has_failures(), "Unexpected failures on clean dataset"
print("Clean dataset passed all checks.")
