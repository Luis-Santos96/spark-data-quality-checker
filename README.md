# spark-data-quality-checker

A lightweight PySpark library for automated data quality validation in Databricks pipelines.

[![Python](https://img.shields.io/badge/Python-3.9+-3776AB?style=flat-square&logo=python&logoColor=white)](https://python.org)
[![PySpark](https://img.shields.io/badge/PySpark-3.x-E25A1C?style=flat-square&logo=apachespark&logoColor=white)](https://spark.apache.org)
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-003366?style=flat-square)](https://delta.io)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue?style=flat-square)](LICENSE)

---

## What it does

Drop-in data quality checks for any PySpark / Delta Lake pipeline. Run validations between pipeline layers to catch issues before they propagate downstream.

### Checks available

- **Null detection** — flag columns exceeding null thresholds
- **Duplicate detection** — validate primary key uniqueness
- **Schema drift** — compare DataFrame schemas against expected definitions
- **Row count validation** — alert on unexpected volume changes between runs
- **Value range checks** — ensure numeric columns stay within expected bounds
- **Freshness checks** — verify data is not stale based on timestamp columns

## Quick start

```python
from dq_checker import DQChecker

# Initialize with your DataFrame
checker = DQChecker(df)

# Run checks
results = (
    checker
    .check_nulls(columns=["customer_id", "order_date"], threshold=0.01)
    .check_duplicates(key_columns=["customer_id", "order_id"])
    .check_row_count(min_rows=1000)
    .run()
)

# View results
results.summary()
```

## Project structure

```
spark-data-quality-checker/
├── dq_checker/
│   ├── __init__.py
│   ├── checker.py          # Core DQChecker class
│   ├── checks/
│   │   ├── nulls.py        # Null validation
│   │   ├── duplicates.py   # PK uniqueness
│   │   ├── schema.py       # Schema drift detection
│   │   ├── volume.py       # Row count checks
│   │   ├── range.py        # Value range validation
│   │   └── freshness.py    # Data staleness checks
│   └── reporting.py        # Results formatting
├── tests/
│   └── test_checker.py
├── examples/
│   └── databricks_notebook.py
├── README.md
└── requirements.txt
```

## Status

🚧 **In development** — core null and duplicate checks implemented, schema drift and reporting in progress.

## Author

**Luis Ricardo** — Data Engineer  
[Portfolio](https://luis-santos96.github.io) · [LinkedIn](https://www.linkedin.com/in/luisr-santos/) · [GitHub](https://github.com/Luis-Santos96)
