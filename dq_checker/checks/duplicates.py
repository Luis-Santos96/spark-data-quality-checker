from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import List


def check_duplicates(df: DataFrame, key_columns: List[str]) -> List[dict]:
    """
    Check for duplicate rows based on one or more key columns.

    Args:
        df:           Input DataFrame.
        key_columns:  Columns that together form the primary key.

    Returns:
        List with a single result dict.
    """
    missing = [c for c in key_columns if c not in df.columns]
    if missing:
        return [
            {
                "check": "duplicate_check",
                "key_columns": key_columns,
                "total_rows": None,
                "duplicate_count": None,
                "passed": False,
                "message": (
                    f"[duplicate_check] FAILED — column(s) not found: {missing}"
                ),
            }
        ]

    total_rows = df.count()
    distinct_rows = df.select(key_columns).distinct().count()
    duplicate_count = total_rows - distinct_rows
    passed = duplicate_count == 0
    key_str = ", ".join(key_columns)

    return [
        {
            "check": "duplicate_check",
            "key_columns": key_columns,
            "total_rows": total_rows,
            "duplicate_count": duplicate_count,
            "passed": passed,
            "message": (
                f"[duplicate_check] OK — no duplicates on ({key_str})"
                if passed
                else (
                    f"[duplicate_check] FAILED — {duplicate_count} duplicate row(s) "
                    f"on ({key_str})"
                )
            ),
        }
    ]
