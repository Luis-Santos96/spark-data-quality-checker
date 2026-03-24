from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import List


def check_nulls(df: DataFrame, columns: List[str], threshold: float = 0.0) -> List[dict]:
    """
    Check for null values in specified columns.

    Args:
        df:         Input DataFrame.
        columns:    Column names to inspect.
        threshold:  Maximum allowed null ratio [0.0, 1.0].
                    0.0 (default) means zero nulls tolerated.

    Returns:
        List of result dicts, one per column.
    """
    total_rows = df.count()

    if total_rows == 0:
        return [
            {
                "check": "null_check",
                "column": col,
                "null_count": 0,
                "null_ratio": 0.0,
                "threshold": threshold,
                "passed": True,
                "message": f"[null_check:{col}] OK — DataFrame is empty",
            }
            for col in columns
        ]

    results = []
    for col in columns:
        if col not in df.columns:
            results.append(
                {
                    "check": "null_check",
                    "column": col,
                    "null_count": None,
                    "null_ratio": None,
                    "threshold": threshold,
                    "passed": False,
                    "message": f"[null_check:{col}] FAILED — column not found in DataFrame",
                }
            )
            continue

        null_count = df.filter(F.col(col).isNull()).count()
        null_ratio = null_count / total_rows
        passed = null_ratio <= threshold

        results.append(
            {
                "check": "null_check",
                "column": col,
                "null_count": null_count,
                "null_ratio": round(null_ratio, 4),
                "threshold": threshold,
                "passed": passed,
                "message": (
                    f"[null_check:{col}] OK — {null_count} nulls ({null_ratio:.2%})"
                    if passed
                    else (
                        f"[null_check:{col}] FAILED — {null_count} nulls "
                        f"({null_ratio:.2%}) exceeds threshold {threshold:.2%}"
                    )
                ),
            }
        )

    return results
