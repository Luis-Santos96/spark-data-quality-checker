from pyspark.sql import DataFrame
from typing import Optional, List


def check_row_count(
    df: DataFrame,
    min_rows: Optional[int] = None,
    max_rows: Optional[int] = None,
) -> List[dict]:
    """
    Validate that the DataFrame row count falls within expected bounds.

    Args:
        df:       Input DataFrame.
        min_rows: Minimum acceptable row count (inclusive).
        max_rows: Maximum acceptable row count (inclusive).

    Returns:
        List with a single result dict.
    """
    if min_rows is None and max_rows is None:
        raise ValueError("At least one of min_rows or max_rows must be provided.")

    row_count = df.count()

    below_min = min_rows is not None and row_count < min_rows
    above_max = max_rows is not None and row_count > max_rows
    passed = not below_min and not above_max

    if passed:
        message = f"[row_count_check] OK — {row_count} rows"
    elif below_min:
        message = (
            f"[row_count_check] FAILED — {row_count} rows is below minimum {min_rows}"
        )
    else:
        message = (
            f"[row_count_check] FAILED — {row_count} rows exceeds maximum {max_rows}"
        )

    return [
        {
            "check": "row_count_check",
            "row_count": row_count,
            "min_rows": min_rows,
            "max_rows": max_rows,
            "passed": passed,
            "message": message,
        }
    ]
