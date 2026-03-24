from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import List, Optional


def check_value_range(
    df: DataFrame,
    column: str,
    min_value: Optional[float] = None,
    max_value: Optional[float] = None,
) -> List[dict]:
    """
    Ensure a numeric column's values stay within [min_value, max_value].

    Args:
        df:         Input DataFrame.
        column:     Numeric column to validate.
        min_value:  Inclusive lower bound (None = no lower bound).
        max_value:  Inclusive upper bound (None = no upper bound).

    Returns:
        List with a single result dict.
    """
    if min_value is None and max_value is None:
        raise ValueError("At least one of min_value or max_value must be provided.")

    if column not in df.columns:
        return [
            {
                "check": "range_check",
                "column": column,
                "min_value": min_value,
                "max_value": max_value,
                "violation_count": None,
                "passed": False,
                "message": (
                    f"[range_check:{column}] FAILED — column not found in DataFrame"
                ),
            }
        ]

    condition = F.lit(False)
    if min_value is not None:
        condition = condition | (F.col(column) < min_value)
    if max_value is not None:
        condition = condition | (F.col(column) > max_value)

    violation_count = df.filter(condition).count()
    passed = violation_count == 0

    bounds = []
    if min_value is not None:
        bounds.append(f"min={min_value}")
    if max_value is not None:
        bounds.append(f"max={max_value}")
    bounds_str = ", ".join(bounds)

    return [
        {
            "check": "range_check",
            "column": column,
            "min_value": min_value,
            "max_value": max_value,
            "violation_count": violation_count,
            "passed": passed,
            "message": (
                f"[range_check:{column}] OK — all values within bounds ({bounds_str})"
                if passed
                else (
                    f"[range_check:{column}] FAILED — {violation_count} value(s) "
                    f"out of bounds ({bounds_str})"
                )
            ),
        }
    ]
