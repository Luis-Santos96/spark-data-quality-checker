from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import List
import datetime


def check_freshness(
    df: DataFrame,
    timestamp_column: str,
    max_age_hours: float,
) -> List[dict]:
    """
    Verify that the most recent timestamp in a column is within max_age_hours.

    Args:
        df:                Input DataFrame.
        timestamp_column:  Name of the timestamp column to inspect.
        max_age_hours:     Maximum allowed age of the latest record in hours.

    Returns:
        List with a single result dict.
    """
    if timestamp_column not in df.columns:
        return [
            {
                "check": "freshness_check",
                "column": timestamp_column,
                "max_age_hours": max_age_hours,
                "latest_timestamp": None,
                "age_hours": None,
                "passed": False,
                "message": (
                    f"[freshness_check:{timestamp_column}] FAILED — "
                    f"column not found in DataFrame"
                ),
            }
        ]

    row = df.agg(F.max(F.col(timestamp_column)).alias("latest")).collect()[0]
    latest = row["latest"]

    if latest is None:
        return [
            {
                "check": "freshness_check",
                "column": timestamp_column,
                "max_age_hours": max_age_hours,
                "latest_timestamp": None,
                "age_hours": None,
                "passed": False,
                "message": (
                    f"[freshness_check:{timestamp_column}] FAILED — "
                    f"no non-null timestamps found"
                ),
            }
        ]

    # Support both datetime and date objects
    if isinstance(latest, datetime.datetime):
        latest_dt = latest
    else:
        latest_dt = datetime.datetime(latest.year, latest.month, latest.day)

    now = datetime.datetime.utcnow()
    age_hours = (now - latest_dt).total_seconds() / 3600
    passed = age_hours <= max_age_hours

    return [
        {
            "check": "freshness_check",
            "column": timestamp_column,
            "max_age_hours": max_age_hours,
            "latest_timestamp": latest_dt.isoformat(),
            "age_hours": round(age_hours, 4),
            "passed": passed,
            "message": (
                f"[freshness_check:{timestamp_column}] OK — "
                f"latest record is {age_hours:.2f}h old (max {max_age_hours}h)"
                if passed
                else (
                    f"[freshness_check:{timestamp_column}] FAILED — "
                    f"latest record is {age_hours:.2f}h old, exceeds max {max_age_hours}h"
                )
            ),
        }
    ]
