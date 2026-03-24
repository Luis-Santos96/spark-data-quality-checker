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
    raise NotImplementedError("row count check — coming soon")
