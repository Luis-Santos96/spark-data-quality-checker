from pyspark.sql import DataFrame
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
    raise NotImplementedError("value range check — coming soon")
