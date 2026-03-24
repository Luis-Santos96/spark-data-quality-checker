from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from typing import List


def check_schema(df: DataFrame, expected_schema: StructType) -> List[dict]:
    """
    Compare a DataFrame schema against an expected StructType definition.

    Detects:
        - Missing columns (present in expected, absent in df)
        - Extra columns  (present in df, absent in expected)
        - Type mismatches

    Args:
        df:              Input DataFrame.
        expected_schema: StructType that the DataFrame should conform to.

    Returns:
        List of result dicts, one per discrepancy (or one passing result).
    """
    raise NotImplementedError("schema check — coming soon")
