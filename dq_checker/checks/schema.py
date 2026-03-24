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
    actual_fields = {f.name: f.dataType for f in df.schema.fields}
    expected_fields = {f.name: f.dataType for f in expected_schema.fields}

    results = []

    for name, expected_type in expected_fields.items():
        if name not in actual_fields:
            results.append({
                "check": "schema_check",
                "column": name,
                "issue": "missing_column",
                "expected_type": str(expected_type),
                "actual_type": None,
                "passed": False,
                "message": (
                    f"[schema_check:{name}] FAILED — column missing from DataFrame"
                ),
            })
        elif type(actual_fields[name]) != type(expected_type):
            results.append({
                "check": "schema_check",
                "column": name,
                "issue": "type_mismatch",
                "expected_type": str(expected_type),
                "actual_type": str(actual_fields[name]),
                "passed": False,
                "message": (
                    f"[schema_check:{name}] FAILED — type mismatch: "
                    f"expected {expected_type}, got {actual_fields[name]}"
                ),
            })
        else:
            results.append({
                "check": "schema_check",
                "column": name,
                "issue": None,
                "expected_type": str(expected_type),
                "actual_type": str(actual_fields[name]),
                "passed": True,
                "message": f"[schema_check:{name}] OK",
            })

    for name in actual_fields:
        if name not in expected_fields:
            results.append({
                "check": "schema_check",
                "column": name,
                "issue": "extra_column",
                "expected_type": None,
                "actual_type": str(actual_fields[name]),
                "passed": False,
                "message": (
                    f"[schema_check:{name}] FAILED — unexpected column in DataFrame"
                ),
            })

    return results
