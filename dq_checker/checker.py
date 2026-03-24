from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from typing import List, Optional

from dq_checker.checks.nulls import check_nulls
from dq_checker.checks.duplicates import check_duplicates
from dq_checker.checks.schema import check_schema
from dq_checker.checks.volume import check_row_count
from dq_checker.checks.range import check_value_range
from dq_checker.checks.freshness import check_freshness
from dq_checker.reporting import DQResults


class DQChecker:
    """
    Fluent interface for running data quality checks on a PySpark DataFrame.

    Checks are registered via chained method calls and executed lazily when
    .run() is called.

    Example::

        results = (
            DQChecker(df)
            .check_nulls(columns=["customer_id", "order_date"], threshold=0.01)
            .check_duplicates(key_columns=["customer_id", "order_id"])
            .check_schema(expected_schema=my_schema)
            .check_row_count(min_rows=100)
            .check_value_range(column="amount", min_value=0.0)
            .check_freshness(timestamp_column="event_time", max_age_hours=24)
            .run()
        )
        results.summary()
    """

    def __init__(self, df: DataFrame):
        self.df = df
        self._pending: list = []

    # ------------------------------------------------------------------
    # Check registration (fluent)
    # ------------------------------------------------------------------

    def check_nulls(self, columns: List[str], threshold: float = 0.0) -> "DQChecker":
        """
        Register a null-detection check.

        Args:
            columns:   Columns to inspect for nulls.
            threshold: Max allowed null ratio per column [0.0, 1.0].
        """
        self._pending.append(("nulls", {"columns": columns, "threshold": threshold}))
        return self

    def check_duplicates(self, key_columns: List[str]) -> "DQChecker":
        """
        Register a duplicate-detection check.

        Args:
            key_columns: Columns that together form the primary key.
        """
        self._pending.append(("duplicates", {"key_columns": key_columns}))
        return self

    def check_schema(self, expected_schema: StructType) -> "DQChecker":
        """
        Register a schema drift check.

        Args:
            expected_schema: StructType the DataFrame must conform to.
        """
        self._pending.append(("schema", {"expected_schema": expected_schema}))
        return self

    def check_row_count(
        self,
        min_rows: Optional[int] = None,
        max_rows: Optional[int] = None,
    ) -> "DQChecker":
        """
        Register a row count validation check.

        Args:
            min_rows: Minimum acceptable row count (inclusive).
            max_rows: Maximum acceptable row count (inclusive).
        """
        self._pending.append(("row_count", {"min_rows": min_rows, "max_rows": max_rows}))
        return self

    def check_value_range(
        self,
        column: str,
        min_value: Optional[float] = None,
        max_value: Optional[float] = None,
    ) -> "DQChecker":
        """
        Register a value range check for a numeric column.

        Args:
            column:    Numeric column to validate.
            min_value: Inclusive lower bound (None = no lower bound).
            max_value: Inclusive upper bound (None = no upper bound).
        """
        self._pending.append(
            ("range", {"column": column, "min_value": min_value, "max_value": max_value})
        )
        return self

    def check_freshness(
        self,
        timestamp_column: str,
        max_age_hours: float,
    ) -> "DQChecker":
        """
        Register a data freshness check.

        Args:
            timestamp_column: Name of the timestamp column to inspect.
            max_age_hours:    Maximum allowed age of the latest record in hours.
        """
        self._pending.append(
            ("freshness", {"timestamp_column": timestamp_column, "max_age_hours": max_age_hours})
        )
        return self

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------

    def run(self) -> DQResults:
        """Execute all registered checks and return a DQResults object."""
        all_results: list = []

        for check_type, kwargs in self._pending:
            if check_type == "nulls":
                all_results.extend(check_nulls(self.df, **kwargs))
            elif check_type == "duplicates":
                all_results.extend(check_duplicates(self.df, **kwargs))
            elif check_type == "schema":
                all_results.extend(check_schema(self.df, **kwargs))
            elif check_type == "row_count":
                all_results.extend(check_row_count(self.df, **kwargs))
            elif check_type == "range":
                all_results.extend(check_value_range(self.df, **kwargs))
            elif check_type == "freshness":
                all_results.extend(check_freshness(self.df, **kwargs))

        self._pending.clear()
        return DQResults(all_results)
