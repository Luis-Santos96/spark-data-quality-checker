from pyspark.sql import DataFrame
from typing import List

from dq_checker.checks.nulls import check_nulls
from dq_checker.checks.duplicates import check_duplicates
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

        self._pending.clear()
        return DQResults(all_results)
