from typing import List


class DQResults:
    """Container for data quality check results with summary reporting."""

    def __init__(self, results: List[dict]):
        self.results = results

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def summary(self) -> None:
        """Print a human-readable summary of all check results."""
        passed = [r for r in self.results if r["passed"]]
        failed = [r for r in self.results if not r["passed"]]

        print(f"\n{'=' * 60}")
        print("  Data Quality Report")
        print(f"{'=' * 60}")
        print(f"  Total checks : {len(self.results)}")
        print(f"  Passed       : {len(passed)}")
        print(f"  Failed       : {len(failed)}")
        print(f"{'=' * 60}")

        for result in self.results:
            status = "PASS" if result["passed"] else "FAIL"
            print(f"  [{status}] {result['message']}")

        print(f"{'=' * 60}\n")

    def has_failures(self) -> bool:
        """Return True if any check failed."""
        return any(not r["passed"] for r in self.results)

    def failed_checks(self) -> List[dict]:
        """Return only the failed check results."""
        return [r for r in self.results if not r["passed"]]

    def to_dict(self) -> List[dict]:
        """Return raw results as a list of dicts."""
        return self.results
