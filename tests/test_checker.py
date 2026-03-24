import pytest
from pyspark.sql import SparkSession
from pyspark.sql import Row

from dq_checker import DQChecker


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[1]")
        .appName("dq_checker_tests")
        .getOrCreate()
    )


@pytest.fixture()
def clean_df(spark):
    data = [
        Row(customer_id=1, order_id=101, amount=50.0,  order_date="2024-01-01"),
        Row(customer_id=2, order_id=102, amount=75.0,  order_date="2024-01-02"),
        Row(customer_id=3, order_id=103, amount=100.0, order_date="2024-01-03"),
    ]
    return spark.createDataFrame(data)


@pytest.fixture()
def null_df(spark):
    data = [
        Row(customer_id=1,    order_id=101, amount=50.0),
        Row(customer_id=None, order_id=102, amount=75.0),
        Row(customer_id=None, order_id=103, amount=None),
    ]
    return spark.createDataFrame(data)


@pytest.fixture()
def duplicate_df(spark):
    data = [
        Row(customer_id=1, order_id=101),
        Row(customer_id=1, order_id=101),   # duplicate
        Row(customer_id=2, order_id=102),
    ]
    return spark.createDataFrame(data)


# ---------------------------------------------------------------------------
# Null checks
# ---------------------------------------------------------------------------

class TestNullChecks:
    def test_no_nulls_passes(self, clean_df):
        results = DQChecker(clean_df).check_nulls(["customer_id"]).run()
        assert not results.has_failures()

    def test_nulls_exceed_threshold_fails(self, null_df):
        results = DQChecker(null_df).check_nulls(["customer_id"], threshold=0.0).run()
        assert results.has_failures()

    def test_nulls_within_threshold_passes(self, null_df):
        # 2/3 ≈ 0.67 — set threshold above that
        results = DQChecker(null_df).check_nulls(["customer_id"], threshold=0.70).run()
        assert not results.has_failures()

    def test_missing_column_fails(self, clean_df):
        results = DQChecker(clean_df).check_nulls(["nonexistent_col"]).run()
        assert results.has_failures()

    def test_null_ratio_value(self, null_df):
        results = DQChecker(null_df).check_nulls(["customer_id"]).run()
        record = results.to_dict()[0]
        assert record["null_count"] == 2
        assert record["null_ratio"] == pytest.approx(0.6667, abs=1e-3)

    def test_empty_dataframe_passes(self, spark):
        empty_df = spark.createDataFrame([], schema="customer_id INT")
        results = DQChecker(empty_df).check_nulls(["customer_id"]).run()
        assert not results.has_failures()


# ---------------------------------------------------------------------------
# Duplicate checks
# ---------------------------------------------------------------------------

class TestDuplicateChecks:
    def test_no_duplicates_passes(self, clean_df):
        results = DQChecker(clean_df).check_duplicates(["customer_id"]).run()
        assert not results.has_failures()

    def test_duplicates_detected(self, duplicate_df):
        results = DQChecker(duplicate_df).check_duplicates(["customer_id", "order_id"]).run()
        assert results.has_failures()
        record = results.to_dict()[0]
        assert record["duplicate_count"] == 1

    def test_missing_key_column_fails(self, clean_df):
        results = DQChecker(clean_df).check_duplicates(["nonexistent_col"]).run()
        assert results.has_failures()

    def test_composite_key_no_duplicates(self, clean_df):
        results = (
            DQChecker(clean_df)
            .check_duplicates(["customer_id", "order_id"])
            .run()
        )
        assert not results.has_failures()


# ---------------------------------------------------------------------------
# Chaining
# ---------------------------------------------------------------------------

class TestChaining:
    def test_multiple_checks_all_pass(self, clean_df):
        results = (
            DQChecker(clean_df)
            .check_nulls(["customer_id", "order_id"])
            .check_duplicates(["customer_id"])
            .run()
        )
        assert not results.has_failures()
        assert len(results.to_dict()) == 3  # 2 null checks + 1 dup check

    def test_mixed_pass_fail(self, null_df):
        results = (
            DQChecker(null_df)
            .check_nulls(["order_id"], threshold=0.0)   # passes (no nulls)
            .check_nulls(["customer_id"], threshold=0.0)  # fails
            .run()
        )
        assert results.has_failures()
        assert len(results.failed_checks()) == 1
