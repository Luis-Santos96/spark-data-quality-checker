import datetime

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType

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


# ---------------------------------------------------------------------------
# Schema checks
# ---------------------------------------------------------------------------

class TestSchemaChecks:
    def test_matching_schema_passes(self, spark):
        expected = StructType([
            StructField("id", IntegerType()),
            StructField("value", DoubleType()),
        ])
        df = spark.createDataFrame([Row(id=1, value=1.0)])
        results = DQChecker(df).check_schema(expected).run()
        assert not results.has_failures()

    def test_missing_column_fails(self, spark):
        expected = StructType([
            StructField("id", IntegerType()),
            StructField("missing_col", StringType()),
        ])
        df = spark.createDataFrame([Row(id=1)])
        results = DQChecker(df).check_schema(expected).run()
        failed = results.failed_checks()
        assert any(r["issue"] == "missing_column" for r in failed)

    def test_extra_column_fails(self, spark):
        expected = StructType([StructField("id", IntegerType())])
        df = spark.createDataFrame([Row(id=1, extra=99)])
        results = DQChecker(df).check_schema(expected).run()
        failed = results.failed_checks()
        assert any(r["issue"] == "extra_column" for r in failed)

    def test_type_mismatch_fails(self, spark):
        expected = StructType([StructField("id", DoubleType())])
        df = spark.createDataFrame([Row(id=1)])  # IntegerType in practice
        results = DQChecker(df).check_schema(expected).run()
        failed = results.failed_checks()
        assert any(r["issue"] == "type_mismatch" for r in failed)


# ---------------------------------------------------------------------------
# Row count checks
# ---------------------------------------------------------------------------

class TestRowCountChecks:
    def test_within_bounds_passes(self, clean_df):
        results = DQChecker(clean_df).check_row_count(min_rows=1, max_rows=10).run()
        assert not results.has_failures()

    def test_below_minimum_fails(self, clean_df):
        results = DQChecker(clean_df).check_row_count(min_rows=100).run()
        assert results.has_failures()
        assert results.to_dict()[0]["row_count"] == 3

    def test_above_maximum_fails(self, clean_df):
        results = DQChecker(clean_df).check_row_count(max_rows=2).run()
        assert results.has_failures()

    def test_exact_min_boundary_passes(self, clean_df):
        results = DQChecker(clean_df).check_row_count(min_rows=3).run()
        assert not results.has_failures()

    def test_exact_max_boundary_passes(self, clean_df):
        results = DQChecker(clean_df).check_row_count(max_rows=3).run()
        assert not results.has_failures()

    def test_no_bounds_raises(self, clean_df):
        with pytest.raises(ValueError):
            DQChecker(clean_df).check_row_count().run()


# ---------------------------------------------------------------------------
# Value range checks
# ---------------------------------------------------------------------------

class TestValueRangeChecks:
    def test_all_values_in_range_passes(self, clean_df):
        # amounts are 50, 75, 100
        results = DQChecker(clean_df).check_value_range("amount", min_value=0.0, max_value=200.0).run()
        assert not results.has_failures()

    def test_value_below_min_fails(self, clean_df):
        results = DQChecker(clean_df).check_value_range("amount", min_value=60.0).run()
        assert results.has_failures()
        assert results.to_dict()[0]["violation_count"] == 1  # 50.0 is below 60.0

    def test_value_above_max_fails(self, clean_df):
        results = DQChecker(clean_df).check_value_range("amount", max_value=90.0).run()
        assert results.has_failures()
        assert results.to_dict()[0]["violation_count"] == 1  # 100.0 exceeds 90.0

    def test_missing_column_fails(self, clean_df):
        results = DQChecker(clean_df).check_value_range("nonexistent", min_value=0.0).run()
        assert results.has_failures()
        assert results.to_dict()[0]["violation_count"] is None

    def test_no_bounds_raises(self, clean_df):
        with pytest.raises(ValueError):
            DQChecker(clean_df).check_value_range("amount").run()

    def test_only_min_bound(self, clean_df):
        results = DQChecker(clean_df).check_value_range("amount", min_value=0.0).run()
        assert not results.has_failures()

    def test_only_max_bound(self, clean_df):
        results = DQChecker(clean_df).check_value_range("amount", max_value=200.0).run()
        assert not results.has_failures()


# ---------------------------------------------------------------------------
# Freshness checks
# ---------------------------------------------------------------------------

class TestFreshnessChecks:
    def test_fresh_data_passes(self, spark):
        now = datetime.datetime.utcnow()
        recent = now - datetime.timedelta(hours=1)
        df = spark.createDataFrame([Row(ts=recent)])
        results = DQChecker(df).check_freshness("ts", max_age_hours=24).run()
        assert not results.has_failures()

    def test_stale_data_fails(self, spark):
        old = datetime.datetime.utcnow() - datetime.timedelta(hours=48)
        df = spark.createDataFrame([Row(ts=old)])
        results = DQChecker(df).check_freshness("ts", max_age_hours=24).run()
        assert results.has_failures()

    def test_missing_column_fails(self, spark):
        df = spark.createDataFrame([Row(id=1)])
        results = DQChecker(df).check_freshness("nonexistent_ts", max_age_hours=24).run()
        assert results.has_failures()
        assert results.to_dict()[0]["latest_timestamp"] is None

    def test_all_null_timestamps_fails(self, spark):
        df = spark.createDataFrame(
            [Row(ts=None)],
            schema="ts TIMESTAMP"
        )
        results = DQChecker(df).check_freshness("ts", max_age_hours=24).run()
        assert results.has_failures()

    def test_age_hours_recorded(self, spark):
        one_hour_ago = datetime.datetime.utcnow() - datetime.timedelta(hours=1)
        df = spark.createDataFrame([Row(ts=one_hour_ago)])
        results = DQChecker(df).check_freshness("ts", max_age_hours=24).run()
        record = results.to_dict()[0]
        assert record["age_hours"] == pytest.approx(1.0, abs=0.1)
