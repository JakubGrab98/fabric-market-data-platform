from datetime import date

from pyspark.sql import Row

from notebooks.quality.reconciliation.transforms import (
    compare_range,
    compare_row_counts,
    summarize_check_results,
)


def test_compare_row_counts_exact_passes_when_equal(spark):
    source_df = spark.createDataFrame([Row(x=1), Row(x=2)])
    target_df = spark.createDataFrame([Row(x=1), Row(x=2)])

    result = compare_row_counts(source_df, target_df, exact=True)

    assert result == {
        "passed": True,
        "source_count": 2,
        "target_count": 2,
        "difference": 0,
    }


def test_compare_row_counts_exact_fails_when_target_smaller(spark):
    source_df = spark.createDataFrame([Row(x=1), Row(x=2)])
    target_df = spark.createDataFrame([Row(x=1)])

    result = compare_row_counts(source_df, target_df, exact=True)

    assert result["passed"] is False
    assert result["difference"] == 1


def test_compare_row_counts_non_exact_passes_when_target_smaller(spark):
    source_df = spark.createDataFrame([Row(x=1), Row(x=2), Row(x=3)])
    target_df = spark.createDataFrame([Row(x=1)])

    result = compare_row_counts(source_df, target_df, exact=False)

    assert result["passed"] is True


def test_compare_row_counts_non_exact_fails_when_target_larger(spark):
    source_df = spark.createDataFrame([Row(x=1)])
    target_df = spark.createDataFrame([Row(x=1), Row(x=2)])

    result = compare_row_counts(source_df, target_df, exact=False)

    assert result["passed"] is False
    assert result["difference"] == -1


def test_compare_range_passes_when_ranges_match_same_column_name(spark):
    source_df = spark.createDataFrame([Row(date=date(2024, 1, 1)), Row(date=date(2024, 1, 5))])
    target_df = spark.createDataFrame([Row(date=date(2024, 1, 1)), Row(date=date(2024, 1, 5))])

    result = compare_range(source_df, target_df, "date", "date")

    assert result["passed"] is True
    assert result["source_min"] == date(2024, 1, 1)
    assert result["source_max"] == date(2024, 1, 5)


def test_compare_range_fails_when_target_range_narrower(spark):
    source_df = spark.createDataFrame([Row(date=date(2024, 1, 1)), Row(date=date(2024, 1, 5))])
    target_df = spark.createDataFrame([Row(date=date(2024, 1, 1))])

    result = compare_range(source_df, target_df, "date", "date")

    assert result["passed"] is False
    assert result["target_max"] == date(2024, 1, 1)


def test_compare_range_supports_different_column_names(spark):
    source_df = spark.createDataFrame([Row(date=date(2024, 1, 1))])
    target_df = spark.createDataFrame([Row(period_end_date=date(2024, 1, 1))])

    result = compare_range(source_df, target_df, "date", "period_end_date")

    assert result["passed"] is True


def test_summarize_check_results_all_passed():
    results = {
        "a": {"passed": True},
        "b": {"passed": True},
    }

    summary = summarize_check_results(results)

    assert summary == {"all_passed": True, "failed": []}


def test_summarize_check_results_lists_failed_checks():
    results = {
        "a": {"passed": True},
        "b": {"passed": False},
        "c": {"passed": False},
    }

    summary = summarize_check_results(results)

    assert summary["all_passed"] is False
    assert summary["failed"] == ["b", "c"]
