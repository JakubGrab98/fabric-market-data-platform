"""Transform functions for the Gold reconciliation (cross-layer data quality) notebook."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.functions import max as spark_max
from pyspark.sql.functions import min as spark_min


def compare_row_counts(source_df: DataFrame, target_df: DataFrame, *, exact: bool) -> dict:
    """Compare row counts between a source layer and the layer built from it.

    Args:
        source_df: The upstream DataFrame (e.g. Bronze or Silver).
        target_df: The downstream DataFrame built from source_df (e.g. Silver or Gold).
        exact: True if target_df must have exactly the same row count as source_df
            (a lossless passthrough, e.g. Silver -> Gold). False if target_df may have
            fewer rows than source_df (e.g. Bronze -> Silver, where deduplication
            legitimately drops duplicate retrievals) but never more.
    Returns:
        dict with passed (bool), source_count, target_count, difference (source - target).
    """
    source_count = source_df.count()
    target_count = target_df.count()
    passed = target_count == source_count if exact else target_count <= source_count
    return {
        "passed": passed,
        "source_count": source_count,
        "target_count": target_count,
        "difference": source_count - target_count,
    }


def compare_range(
    source_df: DataFrame, target_df: DataFrame, source_column: str, target_column: str
) -> dict:
    """Compare the [min, max] range of a column between a source layer and its target.

    Deduplication (Bronze -> Silver) drops duplicate retrievals of the same date/period,
    not distinct ones, so the range is expected to be identical across every layer
    transition this module checks — unlike row counts, there's no "exact" toggle here.
    source_column/target_column may differ (e.g. Bronze FMP's "date" becomes Silver's
    "period_end_date").

    Args:
        source_df: The upstream DataFrame.
        target_df: The downstream DataFrame.
        source_column: Name of the range column in source_df.
        target_column: Name of the range column in target_df.
    Returns:
        dict with passed (bool), source_min, source_max, target_min, target_max.
    """
    source_row = source_df.agg(
        spark_min(source_column).alias("min"), spark_max(source_column).alias("max")
    ).collect()[0]
    target_row = target_df.agg(
        spark_min(target_column).alias("min"), spark_max(target_column).alias("max")
    ).collect()[0]
    passed = source_row["min"] == target_row["min"] and source_row["max"] == target_row["max"]
    return {
        "passed": passed,
        "source_min": source_row["min"],
        "source_max": source_row["max"],
        "target_min": target_row["min"],
        "target_max": target_row["max"],
    }


def summarize_check_results(results: dict[str, dict]) -> dict:
    """Summarize named reconciliation check results into an overall pass/fail.

    Args:
        results: Mapping of check name -> result dict from compare_row_counts/compare_range
            (each must have a "passed" key).
    Returns:
        dict with all_passed (bool) and failed (list of check names that did not pass).
    """
    failed = [name for name, result in results.items() if not result["passed"]]
    return {"all_passed": not failed, "failed": failed}
