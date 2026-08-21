"""Transform functions for the Silver fundamentals deduplication/standardization notebook."""

from __future__ import annotations

import re

from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import col, expr, lit, row_number, to_date, when

# FMP's documented envelope fields, present on every statement type — not
# unpivoted as metrics. Not verified against a live FMP response (no API key
# has been available in this project — see docs/next-steps.md); this is the
# one place to fix the list if a live call ever shows otherwise.
FMP_ENVELOPE_COLUMNS = {
    "date",
    "symbol",
    "reportedCurrency",
    "cik",
    "filingDate",
    "acceptedDate",
    "fiscalYear",
    "period",
    "ticker",
    "source",
    "retrieved_at",
}


def deduplicate_fundamentals(bronze_df: DataFrame) -> DataFrame:
    """Keep one row per (ticker, date, period, statement_type), latest retrieved_at wins.

    Args:
        bronze_df: Union of the three Bronze FMP statement tables, each with a
            statement_type column stamped on before the union.
    Returns:
        DataFrame with the same columns, one row per (ticker, date, period, statement_type).
    """
    window = Window.partitionBy("ticker", "date", "period", "statement_type").orderBy(
        col("retrieved_at").desc()
    )
    return (
        bronze_df.withColumn("_rn", row_number().over(window)).filter(col("_rn") == 1).drop("_rn")
    )


def standardize_fundamentals(deduped_df: DataFrame) -> DataFrame:
    """Cast the FMP period-end date to DateType and derive period_type from FMP's period field.

    Args:
        deduped_df: Deduplicated DataFrame with FMP's date/period columns.
    Returns:
        DataFrame with date as DateType and a new period_type column
        ("annual" when period == "FY", "quarter" otherwise). Rows whose date
        string fails to parse are dropped — a row with no valid date has no
        valid Silver identity and would otherwise silently duplicate on every
        MERGE INTO re-run (null-to-null never matches in SQL).
    """
    with_date = deduped_df.withColumn("date", to_date(col("date"), "yyyy-MM-dd")).filter(
        col("date").isNotNull()
    )
    return with_date.withColumn(
        "period_type", when(col("period") == "FY", lit("annual")).otherwise(lit("quarter"))
    )


def _camel_to_snake(name: str) -> str:
    """Convert a camelCase FMP field name to snake_case (e.g. totalAssets -> total_assets)."""
    s1 = re.sub(r"(.)([A-Z][a-z]+)", r"\1_\2", name)
    return re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s1).lower()


def unpivot_fundamentals_metrics(standardized_df: DataFrame) -> DataFrame:
    """Melt every FMP financial-statement field into long (metric_name, metric_value) rows.

    FMP's field set differs per statement type and can change between API
    revisions (Bronze infers it from the payload's field union rather than a
    fixed schema — see notebooks/bronze/fmp/transforms.py). Rather than
    hardcoding a fixed list of line items, every column that isn't part of
    FMP_ENVELOPE_COLUMNS (plus the statement_type/period_type columns this
    pipeline adds) is treated as a metric and unpivoted generically.

    Args:
        standardized_df: Output of standardize_fundamentals.
    Returns:
        DataFrame with columns: ticker, period_end_date, statement_type,
        period_type, fiscal_year, reported_currency, metric_name,
        metric_value, source, retrieved_at. Rows where the metric value
        isn't castable to double (or is genuinely null) are dropped.
    Raises:
        ValueError: if no metric columns remain after excluding the envelope
            (nothing to unpivot).
    """
    id_columns = FMP_ENVELOPE_COLUMNS | {"statement_type", "period_type"}
    metric_columns = [c for c in standardized_df.columns if c not in id_columns]
    if not metric_columns:
        raise ValueError("No metric columns found to unpivot")

    stack_args = ", ".join(f"'{_camel_to_snake(c)}', CAST(`{c}` AS DOUBLE)" for c in metric_columns)
    stack_expr = f"stack({len(metric_columns)}, {stack_args}) AS (metric_name, metric_value)"

    return standardized_df.select(
        col("ticker"),
        col("date").alias("period_end_date"),
        col("statement_type"),
        col("period_type"),
        col("fiscalYear").alias("fiscal_year"),
        col("reportedCurrency").alias("reported_currency"),
        col("source"),
        col("retrieved_at"),
        expr(stack_expr),
    ).filter(col("metric_value").isNotNull())
