"""Transform functions for the Gold fact_fundamentals build notebook."""

from __future__ import annotations

from pyspark.sql import DataFrame

FACT_FUNDAMENTALS_COLUMNS = [
    "ticker",
    "period_end_date",
    "statement_type",
    "period_type",
    "fiscal_year",
    "metric_name",
    "metric_value",
    "reported_currency",
    "source",
    "retrieved_at",
]


def build_fact_fundamentals(silver_fundamentals_df: DataFrame) -> DataFrame:
    """Select fact_fundamentals' canonical columns, in the order defined in docs/data-model.md.

    silver_fundamentals is already at fact_fundamentals' long/EAV grain — see
    docs/data-model.md for why this is a long, not wide, fact — so this makes
    the Gold contract explicit rather than passing the Silver DataFrame
    through unchanged.

    Args:
        silver_fundamentals_df: DataFrame matching silver_fundamentals' schema.
    Returns:
        DataFrame with exactly FACT_FUNDAMENTALS_COLUMNS.
    """
    return silver_fundamentals_df.select(*FACT_FUNDAMENTALS_COLUMNS)
