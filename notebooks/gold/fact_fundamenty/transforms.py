"""Transform functions for the Gold fact_fundamenty build notebook."""

from __future__ import annotations

from pyspark.sql import DataFrame

FACT_FUNDAMENTY_COLUMNS = [
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


def build_fact_fundamenty(silver_fundamentals_df: DataFrame) -> DataFrame:
    """Select fact_fundamenty's canonical columns, in the order defined in docs/data-model.md.

    silver_fundamentals is already at fact_fundamenty's long/EAV grain — see
    docs/data-model.md for why this is a long, not wide, fact — so this makes
    the Gold contract explicit rather than passing the Silver DataFrame
    through unchanged.

    Args:
        silver_fundamentals_df: DataFrame matching silver_fundamentals' schema.
    Returns:
        DataFrame with exactly FACT_FUNDAMENTY_COLUMNS.
    """
    return silver_fundamentals_df.select(*FACT_FUNDAMENTY_COLUMNS)
