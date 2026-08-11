"""Transform functions for the Silver fx_rates deduplication/standardization notebook."""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import col, row_number


def deduplicate_fx_rates(bronze_df: DataFrame) -> DataFrame:
    """Keep one row per (currency_code, effective_date), latest retrieved_at wins.

    Args:
        bronze_df: DataFrame matching bronze_nbp_fx_rates' schema (currency_code,
            effective_date, mid_rate, source, retrieved_at).
    Returns:
        DataFrame with the same columns, one row per (currency_code, effective_date).
    """
    window = Window.partitionBy("currency_code", "effective_date").orderBy(
        col("retrieved_at").desc()
    )
    return (
        bronze_df.withColumn("_rn", row_number().over(window)).filter(col("_rn") == 1).drop("_rn")
    )
