"""Transform functions for the Silver fx_rates deduplication/standardization notebook."""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import col, row_number, to_date


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


def standardize_fx_rates(deduped_df: DataFrame) -> DataFrame:
    """Cast effective_date from a yyyy-MM-dd string to a proper date type.

    Args:
        deduped_df: Deduplicated DataFrame matching bronze_nbp_fx_rates' schema.
    Returns:
        DataFrame with effective_date as DateType; all other columns unchanged. Rows whose
        effective_date string fails to parse (to_date returns null) are dropped — a row with
        no valid date has no valid Silver identity and would otherwise silently duplicate on
        every MERGE INTO re-run (null-to-null never matches in SQL).
    """
    return deduped_df.withColumn(
        "effective_date", to_date(col("effective_date"), "yyyy-MM-dd")
    ).filter(col("effective_date").isNotNull())
