"""Transform functions for the Silver prices deduplication/standardization notebook."""

from __future__ import annotations

from pathlib import Path

import yaml
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.functions import col, row_number, to_date


def deduplicate_prices(bronze_df: DataFrame) -> DataFrame:
    """Keep one row per (ticker, date), latest retrieved_at wins.

    Args:
        bronze_df: DataFrame matching bronze_stooq_prices' schema (ticker, date, open,
            high, low, close, volume, source, retrieved_at).
    Returns:
        DataFrame with the same columns, one row per (ticker, date).
    """
    window = Window.partitionBy("ticker", "date").orderBy(col("retrieved_at").desc())
    return (
        bronze_df.withColumn("_rn", row_number().over(window)).filter(col("_rn") == 1).drop("_rn")
    )


def standardize_prices(deduped_df: DataFrame) -> DataFrame:
    """Cast date from a yyyy-MM-dd string to a proper date type.

    Args:
        deduped_df: Deduplicated DataFrame matching bronze_stooq_prices' schema.
    Returns:
        DataFrame with date as DateType; all other columns unchanged. Rows whose date string
        fails to parse (to_date returns null) are dropped — a row with no valid date has no
        valid Silver identity and would otherwise silently duplicate on every MERGE INTO
        re-run (null-to-null never matches in SQL).
    """
    return deduped_df.withColumn("date", to_date(col("date"), "yyyy-MM-dd")).filter(
        col("date").isNotNull()
    )


def load_ticker_config(path: str | Path) -> list[dict]:
    """Load the ticker list used to look up each ticker's trading currency.

    Args:
        path: Path to a YAML file shaped like notebooks/config/tickers.yaml.
    Returns:
        List of ticker config dicts (ticker, stooq_symbol, fmp_symbol, company_name, currency).
    """
    with open(path, encoding="utf-8") as f:
        config = yaml.safe_load(f)
    return config["tickers"]


def add_currency_column(
    prices_df: DataFrame, tickers: list[dict], spark: SparkSession
) -> DataFrame:
    """Join each price row to its ticker's trading currency from config.

    No FX conversion is performed — currency is a passthrough label. A ticker missing from
    the config yields a null currency rather than dropping the row.

    Args:
        prices_df: DataFrame with at least a ticker column.
        tickers: Ticker config dicts from load_ticker_config (must have ticker, currency keys).
        spark: Active SparkSession, used to build the small lookup DataFrame.
    Returns:
        prices_df with a currency column added.
    """
    currency_lookup = spark.createDataFrame(
        [(t["ticker"], t["currency"]) for t in tickers], ["ticker", "currency"]
    )
    return prices_df.join(currency_lookup, on="ticker", how="left")
