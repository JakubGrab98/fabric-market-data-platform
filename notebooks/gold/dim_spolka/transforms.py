"""Transform functions for the Gold dim_spolka build notebook."""

from __future__ import annotations

from pathlib import Path

import yaml
from pyspark.sql import DataFrame, SparkSession


def load_ticker_config(path: str | Path) -> list[dict]:
    """Load the ticker list used to build dim_spolka.

    Args:
        path: Path to a YAML file shaped like notebooks/config/tickers.yaml.
    Returns:
        List of ticker config dicts (ticker, stooq_symbol, fmp_symbol, company_name, currency).
    """
    with open(path, encoding="utf-8") as f:
        config = yaml.safe_load(f)
    return config["tickers"]


def build_dim_spolka(tickers: list[dict], spark: SparkSession) -> DataFrame:
    """Build dim_spolka directly from ticker config.

    dim_spolka is static reference data, not something ingested through
    Bronze/Silver — see docs/data-model.md — so this reads notebooks/config/
    tickers.yaml directly rather than a Silver table.

    Args:
        tickers: Ticker config dicts from load_ticker_config.
        spark: Active SparkSession, used to build the DataFrame.
    Returns:
        DataFrame with columns: ticker, company_name, listing_currency, fmp_symbol.
    """
    rows = [(t["ticker"], t["company_name"], t["currency"], t.get("fmp_symbol")) for t in tickers]
    return spark.createDataFrame(rows, ["ticker", "company_name", "listing_currency", "fmp_symbol"])
