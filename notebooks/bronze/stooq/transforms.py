"""Transform functions for the Stooq daily-prices Bronze ingestion notebook."""

from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path

import requests
import yaml
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

STOOQ_CSV_URL = "https://stooq.com/q/d/l/"
# Header verified against Stooq's documented daily CSV export. Not yet
# re-verified live from this environment (blocked by Stooq's anti-bot JS
# challenge here) — confirm against a real Fabric run before trusting silently.
EXPECTED_STOOQ_COLUMNS = ["Date", "Open", "High", "Low", "Close", "Volume"]

BRONZE_PRICES_SCHEMA = StructType(
    [
        StructField("ticker", StringType(), nullable=False),
        StructField("date", StringType(), nullable=False),
        StructField("open", DoubleType(), nullable=True),
        StructField("high", DoubleType(), nullable=True),
        StructField("low", DoubleType(), nullable=True),
        StructField("close", DoubleType(), nullable=True),
        StructField("volume", LongType(), nullable=True),
        StructField("source", StringType(), nullable=False),
        StructField("retrieved_at", TimestampType(), nullable=False),
    ]
)


class StooqFetchError(RuntimeError):
    """Raised when Stooq returns something other than the expected CSV."""


def load_ticker_config(path: str | Path) -> list[dict]:
    """Load the ticker/company list used to parameterize ingestion.

    Args:
        path: Path to a YAML file shaped like notebooks/config/tickers.yaml.
    Returns:
        List of ticker config dicts (ticker, stooq_symbol, company_name, currency).
    """
    with open(path, encoding="utf-8") as f:
        config = yaml.safe_load(f)
    return config["tickers"]


def build_stooq_csv_url(stooq_symbol: str, start_date: date, end_date: date) -> str:
    """Build the Stooq daily-history CSV download URL for one symbol."""
    return f"{STOOQ_CSV_URL}?s={stooq_symbol}" f"&d1={start_date:%Y%m%d}&d2={end_date:%Y%m%d}&i=d"


def fetch_stooq_csv(url: str, timeout: int = 30) -> str:
    """Fetch raw CSV text from a Stooq download URL.

    Raises:
        StooqFetchError: if the response isn't CSV (e.g. Stooq's anti-bot
            JS challenge page, or an empty/error body).
    """
    response = requests.get(url, timeout=timeout)
    response.raise_for_status()
    text = response.text.strip()
    if not text or text.lstrip().startswith("<"):
        raise StooqFetchError(f"Expected CSV from Stooq, got non-CSV content for url={url}")
    return text


def parse_stooq_prices_csv(
    csv_text: str,
    ticker: str,
    source: str,
    retrieved_at: datetime,
    spark: SparkSession,
) -> DataFrame:
    """Parse raw Stooq daily-prices CSV text into the Bronze prices schema.

    Args:
        csv_text: Raw CSV body from fetch_stooq_csv.
        ticker: Internal ticker symbol (not the Stooq symbol) for this data.
        source: Source name to stamp on every row (e.g. "stooq").
        retrieved_at: Retrieval timestamp to stamp on every row.
        spark: Active SparkSession.
    Returns:
        DataFrame matching BRONZE_PRICES_SCHEMA.
    Raises:
        StooqFetchError: if the CSV header doesn't match EXPECTED_STOOQ_COLUMNS.
    """
    # Spark converts naive Python datetimes using the JVM's local default
    # timezone on write, ignoring spark.sql.session.timeZone for this path —
    # normalize to a UTC wall-clock value here so the stored timestamp is
    # unambiguous regardless of the cluster's local timezone.
    retrieved_at_utc = retrieved_at.astimezone(timezone.utc).replace(tzinfo=None)

    lines = csv_text.splitlines()
    header = lines[0].split(",")
    if header != EXPECTED_STOOQ_COLUMNS:
        raise StooqFetchError(
            f"Unexpected Stooq CSV header {header}, expected {EXPECTED_STOOQ_COLUMNS}"
        )

    rows = []
    for line in lines[1:]:
        if not line.strip():
            continue
        d, o, h, low_, c, v = line.split(",")
        rows.append(
            (
                ticker,
                d,
                float(o),
                float(h),
                float(low_),
                float(c),
                int(v),
                source,
                retrieved_at_utc,
            )
        )

    return spark.createDataFrame(rows, schema=BRONZE_PRICES_SCHEMA)
