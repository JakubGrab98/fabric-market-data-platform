"""Transform functions for the NBP FX-rates Bronze ingestion notebook."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import requests
import yaml
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

NBP_RATES_URL = "https://api.nbp.pl/api/exchangerates/rates/A"
# NBP rejects any single request spanning more than 367 days.
NBP_MAX_RANGE_DAYS = 367

BRONZE_FX_SCHEMA = StructType(
    [
        StructField("currency_code", StringType(), nullable=False),
        StructField("effective_date", StringType(), nullable=False),
        StructField("mid_rate", DoubleType(), nullable=False),
        StructField("source", StringType(), nullable=False),
        StructField("retrieved_at", TimestampType(), nullable=False),
    ]
)


class NbpFetchError(RuntimeError):
    """Raised when the NBP API returns an unexpected error response."""


def load_currency_config(path: str | Path) -> list[dict]:
    """Load the currency list used to parameterize FX ingestion.

    Args:
        path: Path to a YAML file shaped like notebooks/config/currencies.yaml.
    Returns:
        List of currency config dicts (code, name).
    """
    with open(path, encoding="utf-8") as f:
        config = yaml.safe_load(f)
    return config["currencies"]


def chunk_date_range(
    start_date: date, end_date: date, max_days: int = NBP_MAX_RANGE_DAYS
) -> list[tuple[date, date]]:
    """Split a date range into chunks no wider than max_days (inclusive).

    NBP's date-range endpoint 400s above 367 days in a single request.
    """
    chunks = []
    chunk_start = start_date
    while chunk_start <= end_date:
        chunk_end = min(chunk_start + timedelta(days=max_days - 1), end_date)
        chunks.append((chunk_start, chunk_end))
        chunk_start = chunk_end + timedelta(days=1)
    return chunks


def build_nbp_rates_url(code: str, start_date: date, end_date: date) -> str:
    """Build the NBP Table-A rates URL for one currency and date range."""
    return f"{NBP_RATES_URL}/{code}/{start_date:%Y-%m-%d}/{end_date:%Y-%m-%d}/?format=json"


def fetch_nbp_rates(url: str, timeout: int = 30) -> dict | None:
    """Fetch a currency's rates payload from NBP.

    Returns:
        Parsed JSON payload, or None if NBP has no data for the requested
        range (404 — e.g. a range covering only weekends/holidays).
    Raises:
        NbpFetchError: on any other non-2xx response.
    """
    response = requests.get(url, timeout=timeout)
    if response.status_code == 404:
        return None
    if not response.ok:
        raise NbpFetchError(
            f"NBP request failed with {response.status_code} for url={url}: {response.text}"
        )
    return response.json()


def parse_nbp_rates(
    payload: dict,
    source: str,
    retrieved_at: datetime,
    spark: SparkSession,
) -> DataFrame:
    """Parse one NBP Table-A rates payload into the Bronze FX schema.

    Args:
        payload: JSON payload from fetch_nbp_rates (not None).
        source: Source name to stamp on every row (e.g. "nbp").
        retrieved_at: Retrieval timestamp to stamp on every row.
        spark: Active SparkSession.
    Returns:
        DataFrame matching BRONZE_FX_SCHEMA.
    """
    # Spark converts naive Python datetimes using the JVM's local default
    # timezone on write, ignoring spark.sql.session.timeZone for this path —
    # normalize to a UTC wall-clock value here so the stored timestamp is
    # unambiguous regardless of the cluster's local timezone.
    retrieved_at_utc = retrieved_at.astimezone(timezone.utc).replace(tzinfo=None)

    code = payload["code"]
    rows = [
        (code, rate["effectiveDate"], float(rate["mid"]), source, retrieved_at_utc)
        for rate in payload["rates"]
    ]
    return spark.createDataFrame(rows, schema=BRONZE_FX_SCHEMA)
