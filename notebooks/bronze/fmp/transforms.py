"""Transform functions for the FMP fundamentals Bronze ingestion notebook."""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path

import requests
import yaml
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import lit

FMP_BASE_URL = "https://financialmodelingprep.com/stable"


class FmpFetchError(RuntimeError):
    """Raised when the FMP API returns an unexpected error response."""


def _redact_api_key_from_url(url: str) -> str:
    """Redact the API key from an FMP URL to prevent leaking credentials in error messages."""
    return re.sub(r"apikey=[^&]*", "apikey=***", url)


def load_ticker_config(path: str | Path) -> list[dict]:
    """Load the ticker list used to parameterize fundamentals ingestion.

    Args:
        path: Path to a YAML file shaped like notebooks/config/tickers.yaml.
    Returns:
        List of ticker config dicts (ticker, stooq_symbol, fmp_symbol, company_name, currency).
    """
    with open(path, encoding="utf-8") as f:
        config = yaml.safe_load(f)
    return config["tickers"]


def _build_fmp_statement_url(
    statement_path: str, symbol: str, period_limit: int, api_key: str
) -> str:
    return (
        f"{FMP_BASE_URL}/{statement_path}"
        f"?symbol={symbol}&period=quarter&limit={period_limit}&apikey={api_key}"
    )


def build_balance_sheet_url(symbol: str, period_limit: int, api_key: str) -> str:
    """Build the FMP quarterly balance-sheet-statement URL for one symbol."""
    return _build_fmp_statement_url("balance-sheet-statement", symbol, period_limit, api_key)


def build_income_statement_url(symbol: str, period_limit: int, api_key: str) -> str:
    """Build the FMP quarterly income-statement URL for one symbol."""
    return _build_fmp_statement_url("income-statement", symbol, period_limit, api_key)


def build_cash_flow_url(symbol: str, period_limit: int, api_key: str) -> str:
    """Build the FMP quarterly cashflow-statement URL for one symbol."""
    return _build_fmp_statement_url("cashflow-statement", symbol, period_limit, api_key)


def fetch_fmp_statement(url: str, timeout: int = 30) -> list[dict]:
    """Fetch one statement payload from FMP.

    Returns:
        List of period records (empty list if FMP has no data for the symbol).
    Raises:
        FmpFetchError: on any non-2xx response, an unexpected (non-list) body,
            or a transport-level failure (connection error, timeout, etc.) —
            in all cases the API key is redacted from the raised message.
    """
    redacted_url = _redact_api_key_from_url(url)
    try:
        response = requests.get(url, timeout=timeout)
    except requests.RequestException as exc:
        raise FmpFetchError(
            f"FMP request failed for url={redacted_url}: {type(exc).__name__}"
        ) from None
    if not response.ok:
        raise FmpFetchError(
            f"FMP request failed with {response.status_code} for url={redacted_url}: {response.text}"
        )
    payload = response.json()
    if not isinstance(payload, list):
        raise FmpFetchError(
            f"Expected a list response from FMP, got {type(payload)} for url={redacted_url}"
        )
    return payload


def parse_fmp_statement(
    records: list[dict],
    ticker: str,
    source: str,
    retrieved_at: datetime,
    spark: SparkSession,
) -> DataFrame:
    """Parse a raw FMP statement payload into a Bronze DataFrame.

    Keeps every field FMP returns (Bronze is 1:1 with source) and stamps
    ticker/source/retrieved_at provenance columns on every row. Schema is
    inferred from the payload's field union rather than a fixed StructType,
    since FMP's field set differs across statement types and can change
    between API revisions.

    Records are serialized to JSON and parsed via spark.read.json rather
    than spark.createDataFrame(rows) — Spark's JSON schema inference
    handles two payload shapes that createDataFrame(list[Row]) cannot:
    a field that is None in every record (createDataFrame can't determine
    a type for an all-null column), and a field whose values are a mix of
    int and float across records (createDataFrame raises CANNOT_MERGE_TYPE;
    the JSON reader widens to double).

    Args:
        records: List of period dicts from fetch_fmp_statement (non-empty).
        ticker: Internal ticker symbol (not the FMP symbol) for this data.
        source: Source name to stamp on every row (e.g. "fmp").
        retrieved_at: Retrieval timestamp to stamp on every row.
        spark: Active SparkSession.
    Returns:
        DataFrame with FMP's fields (union across records) plus
        ticker/source/retrieved_at columns.
    Raises:
        ValueError: if records is empty (nothing to infer a schema from).
    """
    if not records:
        raise ValueError("records must be non-empty to build a DataFrame")

    retrieved_at_utc = retrieved_at.astimezone(timezone.utc).replace(tzinfo=None)

    all_keys: list[str] = []
    seen: set[str] = set()
    for record in records:
        for key in record:
            if key not in seen:
                seen.add(key)
                all_keys.append(key)

    row_dicts = [{key: record.get(key) for key in all_keys} for record in records]

    json_lines = [json.dumps(row_dict) for row_dict in row_dicts]
    rdd = spark.sparkContext.parallelize(json_lines)
    df = spark.read.json(rdd)

    # ticker/source/retrieved_at are stamped as literal columns (rather than
    # baked into the JSON payload) so the provenance timestamp round-trips
    # through Spark's native python-datetime <-> Timestamp conversion —
    # the same conversion createDataFrame(rows) used previously — instead
    # of a string round-trip through the JSON reader's timestamp parsing,
    # which is keyed off spark.sql.session.timeZone on the way in but the
    # JVM's local timezone on the way out via collect().
    return (
        df.withColumn("ticker", lit(ticker))
        .withColumn("source", lit(source))
        .withColumn("retrieved_at", lit(retrieved_at_utc))
    )
