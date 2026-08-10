"""Transform functions for the FMP fundamentals Bronze ingestion notebook."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from pathlib import Path

import requests
import yaml
from pyspark.sql import DataFrame, Row, SparkSession

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
        FmpFetchError: on any non-2xx response or an unexpected (non-list) body.
    """
    response = requests.get(url, timeout=timeout)
    redacted_url = _redact_api_key_from_url(url)
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

    rows = []
    for record in records:
        merged = {key: record.get(key) for key in all_keys}
        merged["ticker"] = ticker
        merged["source"] = source
        merged["retrieved_at"] = retrieved_at_utc
        rows.append(Row(**merged))

    return spark.createDataFrame(rows)
