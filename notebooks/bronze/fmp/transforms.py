"""Transform functions for the FMP fundamentals Bronze ingestion notebook."""

from __future__ import annotations

from pathlib import Path

import requests
import yaml

FMP_BASE_URL = "https://financialmodelingprep.com/stable"


class FmpFetchError(RuntimeError):
    """Raised when the FMP API returns an unexpected error response."""


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
    if not response.ok:
        raise FmpFetchError(
            f"FMP request failed with {response.status_code} for url={url}: {response.text}"
        )
    payload = response.json()
    if not isinstance(payload, list):
        raise FmpFetchError(
            f"Expected a list response from FMP, got {type(payload)} for url={url}"
        )
    return payload
