from datetime import datetime, timezone
from unittest.mock import patch

import pytest
import requests

from notebooks.bronze.fmp.transforms import (
    FmpFetchError,
    build_balance_sheet_url,
    build_cash_flow_url,
    build_income_statement_url,
    fetch_fmp_statement,
    load_ticker_config,
    parse_fmp_statement,
)


def test_load_ticker_config(tmp_path):
    config_file = tmp_path / "tickers.yaml"
    config_file.write_text(
        "tickers:\n"
        "  - ticker: PKN\n"
        "    stooq_symbol: pkn.wa\n"
        "    fmp_symbol: PKN\n"
        "    company_name: PKN Orlen\n"
        "    currency: PLN\n",
        encoding="utf-8",
    )

    tickers = load_ticker_config(config_file)

    assert tickers == [
        {
            "ticker": "PKN",
            "stooq_symbol": "pkn.wa",
            "fmp_symbol": "PKN",
            "company_name": "PKN Orlen",
            "currency": "PLN",
        }
    ]


def test_build_balance_sheet_url():
    url = build_balance_sheet_url("PKN", 8, "test-key")
    assert (
        url == "https://financialmodelingprep.com/stable/balance-sheet-statement"
        "?symbol=PKN&period=quarter&limit=8&apikey=test-key"
    )


def test_build_income_statement_url():
    url = build_income_statement_url("PKN", 8, "test-key")
    assert (
        url == "https://financialmodelingprep.com/stable/income-statement"
        "?symbol=PKN&period=quarter&limit=8&apikey=test-key"
    )


def test_build_cash_flow_url():
    url = build_cash_flow_url("PKN", 8, "test-key")
    assert (
        url == "https://financialmodelingprep.com/stable/cashflow-statement"
        "?symbol=PKN&period=quarter&limit=8&apikey=test-key"
    )


SAMPLE_BALANCE_SHEET_RECORDS = [
    {
        "date": "2024-06-30",
        "symbol": "PKN",
        "reportedCurrency": "PLN",
        "totalAssets": 123456.0,
        "totalLiabilities": 65432.0,
        "totalStockholdersEquity": 58024.0,
    },
    {
        "date": "2024-03-31",
        "symbol": "PKN",
        "reportedCurrency": "PLN",
        "totalAssets": 119000.0,
        "totalLiabilities": 63000.0,
        "totalStockholdersEquity": 56000.0,
        "cashAndCashEquivalents": 4200.0,
    },
]


def test_parse_fmp_statement_stamps_provenance(spark):
    retrieved_at = datetime(2024, 7, 1, tzinfo=timezone.utc)

    df = parse_fmp_statement(SAMPLE_BALANCE_SHEET_RECORDS, "PKN", "fmp", retrieved_at, spark)
    rows = df.orderBy("date").collect()

    assert len(rows) == 2
    first, second = rows
    assert first.date == "2024-03-31"
    assert first.ticker == "PKN"
    assert first.source == "fmp"
    assert first.retrieved_at == retrieved_at.replace(tzinfo=None)
    assert first.cashAndCashEquivalents == 4200.0
    assert second.date == "2024-06-30"


def test_parse_fmp_statement_fills_missing_fields_with_null(spark):
    retrieved_at = datetime(2024, 7, 1, tzinfo=timezone.utc)

    df = parse_fmp_statement(SAMPLE_BALANCE_SHEET_RECORDS, "PKN", "fmp", retrieved_at, spark)
    rows = {row.date: row for row in df.collect()}

    # The 2024-06-30 record has no cashAndCashEquivalents in the source payload —
    # the column must still exist (union of all keys across records) and be null.
    assert rows["2024-06-30"].cashAndCashEquivalents is None


def test_parse_fmp_statement_empty_records_raises(spark):
    with pytest.raises(ValueError):
        parse_fmp_statement([], "PKN", "fmp", datetime.now(timezone.utc), spark)


def test_fetch_fmp_statement_connection_error_redacts_api_key():
    real_key = "super-secret-test-key"
    url = (
        "https://financialmodelingprep.com/stable/balance-sheet-statement"
        f"?symbol=PKN&period=quarter&limit=8&apikey={real_key}"
    )

    with patch("notebooks.bronze.fmp.transforms.requests.get") as mock_get:
        mock_get.side_effect = requests.ConnectionError(
            f"Failed to establish a new connection: apikey={real_key} unreachable"
        )
        with pytest.raises(FmpFetchError) as exc_info:
            fetch_fmp_statement(url)

    message = str(exc_info.value)
    assert real_key not in message
    assert "ConnectionError" in message


SAMPLE_RECORDS_WITH_ALL_NULL_FIELD = [
    {
        "date": "2024-06-30",
        "symbol": "PKN",
        "goodwillImpairment": None,
    },
    {
        "date": "2024-03-31",
        "symbol": "PKN",
        "goodwillImpairment": None,
    },
]


def test_parse_fmp_statement_handles_all_null_column(spark):
    retrieved_at = datetime(2024, 7, 1, tzinfo=timezone.utc)

    df = parse_fmp_statement(SAMPLE_RECORDS_WITH_ALL_NULL_FIELD, "PKN", "fmp", retrieved_at, spark)
    rows = {row.date: row for row in df.collect()}

    assert len(rows) == 2
    assert rows["2024-06-30"].goodwillImpairment is None
    assert rows["2024-03-31"].goodwillImpairment is None


SAMPLE_RECORDS_WITH_MIXED_NUMERIC_TYPES = [
    {
        "date": "2024-06-30",
        "symbol": "PKN",
        "epsdiluted": 123456,
    },
    {
        "date": "2024-03-31",
        "symbol": "PKN",
        "epsdiluted": 123456.5,
    },
]


def test_parse_fmp_statement_handles_mixed_int_and_float_column(spark):
    retrieved_at = datetime(2024, 7, 1, tzinfo=timezone.utc)

    df = parse_fmp_statement(
        SAMPLE_RECORDS_WITH_MIXED_NUMERIC_TYPES, "PKN", "fmp", retrieved_at, spark
    )
    rows = {row.date: row for row in df.collect()}

    assert len(rows) == 2
    assert rows["2024-06-30"].epsdiluted == 123456.0
    assert rows["2024-03-31"].epsdiluted == 123456.5
