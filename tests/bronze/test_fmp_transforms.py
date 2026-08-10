from datetime import datetime, timezone

import pytest
from pyspark.sql import SparkSession

from notebooks.bronze.fmp.transforms import (
    build_balance_sheet_url,
    build_cash_flow_url,
    build_income_statement_url,
    load_ticker_config,
    parse_fmp_statement,
)


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    session = SparkSession.builder.master("local[1]").appName("tests").getOrCreate()
    session.conf.set("spark.sql.session.timeZone", "UTC")
    yield session
    session.stop()


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
