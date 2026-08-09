# FMP Fundamentals Bronze Ingestion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a Bronze ingestion notebook that pulls quarterly balance sheet, income statement,
and cash flow data from Financial Modeling Prep (FMP) for the tickers in
`notebooks/config/tickers.yaml`, following the existing NBP/Stooq notebook pattern.

**Architecture:** Thin `notebook.py` (Fabric notebook, cell-based) imports pure functions from a
sibling `transforms.py`. Three FMP statement endpoints → three separate Bronze Delta tables (each
statement type has a different, wide field set — unlike GUS's uniform shape, these don't share a
schema). Schema is inferred from the API response per row rather than hand-maintained as a fixed
`StructType`, because FMP's field set (~30-50 columns per statement) isn't fully known ahead of
implementation and can change between FMP API revisions — see Task 3.

**Tech Stack:** Python 3.11, PySpark (local `local[1]` SparkSession in tests, Fabric runtime in
production), `requests`, `pyyaml`, `pytest`.

## Global Constraints

- Idempotent notebooks: Bronze landing tables are append-only by convention (dedup happens in
  Silver) — matches the existing NBP/Stooq notebooks, no upsert logic needed here.
- No hardcoded IDs: the FMP API key and the ticker→FMP-symbol mapping must come from a notebook
  parameter / config file, never literals in code.
- Every raw record keeps `source` and `retrieved_at` columns (CLAUDE.md, non-optional).
- Type hints required on every function in `/notebooks/**/transforms.py`.
- One-line docstring (+ Args/Returns if non-trivial) on public transform functions only.
- Formatting/linting: `ruff check .` and `black .` (line-length 100) must pass before each commit.
- Conventional Commits prefix + short imperative summary (`feat:`, `test:`, `chore:`).
- Atomic commits — one logical change per commit.
- Current branch is `feature/repo-skeleton`; commit directly there (matches how the existing
  NBP/Stooq notebooks were added) unless told otherwise.

---

### Task 1: Ticker config — add `fmp_symbol`

**Files:**
- Modify: `notebooks/config/tickers.yaml`
- Create: `notebooks/bronze/fmp/__init__.py`
- Create: `notebooks/bronze/fmp/transforms.py`
- Test: `tests/bronze/test_fmp_transforms.py`

**Interfaces:**
- Produces: `load_ticker_config(path: str | Path) -> list[dict]` — each dict has keys `ticker`,
  `stooq_symbol`, `fmp_symbol`, `company_name`, `currency`. Consumed by Task 4's `notebook.py`.

- [ ] **Step 1: Add `fmp_symbol` to the ticker config**

Edit `notebooks/config/tickers.yaml` so every entry has an `fmp_symbol` field. FMP uses plain
tickers for GPW-listed companies (no `.wa`-style suffix like Stooq) — this is FMP's documented
convention for Warsaw Stock Exchange listings:

```yaml
# Ticker universe for batch ingestion notebooks.
# stooq_symbol follows Stooq's convention: <ticker>.<market> (GPW = "wa").
# fmp_symbol follows FMP's convention: plain ticker, no market suffix, for GPW listings.
tickers:
  - ticker: PKN
    stooq_symbol: pkn.wa
    fmp_symbol: PKN
    company_name: "PKN Orlen"
    currency: PLN
  - ticker: PKO
    stooq_symbol: pko.wa
    fmp_symbol: PKO
    company_name: "PKO Bank Polski"
    currency: PLN
  - ticker: CDR
    stooq_symbol: cdr.wa
    fmp_symbol: CDR
    company_name: "CD Projekt"
    currency: PLN
```

Before trusting `fmp_symbol` in a real run, verify each one resolves via
`https://financialmodelingprep.com/stable/search-symbol?query=PKN&apikey=<key>` (or the FMP UI) —
FMP's coverage of GPW tickers isn't guaranteed to match Stooq's, and this is a genuine unverified
assumption, not a confirmed fact.

- [ ] **Step 2: Write the failing test for the config loader**

Create `tests/bronze/test_fmp_transforms.py`:

```python
from datetime import datetime, timezone

import pytest
from pyspark.sql import SparkSession

from notebooks.bronze.fmp.transforms import load_ticker_config


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
```

- [ ] **Step 3: Run the test to verify it fails**

Run: `pytest tests/bronze/test_fmp_transforms.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'notebooks.bronze.fmp'`

- [ ] **Step 4: Create the package and the loader**

Create `notebooks/bronze/fmp/__init__.py` (empty file, matching `notebooks/bronze/nbp/__init__.py`
and `notebooks/bronze/stooq/__init__.py`).

Create `notebooks/bronze/fmp/transforms.py`:

```python
"""Transform functions for the FMP fundamentals Bronze ingestion notebook."""

from __future__ import annotations

from pathlib import Path

import yaml


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
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `pytest tests/bronze/test_fmp_transforms.py -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add notebooks/config/tickers.yaml notebooks/bronze/fmp/__init__.py notebooks/bronze/fmp/transforms.py tests/bronze/test_fmp_transforms.py
git commit -m "feat: add fmp_symbol to ticker config and FMP config loader"
```

---

### Task 2: URL builders and fetch function

**Files:**
- Modify: `notebooks/bronze/fmp/transforms.py`
- Test: `tests/bronze/test_fmp_transforms.py`

**Interfaces:**
- Consumes: none from other tasks.
- Produces: `build_balance_sheet_url(symbol: str, period_limit: int, api_key: str) -> str`,
  `build_income_statement_url(symbol: str, period_limit: int, api_key: str) -> str`,
  `build_cash_flow_url(symbol: str, period_limit: int, api_key: str) -> str`,
  `fetch_fmp_statement(url: str, timeout: int = 30) -> list[dict]`,
  `class FmpFetchError(RuntimeError)`. All consumed by Task 4's `notebook.py`.

`fetch_fmp_statement` has no dedicated unit test in this task — it's a thin `requests` wrapper
around live HTTP, and the existing codebase doesn't unit-test the equivalent functions either
(`fetch_nbp_rates` in `notebooks/bronze/nbp/transforms.py`, `fetch_stooq_csv` in
`notebooks/bronze/stooq/transforms.py` are both untested for the same reason). Its error-handling
path is exercised implicitly by the manual verification in Step 5.

- [ ] **Step 1: Write the failing tests for the URL builders**

Append to `tests/bronze/test_fmp_transforms.py`:

```python
from notebooks.bronze.fmp.transforms import (
    build_balance_sheet_url,
    build_cash_flow_url,
    build_income_statement_url,
    load_ticker_config,
)


def test_build_balance_sheet_url():
    url = build_balance_sheet_url("PKN", 8, "test-key")
    assert (
        url
        == "https://financialmodelingprep.com/stable/balance-sheet-statement"
        "?symbol=PKN&period=quarter&limit=8&apikey=test-key"
    )


def test_build_income_statement_url():
    url = build_income_statement_url("PKN", 8, "test-key")
    assert (
        url
        == "https://financialmodelingprep.com/stable/income-statement"
        "?symbol=PKN&period=quarter&limit=8&apikey=test-key"
    )


def test_build_cash_flow_url():
    url = build_cash_flow_url("PKN", 8, "test-key")
    assert (
        url
        == "https://financialmodelingprep.com/stable/cashflow-statement"
        "?symbol=PKN&period=quarter&limit=8&apikey=test-key"
    )
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pytest tests/bronze/test_fmp_transforms.py -v`
Expected: FAIL — `ImportError: cannot import name 'build_balance_sheet_url'`

- [ ] **Step 3: Implement the URL builders and fetch function**

Add to `notebooks/bronze/fmp/transforms.py` (below the existing `load_ticker_config`):

```python
import requests

FMP_BASE_URL = "https://financialmodelingprep.com/stable"


class FmpFetchError(RuntimeError):
    """Raised when the FMP API returns an unexpected error response."""


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
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `pytest tests/bronze/test_fmp_transforms.py -v`
Expected: PASS (4 tests: the 1 from Task 1 + 3 new)

- [ ] **Step 5: Manually verify the endpoint shape against a live response**

This step requires a real FMP API key (free tier: register at financialmodelingprep.com). Run
once, locally, before relying on this notebook for a real ingestion — the endpoint path
(`stable/balance-sheet-statement` etc., migrated from the older `api/v3/` path) and the `limit`
parameter's exact behavior are believed correct from FMP's current docs but have not been
exercised against a live response in this environment:

```bash
curl -s "https://financialmodelingprep.com/stable/balance-sheet-statement?symbol=AAPL&period=quarter&limit=1&apikey=YOUR_KEY" | python -m json.tool | head -40
```

Confirm the response is a JSON array of objects (not an error object) and note any field-name
surprises for Task 3.

- [ ] **Step 6: Commit**

```bash
git add notebooks/bronze/fmp/transforms.py tests/bronze/test_fmp_transforms.py
git commit -m "feat: add FMP statement URL builders and fetch function"
```

---

### Task 3: Parse FMP statement payloads into Bronze DataFrames

**Files:**
- Modify: `notebooks/bronze/fmp/transforms.py`
- Test: `tests/bronze/test_fmp_transforms.py`

**Interfaces:**
- Consumes: nothing from Tasks 1-2 directly (takes already-fetched `records: list[dict]`).
- Produces: `parse_fmp_statement(records: list[dict], ticker: str, source: str, retrieved_at:
  datetime, spark: SparkSession) -> DataFrame`. Consumed by Task 4's `notebook.py`.

Schema is inferred per-call from whichever fields are present in `records`, rather than a fixed
`StructType` — FMP's statement payloads are wide (~30-50 fields) and the exact set isn't pinned
down in this plan (see Task 2 Step 5). Keeping every field the API returns satisfies the
"Bronze is 1:1 with source" rule without hand-maintaining three large schemas that would silently
drop any field FMP adds or renames. Records within one payload are normalized to a common key set
first (missing keys filled with `None`) so `spark.createDataFrame` doesn't choke on inconsistent
per-row shapes across quarters.

- [ ] **Step 1: Write the failing tests**

Append to `tests/bronze/test_fmp_transforms.py`:

```python
from notebooks.bronze.fmp.transforms import parse_fmp_statement

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
```

Add `import pytest` to the top of the test file if not already present from Task 1 (it is not —
Task 1's test file only imports `datetime`/`SparkSession`; add `import pytest` now).

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pytest tests/bronze/test_fmp_transforms.py -v`
Expected: FAIL — `ImportError: cannot import name 'parse_fmp_statement'`

- [ ] **Step 3: Implement `parse_fmp_statement`**

Add to `notebooks/bronze/fmp/transforms.py`:

```python
from datetime import datetime, timezone

from pyspark.sql import DataFrame, Row, SparkSession


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
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `pytest tests/bronze/test_fmp_transforms.py -v`
Expected: PASS (7 tests total)

- [ ] **Step 5: Commit**

```bash
git add notebooks/bronze/fmp/transforms.py tests/bronze/test_fmp_transforms.py
git commit -m "feat: parse FMP statement payloads into Bronze DataFrames"
```

---

### Task 4: Assemble the notebook

**Files:**
- Create: `notebooks/bronze/fmp/notebook.py`
- Modify: nothing else (no new tests — `notebook.py` glues Tasks 1-3's tested functions together
  and isn't itself unit-testable, matching `notebooks/bronze/nbp/notebook.py` and
  `notebooks/bronze/stooq/notebook.py`, neither of which has a dedicated test file).

**Interfaces:**
- Consumes: `load_ticker_config`, `build_balance_sheet_url`, `build_income_statement_url`,
  `build_cash_flow_url`, `fetch_fmp_statement`, `parse_fmp_statement` (all from Tasks 1-3).

- [ ] **Step 1: Write `notebook.py`**

Create `notebooks/bronze/fmp/notebook.py`, mirroring the cell structure of
`notebooks/bronze/stooq/notebook.py`:

```python
# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# MARKDOWN ********************

# ## Bronze — FMP fundamentals
#
# Ingests quarterly balance sheet, income statement, and cash flow data from
# Financial Modeling Prep for the tickers in
# `notebooks/config/tickers.yaml`, and appends to three Bronze landing tables
# (raw, 1:1 with source — append-only, deduplicated later in Silver).
#
# Unlike the date-range parameters on the NBP/Stooq notebooks, FMP's
# statement endpoints return the most recent N periods via `period_limit`
# rather than a start/end date.
#
# Logic lives in `transforms.py` next to this notebook; this cell stays thin.

# CELL ********************

from datetime import datetime, timezone

from transforms import (
    build_balance_sheet_url,
    build_cash_flow_url,
    build_income_statement_url,
    fetch_fmp_statement,
    load_ticker_config,
    parse_fmp_statement,
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# PARAMETERS CELL — override via Data Factory pipeline / notebook run parameters.
fmp_api_key: str = ""
period_limit: int = 8
ticker_config_path: str = "notebooks/config/tickers.yaml"
balance_sheet_table_name: str = "bronze_fmp_balance_sheet"
income_statement_table_name: str = "bronze_fmp_income_statement"
cash_flow_table_name: str = "bronze_fmp_cash_flow"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.conf.set("spark.sql.session.timeZone", "UTC")

source = "fmp"
retrieved_at = datetime.now(timezone.utc)

tickers = load_ticker_config(ticker_config_path)

statements = [
    (build_balance_sheet_url, balance_sheet_table_name),
    (build_income_statement_url, income_statement_table_name),
    (build_cash_flow_url, cash_flow_table_name),
]

for build_url, table_name in statements:
    frames = []
    for entry in tickers:
        url = build_url(entry["fmp_symbol"], period_limit, fmp_api_key)
        records = fetch_fmp_statement(url)
        if not records:
            continue
        frames.append(parse_fmp_statement(records, entry["ticker"], source, retrieved_at, spark))

    if not frames:
        continue

    bronze_df = frames[0]
    for frame in frames[1:]:
        bronze_df = bronze_df.unionByName(frame, allowMissingColumns=True)

    # Bronze landing table is append-only by convention — dedup happens in Silver.
    bronze_df.write.format("delta").mode("append").saveAsTable(table_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
```

- [ ] **Step 2: Run the full test suite**

Run: `pytest tests/ -v`
Expected: PASS — all FMP tests plus the pre-existing NBP/Stooq tests (16 total: 9 existing + 7
new).

- [ ] **Step 3: Run lint and formatting checks**

Run: `ruff check . && black --check .`
Expected: both report no issues. If `black --check` fails, run `black .` and re-check.

- [ ] **Step 4: Commit**

```bash
git add notebooks/bronze/fmp/notebook.py
git commit -m "feat: add FMP Bronze ingestion notebook"
```
