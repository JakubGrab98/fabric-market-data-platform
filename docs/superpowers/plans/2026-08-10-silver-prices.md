# Silver prices Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a Silver notebook that deduplicates and standardizes `bronze_stooq_prices` into an
idempotent, upserted `silver_prices` Delta table, tagged with each ticker's trading currency.

**Architecture:** Thin `notebook.py` (Fabric notebook, cell-based) imports pure functions from a
sibling `transforms.py`. Every run: read the whole `bronze_stooq_prices` table, deduplicate to one
row per `(ticker, date)` keeping the latest `retrieved_at`, cast `date` to a real date, join in
`currency` from `notebooks/config/tickers.yaml`, then `MERGE INTO` the Silver Delta table
(idempotent upsert, not append). Dedup/standardize/currency-join are pure Spark logic and live in
`transforms.py` (testable); the `MERGE INTO` call needs a real Delta-enabled session and stays
untested in `notebook.py`, matching this repo's existing convention for Delta writes.

**Tech Stack:** Python 3.11, PySpark (local `local[1]` SparkSession in tests, Fabric runtime in
production), `pyyaml`, `pytest`.

## Global Constraints

- Idempotent notebooks: "Every ... transform notebook must be safe to re-run for the same
  date/parameters without creating duplicates (upsert/merge into Delta, not blind append)" — this
  is the opposite of Bronze's append-only rule. `silver_prices` is written via `MERGE INTO` keyed
  on `(ticker, date)`, never `mode("append")`.
- No hardcoded IDs: table names, paths, and the ticker→currency mapping all come from parameters
  / config, never literals in code.
- Type hints required on every function in `/notebooks/**/transforms.py`.
- One-line docstring (+ Args/Returns if non-trivial) on public transform functions only.
- Formatting/linting: `ruff check .` and `black .` (line-length 100) must pass before each commit.
- Conventional Commits prefix + short imperative summary (`feat:`, `test:`, `chore:`).
- Atomic commits — one logical change per commit.
- No FX conversion in this plan — `currency` is a passthrough column from config, not a converted
  value. Every ticker in `notebooks/config/tickers.yaml` trades in PLN today; there is nothing to
  convert yet (see the design spec for the full rationale).
- Do not add `delta-spark` (or any Delta-Lake Python package) as a project dependency — same
  reasoning as the sibling `silver_fx_rates` plan.
- Do not extract a shared `notebooks/silver/common.py` or reuse `load_ticker_config` from
  `notebooks/bronze/stooq/transforms.py` or `notebooks/bronze/fmp/transforms.py` — duplicate a
  minimal loader in this module's own `transforms.py`, matching the existing repo-wide pattern
  (this is a deliberate spec decision, not an oversight — see
  `docs/superpowers/specs/2026-08-10-phase2-silver-fx-prices-design.md`).
- Work happens on branch `feature/phase2-silver-transformation` (already checked out at the repo
  root — no worktree needed for this plan unless the executor prefers one).

---

### Task 1: Deduplicate prices by natural key

**Files:**
- Create: `notebooks/silver/prices/__init__.py`
- Create: `notebooks/silver/prices/transforms.py`
- Test: `tests/silver/test_prices_transforms.py`

**Interfaces:**
- Produces: `deduplicate_prices(bronze_df: DataFrame) -> DataFrame` — same columns as
  `bronze_stooq_prices` (`ticker`, `date`, `open`, `high`, `low`, `close`, `volume`, `source`,
  `retrieved_at`), one row per `(ticker, date)`, the row with the latest `retrieved_at` wins.
  Consumed by Task 4's `notebook.py`.

Note: `notebooks/silver/__init__.py` already exists if the sibling `silver_fx_rates` plan ran
first; create it here too if it doesn't (an empty file, safe to no-op if already present).

- [ ] **Step 1: Write the failing test**

Create `tests/silver/test_prices_transforms.py`. If `tests/silver/__init__.py` doesn't already
exist (e.g. because the sibling `silver_fx_rates` plan already created it), create it empty first
— it's a no-op if it's already there.

```python
from datetime import datetime, timezone

import pytest
from pyspark.sql import Row, SparkSession

from notebooks.silver.prices.transforms import deduplicate_prices


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    session = SparkSession.builder.master("local[1]").appName("tests").getOrCreate()
    session.conf.set("spark.sql.session.timeZone", "UTC")
    yield session
    session.stop()


def test_deduplicate_prices_keeps_latest_retrieved_at(spark):
    rows = [
        Row(
            ticker="PKN",
            date="2024-01-02",
            open=60.0,
            high=61.0,
            low=59.5,
            close=60.5,
            volume=100000,
            source="stooq",
            retrieved_at=datetime(2024, 1, 3, 8, 0, 0),
        ),
        Row(
            ticker="PKN",
            date="2024-01-02",
            open=60.0,
            high=61.5,
            low=59.5,
            close=61.0,
            volume=105000,
            source="stooq",
            retrieved_at=datetime(2024, 1, 5, 8, 0, 0),
        ),
        Row(
            ticker="PKO",
            date="2024-01-02",
            open=40.0,
            high=40.5,
            low=39.5,
            close=40.2,
            volume=50000,
            source="stooq",
            retrieved_at=datetime(2024, 1, 3, 8, 0, 0),
        ),
    ]
    bronze_df = spark.createDataFrame(rows)

    deduped = deduplicate_prices(bronze_df)
    result = {(r.ticker, r.date): r.close for r in deduped.collect()}

    assert len(result) == 2
    assert result[("PKN", "2024-01-02")] == 61.0
    assert result[("PKO", "2024-01-02")] == 40.2


def test_deduplicate_prices_single_row_per_key_unaffected(spark):
    rows = [
        Row(
            ticker="PKN",
            date="2024-01-02",
            open=60.0,
            high=61.0,
            low=59.5,
            close=60.5,
            volume=100000,
            source="stooq",
            retrieved_at=datetime(2024, 1, 3, 8, 0, 0),
        ),
    ]
    bronze_df = spark.createDataFrame(rows)

    deduped = deduplicate_prices(bronze_df)

    assert deduped.count() == 1
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `pytest tests/silver/test_prices_transforms.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'notebooks.silver.prices'`

- [ ] **Step 3: Create the package and the dedup function**

Create `notebooks/silver/__init__.py` (empty, only if it doesn't already exist) and
`notebooks/silver/prices/__init__.py` (empty).

Create `notebooks/silver/prices/transforms.py`:

```python
"""Transform functions for the Silver prices deduplication/standardization notebook."""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import col, row_number


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
        bronze_df.withColumn("_rn", row_number().over(window))
        .filter(col("_rn") == 1)
        .drop("_rn")
    )
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `pytest tests/silver/test_prices_transforms.py -v`
Expected: PASS (2 tests)

- [ ] **Step 5: Commit**

```bash
git add notebooks/silver/__init__.py notebooks/silver/prices/__init__.py notebooks/silver/prices/transforms.py tests/silver/__init__.py tests/silver/test_prices_transforms.py
git commit -m "feat: add prices Silver deduplication"
```

---

### Task 2: Standardize date to a real date type

**Files:**
- Modify: `notebooks/silver/prices/transforms.py`
- Test: `tests/silver/test_prices_transforms.py`

**Interfaces:**
- Consumes: nothing from Task 1 directly (takes an already-deduplicated `DataFrame`).
- Produces: `standardize_prices(deduped_df: DataFrame) -> DataFrame` — same columns, with `date`
  as `DateType` instead of a `yyyy-MM-dd` string. Consumed by Task 4's `notebook.py`.

- [ ] **Step 1: Write the failing test**

Append to `tests/silver/test_prices_transforms.py`:

```python
from datetime import date

from notebooks.silver.prices.transforms import standardize_prices


def test_standardize_prices_casts_date(spark):
    rows = [
        Row(
            ticker="PKN",
            date="2024-01-02",
            open=60.0,
            high=61.0,
            low=59.5,
            close=60.5,
            volume=100000,
            source="stooq",
            retrieved_at=datetime(2024, 1, 3, 8, 0, 0),
        ),
    ]
    df = spark.createDataFrame(rows)

    standardized = standardize_prices(df)
    row = standardized.collect()[0]

    assert row.date == date(2024, 1, 2)
    assert standardized.schema["date"].dataType.typeName() == "date"
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `pytest tests/silver/test_prices_transforms.py -v`
Expected: FAIL — `ImportError: cannot import name 'standardize_prices'`

- [ ] **Step 3: Implement `standardize_prices`**

Add to `notebooks/silver/prices/transforms.py`:

```python
from pyspark.sql.functions import to_date


def standardize_prices(deduped_df: DataFrame) -> DataFrame:
    """Cast date from a yyyy-MM-dd string to a proper date type.

    Args:
        deduped_df: Deduplicated DataFrame matching bronze_stooq_prices' schema.
    Returns:
        DataFrame with date as DateType; all other columns unchanged.
    """
    return deduped_df.withColumn("date", to_date(col("date"), "yyyy-MM-dd"))
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `pytest tests/silver/test_prices_transforms.py -v`
Expected: PASS (3 tests total)

- [ ] **Step 5: Commit**

```bash
git add notebooks/silver/prices/transforms.py tests/silver/test_prices_transforms.py
git commit -m "feat: standardize prices date to DateType"
```

---

### Task 3: Join currency from ticker config

**Files:**
- Modify: `notebooks/silver/prices/transforms.py`
- Test: `tests/silver/test_prices_transforms.py`

**Interfaces:**
- Consumes: nothing from Tasks 1-2 directly.
- Produces: `load_ticker_config(path: str | Path) -> list[dict]` — identical shape to the copies
  already in `notebooks/bronze/stooq/transforms.py` and `notebooks/bronze/fmp/transforms.py`
  (returns dicts with `ticker`, `stooq_symbol`, `fmp_symbol`, `company_name`, `currency`). Also
  produces `add_currency_column(prices_df: DataFrame, tickers: list[dict], spark: SparkSession) ->
  DataFrame` — same columns as its input plus `currency` (string). Both consumed by Task 4's
  `notebook.py`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/silver/test_prices_transforms.py`:

```python
from notebooks.silver.prices.transforms import add_currency_column, load_ticker_config


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


def test_add_currency_column_joins_by_ticker(spark):
    prices_rows = [
        Row(ticker="PKN", date=date(2024, 1, 2), close=60.5),
        Row(ticker="PKO", date=date(2024, 1, 2), close=40.2),
    ]
    prices_df = spark.createDataFrame(prices_rows)
    tickers = [
        {"ticker": "PKN", "currency": "PLN"},
        {"ticker": "PKO", "currency": "PLN"},
    ]

    result = add_currency_column(prices_df, tickers, spark)
    rows = {r.ticker: r.currency for r in result.collect()}

    assert rows == {"PKN": "PLN", "PKO": "PLN"}


def test_add_currency_column_leaves_unknown_ticker_null(spark):
    prices_rows = [Row(ticker="XYZ", date=date(2024, 1, 2), close=1.0)]
    prices_df = spark.createDataFrame(prices_rows)
    tickers = [{"ticker": "PKN", "currency": "PLN"}]

    result = add_currency_column(prices_df, tickers, spark)
    row = result.collect()[0]

    assert row.ticker == "XYZ"
    assert row.currency is None
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pytest tests/silver/test_prices_transforms.py -v`
Expected: FAIL — `ImportError: cannot import name 'load_ticker_config'`

- [ ] **Step 3: Implement `load_ticker_config` and `add_currency_column`**

Add to `notebooks/silver/prices/transforms.py`:

```python
from pathlib import Path

import yaml
from pyspark.sql import SparkSession


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
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `pytest tests/silver/test_prices_transforms.py -v`
Expected: PASS (6 tests total)

- [ ] **Step 5: Commit**

```bash
git add notebooks/silver/prices/transforms.py tests/silver/test_prices_transforms.py
git commit -m "feat: join ticker currency into Silver prices"
```

---

### Task 4: Assemble the notebook

**Files:**
- Create: `notebooks/silver/prices/notebook.py`
- Modify: nothing else (no new tests — glues Tasks 1-3's tested functions together plus the
  untested `MERGE INTO` write, matching every existing Bronze `notebook.py`).

**Interfaces:**
- Consumes: `deduplicate_prices`, `standardize_prices`, `load_ticker_config`,
  `add_currency_column` (all from Tasks 1-3).

- [ ] **Step 1: Write `notebook.py`**

Create `notebooks/silver/prices/notebook.py`:

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

# ## Silver — prices
#
# Deduplicates and standardizes `bronze_stooq_prices` into `silver_prices`: one row per
# (ticker, date), latest retrieved_at wins, date cast to a real date, currency joined in from
# `notebooks/config/tickers.yaml` (passthrough label, no FX conversion). Idempotent — upserts
# via MERGE INTO, safe to re-run.
#
# Logic lives in `transforms.py` next to this notebook; this cell stays thin.

# CELL ********************

from delta.tables import DeltaTable

from transforms import (
    add_currency_column,
    deduplicate_prices,
    load_ticker_config,
    standardize_prices,
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# PARAMETERS CELL — override via Data Factory pipeline / notebook run parameters.
bronze_table_name: str = "bronze_stooq_prices"
silver_table_name: str = "silver_prices"
ticker_config_path: str = "notebooks/config/tickers.yaml"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bronze_df = spark.read.table(bronze_table_name)
tickers = load_ticker_config(ticker_config_path)

deduped_df = deduplicate_prices(bronze_df)
standardized_df = standardize_prices(deduped_df)
silver_df = add_currency_column(standardized_df, tickers, spark)

if spark.catalog.tableExists(silver_table_name):
    delta_table = DeltaTable.forName(spark, silver_table_name)
    (
        delta_table.alias("target")
        .merge(
            silver_df.alias("source"),
            "target.ticker = source.ticker AND target.date = source.date",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    silver_df.write.format("delta").saveAsTable(silver_table_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
```

- [ ] **Step 2: Run the full test suite**

Run: `pytest tests/ -v`
Expected: PASS — all pre-existing tests plus the 6 new prices tests, plus the sibling
`silver_fx_rates` plan's 3 tests if that plan has already run (33 total if both Silver plans are
done: 24 pre-existing + 3 fx_rates + 6 prices — count from whichever tests actually exist in the
tree at the time you run this).

- [ ] **Step 3: Run lint and formatting checks**

Run: `ruff check . && black --check .`
Expected: both report no issues. If `black --check .` fails, run `black .` and re-check.

- [ ] **Step 4: Commit**

```bash
git add notebooks/silver/prices/notebook.py
git commit -m "feat: add Silver prices notebook"
```