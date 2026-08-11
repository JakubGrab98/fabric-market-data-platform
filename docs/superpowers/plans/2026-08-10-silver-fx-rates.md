# Silver fx_rates Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a Silver notebook that deduplicates and standardizes `bronze_nbp_fx_rates` into an
idempotent, upserted `silver_fx_rates` Delta table.

**Architecture:** Thin `notebook.py` (Fabric notebook, cell-based) imports pure functions from a
sibling `transforms.py`. Every run: read the whole `bronze_nbp_fx_rates` table, deduplicate to one
row per `(currency_code, effective_date)` keeping the latest `retrieved_at`, cast `effective_date`
to a real date, then `MERGE INTO` the Silver Delta table (idempotent upsert, not append). The dedup
and type-cast steps are pure Spark DataFrame logic and live in `transforms.py` (testable); the
`MERGE INTO` call needs a real Delta-enabled session and stays untested in `notebook.py`, matching
this repo's existing convention for Delta writes.

**Tech Stack:** Python 3.11, PySpark (local `local[1]` SparkSession in tests, Fabric runtime in
production), `pytest`.

## Global Constraints

- Idempotent notebooks: "Every ... transform notebook must be safe to re-run for the same
  date/parameters without creating duplicates (upsert/merge into Delta, not blind append)" — this
  is the opposite of Bronze's append-only rule. `silver_fx_rates` is written via `MERGE INTO`
  keyed on `(currency_code, effective_date)`, never `mode("append")`.
- No hardcoded IDs: table names and paths are notebook parameters, not literals.
- Type hints required on every function in `/notebooks/**/transforms.py`.
- One-line docstring (+ Args/Returns if non-trivial) on public transform functions only.
- Formatting/linting: `ruff check .` and `black .` (line-length 100) must pass before each commit.
- Conventional Commits prefix + short imperative summary (`feat:`, `test:`, `chore:`).
- Atomic commits — one logical change per commit.
- Do not add `delta-spark` (or any Delta-Lake Python package) as a project dependency. The `MERGE
  INTO`/`DeltaTable` call in `notebook.py` is Fabric-runtime-only and untested locally, exactly
  like every existing Bronze notebook's `.write.format("delta")` call.
- Do not extract a shared `notebooks/silver/common.py` for dedup logic — duplicate the pattern in
  each module's own `transforms.py`, matching how Bronze duplicates `load_ticker_config` across
  its four source modules. (This is a deliberate spec decision, not an oversight — see
  `docs/superpowers/specs/2026-08-10-phase2-silver-fx-prices-design.md`.)
- Work happens on branch `feature/phase2-silver-transformation` (already checked out at the repo
  root — no worktree needed for this plan unless the executor prefers one).

---

### Task 1: Deduplicate fx_rates by natural key

**Files:**
- Create: `notebooks/silver/__init__.py`
- Create: `notebooks/silver/fx_rates/__init__.py`
- Create: `notebooks/silver/fx_rates/transforms.py`
- Create: `tests/silver/__init__.py`
- Test: `tests/silver/test_fx_rates_transforms.py`

**Interfaces:**
- Produces: `deduplicate_fx_rates(bronze_df: DataFrame) -> DataFrame` — same columns as
  `bronze_nbp_fx_rates` (`currency_code`, `effective_date`, `mid_rate`, `source`, `retrieved_at`),
  one row per `(currency_code, effective_date)`, the row with the latest `retrieved_at` wins.
  Consumed by Task 3's `notebook.py`.

- [ ] **Step 1: Write the failing test**

Create `tests/silver/__init__.py` (empty file) and `tests/silver/test_fx_rates_transforms.py`:

```python
from datetime import datetime, timezone

import pytest
from pyspark.sql import Row, SparkSession

from notebooks.silver.fx_rates.transforms import deduplicate_fx_rates


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    session = SparkSession.builder.master("local[1]").appName("tests").getOrCreate()
    session.conf.set("spark.sql.session.timeZone", "UTC")
    yield session
    session.stop()


def test_deduplicate_fx_rates_keeps_latest_retrieved_at(spark):
    rows = [
        Row(
            currency_code="USD",
            effective_date="2024-01-02",
            mid_rate=3.9432,
            source="nbp",
            retrieved_at=datetime(2024, 1, 3, 8, 0, 0),
        ),
        Row(
            currency_code="USD",
            effective_date="2024-01-02",
            mid_rate=3.9500,
            source="nbp",
            retrieved_at=datetime(2024, 1, 5, 8, 0, 0),
        ),
        Row(
            currency_code="EUR",
            effective_date="2024-01-02",
            mid_rate=4.3,
            source="nbp",
            retrieved_at=datetime(2024, 1, 3, 8, 0, 0),
        ),
    ]
    bronze_df = spark.createDataFrame(rows)

    deduped = deduplicate_fx_rates(bronze_df)
    result = {(r.currency_code, r.effective_date): r.mid_rate for r in deduped.collect()}

    assert len(result) == 2
    assert result[("USD", "2024-01-02")] == 3.9500
    assert result[("EUR", "2024-01-02")] == 4.3


def test_deduplicate_fx_rates_single_row_per_key_unaffected(spark):
    rows = [
        Row(
            currency_code="USD",
            effective_date="2024-01-02",
            mid_rate=3.9432,
            source="nbp",
            retrieved_at=datetime(2024, 1, 3, 8, 0, 0),
        ),
    ]
    bronze_df = spark.createDataFrame(rows)

    deduped = deduplicate_fx_rates(bronze_df)

    assert deduped.count() == 1
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `pytest tests/silver/test_fx_rates_transforms.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'notebooks.silver'`

- [ ] **Step 3: Create the packages and the dedup function**

Create `notebooks/silver/__init__.py` and `notebooks/silver/fx_rates/__init__.py` (both empty,
matching every existing `__init__.py` in `notebooks/bronze/*/`).

Create `notebooks/silver/fx_rates/transforms.py`:

```python
"""Transform functions for the Silver fx_rates deduplication/standardization notebook."""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import col, row_number


def deduplicate_fx_rates(bronze_df: DataFrame) -> DataFrame:
    """Keep one row per (currency_code, effective_date), latest retrieved_at wins.

    Args:
        bronze_df: DataFrame matching bronze_nbp_fx_rates' schema (currency_code,
            effective_date, mid_rate, source, retrieved_at).
    Returns:
        DataFrame with the same columns, one row per (currency_code, effective_date).
    """
    window = Window.partitionBy("currency_code", "effective_date").orderBy(
        col("retrieved_at").desc()
    )
    return (
        bronze_df.withColumn("_rn", row_number().over(window))
        .filter(col("_rn") == 1)
        .drop("_rn")
    )
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `pytest tests/silver/test_fx_rates_transforms.py -v`
Expected: PASS (2 tests)

- [ ] **Step 5: Commit**

```bash
git add notebooks/silver/__init__.py notebooks/silver/fx_rates/__init__.py notebooks/silver/fx_rates/transforms.py tests/silver/__init__.py tests/silver/test_fx_rates_transforms.py
git commit -m "feat: add fx_rates Silver deduplication"
```

---

### Task 2: Standardize effective_date to a real date type

**Files:**
- Modify: `notebooks/silver/fx_rates/transforms.py`
- Test: `tests/silver/test_fx_rates_transforms.py`

**Interfaces:**
- Consumes: nothing from Task 1 directly (takes an already-deduplicated `DataFrame`).
- Produces: `standardize_fx_rates(deduped_df: DataFrame) -> DataFrame` — same columns, with
  `effective_date` as `DateType` instead of a `yyyy-MM-dd` string. Consumed by Task 3's
  `notebook.py`.

- [ ] **Step 1: Write the failing test**

Append to `tests/silver/test_fx_rates_transforms.py`:

```python
from datetime import date

from notebooks.silver.fx_rates.transforms import standardize_fx_rates


def test_standardize_fx_rates_casts_effective_date(spark):
    rows = [
        Row(
            currency_code="USD",
            effective_date="2024-01-02",
            mid_rate=3.9432,
            source="nbp",
            retrieved_at=datetime(2024, 1, 3, 8, 0, 0),
        ),
    ]
    df = spark.createDataFrame(rows)

    standardized = standardize_fx_rates(df)
    row = standardized.collect()[0]

    assert row.effective_date == date(2024, 1, 2)
    assert standardized.schema["effective_date"].dataType.typeName() == "date"
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `pytest tests/silver/test_fx_rates_transforms.py -v`
Expected: FAIL — `ImportError: cannot import name 'standardize_fx_rates'`

- [ ] **Step 3: Implement `standardize_fx_rates`**

Add to `notebooks/silver/fx_rates/transforms.py`:

```python
from pyspark.sql.functions import to_date


def standardize_fx_rates(deduped_df: DataFrame) -> DataFrame:
    """Cast effective_date from a yyyy-MM-dd string to a proper date type.

    Args:
        deduped_df: Deduplicated DataFrame matching bronze_nbp_fx_rates' schema.
    Returns:
        DataFrame with effective_date as DateType; all other columns unchanged.
    """
    return deduped_df.withColumn("effective_date", to_date(col("effective_date"), "yyyy-MM-dd"))
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `pytest tests/silver/test_fx_rates_transforms.py -v`
Expected: PASS (3 tests total)

- [ ] **Step 5: Commit**

```bash
git add notebooks/silver/fx_rates/transforms.py tests/silver/test_fx_rates_transforms.py
git commit -m "feat: standardize fx_rates effective_date to DateType"
```

---

### Task 3: Assemble the notebook

**Files:**
- Create: `notebooks/silver/fx_rates/notebook.py`
- Modify: nothing else (no new tests — glues Tasks 1-2's tested functions together plus the
  untested `MERGE INTO` write, matching every existing Bronze `notebook.py`).

**Interfaces:**
- Consumes: `deduplicate_fx_rates`, `standardize_fx_rates` (both from Tasks 1-2).

- [ ] **Step 1: Write `notebook.py`**

Create `notebooks/silver/fx_rates/notebook.py`:

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

# ## Silver — fx_rates
#
# Deduplicates and standardizes `bronze_nbp_fx_rates` into `silver_fx_rates`: one row per
# (currency_code, effective_date), latest retrieved_at wins, effective_date cast to a real
# date. Idempotent — upserts via MERGE INTO, safe to re-run.
#
# Logic lives in `transforms.py` next to this notebook; this cell stays thin.

# CELL ********************

from delta.tables import DeltaTable

from transforms import deduplicate_fx_rates, standardize_fx_rates

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# PARAMETERS CELL — override via Data Factory pipeline / notebook run parameters.
bronze_table_name: str = "bronze_nbp_fx_rates"
silver_table_name: str = "silver_fx_rates"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

bronze_df = spark.read.table(bronze_table_name)

deduped_df = deduplicate_fx_rates(bronze_df)
silver_df = standardize_fx_rates(deduped_df)

if spark.catalog.tableExists(silver_table_name):
    delta_table = DeltaTable.forName(spark, silver_table_name)
    (
        delta_table.alias("target")
        .merge(
            silver_df.alias("source"),
            "target.currency_code = source.currency_code "
            "AND target.effective_date = source.effective_date",
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
Expected: PASS — all pre-existing tests plus the 3 new fx_rates tests (27 total: 24 existing + 3
new).

- [ ] **Step 3: Run lint and formatting checks**

Run: `ruff check . && black --check .`
Expected: both report no issues. If `black --check .` fails, run `black .` and re-check.

- [ ] **Step 4: Commit**

```bash
git add notebooks/silver/fx_rates/notebook.py
git commit -m "feat: add Silver fx_rates notebook"
```