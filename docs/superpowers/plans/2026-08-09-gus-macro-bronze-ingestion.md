# GUS BDL Macro Bronze Ingestion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a Bronze ingestion notebook that pulls national-level (Poland) CPI/inflation,
unemployment rate, and GDP time series from the GUS BDL API, following the existing NBP/Stooq
notebook pattern.

**Architecture:** Thin `notebook.py` (Fabric notebook, cell-based) imports pure functions from a
sibling `transforms.py`. One request per (indicator, year) pair against BDL's
`/data/by-variable/{id}` endpoint — chunked by year like NBP's date-range chunking, since it's
unconfirmed whether the endpoint accepts a multi-year range in one call (see Task 1). Unlike FMP's
three differently-shaped statement endpoints, every GUS BDL variable returns the same generic
time-series shape, so this uses one Bronze table (`bronze_gus_macro`) with a fixed `StructType`
rather than three or an inferred schema.

**Tech Stack:** Python 3.11, PySpark (local `local[1]` SparkSession in tests, Fabric runtime in
production), `requests`, `pyyaml`, `pytest`.

## Global Constraints

- Idempotent notebooks: Bronze landing tables are append-only by convention (dedup happens in
  Silver) — matches the existing NBP/Stooq notebooks.
- No hardcoded IDs: BDL variable IDs must live in a config file, never literals in
  `transforms.py`/`notebook.py`.
- Every raw record keeps `source` and `retrieved_at` columns (CLAUDE.md, non-optional).
- Type hints required on every function in `/notebooks/**/transforms.py`.
- One-line docstring (+ Args/Returns if non-trivial) on public transform functions only.
- Formatting/linting: `ruff check .` and `black .` (line-length 100) must pass before each commit.
- Conventional Commits prefix + short imperative summary (`feat:`, `test:`, `chore:`).
- Atomic commits — one logical change per commit.
- Current branch is `feature/repo-skeleton`; commit directly there unless told otherwise.

---

### Task 1: Look up real BDL variable IDs and create the macro indicator config

**Files:**
- Create: `notebooks/config/macro_indicators.yaml`
- Create: `notebooks/bronze/gus/__init__.py`
- Create: `notebooks/bronze/gus/transforms.py`
- Test: `tests/bronze/test_gus_transforms.py`

**Interfaces:**
- Produces: `load_macro_indicator_config(path: str | Path) -> list[dict]` — each dict has keys
  `name`, `gus_variable_id`, `unit`. Consumed by Task 4's `notebook.py`.

This task's config values (the three numeric `gus_variable_id`s) genuinely cannot be filled in
from this plan alone — BDL's ~40,000 variables aren't enumerable here, and guessing IDs would
silently point the notebook at the wrong statistic. The lookup below is a real, runnable step, not
a placeholder to skip.

- [ ] **Step 1: Look up the CPI/inflation variable ID**

Run (no API key required for anonymous access, well under the 100 req/15min limit for 3 lookups):

```bash
curl -s "https://bdl.stat.gov.pl/api/v1/variables/search?name=wska%C5%BAnik%20cen%20towar%C3%B3w%20i%20us%C5%82ug%20konsumpcyjnych&lang=pl&page-size=10&format=json" | python -m json.tool
```

Inspect the `results` array. Each entry has an `id` field and one or more descriptive name fields.
Pick the entry that most precisely matches "CPI, ogółem, poprzedni miesiąc/analogiczny okres roku
poprzedniego = 100, Polska" (avoid regional/voivodeship-scoped variants — this notebook only
requests national-level data via `unit-level=0`, but some BDL variables are inherently
region-only and won't return anything at that level). Note the `id` value.

- [ ] **Step 2: Look up the unemployment rate variable ID**

```bash
curl -s "https://bdl.stat.gov.pl/api/v1/variables/search?name=stopa%20bezrobocia%20rejestrowanego&lang=pl&page-size=10&format=json" | python -m json.tool
```

Same procedure: pick the national-level registered unemployment rate variable, note its `id`.

- [ ] **Step 3: Look up the GDP variable ID**

```bash
curl -s "https://bdl.stat.gov.pl/api/v1/variables/search?name=produkt%20krajowy%20brutto&lang=pl&page-size=10&format=json" | python -m json.tool
```

Pick the national-level GDP variable (current prices, annual — not per-capita, not regional), note
its `id`.

- [ ] **Step 4: Write the config file with the three real IDs from Steps 1-3**

Create `notebooks/config/macro_indicators.yaml`, substituting `<id-from-step-N>` with the actual
integers found above — no placeholder values should remain in the committed file:

```yaml
# Macro indicators for GUS BDL ingestion (Poland, national level only).
# gus_variable_id values were looked up via GET /variables/search on the BDL
# API (https://bdl.stat.gov.pl/api/v1) — see Task 1 of the implementation
# plan (docs/superpowers/plans/2026-08-09-gus-macro-bronze-ingestion.md) for
# the exact lookup commands and how each id was selected.
indicators:
  - name: cpi
    gus_variable_id: <id-from-step-1>
    unit: "%"
  - name: unemployment_rate
    gus_variable_id: <id-from-step-2>
    unit: "%"
  - name: gdp
    gus_variable_id: <id-from-step-3>
    unit: "PLN"
```

(`unit` values above are placeholders for the *typical* unit of each indicator — replace with
whatever unit the `/variables/search` result actually reports for the chosen variable, since BDL
sometimes expresses the same concept in different units across variants, e.g. index points vs.
percent change.)

- [ ] **Step 5: Write the failing test for the config loader**

Create `tests/bronze/test_gus_transforms.py`:

```python
from notebooks.bronze.gus.transforms import load_macro_indicator_config


def test_load_macro_indicator_config(tmp_path):
    config_file = tmp_path / "macro_indicators.yaml"
    config_file.write_text(
        "indicators:\n"
        "  - name: cpi\n"
        "    gus_variable_id: 12345\n"
        "    unit: '%'\n",
        encoding="utf-8",
    )

    indicators = load_macro_indicator_config(config_file)

    assert indicators == [{"name": "cpi", "gus_variable_id": 12345, "unit": "%"}]
```

- [ ] **Step 6: Run the test to verify it fails**

Run: `pytest tests/bronze/test_gus_transforms.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'notebooks.bronze.gus'`

- [ ] **Step 7: Create the package and the loader**

Create `notebooks/bronze/gus/__init__.py` (empty file).

Create `notebooks/bronze/gus/transforms.py`:

```python
"""Transform functions for the GUS BDL macro-indicators Bronze ingestion notebook."""

from __future__ import annotations

from pathlib import Path

import yaml


def load_macro_indicator_config(path: str | Path) -> list[dict]:
    """Load the macro indicator list used to parameterize GUS ingestion.

    Args:
        path: Path to a YAML file shaped like notebooks/config/macro_indicators.yaml.
    Returns:
        List of indicator config dicts (name, gus_variable_id, unit).
    """
    with open(path, encoding="utf-8") as f:
        config = yaml.safe_load(f)
    return config["indicators"]
```

- [ ] **Step 8: Run the test to verify it passes**

Run: `pytest tests/bronze/test_gus_transforms.py -v`
Expected: PASS

- [ ] **Step 9: Commit**

```bash
git add notebooks/config/macro_indicators.yaml notebooks/bronze/gus/__init__.py notebooks/bronze/gus/transforms.py tests/bronze/test_gus_transforms.py
git commit -m "feat: add GUS BDL macro indicator config and config loader"
```

---

### Task 2: URL builder and fetch function

**Files:**
- Modify: `notebooks/bronze/gus/transforms.py`
- Test: `tests/bronze/test_gus_transforms.py`

**Interfaces:**
- Consumes: none from Task 1 directly.
- Produces: `build_gus_data_url(variable_id: int, year: int, unit_level: str = "0") -> str`,
  `fetch_gus_data(url: str, timeout: int = 30) -> dict | None`, `class
  GusFetchError(RuntimeError)`. Consumed by Task 4's `notebook.py`.

`fetch_gus_data` has no dedicated unit test, matching the existing `fetch_nbp_rates`/
`fetch_stooq_csv` convention (thin `requests` wrappers around live HTTP aren't unit-tested in this
codebase). Its behavior is exercised manually in Step 5.

- [ ] **Step 1: Write the failing test for the URL builder**

Append to `tests/bronze/test_gus_transforms.py`:

```python
from notebooks.bronze.gus.transforms import build_gus_data_url


def test_build_gus_data_url_defaults_to_national_level():
    url = build_gus_data_url(12345, 2024)
    assert (
        url
        == "https://bdl.stat.gov.pl/api/v1/data/by-variable/12345"
        "?unit-level=0&year=2024&format=json"
    )


def test_build_gus_data_url_custom_unit_level():
    url = build_gus_data_url(12345, 2024, unit_level="2")
    assert "unit-level=2" in url
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pytest tests/bronze/test_gus_transforms.py -v`
Expected: FAIL — `ImportError: cannot import name 'build_gus_data_url'`

- [ ] **Step 3: Implement the URL builder and fetch function**

Add to `notebooks/bronze/gus/transforms.py`:

```python
import requests

BDL_DATA_URL = "https://bdl.stat.gov.pl/api/v1/data/by-variable"


class GusFetchError(RuntimeError):
    """Raised when the GUS BDL API returns an unexpected error response."""


def build_gus_data_url(variable_id: int, year: int, unit_level: str = "0") -> str:
    """Build the BDL by-variable data URL for one variable and year.

    unit_level="0" selects the national (Poland-wide) aggregate.
    """
    return f"{BDL_DATA_URL}/{variable_id}?unit-level={unit_level}&year={year}&format=json"


def fetch_gus_data(url: str, timeout: int = 30) -> dict | None:
    """Fetch one year's data for a variable from GUS BDL.

    Returns:
        Parsed JSON payload, or None if BDL has no data for the requested
        variable/year (404).
    Raises:
        GusFetchError: on any other non-2xx response.
    """
    response = requests.get(url, timeout=timeout)
    if response.status_code == 404:
        return None
    if not response.ok:
        raise GusFetchError(
            f"GUS BDL request failed with {response.status_code} for url={url}: {response.text}"
        )
    return response.json()
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `pytest tests/bronze/test_gus_transforms.py -v`
Expected: PASS (3 tests total)

- [ ] **Step 5: Manually verify the response shape against a live call**

Using one of the real variable IDs found in Task 1:

```bash
curl -s "https://bdl.stat.gov.pl/api/v1/data/by-variable/<real-id>?unit-level=0&year=2023&format=json" | python -m json.tool
```

Confirm the top-level shape is `{"results": [...], ...}` where each result has a `values` list of
`{"year": ..., "val": ..., ...}` entries — this is the shape Task 3's `parse_gus_data` assumes,
based on BDL's documented API structure but not yet exercised against a live response in this
environment. If the real shape differs (e.g. a different key for the numeric value, or no `unit`
field per entry), adjust `parse_gus_data` in Task 3 accordingly before treating it as correct.

- [ ] **Step 6: Commit**

```bash
git add notebooks/bronze/gus/transforms.py tests/bronze/test_gus_transforms.py
git commit -m "feat: add GUS BDL data URL builder and fetch function"
```

---

### Task 3: Parse GUS BDL payloads into Bronze DataFrames

**Files:**
- Modify: `notebooks/bronze/gus/transforms.py`
- Test: `tests/bronze/test_gus_transforms.py`

**Interfaces:**
- Consumes: nothing from Tasks 1-2 directly (takes an already-fetched `payload: dict`).
- Produces: `parse_gus_data(payload: dict, indicator_name: str, variable_id: int, source: str,
  retrieved_at: datetime, spark: SparkSession) -> DataFrame` matching `BRONZE_MACRO_SCHEMA`
  (`indicator_name: str`, `variable_id: int`, `year: int`, `value: float | None`, `unit: str |
  None`, `source: str`, `retrieved_at: datetime`). Consumed by Task 4's `notebook.py`.

- [ ] **Step 1: Write the failing tests**

Append to `tests/bronze/test_gus_transforms.py`:

```python
from datetime import datetime, timezone

import pytest
from pyspark.sql import SparkSession

from notebooks.bronze.gus.transforms import parse_gus_data

SAMPLE_PAYLOAD = {
    "results": [
        {
            "id": "000000000000",
            "values": [
                {"year": 2023, "val": 3.5, "unit": "%"},
                {"year": 2024, "val": 4.2, "unit": "%"},
            ],
        }
    ]
}


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    session = SparkSession.builder.master("local[1]").appName("tests").getOrCreate()
    session.conf.set("spark.sql.session.timeZone", "UTC")
    yield session
    session.stop()


def test_parse_gus_data_maps_rows_and_stamps_provenance(spark):
    retrieved_at = datetime(2024, 6, 1, tzinfo=timezone.utc)

    df = parse_gus_data(SAMPLE_PAYLOAD, "cpi", 12345, "gus", retrieved_at, spark)
    rows = df.orderBy("year").collect()

    assert len(rows) == 2
    first = rows[0]
    assert first.indicator_name == "cpi"
    assert first.variable_id == 12345
    assert first.year == 2023
    assert first.value == 3.5
    assert first.unit == "%"
    assert first.source == "gus"
    assert first.retrieved_at == retrieved_at.replace(tzinfo=None)


def test_parse_gus_data_handles_null_value(spark):
    payload = {
        "results": [{"id": "x", "values": [{"year": 2023, "val": None, "unit": "%"}]}]
    }
    retrieved_at = datetime(2024, 6, 1, tzinfo=timezone.utc)

    df = parse_gus_data(payload, "cpi", 12345, "gus", retrieved_at, spark)
    row = df.collect()[0]

    assert row.value is None
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `pytest tests/bronze/test_gus_transforms.py -v`
Expected: FAIL — `ImportError: cannot import name 'parse_gus_data'`

- [ ] **Step 3: Implement `parse_gus_data`**

Add to `notebooks/bronze/gus/transforms.py`:

```python
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

BRONZE_MACRO_SCHEMA = StructType(
    [
        StructField("indicator_name", StringType(), nullable=False),
        StructField("variable_id", IntegerType(), nullable=False),
        StructField("year", IntegerType(), nullable=False),
        StructField("value", DoubleType(), nullable=True),
        StructField("unit", StringType(), nullable=True),
        StructField("source", StringType(), nullable=False),
        StructField("retrieved_at", TimestampType(), nullable=False),
    ]
)


def parse_gus_data(
    payload: dict,
    indicator_name: str,
    variable_id: int,
    source: str,
    retrieved_at: datetime,
    spark: SparkSession,
) -> DataFrame:
    """Parse one BDL by-variable payload into the Bronze macro schema.

    Response shape (results[].values[].{year,val,unit}) is this project's
    best-effort read of the BDL API docs — verify it against a live call
    (Task 2 Step 5 of the implementation plan) before trusting it silently,
    same posture as the Stooq CSV-header caveat in ../stooq/transforms.py.

    Args:
        payload: JSON payload from fetch_gus_data (not None).
        indicator_name: Config-level indicator name to stamp on every row
            (e.g. "cpi").
        variable_id: BDL variable id to stamp on every row.
        source: Source name to stamp on every row (e.g. "gus").
        retrieved_at: Retrieval timestamp to stamp on every row.
        spark: Active SparkSession.
    Returns:
        DataFrame matching BRONZE_MACRO_SCHEMA.
    """
    retrieved_at_utc = retrieved_at.astimezone(timezone.utc).replace(tzinfo=None)

    rows = []
    for result in payload.get("results", []):
        for entry in result.get("values", []):
            value = entry.get("val")
            rows.append(
                (
                    indicator_name,
                    variable_id,
                    int(entry["year"]),
                    float(value) if value is not None else None,
                    entry.get("unit"),
                    source,
                    retrieved_at_utc,
                )
            )
    return spark.createDataFrame(rows, schema=BRONZE_MACRO_SCHEMA)
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `pytest tests/bronze/test_gus_transforms.py -v`
Expected: PASS (5 tests total)

- [ ] **Step 5: Commit**

```bash
git add notebooks/bronze/gus/transforms.py tests/bronze/test_gus_transforms.py
git commit -m "feat: parse GUS BDL payloads into Bronze DataFrames"
```

---

### Task 4: Assemble the notebook

**Files:**
- Create: `notebooks/bronze/gus/notebook.py`
- Modify: nothing else (no new tests — glues Tasks 1-3's tested functions together, matching
  `notebooks/bronze/nbp/notebook.py`/`notebooks/bronze/stooq/notebook.py`, neither of which has a
  dedicated test file).

**Interfaces:**
- Consumes: `load_macro_indicator_config`, `build_gus_data_url`, `fetch_gus_data`,
  `parse_gus_data` (all from Tasks 1-3).

- [ ] **Step 1: Write `notebook.py`**

Create `notebooks/bronze/gus/notebook.py`:

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

# ## Bronze — GUS BDL macro indicators
#
# Ingests national-level (Poland) CPI/inflation, unemployment rate, and GDP
# time series from the GUS Bank Danych Lokalnych (BDL) API for the
# indicators in `notebooks/config/macro_indicators.yaml`, and appends to the
# Bronze landing table (raw, 1:1 with source — append-only, deduplicated
# later in Silver).
#
# One request per (indicator, year) — chunked by year like the NBP
# notebook's date-range chunking, since a multi-year range in a single BDL
# request isn't confirmed to work.
#
# Logic lives in `transforms.py` next to this notebook; this cell stays thin.

# CELL ********************

from datetime import datetime, timezone

from transforms import (
    build_gus_data_url,
    fetch_gus_data,
    load_macro_indicator_config,
    parse_gus_data,
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# PARAMETERS CELL — override via Data Factory pipeline / notebook run parameters.
start_year: int = 2015
end_year: int = datetime.now(timezone.utc).date().year
unit_level: str = "0"
macro_config_path: str = "notebooks/config/macro_indicators.yaml"
bronze_table_name: str = "bronze_gus_macro"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.conf.set("spark.sql.session.timeZone", "UTC")

source = "gus"
retrieved_at = datetime.now(timezone.utc)

indicators = load_macro_indicator_config(macro_config_path)

frames = []
for entry in indicators:
    for year in range(start_year, end_year + 1):
        url = build_gus_data_url(entry["gus_variable_id"], year, unit_level)
        payload = fetch_gus_data(url)
        if payload is None:
            continue
        frames.append(
            parse_gus_data(payload, entry["name"], entry["gus_variable_id"], source, retrieved_at, spark)
        )

bronze_df = frames[0]
for frame in frames[1:]:
    bronze_df = bronze_df.unionByName(frame)

# Bronze landing table is append-only by convention — dedup happens in Silver.
bronze_df.write.format("delta").mode("append").saveAsTable(bronze_table_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
```

- [ ] **Step 2: Run the full test suite**

Run: `pytest tests/ -v`
Expected: PASS — all GUS tests plus every pre-existing test (14 total: 9 existing + 5 new).

- [ ] **Step 3: Run lint and formatting checks**

Run: `ruff check . && black --check .`
Expected: both report no issues. If `black --check` fails, run `black .` and re-check.

- [ ] **Step 4: Commit**

```bash
git add notebooks/bronze/gus/notebook.py
git commit -m "feat: add GUS BDL Bronze ingestion notebook"
```
