"""Transform functions for the GUS BDL macro-indicators Bronze ingestion notebook."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import requests
import yaml
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

BDL_DATA_URL = "https://bdl.stat.gov.pl/api/v1/data/by-variable"

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


class GusFetchError(RuntimeError):
    """Raised when the GUS BDL API returns an unexpected error response."""


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


def parse_gus_data(
    payload: dict,
    indicator_name: str,
    variable_id: int,
    unit: str,
    source: str,
    retrieved_at: datetime,
    spark: SparkSession,
) -> DataFrame:
    """Parse one BDL by-variable payload into the Bronze macro schema.

    Response shape (results[].values[].{year,val,attrId}) is confirmed
    against a live call (Task 2 Step 5 of the implementation plan): the API
    has no per-record unit string, only a numeric measureUnitId at the
    payload's top level, which this function doesn't attempt to resolve —
    `unit` is supplied by the caller from config instead.

    Args:
        payload: JSON payload from fetch_gus_data (not None).
        indicator_name: Config-level indicator name to stamp on every row
            (e.g. "cpi").
        variable_id: BDL variable id to stamp on every row.
        unit: Unit label from the indicator's config entry (e.g. "%"),
            stamped on every row — not read from the API response.
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
                    unit,
                    source,
                    retrieved_at_utc,
                )
            )
    return spark.createDataFrame(rows, schema=BRONZE_MACRO_SCHEMA)
