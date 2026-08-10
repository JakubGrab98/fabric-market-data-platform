"""Transform functions for the GUS BDL macro-indicators Bronze ingestion notebook."""

from __future__ import annotations

from pathlib import Path

import requests
import yaml

BDL_DATA_URL = "https://bdl.stat.gov.pl/api/v1/data/by-variable"


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
