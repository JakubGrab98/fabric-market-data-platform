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
