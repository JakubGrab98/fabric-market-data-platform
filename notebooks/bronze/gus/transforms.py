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
