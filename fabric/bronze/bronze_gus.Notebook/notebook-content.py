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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

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
            parse_gus_data(
                payload,
                entry["name"],
                entry["gus_variable_id"],
                entry["unit"],
                source,
                retrieved_at,
                spark,
            )
        )

if not frames:
    raise RuntimeError(
        f"No GUS data returned for any indicator in {start_year}-{end_year} (unit_level={unit_level})"
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
