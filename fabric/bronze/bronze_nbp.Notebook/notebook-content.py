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

# ## Bronze — NBP FX rates
#
# Ingests daily average (Table A) FX rates from the Polish National Bank
# for the currencies in `notebooks/config/currencies.yaml`, and appends to
# the Bronze landing table (raw, 1:1 with source — append-only,
# deduplicated later in Silver).
#
# NBP rejects date ranges over 367 days, so requests are chunked; a range
# covering only non-trading days (weekends/holidays) returns no rows
# rather than an error.
#
# Logic lives in `transforms.py` next to this notebook; this cell stays thin.

# CELL ********************

from datetime import date, datetime, timezone

from transforms import (
    build_nbp_rates_url,
    chunk_date_range,
    fetch_nbp_rates,
    load_currency_config,
    parse_nbp_rates,
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# PARAMETERS CELL — override via Data Factory pipeline / notebook run parameters.
start_date: str = "2024-01-01"
end_date: str = datetime.now(timezone.utc).date().isoformat()
currency_config_path: str = "notebooks/config/currencies.yaml"
bronze_table_name: str = "bronze_nbp_fx_rates"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Pin session tz to UTC for any SQL-level timestamp functions used downstream
# (transforms.py separately normalizes retrieved_at before it reaches Spark,
# since this conf doesn't affect createDataFrame's Python-datetime handling).
spark.conf.set("spark.sql.session.timeZone", "UTC")

source = "nbp"
retrieved_at = datetime.now(timezone.utc)
start = date.fromisoformat(start_date)
end = date.fromisoformat(end_date)

currencies = load_currency_config(currency_config_path)

frames = []
for entry in currencies:
    for chunk_start, chunk_end in chunk_date_range(start, end):
        url = build_nbp_rates_url(entry["code"], chunk_start, chunk_end)
        payload = fetch_nbp_rates(url)
        if payload is None:
            continue
        frames.append(parse_nbp_rates(payload, source, retrieved_at, spark))

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
