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

# ## Bronze — Stooq daily prices
#
# Ingests daily OHLCV history from Stooq for the tickers in
# `notebooks/config/tickers.yaml`, and appends to the Bronze landing table
# (raw, 1:1 with source — append-only, deduplicated later in Silver).
#
# Logic lives in `transforms.py` next to this notebook; this cell stays thin.

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from datetime import date, datetime, timezone

from transforms import (
    build_stooq_csv_url,
    fetch_stooq_csv,
    load_ticker_config,
    parse_stooq_prices_csv,
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
ticker_config_path: str = "notebooks/config/tickers.yaml"
bronze_table_name: str = "bronze_stooq_prices"

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

source = "stooq"
retrieved_at = datetime.now(timezone.utc)
start = date.fromisoformat(start_date)
end = date.fromisoformat(end_date)

tickers = load_ticker_config(ticker_config_path)

frames = []
for entry in tickers:
    url = build_stooq_csv_url(entry["stooq_symbol"], start, end)
    csv_text = fetch_stooq_csv(url)
    frames.append(parse_stooq_prices_csv(csv_text, entry["ticker"], source, retrieved_at, spark))

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
