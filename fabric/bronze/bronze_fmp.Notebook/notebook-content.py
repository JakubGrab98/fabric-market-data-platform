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

# ## Bronze — FMP fundamentals
#
# Ingests quarterly balance sheet, income statement, and cash flow data from
# Financial Modeling Prep for the tickers in
# `notebooks/config/tickers.yaml`, and appends to three Bronze landing tables
# (raw, 1:1 with source — append-only, deduplicated later in Silver).
#
# Unlike the date-range parameters on the NBP/Stooq notebooks, FMP's
# statement endpoints return the most recent N periods via `period_limit`
# rather than a start/end date.
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
    FmpFetchError,
    build_balance_sheet_url,
    build_cash_flow_url,
    build_income_statement_url,
    fetch_fmp_statement,
    load_ticker_config,
    parse_fmp_statement,
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# PARAMETERS CELL — override via Data Factory pipeline / notebook run parameters.
fmp_api_key: str = ""
period_limit: int = 8
ticker_config_path: str = "notebooks/config/tickers.yaml"
balance_sheet_table_name: str = "bronze_fmp_balance_sheet"
income_statement_table_name: str = "bronze_fmp_income_statement"
cash_flow_table_name: str = "bronze_fmp_cash_flow"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.conf.set("spark.sql.session.timeZone", "UTC")

source = "fmp"
retrieved_at = datetime.now(timezone.utc)

tickers = load_ticker_config(ticker_config_path)

statements = [
    (build_balance_sheet_url, balance_sheet_table_name),
    (build_income_statement_url, income_statement_table_name),
    (build_cash_flow_url, cash_flow_table_name),
]

for build_url, table_name in statements:
    frames = []
    for entry in tickers:
        url = build_url(entry["fmp_symbol"], period_limit, fmp_api_key)
        records = fetch_fmp_statement(url)
        if not records:
            continue
        frames.append(parse_fmp_statement(records, entry["ticker"], source, retrieved_at, spark))

    if not frames:
        raise FmpFetchError(f"No FMP data returned for any ticker for {table_name}")

    bronze_df = frames[0]
    for frame in frames[1:]:
        bronze_df = bronze_df.unionByName(frame, allowMissingColumns=True)

    # Bronze landing table is append-only by convention — dedup happens in Silver.
    # mergeSchema is required because the schema is inferred from FMP's payload
    # (see parse_fmp_statement) rather than a fixed StructType, so a run that
    # picks up a new/changed FMP field must be allowed to evolve the table schema.
    bronze_df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(
        table_name
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
