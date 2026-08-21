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

# ## Gold — dim_date
#
# Generates a standard calendar dimension over [start_date, end_date] — one row per date, not
# ingested from Bronze/Silver. is_trading_day_gpw is currently a weekday-only approximation (see
# transforms.py docstring). Idempotent — upserts via MERGE INTO, safe to re-run.
#
# Logic lives in `transforms.py` next to this notebook; this cell stays thin.

# CELL ********************

from datetime import date

from delta.tables import DeltaTable
from transforms import generate_date_dimension

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# PARAMETERS CELL — override via Data Factory pipeline / notebook run parameters.
start_date: str = "2000-01-01"
end_date: str = "2035-12-31"
gold_table_name: str = "dim_date"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gold_df = generate_date_dimension(
    date.fromisoformat(start_date), date.fromisoformat(end_date), spark
)

if spark.catalog.tableExists(gold_table_name):
    delta_table = DeltaTable.forName(spark, gold_table_name)
    (
        delta_table.alias("target")
        .merge(gold_df.alias("source"), "target.date = source.date")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    gold_df.write.format("delta").saveAsTable(gold_table_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
