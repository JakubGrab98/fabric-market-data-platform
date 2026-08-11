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

# ## Silver — fx_rates
#
# Deduplicates and standardizes `bronze_nbp_fx_rates` into `silver_fx_rates`: one row per
# (currency_code, effective_date), latest retrieved_at wins, effective_date cast to a real
# date. Idempotent — upserts via MERGE INTO, safe to re-run.
#
# Logic lives in `transforms.py` next to this notebook; this cell stays thin.

# CELL ********************

from delta.tables import DeltaTable
from transforms import deduplicate_fx_rates, standardize_fx_rates

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# }

# CELL ********************

# PARAMETERS CELL — override via Data Factory pipeline / notebook run parameters.
bronze_table_name: str = "bronze_nbp_fx_rates"
silver_table_name: str = "silver_fx_rates"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# }

# CELL ********************

bronze_df = spark.read.table(bronze_table_name)

deduped_df = deduplicate_fx_rates(bronze_df)
silver_df = standardize_fx_rates(deduped_df)

if spark.catalog.tableExists(silver_table_name):
    delta_table = DeltaTable.forName(spark, silver_table_name)
    (
        delta_table.alias("target")
        .merge(
            silver_df.alias("source"),
            "target.currency_code = source.currency_code "
            "AND target.effective_date = source.effective_date",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
else:
    silver_df.write.format("delta").saveAsTable(silver_table_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# }
