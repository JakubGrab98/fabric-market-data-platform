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

# ## Gold — fact_makro
#
# Selects silver_macro into fact_makro's canonical column order, dropping Silver's variable_id
# (lineage-only, not part of the Gold contract). Idempotent — upserts via MERGE INTO, safe to
# re-run.
#
# Logic lives in `transforms.py` next to this notebook; this cell stays thin.

# CELL ********************

from delta.tables import DeltaTable
from transforms import build_fact_makro

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# PARAMETERS CELL — override via Data Factory pipeline / notebook run parameters.
silver_table_name: str = "silver_macro"
gold_table_name: str = "fact_makro"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_df = spark.read.table(silver_table_name)
gold_df = build_fact_makro(silver_df)

if spark.catalog.tableExists(gold_table_name):
    delta_table = DeltaTable.forName(spark, gold_table_name)
    (
        delta_table.alias("target")
        .merge(
            gold_df.alias("source"),
            "target.country = source.country "
            "AND target.indicator_name = source.indicator_name "
            "AND target.reference_date = source.reference_date",
        )
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
