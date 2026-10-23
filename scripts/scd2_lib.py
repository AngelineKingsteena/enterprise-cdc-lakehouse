# ~/de_lab/projects/enterprise-cdc-lakehouse/scripts/scd2_lib.py
# SCD Type 2 as a pure function of the event history. No Delta needed, so pytest runs it anywhere.
from pyspark.sql import DataFrame, Window, functions as F
 
TRACKED = ["sale_amount", "warranty_tier", "dealer_id", "currency"]  # a change in these makes a new version
 
 
def build_scd2(events: DataFrame, tracked=TRACKED) -> DataFrame:
    """events needs: sale_id, event_id, event_ts, and the tracked columns.
    Returns one row per VERSION of each sale: valid_from, valid_to, is_current, version_key."""
    w = Window.partitionBy("sale_id").orderBy("event_ts", "event_id")
    v = events.withColumn("row_hash", F.sha2(F.concat_ws("|", *[F.col(c).cast("string") for c in tracked]), 256))
    v = v.withColumn("prev_hash", F.lag("row_hash").over(w))
    v = v.filter(F.col("prev_hash").isNull() | (F.col("prev_hash") != F.col("row_hash")))  # drop repeats: nothing changed
    v = (
        v.withColumn("valid_from", F.col("event_ts"))
        .withColumn("valid_to", F.lead("event_ts").over(w))  # lead AFTER the filter
        .withColumn("is_current", F.col("valid_to").isNull())
        .withColumn("version_key", F.sha2(F.concat_ws("|", F.col("sale_id"), F.col("valid_from").cast("string")), 256))
    )
    return v.drop("prev_hash")
