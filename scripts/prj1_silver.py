#!/usr/bin/env python3
# ~/de_lab/projects/enterprise-cdc-lakehouse/scripts/prj1_silver.py
# Bronze -> Silver. Hash the PII with a key, then build SCD2 versions. Then export the current rows for dbt.
import sys, os, argparse
from dotenv import load_dotenv
 
load_dotenv()
sys.path.insert(0, os.path.dirname(__file__))
from spark_common import get_spark, BRONZE, SILVER, EXPORT
from scd2_lib import build_scd2
from pii_lib import hmac_column
from pyspark.sql import functions as F
 
ap = argparse.ArgumentParser()
ap.add_argument("--format", default="delta", choices=["delta", "parquet"], help="parquet exists so tests run without Delta jars")
ap.add_argument("--bronze", default=BRONZE)
ap.add_argument("--silver", default=SILVER)
ap.add_argument("--export", default=EXPORT)
ap.add_argument("--mem", default="2g")
a = ap.parse_args()
 
key = os.getenv("PII_HMAC_KEY", "")
if len(key) < 16 or key.startswith("change-this"):
    sys.exit("PII_HMAC_KEY in .env is missing or still the placeholder. Set a long random value first.")
 
spark = get_spark("silver", mem=a.mem, delta=(a.format == "delta"))
spark.sparkContext.setLogLevel("WARN")
bronze = spark.read.format(a.format).load(a.bronze)
print(f"BEFORE: bronze events = {bronze.count():,}")
 
ev = (
    bronze.select(
        "sale_id",
        "event_id",
        F.to_timestamp("occurred_at").alias("event_ts"),
        "vin",
        "plant_id",
        "dealer_id",
        "sale_date",
        "sale_amount",
        "currency",
        "engine_type",
        "model_code",
        "warranty_tier",
        "customer_id",
        "customer_email",
        "customer_phone",
    )
    # PII leaves here. After these three lines no raw value goes to Silver.
    .withColumn("customer_hash", hmac_column("customer_id", key))
    .withColumn("email_hash", hmac_column("customer_email", key))
    .withColumn("phone_hash", hmac_column("customer_phone", key))
    .drop("customer_id", "customer_email", "customer_phone")
)
 
silver = build_scd2(ev)
silver.write.format(a.format).mode("overwrite").save(a.silver)
cur = spark.read.format(a.format).load(a.silver).filter("is_current")
cur.drop("row_hash").write.mode("overwrite").parquet(a.export)  # dbt and the DQ gate read this
total = spark.read.format(a.format).load(a.silver).count()
print(f"AFTER : silver versions = {total:,}  current rows = {cur.count():,}  export = {a.export}")
