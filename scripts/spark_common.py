# ~/de_lab/projects/enterprise-cdc-lakehouse/scripts/spark_common.py
# Shared Spark helpers: one place builds the session, one place turns the contract into a Spark schema.
import os, json
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, BooleanType
 
DATA = os.path.expanduser(os.getenv("LAB_DATA", "~/de_lab/data"))
BRONZE = f"{DATA}/delta/bronze_sales"
DLQ = f"{DATA}/delta/bronze_dlq"
SILVER = f"{DATA}/delta/silver_sales_scd2"
EXPORT = f"{DATA}/export/silver_sales_current"
CONTRACT = os.path.expanduser(os.getenv("CONTRACT_FILE", "~/de_lab/projects/enterprise-cdc-lakehouse/contracts/sale_event_v1.avsc"))
 
 
def get_spark(app, mem="2g", master="local[*]", delta=True, kafka=False, parts=32):
    b = (
        SparkSession.builder.master(master)
        .appName(app)
        .config("spark.driver.memory", mem)
        .config("spark.sql.shuffle.partitions", str(parts))
        .config("spark.ui.showConsoleProgress", "false")
    )
    if not delta:
        return b.getOrCreate()
    b = b.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension").config(
        "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
    from delta import configure_spark_with_delta_pip
 
    pk = ["org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"] if kafka else []
    return configure_spark_with_delta_pip(b, extra_packages=pk).getOrCreate()
 
 
_AVRO_TO_SPARK = {"string": StringType(), "double": DoubleType(), "long": LongType(), "boolean": BooleanType()}
 
 
def schema_from_contract(path=CONTRACT):
    """Read the Avro contract and return (Spark schema, list of required field names)."""
    fields = json.load(open(path))["fields"]
    st, required = [], []
    for f in fields:
        t = f["type"]
        optional = isinstance(t, list)
        base = [x for x in t if x != "null"][0] if optional else t
        st.append(StructField(f["name"], _AVRO_TO_SPARK[base], True))
        if not optional:
            required.append(f["name"])
    return StructType(st), required
