#!/usr/bin/env python3
# ~/de_lab/projects/enterprise-cdc-lakehouse/scripts/prj1_bronze_stream.py
# Kafka topic -> Bronze Delta table. Good rows are merged by event_id. Bad rows go to the dead-letter table.
import sys, os, argparse
from functools import reduce
from operator import or_
from dotenv import load_dotenv
 
load_dotenv()
sys.path.insert(0, os.path.dirname(__file__))
from spark_common import get_spark, schema_from_contract, BRONZE, DLQ, DATA
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, IntegerType, TimestampType
 
ap = argparse.ArgumentParser()
ap.add_argument("--topic", default="outbox.event.sale")
ap.add_argument("--live", action="store_true", help="keep running and poll every 5 seconds")
ap.add_argument("--max-offsets", type=int, default=10_000, help="events per micro-batch")
ap.add_argument("--mem", default="2g")
a = ap.parse_args()
 
spark = get_spark("bronze_stream", mem=a.mem, kafka=True)
spark.sparkContext.setLogLevel("WARN")
SCHEMA, REQUIRED = schema_from_contract()
KAFKA_COLS = [
    StructField("kafka_topic", StringType()),
    StructField("kafka_partition", IntegerType()),
    StructField("kafka_offset", LongType()),
    StructField("kafka_ts", TimestampType()),
    StructField("ingested_at", TimestampType()),
]
BRONZE_SCHEMA = StructType(list(SCHEMA.fields) + KAFKA_COLS)
DLQ_SCHEMA = StructType([StructField("raw_value", StringType()), StructField("error", StringType())] + KAFKA_COLS)
 
from delta import DeltaTable
 
for path, sch in ((BRONZE, BRONZE_SCHEMA), (DLQ, DLQ_SCHEMA)):  # create the empty tables once
    if not DeltaTable.isDeltaTable(spark, path):
        spark.createDataFrame([], sch).write.format("delta").save(path)
 
 
def process_batch(batch, batch_id):
    p = (
        batch.select(
            F.col("value").cast("string").alias("raw_value"),
            F.col("topic").alias("kafka_topic"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_ts"),
        )
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn("e", F.from_json("raw_value", SCHEMA))
    )  # bad JSON gives e = null
    is_bad = reduce(or_, [F.col(f"e.{c}").isNull() for c in REQUIRED])  # any required field missing or wrong type
    good = p.filter(~is_bad).select("e.*", *[c.name for c in KAFKA_COLS]).dropDuplicates(["event_id"])  # duplicates inside this batch
    bad = (
        p.filter(is_bad)
        .withColumn("error", F.lit("missing or invalid required field per contract"))
        .select("raw_value", "error", *[c.name for c in KAFKA_COLS])
    )
    # Bad rows: idempotent append. If this batch replays after a crash, txnVersion makes Delta skip it.
    (bad.write.format("delta").mode("append").option("txnAppId", "bronze_dlq").option("txnVersion", batch_id).save(DLQ))
    # Good rows: MERGE on event_id. A replay finds the ids already there and inserts nothing.
    (DeltaTable.forPath(spark, BRONZE).alias("t").merge(good.alias("s"), "t.event_id = s.event_id").whenNotMatchedInsertAll().execute())
 
 
reader = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", os.getenv("KAFKA_BOOTSTRAP", "localhost:9092"))
    .option("subscribe", a.topic)
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", a.max_offsets)
    .option("failOnDataLoss", "true")  # if Kafka expired data we have not read, STOP loudly
    .load()
)
trigger = {"processingTime": "5 seconds"} if a.live else {"availableNow": True}
q = reader.writeStream.foreachBatch(process_batch).option("checkpointLocation", f"{DATA}/checkpoints/bronze").trigger(**trigger).start()
q.awaitTermination()
print("Bronze run finished.")
