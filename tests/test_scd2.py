# ~/de_lab/projects/enterprise-cdc-lakehouse/tests/test_scd2.py
import sys, os
 
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))
import pytest
from pyspark.sql import SparkSession
from scd2_lib import build_scd2
 
 
@pytest.fixture(scope="module")
def spark():
    s = SparkSession.builder.master("local[1]").config("spark.ui.enabled", "false").getOrCreate()
    yield s
    s.stop()
 
 
COLS = ["sale_id", "event_id", "event_ts", "sale_amount", "warranty_tier", "dealer_id", "currency"]
 
 
def rows(spark, data):
    return spark.createDataFrame(data, COLS)
 
 
def test_one_version_per_real_change(spark):
    ev = rows(
        spark,
        [
            ("S1", "e1", "2026-01-01 10:00:00", 100.0, "STANDARD", "D1", "EUR"),
            ("S1", "e2", "2026-01-02 10:00:00", 100.0, "STANDARD", "D1", "EUR"),  # repeat, nothing changed
            ("S1", "e3", "2026-01-03 10:00:00", 120.0, "STANDARD", "D1", "EUR"),  # amount changed
            ("S1", "e4", "2026-01-04 10:00:00", 120.0, "PREMIUM", "D1", "EUR"),  # warranty changed
        ],
    )
    out = build_scd2(ev).orderBy("valid_from").collect()
    assert len(out) == 3  # the repeat made no version
    assert [r.is_current for r in out] == [False, False, True]
    assert out[0].valid_to == out[1].valid_from  # no gap, no overlap
    assert out[1].valid_to == out[2].valid_from
    assert out[2].valid_to is None
 
 
def test_exactly_one_current_row_per_sale(spark):
    ev = rows(
        spark, [("S%d" % (i % 5), "e%d" % i, "2026-01-%02d 10:00:00" % (i + 1), 100.0 + i, "STANDARD", "D1", "EUR") for i in range(20)]
    )
    out = build_scd2(ev)
    assert out.filter("is_current").groupBy("sale_id").count().filter("count != 1").count() == 0
 
 
def test_replay_gives_the_same_answer(spark):
    ev = rows(
        spark,
        [
            ("S1", "e1", "2026-01-01 10:00:00", 100.0, "STANDARD", "D1", "EUR"),
            ("S1", "e2", "2026-01-03 10:00:00", 120.0, "STANDARD", "D1", "EUR"),
        ],
    )
    a = sorted(build_scd2(ev).select("version_key").rdd.flatMap(lambda r: r).collect())
    b = sorted(build_scd2(ev.union(ev).dropDuplicates(["event_id"])).select("version_key").rdd.flatMap(lambda r: r).collect())
    assert a == b  # idempotent: same input, same versions
 
 
def test_out_of_order_arrival_is_sorted_by_event_time(spark):
    ev = rows(
        spark,
        [
            ("S1", "e2", "2026-01-03 10:00:00", 120.0, "STANDARD", "D1", "EUR"),
            ("S1", "e1", "2026-01-01 10:00:00", 100.0, "STANDARD", "D1", "EUR"),
        ],
    )  # arrived late
    out = build_scd2(ev).orderBy("valid_from").collect()
    assert [r.sale_amount for r in out] == [100.0, 120.0] and out[-1].is_current
