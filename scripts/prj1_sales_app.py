# ~/de_lab/projects/enterprise-cdc-lakehouse/scripts/prj1_sales_app.py
# The "application". It saves a sale. Two modes show the old way and the fix.
import os, json, uuid, random, argparse, datetime as dt
from dotenv import load_dotenv
 
load_dotenv()  # reads ~/de_lab/.env
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from sqlalchemy.orm import Session
import avro.schema, avro.io
 
CONTRACT = avro.schema.parse(open(os.path.join(os.path.dirname(__file__), "..", "contracts", "sale_event_v1.avsc")).read())
 
 
class SimulatedCrash(Exception):
    """Stands in for the app process dying at one exact line."""
 
 
def make_engine():
    url = URL.create(
        "postgresql+psycopg2",
        username=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        host=os.getenv("PG_HOST", "localhost"),
        port=int(os.getenv("PG_PORT", "5432")),
        database=os.getenv("PG_DATABASE"),
    )
    return create_engine(url)
 
 
PLANTS = ["PLT001"] * 320 + ["PLT002"] * 280 + ["PLT003"] * 180 + ["PLT004"] * 120 + ["PLT005"] * 100
ENGINES = ["DIESEL", "PETROL", "HYBRID", "ELECTRIC"]
MODELS = ["GLC300", "C200", "E220D", "S500", "EQS"]
 
 
def fake_sale(i, rng):
    return dict(
        sale_id=f"SALE_{i:08d}",
        vin=f"VIN_{i:08d}",
        plant_id=rng.choice(PLANTS),
        dealer_id=f"DLR_{rng.randint(1, 10):03d}",
        customer_id=f"CUST_{rng.randint(1, 5000):05d}",
        customer_email=f"user{rng.randint(1, 5000)}@example.com",
        customer_phone=f"+49{rng.randint(1000000000, 1999999999)}",
        sale_date=dt.date.today().isoformat(),
        sale_amount=round(rng.uniform(30000, 120000), 2),
        currency="EUR",
        engine_type=rng.choice(ENGINES),
        model_code=rng.choice(MODELS),
        warranty_tier="STANDARD",
    )
 
 
INSERT_SALE_SQL = """INSERT INTO daimler.vehicle_sales
 (sale_id,vin,plant_id,dealer_id,customer_id,customer_email,customer_phone,
  sale_date,sale_amount,currency,engine_type,model_code,warranty_tier)
 VALUES (:sale_id,:vin,:plant_id,:dealer_id,:customer_id,:customer_email,:customer_phone,
  :sale_date,:sale_amount,:currency,:engine_type,:model_code,:warranty_tier)"""
INSERT_SALE = text(INSERT_SALE_SQL)
INSERT_EVENT_SQL = """INSERT INTO outbox.event (id,aggregatetype,aggregateid,type,payload)
 VALUES (:id,:aggregatetype,:aggregateid,:type,CAST(:payload AS JSONB))"""
INSERT_EVENT = text(INSERT_EVENT_SQL)
DELETE_EVENT = text("DELETE FROM outbox.event WHERE id = :id")
 
 
def event_for(sale, etype="sale.created"):
    eid = str(uuid.uuid4())
    body = dict(sale, event_id=eid, event_type=etype, occurred_at=dt.datetime.now(dt.timezone.utc).isoformat())
    if not avro.io.validate(CONTRACT, body):  # producer-side gate: a bad shape never enters the outbox
        raise ValueError(f"event does not match contracts/sale_event_v1.avsc: {sale['sale_id']}")
    return dict(id=eid, aggregatetype="sale", aggregateid=sale["sale_id"], type=etype, payload=json.dumps(body))
 
 
def save_dual_write(engine, sale, publish, crash_rate, rng):
    """OLD WAY. Step 1 saves the row. Step 2 sends the event. Two separate steps."""
    with Session(engine) as s, s.begin():
        s.execute(INSERT_SALE, sale)  # step 1: committed when this block ends
    if rng.random() < crash_rate:  # the process dies HERE
        raise SimulatedCrash("died between row commit and publish")
    publish(event_for(sale))  # step 2: never reached after a crash
 
 
def save_outbox(engine, sale, crash_rate, rng, delete_after=True):
    """NEW WAY. The row and the event are written in ONE transaction."""
    ev = event_for(sale)
    with Session(engine) as s, s.begin():  # begin() = one transaction
        s.execute(INSERT_SALE, sale)
        if rng.random() < crash_rate:  # the process dies HERE, before commit
            raise SimulatedCrash("died before commit")  # Postgres rolls BOTH back
        s.execute(INSERT_EVENT, ev)
        if delete_after:
            s.execute(DELETE_EVENT, {"id": ev["id"]})  # table stays small; insert is still in the WAL
 
 
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["dual", "outbox"], required=True)
    ap.add_argument("--n", type=int, default=1000)
    ap.add_argument("--start", type=int, default=0)
    ap.add_argument("--crash-rate", type=float, default=0.0)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--sink", choices=["kafka", "file"], default="kafka", help="where the dual-write mode sends its event")
    ap.add_argument("--keep-outbox-rows", action="store_true")
    a = ap.parse_args()
    rng = random.Random(a.seed)
    engine = make_engine()
    if a.mode == "dual" and a.sink == "file":
        fh = open("docs/dual_write_events.jsonl", "a")
 
        def publish(ev):
            fh.write(ev["payload"] + "\n")
 
    elif a.mode == "dual":
        from confluent_kafka import Producer
 
        p = Producer({"bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")})
 
        def publish(ev):
            p.produce("dual.sale", key=ev["aggregateid"], value=ev["payload"])
            p.flush()  # wait until Kafka confirms. This is step 2, and it can fail on its own.
 
    crashes = 0
    for i in range(a.start, a.start + a.n):
        sale = fake_sale(i, rng)
        try:
            if a.mode == "dual":
                save_dual_write(engine, sale, publish, a.crash_rate, rng)
            else:
                save_outbox(engine, sale, a.crash_rate, rng, not a.keep_outbox_rows)
        except SimulatedCrash:
            crashes += 1
    print(f"mode={a.mode} attempted={a.n} simulated_crashes={crashes}")
 
 
if __name__ == "__main__":
    main()
