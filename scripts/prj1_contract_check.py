#!/usr/bin/env python3
# ~/de_lab/projects/enterprise-cdc-lakehouse/scripts/prj1_contract_check.py
# Contract gate for the sale event. Runs offline in CI. Needs only the `avro` package.
#   --validate FILE.json     does one sample event match the contract?
#   --compat OLD NEW         BACKWARD check: can a reader on NEW read data written with OLD?
import sys, io, json, argparse
import avro.schema, avro.io
 
 
def load(path):
    return avro.schema.parse(open(path).read())
 
 
def can_read(old_path, new_path):
    """Write one maximal sample with the OLD schema, then read it with the NEW schema as the reader."""
    old, new = load(old_path), load(new_path)
    sample = {}
    for f in old.fields:
        t = f.type.type if f.type.type != "union" else "null"
        sample[f.name] = {"string": "x", "double": 1.0, "long": 1, "int": 1, "boolean": True, "null": None}.get(t, "x")
    buf = io.BytesIO()
    avro.io.DatumWriter(old).write(sample, avro.io.BinaryEncoder(buf))
    buf.seek(0)
    try:
        avro.io.DatumReader(old, new).read(avro.io.BinaryDecoder(buf))
        return True, "compatible"
    except Exception as e:  # SchemaResolutionException and friends
        return False, f"{type(e).__name__}: {str(e).splitlines()[0]}"
 
 
def main():
    ap = argparse.ArgumentParser()
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--validate", metavar="SAMPLE_JSON")
    g.add_argument("--compat", nargs=2, metavar=("OLD", "NEW"))
    ap.add_argument("--schema", default="contracts/sale_event_v1.avsc")
    a = ap.parse_args()
    if a.validate:
        datum = json.load(open(a.validate))
        ok = avro.io.validate(load(a.schema), datum)
        print("VALID" if ok else "INVALID: the sample does not match the contract")
        sys.exit(0 if ok else 1)
    ok, why = can_read(*a.compat)
    print(("BACKWARD COMPATIBLE" if ok else "BREAKING CHANGE") + f"  ({why})")
    sys.exit(0 if ok else 1)
 
 
if __name__ == "__main__":
    main()
