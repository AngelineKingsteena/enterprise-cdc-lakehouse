# ~/de_lab/projects/enterprise-cdc-lakehouse/tests/test_contract.py
import json, os, sys
 
ROOT = os.path.join(os.path.dirname(__file__), "..")
sys.path.insert(0, os.path.join(ROOT, "scripts"))
import pytest
from prj1_contract_check import can_read, load
import avro.io
 
OLD = os.path.join(ROOT, "contracts", "sale_event_v1.avsc")
 
 
def variant(tmp_path, edit):
    s = json.load(open(OLD))
    edit(s)
    p = tmp_path / "new.avsc"
    p.write_text(json.dumps(s))
    return str(p)
 
 
def test_adding_an_optional_field_is_safe(tmp_path):
    new = variant(tmp_path, lambda s: s["fields"].append({"name": "loyalty_tier", "type": ["null", "string"], "default": None}))
    assert can_read(OLD, new)[0] is True
 
 
def test_adding_a_required_field_breaks(tmp_path):
    new = variant(tmp_path, lambda s: s["fields"].append({"name": "loyalty_tier", "type": "string"}))
    assert can_read(OLD, new)[0] is False
 
 
def test_renaming_a_field_breaks(tmp_path):
    def edit(s):
        for f in s["fields"]:
            if f["name"] == "sale_amount":
                f["name"] = "amount"
 
    assert can_read(OLD, variant(tmp_path, edit))[0] is False
 
 
def test_changing_a_type_breaks(tmp_path):
    def edit(s):
        for f in s["fields"]:
            if f["name"] == "sale_amount":
                f["type"] = "string"
 
    assert can_read(OLD, variant(tmp_path, edit))[0] is False
 
 
@pytest.mark.parametrize("name,ok", [("good_event", True), ("bad_missing_amount", False), ("bad_amount_is_text", False)])
def test_sample_events(name, ok):
    datum = json.load(open(os.path.join(ROOT, "tests", "fixtures", name + ".json")))
    assert avro.io.validate(load(OLD), datum) is ok
