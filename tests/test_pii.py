# ~/de_lab/projects/enterprise-cdc-lakehouse/tests/test_pii.py
import os, sys, uuid
 
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "scripts"))
from pii_lib import hmac_hex, scan_values
 
 
def test_same_value_same_key_gives_same_hash():
    assert hmac_hex("a@b.io", "k1") == hmac_hex("a@b.io", "k1")
 
 
def test_a_different_key_gives_a_different_hash():
    assert hmac_hex("a@b.io", "k1") != hmac_hex("a@b.io", "k2")
 
 
def test_hashes_dates_and_uuids_are_clean():
    clean = [hmac_hex(i, "k") for i in range(200)] + ["2026-10-01", "2026-10-01 10:00:00"] + [str(uuid.uuid4()) for _ in range(200)]
    assert scan_values(clean) == (0, 0)
 
 
def test_planted_email_and_phone_are_caught():
    mixed = [hmac_hex(i, "k") for i in range(50)] + ["bob@corp.io", "+4915112345678"]
    assert scan_values(mixed) == (1, 1)
