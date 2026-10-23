# ~/de_lab/projects/enterprise-cdc-lakehouse/scripts/pii_lib.py
# Keyed hashing (HMAC-SHA256) and a PII pattern scan. Shared by Silver, the scan and the tests.
import re, hmac, hashlib
from pyspark.sql import functions as F
 
EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
PHONE_RE = re.compile(r"\+?\d[\d\s().-]{8,}\d")
ISO_DT_RE = re.compile(r"\d{4}-\d{2}-\d{2}(?:[T ]\d{2}:\d{2}(?::\d{2})?)?")
UUID_RE = re.compile(r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}")
HEX64_RE = re.compile(r"^[0-9a-f]{64}$")  # a finished HMAC-SHA256 hash is not PII
 
 
def hmac_hex(value, key):
    """One keyed hash. Same value + same key = same output, so joins still work."""
    return hmac.new(key.encode(), str(value).encode(), hashlib.sha256).hexdigest()
 
 
def hmac_column(col, key):
    """Spark column version. A plain row UDF, so it needs no Arrow and runs on any Java.
    The key is captured in the function, never printed or logged."""
    kb = key.encode()
 
    @F.udf("string")
    def _h(v):
        return None if v is None else hmac.new(kb, str(v).encode(), hashlib.sha256).hexdigest()
 
    return _h(F.col(col))
 
 
def looks_like_phone(text):
    """A phone number has 10 to 15 digits. A date such as 2026-10-01 has only 8, so it passes."""
    text = ISO_DT_RE.sub(" ", text)  # dates and timestamps are not phone numbers
    text = UUID_RE.sub(" ", text)  # neither are UUIDs such as event ids
    for m in PHONE_RE.finditer(text):
        digits = sum(ch.isdigit() for ch in m.group())
        if 10 <= digits <= 15:
            return True
    return False
 
 
def scan_values(values):
    """Count values that still look like an email or a phone number.
    Four false alarms were found by running this on real output, and all are handled:
    1. A 64-character hex string is a finished hash, so it is skipped (it had 17,430 false hits).
    2. A date like 2026-10-01 matched the first phone pattern, so phones need 10 to 15 digits.
    3. A timestamp such as 2026-10-01 10:00:00 reached 10 digits, so dates and times are removed first.
    4. A UUID (an event id) is long runs of digits split by hyphens, so UUIDs are removed first."""
    emails = phones = 0
    for v in values:
        if v is None:
            continue
        v = str(v)
        if HEX64_RE.match(v):
            continue
        if EMAIL_RE.search(v):
            emails += 1
        if looks_like_phone(v):
            phones += 1
    return emails, phones
