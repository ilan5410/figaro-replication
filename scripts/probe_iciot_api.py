"""Probe the Eurostat IC-IOT API to understand dimensions and data structure."""
import urllib.request
import urllib.error
import gzip
import json

URL = (
    "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/"
    "naio_10_fcp_ip1?format=json&time=2010"
)

print(f"Fetching: {URL}")
req = urllib.request.Request(URL, headers={"Accept-Encoding": "gzip"})
with urllib.request.urlopen(req, timeout=120) as resp:
    raw = resp.read()

# Decompress if gzipped
try:
    data = json.loads(gzip.decompress(raw))
except Exception:
    data = json.loads(raw)

dims = data.get("id", [])
size = data.get("size", [])
print(f"\nDimensions: {dims}")
print(f"Size: {size}")

for dim in dims:
    cats = data.get("dimension", {}).get(dim, {}).get("category", {})
    codes = list(cats.get("index", {}).keys())
    print(f"\n  {dim}: {len(codes)} codes")
    if len(codes) <= 20:
        print(f"    {codes}")
    else:
        print(f"    first 10: {codes[:10]}")
        print(f"    last 10: {codes[-10:]}")

total_vals = len(data.get("value", {}))
print(f"\nTotal non-zero values: {total_vals}")

# Save full response for inspection
with open("/tmp/iciot_2010_sample.json", "w") as f:
    json.dump(data, f, indent=2)
print("Full response saved to /tmp/iciot_2010_sample.json")
