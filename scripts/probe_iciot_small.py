"""Probe IC-IOT API with a small slice to understand the dimension structure quickly."""
import urllib.request
import gzip
import json

# Request just the structure info - filter to one country pair to keep it tiny
# The naio_10_fcp_ip1 dataset dimensions are unknown - let's try with just time filter
# and a very small geographic filter to see what comes back
BASE = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/naio_10_fcp_ip1"
URL = f"{BASE}?format=JSON&lang=EN&time=2010"

print(f"Fetching small slice from: {BASE}")
print("(This may be large - checking structure only)")

req = urllib.request.Request(URL, headers={"Accept-Encoding": "gzip"})
try:
    with urllib.request.urlopen(req, timeout=30) as resp:
        raw = resp.read()
        print(f"Downloaded {len(raw)} bytes")
except Exception as e:
    print(f"Error: {e}")
    raise

try:
    content = gzip.decompress(raw)
except Exception:
    content = raw

# Parse just the structure (not all values)
data = json.loads(content)

print(f"\nDimensions: {data.get('id', [])}")
print(f"Size: {data.get('size', [])}")
print(f"Label: {data.get('label', '')}")
print(f"Updated: {data.get('updated', '')}")

for dim in data.get("id", []):
    cats = data.get("dimension", {}).get(dim, {}).get("category", {})
    codes = list(cats.get("index", {}).keys())
    labels = cats.get("label", {})
    print(f"\n  {dim}: {len(codes)} codes")
    for c in codes[:15]:
        print(f"    {c}: {labels.get(c, '')}")
    if len(codes) > 15:
        print(f"    ... ({len(codes) - 15} more)")

total = len(data.get("value", {}))
print(f"\nTotal non-zero values returned: {total}")

# Save full structure
with open("/tmp/iciot_structure.json", "w") as f:
    # Save only dimensions, not values (values could be huge)
    summary = {k: v for k, v in data.items() if k != "value"}
    json.dump(summary, f, indent=2)
print("Structure saved to /tmp/iciot_structure.json")
