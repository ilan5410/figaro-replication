"""Get all available dimension codes for naio_10_fcp_ip1 by querying with a known c_orig."""
import urllib.request
import json

# Use c_orig=DE, which has data for all destinations and products
# The statistics API returns all codes for the active dimensions
url = (
    "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/"
    "naio_10_fcp_ip1?c_orig=DE&time=2010&unit=MIO_EUR&format=JSON&lang=EN"
)
print("Fetching all codes via c_orig=DE query...")
with urllib.request.urlopen(url, timeout=60) as resp:
    data = json.load(resp)

dims = data.get("id", [])
size = data.get("size", [])
print(f"Dims: {dims}")
print(f"Size: {size}")
print(f"Total values: {len(data.get('value', {}))}")

for dim in dims:
    cats = data.get("dimension", {}).get(dim, {}).get("category", {})
    codes = list(cats.get("index", {}).keys())
    labels = cats.get("label", {})
    print(f"\n--- {dim} ({len(codes)} codes) ---")
    for c in codes:
        print(f"  {c}: {labels.get(c, '')}")

# Save
with open("/tmp/iciot_de_allcodes.json", "w") as f:
    meta = {k: v for k, v in data.items() if k != "value"}
    json.dump(meta, f, indent=2)
print("\nSaved to /tmp/iciot_de_allcodes.json")
