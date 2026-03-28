"""Probe the Eurostat employment API to understand available NACE codes and data."""
import urllib.request
import json

# Get full Germany data to see all NACE codes
URL = (
    "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/"
    "nama_10_a64_e?geo=DE&na_item=EMP_DC&unit=THS_PER&time=2010&format=JSON&lang=EN"
)

print(f"Fetching: {URL}")
with urllib.request.urlopen(URL, timeout=60) as resp:
    data = json.load(resp)

dims = data.get("id", [])
size = data.get("size", [])
print(f"Dimensions: {dims}")
print(f"Size: {size}")

nace_cats = data.get("dimension", {}).get("nace_r2", {}).get("category", {})
nace_codes = list(nace_cats.get("index", {}).keys())
print(f"\nAll NACE codes ({len(nace_codes)}):")
print(nace_codes)

# Find the 64 individual industry codes (leaf nodes, not aggregates)
# These are the ones we need - single-letter + numbers, not aggregate codes like TOTAL, A, B-E etc
leaf_codes = [c for c in nace_codes if len(c) > 2 and "_" not in c and "-" not in c]
# Also include specific codes with underscores that are leaf nodes
leaf_codes_all = [
    c for c in nace_codes
    if c not in ["TOTAL", "A", "B", "B-E", "C", "D", "E", "F", "G", "G-I",
                 "H", "I", "J", "K", "L", "M", "M_N", "N", "O", "O-Q",
                 "P", "Q", "R", "R-U", "S", "T", "U"]
]
print(f"\nNon-aggregate codes ({len(leaf_codes_all)}):")
print(leaf_codes_all)

vals = data.get("value", {})
print(f"\nNon-zero values: {len(vals)}")

# Show some actual values
nace_idx = nace_cats.get("index", {})
time_cats = data.get("dimension", {}).get("time", {}).get("category", {}).get("label", {})
print(f"\nSample values for DE 2010:")
for k, v in list(vals.items())[:10]:
    # Decode index - dims order: freq, unit, nace_r2, na_item, geo, time
    idx = int(k)
    # size: [1, 1, 96, 1, 1, 1] -> nace is dim 2 (0-indexed)
    nace_i = idx  # only nace varies since others are fixed (size=1)
    # Reverse lookup
    for code, pos in nace_idx.items():
        if pos == nace_i:
            print(f"  {code}: {v} thousand persons")
            break
