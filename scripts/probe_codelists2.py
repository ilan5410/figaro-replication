"""Try multiple approaches to get codelist data from Eurostat."""
import urllib.request
import gzip
import json

# Try the SDMX API without version number, and also try the statistics API
# to get actual codes from a small data query

# First: try a tiny SDMX data query to see what codes come back in the response
# Use c_orig=DE (Germany) only, restrict to one year
url = (
    "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/"
    "naio_10_fcp_ip1/.....MIO_EUR./DE?format=json&startPeriod=2010&endPeriod=2010"
)
print(f"Trying: {url}")
req = urllib.request.Request(url, headers={"Accept-Encoding": "gzip"})
try:
    with urllib.request.urlopen(req, timeout=60) as resp:
        raw = resp.read()
    try:
        content = gzip.decompress(raw)
    except Exception:
        content = raw
    data = json.loads(content)
    dims = data.get("id", [])
    size = data.get("size", [])
    nvals = len(data.get("value", {}))
    print(f"  Dims: {dims}")
    print(f"  Size: {size}")
    print(f"  Values: {nvals}")
    for dim in dims:
        cats = data.get("dimension", {}).get(dim, {}).get("category", {})
        codes = list(cats.get("index", {}).keys())
        labels = cats.get("label", {})
        print(f"\n  {dim} ({len(codes)} codes):")
        for c in codes[:20]:
            print(f"    {c}: {labels.get(c, '')}")
        if len(codes) > 20:
            print(f"    ... ({len(codes) - 20} more)")

    # Save the dimension metadata
    meta = {k: v for k, v in data.items() if k != "value"}
    with open("/tmp/iciot_de_meta.json", "w") as f:
        json.dump(meta, f, indent=2)
    print("\nMetadata saved to /tmp/iciot_de_meta.json")
except Exception as e:
    print(f"  Error: {e}")

# Try alternative URL formats
alt_urls = [
    "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/naio_10_fcp_ip1/A..MIO_EUR.DE.?format=json&startPeriod=2010&endPeriod=2010",
    "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/naio_10_fcp_ip1?format=json&c_orig=DE&time=2010&unit=MIO_EUR",
]
for url2 in alt_urls:
    print(f"\nAlso trying: {url2}")
    req2 = urllib.request.Request(url2, headers={"Accept-Encoding": "gzip"})
    try:
        with urllib.request.urlopen(req2, timeout=30) as resp2:
            raw2 = resp2.read()
        try:
            content2 = gzip.decompress(raw2)
        except Exception:
            content2 = raw2
        data2 = json.loads(content2)
        print(f"  Dims: {data2.get('id', [])}, Size: {data2.get('size', [])}, Values: {len(data2.get('value', {}))}")
    except Exception as e2:
        print(f"  Error: {e2}")
