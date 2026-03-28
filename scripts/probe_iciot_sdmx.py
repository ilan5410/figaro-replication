"""Probe IC-IOT SDMX API with a small country slice to learn dimensions."""
import urllib.request
import gzip
import json

# The SDMX endpoint for naio_10_fcp_ip1
# Try getting just Germany's rows
BASE = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/naio_10_fcp_ip1"

# Try several URL formats
urls_to_try = [
    f"{BASE}/.DE.....?format=json&startPeriod=2010&endPeriod=2010",
    f"{BASE}/A.DE.....?format=json&startPeriod=2010&endPeriod=2010",
    f"{BASE}?format=json&geo=DE&time=2010",
]

for url in urls_to_try:
    print(f"\nTrying: {url}")
    req = urllib.request.Request(url, headers={"Accept-Encoding": "gzip"})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read()
        try:
            content = gzip.decompress(raw)
        except Exception:
            content = raw
        data = json.loads(content)
        dims = data.get("id", [])
        size = data.get("size", [])
        nvals = len(data.get("value", {}))
        print(f"  OK - dims={dims}, size={size}, values={nvals}")
        if dims:
            for dim in dims:
                cats = data.get("dimension", {}).get(dim, {}).get("category", {})
                codes = list(cats.get("index", {}).keys())
                print(f"    {dim}: {codes[:10]}")
            # Save and stop on first success
            with open("/tmp/iciot_sdmx_sample.json", "w") as f:
                summary = {k: v for k, v in data.items() if k != "value"}
                json.dump(summary, f, indent=2)
            print("  Structure saved to /tmp/iciot_sdmx_sample.json")
            break
    except Exception as e:
        print(f"  Error: {e}")
