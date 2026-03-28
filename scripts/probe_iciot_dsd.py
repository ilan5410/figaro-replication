"""Get the Data Structure Definition for IC-IOT to understand dimensions without downloading data."""
import urllib.request
import gzip

# The DSD (Data Structure Definition) tells us all dimensions and codes
urls = [
    "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/datastructure/ESTAT/NAIO_10_FCP_IP1?format=json",
    "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/dataflow/ESTAT/NAIO_10_FCP_IP1/1.0?format=json&references=all",
    "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/dataflow/ESTAT/NAIO_10_FCP_IP1/+?format=json",
]

for url in urls:
    print(f"\nFetching: {url}")
    req = urllib.request.Request(url, headers={"Accept-Encoding": "gzip"})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read()
        try:
            content = gzip.decompress(raw).decode()
        except Exception:
            content = raw.decode("utf-8", errors="replace")
        print(f"  Response ({len(content)} chars):")
        print(content[:3000])
        break
    except Exception as e:
        print(f"  Error: {e}")
