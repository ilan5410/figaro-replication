"""Fetch the PRD_USE, PRD_AVA, C_DEST, C_ORIG codelists to understand dimension values."""
import urllib.request
import gzip
import re

BASE = "https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/codelist/ESTAT"

codelists = [
    ("PRD_USE", "4.1"),
    ("PRD_AVA", "5.1"),
    ("C_DEST", "25.0"),
    ("C_ORIG", "6.1"),
]

for cl_id, version in codelists:
    url = f"{BASE}/{cl_id}/{version}?format=json"
    print(f"\nFetching {cl_id} v{version}...")
    req = urllib.request.Request(url, headers={"Accept-Encoding": "gzip"})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read()
        try:
            content = gzip.decompress(raw).decode()
        except Exception:
            content = raw.decode("utf-8", errors="replace")

        # Extract code IDs and names
        codes = re.findall(r'<s:Code id="([^"]+)"[^>]*>.*?<c:Name[^>]*>([^<]+)</c:Name>', content, re.DOTALL)
        print(f"  {len(codes)} codes:")
        for code_id, label in codes:
            print(f"    {code_id}: {label.strip()}")
    except Exception as e:
        print(f"  Error: {e}")
        # Try without version
        url2 = f"{BASE}/{cl_id}?format=json"
        print(f"  Retrying without version: {url2}")
        req2 = urllib.request.Request(url2, headers={"Accept-Encoding": "gzip"})
        try:
            with urllib.request.urlopen(req2, timeout=30) as resp2:
                raw2 = resp2.read()
            try:
                content2 = gzip.decompress(raw2).decode()
            except Exception:
                content2 = raw2.decode("utf-8", errors="replace")
            codes2 = re.findall(r'<s:Code id="([^"]+)"[^>]*>.*?<c:Name[^>]*>([^<]+)</c:Name>', content2, re.DOTALL)
            print(f"  {len(codes2)} codes found")
            for code_id, label in codes2:
                print(f"    {code_id}: {label.strip()}")
        except Exception as e2:
            print(f"  Also failed: {e2}")
