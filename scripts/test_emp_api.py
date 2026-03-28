"""Test employment API approaches."""
import urllib.request
import json

STAT_API = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data"

# Test 1: No nace filter, just geo+na_item+unit+time
url1 = f"{STAT_API}/nama_10_a64_e?geo=AT&na_item=EMP_DC&unit=THS_PER&time=2010&format=JSON&lang=EN"
print(f"Test 1 (no nace filter): {url1[:80]}...")
try:
    with urllib.request.urlopen(url1, timeout=30) as r:
        data = json.load(r)
    nace_cats = data.get("dimension", {}).get("nace_r2", {}).get("category", {})
    codes = list(nace_cats.get("index", {}).keys())
    print(f"  OK - {len(codes)} NACE codes, {len(data.get('value', {}))} values")
except Exception as e:
    print(f"  Error: {e}")

# Test 2: A few specific NACE codes
url2 = f"{STAT_API}/nama_10_a64_e?geo=AT&na_item=EMP_DC&unit=THS_PER&nace_r2=A01+A02&time=2010&format=JSON&lang=EN"
print(f"\nTest 2 (2 nace codes): {url2[:80]}...")
try:
    with urllib.request.urlopen(url2, timeout=30) as r:
        data = json.load(r)
    print(f"  OK - {len(data.get('value', {}))} values")
except Exception as e:
    print(f"  Error: {e}")

# Test 3: Try with hyphens encoded - A01 A02 C10-C12
url3 = f"{STAT_API}/nama_10_a64_e?geo=AT&na_item=EMP_DC&unit=THS_PER&nace_r2=A01+C10-C12&time=2010&format=JSON&lang=EN"
print(f"\nTest 3 (hyphenated code): {url3}")
try:
    with urllib.request.urlopen(url3, timeout=30) as r:
        data = json.load(r)
    print(f"  OK - {len(data.get('value', {}))} values")
    nace_cats = data.get("dimension", {}).get("nace_r2", {}).get("category", {})
    print(f"  NACE codes returned: {list(nace_cats.get('index', {}).keys())}")
except Exception as e:
    print(f"  Error: {e}")
