
import requests, pandas as pd, time, itertools, os

# First, let's probe the API for one country to understand the dimension structure
ICIOT_ENDPOINT = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/naio_10_fcp_ip1"

params = {
    "c_orig": "AT",
    "unit":   "MIO_EUR",
    "time":   "2010",
    "format": "JSON",
    "lang":   "EN",
}
print("Probing API for AT (Austria) …")
r = requests.get(ICIOT_ENDPOINT, params=params, timeout=180)
print(f"Status: {r.status_code}")
print(f"Response size: {len(r.content):,} bytes")

if r.status_code == 200:
    js = r.json()
    print(f"\nDimensions: {js.get('id')}")
    print(f"Sizes:      {js.get('size')}")
    # Sample the category codes for each dimension
    for dim in js.get('id', []):
        cats = list(js['dimension'][dim]['category']['index'].keys())
        print(f"\n  {dim} ({len(cats)} codes): first 5 = {cats[:5]}  last 5 = {cats[-5:]}")
    total_vals = sum(1 for v in (js.get('value', {}) if isinstance(js.get('value'), dict) 
                                  else enumerate(js.get('value', []))) if True)
    val_obj = js.get('value', {})
    n_obs = len(val_obj) if isinstance(val_obj, dict) else len([v for v in val_obj if v is not None])
    print(f"\n  Non-null observations: {n_obs:,}")
