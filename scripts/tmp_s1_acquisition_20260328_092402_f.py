
import requests, pandas as pd, time, itertools, os

# From the probe: 50 c_dest countries total
# Let's get the full list of c_orig countries available in the dataset
ICIOT_ENDPOINT = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/naio_10_fcp_ip1"

# The probe showed 50 c_dest countries — get the actual list
params = {
    "c_orig": "AT",
    "unit":   "MIO_EUR",
    "time":   "2010",
    "format": "JSON",
    "lang":   "EN",
}
r = requests.get(ICIOT_ENDPOINT, params=params, timeout=180)
js = r.json()

# Extract all country codes from c_dest (same set as c_orig)
c_dest_cats = list(js['dimension']['c_dest']['category']['index'].keys())
print(f"Countries in c_dest ({len(c_dest_cats)}):")
print(c_dest_cats)

# Extract prd_ava and prd_use codes
prd_ava_cats = list(js['dimension']['prd_ava']['category']['index'].keys())
prd_use_cats = list(js['dimension']['prd_use']['category']['index'].keys())
print(f"\nprd_ava ({len(prd_ava_cats)}): {prd_ava_cats}")
print(f"\nprd_use ({len(prd_use_cats)}): {prd_use_cats}")
