
import requests
import pandas as pd
import time
import json

# ── Configuration ──────────────────────────────────────────────────────────────
YEAR = 2010
EU28 = ['AT','BE','BG','CY','CZ','DE','DK','EE','EL','ES',
        'FI','FR','HR','HU','IE','IT','LT','LU','LV','MT',
        'NL','PL','PT','RO','SE','SI','SK','UK']
NON_EU = ['US']

EMP_ENDPOINT  = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/nama_10_a64_e"
ICIOT_ENDPOINT = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/naio_10_fcp_ip1"

# 64 leaf NACE codes we need
LEAF_NACE = [
    "A01","A02","A03","B",
    "C10-C12","C13-C15","C16","C17","C18","C19",
    "C20","C21","C22","C23","C24","C25","C26","C27","C28","C29","C30",
    "C31_C32","C33","D35","E36","E37-E39","F",
    "G45","G46","G47","H49","H50","H51","H52","H53",
    "I","J58","J59_J60","J61","J62_J63",
    "K64","K65","K66","L68",
    "M69_M70","M71","M72","M73","M74_M75",
    "N77","N78","N79","N80-N82",
    "O84","P85","Q86","Q87_Q88",
    "R90-R92","R93","S94","S95","S96","T","U"
]
print(f"Expected leaf NACE codes: {len(LEAF_NACE)}")

# ── Helper: robust GET with retries ───────────────────────────────────────────
def fetch_json(url, params, max_retries=3, backoff=5):
    for attempt in range(1, max_retries + 1):
        try:
            r = requests.get(url, params=params, timeout=120)
            if r.status_code == 200:
                return r.json()
            else:
                print(f"  ⚠  HTTP {r.status_code} (attempt {attempt}/{max_retries})")
        except Exception as e:
            print(f"  ⚠  Exception: {e} (attempt {attempt}/{max_retries})")
        if attempt < max_retries:
            time.sleep(backoff * attempt)
    return None

# ── Helper: parse Eurostat JSON-stat response → DataFrame ─────────────────────
def jsonstat_to_df(js):
    """Convert Eurostat JSON-stat v2 response to a flat DataFrame."""
    if js is None:
        return pd.DataFrame()
    dims   = js.get("id", [])          # list of dimension names in order
    sizes  = js.get("size", [])        # number of categories per dimension
    labels = js.get("dimension", {})   # dim → {category: {index: {code: label}}}
    values = js.get("value", {})       # flat dict/list of observations

    if not dims or not values:
        return pd.DataFrame()

    # Build category code lists per dimension (in index order)
    cats = []
    for dim in dims:
        cat_idx = labels[dim]["category"]["index"]  # {code: position}
        # sort by position value to get ordered list of codes
        ordered = sorted(cat_idx.items(), key=lambda x: x[1])
        cats.append([c for c, _ in ordered])

    # Generate all index combinations
    import itertools
    combos = list(itertools.product(*cats))

    # Map flat observation index → value
    if isinstance(values, list):
        val_map = {i: v for i, v in enumerate(values) if v is not None}
    else:
        val_map = {int(k): v for k, v in values.items() if v is not None}

    rows = []
    for flat_idx, combo in enumerate(combos):
        if flat_idx in val_map:
            row = dict(zip(dims, combo))
            row["OBS_VALUE"] = val_map[flat_idx]
            rows.append(row)

    return pd.DataFrame(rows)

print("Helper functions defined.")
