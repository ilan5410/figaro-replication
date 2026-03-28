
import requests, pandas as pd, time, itertools, os

YEAR = 2010
ICIOT_ENDPOINT = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/naio_10_fcp_ip1"
OUT_PATH = "data/raw/figaro_iciot_2010.csv"
os.makedirs("data/raw", exist_ok=True)

# Full list of 50 c_orig countries (same set as c_dest)
ALL_ORIG = [
    'BE','BG','CZ','DK','DE','EE','IE','EL','ES','FR',
    'HR','IT','CY','LV','LT','LU','HU','MT','NL','AT',
    'PL','PT','RO','SI','SK','FI','SE','NO','CH','UK',
    'ME','MK','AL','RS','TR','RU','ZA','CA','US','MX',
    'AR','BR','CN','JP','KR','IN','ID','SA','AU','WRL_REST'
]
print(f"Total c_orig countries to download: {len(ALL_ORIG)}")

def fetch_json(url, params, max_retries=3, backoff=5):
    for attempt in range(1, max_retries + 1):
        try:
            r = requests.get(url, params=params, timeout=180)
            if r.status_code == 200:
                return r.json()
            print(f"    ⚠  HTTP {r.status_code} (attempt {attempt}/{max_retries})")
        except Exception as e:
            print(f"    ⚠  {e} (attempt {attempt}/{max_retries})")
        if attempt < max_retries:
            time.sleep(backoff * attempt)
    return None

def jsonstat_to_df(js):
    if js is None:
        return pd.DataFrame()
    dims   = js.get("id", [])
    labels = js.get("dimension", {})
    values = js.get("value", {})
    if not dims or not values:
        return pd.DataFrame()
    cats = []
    for dim in dims:
        cat_idx = labels[dim]["category"]["index"]
        ordered = sorted(cat_idx.items(), key=lambda x: x[1])
        cats.append([c for c, _ in ordered])
    combos = list(itertools.product(*cats))
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

# Track already-downloaded countries (resume support)
if os.path.exists(OUT_PATH):
    existing = pd.read_csv(OUT_PATH, usecols=["c_orig"])
    done_set = set(existing["c_orig"].unique())
    print(f"Resuming: {len(done_set)} countries already saved: {sorted(done_set)}")
else:
    done_set = set()
    # Write header
    header_df = pd.DataFrame(columns=["freq","prd_use","prd_ava","c_dest","unit","c_orig","time","OBS_VALUE"])
    header_df.to_csv(OUT_PATH, index=False)
    print("Starting fresh download …")

failed_iciot = []
total_rows = sum(1 for _ in open(OUT_PATH)) - 1  # subtract header

for i, c_orig in enumerate(ALL_ORIG, 1):
    if c_orig in done_set:
        print(f"  [{i:2d}/{len(ALL_ORIG)}] {c_orig:10s}: ⏭  already downloaded")
        continue

    params = {
        "c_orig": c_orig,
        "unit":   "MIO_EUR",
        "time":   str(YEAR),
        "format": "JSON",
        "lang":   "EN",
    }
    js = fetch_json(ICIOT_ENDPOINT, params)
    df = jsonstat_to_df(js)

    if df.empty:
        print(f"  [{i:2d}/{len(ALL_ORIG)}] {c_orig:10s}: ✗ empty / failed")
        failed_iciot.append(c_orig)
        continue

    # Append to CSV (no header since already written)
    df.to_csv(OUT_PATH, mode="a", header=False, index=False)
    total_rows += len(df)
    done_set.add(c_orig)
    print(f"  [{i:2d}/{len(ALL_ORIG)}] {c_orig:10s}: ✓  {len(df):>7,} rows  |  cumulative: {total_rows:>9,}")
    time.sleep(0.4)   # polite pause

print(f"\n{'='*60}")
print(f"IC-IOT download complete!")
print(f"Countries downloaded: {len(done_set)}/{len(ALL_ORIG)}")
print(f"Total rows in file:   {total_rows:,}")
print(f"Failed:               {failed_iciot}")
