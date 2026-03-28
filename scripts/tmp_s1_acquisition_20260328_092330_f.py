
import requests, pandas as pd, time, itertools, os

YEAR = 2010
EU28 = ['AT','BE','BG','CY','CZ','DE','DK','EE','EL','ES',
        'FI','FR','HR','HU','IE','IT','LT','LU','LV','MT',
        'NL','PL','PT','RO','SE','SI','SK','UK']
NON_EU = ['US']
EMP_ENDPOINT = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/nama_10_a64_e"
os.makedirs("data/raw", exist_ok=True)

LEAF_NACE = [
    "A01","A02","A03","B","C10-C12","C13-C15","C16","C17","C18","C19",
    "C20","C21","C22","C23","C24","C25","C26","C27","C28","C29","C30",
    "C31_C32","C33","D35","E36","E37-E39","F","G45","G46","G47",
    "H49","H50","H51","H52","H53","I","J58","J59_J60","J61","J62_J63",
    "K64","K65","K66","L68","M69_M70","M71","M72","M73","M74_M75",
    "N77","N78","N79","N80-N82","O84","P85","Q86","Q87_Q88",
    "R90-R92","R93","S94","S95","S96","T","U"
]

def fetch_json(url, params, max_retries=3, backoff=5):
    for attempt in range(1, max_retries + 1):
        try:
            r = requests.get(url, params=params, timeout=120)
            if r.status_code == 200:
                return r.json()
            print(f"  ⚠  HTTP {r.status_code} (attempt {attempt})")
        except Exception as e:
            print(f"  ⚠  {e} (attempt {attempt})")
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

# Download employment for all relevant countries
all_countries = EU28 + NON_EU + ["NO", "CH"]
emp_frames, failed_emp = [], []

print(f"Downloading employment data for {len(all_countries)} countries …")
for i, geo in enumerate(all_countries, 1):
    params = {"na_item":"EMP_DC","unit":"THS_PER","geo":geo,
              "time":str(YEAR),"format":"JSON","lang":"EN"}
    js  = fetch_json(EMP_ENDPOINT, params)
    df  = jsonstat_to_df(js)
    if df.empty:
        print(f"  [{i:2d}] {geo}: ✗ empty")
        failed_emp.append(geo)
        continue
    # post-filter to leaf NACE
    df = df[df["nace_r2"].isin(LEAF_NACE)].copy()
    emp_frames.append(df)
    print(f"  [{i:2d}] {geo}: ✓  {len(df):3d} rows, {df['nace_r2'].nunique()} NACE codes")
    time.sleep(0.3)

emp_df = pd.concat(emp_frames, ignore_index=True)

# Save
emp_df.to_csv("data/raw/employment_2010.csv", index=False)
print(f"\n✓ Saved employment_2010.csv  shape={emp_df.shape}")

# Verify
eu_emp     = emp_df[emp_df["geo"].isin(EU28)]["OBS_VALUE"].sum()
target     = 225_677
pct_diff   = abs(eu_emp - target) / target * 100
missing_eu = [c for c in EU28 if c not in emp_df["geo"].values]
print(f"\nEU-28 total employment : {eu_emp:>10,.1f} thousand")
print(f"Target                 : {target:>10,}  thousand")
print(f"Deviation              : {pct_diff:.2f}%  {'✓ OK' if pct_diff<=5 else '✗ WARN'}")
print(f"Missing EU countries   : {missing_eu if missing_eu else 'None ✓'}")
print(f"Failed downloads       : {failed_emp}")
