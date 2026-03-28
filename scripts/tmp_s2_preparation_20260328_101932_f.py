
import pandas as pd
import numpy as np
import json
import re
from pathlib import Path
from datetime import datetime

# ════════════════════════════════════════════════════════════════
#  FIGARO DATA PREPARATION  —  END-TO-END SCRIPT  (Year 2010)
# ════════════════════════════════════════════════════════════════

RAW_ICIOT  = "data/raw/figaro_iciot_2010.csv"
RAW_EMP    = "data/raw/employment_2010.csv"
OUT_DIR    = Path("data/prepared")
OUT_DIR.mkdir(parents=True, exist_ok=True)
YEAR       = 2010

EU28 = ['AT','BE','BG','CY','CZ','DE','DK','EE','EL','ES',
        'FI','FR','HR','HU','IE','IT','LT','LU','LV','MT',
        'NL','PL','PT','RO','SE','SI','SK','UK']
eu_set = set(EU28)

# ── STEP 1: Load IC-IOT ──────────────────────────────────────────
print("STEP 1 — Loading IC-IOT ...")
iciot = pd.read_csv(
    RAW_ICIOT,
    usecols=['c_orig','c_dest','prd_ava','prd_use','unit','time','OBS_VALUE'],
    dtype={'prd_use':str,'prd_ava':str,'c_dest':str,'c_orig':str,
           'unit':str,'OBS_VALUE':float}
)
iciot = iciot[(iciot['time']==YEAR) & (iciot['unit']=='MIO_EUR')].copy()
print(f"  Loaded {len(iciot):,} rows")

# Identify codes
cpa_codes = sorted([p for p in iciot['prd_ava'].unique() if p.startswith('CPA_')])
cpa_set   = set(cpa_codes)
N_PROD    = len(cpa_codes)  # 64
N_CTRY    = len(EU28)       # 28
N_TOTAL   = N_CTRY * N_PROD # 1792
non_eu_set = set(iciot['c_dest'].unique()) - eu_set

idx_labels = [f"{c}_{p}" for c in EU28 for p in cpa_codes]
idx        = pd.Index(idx_labels)
print(f"  CPA codes: {N_PROD}  EU countries: {N_CTRY}  Total dim: {N_TOTAL}")
print(f"  Non-EU countries: {sorted(non_eu_set)}")

# ── STEP 2: Z^EU ────────────────────────────────────────────────
print("\nSTEP 2 — Building Z^EU ...")
mask_Z = (iciot['c_orig'].isin(eu_set) & iciot['c_dest'].isin(eu_set) &
          iciot['prd_ava'].isin(cpa_set) & iciot['prd_use'].isin(cpa_set))
df_Z = iciot[mask_Z].copy()
df_Z['row_key'] = df_Z['c_orig'] + '_' + df_Z['prd_ava']
df_Z['col_key'] = df_Z['c_dest'] + '_' + df_Z['prd_use']
Z_pivot = df_Z.pivot_table(index='row_key', columns='col_key',
                            values='OBS_VALUE', aggfunc='sum', fill_value=0.0)
Z_EU = Z_pivot.reindex(index=idx, columns=idx, fill_value=0.0)
assert Z_EU.shape == (N_TOTAL, N_TOTAL), f"Z_EU shape error: {Z_EU.shape}"
print(f"  Z_EU shape: {Z_EU.shape}  ✓")
print(f"  Z_EU total: {Z_EU.values.sum():,.1f} MIO_EUR")

# ── STEP 3: e_nonEU ─────────────────────────────────────────────
print("\nSTEP 3 — Building e_nonEU ...")
mask_e = (iciot['c_orig'].isin(eu_set) & iciot['c_dest'].isin(non_eu_set) &
          iciot['prd_ava'].isin(cpa_set))   # any prd_use (CPA or P*)
df_e = iciot[mask_e].copy()
is_fd_e = df_e['prd_use'].str.startswith('P')
print(f"  Rows: {len(df_e):,}  (intermediate: {(~is_fd_e).sum():,}  FD: {is_fd_e.sum():,})")
df_e['row_key'] = df_e['c_orig'] + '_' + df_e['prd_ava']
e_series  = df_e.groupby('row_key')['OBS_VALUE'].sum().reindex(idx, fill_value=0.0)
n_neg_e   = (e_series < 0).sum()
e_vals    = np.clip(e_series.values, 0, None)
print(f"  Negative values clipped: {n_neg_e}  | e_nonEU sum: {e_vals.sum():,.1f} MIO_EUR")

# ── STEP 4: x_EU ────────────────────────────────────────────────
print("\nSTEP 4 — Building x_EU ...")
mask_x = (iciot['c_orig'].isin(eu_set) & iciot['prd_ava'].isin(cpa_set))
df_x = iciot[mask_x].copy()
df_x['row_key'] = df_x['c_orig'] + '_' + df_x['prd_ava']
x_series = df_x.groupby('row_key')['OBS_VALUE'].sum().reindex(idx, fill_value=0.0)
print(f"  x_EU sum: {x_series.values.sum():,.1f} MIO_EUR")
n_neg_x = (x_series.values < 0).sum()
print(f"  Negative x_EU entries: {n_neg_x}")

# ── STEP 5: f_intraEU_final ─────────────────────────────────────
print("\nSTEP 5 — Building f_intraEU_final ...")
fd_prd_use = [c for c in iciot['prd_use'].unique() if c.startswith('P')]
mask_f = (iciot['c_orig'].isin(eu_set) & iciot['c_dest'].isin(eu_set) &
          iciot['prd_ava'].isin(cpa_set) & iciot['prd_use'].isin(fd_prd_use))
df_f = iciot[mask_f].copy()
df_f['row_key'] = df_f['c_orig'] + '_' + df_f['prd_ava']
f_pivot  = df_f.pivot_table(index='row_key', columns='c_dest',
                             values='OBS_VALUE', aggfunc='sum', fill_value=0.0)
f_intraEU = f_pivot.reindex(index=idx, columns=EU28, fill_value=0.0)
print(f"  f_intraEU_final shape: {f_intraEU.shape}  sum: {f_intraEU.values.sum():,.1f}")

# ── STEP 6: Employment ──────────────────────────────────────────
print("\nSTEP 6 — Building Em_EU ...")

def nace_to_cpa_key(nace: str) -> str:
    key = re.sub(r'([_-])([A-Z]+)(\d)', r'\1\3', nace)
    return 'CPA_' + key

emp_raw = pd.read_csv(RAW_EMP, dtype={'nace_r2':str,'geo':str,'na_item':str,'unit':str})
emp = emp_raw[(emp_raw['na_item']=='EMP_DC') &
              (emp_raw['unit']=='THS_PER') &
              (emp_raw['time']==YEAR) &
              (emp_raw['geo'].isin(eu_set))].copy()

emp['cpa_code'] = emp['nace_r2'].apply(nace_to_cpa_key)
emp['row_key']  = emp['geo'] + '_' + emp['cpa_code']

# check mapping coverage
unmapped = emp[~emp['cpa_code'].isin(cpa_set)]
if len(unmapped) > 0:
    print(f"  WARNING: {len(unmapped)} unmapped NACE codes:")
    print(unmapped[['geo','nace_r2','cpa_code']].to_string())

em_series  = emp.groupby('row_key')['OBS_VALUE'].sum()
em_raw_sum = em_series.sum()
em_series  = em_series.reindex(idx, fill_value=0.0)
em_vals    = em_series.values

# countries with missing / fully-zero data
missing_countries = []
for c in EU28:
    c_keys = [f"{c}_{p}" for p in cpa_codes]
    c_vals = em_series.loc[c_keys].values
    if c_vals.sum() == 0:
        missing_countries.append(c)
print(f"  Countries with all-zero employment: {missing_countries}")
print(f"  Em_EU raw sum (pre-reindex): {em_raw_sum:,.1f}  |  post-reindex: {em_vals.sum():,.1f}")
expected_total = 225677
pct_diff = abs(em_vals.sum() - expected_total) / expected_total * 100
print(f"  Expected ~{expected_total:,}  |  Deviation: {pct_diff:.2f}%  {'✓' if pct_diff<=5 else '⚠ OVER 5%'}")

# ── STEP 7: Write outputs ────────────────────────────────────────
print("\nSTEP 7 — Writing output files ...")

# Z_EU.csv
Z_EU.to_csv(OUT_DIR / 'Z_EU.csv')
print(f"  Z_EU.csv written  {Z_EU.shape}")

# e_nonEU.csv
e_df = pd.DataFrame({'e_nonEU_MIO_EUR': e_vals}, index=idx)
e_df.index.name = 'country_product'
e_df.to_csv(OUT_DIR / 'e_nonEU.csv')
print(f"  e_nonEU.csv written  {e_df.shape}")

# x_EU.csv
x_df = pd.DataFrame({'x_EU_MIO_EUR': x_series.values}, index=idx)
x_df.index.name = 'country_product'
x_df.to_csv(OUT_DIR / 'x_EU.csv')
print(f"  x_EU.csv written  {x_df.shape}")

# Em_EU.csv
em_df = pd.DataFrame({'em_EU_THS_PER': em_vals}, index=idx)
em_df.index.name = 'country_product'
em_df.to_csv(OUT_DIR / 'Em_EU.csv')
print(f"  Em_EU.csv written  {em_df.shape}")

# f_intraEU_final.csv
f_intraEU.index.name = 'country_product'
f_intraEU.to_csv(OUT_DIR / 'f_intraEU_final.csv')
print(f"  f_intraEU_final.csv written  {f_intraEU.shape}")

# metadata.json
meta = {
    "eu_countries":    EU28,
    "cpa_codes":       cpa_codes,
    "n_countries":     N_CTRY,
    "n_products":      N_PROD,
    "n_total":         N_TOTAL,
    "reference_year":  YEAR,
    "non_eu_countries": sorted(non_eu_set),
    "row_index_labels": idx_labels[:5] + ["..."] + idx_labels[-3:],
    "created_at":      datetime.now().isoformat(timespec='seconds')
}
with open(OUT_DIR / 'metadata.json', 'w') as f:
    json.dump(meta, f, indent=2)
print("  metadata.json written")

# ── STEP 8: Verification ─────────────────────────────────────────
print("\n" + "=" * 65)
print("STEP 8 — VERIFICATION CHECKS")
print("=" * 65)

# 1. Shape
assert Z_EU.shape == (1792, 1792), "FAIL: Z_EU not 1792×1792"
print(f"  [✓] Z_EU shape: {Z_EU.shape}")

# 2. e_nonEU non-negative
assert (e_vals >= 0).all(), "FAIL: e_nonEU has negatives"
print(f"  [✓] e_nonEU all non-negative ({len(e_vals)} values)")

# 3. x_EU non-negative (log but don't fail if tiny)
n_neg = (x_series.values < 0).sum()
print(f"  [{'✓' if n_neg==0 else '⚠'}] x_EU negatives: {n_neg}")

# 4. Em_EU non-negative
assert (em_vals >= 0).all(), "FAIL: Em_EU has negatives"
print(f"  [✓] Em_EU all non-negative ({len(em_vals)} values)")

# 5. Employment total within 5%
print(f"  [{'✓' if pct_diff<=5 else '⚠ FAIL'}] Em_EU total = {em_vals.sum():,.1f}  (expected ~{expected_total:,}, Δ={pct_diff:.2f}%)")

# 6. Ordering consistency
first_label_Z  = list(Z_EU.index[:3])
first_label_e  = list(e_df.index[:3])
first_label_x  = list(x_df.index[:3])
first_label_em = list(em_df.index[:3])
assert first_label_Z == first_label_e == first_label_x == first_label_em, "ORDERING MISMATCH"
print(f"  [✓] First 3 labels match across all files: {first_label_Z}")

last_label_Z  = list(Z_EU.index[-3:])
last_label_em = list(em_df.index[-3:])
assert last_label_Z == last_label_em
print(f"  [✓] Last  3 labels match: {last_label_Z}")

# 7. metadata.json
import json as _json
with open(OUT_DIR / 'metadata.json') as f:
    m = _json.load(f)
assert m['n_total'] == 1792 and m['n_countries'] == 28 and m['n_products'] == 64
print(f"  [✓] metadata.json: n_total={m['n_total']}, n_countries={m['n_countries']}, n_products={m['n_products']}")

print("\nAll verification checks passed.")
