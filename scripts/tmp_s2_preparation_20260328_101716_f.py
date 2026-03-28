
import pandas as pd
import numpy as np
import json
import os
from pathlib import Path

RAW_ICIOT  = "data/raw/figaro_iciot_2010.csv"
RAW_EMP    = "data/raw/employment_2010.csv"
OUT_DIR    = Path("data/prepared")
OUT_DIR.mkdir(parents=True, exist_ok=True)
YEAR = 2010

EU28 = ['AT','BE','BG','CY','CZ','DE','DK','EE','EL','ES',
        'FI','FR','HR','HU','IE','IT','LT','LU','LV','MT',
        'NL','PL','PT','RO','SE','SI','SK','UK']
eu_set = set(EU28)

print("Reloading IC-IOT ...")
dtype_map = {'prd_use': str,'prd_ava': str,'c_dest': str,'c_orig': str,
             'unit': str,'OBS_VALUE': float}
iciot = pd.read_csv(
    RAW_ICIOT,
    usecols=['c_orig','c_dest','prd_ava','prd_use','unit','time','OBS_VALUE'],
    dtype=dtype_map
)
iciot = iciot[(iciot['time'] == YEAR) & (iciot['unit'] == 'MIO_EUR')].copy()

cpa_codes = sorted([p for p in iciot['prd_ava'].unique() if p.startswith('CPA_')])
N_PROD = len(cpa_codes); N_CTRY = len(EU28); N_TOTAL = N_CTRY * N_PROD
cpa_set = set(cpa_codes)
idx_labels = [f"{c}_{p}" for c in EU28 for p in cpa_codes]
idx = pd.Index(idx_labels)

# ── Rebuild Z_EU quickly (already validated shape) ────────────────────────────
mask_Z = (iciot['c_orig'].isin(eu_set) & iciot['c_dest'].isin(eu_set) &
          iciot['prd_ava'].isin(cpa_set) & iciot['prd_use'].isin(cpa_set))
df_Z = iciot[mask_Z].copy()
df_Z['row_key'] = df_Z['c_orig'] + '_' + df_Z['prd_ava']
df_Z['col_key'] = df_Z['c_dest'] + '_' + df_Z['prd_use']
Z_pivot = df_Z.pivot_table(index='row_key', columns='col_key',
                            values='OBS_VALUE', aggfunc='sum', fill_value=0.0)
Z_EU = Z_pivot.reindex(index=idx, columns=idx, fill_value=0.0)

# ── STEP 3: Build e_nonEU (Arto 2015 definition) ─────────────────────────────
print("=" * 65)
print("STEP 3 — Building e_nonEU (EU→non-EU, all flows)")
print("=" * 65)

# ALL deliveries from EU c_orig to non-EU c_dest where prd_ava is CPA
# (includes prd_use = CPA code or P* final demand code)
non_eu_set = set(iciot['c_dest'].unique()) - eu_set
mask_e = (
    iciot['c_orig'].isin(eu_set) &
    iciot['c_dest'].isin(non_eu_set) &
    iciot['prd_ava'].isin(cpa_set)            # row is a product (not value-added)
)
df_e = iciot[mask_e].copy()
print(f"  EU→non-EU rows (prd_ava=CPA, all prd_use types): {len(df_e):,}")

# how many are intermediate vs final demand?
is_fd = df_e['prd_use'].str.startswith('P')
print(f"    → intermediate (prd_use=CPA): {(~is_fd).sum():,}")
print(f"    → final demand  (prd_use=P*): {is_fd.sum():,}")

# aggregate per (c_orig, prd_ava)
df_e['row_key'] = df_e['c_orig'] + '_' + df_e['prd_ava']
e_series = df_e.groupby('row_key')['OBS_VALUE'].sum()
e_nonEU  = e_series.reindex(idx, fill_value=0.0)

# clip small negatives (CIF/FOB adjustments)
n_neg = (e_nonEU < 0).sum()
e_nonEU_vals = np.clip(e_nonEU.values, 0, None)
print(f"  Values clipped from negative to 0: {n_neg}")
print(f"  e_nonEU sum: {e_nonEU_vals.sum():,.1f} MIO_EUR")
print(f"  e_nonEU min: {e_nonEU_vals.min():.4f}  max: {e_nonEU_vals.max():,.1f}")

# ── STEP 4: Build x_EU ───────────────────────────────────────────────────────
print("\n" + "=" * 65)
print("STEP 4 — Building x_EU (row sums of all deliveries from EU origins)")
print("=" * 65)

# x[r,i] = sum over ALL c_dest and ALL prd_use (intermediate + final demand)
# where c_orig ∈ EU, prd_ava is CPA
mask_x = (
    iciot['c_orig'].isin(eu_set) &
    iciot['prd_ava'].isin(cpa_set)   # prd_use can be anything (CPA or P*)
)
df_x = iciot[mask_x].copy()
print(f"  EU-origin CPA rows (all destinations, all prd_use): {len(df_x):,}")
df_x['row_key'] = df_x['c_orig'] + '_' + df_x['prd_ava']
x_series = df_x.groupby('row_key')['OBS_VALUE'].sum()
x_EU = x_series.reindex(idx, fill_value=0.0)
print(f"  x_EU sum:  {x_EU.values.sum():,.1f} MIO_EUR")
print(f"  x_EU min:  {x_EU.values.min():.4f}  (negatives expected?)")
n_neg_x = (x_EU.values < 0).sum()
print(f"  Negative x_EU entries: {n_neg_x}")

# ── STEP 5: Build f_intraEU_final (for reference) ────────────────────────────
print("\n" + "=" * 65)
print("STEP 5 — Building f_intraEU_final (EU→EU final demand)")
print("=" * 65)
fd_codes_prd_use = [c for c in iciot['prd_use'].unique() if c.startswith('P')]
print(f"  Final demand prd_use codes: {sorted(fd_codes_prd_use)}")

mask_f = (
    iciot['c_orig'].isin(eu_set) &
    iciot['c_dest'].isin(eu_set) &
    iciot['prd_ava'].isin(cpa_set) &
    iciot['prd_use'].isin(fd_codes_prd_use)
)
df_f = iciot[mask_f].copy()
df_f['row_key'] = df_f['c_orig'] + '_' + df_f['prd_ava']
# columns = destination country (28 EU countries)
f_pivot = df_f.pivot_table(index='row_key', columns='c_dest',
                            values='OBS_VALUE', aggfunc='sum', fill_value=0.0)
f_intraEU = f_pivot.reindex(index=idx, columns=EU28, fill_value=0.0)
print(f"  f_intraEU_final shape: {f_intraEU.shape}")
print(f"  f_intraEU_final sum: {f_intraEU.values.sum():,.1f} MIO_EUR")
