
import pandas as pd
import numpy as np
import json
import os
from pathlib import Path

# ── paths / config ───────────────────────────────────────────────────────────
RAW_ICIOT  = "data/raw/figaro_iciot_2010.csv"
RAW_EMP    = "data/raw/employment_2010.csv"
OUT_DIR    = Path("data/prepared")
OUT_DIR.mkdir(parents=True, exist_ok=True)
YEAR       = 2010

EU28 = ['AT','BE','BG','CY','CZ','DE','DK','EE','EL','ES',
        'FI','FR','HR','HU','IE','IT','LT','LU','LV','MT',
        'NL','PL','PT','RO','SE','SI','SK','UK']
eu_set = set(EU28)

# ── reload (fast path: reuse previous load if still in memory) ───────────────
print("=" * 65)
print("STEP 1 — Loading IC-IOT")
print("=" * 65)
dtype_map = {'prd_use': str,'prd_ava': str,'c_dest': str,'c_orig': str,
             'unit': str,'OBS_VALUE': float}
iciot = pd.read_csv(
    RAW_ICIOT,
    usecols=['c_orig','c_dest','prd_ava','prd_use','unit','time','OBS_VALUE'],
    dtype=dtype_map
)
iciot = iciot[(iciot['time'] == YEAR) & (iciot['unit'] == 'MIO_EUR')].copy()

cpa_codes = sorted([p for p in iciot['prd_ava'].unique() if p.startswith('CPA_')])
N_PROD    = len(cpa_codes)   # 64
N_CTRY    = len(EU28)        # 28
N_TOTAL   = N_CTRY * N_PROD  # 1792
cpa_set   = set(cpa_codes)

# ── build canonical index ────────────────────────────────────────────────────
# outer loop: EU28 countries, inner loop: CPA codes
idx_labels = [f"{c}_{p}" for c in EU28 for p in cpa_codes]
idx        = pd.Index(idx_labels)   # length 1792

print(f"  N_CTRY={N_CTRY}, N_PROD={N_PROD}, N_TOTAL={N_TOTAL}")
print(f"  First index label: {idx_labels[0]}")
print(f"  Last  index label: {idx_labels[-1]}")
print(f"  Index length: {len(idx)}")

# ── STEP 2: Build Z^EU ───────────────────────────────────────────────────────
print("\n" + "=" * 65)
print("STEP 2 — Building Z^EU (1792×1792)")
print("=" * 65)

mask_Z = (
    iciot['c_orig'].isin(eu_set) &
    iciot['c_dest'].isin(eu_set) &
    iciot['prd_ava'].isin(cpa_set) &
    iciot['prd_use'].isin(cpa_set)
)
df_Z = iciot[mask_Z].copy()
print(f"  Intermediate EU→EU rows: {len(df_Z):,}")

# create row/col keys
df_Z['row_key'] = df_Z['c_orig'] + '_' + df_Z['prd_ava']
df_Z['col_key'] = df_Z['c_dest'] + '_' + df_Z['prd_use']

# pivot to matrix
Z_pivot = df_Z.pivot_table(index='row_key', columns='col_key',
                            values='OBS_VALUE', aggfunc='sum', fill_value=0.0)
# reindex to canonical order
Z_EU = Z_pivot.reindex(index=idx, columns=idx, fill_value=0.0)
print(f"  Z_EU shape: {Z_EU.shape}")
print(f"  Z_EU first 3 row labels: {list(Z_EU.index[:3])}")
print(f"  Z_EU first 3 col labels: {list(Z_EU.columns[:3])}")
print(f"  Z_EU sum (total intermediate): {Z_EU.values.sum():,.1f} MIO_EUR")
