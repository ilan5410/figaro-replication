
import pandas as pd
import numpy as np
import json
import os
from pathlib import Path

# ── paths ────────────────────────────────────────────────────────────────────
RAW_ICIOT  = "data/raw/figaro_iciot_2010.csv"
RAW_EMP    = "data/raw/employment_2010.csv"
OUT_DIR    = Path("data/prepared")
OUT_DIR.mkdir(parents=True, exist_ok=True)
YEAR       = 2010

# ── EU-28 countries (given) ───────────────────────────────────────────────────
EU28 = ['AT','BE','BG','CY','CZ','DE','DK','EE','EL','ES',
        'FI','FR','HR','HU','IE','IT','LT','LU','LV','MT',
        'NL','PL','PT','RO','SE','SI','SK','UK']
eu_set = set(EU28)

print("=" * 65)
print("STEP 1 — Loading IC-IOT (this may take ~30 s for 11 M rows)")
print("=" * 65)

dtype_map = {
    'prd_use': str, 'prd_ava': str,
    'c_dest':  str, 'c_orig':  str,
    'unit':    str, 'OBS_VALUE': float
}
iciot = pd.read_csv(
    RAW_ICIOT,
    usecols=['c_orig','c_dest','prd_ava','prd_use','unit','time','OBS_VALUE'],
    dtype=dtype_map
)
# Keep only 2010 and MIO_EUR (file may have multiple years/units)
iciot = iciot[(iciot['time'] == YEAR) & (iciot['unit'] == 'MIO_EUR')].copy()
print(f"  Rows after year+unit filter: {len(iciot):,}")

# ── Identify CPA codes & value-added / final-demand codes ────────────────────
all_prd_ava = iciot['prd_ava'].unique()
cpa_codes   = sorted([p for p in all_prd_ava if p.startswith('CPA_')])
print(f"  CPA product codes found: {len(cpa_codes)}")
print(f"  First 5: {cpa_codes[:5]}")
print(f"  Last  5: {cpa_codes[-5:]}")

all_c_orig = iciot['c_orig'].unique()
all_c_dest = iciot['c_dest'].unique()
non_eu_orig = sorted(set(all_c_orig) - eu_set)
non_eu_dest = sorted(set(all_c_dest) - eu_set)
print(f"\n  Non-EU c_orig: {non_eu_orig}")
print(f"  Non-EU c_dest: {non_eu_dest}")
