
import pandas as pd
import numpy as np
import os, json

# ─── Paths ───────────────────────────────────────────────────────────────────
ICIOT_PATH = "/Users/ilanpargamin/Dossiers importants non sync/figaro_replication/data/raw/figaro_iciot_2010.csv"
EMP_PATH   = "/Users/ilanpargamin/Dossiers importants non sync/figaro_replication/data/raw/employment_2010.csv"
OUT_DIR    = "data/prepared"
os.makedirs(OUT_DIR, exist_ok=True)

YEAR = 2010
EU28 = ['AT','BE','BG','CY','CZ','DE','DK','EE','EL','ES','FI','FR',
        'HR','HU','IE','IT','LT','LU','LV','MT','NL','PL','PT','RO',
        'SE','SI','SK','UK']
eu_set = set(EU28)

# ─── Step 1: Load IC-IOT ─────────────────────────────────────────────────────
print("Loading IC-IOT …")
df = pd.read_csv(ICIOT_PATH)
print(f"  Raw rows: {len(df):,}")

# ─── Step 2: Extract CPA codes (product list, natural sorted order from data) ─
cpa_codes = sorted([x for x in df['prd_ava'].unique() if str(x).startswith('CPA_')])
print(f"  CPA codes found: {len(cpa_codes)}")
assert len(cpa_codes) == 64, f"Expected 64 CPA codes, got {len(cpa_codes)}"

# ─── Step 3: Identify non-EU countries ───────────────────────────────────────
all_countries = sorted(df['c_orig'].unique())
non_eu = [c for c in all_countries if c not in eu_set]
print(f"  All countries in data ({len(all_countries)}): {all_countries}")
print(f"  Non-EU countries ({len(non_eu)}): {non_eu}")

# ─── Step 4: Build Z^EU (1792 × 1792) ────────────────────────────────────────
print("\nBuilding Z_EU …")
mask_Z = (
    df['c_orig'].isin(eu_set) &
    df['c_dest'].isin(eu_set) &
    df['prd_ava'].isin(cpa_codes) &
    df['prd_use'].isin(cpa_codes)
)
df_Z = df[mask_Z].copy()
print(f"  Intermediate EU×EU rows: {len(df_Z):,}")

# Create row/col labels
df_Z['row_label'] = df_Z['c_orig'] + '_' + df_Z['prd_ava']
df_Z['col_label'] = df_Z['c_dest'] + '_' + df_Z['prd_use']

# Build ordered index: countries outer, CPA inner
index_labels = [f"{c}_{p}" for c in EU28 for p in cpa_codes]
print(f"  Expected index size: {len(index_labels)}")

# Pivot to matrix
Z_pivot = df_Z.pivot_table(
    index='row_label', columns='col_label',
    values='OBS_VALUE', aggfunc='sum', fill_value=0
)
print(f"  Pivot shape before reindex: {Z_pivot.shape}")

# Reindex to ensure exact 1792×1792 with correct ordering
Z_EU = Z_pivot.reindex(index=index_labels, columns=index_labels, fill_value=0)
print(f"  Z_EU shape: {Z_EU.shape}")
assert Z_EU.shape == (1792, 1792), f"Z_EU shape mismatch: {Z_EU.shape}"
print("  ✓ Z_EU is 1792×1792")
