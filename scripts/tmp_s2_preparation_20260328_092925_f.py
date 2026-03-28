
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

print("Loading IC-IOT …")
df = pd.read_csv(ICIOT_PATH)
cpa_codes = sorted([x for x in df['prd_ava'].unique() if str(x).startswith('CPA_')])
all_countries = sorted(df['c_orig'].unique())
non_eu = [c for c in all_countries if c not in eu_set]
index_labels = [f"{c}_{p}" for c in EU28 for p in cpa_codes]

# ─── Step 5: Build e_nonEU (Arto 2015) ───────────────────────────────────────
# ALL deliveries from EU to non-EU: intermediate (prd_use is CPA) + final demand (prd_use starts P)
print("\nBuilding e_nonEU (Arto 2015) …")
final_demand_codes = [x for x in df['prd_use'].unique() if str(x).startswith('P')]
print(f"  Final demand codes: {final_demand_codes}")

# Filter: c_orig in EU, c_dest NOT in EU, prd_ava is CPA code
# prd_use can be CPA (intermediate) or P* (final demand) → both included
mask_export = (
    df['c_orig'].isin(eu_set) &
    ~df['c_dest'].isin(eu_set) &
    df['prd_ava'].isin(cpa_codes)  # only product rows (no VA rows)
)
df_exp = df[mask_export].copy()
print(f"  Export rows (EU→non-EU, CPA prd_ava): {len(df_exp):,}")
print(f"  prd_use categories in export rows:")
print(f"    CPA (intermediate): {df_exp['prd_use'].str.startswith('CPA').sum():,}")
print(f"    P* (final demand):  {df_exp['prd_use'].str.startswith('P').sum():,}")

df_exp['row_label'] = df_exp['c_orig'] + '_' + df_exp['prd_ava']
e_series = df_exp.groupby('row_label')['OBS_VALUE'].sum()
e_nonEU = e_series.reindex(index_labels, fill_value=0)
print(f"  e_nonEU shape: {e_nonEU.shape}")
print(f"  e_nonEU min: {e_nonEU.min():.4f}, max: {e_nonEU.max():.2f}")
print(f"  e_nonEU total: {e_nonEU.sum():,.1f} MIO_EUR")
assert e_nonEU.shape == (1792,)
assert (e_nonEU >= 0).all(), "Negative export values found!"
print("  ✓ e_nonEU valid")
