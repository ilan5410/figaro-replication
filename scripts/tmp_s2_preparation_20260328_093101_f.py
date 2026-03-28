
import pandas as pd
import numpy as np
import os, json

# ─── Full pipeline ─────────────────────────────────────────────────────────────
ICIOT_PATH = "/Users/ilanpargamin/Dossiers importants non sync/figaro_replication/data/raw/figaro_iciot_2010.csv"
EMP_PATH   = "/Users/ilanpargamin/Dossiers importants non sync/figaro_replication/data/raw/employment_2010.csv"
OUT_DIR    = "data/prepared"
os.makedirs(OUT_DIR, exist_ok=True)

YEAR = 2010
EU28 = ['AT','BE','BG','CY','CZ','DE','DK','EE','EL','ES','FI','FR',
        'HR','HU','IE','IT','LT','LU','LV','MT','NL','PL','PT','RO',
        'SE','SI','SK','UK']
eu_set = set(EU28)

print("=" * 60)
print("STEP 1: Loading IC-IOT")
print("=" * 60)
df = pd.read_csv(ICIOT_PATH)
print(f"  Rows loaded: {len(df):,}")

cpa_codes = sorted([x for x in df['prd_ava'].unique() if str(x).startswith('CPA_')])
all_countries = sorted(df['c_orig'].unique())
non_eu = [c for c in all_countries if c not in eu_set]
print(f"  CPA codes: {len(cpa_codes)}")
print(f"  Non-EU countries ({len(non_eu)}): {non_eu}")

# Master ordered index
index_labels = [f"{c}_{p}" for c in EU28 for p in cpa_codes]
assert len(index_labels) == 1792

print("\n" + "=" * 60)
print("STEP 2: Building Z_EU (1792×1792)")
print("=" * 60)
mask_Z = (
    df['c_orig'].isin(eu_set) &
    df['c_dest'].isin(eu_set) &
    df['prd_ava'].isin(cpa_codes) &
    df['prd_use'].isin(cpa_codes)
)
df_Z = df[mask_Z].copy()
df_Z['row_label'] = df_Z['c_orig'] + '_' + df_Z['prd_ava']
df_Z['col_label'] = df_Z['c_dest'] + '_' + df_Z['prd_use']

Z_pivot = df_Z.pivot_table(
    index='row_label', columns='col_label',
    values='OBS_VALUE', aggfunc='sum', fill_value=0
)
Z_EU = Z_pivot.reindex(index=index_labels, columns=index_labels, fill_value=0)
print(f"  Z_EU shape: {Z_EU.shape} ✓")
print(f"  Z_EU sum: {Z_EU.values.sum():,.1f} MIO_EUR")
print(f"  Negative cells: {(Z_EU.values < 0).sum()}")
print(f"  First 3 labels: {Z_EU.index[:3].tolist()}")

print("\n" + "=" * 60)
print("STEP 3: Building e_nonEU (Arto 2015)")
print("=" * 60)
# ALL deliveries (intermediate CPA + final demand P*) from EU to non-EU
mask_exp = (
    df['c_orig'].isin(eu_set) &
    ~df['c_dest'].isin(eu_set) &
    df['prd_ava'].isin(cpa_codes)  # product rows only (no VA rows)
)
df_exp = df[mask_exp].copy()
df_exp['row_label'] = df_exp['c_orig'] + '_' + df_exp['prd_ava']
e_series = df_exp.groupby('row_label')['OBS_VALUE'].sum()
e_nonEU = e_series.reindex(index_labels, fill_value=0)
e_df = pd.DataFrame({'e_nonEU_MIO_EUR': e_nonEU}, index=index_labels)
e_df.index.name = 'country_product'
print(f"  e_nonEU shape: {e_df.shape}")
print(f"  e_nonEU sum: {e_df['e_nonEU_MIO_EUR'].sum():,.1f} MIO_EUR")
print(f"  Negatives (P5M inventory): {(e_df['e_nonEU_MIO_EUR'] < 0).sum()} rows")
print(f"  Min: {e_df['e_nonEU_MIO_EUR'].min():.3f}, Max: {e_df['e_nonEU_MIO_EUR'].max():.1f}")

print("\n" + "=" * 60)
print("STEP 4: Building x_EU (total output, all destinations)")
print("=" * 60)
# x[r,i] = Z[r,i,:].sum() + f[r,i,:].sum() — all c_dest, all prd_use
mask_x = (
    df['c_orig'].isin(eu_set) &
    df['prd_ava'].isin(cpa_codes)
)
df_x = df[mask_x].copy()
df_x['row_label'] = df_x['c_orig'] + '_' + df_x['prd_ava']
x_series = df_x.groupby('row_label')['OBS_VALUE'].sum()
x_EU = x_series.reindex(index_labels, fill_value=0)
x_df = pd.DataFrame({'x_EU_MIO_EUR': x_EU}, index=index_labels)
x_df.index.name = 'country_product'
print(f"  x_EU shape: {x_df.shape}")
print(f"  x_EU sum: {x_df['x_EU_MIO_EUR'].sum():,.1f} MIO_EUR")
print(f"  Negatives: {(x_df['x_EU_MIO_EUR'] < 0).sum()} rows")
print(f"  Min: {x_df['x_EU_MIO_EUR'].min():.3f}, Max: {x_df['x_EU_MIO_EUR'].max():.1f}")
