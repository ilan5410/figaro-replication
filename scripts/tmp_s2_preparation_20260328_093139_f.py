
import pandas as pd
import numpy as np
import os, json

# ─── Paths & Constants (redefine for this script) ────────────────────────────
ICIOT_PATH = "/Users/ilanpargamin/Dossiers importants non sync/figaro_replication/data/raw/figaro_iciot_2010.csv"
EMP_PATH   = "/Users/ilanpargamin/Dossiers importants non sync/figaro_replication/data/raw/employment_2010.csv"
OUT_DIR    = "data/prepared"
YEAR = 2010
EU28 = ['AT','BE','BG','CY','CZ','DE','DK','EE','EL','ES','FI','FR',
        'HR','HU','IE','IT','LT','LU','LV','MT','NL','PL','PT','RO',
        'SE','SI','SK','UK']
eu_set = set(EU28)

df = pd.read_csv(ICIOT_PATH)
cpa_codes = sorted([x for x in df['prd_ava'].unique() if str(x).startswith('CPA_')])
index_labels = [f"{c}_{p}" for c in EU28 for p in cpa_codes]

print("=" * 60)
print("STEP 5: Building f_intraEU_final (1792×28)")
print("=" * 60)
# Final demand codes (prd_use starts with P)
final_codes = [x for x in df['prd_use'].unique() if str(x).startswith('P')]
print(f"  Final demand prd_use codes: {sorted(final_codes)}")

# Intra-EU final demand: c_orig EU, c_dest EU, prd_ava CPA, prd_use P*
mask_f = (
    df['c_orig'].isin(eu_set) &
    df['c_dest'].isin(eu_set) &
    df['prd_ava'].isin(cpa_codes) &
    df['prd_use'].isin(final_codes)
)
df_f = df[mask_f].copy()
df_f['row_label'] = df_f['c_orig'] + '_' + df_f['prd_ava']

# Sum over all final demand categories per destination country
df_f_agg = df_f.groupby(['row_label', 'c_dest'])['OBS_VALUE'].sum().unstack(fill_value=0)

# Reindex rows and columns
f_intraEU = df_f_agg.reindex(index=index_labels, columns=EU28, fill_value=0)
f_intraEU.index.name = 'country_product'
print(f"  f_intraEU shape: {f_intraEU.shape}")
print(f"  f_intraEU sum: {f_intraEU.values.sum():,.1f} MIO_EUR")
print(f"  Columns (destination countries): {f_intraEU.columns.tolist()}")

print("\n" + "=" * 60)
print("STEP 6: Employment mapping (NACE → CPA)")
print("=" * 60)

df_emp = pd.read_csv(EMP_PATH)
# Filter to EU28, EMP_DC, THS_PER, 2010
df_emp = df_emp[
    (df_emp['geo'].isin(eu_set)) &
    (df_emp['na_item'] == 'EMP_DC') &
    (df_emp['unit'] == 'THS_PER') &
    (df_emp['time'] == YEAR)
].copy()
print(f"  Filtered employment rows: {len(df_emp)}")

# NACE → CPA mapping (explicit, handling all mismatches)
# NACE uses 'C' prefix in compound codes: C10-C12, C13-C15, C31_C32, etc.
# CPA strips that: C10-12, C13-15, C31_32, etc.
nace_to_cpa = {}
for cpa in cpa_codes:
    suffix = cpa.replace('CPA_', '')  # e.g. 'C10-12'
    # Possible NACE forms of the same code
    nace_candidates = [
        suffix,                                    # exact match (A01, B, etc.)
        suffix.replace('-', '-C'),                 # C10-12 → C10-C12
        suffix.replace('_', '_C'),                 # C31_32 → C31_C32
        suffix.replace('-', '-C').replace('_','_C'),  # both replacements
    ]
    # Also handle L (CPA) → L68 (NACE)
    nace_candidates.append('L68' if suffix == 'L' else None)
    # M69_70 → M69_M70
    nace_candidates.append(suffix.replace('_', '_M', 1) if suffix.startswith('M') else None)
    # N80-82 → N80-N82, R90-92 → R90-R92
    nace_candidates.append(suffix.replace('-', '-N', 1) if suffix.startswith('N') else None)
    nace_candidates.append(suffix.replace('-', '-R', 1) if suffix.startswith('R') else None)
    # Q87_88 → Q87_Q88
    nace_candidates.append(suffix.replace('_', '_Q', 1) if suffix.startswith('Q') else None)
    # J59_60 → J59_J60; J62_63 → J62_J63
    nace_candidates.append(suffix.replace('_', '_J', 1) if suffix.startswith('J') else None)
    # M74_75 → M74_M75
    nace_candidates.append(suffix.replace('_', '_M', 1) if suffix.startswith('M') else None)
    
    nace_candidates = [n for n in nace_candidates if n is not None]
    
    nace_actual = df_emp['nace_r2'].unique()
    matched = [n for n in nace_candidates if n in nace_actual]
    if matched:
        nace_to_cpa[matched[0]] = cpa
    else:
        print(f"  WARNING: No NACE match for CPA={cpa}, tried: {nace_candidates}")

print(f"\n  NACE→CPA mapping ({len(nace_to_cpa)} pairs):")
for n, c in nace_to_cpa.items():
    print(f"    {n:20s} → {c}")
