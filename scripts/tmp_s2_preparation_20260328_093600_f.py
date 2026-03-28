
import pandas as pd
import numpy as np
import os, json

# ─── FULL PIPELINE: SAVE ALL OUTPUTS ─────────────────────────────────────────
ICIOT_PATH = "/Users/ilanpargamin/Dossiers importants non sync/figaro_replication/data/raw/figaro_iciot_2010.csv"
EMP_PATH   = "/Users/ilanpargamin/Dossiers importants non sync/figaro_replication/data/raw/employment_2010.csv"
OUT_DIR    = "data/prepared"
os.makedirs(OUT_DIR, exist_ok=True)

YEAR = 2010
EU28 = ['AT','BE','BG','CY','CZ','DE','DK','EE','EL','ES','FI','FR',
        'HR','HU','IE','IT','LT','LU','LV','MT','NL','PL','PT','RO',
        'SE','SI','SK','UK']
eu_set = set(EU28)

nace_to_cpa_map = {
    'A01': 'CPA_A01', 'A02': 'CPA_A02', 'A03': 'CPA_A03', 'B': 'CPA_B',
    'C10-C12': 'CPA_C10-12', 'C13-C15': 'CPA_C13-15',
    'C16': 'CPA_C16', 'C17': 'CPA_C17', 'C18': 'CPA_C18', 'C19': 'CPA_C19',
    'C20': 'CPA_C20', 'C21': 'CPA_C21', 'C22': 'CPA_C22', 'C23': 'CPA_C23',
    'C24': 'CPA_C24', 'C25': 'CPA_C25', 'C26': 'CPA_C26', 'C27': 'CPA_C27',
    'C28': 'CPA_C28', 'C29': 'CPA_C29', 'C30': 'CPA_C30',
    'C31_C32': 'CPA_C31_32', 'C33': 'CPA_C33',
    'D35': 'CPA_D35', 'E36': 'CPA_E36', 'E37-E39': 'CPA_E37-39',
    'F': 'CPA_F', 'G45': 'CPA_G45', 'G46': 'CPA_G46', 'G47': 'CPA_G47',
    'H49': 'CPA_H49', 'H50': 'CPA_H50', 'H51': 'CPA_H51',
    'H52': 'CPA_H52', 'H53': 'CPA_H53', 'I': 'CPA_I',
    'J58': 'CPA_J58', 'J59_J60': 'CPA_J59_60', 'J61': 'CPA_J61',
    'J62_J63': 'CPA_J62_63',
    'K64': 'CPA_K64', 'K65': 'CPA_K65', 'K66': 'CPA_K66',
    'L68': 'CPA_L',
    'M69_M70': 'CPA_M69_70', 'M71': 'CPA_M71', 'M72': 'CPA_M72',
    'M73': 'CPA_M73', 'M74_M75': 'CPA_M74_75',
    'N77': 'CPA_N77', 'N78': 'CPA_N78', 'N79': 'CPA_N79', 'N80-N82': 'CPA_N80-82',
    'O84': 'CPA_O84', 'P85': 'CPA_P85', 'Q86': 'CPA_Q86',
    'Q87_Q88': 'CPA_Q87_88',
    'R90-R92': 'CPA_R90-92', 'R93': 'CPA_R93',
    'S94': 'CPA_S94', 'S95': 'CPA_S95', 'S96': 'CPA_S96',
    'T': 'CPA_T', 'U': 'CPA_U',
}

# ── Load IC-IOT ───────────────────────────────────────────────────────────────
print("Loading IC-IOT (this may take a moment) …")
df = pd.read_csv(ICIOT_PATH)
cpa_codes = sorted([x for x in df['prd_ava'].unique() if str(x).startswith('CPA_')])
index_labels = [f"{c}_{p}" for c in EU28 for p in cpa_codes]
non_eu = [c for c in sorted(df['c_orig'].unique()) if c not in eu_set]

# ── 1. Z_EU ───────────────────────────────────────────────────────────────────
print("Building and saving Z_EU …")
mask_Z = (df['c_orig'].isin(eu_set) & df['c_dest'].isin(eu_set) &
          df['prd_ava'].isin(cpa_codes) & df['prd_use'].isin(cpa_codes))
df_Z = df[mask_Z].copy()
df_Z['row_label'] = df_Z['c_orig'] + '_' + df_Z['prd_ava']
df_Z['col_label'] = df_Z['c_dest'] + '_' + df_Z['prd_use']
Z_pivot = df_Z.pivot_table(index='row_label', columns='col_label',
                            values='OBS_VALUE', aggfunc='sum', fill_value=0)
Z_EU = Z_pivot.reindex(index=index_labels, columns=index_labels, fill_value=0)
Z_EU.index.name = 'country_product'
Z_EU.columns.name = 'country_product'
Z_EU.to_csv(os.path.join(OUT_DIR, 'Z_EU.csv'))
print(f"  ✓ Z_EU.csv  shape={Z_EU.shape}  sum={Z_EU.values.sum():,.0f} MIO_EUR")

# ── 2. e_nonEU ────────────────────────────────────────────────────────────────
print("Building and saving e_nonEU …")
mask_exp = (df['c_orig'].isin(eu_set) & ~df['c_dest'].isin(eu_set) &
            df['prd_ava'].isin(cpa_codes))
df_exp = df[mask_exp].copy()
df_exp['row_label'] = df_exp['c_orig'] + '_' + df_exp['prd_ava']
e_series = df_exp.groupby('row_label')['OBS_VALUE'].sum()
e_nonEU = e_series.reindex(index_labels, fill_value=0)
e_df = pd.DataFrame({'e_nonEU_MIO_EUR': e_nonEU}, index=index_labels)
e_df.index.name = 'country_product'
e_df.to_csv(os.path.join(OUT_DIR, 'e_nonEU.csv'))
print(f"  ✓ e_nonEU.csv  rows={len(e_df)}  sum={e_df['e_nonEU_MIO_EUR'].sum():,.0f} MIO_EUR"
      f"  negatives={( e_df['e_nonEU_MIO_EUR'] < 0).sum()} (P5M inventory)")

# ── 3. x_EU ───────────────────────────────────────────────────────────────────
print("Building and saving x_EU …")
mask_x = (df['c_orig'].isin(eu_set) & df['prd_ava'].isin(cpa_codes))
df_x = df[mask_x].copy()
df_x['row_label'] = df_x['c_orig'] + '_' + df_x['prd_ava']
x_series = df_x.groupby('row_label')['OBS_VALUE'].sum()
x_EU_raw = x_series.reindex(index_labels, fill_value=0)
x_df = pd.DataFrame({'x_EU_MIO_EUR': x_EU_raw}, index=index_labels)
x_df.index.name = 'country_product'
x_df.to_csv(os.path.join(OUT_DIR, 'x_EU.csv'))
print(f"  ✓ x_EU.csv  rows={len(x_df)}  sum={x_df['x_EU_MIO_EUR'].sum():,.0f} MIO_EUR"
      f"  negatives={(x_df['x_EU_MIO_EUR'] < 0).sum()}")

# ── 4. f_intraEU_final ────────────────────────────────────────────────────────
print("Building and saving f_intraEU_final …")
final_codes = [x for x in df['prd_use'].unique() if str(x).startswith('P')]
mask_f = (df['c_orig'].isin(eu_set) & df['c_dest'].isin(eu_set) &
          df['prd_ava'].isin(cpa_codes) & df['prd_use'].isin(final_codes))
df_f = df[mask_f].copy()
df_f['row_label'] = df_f['c_orig'] + '_' + df_f['prd_ava']
df_f_agg = df_f.groupby(['row_label','c_dest'])['OBS_VALUE'].sum().unstack(fill_value=0)
f_intraEU = df_f_agg.reindex(index=index_labels, columns=EU28, fill_value=0)
f_intraEU.index.name = 'country_product'
f_intraEU.to_csv(os.path.join(OUT_DIR, 'f_intraEU_final.csv'))
print(f"  ✓ f_intraEU_final.csv  shape={f_intraEU.shape}  sum={f_intraEU.values.sum():,.0f} MIO_EUR")

# ── 5. Em_EU ──────────────────────────────────────────────────────────────────
print("Building and saving Em_EU …")
df_emp = pd.read_csv(EMP_PATH)
df_emp = df_emp[
    df_emp['geo'].isin(eu_set) &
    (df_emp['na_item'] == 'EMP_DC') &
    (df_emp['unit'] == 'THS_PER') &
    (df_emp['time'] == YEAR)
].copy()
df_emp['cpa_code'] = df_emp['nace_r2'].map(nace_to_cpa_map)
df_emp_clean = df_emp.dropna(subset=['cpa_code']).copy()
df_emp_clean['row_label'] = df_emp_clean['geo'] + '_' + df_emp_clean['cpa_code']
em_series = df_emp_clean.groupby('row_label')['OBS_VALUE'].sum()
em_EU = em_series.reindex(index_labels, fill_value=0)
em_df = pd.DataFrame({'em_EU_THS_PER': em_EU}, index=index_labels)
em_df.index.name = 'country_product'
em_df.to_csv(os.path.join(OUT_DIR, 'Em_EU.csv'))
em_total = em_df['em_EU_THS_PER'].sum()
print(f"  ✓ Em_EU.csv  rows={len(em_df)}  total={em_total:,.1f} THS_PER"
      f"  (expected ~225,677, deviation={(abs(em_total-225677)/225677*100):.2f}%)")

print("\nAll data files saved ✓")
