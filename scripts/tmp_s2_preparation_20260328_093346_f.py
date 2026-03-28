
import pandas as pd
import numpy as np

# Investigate employment: which specific rows have missing data per country
ICIOT_PATH = "/Users/ilanpargamin/Dossiers importants non sync/figaro_replication/data/raw/figaro_iciot_2010.csv"
EMP_PATH   = "/Users/ilanpargamin/Dossiers importants non sync/figaro_replication/data/raw/employment_2010.csv"
EU28 = ['AT','BE','BG','CY','CZ','DE','DK','EE','EL','ES','FI','FR',
        'HR','HU','IE','IT','LT','LU','LV','MT','NL','PL','PT','RO',
        'SE','SI','SK','UK']
eu_set = set(EU28)

df = pd.read_csv(ICIOT_PATH, usecols=['prd_ava'])
cpa_codes = sorted([x for x in df['prd_ava'].unique() if str(x).startswith('CPA_')])
index_labels = [f"{c}_{p}" for c in EU28 for p in cpa_codes]

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

df_emp = pd.read_csv(EMP_PATH)
df_emp_eu = df_emp[
    df_emp['geo'].isin(eu_set) &
    (df_emp['na_item'] == 'EMP_DC') &
    (df_emp['unit'] == 'THS_PER') &
    (df_emp['time'] == 2010)
].copy()
df_emp_eu['cpa_code'] = df_emp_eu['nace_r2'].map(nace_to_cpa_map)
df_emp_eu['row_label'] = df_emp_eu['geo'] + '_' + df_emp_eu['cpa_code']
em_present = set(df_emp_eu['row_label'].unique())

missing = [lbl for lbl in index_labels if lbl not in em_present]
print(f"Missing employment rows: {len(missing)}")
for lbl in missing:
    print(f"  {lbl}")
