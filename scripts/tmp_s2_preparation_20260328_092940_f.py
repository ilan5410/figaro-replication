
import pandas as pd
import numpy as np

# Investigate negative values in export data
ICIOT_PATH = "/Users/ilanpargamin/Dossiers importants non sync/figaro_replication/data/raw/figaro_iciot_2010.csv"
EU28 = ['AT','BE','BG','CY','CZ','DE','DK','EE','EL','ES','FI','FR',
        'HR','HU','IE','IT','LT','LU','LV','MT','NL','PL','PT','RO',
        'SE','SI','SK','UK']
eu_set = set(EU28)

df = pd.read_csv(ICIOT_PATH)
cpa_codes = sorted([x for x in df['prd_ava'].unique() if str(x).startswith('CPA_')])

mask_export = (
    df['c_orig'].isin(eu_set) &
    ~df['c_dest'].isin(eu_set) &
    df['prd_ava'].isin(cpa_codes)
)
df_exp = df[mask_export].copy()
df_exp['row_label'] = df_exp['c_orig'] + '_' + df_exp['prd_ava']
e_series = df_exp.groupby('row_label')['OBS_VALUE'].sum()

neg = e_series[e_series < 0]
print(f"Negative export aggregates ({len(neg)}):")
print(neg.sort_values())

# Inspect the raw rows driving negative aggregates
print("\nRaw negative OBS_VALUE rows in export data:")
neg_raw = df_exp[df_exp['OBS_VALUE'] < 0].copy()
print(f"  Count: {len(neg_raw)}")
print(neg_raw[['c_orig','c_dest','prd_ava','prd_use','OBS_VALUE']].sort_values('OBS_VALUE').head(30))
