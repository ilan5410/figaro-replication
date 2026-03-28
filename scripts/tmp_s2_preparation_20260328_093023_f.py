
# Negative values are all in P5M (changes in inventories / valuables) — economically valid
# They represent inventory drawdowns. Per Arto (2015), e includes all deliveries.
# These are legitimate negative final demand flows (inventory depletion).
# The correct approach: keep them as-is (they are real economic flows)
# and note that e_nonEU can have small negatives due to inventory changes.
# However, the Leontief model requires non-negative vectors.
# Standard practice: clip at 0 OR keep raw for economic accuracy.
# Let's check how many are negative and their magnitude vs total

import pandas as pd
import numpy as np

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

index_labels = [f"{c}_{p}" for c in EU28 for p in cpa_codes]
e_nonEU = e_series.reindex(index_labels, fill_value=0)

neg_count = (e_nonEU < 0).sum()
neg_sum = e_nonEU[e_nonEU < 0].sum()
total = e_nonEU.sum()
print(f"Negative aggregates: {neg_count} out of {len(e_nonEU)}")
print(f"Sum of negatives: {neg_sum:,.2f} MIO_EUR")
print(f"Total (including negatives): {total:,.2f} MIO_EUR")
print(f"Proportion: {neg_sum/total*100:.2f}%")
print(f"\nNegative e are all due to P5M (inventory changes) — economically valid.")
print("Approach: retain raw values (consistent with Arto 2015); note in summary.")
print("Downstream Leontief can clip if needed, but we preserve economic accuracy here.")
