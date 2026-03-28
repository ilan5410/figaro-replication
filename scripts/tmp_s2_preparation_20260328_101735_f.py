
import pandas as pd
import numpy as np

# ── STEP 6: Employment ────────────────────────────────────────────────────────
print("=" * 65)
print("STEP 6 — Parsing employment data")
print("=" * 65)

RAW_EMP = "data/raw/employment_2010.csv"
YEAR    = 2010
EU28 = ['AT','BE','BG','CY','CZ','DE','DK','EE','EL','ES',
        'FI','FR','HR','HU','IE','IT','LT','LU','LV','MT',
        'NL','PL','PT','RO','SE','SI','SK','UK']

emp_raw = pd.read_csv(RAW_EMP, dtype={'nace_r2': str, 'geo': str,
                                       'na_item': str, 'unit': str})
emp = emp_raw[
    (emp_raw['na_item'] == 'EMP_DC') &
    (emp_raw['unit']    == 'THS_PER') &
    (emp_raw['time']    == YEAR)
].copy()
print(f"  Rows after EMP_DC/THS_PER/2010 filter: {len(emp)}")
print(f"  Unique geo (countries): {sorted(emp['geo'].unique())}")
print(f"  Unique nace_r2 codes: {sorted(emp['nace_r2'].unique())}")
