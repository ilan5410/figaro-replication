
import pandas as pd

# Inspect IC-IOT: unique values
iciot_path = "/Users/ilanpargamin/Dossiers importants non sync/figaro_replication/data/raw/figaro_iciot_2010.csv"
df = pd.read_csv(iciot_path)
print(f"Total rows: {len(df):,}")
print(f"\nColumns: {df.columns.tolist()}")
print(f"\nUnique c_orig: {sorted(df['c_orig'].unique())}")
print(f"\nUnique c_dest: {sorted(df['c_dest'].unique())}")
print(f"\nSample prd_ava: {sorted(df['prd_ava'].unique())[:20]}")
print(f"...(total {df['prd_ava'].nunique()})")
print(f"\nSample prd_use: {sorted(df['prd_use'].unique())[:20]}")
print(f"...(total {df['prd_use'].nunique()})")
print(f"\nAll prd_use final demand codes (starting with P): {sorted([x for x in df['prd_use'].unique() if str(x).startswith('P')])}")
print(f"\nAll prd_ava VA codes (not starting with CPA): {sorted([x for x in df['prd_ava'].unique() if not str(x).startswith('CPA')])}")
print(f"\nUnit values: {df['unit'].unique()}")
print(f"Time values: {df['time'].unique()}")
