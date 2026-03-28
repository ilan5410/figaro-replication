
import pandas as pd

emp_path = "/Users/ilanpargamin/Dossiers importants non sync/figaro_replication/data/raw/employment_2010.csv"
df_emp = pd.read_csv(emp_path)
print(f"Total rows: {len(df_emp):,}")
print(f"\nUnique geo (countries): {sorted(df_emp['geo'].unique())}")
print(f"\nUnique nace_r2 codes ({df_emp['nace_r2'].nunique()}): {sorted(df_emp['nace_r2'].unique())}")
print(f"\nUnique na_item: {df_emp['na_item'].unique()}")
print(f"Unique unit: {df_emp['unit'].unique()}")
print(f"Unique time: {df_emp['time'].unique()}")

# Check EU countries coverage
eu28 = ['AT','BE','BG','CY','CZ','DE','DK','EE','EL','ES','FI','FR','HR','HU','IE','IT','LT','LU','LV','MT','NL','PL','PT','RO','SE','SI','SK','UK']
emp_countries = sorted(df_emp['geo'].unique())
print(f"\nEU28 countries present in employment: {[c for c in eu28 if c in emp_countries]}")
print(f"EU28 countries MISSING from employment: {[c for c in eu28 if c not in emp_countries]}")
