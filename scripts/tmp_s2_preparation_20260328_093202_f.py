
import pandas as pd

# Find the NACE code for E37-39 in the employment data
EMP_PATH = "/Users/ilanpargamin/Dossiers importants non sync/figaro_replication/data/raw/employment_2010.csv"
df_emp = pd.read_csv(EMP_PATH)
e_codes = sorted(df_emp['nace_r2'].unique())
# Look for anything E37-ish
e37_candidates = [c for c in e_codes if 'E37' in c or '37' in c]
print(f"E37-related NACE codes in data: {e37_candidates}")
print(f"\nAll NACE codes containing E: {[c for c in e_codes if c.startswith('E')]}")
