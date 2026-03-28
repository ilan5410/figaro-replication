
import pandas as pd

# Inspect employment file
emp_path = "/Users/ilanpargamin/Dossiers importants non sync/figaro_replication/data/raw/employment_2010.csv"
df_emp = pd.read_csv(emp_path, nrows=20)
print("=== Employment columns ===")
print(df_emp.columns.tolist())
print("\n=== First 20 rows ===")
print(df_emp.to_string())
