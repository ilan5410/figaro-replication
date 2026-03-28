
import pandas as pd

# Inspect IC-IOT file
iciot_path = "/Users/ilanpargamin/Dossiers importants non sync/figaro_replication/data/raw/figaro_iciot_2010.csv"
df = pd.read_csv(iciot_path, nrows=20)
print("=== IC-IOT columns ===")
print(df.columns.tolist())
print("\n=== First 20 rows ===")
print(df.to_string())
