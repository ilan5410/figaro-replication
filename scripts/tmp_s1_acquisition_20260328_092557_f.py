
import pandas as pd
import os

YEAR = 2010
EU28 = ['AT','BE','BG','CY','CZ','DE','DK','EE','EL','ES',
        'FI','FR','HR','HU','IE','IT','LT','LU','LV','MT',
        'NL','PL','PT','RO','SE','SI','SK','UK']

# ── Load both files ────────────────────────────────────────────────────────────
print("Loading employment CSV …")
emp_df   = pd.read_csv("data/raw/employment_2010.csv")
print(f"  shape: {emp_df.shape}")

print("Loading IC-IOT CSV (this may take a moment) …")
iciot_df = pd.read_csv("data/raw/figaro_iciot_2010.csv")
print(f"  shape: {iciot_df.shape}")

# ── Employment checks ──────────────────────────────────────────────────────────
emp_eu         = emp_df[emp_df["geo"].isin(EU28)]
total_eu_emp   = emp_eu["OBS_VALUE"].sum()
target         = 225_677.0
emp_pct_diff   = abs(total_eu_emp - target) / target * 100
missing_eu_emp = [c for c in EU28 if c not in emp_df["geo"].values]

# ── IC-IOT checks ─────────────────────────────────────────────────────────────
iciot_eu_orig  = iciot_df[iciot_df["c_orig"].isin(EU28)]
iciot_eu_dest  = iciot_df[iciot_df["c_dest"].isin(EU28)]
missing_eu_icio = [c for c in EU28 if c not in iciot_df["c_orig"].values]
iciot_total_mio = iciot_df["OBS_VALUE"].sum()
n_c_orig        = iciot_df["c_orig"].nunique()
n_c_dest        = iciot_df["c_dest"].nunique()
n_prd_ava       = iciot_df["prd_ava"].nunique()
n_prd_use       = iciot_df["prd_use"].nunique()

# File sizes on disk
iciot_size_mb = os.path.getsize("data/raw/figaro_iciot_2010.csv") / 1e6
emp_size_mb   = os.path.getsize("data/raw/employment_2010.csv") / 1e6

print("\n" + "="*60)
print("VERIFICATION SUMMARY")
print("="*60)
print(f"\n[ Employment ]")
print(f"  Total rows           : {len(emp_df):,}")
print(f"  Countries            : {emp_df['geo'].nunique()}")
print(f"  EU-28 employment     : {total_eu_emp:,.1f} thousand persons")
print(f"  Target               : {target:,.0f} thousand persons")
print(f"  Deviation            : {emp_pct_diff:.2f}%  ({'✓ OK' if emp_pct_diff<=5 else '✗ WARN'})")
print(f"  Missing EU countries : {missing_eu_emp if missing_eu_emp else 'None ✓'}")

print(f"\n[ IC-IOT ]")
print(f"  Total rows           : {len(iciot_df):,}")
print(f"  c_orig countries     : {n_c_orig}")
print(f"  c_dest countries     : {n_c_dest}")
print(f"  prd_ava codes        : {n_prd_ava}")
print(f"  prd_use codes        : {n_prd_use}")
print(f"  Total value (M€)     : {iciot_total_mio:,.1f}")
print(f"  Missing EU c_orig    : {missing_eu_icio if missing_eu_icio else 'None ✓'}")
print(f"  File size            : {iciot_size_mb:.1f} MB")

human_needed = bool(missing_eu_emp or missing_eu_icio)
print(f"\nhuman_intervention_needed = {human_needed}")
