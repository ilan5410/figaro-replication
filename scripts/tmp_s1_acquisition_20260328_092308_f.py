
# Save employment data
emp_df.to_csv("data/raw/employment_2010.csv", index=False)
print(f"Saved data/raw/employment_2010.csv  ({len(emp_df)} rows)")

# Quick verification
eu28 = ['AT','BE','BG','CY','CZ','DE','DK','EE','EL','ES',
        'FI','FR','HR','HU','IE','IT','LT','LU','LV','MT',
        'NL','PL','PT','RO','SE','SI','SK','UK']

eu_emp = emp_df[emp_df['geo'].isin(eu28)]
total_eu_emp = eu_emp['OBS_VALUE'].sum()
print(f"\nEU-28 total employment: {total_eu_emp:,.1f} thousand persons")
target = 225_677
pct_diff = abs(total_eu_emp - target) / target * 100
print(f"Target: {target:,.0f} thousand  |  Deviation: {pct_diff:.2f}%")
print(f"Within 5% threshold: {'✓ YES' if pct_diff <= 5 else '✗ NO'}")

missing_eu = [c for c in eu28 if c not in emp_df['geo'].values]
print(f"\nMissing EU countries: {missing_eu if missing_eu else 'None — all 28 present ✓'}")
