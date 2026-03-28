
# Employment total is 220,212 vs expected 225,677 — deviation of ~2.4% (within 5% threshold)
# Missing data for LU, MT, and a few other countries in specific industries is expected
# Let's verify the deviation and proceed

em_total = 220212.2
expected = 225677.0
deviation_pct = abs(em_total - expected) / expected * 100
print(f"Employment total: {em_total:,.1f} THS_PER")
print(f"Expected:         {expected:,.1f} THS_PER")
print(f"Deviation:        {deviation_pct:.2f}%")
print(f"Within 5% threshold: {'YES ✓' if deviation_pct <= 5.0 else 'NO ✗'}")
