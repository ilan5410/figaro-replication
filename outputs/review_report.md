# FIGARO Employment Content Replication — Review Report

**Overall Assessment: SUCCESSFUL REPLICATION**

- PASS: 22/25
- WARN: 3/25
- FAIL: 0/25

---

## 7.1 Data Integrity

✅ **7.1.1 EU-28 total employment ~225,677 thousand** — PASS
     Actual: 220212, Expected: ~225677, Deviation: 2.4%

✅ **7.1.2 No negative values in Z, x, em** — PASS
     Z_EU: no negatives ✓
     x_EU: no negatives ✓
     em_EU: no negatives ✓

✅ **7.1.3 Dimensions: Z (1792×1792), e (1792,), d (1792,)** — PASS
     Z_EU: (1792, 1792) ✓
     A: (1792, 1792) ✓
     L: (1792, 1792) ✓
     e_nonEU: (1792,) ✓
     d: (1792,) ✓
     em_EU: (1792,) ✓

✅ **7.1.4 Country matrix dimensions (28×28)** — PASS
     em_mat: (28, 28) ✓

## 7.2 Leontief Model

✅ **7.2.1 A column sums in [0, 1)** — PASS
     Max column sum: 0.841320, columns >= 1: 0, negative cols: 0

✅ **7.2.2 L non-negative and diagonal >= 1** — PASS
     L min=0.000000, negative elements: 0
     Diagonal: min=1.000000, max=2.257612, count < 1: 0

✅ **7.2.3 Identity check: L*(I-A) ≈ I (tolerance 1e-6)** — PASS
     Max |L*(I-A) - I| = 1.18e-14

✅ **7.2.4 Employment coefficients d >= 0** — PASS
     d: min=0.000000, max=1.145008, negatives: 0

## 7.3 Accounting Identities

✅ **7.3.1 Total export-supported employment ≈ 25,597 thousand** — PASS
     Actual: 24946, Expected: ~25597, Deviation: 2.5%

✅ **7.3.2 Domestic + spillover_received = total employment in country** — PASS
     Max error: 0.0000
     All countries pass ✓

✅ **7.3.3 Domestic + spillover_generated = total employment by country** — PASS
     Max error: 0.0000
     All countries pass ✓

✅ **7.3.4 Direct + indirect = domestic effect** — PASS
     Max error: 0.0000
     All countries pass ✓

✅ **7.3.5 Annex C: row sums = employment in country, col sums = employment by country** — PASS
     Max row sum error: 0.0000
     Max col sum error: 0.0000

## 7.4 Cross-checks vs. Paper

✅ **7.4.1 EU-28 total export employment ≈ 25,597 THS** — PASS
     Actual: 24946.1, Expected: ~25597, Deviation: 2.5%

✅ **7.4.2 EU-28 export employment share ≈ 11.3%** — PASS
     Actual: 11.3, Expected: ~11.3, Deviation: 0.2%

✅ **7.4.3 Germany: employment IN DE ≈ 5,700 THS** — PASS
     Actual: 5144.0, Expected: ~5700, Deviation: 9.8%

⚠️ **7.4.4 Germany: employment BY DE exports ≈ 6,056 THS** — WARN
     Actual: 5190.3, Expected: ~6056, Deviation: 14.3%
   WARN: Deviation 14.3% > 10%

⚠️ **7.4.5 Luxembourg export employment share ≈ 25%** — WARN
     Actual: 19.4, Expected: ~25, Deviation: 22.2%
   WARN: Deviation 22.2% > 10%

⚠️ **7.4.6 Luxembourg spillover share ≈ 46.7% [known bias: 29 missing emp cells]** — WARN
     Actual: 60.6, Expected: ~46.7, Deviation: 29.8%
   WARN: Deviation 29.8% > 20%

✅ **7.4.7 Romania spillover share ≈ 4.5%** — PASS
     Actual: 4.7, Expected: ~4.5, Deviation: 4.1%

✅ **7.4.8 Industry B-E total ≈ 9,889 THS** — PASS
     Actual: 8929.4, Expected: ~9889, Deviation: 9.7%

## 7.5 Reasonableness

✅ **7.5.1 No country > 50% export employment share** — PASS
     Max share: 21.0% (IE)

✅ **7.5.2 Direct effects < domestic effects for all countries** — PASS
     All countries: direct < domestic ✓

✅ **7.5.3 Large countries (DE, UK, FR, IT) in top 5 by absolute employment** — PASS
     Top 5 countries: {'ES', 'UK', 'IT', 'DE', 'FR'}
     Large countries in top 5: {'UK', 'IT', 'DE', 'FR'}

✅ **7.5.4 Small open economies (LU, IE) in top 5 by export employment share** — PASS
     Top 5 by share: {'LU', 'BG', 'EE', 'IE', 'LT'}
     Small open economies in top 5: {'IE', 'LU'}

---

## Country Summary

| Country | Total Emp (THS) | Domestic (THS) | Spill Gen (THS) | Share % | Spill% |
|---------|----------------|----------------|-----------------|---------|--------|
| BG | 3604 | 497 | 22 | 16.1% | 4.3% |
| RO | 8725 | 702 | 34 | 9.8% | 4.7% |
| HR | 1670 | 137 | 8 | 9.6% | 5.3% |
| LV | 844 | 109 | 8 | 15.6% | 6.7% |
| LT | 1247 | 177 | 14 | 16.1% | 7.4% |
| PL | 15370 | 1276 | 107 | 10.8% | 7.7% |
| UK | 24563 | 2986 | 258 | 13.8% | 8.0% |
| EL | 4706 | 304 | 33 | 7.2% | 9.8% |
| CY | 406 | 50 | 5 | 13.8% | 9.8% |
| ES | 19606 | 1472 | 162 | 8.7% | 9.9% |
| PT | 4871 | 289 | 47 | 7.3% | 14.0% |
| DE | 41099 | 4390 | 800 | 12.5% | 15.4% |
| EE | 570 | 81 | 15 | 17.6% | 15.8% |
| NL | 8781 | 841 | 165 | 12.0% | 16.4% |
| IT | 24594 | 2027 | 437 | 9.4% | 17.7% |
| SI | 963 | 113 | 24 | 15.3% | 17.7% |
| FR | 26949 | 2189 | 478 | 9.4% | 17.9% |
| CZ | 5057 | 421 | 92 | 12.1% | 17.9% |
| FI | 2491 | 313 | 86 | 14.5% | 21.5% |
| HU | 3926 | 375 | 111 | 12.5% | 22.7% |
| SK | 2170 | 164 | 54 | 11.8% | 24.7% |
| SE | 4296 | 440 | 173 | 11.9% | 28.2% |
| DK | 2790 | 329 | 133 | 13.4% | 28.9% |
| BE | 4497 | 532 | 225 | 15.0% | 29.7% |
| AT | 4098 | 373 | 160 | 11.7% | 30.0% |
| MT | 137 | 13 | 6 | 10.9% | 30.9% |
| IE | 1923 | 355 | 231 | 21.0% | 39.4% |
| LU | 259 | 40 | 62 | 19.4% | 60.6% |

---

## Known Limitations

1. **Product-by-product vs. industry-by-industry**: The paper uses industry-by-industry IC-IOT (not publicly available). This replication uses product-by-product tables. Results differ.

2. **Employment data vintage**: The paper uses a 2019 vintage of `nama_10_a64_e`. Current download may reflect revised figures.

3. **Missing employment data**: Some country-industry cells are suppressed (confidential), notably for Luxembourg (29 missing) and Malta (31 missing). These are set to 0, causing underestimation of employment effects for those countries.

4. **Upward bias**: Employment coefficients don't distinguish exporting vs. non-exporting firms. Exporters tend to be more productive, leading to upward bias (paper footnote 5).

5. **FIGARO data vintage**: The table may have been revised since 2019.

6. **Non-EU countries in Leontief system**: The full FIGARO table has 50 countries. This replication uses only EU-28 for the Leontief inverse (RoW not included as a full matrix block). The export vector e captures flows to all 22 non-EU countries.

*Report generated: 2026-03-25 15:46:16*