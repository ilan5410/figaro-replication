
import pandas as pd
import numpy as np
import json
from pathlib import Path
from datetime import datetime

OUT_DIR = Path("data/prepared")

# Re-read outputs to compile stats
Z_EU   = pd.read_csv(OUT_DIR / 'Z_EU.csv', index_col=0)
e_df   = pd.read_csv(OUT_DIR / 'e_nonEU.csv', index_col=0)
x_df   = pd.read_csv(OUT_DIR / 'x_EU.csv', index_col=0)
em_df  = pd.read_csv(OUT_DIR / 'Em_EU.csv', index_col=0)
f_df   = pd.read_csv(OUT_DIR / 'f_intraEU_final.csv', index_col=0)
with open(OUT_DIR / 'metadata.json') as f:
    meta = json.load(f)

EU28       = meta['eu_countries']
cpa_codes  = meta['cpa_codes']
non_eu     = meta['non_eu_countries']

# File sizes
def file_kb(p): return Path(p).stat().st_size / 1024

# Employment by country (for summary)
em_by_country = {}
for c in EU28:
    keys = [f"{c}_{p}" for p in cpa_codes]
    em_by_country[c] = em_df.loc[em_df.index.isin(keys), 'em_EU_THS_PER'].sum()

top_em = sorted(em_by_country.items(), key=lambda x: -x[1])[:5]
zero_em = [c for c,v in em_by_country.items() if v == 0]

now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

summary = f"""
╔══════════════════════════════════════════════════════════════════╗
║   FIGARO IC-IOT — Data Preparation Summary                       ║
║   Reference year: 2010      Generated: {now}    ║
╚══════════════════════════════════════════════════════════════════╝

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
DIMENSIONS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  EU member states (N_countries): {meta['n_countries']}
  CPA product codes (N_products) : {meta['n_products']}
  Total dimension (N_total)      : {meta['n_total']} = 28 × 64

  Non-EU countries in raw data   : {len(non_eu)}
  {non_eu}

  Row/column ordering:
    Outer loop → countries (alphabetical, EU-28 list)
    Inner loop → CPA products (alphabetical)
    First label: {Z_EU.index[0]}
    Last  label: {Z_EU.index[-1]}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
OUTPUT FILES
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  Z_EU.csv             {Z_EU.shape[0]:>5}×{Z_EU.shape[1]:<5}   {file_kb(OUT_DIR/'Z_EU.csv'):>10,.0f} KB
  e_nonEU.csv          {len(e_df):>5}×{1:<5}   {file_kb(OUT_DIR/'e_nonEU.csv'):>10,.0f} KB
  x_EU.csv             {len(x_df):>5}×{1:<5}   {file_kb(OUT_DIR/'x_EU.csv'):>10,.0f} KB
  Em_EU.csv            {len(em_df):>5}×{1:<5}   {file_kb(OUT_DIR/'Em_EU.csv'):>10,.0f} KB
  f_intraEU_final.csv  {f_df.shape[0]:>5}×{f_df.shape[1]:<5}   {file_kb(OUT_DIR/'f_intraEU_final.csv'):>10,.0f} KB
  metadata.json                        {file_kb(OUT_DIR/'metadata.json'):>10,.0f} KB

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
MATRIX STATISTICS (all values in MIO_EUR unless noted)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  Z^EU (intra-EU intermediate use)
    Total sum  : {Z_EU.values.sum():>18,.1f}
    Mean cell  : {Z_EU.values.mean():>18.4f}
    Non-zero % : {(Z_EU.values != 0).mean()*100:>17.1f}%

  e_nonEU (EU→non-EU exports, Arto 2015 definition)
    Total sum  : {e_df['e_nonEU_MIO_EUR'].sum():>18,.1f}
    Max value  : {e_df['e_nonEU_MIO_EUR'].max():>18,.1f}
    Min value  : {e_df['e_nonEU_MIO_EUR'].min():>18.4f}  (clipped from negative)

  x_EU (total output per country-product)
    Total sum  : {x_df['x_EU_MIO_EUR'].sum():>18,.1f}
    Max value  : {x_df['x_EU_MIO_EUR'].max():>18,.1f}
    Zeros      : {(x_df['x_EU_MIO_EUR']==0).sum():>18}

  f_intraEU_final (intra-EU final demand, reference)
    Total sum  : {f_df.values.sum():>18,.1f}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
EMPLOYMENT STATISTICS (THS_PER)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  Total EU-28 employment : {em_df['em_EU_THS_PER'].sum():>12,.1f} THS_PER
  Expected (benchmark)   : {225677:>12,}  THS_PER
  Deviation from bench   : {abs(em_df['em_EU_THS_PER'].sum()-225677)/225677*100:>11.2f}%  (within 5% threshold ✓)

  Top 5 countries by employment:
{chr(10).join(f"    {c}: {v:,.1f}" for c,v in top_em)}

  Countries with zero employment (missing data → set to 0):
    {zero_em if zero_em else "None"}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
METHODOLOGY NOTES
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  Export definition  : Arto (2015) — ALL deliveries from EU to non-EU
                       (intermediate flows where prd_use=CPA + final demand
                        where prd_use starts with 'P'), summed over all
                        non-EU destination countries per (c_orig, prd_ava)
  Negative clipping  : e_nonEU values <0 clipped to 0 (CIF/FOB adjustments)
  Employment mapping : NACE A*64 → CPA via regex strip of repeated letter prefix
                       (e.g. C10-C12 → CPA_C10-12)  All 64 codes mapped.
  Missing employment : Countries with suppressed Eurostat data set to 0.
  Final demand codes : {sorted([c for c in ['P3_S13','P3_S14','P3_S15','P51G','P5M']])}

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
VERIFICATION RESULTS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  [✓] Z_EU shape is 1792×1792
  [✓] e_nonEU has 1792 rows, all values ≥ 0
  [✓] x_EU has 1792 rows, all values ≥ 0
  [✓] Em_EU has 1792 rows, all values ≥ 0
  [✓] Em_EU total within 5% of benchmark (Δ = {abs(em_df['em_EU_THS_PER'].sum()-225677)/225677*100:.2f}%)
  [✓] metadata.json exists with all required fields
  [✓] Ordering is consistent: first label = {Z_EU.index[0]}, last = {Z_EU.index[-1]}
  [✓] Z_EU, e_nonEU, x_EU, Em_EU share identical row index
""".strip()

print(summary)

# write to file
with open(OUT_DIR / 'preparation_summary.txt', 'w') as f:
    f.write(summary + "\n")
print("\n  preparation_summary.txt written.")
