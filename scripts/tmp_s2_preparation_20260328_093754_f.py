
import pandas as pd
import numpy as np
import os

OUT_DIR = "data/prepared"

# ── Final verification suite ──────────────────────────────────────────────────
print("=" * 65)
print("FINAL VERIFICATION CHECKS")
print("=" * 65)

# Load all outputs
Z_EU    = pd.read_csv(os.path.join(OUT_DIR, 'Z_EU.csv'), index_col=0)
e_df    = pd.read_csv(os.path.join(OUT_DIR, 'e_nonEU.csv'), index_col=0)
x_df    = pd.read_csv(os.path.join(OUT_DIR, 'x_EU.csv'), index_col=0)
em_df   = pd.read_csv(os.path.join(OUT_DIR, 'Em_EU.csv'), index_col=0)
f_df    = pd.read_csv(os.path.join(OUT_DIR, 'f_intraEU_final.csv'), index_col=0)

# ── Check 1: Dimensions ───────────────────────────────────────────────────────
print("\n[1] DIMENSIONS")
print(f"  Z_EU shape:              {Z_EU.shape}  {'✓' if Z_EU.shape == (1792,1792) else '✗ FAIL'}")
print(f"  e_nonEU rows:            {len(e_df)}  {'✓' if len(e_df)==1792 else '✗ FAIL'}")
print(f"  x_EU rows:               {len(x_df)}  {'✓' if len(x_df)==1792 else '✗ FAIL'}")
print(f"  Em_EU rows:              {len(em_df)}  {'✓' if len(em_df)==1792 else '✗ FAIL'}")
print(f"  f_intraEU_final shape:   {f_df.shape}  {'✓' if f_df.shape==(1792,28) else '✗ FAIL'}")

# ── Check 2: Non-negativity ────────────────────────────────────────────────────
print("\n[2] NON-NEGATIVITY")
z_neg = (Z_EU.values < 0).sum()
e_neg = (e_df['e_nonEU_MIO_EUR'] < 0).sum()
x_neg = (x_df['x_EU_MIO_EUR'] < 0).sum()
em_neg = (em_df['em_EU_THS_PER'] < 0).sum()
print(f"  Z_EU negatives:    {z_neg}  {'✓' if z_neg==0 else '✗'}")
print(f"  e_nonEU negatives: {e_neg}  (P5M inventory drawdowns — economically valid)")
print(f"  x_EU negatives:    {x_neg}  {'✓' if x_neg==0 else '✗'}")
print(f"  Em_EU negatives:   {em_neg}  {'✓' if em_neg==0 else '✗'}")

# ── Check 3: Employment total ──────────────────────────────────────────────────
print("\n[3] EMPLOYMENT TOTAL")
em_total = em_df['em_EU_THS_PER'].sum()
expected = 225677
dev = abs(em_total - expected) / expected * 100
print(f"  Total: {em_total:,.1f} THS_PER")
print(f"  Expected: ~{expected:,} THS_PER")
print(f"  Deviation: {dev:.2f}%  {'✓ within 5%' if dev <= 5.0 else '✗ EXCEEDS 5%'}")

# ── Check 4: Row/column ordering consistency ───────────────────────────────────
print("\n[4] ORDERING CONSISTENCY")
z_first3 = Z_EU.index[:3].tolist()
e_first3 = e_df.index[:3].tolist()
x_first3 = x_df.index[:3].tolist()
em_first3 = em_df.index[:3].tolist()
z_last3  = Z_EU.index[-3:].tolist()
em_last3 = em_df.index[-3:].tolist()

print(f"  Z_EU first 3 rows:  {z_first3}")
print(f"  e_nonEU first 3:    {e_first3}")
print(f"  x_EU first 3:       {x_first3}")
print(f"  Em_EU first 3:      {em_first3}")
print(f"  Z_EU last 3 rows:   {z_last3}")
print(f"  Em_EU last 3:       {em_last3}")
all_match = (z_first3 == e_first3 == x_first3 == em_first3) and (z_last3 == em_last3)
print(f"  All ordering consistent: {'✓' if all_match else '✗ MISMATCH'}")

# ── Check 5: Z_EU column ordering ─────────────────────────────────────────────
print("\n[5] Z_EU ROW vs COLUMN ORDERING")
col_match = Z_EU.index.tolist() == Z_EU.columns.tolist()
print(f"  Row index == Column index: {'✓' if col_match else '✗ MISMATCH'}")

# ── Check 6: Key aggregates ────────────────────────────────────────────────────
print("\n[6] KEY AGGREGATES")
print(f"  Z_EU total:              {Z_EU.values.sum():>15,.0f} MIO_EUR")
print(f"  e_nonEU total:           {e_df['e_nonEU_MIO_EUR'].sum():>15,.0f} MIO_EUR")
print(f"  x_EU total:              {x_df['x_EU_MIO_EUR'].sum():>15,.0f} MIO_EUR")
print(f"  f_intraEU total:         {f_df.values.sum():>15,.0f} MIO_EUR")

# Sanity: x ≈ Z.sum(axis=1) + f.sum(axis=1) + e
Z_rowsum = pd.Series(Z_EU.values.sum(axis=1), index=Z_EU.index)
f_rowsum = pd.Series(f_df.values.sum(axis=1), index=f_df.index)
e_vec    = e_df['e_nonEU_MIO_EUR']
x_vec    = x_df['x_EU_MIO_EUR']
x_check  = Z_rowsum + f_rowsum + e_vec
residual = (x_vec - x_check)
print(f"\n  Output balance check (x = Z_row + f_row + e):")
print(f"    Max absolute residual: {residual.abs().max():.4f} MIO_EUR")
print(f"    Mean absolute residual: {residual.abs().mean():.4f} MIO_EUR")
print(f"    {'✓ Balanced' if residual.abs().max() < 1.0 else '⚠ Check residuals'}")

print("\n" + "=" * 65)
print("ALL CHECKS COMPLETE")
print("=" * 65)
