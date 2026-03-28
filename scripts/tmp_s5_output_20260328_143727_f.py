
import matplotlib
matplotlib.use('Agg')

import os
import json
import warnings
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from matplotlib.patches import Patch

# ── 0. Create output directories ────────────────────────────────────────────
os.makedirs("outputs/figures", exist_ok=True)
os.makedirs("outputs/tables",  exist_ok=True)

warnings_list = []

# ── 1. Load all input files ──────────────────────────────────────────────────
with open("data/prepared/metadata.json") as f:
    meta = json.load(f)
eu_countries = meta["eu_countries"]          # list of 28 ISO-2 codes, ordered

Em_EU      = pd.read_csv("data/prepared/Em_EU.csv")
e_nonEU    = pd.read_csv("data/prepared/e_nonEU.csv")
em_exports = pd.read_csv("data/model/em_exports_total.csv")
em_matrix  = pd.read_csv("data/model/em_exports_country_matrix.csv", index_col=0)
country_dec= pd.read_csv("data/decomposition/country_decomposition.csv")
annex_c    = pd.read_csv("data/decomposition/annex_c_matrix.csv", index_col=0)
ind_table4 = pd.read_csv("data/decomposition/industry_table4.csv", index_col=0)
ind_fig3   = pd.read_csv("data/decomposition/industry_figure3.csv")

print("All files loaded successfully.")
print(f"  Em_EU shape:       {Em_EU.shape}")
print(f"  e_nonEU shape:     {e_nonEU.shape}")
print(f"  em_exports shape:  {em_exports.shape}")
print(f"  em_matrix shape:   {em_matrix.shape}")
print(f"  country_dec shape: {country_dec.shape}")
print(f"  annex_c shape:     {annex_c.shape}")
print(f"  ind_table4 shape:  {ind_table4.shape}")
print(f"  ind_fig3 shape:    {ind_fig3.shape}")
print(f"\nEU countries ({len(eu_countries)}): {eu_countries}")
print(f"\ncountry_dec columns: {list(country_dec.columns)}")
print(f"ind_fig3 columns: {list(ind_fig3.columns)}")
print(f"ind_table4 index: {list(ind_table4.index)}")
print(f"ind_table4 columns: {list(ind_table4.columns)}")
