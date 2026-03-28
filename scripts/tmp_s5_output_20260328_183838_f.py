
import matplotlib
matplotlib.use('Agg')

import os
import json
import warnings
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.ticker import FuncFormatter
import openpyxl
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils.dataframe import dataframe_to_rows

warnings.filterwarnings('ignore')

# ── palette ──────────────────────────────────────────────────────────────────
PINK       = '#E91E8C'
LIGHT_PINK = '#F9B4D5'
LIME       = '#7CB342'
SOURCE_NOTE = "Source: Eurostat FIGARO, authors' calculations"

# ── directories ──────────────────────────────────────────────────────────────
os.makedirs('outputs/figures', exist_ok=True)
os.makedirs('outputs/tables',  exist_ok=True)

# ── load inputs ──────────────────────────────────────────────────────────────
with open('data/prepared/metadata.json') as f:
    meta = json.load(f)
eu_countries = meta['eu_countries']          # list of 28 ISO-2 codes
cpa_codes    = meta['cpa_codes']             # list of 64 CPA codes

em_eu     = pd.read_csv('data/prepared/Em_EU.csv')
e_nonEU   = pd.read_csv('data/prepared/e_nonEU.csv')
cd        = pd.read_csv('data/decomposition/country_decomposition.csv')
annex_c   = pd.read_csv('data/decomposition/annex_c_matrix.csv', index_col=0)
ind_t4    = pd.read_csv('data/decomposition/industry_table4.csv', index_col=0)
ind_f3    = pd.read_csv('data/decomposition/industry_figure3.csv')
em_mat    = pd.read_csv('data/model/em_exports_country_matrix.csv', index_col=0)

print("=== Input shapes ===")
print(f"em_eu     : {em_eu.shape}")
print(f"e_nonEU   : {e_nonEU.shape}")
print(f"cd        : {cd.shape}  cols={list(cd.columns)}")
print(f"annex_c   : {annex_c.shape}")
print(f"ind_t4    : {ind_t4.shape}")
print(f"ind_f3    : {ind_f3.shape}  cols={list(ind_f3.columns)}")
print(f"em_mat    : {em_mat.shape}")
print(f"eu_countries ({len(eu_countries)}): {eu_countries}")
print(f"\ncd.head():\n{cd.head()}")
print(f"\nind_f3:\n{ind_f3}")
