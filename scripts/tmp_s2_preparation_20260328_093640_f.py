
import pandas as pd
import json, os

# ── metadata.json ─────────────────────────────────────────────────────────────
ICIOT_PATH = "/Users/ilanpargamin/Dossiers importants non sync/figaro_replication/data/raw/figaro_iciot_2010.csv"
OUT_DIR    = "data/prepared"

EU28 = ['AT','BE','BG','CY','CZ','DE','DK','EE','EL','ES','FI','FR',
        'HR','HU','IE','IT','LT','LU','LV','MT','NL','PL','PT','RO',
        'SE','SI','SK','UK']

df = pd.read_csv(ICIOT_PATH, usecols=['prd_ava'])
cpa_codes = sorted([x for x in df['prd_ava'].unique() if str(x).startswith('CPA_')])
index_labels = [f"{c}_{p}" for c in EU28 for p in cpa_codes]

nace_to_cpa_map = {
    'A01': 'CPA_A01', 'A02': 'CPA_A02', 'A03': 'CPA_A03', 'B': 'CPA_B',
    'C10-C12': 'CPA_C10-12', 'C13-C15': 'CPA_C13-15',
    'C16': 'CPA_C16', 'C17': 'CPA_C17', 'C18': 'CPA_C18', 'C19': 'CPA_C19',
    'C20': 'CPA_C20', 'C21': 'CPA_C21', 'C22': 'CPA_C22', 'C23': 'CPA_C23',
    'C24': 'CPA_C24', 'C25': 'CPA_C25', 'C26': 'CPA_C26', 'C27': 'CPA_C27',
    'C28': 'CPA_C28', 'C29': 'CPA_C29', 'C30': 'CPA_C30',
    'C31_C32': 'CPA_C31_32', 'C33': 'CPA_C33',
    'D35': 'CPA_D35', 'E36': 'CPA_E36', 'E37-E39': 'CPA_E37-39',
    'F': 'CPA_F', 'G45': 'CPA_G45', 'G46': 'CPA_G46', 'G47': 'CPA_G47',
    'H49': 'CPA_H49', 'H50': 'CPA_H50', 'H51': 'CPA_H51',
    'H52': 'CPA_H52', 'H53': 'CPA_H53', 'I': 'CPA_I',
    'J58': 'CPA_J58', 'J59_J60': 'CPA_J59_60', 'J61': 'CPA_J61',
    'J62_J63': 'CPA_J62_63',
    'K64': 'CPA_K64', 'K65': 'CPA_K65', 'K66': 'CPA_K66',
    'L68': 'CPA_L',
    'M69_M70': 'CPA_M69_70', 'M71': 'CPA_M71', 'M72': 'CPA_M72',
    'M73': 'CPA_M73', 'M74_M75': 'CPA_M74_75',
    'N77': 'CPA_N77', 'N78': 'CPA_N78', 'N79': 'CPA_N79', 'N80-N82': 'CPA_N80-82',
    'O84': 'CPA_O84', 'P85': 'CPA_P85', 'Q86': 'CPA_Q86',
    'Q87_Q88': 'CPA_Q87_88',
    'R90-R92': 'CPA_R90-92', 'R93': 'CPA_R93',
    'S94': 'CPA_S94', 'S95': 'CPA_S95', 'S96': 'CPA_S96',
    'T': 'CPA_T', 'U': 'CPA_U',
}

non_eu_countries = ['AL','AR','AU','BR','CA','CH','CN','ID','IN','JP','KR',
                    'ME','MK','MX','NO','RS','RU','SA','TR','US','WRL_REST','ZA']

metadata = {
    "reference_year": 2010,
    "eu_countries": EU28,
    "cpa_codes": cpa_codes,
    "n_countries": len(EU28),
    "n_products": len(cpa_codes),
    "n_total": len(EU28) * len(cpa_codes),
    "index_labels": index_labels,
    "non_eu_countries": non_eu_countries,
    "nace_to_cpa_mapping": nace_to_cpa_map,
    "export_definition": "arto_2015",
    "export_definition_description": (
        "e[r,i] = sum over all non-EU destinations t of "
        "(Z[r,i][t,*].sum() + f[r,i][t,*].sum()). "
        "Includes both intermediate and final demand deliveries from EU to non-EU. "
        "233/1792 cells are slightly negative due to P5M inventory drawdowns."
    ),
    "missing_employment_note": (
        "77 of 1792 employment cells are set to 0 due to missing/suppressed Eurostat data. "
        "Affected countries: AT (CPA_U), BE (CPA_U), DE (CPA_U), FI (CPA_U), "
        "LT (CPA_C19, CPA_C21), LU (many manufacturing codes), LV (CPA_U), "
        "MT (many codes), SE (several codes), UK (CPA_D35, CPA_L, CPA_O84, CPA_P85). "
        "Total employment: 220,212 THS_PER vs expected 225,677 THS_PER (−2.42%)."
    ),
    "files": {
        "Z_EU.csv": "1792x1792 intermediate use matrix, EU-only (MIO_EUR)",
        "e_nonEU.csv": "1792x1 export vector EU→non-EU (MIO_EUR), Arto 2015 definition",
        "x_EU.csv": "1792x1 total output, all destinations (MIO_EUR)",
        "Em_EU.csv": "1792x1 employment (THS_PER), zeros where suppressed",
        "f_intraEU_final.csv": "1792x28 intra-EU final demand (MIO_EUR), reference only",
    }
}

meta_path = os.path.join(OUT_DIR, 'metadata.json')
with open(meta_path, 'w') as f:
    json.dump(metadata, f, indent=2)

print(f"✓ metadata.json saved ({os.path.getsize(meta_path):,} bytes)")
print(f"  n_countries={metadata['n_countries']}, n_products={metadata['n_products']}, n_total={metadata['n_total']}")
print(f"  First 3 country-products: {index_labels[:3]}")
print(f"  Last 3 country-products:  {index_labels[-3:]}")
