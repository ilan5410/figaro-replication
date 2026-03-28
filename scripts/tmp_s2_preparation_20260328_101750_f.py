
import pandas as pd
import numpy as np

# The 64 CPA codes (from Step 1)
cpa_codes = ['CPA_A01','CPA_A02','CPA_A03','CPA_B','CPA_C10-12','CPA_C13-15',
 'CPA_C16','CPA_C17','CPA_C18','CPA_C19','CPA_C20','CPA_C21','CPA_C22',
 'CPA_C23','CPA_C24','CPA_C25','CPA_C26','CPA_C27','CPA_C28','CPA_C29',
 'CPA_C30','CPA_C31_32','CPA_C33','CPA_D35','CPA_E36','CPA_E37-39','CPA_F',
 'CPA_G45','CPA_G46','CPA_G47','CPA_H49','CPA_H50','CPA_H51','CPA_H52',
 'CPA_H53','CPA_I','CPA_J58','CPA_J59_60','CPA_J61','CPA_J62_63','CPA_K64',
 'CPA_K65','CPA_K66','CPA_L68','CPA_M69_70','CPA_M71','CPA_M72','CPA_M73',
 'CPA_M74_75','CPA_N77','CPA_N78','CPA_N79','CPA_N80-82','CPA_O84','CPA_P85',
 'CPA_Q86','CPA_Q87_88','CPA_R90-92','CPA_R93','CPA_S94','CPA_S95','CPA_S96',
 'CPA_T','CPA_U']

# The 64 NACE codes (from Step 6)
nace_codes = ['A01','A02','A03','B','C10-C12','C13-C15','C16','C17','C18',
 'C19','C20','C21','C22','C23','C24','C25','C26','C27','C28','C29','C30',
 'C31_C32','C33','D35','E36','E37-E39','F','G45','G46','G47','H49','H50',
 'H51','H52','H53','I','J58','J59_J60','J61','J62_J63','K64','K65','K66',
 'L68','M69_M70','M71','M72','M73','M74_M75','N77','N78','N79','N80-N82',
 'O84','P85','Q86','Q87_Q88','R90-R92','R93','S94','S95','S96','T','U']

print(f"Number of CPA codes: {len(cpa_codes)}")
print(f"Number of NACE codes: {len(nace_codes)}")

# Build NACE→CPA mapping by stripping "CPA_" and normalising separators
# CPA uses _ as separator (e.g. CPA_C31_32, CPA_J59_60)
# NACE uses _ or - or C prefix differently

def nace_to_cpa_key(nace: str) -> str:
    """Normalise NACE code to match CPA suffix after stripping 'CPA_'."""
    # CPA suffix examples: A01, C10-12, C31_32, J59_60, E37-39, N80-82
    # NACE examples:       A01, C10-C12, C31_C32, J59_J60, E37-E39, N80-N82
    # Rule: remove repeated letter prefix inside the range part
    import re
    # Remove embedded letter (e.g. C10-C12 -> C10-12, J59_J60 -> J59_60)
    key = re.sub(r'([_-])([A-Z]+)(\d)', r'\1\3', nace)
    return key

nace_to_cpa = {}
for nace in nace_codes:
    key = nace_to_cpa_key(nace)
    cpa_candidate = 'CPA_' + key
    if cpa_candidate in cpa_codes:
        nace_to_cpa[nace] = cpa_candidate
    else:
        print(f"  UNMAPPED: NACE={nace}  key={key}  candidate={cpa_candidate}")

print(f"\nMapped {len(nace_to_cpa)}/{len(nace_codes)} NACE codes to CPA")
# Show first few
for k,v in list(nace_to_cpa.items())[:5]:
    print(f"  {k} → {v}")
