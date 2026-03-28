
import pandas as pd

# Verify NACE codes vs CPA codes alignment
iciot_path = "/Users/ilanpargamin/Dossiers importants non sync/figaro_replication/data/raw/figaro_iciot_2010.csv"
df = pd.read_csv(iciot_path, usecols=['prd_ava'])
cpa_codes = sorted([x for x in df['prd_ava'].unique() if str(x).startswith('CPA_')])
print(f"CPA codes ({len(cpa_codes)}):")
for c in cpa_codes:
    print(f"  {c}")

# Show NACE codes for comparison
nace_codes = ['A01','A02','A03','B','C10-C12','C13-C15','C16','C17','C18','C19','C20',
              'C21','C22','C23','C24','C25','C26','C27','C28','C29','C30','C31_C32','C33',
              'D35','E36','E37-E39','F','G45','G46','G47','H49','H50','H51','H52','H53',
              'I','J58','J59_J60','J61','J62_J63','K64','K65','K66','L68','M69_M70',
              'M71','M72','M73','M74_M75','N77','N78','N79','N80-N82','O84','P85',
              'Q86','Q87_Q88','R90-R92','R93','S94','S95','S96','T','U']
print(f"\nNACE codes ({len(nace_codes)}):")
for n in nace_codes:
    cpa_equiv = 'CPA_' + n.replace('-C','-').replace('C','')
    # show side by side
    pass

# Build the mapping: strip CPA_ prefix and compare
cpa_suffixes = [c.replace('CPA_', '') for c in cpa_codes]
print(f"\nCPA suffixes: {cpa_suffixes}")
print(f"NACE codes:  {nace_codes}")
print(f"\nPairs:")
for cpa_s, nace in zip(cpa_suffixes, nace_codes):
    match = "✓" if cpa_s == nace else "✗"
    print(f"  {match} CPA_{cpa_s} <-> {nace}")
