# FIGARO Employment Content of EU Exports — Agentic Replication Flow

## Instructions for Claude Code

You are building an agentic workflow that replicates, from scratch, the analysis in:

> Rémond-Tiedrez, Valderas-Jaramillo, Amores & Rueda-Cantuche (2019), "The employment content of EU exports: an application of FIGARO tables", *EURONA*, Issue 1, pp. 59–78.

The workflow must be **fully automated** end-to-end: it downloads public data, constructs all matrices, computes all results, produces all tables and figures from the paper, and then **reviews its own work for correctness**.

---

## 0. Architecture overview

Build this as a **multi-agent pipeline** with the following sequential stages. Each stage produces artefacts consumed by subsequent stages. Choose the best language/tooling (Python or R) for the task — you may use different languages for different stages.

| Stage | Name | Role |
|-------|------|------|
| 1 | `data_acquisition` | Download FIGARO IC-IOT and employment data from Eurostat |
| 2 | `data_preparation` | Parse, clean, reshape data into analysis-ready matrices |
| 3 | `model_construction` | Build Leontief model, compute employment content |
| 4 | `decomposition` | Decompose into domestic/spillover, direct/indirect effects |
| 5 | `output_generation` | Produce all tables and figures matching the paper |
| 6 | `review_agent` | Independently verify all results for correctness |

Each stage should be a self-contained script/module that:
- Logs what it's doing
- Saves intermediate outputs to disk (CSV/RDS/parquet)
- Can be re-run independently
- Has clear error handling

There should also be a **master orchestrator** script that runs all stages in sequence.

---

## 1. Configuration & Parameters

Create a `config.yaml` (or `config.json`) file with the following configurable parameters. The orchestrator reads this at startup.

```yaml
# === PARAMETERS (human inputs) ===

# Reference year for the analysis
reference_year: 2010

# Which IC-IOT table type to use
# Options: "product-by-product" (publicly available), "industry-by-industry" (if user provides path)
iot_table_type: "product-by-product"

# If industry-by-industry table is available locally, set path here (otherwise null)
iot_local_path: null

# EU member states to include (ISO-2 codes)
# The paper uses EU-28 (pre-Brexit). This list should be the default.
eu_member_states:
  - AT  # Austria
  - BE  # Belgium
  - BG  # Bulgaria
  - CY  # Cyprus
  - CZ  # Czechia
  - DE  # Germany
  - DK  # Denmark
  - EE  # Estonia
  - EL  # Greece
  - ES  # Spain
  - FI  # Finland
  - FR  # France
  - HR  # Croatia
  - HU  # Hungary
  - IE  # Ireland
  - IT  # Italy
  - LT  # Lithuania
  - LU  # Luxembourg
  - LV  # Latvia
  - MT  # Malta
  - NL  # Netherlands
  - PL  # Poland
  - PT  # Portugal
  - RO  # Romania
  - SE  # Sweden
  - SI  # Slovenia
  - SK  # Slovakia
  - UK  # United Kingdom

# Non-EU country(ies) included in the IC-IOT (not analysed, but part of the Leontief system)
non_eu_countries:
  - US  # United States

# Rest of world treatment: "import_vector" (FIGARO default — RoW is not a full matrix, just import/export vectors)
row_treatment: "import_vector"

# === METHODOLOGICAL ASSUMPTIONS (configurable) ===

# Export definition for the employment content calculation.
# The paper uses a specific specification following Arto et al. (2015):
#   - EU exports = final demand deliveries from country r to other EU member states
#                 + ALL exports (intermediate + final) to non-member countries
#   - This means intra-EU intermediate flows stay INSIDE the Leontief inverse (as technical coefficients),
#     while intra-EU final demand deliveries are treated as part of the "export" shock.
#   - ONLY exports to non-member countries (both intermediate and final) are treated as exogenous demand.
#
# This avoids endogeneity: intra-EU intermediate trade is endogenous (captured by the Leontief inverse),
# while exports to non-EU countries are exogenous demand shocks.
#
# Alternative: "total_exports" would treat ALL exports (including intra-EU intermediate) as exogenous.
# The paper explicitly uses "arto_2015" to avoid double-counting.
export_definition: "arto_2015"

# Employment data: persons employed (as in the paper). Alternative could be FTE, hours worked, etc.
employment_measure: "persons_employed"

# Number of industries in the classification
n_industries: 64  # NACE Rev. 2, A*64

# Industry aggregation for Table 4 (10 sectors)
industry_aggregation_10:
  A: [1, 2, 3]          # Agriculture, forestry, fishing (divisions 01-03)
  B-E: [4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27]  # Industry
  F: [28]                # Construction
  G-I: [29, 30, 31, 32, 33, 34, 35, 36]  # Trade, transport, accommodation
  J: [37, 38, 39, 40]   # Information and communication
  K: [41, 42, 43]        # Financial and insurance
  L: [44]                # Real estate
  M-N: [45, 46, 47, 48, 49, 50, 51, 52]  # Professional, admin
  O-Q: [53, 54, 55, 56]  # Public admin, education, health
  R-U: [57, 58, 59, 60, 61, 62, 63, 64]  # Arts, other services, households, extraterritorial
```

**IMPORTANT**: The mapping from industry indices to NACE divisions above is illustrative. You MUST verify the exact mapping against the FIGARO metadata/column headers when you download the data. The FIGARO tables use CPA product codes (for product-by-product) or NACE industry codes (for industry-by-industry). The 64 industries correspond to the list in Annex B of the paper (reproduced below for reference). Map them correctly.

---

## 2. Stage 1: Data Acquisition (`data_acquisition`)

### 2.1 FIGARO IC-IOT

Download the FIGARO inter-country input-output table from Eurostat's experimental statistics.

**Where to find it:**
- Main page: `https://ec.europa.eu/eurostat/web/experimental-statistics/figaro`
- The data is distributed as large CSV or TSV files
- Look for the **product-by-product** IC-IOT for the configured `reference_year`
- The file will be very large (28 EU countries + US + RoW = 30 entities × 64 products = 1,920 rows/columns for intermediate use alone, plus final demand columns)

**Steps:**
1. Programmatically navigate the Eurostat FIGARO page or use the direct download URL pattern
2. Download the IC-IOT file for the reference year
3. If the download URL structure is not immediately obvious, search for it using the Eurostat bulk download facility or the FIGARO data explorer
4. Save raw data to `data/raw/figaro_iciot_{year}.csv`

**If download fails or the URL structure changes**: Stop and ask the human for the direct URL. Do not guess.

### 2.2 Employment data

Download employment by country × industry from Eurostat table `nama_10_a64_e`.

**Where to find it:**
- Eurostat database code: `nama_10_a64_e`
- Use the Eurostat bulk download facility or the SDMX/JSON API
- API endpoint pattern: `https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/nama_10_a64_e/...`
- Filter: `unit=THS_PER` (thousands of persons), `na_item=EMP_DC` (persons employed), `nace_r2=A*64 industries`, `geo=EU member states`, `time={reference_year}`

**Steps:**
1. Download employment data for all 28 EU member states, all 64 NACE Rev. 2 industries, for the reference year
2. The measure should be "persons employed" (code: `EMP_DC`) in thousands
3. Save raw data to `data/raw/employment_{year}.csv`

**Handle missing data**: The paper notes (Section 2.A) that some data may be missing or confidential. Log any missing cells. For the replication, use whatever is available in the public Eurostat release — note any gaps.

### 2.3 Verification checksums

After downloading, produce a summary:
- FIGARO IC-IOT: dimensions (rows × columns), list of countries/products found
- Employment: number of country-industry cells, total EU-28 employment (should be ~225,677 thousand for 2010 per Table 1)
- Log any discrepancies

Save this summary to `data/raw/data_summary_{year}.txt`.

---

## 3. Stage 2: Data Preparation (`data_preparation`)

### 3.1 Parse the FIGARO IC-IOT

The FIGARO IC-IOT file has a specific structure. You need to extract:

1. **Z matrix** (intermediate use): An `(N×C) × (N×C)` matrix where N=64 industries and C=30 countries (28 EU + US + RoW or however many are in the file). Element `Z[r,i][s,j]` = intermediate inputs from industry i in country r to industry j in country s.

2. **Final demand columns** (f): For each country, there are multiple final demand categories (household consumption, government consumption, GFCF, etc.). You need the total final demand `f^{rs}` = sum of all final demand categories for goods from country r consumed in country s.

3. **Total output** (x): Either read from the file or compute as row sums: `x = Z·i + f·i` where i is a summation vector.

4. **Exports to non-member countries** (e): For each EU member state r, extract exports to non-EU countries. In the FIGARO structure, this includes:
   - Final demand from non-EU countries for goods produced in EU country r
   - Intermediate deliveries from EU country r to non-EU countries (if the export_definition is "arto_2015", these are part of the e vector, not the Z matrix used for the Leontief inverse)

**CRITICAL STRUCTURAL POINT (Arto et al. 2015 specification):**

The paper's methodology treats the IC-IOT as follows. Let EU = {countries 1..28}, and non-EU = {US, RoW}:

- **Z^EU**: The intermediate use matrix restricted to EU×EU blocks only (i.e., Z^{rs} where both r and s are EU members). This is what goes into the Leontief inverse.
- **e^{rs}**: Exports from EU member r to non-EU country s. Defined as `e^{rs} = f^{rs} + Z^{rs}·i` (final demand from non-EU country s for goods from EU country r, PLUS intermediate deliveries from EU country r to non-EU country s).
- **f^EU**: Final demand within the EU is defined as `f^r = sum over EU countries s of f^{rs} + e^{r,non-EU}`. But for the Leontief system, only `f^{rs}` for s in EU (plus the export vector e) constitutes final demand.

In practice:
- Build `Z^EU` (1792×1792 for 28 countries × 64 industries)
- Build `e` (1792×1 vector): for each EU country r and industry i, sum all deliveries (intermediate + final) to all non-EU countries
- Compute `x^EU` from the EU sub-system
- The Leontief inverse `L^EU = (I - A^EU)^{-1}` where `A^EU = Z^EU · diag(x^EU)^{-1}`

### 3.2 Parse employment data

Reshape employment data into a vector `Em` of dimension (N×C, 1) = (1792, 1), ordered consistently with the IC-IOT rows/columns.

**CRITICAL**: Ensure the ordering of countries and industries in the employment vector matches EXACTLY the ordering in the IC-IOT. Misalignment here will produce completely wrong results.

### 3.3 Output

Save to `data/prepared/`:
- `Z_EU.csv` or binary format — the EU-only intermediate use matrix
- `e_nonEU.csv` — the export vector to non-member countries
- `x_EU.csv` — total output vector for EU countries
- `Em_EU.csv` — employment vector
- `f_intraEU_final.csv` — intra-EU final demand matrix (for reference)
- `metadata.json` — country order, industry order, dimensions

---

## 4. Stage 3: Model Construction (`model_construction`)

### 4.1 Technical coefficients matrix

```
A^EU = Z^EU · diag(x^EU)^{-1}
```

Where `diag(x^EU)^{-1}` is the inverse of the diagonal matrix of total outputs. Handle any zero outputs carefully (set coefficient to 0 if output is 0).

### 4.2 Leontief inverse

```
L^EU = (I - A^EU)^{-1}
```

This requires inverting a 1792×1792 matrix. Use a robust linear algebra library. Verify:
- L should have all non-negative entries
- Diagonal elements of L should be ≥ 1
- L·(I - A) should equal I (within numerical tolerance)

### 4.3 Employment coefficients

```
d^r = diag(x^r)^{-1} · Em^r
```

For each country r, the employment coefficient vector gives employment per unit of output. Stack these into a combined vector `d^EU`.

### 4.4 Employment content of exports

The core computation:

```
Em_exports^EU = diag(d^EU)' · L^EU · e
```

This gives, for each country-industry pair (r,i), the employment in (r,i) that is supported by EU exports to non-member countries.

But we need more granularity. The full decomposition requires computing:

```
Em_exports[r,s] = d^r' · L^{rs} · e^s
```

Where `L^{rs}` is the (r,s) block of the Leontief inverse L^EU, and `e^s` is the export vector for EU country s. This gives: "employment in country r supported by the exports of country s to non-member countries."

This produces a 28×28 matrix of country-level results (this is Annex C of the paper).

### 4.5 Output

Save to `data/model/`:
- `A_EU.csv` — technical coefficients matrix (or binary)
- `L_EU.csv` — Leontief inverse (or binary — this is large)
- `d_EU.csv` — employment coefficients vector
- `em_exports_total.csv` — total employment content by country-industry
- `em_exports_country_matrix.csv` — 28×28 matrix (Annex C)

---

## 5. Stage 4: Decomposition (`decomposition`)

### 5.1 Domestic vs. Spillover

For each EU member state r:
- **Domestic effect**: Employment in country r supported by country r's own exports = `d^r' · L^{rr} · e^r`
- **Spillover received**: Employment in country r supported by OTHER countries' exports = `sum over s≠r of d^r' · L^{rs} · e^s`
- **Spillover generated**: Employment in OTHER countries supported by country r's exports = `sum over s≠r of d^s' · L^{sr} · e^r`

### 5.2 Direct vs. Indirect (within domestic effects)

For each EU member state r:
- **Direct effect**: Employment in country r's exporting industries directly attributable to exports = `d^r' · diag(e^r)` (i.e., employment coefficient × own exports, without the multiplier effect)
  
  More precisely: the direct effect for industry i in country r = `d_i^r · e_i^r`

- **Indirect effect**: Domestic effect minus direct effect = `d^r' · L^{rr} · e^r - d^r' · e^r`
  
  This captures employment in upstream domestic industries supplying inputs to the exporting industries.

**Note**: The direct effect calculation uses only the diagonal of `L^{rr}` (or equivalently, just `d·e`), while the full domestic effect uses the full `L^{rr}` block. The difference is the indirect domestic multiplier effect.

### 5.3 Compute all shares

For each country r:
- Total employment in r: from the employment data
- Employment supported by exports (domestic + spillover received): from 5.1
- Share of total employment supported by exports: (domestic + spillover received) / total employment
- Domestic share (of total export-supported employment from r's exports): domestic / (domestic + spillover generated)
- Spillover share: spillover generated / (domestic + spillover generated)

### 5.4 Industry-level decomposition

Aggregate the country-industry level results to the 10-sector classification defined in the config, producing the equivalent of Table 4.

Table 4 is a 10×10 matrix where:
- Rows = employment in industry group (which industries' workers are supported)
- Columns = exports by product group (which product exports support them)
- Cell (i,j) = employment in industry i supported by exports of product j to non-member countries

Additionally, for Figure 3, decompose each product's employment effect into domestic and spillover.

### 5.5 Output

Save to `data/decomposition/`:
- `country_decomposition.csv` — columns: country, total_employment, domestic_effect, spillover_received, spillover_generated, direct_effect, indirect_effect, export_employment_share, domestic_share, spillover_share
- `annex_c_matrix.csv` — full 28×28 employment supported matrix
- `industry_table4.csv` — 10×10 industry decomposition
- `industry_figure3.csv` — by-product employment shares with domestic/spillover split

---

## 6. Stage 5: Output Generation (`output_generation`)

Reproduce all tables and figures from the paper. Save outputs to `outputs/`.

### 6.1 Table 1: Employment and exports (balanced view) to non-member countries

Columns: Country, Employment (thousands of persons), Exports to non-member countries (million EUR)

Source: Employment from data_preparation; Exports from the e vector summed by country.

The paper's values should be closely matched if using the same data vintage.

### 6.2 Figure 1: Employment supported by EU exports (thousand persons), bar chart

Two series per country (28 countries):
1. **Pink bars**: Employment IN the member state supported by exports of ALL EU countries to non-member countries (= domestic + spillover received for each country)
2. **Light pink bars**: Total employment in ALL EU member states supported by exports FROM the specified member state (= domestic + spillover generated for each country)

Countries ordered by the light pink bars (descending): Germany, UK, France, Italy, Poland, Spain, ...

### 6.3 Figure 2: Employment supported by exports (% of total employment), bar chart

Stacked bars showing:
- **Domestic** share (% of country's total employment)
- **Spillover received** share (% of country's total employment)
- **Lime marker**: Direct effect share (% of country's total employment)

Countries ordered by total share (descending): Luxembourg, Ireland, Lithuania, Sweden, ...

### 6.4 Table 3: Employment supported by exports, ranked by spillover share

Columns:
1. Total employment in all EU MS supported by exports from the specified MS (thousands)
2. Domestic supported (thousands)
3. Spillover supported (thousands)
4. Domestic share (%)
5. Spillover share (%)

Sorted by spillover share ascending (Romania first with 4.5%, Luxembourg last with 46.7%).

### 6.5 Table 4: Employment supported by exports of each industry (10-sector matrix)

The 10×10 matrix described in section 5.4.

### 6.6 Figure 3: Employment by product (% of total employment), bar chart

10 product groups, stacked bars (domestic + spillover).

### 6.7 Annex C: Full 28×28 country matrix

The complete employment-supported matrix (rows = employment location, columns = exporting country).

### Formatting requirements

- **Charts**: Use matplotlib/ggplot2 with a pink/magenta colour scheme matching the paper's Eurostat style. Save as PNG (300 dpi) and PDF.
- **Tables**: Save as CSV and also produce formatted Excel or HTML versions.
- **All figures and tables should have proper titles, axis labels, and source notes.**

---

## 7. Stage 6: Review Agent (`review_agent`)

**This is the most critical stage.** The review agent independently verifies the entire pipeline. It should be a separate script that loads all intermediate and final outputs and runs the following checks:

### 7.1 Data integrity checks

- [ ] Employment vector sums to EU-28 total (~225,677 thousand for 2010)
- [ ] IC-IOT is balanced: for each country-industry, total output = total input (within tolerance)
- [ ] No negative values in Z, f, x, Em
- [ ] Country and industry labels are consistently ordered across all matrices
- [ ] Dimensions match: Z is square (1792×1792), e has 1792 rows, d has 1792 rows, etc.

### 7.2 Leontief model checks

- [ ] A matrix: all elements in [0, 1), column sums < 1
- [ ] L matrix: all elements ≥ 0, diagonal elements ≥ 1
- [ ] Identity check: `L · (I - A)` ≈ I (tolerance: max absolute error < 1e-6)
- [ ] Employment coefficients d: all ≥ 0, reasonable magnitude (e.g., < 1 for most industries — means less than 1 worker per EUR 1000 of output)

### 7.3 Accounting identities

- [ ] Total export-supported employment across all countries = sum of all elements in the em_exports vector ≈ 25,597 thousand (for 2010)
- [ ] For each country: domestic + spillover received = total employment supported in that country by exports
- [ ] For each country: domestic + spillover generated = total employment supported by that country's exports
- [ ] Direct + indirect = domestic effect (for each country)
- [ ] Annex C matrix: row sums = employment in country supported by all EU exports; column sums = employment across EU supported by that country's exports

### 7.4 Cross-checks against paper values

For the 2010 replication, check key values against the paper:

- [ ] EU-28 total export-supported employment ≈ 25,597 thousand (paper says 25.6 million)
- [ ] EU-28 share ≈ 11.3%
- [ ] Germany: ~5,700 thousand supported in Germany by all EU exports
- [ ] Germany: ~6,056 thousand total supported by German exports
- [ ] Luxembourg export employment share ≈ 25%
- [ ] Spillover share for Luxembourg ≈ 46.7%
- [ ] Spillover share for Romania ≈ 4.5%
- [ ] Industry B-E: ~9,889 thousand jobs supported

**Tolerance**: Given that we use product-by-product (not industry-by-industry) tables, expect some deviation. Flag any deviation > 10% from paper values as a **warning** and > 25% as an **error**.

### 7.5 Reasonableness checks

- [ ] No country has > 50% of employment supported by exports (Luxembourg at ~25% is the max)
- [ ] Direct effects < domestic effects for every country
- [ ] Domestic effects > spillover received for every country except possibly Luxembourg
- [ ] Large countries (DE, UK, FR, IT) have the highest absolute employment supported
- [ ] Small open economies (LU, IE, MT, EE) have the highest export employment shares

### 7.6 Output

Produce `outputs/review_report.md` with:
- PASS/FAIL status for each check
- Actual vs. expected values for cross-checks
- Any warnings or anomalies
- Overall assessment: is the replication successful?
- List of known deviations and likely explanations

---

## 8. Known Limitations to Document

The review report should note these limitations:

1. **Product-by-product vs. industry-by-industry**: The paper uses industry-by-industry IC-IOT (footnote 6). We use product-by-product (publicly available). Results will differ.

2. **Employment data vintage**: The paper may use a specific vintage of nama_10_a64_e that has since been revised. Current downloads may give slightly different values.

3. **Upward bias**: Employment data do not distinguish exporting from non-exporting firms. Since exporters tend to be more productive (fewer workers per unit output), the employment coefficients applied uniformly lead to an upward bias in estimated employment effects (footnote 5 of the paper).

4. **Data imputations**: The paper mentions using non-public ESA 2010 data for imputations where public employment data are missing. We cannot replicate these imputations.

5. **FIGARO data vintage**: If the FIGARO tables have been revised since the paper's publication, results will differ.

---

## 9. File structure

```
figaro_replication/
├── config.yaml
├── run_pipeline.py  (or .R — master orchestrator)
├── src/
│   ├── stage1_data_acquisition.py
│   ├── stage2_data_preparation.py
│   ├── stage3_model_construction.py
│   ├── stage4_decomposition.py
│   ├── stage5_output_generation.py
│   └── stage6_review_agent.py
├── data/
│   ├── raw/           (downloaded files)
│   ├── prepared/      (cleaned matrices)
│   ├── model/         (Leontief inverse, coefficients)
│   └── decomposition/ (final results)
├── outputs/
│   ├── tables/
│   ├── figures/
│   └── review_report.md
└── logs/
    └── pipeline_{timestamp}.log
```

---

## 10. Annex B reference: 64 industries in FIGARO

For correct industry mapping, here is the complete list from the paper:

| # | Section | Division(s) | Label |
|---|---------|-------------|-------|
| 1 | A | 01 | Products of agriculture, hunting and related services |
| 2 | A | 02 | Products of forestry, logging and related services |
| 3 | A | 03 | Fish and other fishing products; aquaculture products |
| 4 | B | 05-09 | Mining and quarrying |
| 5 | C | 10-12 | Food, beverages and tobacco products |
| 6 | C | 13-15 | Textiles, wearing apparel, leather and related products |
| 7 | C | 16 | Wood and products of wood and cork |
| 8 | C | 17 | Paper and paper products |
| 9 | C | 18 | Printing and recording services |
| 10 | C | 19 | Coke and refined petroleum products |
| 11 | C | 20 | Chemicals and chemical products |
| 12 | C | 21 | Basic pharmaceutical products and preparations |
| 13 | C | 22 | Rubber and plastic products |
| 14 | C | 23 | Other non-metallic mineral products |
| 15 | C | 24 | Basic metals |
| 16 | C | 25 | Fabricated metal products |
| 17 | C | 26 | Computer, electronic and optical products |
| 18 | C | 27 | Electrical equipment |
| 19 | C | 28 | Machinery and equipment n.e.c. |
| 20 | C | 29 | Motor vehicles, trailers and semi-trailers |
| 21 | C | 30 | Other transport equipment |
| 22 | C | 31-32 | Furniture and other manufactured goods |
| 23 | C | 33 | Repair and installation of machinery and equipment |
| 24 | D | 35 | Electricity, gas, steam and air conditioning |
| 25 | E | 36 | Natural water; water treatment and supply |
| 26 | E | 37-39 | Sewerage, waste, remediation services |
| 27 | F | 41-43 | Construction |
| 28 | G | 45 | Wholesale/retail trade and repair of motor vehicles |
| 29 | G | 46 | Wholesale trade services |
| 30 | G | 47 | Retail trade services |
| 31 | H | 49 | Land transport and pipelines |
| 32 | H | 50 | Water transport |
| 33 | H | 51 | Air transport |
| 34 | H | 52 | Warehousing and transport support |
| 35 | H | 53 | Postal and courier services |
| 36 | I | 55-56 | Accommodation and food services |
| 37 | J | 58 | Publishing services |
| 38 | J | 59-60 | Motion picture, video, TV, broadcasting |
| 39 | J | 61 | Telecommunications |
| 40 | J | 62-63 | Computer programming, consultancy, information |
| 41 | K | 64 | Financial services |
| 42 | K | 65 | Insurance, reinsurance, pension funding |
| 43 | K | 66 | Auxiliary financial/insurance services |
| 44 | L | 68 | Real estate |
| 45 | M | 69-70 | Legal, accounting, head offices, management consultancy |
| 46 | M | 71 | Architecture, engineering, technical testing |
| 47 | M | 72 | Scientific R&D |
| 48 | M | 73 | Advertising and market research |
| 49 | M | 74-75 | Other professional/scientific/veterinary services |
| 50 | N | 77 | Rental and leasing |
| 51 | N | 78 | Employment services |
| 52 | N | 79 | Travel agency and tour operator services |
| 53 | N | 80-82 | Security, building services, office support |
| 54 | O | 84 | Public administration and defence |
| 55 | P | 85 | Education |
| 56 | Q | 86 | Human health |
| 57 | Q | 87-88 | Residential care, social work |
| 58 | R | 90-92 | Creative, arts, entertainment, cultural services |
| 59 | R | 93 | Sporting and recreation services |
| 60 | S | 94 | Membership organisation services |
| 61 | S | 95 | Repair of computers and household goods |
| 62 | S | 96 | Other personal services |
| 63 | T | 97-98 | Household services |
| 64 | U | 99 | Extraterritorial organisations |

The 10-sector aggregation for Table 4 maps these as:
- **A**: industries 1–3
- **B-E**: industries 4–26
- **F**: industry 27
- **G-I**: industries 28–36
- **J**: industries 37–40
- **K**: industries 41–43
- **L**: industry 44
- **M-N**: industries 45–53
- **O-Q**: industries 54–57
- **R-U**: industries 58–64

---

## 11. Key mathematical formulae (for reference)

### Technical coefficients
$$A^{rs} = Z^{rs} \cdot \hat{x}^{s^{-1}}$$

### Leontief inverse
$$L^{EU} = (I - A^{EU})^{-1}$$

### Employment coefficients
$$d^r = \hat{x}^{r^{-1}} \cdot Em^r$$

### Employment content of exports
$$Em_{ex}^{EU} = \hat{d}^{EU'} \cdot L^{EU} \cdot e^{EU}$$

### Expanded (for 2 EU countries and 1 non-EU):
$$Em_{ex}^{EU} = d^{1'} L^{11} e^{13} + d^{1'} L^{12} e^{23} + d^{2'} L^{21} e^{13} + d^{2'} L^{22} e^{23}$$

Where $d^{r'} L^{rs} e^{st}$ = employment in EU country r supported by exports of EU country s to non-EU country t.

---

## 12. Human input points

The following are points where the agentic flow should **pause and ask the human** before proceeding:

1. **Data download failure**: If FIGARO or employment data cannot be downloaded automatically, ask the human for the file path or URL.

2. **Significant data discrepancies**: If EU-28 total employment deviates by more than 5% from the paper's value (225,677 thousand), pause and ask whether to proceed.

3. **Missing country data**: If any EU member state has entirely missing employment or IOT data, ask whether to proceed with a reduced set.

4. **Review agent FAIL**: If the review agent finds critical errors (any check in 7.3 or 7.4 with deviation > 25%), stop and present findings to the human before producing final outputs.

5. **Year selection**: If the reference year is changed from 2010, note that cross-checks against paper values won't apply and ask whether to proceed without them.

---

## 13. Execution instructions for Claude Code

Run the pipeline:

```bash
cd figaro_replication
python run_pipeline.py --config config.yaml
```

Or stage by stage for debugging:

```bash
python -m src.stage1_data_acquisition --config config.yaml
python -m src.stage2_data_preparation --config config.yaml
python -m src.stage3_model_construction --config config.yaml
python -m src.stage4_decomposition --config config.yaml
python -m src.stage5_output_generation --config config.yaml
python -m src.stage6_review_agent --config config.yaml
```

Start by building the full project structure, then implement each stage sequentially. Test each stage before moving to the next. The review agent should be the last thing you build but the first thing you verify works correctly.
