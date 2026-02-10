# OCR Data Pipeline – Flowchart & How-to-Run Guide

## Project Overview

This pipeline processes raw OCR text, extracts structured fields (last name,
first name / initials, occupation, parish, income), and produces a clean CSV
dataset. It is designed to work with any OCR source that provides page, column,
row, and line-level text data.

---

## Repository Structure

```
Codal/
├── main.py                        # Master orchestration script (run this)
├── run_ocr_batch.py               # Generate 6 OCR inputs + run pipeline per provider
├── legacy_monolithic.py           # Original monolithic script (archived)
├── README.md                      # This file
│
└── ocr_modules/                   # Refactored sub-modules
    ├── __init__.py
    ├── config.py                  # Constants, regex patterns, dictionaries
    ├── utils.py                   # Shared utility functions
    ├── data_loader.py             # Data loading & initial dataframe setup
    ├── last_name_matching.py      # Surname matching (A1-A5 algorithm)
    ├── line_processing.py         # Line cleaning, splitting, residuals
    ├── initials_names.py          # Initials & first-name extraction
    ├── occupation.py              # Occupation extraction & fuzzy matching
    ├── income.py                  # Income extraction & splitting
    ├── parish.py                  # Parish extraction, mapping, quality check
    ├── location.py                # Location / municipality assignment
    ├── classification.py          # Certain-lines classification
    ├── firm_estate.py             # Firm & estate token detection
    └── ocr_input_builder.py       # Build OCR inputs from providers
```

---

## Required Input Files

Place these files in the **same directory** as `main.py`:

| File | Description |
|------|-------------|
| `ocr_input.csv` | Main OCR data (page, column, row, line) |
| `Updated_DR.csv` | Death register with last names |
| `Last_names_to_update_DR.xlsx` | Dirty → clean last-name corrections |
| `Burial_names.dta` | First-name reference (Stata format) |
| `occ_list_for_alg.csv` | Occupation reference list |
| `proper_parish.csv` | Parish mapping table |
| `parish_county.csv` | Parish → county reference |
| `final_parish_csv_quality_check.csv` | Parish quality-check reference |
| `df_extra_parish_iterative_check.csv` | Iteratively improved parish list |

**Note:** `Burial_names.dta` is not stored in git (file is too large for GitHub).  
Place it in the repository root before running the pipeline.

---

## How to Run

### 1. Install dependencies

```bash
pip install pandas rapidfuzz python-Levenshtein pyreadstat tqdm tiktoken regex beautifulsoup4
```

### 2. Run the pipeline

```bash
python main.py
```

You can also pass input/output paths:

```bash
python main.py --input ocr_input.csv --out-dir . --output-prefix final_output
```

The script prints progress for each of 14 steps:

```
[Step 1/14]  Loading input data ...
[Step 2/14]  Running last-name matching (A1-A5) ...
[Step 3/14]  V./dash handling & line cleaning ...
   ...
[Step 14/14] Writing final output ...
Done! Output written to: final_output.csv
```

### 3. Output

| File | Description |
|------|-------------|
| `final_output.csv` | Final structured dataset |
| `alt_alg_checkpoint.csv` | Checkpoint after last-name matching |
| `a_4.csv` | Checkpoint after occupation adjustment |
| `aaa_5.csv` | Checkpoint after parish mapping |
| `aaa_6_final.csv` | Checkpoint after double-count resolution |

---

## Batch Run (6 OCR Providers)

Use `run_ocr_batch.py` to build OCR inputs for Amazon, Deepseek, Qwen,
Nanonet, NVIDIA (Nemotron), and MinerU, then run the pipeline for each.

1. Edit the paths at the top of `run_ocr_batch.py`.
2. Run:

```bash
python run_ocr_batch.py
```

Outputs are written to:

```
runs/1912/ocr_inputs/<provider>.csv
runs/1912/outputs/<provider>/final_output_<provider>.csv
runs/1912/outputs/<provider>/reports/
```

Providers: `amazon`, `deepseek`, `qwen`, `nano`, `nvidia`, `mineru`.

---

## Pipeline Flowchart (Step by Step)

```
┌─────────────────────────────────────────────────────────┐
│  STEP 1: LOAD DATA  (data_loader.py)                    │
│  - ocr_input.csv  → main_dataframe                      │
│  - Updated_DR.csv  → death register                     │
│  - Last_names_to_update_DR.xlsx  → dirty last names     │
│  - Burial_names.dta  → first names                      │
└───────────────────────┬─────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────┐
│  STEP 2: LAST-NAME MATCHING  (last_name_matching.py)    │
│                                                         │
│  For each OCR line:                                     │
│    A1 ─ Starts with "hustru/fru/fröken"? → skip         │
│    A2 ─ Exact match against death register?             │
│    A3 ─ Fuzzy match (prefix-filtered, score ≥ 90)?      │
│    A4 ─ Fuzzy match (full scan, score ≥ 85)?            │
│    A5 ─ Unmatched                                       │
│                                                         │
│  Also: V. → von conversion, hyphenated last names       │
│  → Checkpoint: alt_alg_checkpoint.csv                   │
└───────────────────────┬─────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────┐
│  STEP 3: LINE CLEANING  (line_processing.py)            │
│  - Fix "0," → "O.," (initial correction)                │
│  - Fix ".." → ".,"                                      │
│  - Clean comma/dot numbers                              │
└───────────────────────┬─────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────┐
│  STEP 4: MAIN PROCESSING LOOP  (2 passes)               │
│                                                         │
│  ┌─── For each pass (0, 1): ──────────────────────┐     │
│  │                                                 │     │
│  │  4a. Residual line extraction                   │     │
│  │      (remove last name from line)               │     │
│  │              │                                  │     │
│  │              ▼                                  │     │
│  │  4b. Extract initials  (initials_names.py)      │     │
│  │      A. B.  E.  etc.                            │     │
│  │      Also detect first names                    │     │
│  │              │                                  │     │
│  │              ▼                                  │     │
│  │  4c. Second last-name detection                 │     │
│  │      (tokens with ":")                          │     │
│  │              │                                  │     │
│  │              ▼                                  │     │
│  │  4d. Remove "f.d." marker                       │     │
│  │              │                                  │     │
│  │              ▼                                  │     │
│  │  4e. Occupation extraction  (occupation.py)     │     │
│  │      Exact match against occ reference list     │     │
│  │              │                                  │     │
│  │              ▼                                  │     │
│  │  4f. Update residual line                       │     │
│  │              │                                  │     │
│  │              ▼                                  │     │
│  │  Pass 0 only:                                   │     │
│  │    - Line splitting (1=FH, 2=SH, 3=complete)   │     │
│  │    - Unite split lines → income extraction      │     │
│  │                                                 │     │
│  │  Pass 1 only:                                   │     │
│  │    - Income re-extraction                       │     │
│  │    - Parish extraction (3 sub-passes)           │     │
│  │    - Firm-dummy & estate-dummy assignment       │     │
│  │    - Individual + firm-token resolution         │     │
│  └─────────────────────────────────────────────────┘     │
└───────────────────────┬─────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────┐
│  STEP 5: LOCATION ASSIGNMENT  (location.py)             │
│  - Find "inv.)" location markers                        │
│  - Build location list with Stockholm as default        │
│  - Assign municipality to every row by page/row order   │
└───────────────────────┬─────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────┐
│  STEP 6: SUSPECT OCCUPATION ADJUSTMENT (occupation.py)  │
│  - Re-scan lines with no occupation                     │
│  → Checkpoint: a_4.csv                                  │
└───────────────────────┬─────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────┐
│  STEP 7: EXTRA LINE ADJUSTMENTS                         │
│  - Fix orphaned first-half splits                       │
│  - Identify pages to cut (too few names / occupations)  │
└───────────────────────┬─────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────┐
│  STEP 8: CLASSIFICATION LOOP  (classification.py)       │
│  (2 passes)                                             │
│                                                         │
│  Classify every row into buckets:                       │
│    - pages_to_cut        - certain_estate               │
│    - certain_locations   - certain_Tel_int               │
│    - only_dash           - A1 lines                     │
│    - IT_1 (LN+init+occ) - IT_2 (partial info)          │
│    - certain_firms       - certain_noise A/B            │
│                                                         │
│  Detect potential second lines (A, B, C, D)             │
│  Detect potential first lines                           │
│  Re-run parish extraction after pass 0                  │
└───────────────────────┬─────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────┐
│  STEP 9: FUZZY OCCUPATION  (occupation.py)              │
│  - Fuzzy-match remaining unmatched occupations          │
│  - Extract secondary occupations                        │
└───────────────────────┬─────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────┐
│  STEP 10: FINAL PARISH ADJUSTMENTS  (parish.py)         │
│  - Resolve initials == parish conflicts                 │
│  - Re-extract parishes for remaining blank rows         │
└───────────────────────┬─────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────┐
│  STEP 11: PARISH MAPPING  (parish.py)                   │
│  - Map abbreviations → full names (PARISH_DICT_KNOWN)   │
│  - Merge with proper_parish reference table             │
│  - parish_adjustment: fill still-missing parishes       │
│  - Firm-parish assignment                               │
│  - Conflict resolution (parish vs occ / initials)       │
│  → Checkpoint: aaa_5.csv                                │
└───────────────────────┬─────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────┐
│  STEP 12: DOUBLE-COUNT RESOLUTION                       │
│  - Resolve rows where parish == initials (only 1 in     │
│    the source line)                                     │
│  - Determine which field is correct by position         │
│  → Checkpoint: aaa_6_final.csv                          │
└───────────────────────┬─────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────┐
│  STEP 13: PARISH QUALITY CHECK  (parish.py)             │
│  - Cross-check against 3 parish reference lists:        │
│    1. final_parish_csv_quality_check.csv                │
│    2. df_extra_parish_iterative_check.csv               │
│    3. parish_county.csv (fuzzy-matched)                 │
│  - Remove parishes that fail quality check              │
│  - Add matched_parish column                            │
└───────────────────────┬─────────────────────────────────┘
                        │
                        ▼
┌─────────────────────────────────────────────────────────┐
│  STEP 14: FINAL OUTPUT                                  │
│                                                         │
│  → final_output.csv                                     │
│                                                         │
│  Columns:                                               │
│    page, column, row, line, line_complete, index,        │
│    split, firm_dummy, estate_dummy, last_name,           │
│    best_match, initials, occ_reg, occ_reg_2,             │
│    municipality, parish, matched_parish, unique_key,     │
│    income, income_1, income_2                            │
└─────────────────────────────────────────────────────────┘
```

---

## Module Dependency Diagram

```
main.py
 ├── config.py            (used by every module)
 ├── utils.py             (used by every module)
 ├── data_loader.py       (Step 1)
 ├── last_name_matching.py(Step 2)      → uses utils, config
 ├── line_processing.py   (Steps 3-4)   → uses config
 ├── initials_names.py    (Step 4)      → uses config
 ├── occupation.py        (Steps 4,6,9) → uses utils
 ├── income.py            (Step 4)      → uses line_processing
 ├── location.py          (Step 5)
 ├── firm_estate.py       (Step 4)      → uses config
 ├── classification.py    (Step 8)      → uses config
 ├── parish.py            (Steps 10-13) → uses config, utils
 └── ocr_input_builder.py (Pre-step)    → builds OCR inputs from providers
```

---

## Index Codes (last-name matching)

| Code | Meaning |
|------|---------|
| A1 | Line starts with non-occupation word (hustru, fru, ...) |
| A2 | Perfect (exact) match against death register |
| A3 | Fuzzy match (prefix-filtered, score >= 90) |
| A4 | Fuzzy match (full-scan, score >= 85) |
| A5 | Unmatched |

## Split Codes (line splitting)

| Code | Meaning |
|------|---------|
| 0 | Standalone / unclassified |
| 1 | First half of a split line |
| 2 | Second half of a split line |
| 3 | Complete (single-line entry) |
| 4 | Third part of a multi-row entry |
