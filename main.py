# -*- coding: utf-8 -*-
"""
main.py – Master orchestration script for the OCR data pipeline.

Run with:
    python main.py

The pipeline proceeds through numbered steps (printed to stdout).
Intermediate CSVs are saved at checkpoints so you can resume / inspect.
"""

import re
import pandas as pd
from rapidfuzz import fuzz

# ── Import sub-modules ──────────────────────────────────────────────────
from ocr_modules.config import (
    FIRM_PATTERN, INITIALS_PATTERN, PARISH_DICT_KNOWN, CITIES_PAR,
)
from ocr_modules.utils import remove_accents, fuzzy_match_rapidfuzz
from ocr_modules import data_loader
from ocr_modules import last_name_matching
from ocr_modules import line_processing
from ocr_modules import initials_names
from ocr_modules import occupation
from ocr_modules import income
from ocr_modules import parish
from ocr_modules import location
from ocr_modules import firm_estate
from ocr_modules import classification


def main():
    # ================================================================
    # STEP 1 – Load all input data
    # ================================================================
    print("[Step 1/14] Loading input data ...")
    dirty_last_names_list, dirty_last_names_dict = data_loader.load_dirty_last_names()
    df_death_reg_unacc = data_loader.load_death_register()
    first_names = data_loader.load_first_names()
    main_dataframe = data_loader.load_main_dataframe()
    surname_list = data_loader.build_surname_list(main_dataframe)

    # ================================================================
    # STEP 2 – Last-name matching (A1 → A5)  [parallelised]
    # ================================================================
    print("[Step 2/14] Running last-name matching (A1-A5) ...")
    surname_list = last_name_matching.parallel_alt_algorithm(
        surname_list, df_death_reg_unacc, dirty_last_names_list,
    )

    surname_list["unique_key"] = (
        surname_list["page"].astype(str) + "_"
        + surname_list["column"].astype(str) + "_"
        + surname_list["row"].astype(str) + "_"
        + surname_list["line"].astype(str)
    )
    df_dash = surname_list[surname_list["line"] == "-"]
    surname_list = surname_list[surname_list["line"] != "-"]

    # Checkpoint
    surname_list.to_csv("alt_alg_checkpoint.csv", index=False)
    surname_list = pd.read_csv("alt_alg_checkpoint.csv")

    # ================================================================
    # STEP 3 – V. / dash last-name handling + line cleaning
    # ================================================================
    print("[Step 3/14] V./dash handling & line cleaning ...")
    surname_list["fuzzy_v_dash"] = 0
    surname_list["line_complete"] = surname_list["line"].fillna("")

    surname_list = surname_list.apply(line_processing.clean_dot_num, axis=1)
    surname_list = line_processing.fix_initials_and_dots(surname_list)

    # ================================================================
    # STEP 4 – Main processing loop (pass 0 & 1)
    # ================================================================
    occ_list = data_loader.load_occupation_list()
    prefix_dict = initials_names.build_prefix_dict(first_names)

    for loop_i in range(2):
        print(f"[Step 4/14] Main processing loop – pass {loop_i} ...")

        # 4a – Residual line
        surname_list["residual_line"] = ""
        surname_list = surname_list.apply(line_processing.get_the_residual_line, axis=1)

        # 4b – Initials
        surname_list["initials"] = ""
        surname_list = surname_list.apply(initials_names.get_initials, axis=1)
        surname_list = initials_names.first_name(surname_list, first_names, prefix_dict)

        # 4c – Update residual after initials
        surname_list = surname_list.apply(line_processing.update_residual_after_initials, axis=1)

        # 4d – Second last name
        surname_list["second_last_name"] = ""
        surname_list = surname_list.apply(initials_names.second_last_name, axis=1)
        surname_list = surname_list.apply(
            line_processing.update_residual_after_second_last_name, axis=1)

        # 4e – f.d. removal + Occupation extraction
        surname_list["f_d_"] = surname_list["residual_line"].apply(
            lambda x: 1 if re.search(r'\bf\.\s*d\.', x) else 0)
        surname_list["residual_line"] = surname_list.apply(
            lambda x: re.sub(r'\bf\.\s*d\.', "", str(x["residual_line"]))
            if x["f_d_"] == 1 else x["residual_line"], axis=1)
        surname_list["residual_line"] = surname_list.apply(
            lambda x: x["residual_line"][1:].strip()
            if x["f_d_"] == 1 and x["residual_line"] and x["residual_line"][0] == ","
            else x["residual_line"], axis=1)

        surname_list["occ_reg"] = ""
        surname_list = surname_list.apply(
            lambda row: occupation.extract_occ(row, occ_list), axis=1)

        # 4f – Update residual after occupation
        surname_list = surname_list.apply(
            line_processing.update_residual_after_occupation, axis=1)

        # ── Pass-0-only: line splitting + income ──
        if loop_i == 0:
            surname_list = surname_list.groupby(["page", "column"]).apply(
                lambda g: line_processing.split_line(g, occ_list))
            surname_list = surname_list.drop(columns={"column", "page"}).reset_index()
            surname_list = surname_list.drop(columns={"level_2"})
            surname_list = income.find_income(
                surname_list, line_processing.third_line, occ_list)

        # ── Pass-1-only: income re-extraction + parish + firm/estate ──
        if loop_i == 1:
            surname_list["income"] = 0
            surname_list = surname_list.apply(income.extr_inc, axis=1)
            surname_list["income_1"] = ""
            surname_list["income_2"] = ""
            surname_list = surname_list.apply(income.split_income, axis=1)
            surname_list["income"] = surname_list["income"].apply(
                lambda x: x if bool(re.search(r'\d', str(x))) else "")

            # Secondary lowercase last-name adjustment
            surname_list = line_processing.adj_sec_lowercase_LN(surname_list)

            # Parish extraction (3 passes)
            surname_list["parish"] = ""
            surname_list = surname_list.apply(parish.extract_parish, axis=1)
            surname_list = surname_list.apply(parish.extract_parish_no_init, axis=1)
            surname_list = surname_list.apply(parish.extra_parish_residual_cases, axis=1)

            # Spot wrong occupation
            surname_list["change_occ"] = 0
            surname_list = surname_list.apply(
                lambda row: parish.spot_wrong_occ(row, occ_list), axis=1)
            surname_list["parish"] = surname_list.apply(
                lambda x: "" if x["parish"] == x["parish"] and x["index"] == "A1"
                else x["parish"], axis=1)

            # Initials duplicate adjustment
            surname_list = surname_list.apply(initials_names.adj_initials_dupl, axis=1)

            # Parish post-processing
            surname_list["parish"] = surname_list["parish"].apply(
                lambda x: re.sub(r'\d+', "", str(x)))
            surname_list["parish"] = surname_list["parish"].apply(
                lambda x: "" if re.search(FIRM_PATTERN, x) else x)
            surname_list["parish"] = surname_list.apply(
                lambda x: "" if x["parish"].endswith(x["occ_reg"]) else x["parish"], axis=1)
            surname_list["parish"] = surname_list.apply(
                lambda row: ""
                if ((row["parish"].lower() in occ_list["occ_llm"].values
                     or re.search(FIRM_PATTERN, row["parish"])
                     or any(word in occ_list["occ_llm"].values
                            for word in row["parish"].lower().split()))
                    and len(re.findall(r'[a-z]', row["parish"])) > 4
                    and "-" not in row["parish"])
                else row["parish"], axis=1)

            # Firm & estate tokens
            surname_list["firm_dummy"] = 0
            surname_list = surname_list.apply(firm_estate.firm_token, axis=1)
            surname_list = firm_estate._ind_FT(
                surname_list, df_death_reg_unacc, surname_list)

            surname_list["initials"] = surname_list.apply(
                lambda row: ""
                if (row["firm_dummy"] == 1
                    and isinstance(row["initials"], str)
                    and len(re.findall(r'[a-z]', row["initials"])) > 3)
                else row["initials"], axis=1)

            surname_list["estate_dummy"] = 0
            surname_list = surname_list.apply(firm_estate.estate_token, axis=1)

    # ================================================================
    # STEP 5 – Location assignment
    # ================================================================
    print("[Step 5/14] Assigning locations / municipalities ...")
    surname_list["location"] = ""
    surname_list = surname_list.apply(location.find_locations, axis=1)
    location_list = location.build_location_list(surname_list)
    surname_list = location.extract_location(surname_list, location_list)
    surname_list = location.location_limit_case(surname_list)

    # ================================================================
    # STEP 6 – Suspect-occupation adjustment
    # ================================================================
    print("[Step 6/14] Adjusting suspect occupations ...")
    surname_list = surname_list.apply(
        lambda row: occupation.adj_suspect_occ(row, occ_list), axis=1)

    # Checkpoint
    surname_list.to_csv("a_4.csv", index=False)
    surname_list = pd.read_csv("a_4.csv")
    cols = ["second_last_name", "occ_reg", "income", "income_1", "income_2",
            "last_name", "best_match", "initials"]
    for col in cols:
        surname_list[col] = surname_list[col].apply(lambda x: "" if pd.isna(x) else x)

    # ================================================================
    # STEP 7 – Extra first-half adjustment + pages to cut
    # ================================================================
    print("[Step 7/14] Extra adjustments & page filtering ...")
    surname_list = line_processing.adj_extra_FH(surname_list)
    pages_to_cut = classification.find_pages_to_cut(surname_list)

    # ================================================================
    # STEP 8 – Certain-lines classification loop (pass 0 & 1)
    # ================================================================
    certain = {}
    for class_i in range(2):
        print(f"[Step 8/14] Classification loop – pass {class_i} ...")
        surname_list, certain = classification.certain_lines(
            surname_list, pages_to_cut, occ_list)
        surname_list, certain = classification.potential_sec_lines(
            surname_list, certain)

        if class_i == 0:
            surname_list = classification.adj_sec_lines(surname_list, certain)

        remaining_lines = surname_list[
            ~surname_list["unique_key"].isin(
                pd.concat([df["unique_key"] for df in certain.values()]))
        ].sort_values(by="index")

        remaining_lines = classification.potential_FH(
            remaining_lines, surname_list, certain)

        if class_i == 0:
            surname_list = classification._adj_pot_FH(
                surname_list, certain, surname_list)
            surname_list = income.find_income(
                surname_list, line_processing.third_line, occ_list)

            # Re-run parish passes
            surname_list["parish"] = ""
            surname_list = surname_list.apply(parish.extract_parish, axis=1)
            surname_list = surname_list.apply(parish.extract_parish_no_init, axis=1)
            surname_list = surname_list.apply(parish.extra_parish_residual_cases, axis=1)
            surname_list["change_occ"] = 0
            surname_list = surname_list.apply(
                lambda row: parish.spot_wrong_occ(row, occ_list), axis=1)
            surname_list["parish"] = surname_list.apply(
                lambda x: "" if x["parish"] == x["parish"] and x["index"] == "A1"
                else x["parish"], axis=1)
            surname_list = surname_list.apply(initials_names.adj_initials_dupl, axis=1)
            surname_list["parish"] = surname_list["parish"].apply(
                lambda x: re.sub(r'\d+', "", str(x)))
            surname_list["parish"] = surname_list["parish"].apply(
                lambda x: "" if re.search(FIRM_PATTERN, x) else x)
            surname_list["parish"] = surname_list.apply(
                lambda x: "" if x["parish"].endswith(x["occ_reg"]) else x["parish"], axis=1)
            surname_list["parish"] = surname_list.apply(
                lambda row: ""
                if ((row["parish"].lower() in occ_list["occ_llm"].values
                     or re.search(FIRM_PATTERN, row["parish"])
                     or any(word in occ_list["occ_llm"].values
                            for word in row["parish"].lower().split()))
                    and len(re.findall(r'[a-z]', row["parish"])) > 4
                    and "-" not in row["parish"])
                else row["parish"], axis=1)

            surname_list = surname_list.apply(
                lambda row: occupation.adj_suspect_occ(row, occ_list), axis=1)

        if class_i == 0:
            surname_list = surname_list.apply(
                lambda row: classification.adj_tell_split(row, certain), axis=1)
            del certain
            del remaining_lines

    # ================================================================
    # STEP 9 – Fuzzy occupation matching + secondary occupation
    # ================================================================
    print("[Step 9/14] Fuzzy occupation matching ...")
    surname_list["occ_reg"] = surname_list.apply(
        lambda row: row["occ_reg"] if row["occ_reg"] != ""
        else occupation.occ_fuzz(row, occ_list), axis=1)

    surname_list["occ_reg_2"] = surname_list.apply(
        lambda row: row["occ_reg"] if row["occ_reg"] == ""
        else occupation.sec_occup(row, occ_list), axis=1)

    # ================================================================
    # STEP 10 – Final parish adjustments
    # ================================================================
    print("[Step 10/14] Final parish adjustments ...")
    def fin_adj_par(row):
        initials_ = row["initials"]
        if (row["parish"] == initials_
                and any(initials_.split().count(w) == 1 for w in initials_.split())
                and row["firm_dummy"] == 1 and len(initials_.split()) == 1):
            row["initials"] = ""
        return row
    surname_list = surname_list.apply(fin_adj_par, axis=1)

    # Re-extract parishes for the subset still missing them
    df_subset = surname_list[
        (~surname_list["line"].str.contains(FIRM_PATTERN, regex=True, na=False))
        & (surname_list["last_name"] != "")
        & (surname_list["split"].isin([1, 3]))
        & (surname_list["parish"] == "")
        & (surname_list["line"].str.contains(r'[A-Za-z]', regex=True))
        & (surname_list["index"] != "A1")
    ]
    df_subset = df_subset.apply(parish.extract_parish, axis=1)
    df_subset = df_subset.apply(parish.extract_parish_no_init, axis=1)
    df_subset = df_subset.apply(parish.extra_parish_residual_cases, axis=1)
    surname_list.update(df_subset)

    # ================================================================
    # STEP 11 – Parish mapping + proper parish merge
    # ================================================================
    print("[Step 11/14] Parish mapping & merge ...")
    surname_list["parish_cleaned_"] = ""
    surname_list = surname_list.fillna("")
    proper_parish = pd.read_csv("proper_parish.csv", index_col=0).fillna("")

    surname_list = surname_list.apply(parish.cleaned_parish, axis=1)
    surname_list = surname_list.apply(
        lambda row: parish.parish_map(row, proper_parish), axis=1)

    surname_list = surname_list.merge(
        proper_parish[["parish", "mapped_parish", "parish_cleaned"]],
        on="parish", how="left")
    surname_list["parish_cleaned_"] = surname_list.apply(
        lambda row: row["mapped_parish"]
        if pd.notna(row["mapped_parish"]) and row["mapped_parish"] != ""
        else (row["parish_cleaned"]
              if pd.notna(row["parish_cleaned"]) and row["parish_cleaned"] != ""
              else row["parish"]), axis=1)
    surname_list = surname_list.fillna("")
    surname_list = surname_list.apply(parish.cleaned_parish, axis=1)
    surname_list = surname_list.drop(columns={"parish_cleaned", "mapped_parish"})

    # Parish adjustment passes
    surname_list = surname_list.apply(
        lambda row: parish.parish_adjustment(row, proper_parish, comma=True), axis=1)
    surname_list = surname_list.apply(
        lambda row: parish.parish_adjustment(row, proper_parish, comma=False), axis=1)

    # Remove firm patterns from parish
    parish_num = pd.DataFrame(surname_list["parish"].unique())
    parish_firm = parish_num[parish_num[0].str.contains(FIRM_PATTERN, regex=True)]
    parish_num = parish_num[~parish_num[0].isin(parish_firm[0])]
    surname_list = surname_list.apply(
        lambda row: parish.remove_firms_from_parish(row, parish_num, parish_firm), axis=1)

    # Firm parish extraction
    surname_list = surname_list.apply(
        lambda row: parish.firms_parishes_(row, initials=True, comma=True), axis=1)
    surname_list = surname_list.apply(
        lambda row: parish.firms_parishes_(row, initials=True, comma=False), axis=1)
    surname_list = surname_list.apply(
        lambda row: parish.firms_parishes_(row, initials=False, comma=True), axis=1)

    # Refresh parish_num and re-clean
    parish_num = pd.DataFrame(surname_list["parish"].unique())
    parish_firm = parish_num[parish_num[0].str.contains(FIRM_PATTERN, regex=True)]
    parish_num = parish_num[~parish_num[0].isin(parish_firm[0])]
    surname_list = surname_list.apply(
        lambda row: parish.remove_firms_from_parish(row, parish_num, parish_firm), axis=1)

    # Parish vs occupation/initials/second-last-name conflicts
    surname_list["parish"] = surname_list.apply(
        lambda x: "" if x["parish"].lower() == x["occ_reg"].lower() else x["parish"], axis=1)
    surname_list["parish"] = surname_list.apply(
        lambda x: ""
        if (x["parish"] == x["initials"] and not re.search(r'\.', x["initials"])
            and any(name == x["initials"] for name in first_names.values))
        else x["parish"], axis=1)
    surname_list["parish"] = surname_list.apply(
        lambda x: ""
        if x["parish"] == x["second_last_name"] and x["parish"] not in PARISH_DICT_KNOWN
        else x["parish"], axis=1)
    surname_list["second_last_name"] = surname_list.apply(
        lambda x: ""
        if x["parish"] == x["second_last_name"] and x["parish"] in PARISH_DICT_KNOWN
        else x["second_last_name"], axis=1)

    # Double-check firm initials/parishes
    mask = (
        (surname_list["initials"] == surname_list["parish"])
        & (surname_list["parish"] != "")
        & (surname_list["firm_dummy"] == 1)
        & (surname_list.apply(
            lambda x: x["line_complete"].count(str(x["initials"])), axis=1) == 1)
    )
    surname_list.loc[mask, "initials"] = ""

    # Checkpoint
    surname_list.to_csv("aaa_5.csv", index=False)
    surname_list = pd.read_csv("aaa_5.csv").fillna("")

    # ================================================================
    # STEP 12 – Double-count resolution
    # ================================================================
    print("[Step 12/14] Resolving double-counts ...")
    from ocr_modules.parish import cleaned_parish as _cleaned_parish

    double_count_in = surname_list[
        (surname_list["parish"] == surname_list["initials"])
        & (surname_list.apply(
            lambda x: str(x["line_complete"]).count(str(x["initials"])), axis=1) == 1)
        & (surname_list["initials"] != "")
        & (surname_list["firm_dummy"] == 0)
    ]

    def first_clean(row):
        occ_ = row["occ_reg"]
        initials_ = row["initials"]
        line = row["line_complete"]
        last_name = row["last_name"]
        if occ_ != "":
            try:
                pos_occ_ = line.lower().index(occ_)
                pos_init = line.index(initials_)
            except ValueError:
                return row
            if pos_occ_ and pos_init:
                if pos_occ_ > pos_init:
                    row["parish"] = ""
                    return row
                if pos_occ_ < pos_init:
                    row["initials"] = ""
                    line_split_space = [ch.replace(",", "").strip() for ch in line.split()]
                    candidate = [word for word in line_split_space
                                 if word in first_names.values and word != last_name and len(word) > 2]
                    if candidate:
                        row["initials"] = candidate[0]
                    return row
        return row

    double_count_in = double_count_in.apply(first_clean, axis=1)
    surname_list.update(double_count_in)

    # Further double-count passes (simplified)
    double_count_in = double_count_in[
        (double_count_in["parish"] == double_count_in["initials"])
        & (double_count_in.apply(
            lambda x: str(x["line_complete"]).count(str(x["initials"])), axis=1) == 1)
        & (double_count_in["initials"] != "")
        & (double_count_in["firm_dummy"] == 0)
    ]

    def double_count_init_par(row):
        initials_ = row["initials"]
        line = row["line_complete"]
        last_name = row["last_name"]
        line_split = [x.strip() for x in line.split(",")]
        if (last_name != "" and any(w.strip().islower() for w in line.split())
                and re.search(r'[A-Z][a-z]', line)):
            pos_candidates = [i for i, x in enumerate(line_split) if initials_ in x]
            if pos_candidates:
                pos_initial = pos_candidates[0]
                if pos_initial > 0 and any(word.islower()
                                           for word in line_split[pos_initial - 1].split()):
                    row["initials"] = ""
                    if len(line_split) > 1 and line_split[1].strip() in first_names.values:
                        row["initials"] = line_split[1].strip()
        elif (last_name != ""
              and any(word.strip() in first_names.values for word in line_split)):
            candidate = [w for w in line_split
                         if w in first_names.values and w != last_name and len(w) > 2]
            row["initials"] = ' '.join(candidate).strip()
        return row

    double_count_in = double_count_in.apply(double_count_init_par, axis=1)
    surname_list.update(double_count_in)

    # avoid_double_count_init
    def avoid_double_count_init(row):
        init_ = row["initials"]
        par = row["parish"]
        line = row["line_complete"]
        occ_ = row["occ_reg"]
        if (re.search(r'(?:[A-Z]\.|[A-Z][a-z]{1,2}\.)', par)
                and par != "" and par.rstrip(",") in init_
                and line.count(par) == 1 and occ_ == ""
                and row["split"] == 3 and row["firm_dummy"] == 0
                and not re.search(r'\bfru|anke|froken', line)
                and row["estate_dummy"] == 0):
            pos_init = line.index(init_)
            line_cut = line[pos_init: pos_init + len(init_) - 1]
            row["parish"] = "" if not re.search(r',', line_cut) else row["parish"]
        return row
    surname_list = surname_list.apply(avoid_double_count_init, axis=1)

    # manage_wrong_parish
    def manage_wrong_parish(row):
        init_ = row["initials"]
        par = row["parish"]
        line = row["line_complete"]
        occ_ = row["occ_reg"]
        ln = row["last_name"]
        if (occ_ == "" and row["split"] in [1, 3] and row["firm_dummy"] == 0
                and row["estate_dummy"] == 0 and par != "" and init_ == ""
                and re.search(r'[A-Z]\.', line)):
            try:
                pos_par = line.index(par)
            except ValueError:
                return row
            line_cut = line[:pos_par]
            word = re.findall(r"[A-Za-zÀ-ÖØ-öø-ÿ-]+", line_cut)[-1].rstrip(",").strip() if line_cut.strip() else ""
            if ln in word and ln != "":
                row["initials"] = par
                row["parish"] = ""
        return row
    surname_list = surname_list.apply(manage_wrong_parish, axis=1)

    # Checkpoint
    surname_list.to_csv("aaa_6_final.csv", index=False)
    surname_list = pd.read_csv("aaa_6_final.csv").fillna("")

    # ================================================================
    # STEP 13 – Parish quality check
    # ================================================================
    print("[Step 13/14] Running parish quality check ...")
    parish_only_matched = pd.read_csv("final_parish_csv_quality_check.csv")
    df_parish_added_year_by_year = pd.read_csv("df_extra_parish_iterative_check.csv")

    parish_mapped = pd.read_csv("parish_county.csv")
    parish_mapped = parish_mapped.rename(columns={"parish": "Parish", "county": "municipality"})
    replacements = {
        "ö": "o", "ä": "a", "à": "a", "å": "a",
        "Ö": "O", "Ä": "A", "Å": "A",
    }
    for old, new in replacements.items():
        parish_mapped["Parish"] = parish_mapped["Parish"].str.replace(old, new)
        parish_mapped["municipality"] = parish_mapped["municipality"].str.replace(old, new)
    parish_mapped["Parish"] = (
        parish_mapped["Parish"].str[0].str.upper() + parish_mapped["Parish"].str[1:])

    surname_list = parish.run_parish_quality_check(
        surname_list, parish_mapped, parish_only_matched,
        df_parish_added_year_by_year)

    # ================================================================
    # STEP 14 – Final output
    # ================================================================
    print("[Step 14/14] Writing final output ...")
    final_set = surname_list[[
        "page", "column", "row", "line", "line_complete", "index", "split",
        "firm_dummy", "estate_dummy", "last_name", "best_match", "initials",
        "occ_reg", "occ_reg_2", "municipality", "parish", "matched_parish",
        "unique_key", "income", "income_1", "income_2",
    ]]
    final_set.to_csv("final_output.csv", index=False)
    print("Done! Output written to: final_output.csv")


if __name__ == "__main__":
    main()
