# -*- coding: utf-8 -*-
"""
location.py – Location finding and municipality assignment.
"""

import re
import pandas as pd


# ── Find locations (inv.) ───────────────────────────────────────────────

def find_locations(row):
    """Extract location strings from lines containing ``inv.)``."""
    line = row["line"]

    def extr_until_brackets(s):
        s_fin = []
        for i in range(0, len(s)):
            if s[i] not in ["(", ")"]:
                s_fin.append(s[i])
            else:
                break
        return ''.join(s_fin)

    if re.search(r'inv\.\)', line):
        row["location"] = extr_until_brackets(row["line"])
        row["location"] = re.sub(r'\d+', "", row["location"])
        row["location"] = re.sub(r'inv\.', "", row["location"])
        row["location"] = re.sub(r',', "", row["location"])
    return row


# ── Build the location list ─────────────────────────────────────────────

def build_location_list(surname_list):
    """Build the reference ``location_list`` from extracted locations."""
    location_list = surname_list[surname_list["location"] != ""].copy()
    location_list = location_list[["page", "column", "row", "line", "line_complete", "split", "location"]]
    location_list["location"] = location_list.apply(
        lambda x: surname_list.loc[
            (surname_list["page"] == x["page"])
            & (surname_list["column"] == x["column"])
            & (surname_list["row"] == int(x["row"]) - 1),
            "line"
        ].values[0] if x["location"] == " " else x["location"],
        axis=1,
    )
    header = pd.DataFrame({
        "page": [0], "column": [0], "row": [0],
        "line": ["Stockholm"], "line_complete": ["Stockholm"],
        "split": [0], "location": ["Stockholm"],
    })
    location_list = pd.concat([header, location_list], axis=0)
    return location_list


# ── Assign municipality ─────────────────────────────────────────────────

def extract_location(df, location_list):
    """Assign a ``municipality`` to every row based on the location list."""
    df["municipality"] = ""
    i = 0
    index_list_ = df.index.to_list()
    start_value = location_list.iloc[0]["location"]

    for pos, idx in enumerate(index_list_):
        page = int(df.at[idx, "page"])
        row = int(df.at[idx, "row"])

        if page < location_list["page"].min():
            df.at[idx, "municipality"] = start_value
            continue

        if page not in location_list["page"].values:
            df.at[idx, "municipality"] = location_list.iloc[i]["location"]
            continue

        municipalities = location_list[location_list["page"] == page]
        n_ = len(municipalities)

        if pos + 1 < len(df):
            nxt = index_list_[pos + 1]
            if df.at[idx, "page"] != df.at[nxt, "page"]:
                i += n_

        if row < municipalities["row"].min():
            df.at[idx, "municipality"] = location_list.iloc[i]["location"]
            continue

        if row > municipalities["row"].min():
            iter_ = pd.concat([df.loc[[idx]], municipalities], axis=0)
            iter_ = iter_.sort_values(by="row").reset_index(drop=True)
            pos_2 = iter_[iter_["row"] == row].index[0]
            prev_row = iter_.iloc[pos_2 - 1] if pos_2 > 0 else None
            if prev_row is not None:
                df.at[idx, "municipality"] = prev_row["location"]
            continue

    return df


# ── Limit-case municipality ─────────────────────────────────────────────

def location_limit_case(df):
    """Fill municipality for rows that still have none."""
    idx_list = df.index.to_list()
    for pos, idx in enumerate(idx_list):
        line = df.at[idx, "line"]
        prv = idx_list[pos - 1]
        next_mun = ""
        if pos + 3 < len(df):
            nxt = idx_list[pos + 3]
            next_mun = df.at[nxt, "municipality"]
        prev_mun = df.at[prv, "municipality"]
        if df.at[idx, "location"] == "" and df.at[idx, "municipality"] == "":
            if re.search(r'[A-G]', line[0]):
                df.at[idx, "municipality"] = next_mun
                continue
            if re.search(r'[O-Z]', line[0]):
                df.at[idx, "municipality"] = prev_mun
    return df
