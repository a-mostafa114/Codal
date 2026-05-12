# -*- coding: utf-8 -*-
"""
location.py – Location finding and municipality assignment.
"""

import re
import pandas as pd


# ── Find locations (inv.) ───────────────────────────────────────────────

def find_locations(row):
    """Extract location strings from lines containing ``inv.)`` or mineru title entries."""
    line = row["line"]
    source = row.get("source", "")

    def extr_until_brackets(s):
        s_fin = []
        for i in range(len(s)):
            if s[i] not in ["(", ")"]:
                s_fin.append(s[i])
            else:
                break
        return ''.join(s_fin)

    # Pattern 1: classic inv.) form — "Goteborg (170,173 inv.)"
    if re.search(r'inv\.\)', line):
        row["location"] = extr_until_brackets(row["line"])
        row["location"] = re.sub(r'\d+', "", row["location"])
        row["location"] = re.sub(r'inv\.', "", row["location"])
        row["location"] = re.sub(r',', "", row["location"])
        return row

    # Pattern 2: mineru title entries — bare city names or "City (population)"
    if source == "title":
        stripped = line.strip()
        if not stripped or stripped.isupper():
            return row
        # "CityA—CityB" divider: take the last segment (the new city)
        if re.search(r'[—–]', stripped) and not re.search(r'\d', stripped):
            city = re.split(r'[—–]', stripped)[-1].strip()
        else:
            city = re.split(r'\s*\(', stripped)[0].strip()
        city = re.sub(r'[,\.]+$', '', city).strip()
        if city and len(city.split()) <= 5 and re.search(r'[A-Za-z]', city):
            row["location"] = city

    return row


# ── Build the location list ─────────────────────────────────────────────

def build_location_list(surname_list):
    """Build the reference ``location_list`` from extracted locations."""
    location_list = surname_list[surname_list["location"] != ""].copy()
    location_list = location_list[["page", "column", "row", "line", "line_complete", "split", "location"]]

    def resolve_location(x):
        if x["location"] != " ":
            return x["location"]
        try:
            matches = surname_list.loc[
                (surname_list["page"] == x["page"])
                & (surname_list["column"] == x["column"])
                & (surname_list["row"] == int(x["row"]) - 1),
                "line"
            ]
            return matches.values[0]
        except IndexError:
            return x["location"]  # fallback: keep original value

    location_list["location"] = location_list.apply(resolve_location, axis=1)

    header = pd.DataFrame({
        "page": [0], "column": [0], "row": [0],
        "line": ["Stockholm"], "line_complete": ["Stockholm"],
        "split": [0], "location": ["Stockholm"],
    })
    location_list = pd.concat([header, location_list], axis=0)
    return location_list


# ── Assign municipality ─────────────────────────────────────────────────

def extract_location(df, location_list):
    """Assign a ``municipality`` to every row using binary search on location markers."""
    import numpy as np

    df = df.copy()

    loc = location_list[
        location_list["location"].notna() & (location_list["location"] != "")
    ].sort_values(["page", "row"])

    # Composite key: page * 100_000 + row (rows are always < 100_000)
    loc_keys = (loc["page"].astype(int) * 100_000 + loc["row"].astype(int)).values
    loc_vals = loc["location"].tolist()

    df_keys = (df["page"].astype(int) * 100_000 + df["row"].astype(int)).values

    # For each data row find the last location marker at or before (page, row)
    idx = np.searchsorted(loc_keys, df_keys, side="right") - 1
    idx = np.clip(idx, 0, len(loc_vals) - 1)
    df["municipality"] = [loc_vals[i] for i in idx]

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
