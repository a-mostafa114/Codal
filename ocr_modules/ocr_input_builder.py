# -*- coding: utf-8 -*-
"""
ocr_input_builder.py – Build normalized OCR inputs (page/column/row/line)
from multiple OCR providers.

This module turns raw OCR outputs into a consistent DataFrame that
matches the `ocr_input.csv` format expected by the pipeline.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional
import json
import re

import pandas as pd


DEFAULT_SIDE_REGEX = re.compile(
    r".*taxeringskalender_\d+-(\d+)\.jpg_side_(\d).*",
    re.IGNORECASE,
)


@dataclass
class OcrInputConfig:
    amazon_csv: Optional[str] = None
    deepseek_dir: Optional[str] = None
    qwen_dir: Optional[str] = None
    nanonet_dir: Optional[str] = None
    nvidia_nemotron_dir: Optional[str] = None
    mineru_dir: Optional[str] = None
    out_dir: str = "."
    side_regex: re.Pattern[str] = DEFAULT_SIDE_REGEX


def score_page_quality(page_entries: List[Dict[str, Any]]) -> int:
    """Score the quality of OCR results for a single page (higher is better)."""
    if len(page_entries) == 0:
        return 0

    score = 0
    score += len(page_entries) * 10

    pattern_matches = 0
    for entry in page_entries:
        line = entry.get("line", "")
        if re.search(r"^[A-ZÅÄÖ].*,.*,.*\d+-?\d*", line):
            pattern_matches += 1
    score += pattern_matches * 20

    short_entries = sum(1 for e in page_entries if len(e.get("line", "")) < 10)
    score -= short_entries * 5

    columns = set(e.get("column") for e in page_entries)
    if len(columns) == 2:
        score += 50

    hustru_count = sum(1 for e in page_entries if "hustru" in e.get("line", "").lower())
    score += hustru_count * 15

    return score


def parse_nemotron_page(filepath: Path) -> List[Dict[str, Any]]:
    """Parse a single Nemotron .txt file."""
    x_pattern = re.compile(r"<x_([0-9.]+)>")
    valid_line_pattern = re.compile(r"class_Text|class_List-item|class_Bibliography")

    entries: List[Dict[str, Any]] = []
    with open(filepath, "r", encoding="utf-8") as f:
        for line in f:
            if not valid_line_pattern.search(line):
                continue

            if "class_Bibliography" in line:
                source = "class_Bibliography"
            elif "class_List-item" in line:
                source = "class_List-item"
            else:
                source = "class_Text"

            x_match = x_pattern.search(line)
            if not x_match:
                continue

            x_value = float(x_match.group(1))
            column = 1 if x_value < 0.25 else 2
            clean_text = re.sub(r"<[^>]+>", "", line).strip()

            entries.append({
                "column": column,
                "line": clean_text,
                "source": source,
            })

    return entries


def parse_mineru_page(filepath: Path) -> List[Dict[str, Any]]:
    """Parse a single MinerU JSON file."""
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)

        entries: List[Dict[str, Any]] = []
        for entry in data:
            entry_type = entry.get("type")
            if entry_type not in ["text", "ref_text"]:
                continue

            bbox = entry.get("bbox")
            if not bbox or len(bbox) < 4:
                continue

            x_value = bbox[0]
            column = 1 if x_value < 0.25 else 2
            clean_text = entry.get("content", "").strip()

            if clean_text:
                entries.append({
                    "column": column,
                    "line": clean_text,
                    "source": entry_type,
                })

        return entries
    except Exception:
        return []


def load_nvidia_results(nemotron_dir: str) -> pd.DataFrame:
    """Load Nemotron results into a DataFrame."""
    nemotron_folder = Path(nemotron_dir)
    records: List[Dict[str, Any]] = []

    for filepath in sorted(nemotron_folder.glob("*.txt")):
        page = int(filepath.stem)
        entries = parse_nemotron_page(filepath)
        page_score = score_page_quality(entries)

        for entry in entries:
            records.append({
                "page": page,
                "column": entry["column"],
                "line": entry["line"],
                "source": entry["source"],
                "page_score": page_score,
            })

    return pd.DataFrame(records)


def load_mineru_results(mineru_dir: str) -> pd.DataFrame:
    """Load MinerU results into a DataFrame."""
    mineru_folder = Path(mineru_dir)
    records: List[Dict[str, Any]] = []

    for filepath in sorted(mineru_folder.glob("*.json")):
        page = int(filepath.stem.replace("_extracted", ""))
        entries = parse_mineru_page(filepath)
        page_score = score_page_quality(entries)

        for entry in entries:
            records.append({
                "page": page,
                "column": entry["column"],
                "line": entry["line"],
                "source": entry["source"],
                "page_score": page_score,
            })

    return pd.DataFrame(records)


def _extract_html_cells(html_content: str) -> List[str]:
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html_content, "html.parser")
    cells: List[str] = []
    for td in soup.find_all("td"):
        txt = td.get_text(strip=True)
        if txt:
            cells.append(txt)
    return cells


def load_side_files(
    folder: str,
    filename_regex: re.Pattern[str] = DEFAULT_SIDE_REGEX,
) -> pd.DataFrame:
    """
    Load OCR results from side-split files (Deepseek/Qwen/Nanonet-style).

    The regex must capture (page, side) as groups 1 and 2.
    """
    root = Path(folder)
    records: List[Dict[str, Any]] = []

    for file in sorted(root.iterdir()):
        if not file.is_file():
            continue

        match = filename_regex.match(file.name)
        if not match:
            continue

        page = int(match.group(1))
        side = int(match.group(2))
        column = side + 1

        with open(file, encoding="utf-8") as f:
            lines = f.read().splitlines()

        for line in lines:
            if not line.strip():
                continue
            if "<table" in line and "</table>" in line:
                for cell in _extract_html_cells(line):
                    records.append({
                        "page": page,
                        "line": cell,
                        "source": "html_table",
                        "column": column,
                    })
            else:
                records.append({
                    "page": page,
                    "line": line.strip(),
                    "source": "text",
                    "column": column,
                })

    return pd.DataFrame(records)


def load_amazon_csv(path: str) -> pd.DataFrame:
    """Load Amazon OCR CSV as-is (expects page/column/line columns)."""
    df = pd.read_csv(path)
    _require_columns(df, {"page", "column", "line"}, "amazon_csv")
    return df


def _require_columns(df: pd.DataFrame, required: set[str], name: str) -> None:
    missing = required - set(df.columns)
    if missing:
        missing_list = ", ".join(sorted(missing))
        raise ValueError(f"{name} is missing required columns: {missing_list}")


def _parse_html_table(html: str, page: int, column: int) -> List[Dict[str, Any]]:
    entries: List[Dict[str, Any]] = []
    try:
        from bs4 import BeautifulSoup

        soup = BeautifulSoup(html, "html.parser")
        for td in soup.find_all("td"):
            raw = td.get_text(separator=" ", strip=True)
            if not raw:
                continue
            clean = _normalize_entry(raw)
            if clean:
                entries.append({"page": page, "line": clean, "column": column})
    except Exception:
        pass
    return entries


def _normalize_entry(txt: str) -> str:
    txt = txt.strip()
    if not txt:
        return ""

    txt = re.sub(r"[\u2013\u2014]", "—", txt)
    txt = re.sub(r"\s+", " ", txt)
    txt = re.sub(r",\s*,", ",", txt)
    txt = re.sub(r",\s*$", "", txt)
    txt = re.sub(r"^\s*,", "", txt)

    if not re.search(r"\d+—\d+", txt) and not re.search(r"—\d+", txt):
        return ""

    return txt


def clean_html_lines(df: pd.DataFrame) -> pd.DataFrame:
    """Parse HTML table lines into clean rows, preserving page/line/column."""
    _require_columns(df, {"page", "line", "column"}, "clean_html_lines")

    new_rows: List[Dict[str, Any]] = []
    for _, row in df.iterrows():
        page = int(row["page"])
        line = str(row["line"])
        column = row["column"]

        if not any(tag in line for tag in ["<table", "<tr", "<td"]):
            new_rows.append({"page": page, "line": line, "column": column})
            continue

        html_entries = _parse_html_table(line, page, column)
        if html_entries:
            new_rows.extend(html_entries)

    return pd.DataFrame(new_rows)


def explode_multi_entry_lines(df: pd.DataFrame) -> pd.DataFrame:
    """Split lines containing multiple entries until convergence."""
    def split_multi_entries(line: Any) -> List[Any]:
        if not isinstance(line, str):
            return [line]
        text = line.strip()
        name_pattern = r"([A-ZÅÄÖ][a-zåäöA-ZÅÄÖ\.\',\- ]+,[^,]+,[^,]+,[\d\s\-/\.]+)"
        matches = re.findall(name_pattern, text)
        if len(matches) > 1:
            return matches
        if "—" in text:
            parts = re.split(r"(?<=[\d])\s*—\s*(?=[A-ZÅÄÖ])", text)
            if len(parts) > 1:
                return [p.strip() for p in parts if p.strip()]
        if text.count(",") >= 4:
            complex_split = re.split(r"(?<=[\d])\s+(?=[A-ZÅÄÖ][a-zåäö]+,)", text)
            if len(complex_split) > 1:
                return [p.strip() for p in complex_split if p.strip()]
        return [text]

    result_df = df.copy()
    prev_len = -1
    curr_len = len(result_df)
    while prev_len != curr_len:
        result_df["line"] = result_df["line"].apply(split_multi_entries)
        result_df = result_df.explode("line").reset_index(drop=True)
        result_df["line"] = result_df["line"].apply(
            lambda x: re.sub(r"\s+", " ", x.strip()) if isinstance(x, str) else x
        )
        prev_len = curr_len
        curr_len = len(result_df)
    return result_df


def nvidia_explode_multi_entry_lines(df: pd.DataFrame) -> pd.DataFrame:
    COMPANY_SUFFIXES = [
        r"akt\.-?bol\.", r"A\.-B\.", r"fabr", r"fabrik", r"verk", r"bank",
        r"jarnv", r"järnv", r"qvarn", r"forsakr", r"försäkr",
        r"sprit", r"mejeri", r"import", r"export",
        r"rederi", r"bryggeri", r"tapet", r"tricot", r"ullspinn",
        r"gasverk", r"kraft", r"lys", r"smides", r"mek\.", r"mekan",
        r"fören", r"foren", r"bolag",
    ]
    company_suff = "|".join(COMPANY_SUFFIXES)

    def split_entries(line: Any) -> List[Any]:
        if not isinstance(line, str):
            return [line]

        text = line.strip()
        text = re.sub(r"\b(hstru|huttru)\b", "hustru", text, flags=re.IGNORECASE)

        hustru_split = re.split(
            r"(?<=[0-9])\s*(?:-|–)?\s*(?=(?:[Hh]ustru|[Ää]nkefru|[Ää]nka)\b)",
            text,
        )
        if len(hustru_split) > 1:
            return [p.strip(" -") for p in hustru_split if p.strip()]

        dash_split = re.split(
            r"(?<=[0-9])\s*(?:-|–)\s*(?=(?:[A-ZÅÄÖ]|d\'))",
            text,
        )
        if len(dash_split) > 1:
            return [p.strip() for p in dash_split if p.strip()]

        no_space = re.split(r"(?<=[0-9])(?=[A-ZÅÄÖ]|d\')", text)
        if len(no_space) > 1:
            return [p.strip() for p in no_space if p.strip()]

        name_split = re.split(
            r"(?<=[0-9])\s+(?=(?:[A-ZÅÄÖ]|d\')[a-zåäöA-ZÅÄÖ]+)",
            text,
        )
        if len(name_split) > 1:
            return [p.strip() for p in name_split if p.strip()]

        comp = re.split(
            rf"(?<=[0-9])\s+(?=[A-ZÅÄÖ].*?(?:{company_suff}))",
            text,
            flags=re.IGNORECASE,
        )
        if len(comp) > 1:
            return [c.strip() for c in comp if c.strip()]

        if text.count(",") >= 4:
            dbl = re.split(r"\s{2,}(?=[A-ZÅÄÖ])", text)
            if len(dbl) > 1:
                return [d.strip() for d in dbl if d.strip()]

        return [text]

    df2 = df.copy()
    df2["line"] = df2["line"].apply(split_entries)
    df2 = df2.explode("line")
    df2["line"] = df2["line"].str.replace(r"\s+", " ", regex=True).str.strip()
    df2 = df2[df2["line"] != ""]
    return df2.reset_index(drop=True)


def nvidia_explode_hustru_entries(df: pd.DataFrame) -> pd.DataFrame:
    """Split lines specifically on 'hustru' patterns (NVIDIA OCR patterns)."""
    def split_hustru(line: Any) -> List[Any]:
        if not isinstance(line, str):
            return [line]

        text = line.strip()
        text = re.sub(r"\b(hstru|huttru)\b", "hustru", text, flags=re.IGNORECASE)
        parts = re.split(
            r"(?<=[0-9])\s*(?:-|–)?\s*(?=(?:[Hh]ustru)\b)",
            text,
        )

        if len(parts) > 1:
            return [p.strip(" -") for p in parts if p.strip()]

        return [text]

    df_out = df.copy()
    df_out["line"] = df_out["line"].apply(split_hustru)
    df_out = df_out.explode("line")
    df_out["line"] = df_out["line"].str.replace(r"\s+", " ", regex=True).str.strip()
    df_out = df_out[df_out["line"] != ""]
    return df_out.reset_index(drop=True)


def process_df(df: pd.DataFrame) -> pd.DataFrame:
    """Apply standard splitting + row indexing to a raw OCR DataFrame."""
    _require_columns(df, {"page", "column", "line"}, "process_df")

    df = df.drop_duplicates(subset="line")

    p, q = 0, 1
    while p != q:
        p = len(df)
        df = nvidia_explode_multi_entry_lines(df)
        q = len(df)

    df["row"] = df.groupby(["page", "column"]).cumcount() + 1

    p, q = 0, 1
    while p != q:
        p = len(df)
        df = nvidia_explode_hustru_entries(df)
        q = len(df)

    df["row"] = df.groupby(["page", "column"]).cumcount() + 1
    return df


def sort_ocr_df(df: pd.DataFrame) -> pd.DataFrame:
    """Sort rows by page/column/row and reset index."""
    _require_columns(df, {"page", "column", "row"}, "sort_ocr_df")
    return df.sort_values(by=["page", "column", "row"]).reset_index(drop=True).copy()


def build_provider_frames(cfg: OcrInputConfig) -> Dict[str, pd.DataFrame]:
    """Load OCR inputs for all providers configured in cfg."""
    frames: Dict[str, pd.DataFrame] = {}

    if cfg.nvidia_nemotron_dir:
        frames["nvidia"] = load_nvidia_results(cfg.nvidia_nemotron_dir)
    if cfg.mineru_dir:
        frames["mineru"] = load_mineru_results(cfg.mineru_dir)
    if cfg.deepseek_dir:
        frames["deepseek"] = load_side_files(cfg.deepseek_dir, cfg.side_regex)
    if cfg.qwen_dir:
        frames["qwen"] = load_side_files(cfg.qwen_dir, cfg.side_regex)
    if cfg.nanonet_dir:
        frames["nano"] = load_side_files(cfg.nanonet_dir, cfg.side_regex)
    if cfg.amazon_csv:
        frames["amazon"] = load_amazon_csv(cfg.amazon_csv)

    return frames


def build_ocr_inputs(cfg: OcrInputConfig) -> Dict[str, pd.DataFrame]:
    """
    Build processed OCR input frames for each provider in cfg.
    Returns DataFrames with at least page/column/row/line columns.
    """
    raw = build_provider_frames(cfg)
    processed: Dict[str, pd.DataFrame] = {}
    for name, df in raw.items():
        processed[name] = sort_ocr_df(process_df(df))
    return processed


def write_ocr_inputs(frames: Dict[str, pd.DataFrame], out_dir: str) -> None:
    """Write per-provider OCR inputs to CSV in out_dir."""
    out_path = Path(out_dir)
    out_path.mkdir(parents=True, exist_ok=True)
    for name, df in frames.items():
        out_file = out_path / f"{name}.csv"
        df.to_csv(out_file, index=False, encoding="utf-8")
