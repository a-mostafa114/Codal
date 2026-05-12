# -*- coding: utf-8 -*-
"""
Run OCR input generation for multiple providers and execute the main pipeline.

Usage:
    Set YEARS and PROVIDERS below, then run:
        python run_ocr_batch.py

    YEARS    – list of years to process, e.g. ["1913"] or ["1912", "1913"]
    PROVIDERS – list of providers to run, or None to run all configured providers.
                Available: "amazon", "mineru", "nvidia", "nano", "deepseek", "qwen", "glm"

Amazon input:
    final_set_1912.csv / final_set_1913.csv (only page/column/row/line columns are used).

Output layout:
    runs/<year>/ocr_inputs/<provider>.csv   – processed per-provider OCR input
    runs/<year>/outputs/<provider>/         – pipeline output per provider
"""

from dataclasses import replace
from pathlib import Path
from typing import List, Optional

from ocr_modules.ocr_input_builder import OcrInputConfig, build_ocr_inputs, write_ocr_inputs
from main import run_pipeline

# ── Run selection ────────────────────────────────────────────────────────
YEARS: List[str] = ["1912", "1913"]          # years to process
PROVIDERS: Optional[List[str]] = ["amazon", "mineru"]  # None = all in config

# ── Per-year full configurations ─────────────────────────────────────────
CONFIGS = {
    "1912": OcrInputConfig(
        amazon_csv="final_set_1912.csv",
        deepseek_dir="/home7/becomingsweden/output/taxeringskalender_1912/splits/deepseek_ocr/",
        qwen_dir="/home7/becomingsweden/output/taxeringskalender_1912/splits/qwen_ocr_results/",
        nanonet_dir="/home7/becomingsweden/output/taxeringskalender_1912/splits/nanonet_ocr_results/",
        nvidia_nemotron_dir="/home7/becomingsweden/data/taxeringskalender_1912/1912/nemotron_ocr_results/",
        mineru_dir="/home7/becomingsweden/data/taxeringskalender_1912/1912/miner_ocr_results/",
        out_dir="runs/1912",
    ),
    "1913": OcrInputConfig(
        amazon_csv="final_set_1913.csv",
        nanonet_dir="/home7/becomingsweden/output/taxeringskalender_1913/splits/nanonet_ocr_results/",
        nvidia_nemotron_dir="/home7/becomingsweden/data/taxeringskalender_1913/1913/nemotron_ocr_results/",
        mineru_dir="/home7/becomingsweden/data/taxeringskalender_1913/1913/miner_ocr_results/",
        glm_dir="/home7/becomingsweden/data/taxeringskalender_1913/1913/glm_ocr_results/",
        out_dir="runs/1913",
    ),
}

# provider name → OcrInputConfig field name
_PROVIDER_FIELDS = {
    "amazon":  "amazon_csv",
    "deepseek": "deepseek_dir",
    "qwen":    "qwen_dir",
    "nano":    "nanonet_dir",
    "nvidia":  "nvidia_nemotron_dir",
    "mineru":  "mineru_dir",
    "glm":     "glm_dir",
}


def _apply_provider_filter(cfg: OcrInputConfig, providers: List[str]) -> OcrInputConfig:
    """Return a copy of cfg with only the requested providers enabled."""
    overrides = {
        field: None
        for name, field in _PROVIDER_FIELDS.items()
        if name not in providers
    }
    return replace(cfg, **overrides)


def main() -> None:
    for year in YEARS:
        if year not in CONFIGS:
            raise ValueError(f"No config for year {year!r}. Add it to CONFIGS.")

        cfg = CONFIGS[year]
        if PROVIDERS is not None:
            cfg = _apply_provider_filter(cfg, PROVIDERS)

        print(f"\n{'='*60}")
        print(f"  Year: {year}  |  Providers: {PROVIDERS or 'all'}")
        print(f"{'='*60}")

        frames = build_ocr_inputs(cfg)
        ocr_out_dir = Path(cfg.out_dir) / "ocr_inputs"
        write_ocr_inputs(frames, str(ocr_out_dir))

        for name in sorted(frames.keys()):
            input_csv = ocr_out_dir / f"{name}.csv"
            provider_out_dir = Path(cfg.out_dir) / "outputs" / name
            run_pipeline(
                input_csv=str(input_csv),
                out_dir=str(provider_out_dir),
                output_prefix=f"final_output_{name}",
                checkpoint_prefix=name,
                report_dir=str(provider_out_dir / "reports"),
            )


if __name__ == "__main__":
    main()
