# -*- coding: utf-8 -*-
"""
Run dashboard preparation step for six OCR providers.

Edit the input_paths below to point at your final_output_<provider>.csv files,
then run:
    python run_dashboard_prep.py
"""

from ocr_modules.dashboard_prep import DashboardPrepConfig, run_dashboard_prep


def main() -> None:
    input_paths = {
        "amazon": "final_output_amazon.csv",
        "nvidia": "final_output_nvidia.csv",
        "mineru": "final_output_mineru.csv",
        "deepseek": "final_output_deepseek.csv",
        "qwen": "final_output_qwen.csv",
        "nano": "final_output_nano.csv",
    }

    cfg = DashboardPrepConfig(
        input_paths=input_paths,
        output_dir="dashboard_outputs",
        n_workers=None,
        threshold=5,
    )

    run_dashboard_prep(cfg, cleaned_variant=True, apply_hustru=True)


if __name__ == "__main__":
    main()
