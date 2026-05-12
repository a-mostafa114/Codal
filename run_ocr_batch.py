# -*- coding: utf-8 -*-
"""
Run OCR input generation for multiple providers and execute the main pipeline.

Edit the paths in `cfg` below to match your environment, then run:
    python run_ocr_batch.py
"""

from pathlib import Path

from ocr_modules.ocr_input_builder import OcrInputConfig, build_ocr_inputs, write_ocr_inputs
from main import run_pipeline


def main() -> None:
    cfg = OcrInputConfig(
        amazon_csv="voted_lines_1913.csv",
        # deepseek_dir="/home7/becomingsweden/output/taxeringskalender_1912/splits/deepseek_ocr/",
        # qwen_dir="/home7/becomingsweden/output/taxeringskalender_1912/splits/qwen_ocr_results/",
        # nanonet_dir="/home7/becomingsweden/output/taxeringskalender_1913/splits/nanonet_ocr_results/",
        # nvidia_nemotron_dir="/home7/becomingsweden/data/taxeringskalender_1913/1913/nemotron_ocr_results/",
        # mineru_dir="/home7/becomingsweden/data/taxeringskalender_1913/1913/miner_ocr_results/",
        # glm_dir="/home7/becomingsweden/data/taxeringskalender_1913/1913/glm_ocr_results/",
        out_dir="runs/voted_lines_1913",
    )

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
