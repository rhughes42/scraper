from __future__ import annotations

import argparse
import time
from typing import Any, Dict, List

from experimental.cuda.run_experiments import ExperimentResult


def _detect_import(name: str) -> bool:
    try:
        __import__(name)
        return True
    except Exception:
        return False


def run(_: argparse.Namespace) -> ExperimentResult:
    started = time.time()

    checks: Dict[str, Any] = {
        "paddleocr_installed": _detect_import("paddleocr"),
        "paddle_installed": _detect_import("paddle"),
        "easyocr_installed": _detect_import("easyocr"),
        "torch_installed": _detect_import("torch"),
        "opencv_installed": _detect_import("cv2"),
    }

    notes: List[str] = [
        "OCR acceleration is backend-dependent (GPU support typically needs "
        "GPU-specific wheels).",
        "If this project adds OCR for scanned PDFs, GPU OCR is a prime "
        "candidate for large speedups.",
    ]

    finished = time.time()
    return ExperimentResult(
        experiment_id="exp030",
        status="skipped",
        started_at=started,
        finished_at=finished,
        metrics=checks,
        notes=notes,
    )
