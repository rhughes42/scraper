from __future__ import annotations

import argparse
import platform
import shutil
import subprocess
import sys
import time
from typing import Any, Dict, List, Optional

from experimental.cuda.run_experiments import ExperimentResult


def _try_run(cmd: List[str], timeout_s: int = 5) -> Optional[str]:
    try:
        completed = subprocess.run(
            cmd,
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            timeout=timeout_s,
            text=True,
        )
    except Exception:
        return None
    return (completed.stdout or "").strip() or None


def _detect_nvidia_smi() -> Dict[str, Any]:
    if not shutil.which("nvidia-smi"):
        return {"present": False}
    out = _try_run(["nvidia-smi", "-L"])
    return {"present": True, "output": out}


def _optional_cuda_indicators() -> Dict[str, Any]:
    indicators: Dict[str, Any] = {}

    try:
        import torch  # type: ignore

        indicators["torch_installed"] = True
        indicators["torch_cuda_available"] = bool(torch.cuda.is_available())
        if torch.cuda.is_available():
            indicators["torch_cuda_device_count"] = int(torch.cuda.device_count())
            indicators["torch_cuda_device_name0"] = str(torch.cuda.get_device_name(0))
    except Exception:
        indicators["torch_installed"] = False

    try:
        import numba  # noqa: F401
        from numba import cuda  # type: ignore

        indicators["numba_installed"] = True
        indicators["numba_cuda_available"] = bool(cuda.is_available())
        if cuda.is_available():
            try:
                dev = cuda.get_current_device()
                indicators["numba_cuda_device_name"] = str(getattr(dev, "name", "unknown"))
            except Exception:
                pass
    except Exception:
        indicators["numba_installed"] = False

    try:
        import cupy  # type: ignore

        indicators["cupy_installed"] = True
        try:
            indicators["cupy_device_count"] = int(cupy.cuda.runtime.getDeviceCount())
        except Exception:
            indicators["cupy_device_count"] = 0
    except Exception:
        indicators["cupy_installed"] = False

    return indicators


def run(_: argparse.Namespace) -> ExperimentResult:
    started = time.time()
    notes: List[str] = []

    nvidia = _detect_nvidia_smi()
    if nvidia.get("present") and not nvidia.get("output"):
        notes.append("`nvidia-smi` present but returned no output (driver/container issue?)")

    metrics: Dict[str, Any] = {
        "python": sys.version,
        "platform": platform.platform(),
        "nvidia_smi": nvidia,
        "cuda_indicators": _optional_cuda_indicators(),
    }

    finished = time.time()
    return ExperimentResult(
        experiment_id="exp001",
        status="ok",
        started_at=started,
        finished_at=finished,
        metrics=metrics,
        notes=notes,
    )

