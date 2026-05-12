from __future__ import annotations

import argparse
import importlib
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
        torch = importlib.import_module("torch")
    except Exception:
        torch = None

    indicators["torch_installed"] = bool(torch)
    if torch is not None:
        try:
            cuda = torch.cuda
            indicators["torch_cuda_available"] = bool(cuda.is_available())
            if cuda.is_available():
                indicators["torch_cuda_device_count"] = int(
                    cuda.device_count()
                )
                indicators["torch_cuda_device_name0"] = str(
                    cuda.get_device_name(0)
                )
        except Exception:
            pass

    try:
        numba_cuda = importlib.import_module("numba.cuda")
    except Exception:
        numba_cuda = None

    indicators["numba_installed"] = bool(numba_cuda)
    if numba_cuda is not None:
        try:
            indicators["numba_cuda_available"] = bool(
                numba_cuda.is_available()
            )
            if numba_cuda.is_available():
                dev = numba_cuda.get_current_device()
                indicators["numba_cuda_device_name"] = str(
                    getattr(dev, "name", "unknown")
                )
        except Exception:
            pass

    try:
        cupy = importlib.import_module("cupy")
    except Exception:
        cupy = None

    indicators["cupy_installed"] = bool(cupy)
    if cupy is not None:
        try:
            indicators["cupy_device_count"] = int(
                cupy.cuda.runtime.getDeviceCount()
            )
        except Exception:
            indicators["cupy_device_count"] = 0

    return indicators


def run(_: argparse.Namespace) -> ExperimentResult:
    started = time.time()
    notes: List[str] = []

    nvidia = _detect_nvidia_smi()
    if nvidia.get("present") and not nvidia.get("output"):
        notes.append(
            "`nvidia-smi` present but returned no output "
            "(driver/container issue?)"
        )

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
