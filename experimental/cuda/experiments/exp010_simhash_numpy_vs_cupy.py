from __future__ import annotations

import argparse
import time
from typing import Any, Dict, List, Optional, Tuple

import numpy as np

from experimental.cuda.run_experiments import ExperimentResult


def _maybe_import_cupy():
    try:
        import cupy as cp  # type: ignore

        if int(cp.cuda.runtime.getDeviceCount()) <= 0:
            return None
        return cp
    except Exception:
        return None


def _make_features(n_docs: int, n_features: int, density: float, seed: int = 0) -> np.ndarray:
    rng = np.random.default_rng(seed)
    features = (rng.random((n_docs, n_features)) < density).astype(np.float32)
    return features


def _make_projection(n_features: int, n_bits: int, seed: int = 0) -> np.ndarray:
    rng = np.random.default_rng(seed)
    # Random ±1 projection matrix.
    proj = rng.choice([-1.0, 1.0], size=(n_features, n_bits)).astype(np.float32)
    return proj


def _simhash_numpy(features: np.ndarray, proj: np.ndarray) -> Tuple[np.ndarray, float]:
    t0 = time.perf_counter()
    scores = features @ proj
    bits = (scores >= 0).astype(np.uint8)
    dt = time.perf_counter() - t0
    return bits, dt


def _simhash_cupy(cp, features: np.ndarray, proj: np.ndarray) -> Tuple[Any, float]:
    # Transfer to GPU once; time includes transfer + compute (a realistic ingestion cost).
    t0 = time.perf_counter()
    d_features = cp.asarray(features)
    d_proj = cp.asarray(proj)
    d_scores = d_features @ d_proj
    d_bits = (d_scores >= 0).astype(cp.uint8)
    # Ensure completion for correct timing.
    cp.cuda.Stream.null.synchronize()
    dt = time.perf_counter() - t0
    return d_bits, dt


def run(_: argparse.Namespace) -> ExperimentResult:
    started = time.time()

    # Keep sizes moderate so the script can run on laptops; adjust as needed.
    n_docs = 50_000
    n_features = 1_024
    n_bits = 64
    density = 0.03

    features = _make_features(n_docs=n_docs, n_features=n_features, density=density)
    proj = _make_projection(n_features=n_features, n_bits=n_bits)

    _, cpu_s = _simhash_numpy(features, proj)

    cp = _maybe_import_cupy()
    if cp is None:
        finished = time.time()
        return ExperimentResult(
            experiment_id="exp010",
            status="skipped",
            started_at=started,
            finished_at=finished,
            metrics={
                "reason": "cupy not installed or no CUDA device detected",
                "cpu_seconds": cpu_s,
                "cpu_docs_per_sec": n_docs / max(cpu_s, 1e-9),
                "shape": {"n_docs": n_docs, "n_features": n_features, "n_bits": n_bits},
            },
            notes=["Install a CUDA-matching CuPy wheel to enable the GPU variant."],
        )

    _, gpu_s = _simhash_cupy(cp, features, proj)

    finished = time.time()
    metrics: Dict[str, Any] = {
        "shape": {"n_docs": n_docs, "n_features": n_features, "n_bits": n_bits},
        "cpu_seconds": cpu_s,
        "cpu_docs_per_sec": n_docs / max(cpu_s, 1e-9),
        "gpu_seconds_including_transfer": gpu_s,
        "gpu_docs_per_sec_including_transfer": n_docs / max(gpu_s, 1e-9),
        "speedup_including_transfer": cpu_s / max(gpu_s, 1e-9),
    }
    return ExperimentResult(
        experiment_id="exp010",
        status="ok",
        started_at=started,
        finished_at=finished,
        metrics=metrics,
        notes=[
            "This models the 'math' portion of a dedup pipeline after text has already been featurized.",
            "If featurization/tokenization stays on CPU, end-to-end speedups will be smaller.",
        ],
    )

