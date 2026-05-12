from __future__ import annotations

import argparse
import importlib
import random
import time
from typing import Any, Dict, List, Tuple

from experimental.cuda.run_experiments import ExperimentResult

_MASK64 = (1 << 64) - 1


def _splitmix64(x: int) -> int:
    x = (x + 0x9E3779B97F4A7C15) & _MASK64
    z = x
    z = (z ^ (z >> 30)) * 0xBF58476D1CE4E5B9 & _MASK64
    z = (z ^ (z >> 27)) * 0x94D049BB133111EB & _MASK64
    return z ^ (z >> 31)


def _make_sparse_docs(
    n_docs: int,
    n_features: int,
    active_per_doc: int,
    seed: int = 0,
) -> List[List[int]]:
    rng = random.Random(seed)
    features = list(range(n_features))
    docs: List[List[int]] = []
    for _ in range(n_docs):
        docs.append(rng.sample(features, active_per_doc))
    return docs


def _simhash_pure_python(
    docs: List[List[int]], feature_hash: List[int]
) -> Tuple[List[int], float]:
    t0 = time.perf_counter()
    out: List[int] = []
    for doc in docs:
        acc = [0] * 64
        for fid in doc:
            h = feature_hash[fid]
            for bit in range(64):
                if (h >> bit) & 1:
                    acc[bit] += 1
                else:
                    acc[bit] -= 1
        fp = 0
        for bit, score in enumerate(acc):
            if score >= 0:
                fp |= 1 << bit
        out.append(fp)
    dt = time.perf_counter() - t0
    return out, dt


def _optional_numpy():
    try:
        return importlib.import_module("numpy")
    except Exception:
        return None


def _optional_cupy():
    try:
        cupy = importlib.import_module("cupy")
        try:
            if int(cupy.cuda.runtime.getDeviceCount()) <= 0:
                return None
        except Exception:
            return None
        return cupy
    except Exception:
        return None


def _make_dense_features_numpy(
    np: Any,
    n_docs: int,
    n_features: int,
    density: float,
    seed: int = 0,
) -> Any:
    rng = np.random.default_rng(seed)
    return (rng.random((n_docs, n_features)) < density).astype(np.float32)


def _make_projection_numpy(
    np: Any,
    n_features: int,
    n_bits: int,
    seed: int = 0,
) -> Any:
    rng = np.random.default_rng(seed)
    return rng.choice(
        [-1.0, 1.0],
        size=(n_features, n_bits),
    ).astype(np.float32)


def _simhash_numpy(features: Any, proj: Any) -> float:
    t0 = time.perf_counter()
    scores = features @ proj
    _ = (scores >= 0).astype("uint8")
    dt = time.perf_counter() - t0
    return dt


def _simhash_cupy(cp: Any, features: Any, proj: Any) -> float:
    t0 = time.perf_counter()
    d_features = cp.asarray(features)
    d_proj = cp.asarray(proj)
    d_scores = d_features @ d_proj
    _ = (d_scores >= 0).astype(cp.uint8)
    cp.cuda.Stream.null.synchronize()
    dt = time.perf_counter() - t0
    return dt


def run(_: argparse.Namespace) -> ExperimentResult:
    started = time.time()
    notes: List[str] = [
        "Proxy for the batchable 'vector math' portion of a dedup pipeline.",
        "If featurization/tokenization stays on CPU, end-to-end speedups "
        "are smaller.",
    ]

    # Pure-Python baseline: sized to finish quickly without third-party deps.
    n_docs_py = 2_000
    n_features_py = 512
    active_per_doc = 24

    feature_hash = [_splitmix64(i) for i in range(n_features_py)]
    docs = _make_sparse_docs(
        n_docs=n_docs_py,
        n_features=n_features_py,
        active_per_doc=active_per_doc,
        seed=0,
    )
    _, py_s = _simhash_pure_python(docs, feature_hash)

    metrics: Dict[str, Any] = {
        "pure_python": {
            "n_docs": n_docs_py,
            "n_features": n_features_py,
            "active_per_doc": active_per_doc,
            "seconds": py_s,
            "docs_per_sec": n_docs_py / max(py_s, 1e-9),
        }
    }

    # Optional: numpy baseline and cupy GPU variant (if installed).
    np = _optional_numpy()
    if np is None:
        notes.append("NumPy not installed; skipping NumPy/CuPy variants.")
        finished = time.time()
        return ExperimentResult(
            experiment_id="exp010",
            status="ok",
            started_at=started,
            finished_at=finished,
            metrics=metrics,
            notes=notes,
        )

    n_docs_np = 50_000
    n_features_np = 1_024
    n_bits = 64
    density = 0.03

    features = _make_dense_features_numpy(
        np,
        n_docs=n_docs_np,
        n_features=n_features_np,
        density=density,
        seed=0,
    )
    proj = _make_projection_numpy(
        np,
        n_features=n_features_np,
        n_bits=n_bits,
        seed=0,
    )

    cpu_s = _simhash_numpy(features, proj)
    metrics["numpy"] = {
        "n_docs": n_docs_np,
        "n_features": n_features_np,
        "n_bits": n_bits,
        "density": density,
        "seconds": cpu_s,
        "docs_per_sec": n_docs_np / max(cpu_s, 1e-9),
    }

    cp = _optional_cupy()
    if cp is None:
        notes.append(
            "CuPy not installed or no CUDA device detected; GPU variant "
            "skipped."
        )
        finished = time.time()
        return ExperimentResult(
            experiment_id="exp010",
            status="ok",
            started_at=started,
            finished_at=finished,
            metrics=metrics,
            notes=notes,
        )

    gpu_s = _simhash_cupy(cp, features, proj)
    metrics["cupy"] = {
        "seconds_including_transfer": gpu_s,
        "docs_per_sec_including_transfer": n_docs_np / max(gpu_s, 1e-9),
        "speedup_including_transfer": cpu_s / max(gpu_s, 1e-9),
    }

    finished = time.time()
    return ExperimentResult(
        experiment_id="exp010",
        status="ok",
        started_at=started,
        finished_at=finished,
        metrics=metrics,
        notes=notes,
    )
