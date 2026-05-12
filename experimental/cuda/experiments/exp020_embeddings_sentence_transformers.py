from __future__ import annotations

import argparse
import time
from typing import Any, Dict, List, Optional

from experimental.cuda.run_experiments import ExperimentResult


def _optional_sentence_transformers():
    try:
        from sentence_transformers import SentenceTransformer  # type: ignore

        return SentenceTransformer
    except Exception:
        return None


def _optional_torch():
    try:
        import torch  # type: ignore

        return torch
    except Exception:
        return None


def _make_texts(n: int, approx_chars: int) -> List[str]:
    base = "Regulation (EU) 2016/679 on the protection of natural persons with regard to the processing of personal data."
    if approx_chars <= len(base):
        chunk = base[:approx_chars]
    else:
        repeats = (approx_chars // len(base)) + 1
        chunk = (base + " ") * repeats
        chunk = chunk[:approx_chars]
    return [f"[{i}] {chunk}" for i in range(n)]


def _time_encode(model, texts: List[str], device: str) -> float:
    t0 = time.perf_counter()
    model.encode(
        texts,
        device=device,
        batch_size=64,
        normalize_embeddings=False,
        show_progress_bar=False,
    )
    return time.perf_counter() - t0


def run(_: argparse.Namespace) -> ExperimentResult:
    started = time.time()
    SentenceTransformer = _optional_sentence_transformers()
    if SentenceTransformer is None:
        finished = time.time()
        return ExperimentResult(
            experiment_id="exp020",
            status="skipped",
            started_at=started,
            finished_at=finished,
            metrics={"reason": "sentence-transformers not installed"},
            notes=[
                "Install `sentence-transformers` (and `torch`) to enable this benchmark.",
                "On GPU machines, install a CUDA-capable PyTorch build for meaningful results.",
            ],
        )

    torch = _optional_torch()
    cuda_available = bool(torch and torch.cuda.is_available())

    texts = _make_texts(n=5_000, approx_chars=600)

    # Model selection: small & common; will download on first run.
    model_name = "sentence-transformers/all-MiniLM-L6-v2"
    model = SentenceTransformer(model_name)

    cpu_s = _time_encode(model, texts, device="cpu")

    metrics: Dict[str, Any] = {
        "model": model_name,
        "n_texts": len(texts),
        "approx_chars_per_text": 600,
        "cpu_seconds": cpu_s,
        "cpu_texts_per_sec": len(texts) / max(cpu_s, 1e-9),
        "cuda_available": cuda_available,
    }

    notes: List[str] = [
        "This is a proxy for embedding-based dedup/routing/extraction steps that are compute-heavy and batchable.",
        "End-to-end impact depends on how much time is currently spent in parsing vs network/browser.",
    ]

    if cuda_available:
        gpu_s = _time_encode(model, texts, device="cuda")
        metrics.update(
            {
                "gpu_seconds": gpu_s,
                "gpu_texts_per_sec": len(texts) / max(gpu_s, 1e-9),
                "speedup": cpu_s / max(gpu_s, 1e-9),
                "gpu_name0": str(torch.cuda.get_device_name(0)) if torch else None,
            }
        )
    else:
        notes.append("CUDA not available to torch; only CPU timing recorded.")

    finished = time.time()
    return ExperimentResult(
        experiment_id="exp020",
        status="ok" if cuda_available else "skipped",
        started_at=started,
        finished_at=finished,
        metrics=metrics,
        notes=notes,
    )

