from __future__ import annotations

import argparse
import importlib
import json
import os
import platform
import sys
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    # Allow running directly: `python experimental/cuda/run_experiments.py`
    sys.path.insert(0, str(_REPO_ROOT))


@dataclass
class ExperimentResult:
    experiment_id: str
    status: str  # ok | skipped | error
    started_at: float
    finished_at: float
    metrics: Dict[str, Any] = field(default_factory=dict)
    notes: List[str] = field(default_factory=list)

    @property
    def duration_s(self) -> float:
        return max(0.0, self.finished_at - self.started_at)


def _default_output_dir() -> Path:
    base = Path(os.environ.get("TMPDIR", "/tmp"))
    return base / "scraper_cuda_experiments"


def _load_experiment(
    module_path: str,
) -> Callable[[argparse.Namespace], ExperimentResult]:
    module = importlib.import_module(module_path)
    runner = getattr(module, "run", None)
    if not callable(runner):
        raise RuntimeError(f"{module_path}.run(args) is missing")
    return runner


EXPERIMENTS: Dict[str, str] = {
    "exp001": "experimental.cuda.experiments.exp001_system_info",
    "exp010": "experimental.cuda.experiments.exp010_simhash_numpy_vs_cupy",
    "exp020": (
        "experimental.cuda.experiments."
        "exp020_embeddings_sentence_transformers"
    ),
    "exp030": "experimental.cuda.experiments.exp030_ocr_optional",
}


def _iter_selected_experiments(selected: Optional[Iterable[str]]) -> List[str]:
    if not selected:
        return list(EXPERIMENTS.keys())
    missing = [exp_id for exp_id in selected if exp_id not in EXPERIMENTS]
    if missing:
        raise SystemExit(f"Unknown experiment id(s): {', '.join(missing)}")
    return list(selected)


def _write_results(output_dir: Path, results: List[ExperimentResult]) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    payload: Dict[str, Any] = {
        "generated_at": time.time(),
        "python": sys.version,
        "platform": platform.platform(),
        "results": [
            {
                **asdict(r),
                "duration_s": r.duration_s,
            }
            for r in results
        ],
    }
    out_path = output_dir / "results.json"
    out_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return out_path


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run experimental CUDA/NVIDIA benchmarks"
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List available experiments and exit",
    )
    parser.add_argument(
        "--only",
        action="append",
        default=[],
        help=(
            "Run only a specific experiment id (repeatable), "
            "e.g. --only exp010"
        ),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=_default_output_dir(),
        help=(
            "Directory for JSON results "
            "(default: /tmp/scraper_cuda_experiments)"
        ),
    )
    args = parser.parse_args(argv)

    if args.list:
        for exp_id, module_path in EXPERIMENTS.items():
            print(f"{exp_id}: {module_path}")
        return 0

    selected = _iter_selected_experiments(args.only or None)

    results: List[ExperimentResult] = []
    for exp_id in selected:
        module_path = EXPERIMENTS[exp_id]
        started = time.time()
        try:
            run_fn = _load_experiment(module_path)
            result = run_fn(args)
        except SystemExit:
            raise
        except (
            Exception
        ) as exc:  # noqa: BLE001 - experiments should never crash the runner
            finished = time.time()
            result = ExperimentResult(
                experiment_id=exp_id,
                status="error",
                started_at=started,
                finished_at=finished,
                metrics={"exception": repr(exc)},
                notes=[f"Module: {module_path}"],
            )
        else:
            if result.finished_at <= 0:
                result.finished_at = time.time()
            if result.started_at <= 0:
                result.started_at = started
        results.append(result)

    out_path = _write_results(args.output_dir, results)
    print(f"Wrote results: {out_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
