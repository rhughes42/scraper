# CUDA / NVIDIA Acceleration Experiments (Experimental)

This folder explores where CUDA-accelerated compute (and related NVIDIA components) could *theoretically* make this scraper substantially faster.

The scraper’s current critical path is primarily:

1. Network + remote server latency
2. Browser automation (Playwright) page load + DOM evaluation
3. Document processing (PDF generation, HTML parsing, metadata extraction)
4. Storage (hashing, compression, JSON serialization, I/O)

GPU acceleration will not meaningfully improve (1) and only sometimes improves (2). The biggest opportunities tend to appear when we introduce *batchable, compute-heavy workloads* such as OCR, embedding generation, classification/NER, deduplication, or large-scale text/table processing.

## Hypotheses

### H1: GPU-accelerated embeddings for smarter extraction/dedup
If we move from purely rule-based extraction to embedding-based similarity, clustering, and retrieval (e.g., “find similar docs”, “dedup near-duplicates”, “route to parser”), then local GPU inference can provide a large throughput increase.

- **Candidate NVIDIA components**: CUDA, TensorRT, cuBLAS, Tensor Cores, NVLink (multi-GPU)
- **Typical stack**: PyTorch CUDA, ONNX Runtime GPU, TensorRT-LLM (for larger models), or smaller embedding models
- **Success metric**: embeddings/sec and $/doc, plus end-to-end docs/min improvement when integrated
- **Experiment**: `exp020_embeddings_sentence_transformers.py`

### H2: GPU OCR for scanned PDFs and image-based pages
If a meaningful portion of scraped “PDFs” are scanned images (or if we add screenshot-based capture), GPU OCR can be the difference between “impossible at scale” and “fast enough”.

- **Candidate NVIDIA components**: CUDA, TensorRT, NVDEC (decode), Tensor Cores
- **Typical stack**: PaddleOCR (GPU), EasyOCR (GPU), or custom TensorRT OCR pipeline
- **Success metric**: pages/sec OCR at target accuracy
- **Experiment**: `exp030_ocr_optional.py`

### H3: GPU similarity search (FAISS-GPU) for massive dedup/retrieval
If we store embeddings for every document and perform similarity queries (for dedup, incremental updates, or cross-site linking), FAISS on GPU can accelerate both indexing and query throughput.

- **Candidate NVIDIA components**: CUDA, cuVS / RAPIDS (depending on approach), GPU memory
- **Success metric**: queries/sec at target recall; index build time
- **Experiment (future)**: not implemented yet (kept as a follow-up once embeddings are validated)

### H4: GPU-accelerated “feature math” (SimHash / MinHash / projection) for dedup
Even without large ML models, many dedup pipelines eventually reduce to large matrix operations that GPUs excel at (projection, hashing, approximate similarity).

- **Candidate NVIDIA components**: CUDA, cuBLAS
- **Success metric**: documents/sec for vectorized hashing/projection
- **Experiment**: `exp010_simhash_numpy_vs_cupy.py`

### H5: Non-GPU NVIDIA hardware could help at scale (network/storage offload)
In high-throughput deployments, NVIDIA DPUs (BlueField) and GPUDirect Storage (NVMe → GPU memory) *can* reduce CPU overhead and data movement, but only after the workload becomes compute-bound.

- **Candidate NVIDIA components**: BlueField DPU, GPUDirect Storage, NIC offloads
- **Success metric**: CPU% reduction at same docs/min
- **Experiment**: none (hardware-dependent; document-only for now)

## Experiments Included

Run everything via:

```bash
python -m experimental.cuda.run_experiments --list
python -m experimental.cuda.run_experiments
python -m experimental.cuda.run_experiments --only exp010 --only exp020

# Or run as a script:
python experimental/cuda/run_experiments.py --list
python experimental/cuda/run_experiments.py
python experimental/cuda/run_experiments.py --only exp010 --only exp020
```

Experiments are designed to be **safe on machines without GPUs**:
- If CUDA / optional libraries are unavailable, the experiment reports `skipped` instead of failing.
- Results are written to a temp folder by default (see `--output-dir`).

### `exp001_system_info.py`
Collects environment details and CUDA availability hints (via optional imports and `nvidia-smi`).

### `exp010_simhash_numpy_vs_cupy.py`
Computes a simple projection-based SimHash-like fingerprint over synthetic features:
- Baseline: pure Python (always available)
- CPU vector baseline: NumPy (optional)
- GPU candidate: CuPy (optional; requires CUDA + compatible wheel)

### `exp020_embeddings_sentence_transformers.py`
Measures embedding throughput using `sentence-transformers`:
- CPU baseline
- GPU candidate: PyTorch CUDA (if available)

### `exp030_ocr_optional.py`
Placeholder experiment that checks for common OCR GPU stacks and prints “what to install”.

## Installing Optional GPU Dependencies (Opt-In)

These experiments intentionally do **not** add GPU dependencies to the main project `requirements.txt`.

Use `experimental/cuda/requirements.txt` as a starting point and install only what you need for the experiments you want to run.

## Design Principles

- Keep the main scraper dependency set stable.
- Make experiments import-safe and runnable on CPU-only machines.
- Measure throughput in a way that is easy to relate back to `docs/min` in the main scraper.
- Prefer end-to-end pipeline experiments once a promising sub-primitive is identified.
