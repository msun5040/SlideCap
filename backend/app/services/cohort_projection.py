"""
Cohort-wide dimensionality reduction over patch embeddings.

Projects **every patch of every slide** in a cohort into 2D. No sampling — a
projection built from part of a WSI isn't a useful measure of how that slide's
tissue clusters. That constraint is what shapes everything here, because the
naive version doesn't fit in memory: 100 slides x ~10k patches x 1536 float32 is
~6 GB before umap-learn builds its kNN graph.

The way out is to never hold the full-dimensional matrix:

  1. Read each slide's features in HDF5 slices, not whole (uni.py's _read_features
     does `f[key][:]`, which is fine for one slide and fatal for a hundred).
  2. Fit IncrementalPCA 1536 -> ~50 by partial_fit over those chunks. Every patch
     contributes to the fit and every patch gets transformed, so "all patches
     participate" holds literally; this is a preprocessing step, not a sample.
  3. Write the reduced matrix to a np.memmap on local disk. At 1M x 50 x 4B that's
     200 MB paged from disk rather than resident. This is the step that makes the
     whole thing tractable.
  4. Run UMAP on the memmap. PCA-first is standard practice and improves kNN
     quality as well as cost.

  4b. Or run t-SNE (openTSNE, FFT-accelerated) on the same memmap. Slower than
     UMAP at cohort scale and it doesn't preserve global layout as well, but it
     separates local structure sharply.

`method="pca"` short-circuits after step 2 — the first two components are already
the answer, which makes it a cheap smoke test on a large cohort before committing
to a UMAP or t-SNE run.

Because UMAP and t-SNE both start with the PCA steps, their progress first reads
"PCA pre-reduction" — stage labels say which step of which method is running.

Group labels are deliberately absent from this module. A projection is computed
before any label is read, and groups are joined in at plot time purely to colour
points — so re-labelling a cohort never invalidates a projection.
"""
from __future__ import annotations

import json
import struct
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple

# Artifact container magic + version. Bump the version if the layout changes so a
# stale artifact is rejected rather than silently misread as the new shape.
MAGIC = b"SCPROJ01"

# How many patch rows to pull from HDF5 at a time. Governs peak memory alongside
# the feature dimension: 8192 x 1536 x 4B ~= 50 MB per chunk.
DEFAULT_CHUNK_ROWS = 8192

# Target dimensionality for the PCA pre-reduction feeding UMAP.
DEFAULT_PCA_DIM = 50


class ProjectionError(RuntimeError):
    """Raised with a message intended to be shown to the user."""


def _require(module: str, pip_name: Optional[str] = None):
    """
    Import a heavy optional dependency, or fail with something actionable.

    uni.py guards h5py this way but lets sklearn/umap raise bare ImportError,
    which the render endpoint doesn't catch and so surfaces as an opaque 500.
    Everything here goes through this helper instead.
    """
    try:
        return __import__(module)
    except ImportError as e:
        raise ProjectionError(
            f"{module} is required for cohort projections. Install with "
            f"`pip install {pip_name or module}` in the backend's Python env."
        ) from e


@dataclass
class SlideSource:
    """One slide's inputs, already resolved to real files by the caller."""
    slide_hash: str
    features_h5: Path
    patches_h5: Path
    display_name: str = ""
    # Filled in during the scan phase.
    n_patches: int = 0
    patch_size: int = 256
    patch_level: int = 0


@dataclass
class ProjectionResult:
    artifact_path: Path
    point_count: int
    feature_dim: int
    method: str
    slide_hashes: List[str] = field(default_factory=list)
    elapsed_seconds: float = 0.0
    peak_chunk_rows: int = 0
    # The PCA-reduced matrix, kept for clustering (see keep_reduced()).
    reduced_path: Optional[Path] = None
    reduced_dim: int = 0


ProgressFn = Callable[[int, str], None]


def _noop_progress(pct: int, stage: str) -> None:
    pass


# ──────────────────────────────────────────────────────────────────────
# HDF5 access — chunked, and tolerant of the dataset-name variation that
# uni.py already handles for the single-slide path.
# ──────────────────────────────────────────────────────────────────────

_FEATURE_KEYS = ("features", "embeddings", "feats")


def _open_features(h5py_mod, path: Path):
    f = h5py_mod.File(str(path), "r")
    for key in _FEATURE_KEYS:
        if key in f:
            return f, key
    keys = list(f.keys())
    f.close()
    raise ProjectionError(
        f"No features dataset in {path.name}. Looked for {', '.join(_FEATURE_KEYS)}; "
        f"found {keys}."
    )


def _coords_key(f) -> str:
    if "coords" in f:
        return "coords"
    for k in f.keys():
        if "coord" in k.lower():
            return k
    raise ProjectionError(
        f"No patch-coordinate dataset found. Top-level keys: {list(f.keys())}"
    )


def scan_sources(sources: List[SlideSource], progress: ProgressFn = _noop_progress
                 ) -> Tuple[int, int]:
    """
    Read shapes and patch metadata without loading any feature data.

    Returns (total_patches, feature_dim). Raises if slides disagree on
    dimensionality — concatenating those would be meaningless, and it's much
    better to say so up front than to fail deep inside a 20-minute fit.
    """
    h5py = _require("h5py")
    total = 0
    dim: Optional[int] = None

    for i, src in enumerate(sources):
        f, key = _open_features(h5py, src.features_h5)
        try:
            shape = f[key].shape
            if len(shape) != 2:
                raise ProjectionError(
                    f"{src.features_h5.name}: expected a 2-D features dataset, got shape {shape}."
                )
            n_feat, d = int(shape[0]), int(shape[1])
        finally:
            f.close()

        if dim is None:
            dim = d
        elif d != dim:
            raise ProjectionError(
                f"Feature dimension mismatch: {src.features_h5.name} has {d} dims but an "
                f"earlier slide had {dim}. All slides must come from the same analysis "
                f"configuration."
            )

        with h5py.File(str(src.patches_h5), "r") as pf:
            ck = _coords_key(pf)
            n_coord = int(pf[ck].shape[0])
            attrs = pf[ck].attrs
            # patch_size_level0 is the level-0 footprint (e.g. 512); patch_size is
            # the model input (256). Overlay boxes and patch crops are drawn in
            # level-0 pixels, so the former is what we want when present.
            ps = attrs.get("patch_size_level0", attrs.get("patch_size", 256))
            src.patch_size = int(ps)
            src.patch_level = int(attrs.get("patch_level", 0))

        # uni.py truncates to the shorter of the two rather than erroring; match
        # that so a cohort isn't blocked by one slightly-mismatched slide.
        n = min(n_feat, n_coord)
        if n_feat != n_coord:
            print(f"[cohort-projection] {src.slide_hash[:12]}: features n={n_feat} but "
                  f"coords n={n_coord}; using {n}")
        src.n_patches = n
        total += n
        progress(int(5 * (i + 1) / max(1, len(sources))), f"Scanning slides ({i+1}/{len(sources)})")

    if dim is None:
        raise ProjectionError("No slides to project.")
    if total == 0:
        raise ProjectionError("The selected slides contain no patches.")
    return total, dim


def _iter_feature_chunks(src: SlideSource, chunk_rows: int):
    """Yield (start, array) slices of one slide's features, capped at n_patches."""
    h5py = _require("h5py")
    np = _require("numpy")
    f, key = _open_features(h5py, src.features_h5)
    try:
        ds = f[key]
        for start in range(0, src.n_patches, chunk_rows):
            stop = min(start + chunk_rows, src.n_patches)
            yield start, np.asarray(ds[start:stop], dtype=np.float32)
    finally:
        f.close()


def _read_coords(src: SlideSource):
    """(n_patches, 2) int32 level-0 top-left coordinates."""
    h5py = _require("h5py")
    np = _require("numpy")
    with h5py.File(str(src.patches_h5), "r") as f:
        ck = _coords_key(f)
        arr = np.asarray(f[ck][: src.n_patches], dtype=np.int64)
    if arr.ndim != 2 or arr.shape[1] != 2:
        raise ProjectionError(
            f"{src.patches_h5.name}: expected (N, 2) coordinates, got shape {arr.shape}."
        )
    return arr.astype(np.int32)


# ──────────────────────────────────────────────────────────────────────
# The pipeline
# ──────────────────────────────────────────────────────────────────────

def _fit_incremental_pca(sources, total_n, dim, pca_dim, chunk_rows, progress):
    """Pass 1: fit IncrementalPCA across every patch, holding one chunk at a time."""
    _require("sklearn", "scikit-learn")
    from sklearn.decomposition import IncrementalPCA

    # IncrementalPCA needs at least n_components rows per partial_fit call, and
    # can't produce more components than min(n_samples, n_features).
    n_components = min(pca_dim, dim, total_n)
    batch = max(chunk_rows, n_components)

    ipca = IncrementalPCA(n_components=n_components, batch_size=batch)
    seen = 0
    pending = None
    np = _require("numpy")

    for src in sources:
        for _, chunk in _iter_feature_chunks(src, batch):
            # A tail chunk smaller than n_components would raise, so carry it
            # forward and merge it with the next one.
            if pending is not None:
                chunk = np.vstack([pending, chunk])
                pending = None
            if chunk.shape[0] < n_components:
                pending = chunk
                continue
            ipca.partial_fit(chunk)
            seen += chunk.shape[0]
            progress(5 + int(40 * seen / max(1, total_n)),
                     f"Fitting PCA ({seen:,}/{total_n:,} patches)")

    if pending is not None and pending.shape[0] >= n_components:
        ipca.partial_fit(pending)
    elif pending is not None:
        # Fewer than n_components rows left over across the whole cohort: they
        # still get transformed below, they just can't refine the fit.
        print(f"[cohort-projection] {pending.shape[0]} trailing rows too few to "
              f"partial_fit with n_components={n_components}; skipped for fitting only")

    return ipca, n_components


def _transform_to_memmap(sources, ipca, total_n, n_components, chunk_rows, work_dir, progress):
    """Pass 2: project every patch into PCA space, writing straight to a memmap."""
    np = _require("numpy")
    work_dir.mkdir(parents=True, exist_ok=True)
    mm_path = work_dir / "reduced.f32"
    mm = np.memmap(mm_path, dtype=np.float32, mode="w+", shape=(total_n, n_components))

    row = 0
    for src in sources:
        for _, chunk in _iter_feature_chunks(src, chunk_rows):
            out = ipca.transform(chunk)
            mm[row:row + out.shape[0]] = out.astype(np.float32)
            row += out.shape[0]
            progress(45 + int(20 * row / max(1, total_n)),
                     f"Reducing ({row:,}/{total_n:,} patches)")
    mm.flush()
    if row != total_n:
        raise ProjectionError(
            f"Internal error: wrote {row} reduced rows but expected {total_n}."
        )
    return mm, mm_path


def _run_umap(reduced, params, progress):
    """
    UMAP on the reduced matrix.

    Note the seeding trade-off, measured on an 8-core box: passing random_state
    makes umap-learn fall back to a single thread, which cost ~4x across
    20k-100k points (9.3s vs 40.5s at 100k). The per-slide renderer seeds because
    its inputs are small and it re-runs on demand; a cohort projection is
    computed once and kept as an artifact, so throughput matters more than
    reproducing the same fit twice. Seeding is therefore opt-in: pass
    `random_state` explicitly to get a deterministic (slower) run.

    Whatever is chosen gets recorded in the artifact header, so a projection can
    always say how it was produced.
    """
    umap = _require("umap", "umap-learn")
    np = _require("numpy")

    n = reduced.shape[0]
    n_neighbors = int(params.get("n_neighbors", 15))
    # umap requires n_neighbors < n_samples; uni.py clamps the same way.
    n_neighbors = max(2, min(n_neighbors, n - 1))

    kwargs = dict(
        n_components=2,
        n_neighbors=n_neighbors,
        min_dist=float(params.get("min_dist", 0.1)),
        metric=params.get("metric", "cosine"),
        low_memory=True,
        verbose=False,
    )
    seed = params.get("random_state")
    if seed is not None:
        kwargs["random_state"] = int(seed)
        note = f"deterministic, single-threaded (random_state={int(seed)})"
    else:
        kwargs["n_jobs"] = -1
        note = "parallel"
    # Reflect what was actually used back into params for the artifact header.
    params["random_state"] = int(seed) if seed is not None else None
    params["n_neighbors"] = n_neighbors

    progress(66, f"Building neighbour graph — {n:,} points, {note}. This is the slow part.")
    reducer = umap.UMAP(**kwargs)
    # np.asarray materializes the memmap; UMAP needs random access anyway, and at
    # 50 dims this is the 200 MB figure the module docstring quotes, not 6 GB.
    coords = reducer.fit_transform(np.asarray(reduced))
    progress(92, "Projection complete")
    return np.asarray(coords, dtype=np.float32)


def _run_tsne(reduced, params, progress):
    """
    t-SNE on the reduced matrix via openTSNE.

    scikit-learn's TSNE was measured and rejected: 166s at 50k points against
    openTSNE's 39s, and it scales far worse beyond that. openTSNE uses approximate
    nearest neighbours for the affinities and FFT-interpolated gradients, which is
    what keeps a cohort-sized run in minutes rather than hours. Still markedly
    slower than UMAP at the same size.

    Like UMAP, seeding is opt-in and whatever was used is recorded in params.
    """
    openTSNE = _require("openTSNE", "openTSNE")
    np = _require("numpy")

    n = reduced.shape[0]
    if n < 4:
        raise ProjectionError("t-SNE needs at least 4 patches.")
    # openTSNE needs 3*perplexity < n for its neighbour search.
    perplexity = float(params.get("perplexity", 30))
    perplexity = max(2.0, min(perplexity, (n - 1) / 3.0))
    early_iter = int(params.get("early_exaggeration_iter", 250))
    n_iter = int(params.get("n_iter", 500))
    seed = params.get("random_state")
    total = early_iter + n_iter

    # openTSNE restarts the iteration count for each optimisation phase, so
    # carry an offset to report one continuous count.
    state = {"offset": 0, "last": 0}

    def on_iter(iteration, error, embedding):
        if iteration < state["last"]:
            state["offset"] += state["last"]
        state["last"] = iteration
        done = min(total, state["offset"] + iteration)
        progress(70 + int(22 * done / max(1, total)),
                 f"Step 3/3 · t-SNE · optimising layout ({done}/{total} iterations)")
        return False  # never stop early

    params["perplexity"] = perplexity
    params["n_iter"] = n_iter
    params["early_exaggeration_iter"] = early_iter
    params["random_state"] = int(seed) if seed is not None else None

    progress(66, f"Step 3/3 · t-SNE · computing neighbour affinities for {n:,} points. "
                 f"This is the slow part.")
    tsne = openTSNE.TSNE(
        n_components=2,
        perplexity=perplexity,
        n_iter=n_iter,
        early_exaggeration_iter=early_iter,
        initialization="pca",
        metric="euclidean",
        neighbors="auto",
        negative_gradient_method="fft",
        callbacks=on_iter,
        callbacks_every_iters=25,
        random_state=int(seed) if seed is not None else None,
        n_jobs=-1,
        verbose=False,
    )
    emb = tsne.fit(np.ascontiguousarray(np.asarray(reduced), dtype=np.float32))
    progress(92, "Projection complete")
    return np.asarray(emb, dtype=np.float32)


_METHOD_LABELS = {"umap": "UMAP", "tsne": "t-SNE", "pca": "PCA"}


def write_artifact(path: Path, xy, slide_idx, patch_x, patch_y, sources, method, params,
                   feature_dim: int) -> None:
    """
    Columnar binary container:

        magic (8B) | header_len (uint32 LE) | JSON header | raw column bytes

    Columnar rather than the per-point JSON objects the single-slide renderer
    emits (uni.py:236) — at a million patches that shape is hundreds of MB and
    has to be parsed before anything can draw. This reads straight into typed
    arrays in the browser.
    """
    np = _require("numpy")
    path.parent.mkdir(parents=True, exist_ok=True)

    columns = [
        ("x", xy[:, 0].astype(np.float32)),
        ("y", xy[:, 1].astype(np.float32)),
        ("slide_idx", slide_idx),
        ("patch_x", patch_x.astype(np.int32)),
        ("patch_y", patch_y.astype(np.int32)),
    ]

    # slide_offsets lets the viewer restrict a WSI-click hit test to one slide's
    # index range instead of scanning every point in the cohort.
    offsets, run = [], 0
    for src in sources:
        offsets.append({"start": run, "count": src.n_patches})
        run += src.n_patches

    meta, offset = [], 0
    for name, arr in columns:
        meta.append({"name": name, "dtype": arr.dtype.name,
                     "offset": offset, "bytes": int(arr.nbytes)})
        offset += int(arr.nbytes)

    header = {
        "version": 1,
        "method": method,
        "params": params,
        "point_count": int(xy.shape[0]),
        "feature_dim": feature_dim,
        "columns": meta,
        "slides": [
            {
                "slide_hash": s.slide_hash,
                "display_name": s.display_name,
                "n_patches": s.n_patches,
                "patch_size": s.patch_size,
                "patch_level": s.patch_level,
                "start": offsets[i]["start"],
            }
            for i, s in enumerate(sources)
        ],
    }
    header_bytes = json.dumps(header).encode("utf-8")
    # Pad so the column block starts 8-byte aligned. Typed-array views in the
    # browser require the byte offset to be a multiple of the element size, and
    # the header length is otherwise arbitrary. Trailing spaces stay valid JSON.
    pad = (-(len(MAGIC) + 4 + len(header_bytes))) % 8
    header_bytes += b" " * pad

    tmp = path.with_suffix(path.suffix + ".tmp")
    with open(tmp, "wb") as fh:
        fh.write(MAGIC)
        fh.write(struct.pack("<I", len(header_bytes)))
        fh.write(header_bytes)
        for _, arr in columns:
            fh.write(arr.tobytes(order="C"))
    # Atomic swap so a reader never sees a half-written artifact.
    tmp.replace(path)


def read_artifact_header(path: Path) -> dict:
    """Parse just the header — used by tests and by the points endpoint."""
    with open(path, "rb") as fh:
        magic = fh.read(8)
        if magic != MAGIC:
            raise ProjectionError(
                f"{path.name} is not a SlideCap projection artifact (bad magic {magic!r})."
            )
        (hlen,) = struct.unpack("<I", fh.read(4))
        return json.loads(fh.read(hlen).decode("utf-8"))


def keep_reduced(mm_path: Path, dest: Path) -> bool:
    """
    Move the work-dir memmap to its permanent home. Returns False (and logs) on
    failure rather than raising: losing the matrix only means a later clustering
    rebuilds it, which is no reason to fail a finished projection.
    """
    import gc
    import os
    import shutil
    gc.collect()  # drop any lingering ndarray views of the mapping (Windows)
    try:
        dest.parent.mkdir(parents=True, exist_ok=True)
        try:
            os.replace(mm_path, dest)
        except OSError:
            shutil.copyfile(mm_path, dest)
            try:
                mm_path.unlink()
            except OSError:
                pass
        return True
    except OSError as e:
        print(f"[cohort-projection] could not keep reduced matrix at {dest}: {e}")
        return False


def rebuild_reduced(
    sources: List[SlideSource],
    dest: Path,
    work_dir: Path,
    expected_points: Optional[int] = None,
    chunk_rows: int = DEFAULT_CHUNK_ROWS,
    pca_dim: int = DEFAULT_PCA_DIM,
    progress: ProgressFn = _noop_progress,
) -> Tuple[int, int]:
    """
    Recreate the PCA-reduced matrix for a projection that predates keeping it.

    `sources` must be in the projection's pinned slide order: row i of the result
    has to be row i of the artifact, or every label lands on the wrong patch. The
    refit PCA basis can differ slightly from the original — immaterial for
    clustering — but the row count must match exactly, and is checked.

    Returns (n_points, n_components).
    """
    total_n, dim = scan_sources(sources, progress)
    if expected_points is not None and total_n != expected_points:
        raise ProjectionError(
            f"The slides' feature files now hold {total_n:,} patches, but this projection "
            f"was built from {expected_points:,}. The analysis output changed since the "
            f"projection ran; re-run the projection before clustering it."
        )
    ipca, n_components = _fit_incremental_pca(sources, total_n, dim, pca_dim, chunk_rows, progress)
    reduced, mm_path = _transform_to_memmap(
        sources, ipca, total_n, n_components, chunk_rows, work_dir, progress)
    reduced.flush()
    del reduced
    if not keep_reduced(mm_path, dest):
        raise ProjectionError(f"Could not write the reduced matrix to {dest}.")
    return int(total_n), int(n_components)


def build_projection(
    sources: List[SlideSource],
    out_path: Path,
    work_dir: Path,
    method: str = "umap",
    params: Optional[Dict] = None,
    chunk_rows: int = DEFAULT_CHUNK_ROWS,
    pca_dim: int = DEFAULT_PCA_DIM,
    progress: ProgressFn = _noop_progress,
    reduced_out: Optional[Path] = None,
) -> ProjectionResult:
    """
    Project every patch of every source slide into 2D and write the artifact.

    `work_dir` holds the intermediate memmap; point it at local disk, never the
    network drive. When `reduced_out` is given, the PCA-reduced matrix is kept
    there on success — clustering runs on it rather than on the 2D coordinates,
    which for UMAP would mean clustering the embedding's distortions.
    """
    np = _require("numpy")
    params = dict(params or {})
    started = time.time()

    if method not in _METHOD_LABELS:
        raise ProjectionError(f"Unknown method {method!r}. Supported: umap, tsne, pca.")

    # UMAP and t-SNE both run PCA first, so without this their progress reads
    # "Fitting PCA" for the first half and looks like the wrong method is running.
    label = _METHOD_LABELS[method]
    raw_progress = progress

    def progress(pct: int, stage: str):
        if stage.startswith("Step ") or pct >= 93:
            raw_progress(pct, stage)
        elif method == "pca":
            raw_progress(pct, f"PCA · {stage}")
        elif pct < 66:
            raw_progress(pct, f"Step {1 if pct < 45 else 2}/3 · PCA pre-reduction for {label} · {stage}")
        else:
            raw_progress(pct, f"Step 3/3 · {label} · {stage}")

    total_n, dim = scan_sources(sources, progress)
    progress(5, f"{total_n:,} patches across {len(sources)} slides")

    ipca, n_components = _fit_incremental_pca(
        sources, total_n, dim, pca_dim, chunk_rows, progress)
    reduced, mm_path = _transform_to_memmap(
        sources, ipca, total_n, n_components, chunk_rows, work_dir, progress)

    kept = False
    try:
        if method == "pca":
            # Components come out ordered by explained variance, so the first two
            # columns of the reduction already are the PCA projection. An explicit
            # copy, so no view keeps the memmap's file handle open.
            progress(70, "Taking first two principal components")
            xy = np.array(reduced[:, :2], dtype=np.float32, copy=True)
        elif method == "tsne":
            xy = _run_tsne(reduced, params, progress)
        else:
            xy = _run_umap(reduced, params, progress)

        # Coordinates are cheap (2 int32 per patch) so they're gathered in one go.
        progress(93, "Collecting patch coordinates")
        coords = np.concatenate([_read_coords(s) for s in sources], axis=0)
        if coords.shape[0] != total_n:
            raise ProjectionError(
                f"Coordinate count {coords.shape[0]} does not match patch count {total_n}."
            )

        idx_dtype = np.uint16 if len(sources) <= 65535 else np.uint32
        slide_idx = np.concatenate([
            np.full(s.n_patches, i, dtype=idx_dtype) for i, s in enumerate(sources)
        ])

        progress(96, "Writing artifact")
        write_artifact(out_path, xy, slide_idx, coords[:, 0], coords[:, 1],
                       sources, method, params, dim)
        kept = reduced_out is not None
    finally:
        # Free the memmap handle before moving/unlinking, or Windows keeps the file.
        reduced.flush()
        del reduced
        if kept:
            kept = keep_reduced(mm_path, reduced_out)
        else:
            try:
                mm_path.unlink()
            except OSError:
                pass

    progress(100, "Done")
    return ProjectionResult(
        reduced_path=reduced_out if kept else None,
        reduced_dim=int(n_components) if kept else 0,
        artifact_path=out_path,
        point_count=int(total_n),
        feature_dim=int(dim),
        method=method,
        slide_hashes=[s.slide_hash for s in sources],
        elapsed_seconds=time.time() - started,
        peak_chunk_rows=chunk_rows,
    )
