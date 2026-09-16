"""
Overlay a held-out cohort onto an existing projection's map and clusterings.

The reference (e.g. an imported t-SNE + k-means run) stays fixed: nothing is refit.
Overlay patches are pushed through the reference's PCA, assigned to each k-means
clustering by nearest centroid — the rule k-means itself uses — and placed on the
2D map next to their nearest reference patches.

An imported projection carries no models: no PCA basis, no t-SNE object, no
centroids. Everything is recovered from what it does carry:

  * PCA. IncrementalPCA.transform is affine, reduced = (X − mean)·Cᵀ. Given raw
    reference features X (the slides' UNI .h5) and their rows in the kept reduced
    matrix, the map is recovered by least squares. Row alignment is checked
    against the artifact's patch coordinates first, and the fit is validated on
    patches from other slides; a bad fit fails loudly rather than assigning
    clusters on wrong numbers.
  * Centroids. The mean reduced position of each cluster's patches. "Agreement" —
    the share of reference patches whose nearest centroid is their stored label —
    shows how faithfully nearest-centroid reproduces the clustering.
  * Placement. kNN in PCA space against a reference subsample, inverse-distance
    averaged in 2D. Visual only: assignments and statistics never use it. Pure
    numpy (no numba / openTSNE), so it runs where those can't load.
"""
from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple

import numpy as np

from .cohort_projection import (
    MAGIC, SlideSource, ProjectionError, _iter_feature_chunks, _read_coords, _open_features,
    scan_sources, write_artifact, read_artifact_header, DEFAULT_CHUNK_ROWS,
)

ProgressFn = Callable[[int, str], None]

# PCA recovery
PCA_FIT_SLIDES = 40
PCA_VALIDATE_SLIDES = 10
PCA_BLOCK_ROWS = 800          # contiguous rows per sampled slide (cheap HDF5 read)
PCA_MAX_REL_ERROR = 1e-3

# Centroids
FAR_QUANTILE = 0.99

# Placement
PLACE_REF_SAMPLE = 200_000
PLACE_CELLS = 256             # coarse quantiser for the kNN search
PLACE_PROBE = 4               # cells searched per query cell
PLACE_K = 10


class OverlayError(RuntimeError):
    """Raised with a message intended to be shown to the user."""


def _noop(pct: int, stage: str) -> None:
    pass


# ──────────────────────────────────────────────────────────────────────
# Artifact access
# ──────────────────────────────────────────────────────────────────────

@dataclass
class Artifact:
    header: dict
    x: np.ndarray
    y: np.ndarray
    slide_idx: np.ndarray
    patch_x: np.ndarray
    patch_y: np.ndarray

    @property
    def n(self) -> int:
        return int(self.header["point_count"])

    def slide_range(self, i: int) -> Tuple[int, int]:
        s = self.header["slides"][i]
        return int(s["start"]), int(s["start"]) + int(s["n_patches"])


def read_artifact(path: Path) -> Artifact:
    header = read_artifact_header(path)
    with open(path, "rb") as fh:
        fh.seek(8)
        hlen = int.from_bytes(fh.read(4), "little")
    base = len(MAGIC) + 4 + hlen
    n = int(header["point_count"])
    cols = {}
    for c in header["columns"]:
        dt = np.dtype(c["dtype"]).newbyteorder("<")
        count = int(c["bytes"]) // dt.itemsize
        cols[c["name"]] = np.memmap(path, dtype=dt, mode="r", offset=base + int(c["offset"]), shape=(count,))
    for name in ("x", "y", "slide_idx", "patch_x", "patch_y"):
        if name not in cols or cols[name].shape[0] != n:
            raise OverlayError(f"Projection artifact {path.name} is missing or has a short '{name}' column.")
    return Artifact(header, cols["x"], cols["y"], cols["slide_idx"], cols["patch_x"], cols["patch_y"])


def open_reduced(path: Path, n: int, dim: int) -> np.ndarray:
    expected = n * dim * 4
    actual = path.stat().st_size if path.exists() else -1
    if actual != expected:
        raise OverlayError(
            f"Reduced matrix {path.name} is {actual} bytes; expected {expected} for {n:,} × {dim}.")
    return np.memmap(path, dtype=np.float32, mode="r", shape=(n, dim))


# ──────────────────────────────────────────────────────────────────────
# 1. PCA recovery
# ──────────────────────────────────────────────────────────────────────

def _read_block(src: SlideSource, start: int, stop: int) -> np.ndarray:
    import h5py
    f, key = _open_features(h5py, src.features_h5)
    try:
        return np.asarray(f[key][start:stop], dtype=np.float64)
    finally:
        f.close()


def recover_pca(
    ref: Artifact,
    reduced: np.ndarray,
    sources_by_hash: Dict[str, SlideSource],
    out_path: Path,
    seed: int = 0,
    progress: ProgressFn = _noop,
) -> dict:
    """
    Recover the affine map raw features → reduced rows, save it to `out_path`
    (npz: W [dim×K], b [K], report json) and return the report.
    """
    rng = np.random.default_rng(seed)
    slides = ref.header["slides"]
    K = reduced.shape[1]

    available = [i for i, s in enumerate(slides) if s["slide_hash"] in sources_by_hash
                 and int(s["n_patches"]) > 0]
    need = PCA_FIT_SLIDES + PCA_VALIDATE_SLIDES
    if len(available) < 2:
        raise OverlayError(
            "Can't recover the reference PCA: the reference slides' UNI feature files aren't "
            f"available ({len(available)} of {len(slides)} found).")
    picked = rng.permutation(available)[:need]
    n_val = max(1, min(PCA_VALIDATE_SLIDES, len(picked) // 5))
    val_slides, fit_slides = list(picked[:n_val]), list(picked[n_val:])

    # Check alignment for every sampled slide before trusting any row pairing.
    checked = 0
    for j, i in enumerate(fit_slides + val_slides):
        s = slides[i]
        src = sources_by_hash[s["slide_hash"]]
        scan_sources([src])
        if src.n_patches != int(s["n_patches"]):
            raise OverlayError(
                f"Reference slide {s['slide_hash'][:10]}… has {src.n_patches:,} patches in its UNI "
                f"files but {int(s['n_patches']):,} in the projection — the analysis output changed "
                f"since the reference was built, so its rows can't be matched.")
        lo, hi = ref.slide_range(i)
        coords = _read_coords(src)
        if not (np.array_equal(coords[:, 0], ref.patch_x[lo:hi]) and np.array_equal(coords[:, 1], ref.patch_y[lo:hi])):
            raise OverlayError(
                f"Reference slide {s['slide_hash'][:10]}…: patch coordinates in its UNI files don't match "
                f"the projection's rows, so features can't be paired with the reduced matrix.")
        checked += 1
        progress(2 + int(28 * (j + 1) / len(picked)), f"Checking reference row alignment ({j + 1}/{len(picked)} slides)")

    def blocks(idx_list, phase_lo, phase_hi, label):
        Xs, Ys = [], []
        for j, i in enumerate(idx_list):
            s = slides[i]
            src = sources_by_hash[s["slide_hash"]]
            n = int(s["n_patches"])
            b = min(PCA_BLOCK_ROWS, n)
            st = int(rng.integers(0, n - b + 1))
            X = _read_block(src, st, st + b)
            lo, _ = ref.slide_range(i)
            Y = np.asarray(reduced[lo + st: lo + st + b], dtype=np.float64)
            Xs.append(X); Ys.append(Y)
            progress(phase_lo + int((phase_hi - phase_lo) * (j + 1) / len(idx_list)),
                     f"Reading reference features for PCA {label} ({j + 1}/{len(idx_list)} slides)")
        return np.vstack(Xs), np.vstack(Ys)

    Xf, Yf = blocks(fit_slides, 30, 60, "fit")
    dim = Xf.shape[1]
    if Xf.shape[0] <= dim + 1:
        raise OverlayError(f"Too few reference patches ({Xf.shape[0]:,}) to recover a {dim}-d PCA.")

    # Normal equations on the centred data, solved by least squares (tolerates rank deficiency).
    mu = Xf.mean(axis=0)
    Xc = Xf - mu
    G = Xc.T @ Xc
    H = Xc.T @ (Yf - Yf.mean(axis=0))
    W = np.linalg.lstsq(G, H, rcond=None)[0]           # dim × K
    b = Yf.mean(axis=0) - mu @ W                        # K

    Xv, Yv = blocks(val_slides, 60, 75, "validation")
    pred = Xv @ W + b
    err = pred - Yv
    rel = float(np.linalg.norm(err) / max(1e-12, np.linalg.norm(Yv - Yv.mean(axis=0))))
    report = {
        "fit_slides": len(fit_slides), "fit_rows": int(Xf.shape[0]),
        "validate_slides": len(val_slides), "validate_rows": int(Xv.shape[0]),
        "aligned_slides_checked": checked,
        "relative_error": rel, "max_abs_error": float(np.abs(err).max()),
        "feature_dim": int(dim), "reduced_dim": int(K),
    }
    if not np.isfinite(rel) or rel > PCA_MAX_REL_ERROR:
        raise OverlayError(
            f"The reference's PCA couldn't be recovered from its UNI features (relative error "
            f"{rel:.2e}, limit {PCA_MAX_REL_ERROR:.0e}). The reduced matrix may not be a plain PCA of "
            f"these features (e.g. normalised or whitened first), so overlaying would be unreliable.")
    tmp = out_path.with_suffix(".tmp.npz")
    np.savez(tmp, W=W, b=b, report=json.dumps(report))
    tmp.replace(out_path)
    progress(75, f"PCA recovered (relative error {rel:.1e})")
    return report


def load_pca(path: Path) -> Tuple[np.ndarray, np.ndarray, dict]:
    with np.load(path, allow_pickle=False) as z:
        return z["W"], z["b"], json.loads(str(z["report"]))


def transform_sources(sources: List[SlideSource], W: np.ndarray, b: np.ndarray, out_path: Path,
                      total_n: int, progress: ProgressFn = _noop, lo: int = 0, hi: int = 100) -> None:
    K = W.shape[1]
    W32, b32 = W.astype(np.float32), b.astype(np.float32)
    tmp = out_path.with_suffix(out_path.suffix + ".tmp")
    mm = np.memmap(tmp, dtype=np.float32, mode="w+", shape=(total_n, K))
    row = 0
    for src in sources:
        for _, chunk in _iter_feature_chunks(src, DEFAULT_CHUNK_ROWS):
            mm[row:row + chunk.shape[0]] = chunk @ W32 + b32
            row += chunk.shape[0]
            progress(lo + int((hi - lo) * row / max(1, total_n)), f"Projecting into reference PCA ({row:,}/{total_n:,} patches)")
    mm.flush()
    del mm
    if row != total_n:
        raise OverlayError(f"Internal error: projected {row} rows, expected {total_n}.")
    tmp.replace(out_path)


# ──────────────────────────────────────────────────────────────────────
# 2. Centroids per clustering
# ──────────────────────────────────────────────────────────────────────

def nearest_centroid(X: np.ndarray, C: np.ndarray, chunk: int = 65_536) -> Tuple[np.ndarray, np.ndarray]:
    C = C.astype(np.float32)
    c2 = (C * C).sum(axis=1)
    n = X.shape[0]
    lab = np.empty(n, dtype=np.int16)
    dist = np.empty(n, dtype=np.float32)
    for s in range(0, n, chunk):
        xb = np.asarray(X[s:s + chunk], dtype=np.float32)
        d = (xb * xb).sum(axis=1)[:, None] - 2.0 * xb @ C.T + c2[None, :]
        j = d.argmin(axis=1)
        lab[s:s + chunk] = j
        dist[s:s + chunk] = np.sqrt(np.maximum(d[np.arange(d.shape[0]), j], 0.0))
    return lab, dist


def build_centroids(reduced: np.ndarray, labels: np.ndarray, out_path: Path) -> dict:
    """Centroids, per-cluster far radius and agreement for one clustering; saved to npz."""
    labels = np.asarray(labels)
    if labels.shape[0] != reduced.shape[0]:
        raise OverlayError("Clustering labels don't line up with the reference's reduced matrix.")
    member = labels >= 0
    k = int(labels[member].max()) + 1 if member.any() else 0
    if k == 0:
        raise OverlayError("This clustering has no clustered patches.")
    K = reduced.shape[1]
    sums = np.zeros((k, K), dtype=np.float64)
    counts = np.bincount(labels[member], minlength=k).astype(np.int64)
    chunk = 262_144
    for s in range(0, reduced.shape[0], chunk):
        lab = labels[s:s + chunk]
        m = lab >= 0
        xb = np.asarray(reduced[s:s + chunk], dtype=np.float64)[m]
        for d in range(K):
            sums[:, d] += np.bincount(lab[m], weights=xb[:, d], minlength=k)
    C = (sums / np.maximum(counts, 1)[:, None]).astype(np.float32)

    near, dist = nearest_centroid(reduced, C)
    agreement = float((near[member] == labels[member]).mean())
    # Distance of each member to its OWN centroid, for the far radius.
    own = np.empty(reduced.shape[0], dtype=np.float32)
    for s in range(0, reduced.shape[0], chunk):
        xb = np.asarray(reduced[s:s + chunk], dtype=np.float32)
        lab = labels[s:s + chunk]
        dd = np.full(lab.shape[0], np.nan, dtype=np.float32)
        m = lab >= 0
        dd[m] = np.linalg.norm(xb[m] - C[lab[m]], axis=1)
        own[s:s + chunk] = dd
    radius = np.array([np.quantile(own[labels == c], FAR_QUANTILE) if counts[c] else 0.0 for c in range(k)],
                      dtype=np.float32)
    ref_share = counts / max(1, counts.sum())
    report = {"n_clusters": k, "agreement": agreement, "far_quantile": FAR_QUANTILE}
    tmp = out_path.with_suffix(".tmp.npz")
    np.savez(tmp, centroids=C, radius=radius, counts=counts, ref_share=ref_share, report=json.dumps(report))
    tmp.replace(out_path)
    return report


def load_centroids(path: Path) -> dict:
    with np.load(path, allow_pickle=False) as z:
        return {"centroids": z["centroids"], "radius": z["radius"], "counts": z["counts"],
                "ref_share": z["ref_share"], "report": json.loads(str(z["report"]))}


def assign(reduced: np.ndarray, cent: dict) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    """(labels int16, far uint8, dist float32) for every row."""
    lab, dist = nearest_centroid(reduced, cent["centroids"])
    far = (dist > cent["radius"][lab]).astype(np.uint8)
    return lab, far, dist


# ──────────────────────────────────────────────────────────────────────
# 3. Placement on the reference map
# ──────────────────────────────────────────────────────────────────────

def _sqdist(A: np.ndarray, B: np.ndarray, b2: np.ndarray) -> np.ndarray:
    return np.maximum((A * A).sum(axis=1)[:, None] - 2.0 * A @ B.T + b2[None, :], 0.0)


def place(ref_reduced: np.ndarray, ref_xy: np.ndarray, Q: np.ndarray, seed: int = 0,
          progress: ProgressFn = _noop, lo: int = 0, hi: int = 100) -> np.ndarray:
    """
    2D positions for query rows Q (PCA space) on the reference map.

    Approximate kNN: a coarse k-means quantiser over a reference subsample; each
    group of queries sharing a nearest cell searches that cell's members plus
    those of its nearest neighbouring cells, exhaustively.
    """
    from sklearn.cluster import MiniBatchKMeans

    rng = np.random.default_rng(seed)
    n_ref = ref_reduced.shape[0]
    pick = np.sort(rng.choice(n_ref, size=min(PLACE_REF_SAMPLE, n_ref), replace=False))
    R = np.asarray(ref_reduced[pick], dtype=np.float32)
    Rxy = np.asarray(ref_xy[pick], dtype=np.float32)
    r2 = (R * R).sum(axis=1)

    n_cells = max(1, min(PLACE_CELLS, R.shape[0] // 50))
    progress(lo, "Indexing reference patches for placement")
    km = MiniBatchKMeans(n_clusters=n_cells, batch_size=8192, n_init=1, random_state=seed).fit(R)
    Cc = km.cluster_centers_.astype(np.float32)
    cell_of_ref = km.labels_
    members = [np.flatnonzero(cell_of_ref == c) for c in range(n_cells)]
    probe = min(PLACE_PROBE, n_cells)
    cell_nbrs = np.argsort(_sqdist(Cc, Cc, (Cc * Cc).sum(axis=1)), axis=1)[:, :probe]

    Q = np.asarray(Q, dtype=np.float32)
    q_cell, _ = nearest_centroid(Q, Cc)
    q_cell = q_cell.astype(np.int64)
    out = np.empty((Q.shape[0], 2), dtype=np.float32)

    span = np.ptp(Rxy, axis=0)
    diag = float(np.hypot(*span)) or 1.0
    # Averaging neighbours' positions pulls every point toward the middle of its
    # region, so an overlay collapses into tight blobs while the reference is
    # spread out. Instead: sit on the nearest neighbour, jittered by how spread
    # the neighbours are on the map (capped, so neighbours split across two
    # islands can't fling a point between them).
    jitter_cap = 0.01 * diag

    k = PLACE_K
    done = 0
    for c in range(n_cells):
        qi = np.flatnonzero(q_cell == c)
        if qi.size == 0:
            continue
        cand = np.concatenate([members[j] for j in cell_nbrs[c]])
        if cand.size == 0:
            cand = np.arange(R.shape[0])
        kk = min(k, cand.size)
        Rc, Rc2, Rcxy = R[cand], r2[cand], Rxy[cand]
        for s in range(0, qi.size, 2048):
            sel = qi[s:s + 2048]
            d2 = _sqdist(Q[sel], Rc, Rc2)
            nn = np.argpartition(d2, kk - 1, axis=1)[:, :kk]
            dn = np.sqrt(np.take_along_axis(d2, nn, axis=1))
            nxy = Rcxy[nn]                                          # q × k × 2
            nearest = nxy[np.arange(sel.size), dn.argmin(axis=1)]
            spread = np.median(np.linalg.norm(nxy - nearest[:, None, :], axis=2), axis=1)
            sigma = np.minimum(0.5 * spread, jitter_cap)[:, None]
            out[sel] = nearest + rng.normal(size=nearest.shape).astype(np.float32) * sigma
        done += qi.size
        progress(lo + int((hi - lo) * done / Q.shape[0]), f"Placing patches on the map ({done:,}/{Q.shape[0]:,})")
    return out


# ──────────────────────────────────────────────────────────────────────
# The job
# ──────────────────────────────────────────────────────────────────────

def build_overlay(
    ref_artifact_path: Path,
    ref_reduced: np.ndarray,
    pca_path: Path,
    sources: List[SlideSource],
    out_artifact: Path,
    out_reduced: Path,
    reference_projection_id: int,
    progress: ProgressFn = _noop,
) -> dict:
    """
    Transform, place and write the overlay. Assumes the PCA is already recovered.
    Returns a report dict. Cluster assignments are computed separately, per
    clustering, on demand.
    """
    t0 = time.time()
    ref = read_artifact(ref_artifact_path)
    W, b, _ = load_pca(pca_path)

    total_n, dim = scan_sources(sources)
    if dim != W.shape[0]:
        raise OverlayError(
            f"The overlay cohort's UNI features are {dim}-d but the reference's are {W.shape[0]}-d; "
            f"they come from different models or settings and can't share clusters.")
    warnings = []
    ref_sizes = {int(s["patch_size"]) for s in ref.header["slides"]}
    ov_sizes = {s.patch_size for s in sources}
    if not ov_sizes <= ref_sizes:
        warnings.append(
            f"Patch size (level 0) differs: reference {sorted(ref_sizes)}, overlay {sorted(ov_sizes)}. "
            f"Different magnification makes cluster assignments unreliable.")

    progress(5, f"{total_n:,} patches across {len(sources)} slides")
    transform_sources(sources, W, b, out_reduced, total_n, progress, 5, 45)
    Q = np.memmap(out_reduced, dtype=np.float32, mode="r", shape=(total_n, W.shape[1]))

    ref_xy = np.stack([np.asarray(ref.x), np.asarray(ref.y)], axis=1)
    xy = place(ref_reduced, ref_xy, Q, progress=progress, lo=45, hi=90)
    del Q

    progress(92, "Collecting patch coordinates")
    coords = np.concatenate([_read_coords(s) for s in sources], axis=0)
    if coords.shape[0] != total_n:
        raise OverlayError(f"Coordinate count {coords.shape[0]} does not match patch count {total_n}.")
    idx_dtype = np.uint16 if len(sources) <= 65535 else np.uint32
    slide_idx = np.concatenate([np.full(s.n_patches, i, dtype=idx_dtype) for i, s in enumerate(sources)])
    write_artifact(out_artifact, xy, slide_idx, coords[:, 0], coords[:, 1], sources, "overlay",
                   {"reference_projection_id": reference_projection_id, "placement": "knn"}, dim)
    progress(100, "Done")
    return {"point_count": int(total_n), "feature_dim": int(dim), "warnings": warnings,
            "elapsed_seconds": round(time.time() - t0, 1)}
