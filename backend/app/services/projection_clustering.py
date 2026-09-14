"""
Clustering over a cohort projection's patches.

Runs on the projection's **PCA-reduced matrix** (~50-d), never on the 2D
coordinates. UMAP preserves local neighbourhoods but not distances or densities,
so clustering its output clusters the embedding's distortions; the reduced matrix
is what the 2D layout was computed from. scripts/uni_umap_figure.py makes the same
choice.

Output is a label per point, row-aligned with the projection artifact (label i is
artifact row i), written as raw little-endian int16 with -1 meaning noise.
Clusters are renumbered largest-first so colours are stable across re-runs.

Scale, honestly: k-means and Leiden fit every patch. HDBSCAN and agglomerative
don't scale to a million points, so they fit a random subsample and assign the
rest to the nearest cluster centroid; results record `approximate=True` and the
UI says so.
"""
from __future__ import annotations

import os
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Dict, Optional

ALGORITHMS = ("kmeans", "hdbscan", "leiden", "agglomerative")

# The scatter colours points through a Uint8 palette index with 255 reserved for
# "unassigned", so a result can name at most 254 clusters (plus noise).
MAX_CLUSTERS = 254

# Silhouette is O(n^2); score on a subsample.
SILHOUETTE_SAMPLE = 10_000

# Subsample caps for the algorithms that can't fit everything.
HDBSCAN_FIT_SAMPLE = 50_000
# Ward linkage needs the condensed distance matrix: n(n-1)/2 doubles, ~0.9 GB at 15k.
AGGLOMERATIVE_FIT_SAMPLE = 15_000
AGGLOMERATIVE_MAX_SAMPLE = 20_000

ProgressFn = Callable[[int, str], None]


class ClusteringError(RuntimeError):
    """Raised with a message intended to be shown to the user."""


@dataclass
class ClusteringResult:
    labels_path: Path
    n_points: int
    n_clusters: int
    n_noise: int
    silhouette: Optional[float]
    approximate: bool
    params: Dict = field(default_factory=dict)
    elapsed_seconds: float = 0.0


def _noop(pct: int, stage: str) -> None:
    pass


def _require(module: str, pip_name: Optional[str] = None):
    try:
        return __import__(module)
    except ImportError as e:
        raise ClusteringError(
            f"{module} is required for this clustering method. Install with "
            f"`pip install {pip_name or module}` in the backend's Python env."
        ) from e


# ──────────────────────────────────────────────────────────────────────
# Shared helpers
# ──────────────────────────────────────────────────────────────────────

def load_reduced(path: Path, n_points: int, dim: int):
    """Load the reduced matrix into RAM (~200 MB at 1M x 50), checking its size."""
    np = _require("numpy")
    expected = n_points * dim * 4
    actual = path.stat().st_size if path.exists() else -1
    if actual != expected:
        raise ClusteringError(
            f"Reduced matrix {path.name} is {actual} bytes; expected {expected} for "
            f"{n_points:,} x {dim}. It may be incomplete — delete it to force a rebuild."
        )
    mm = np.memmap(path, dtype=np.float32, mode="r", shape=(n_points, dim))
    X = np.array(mm, dtype=np.float32, copy=True)
    del mm
    return X


def _subsample(n: int, cap: int, rng):
    np = _require("numpy")
    if n <= cap:
        return np.arange(n)
    return np.sort(rng.choice(n, size=cap, replace=False))


def _silhouette(X, labels, rng) -> Optional[float]:
    np = _require("numpy")
    from sklearn.metrics import silhouette_score
    mask = labels >= 0
    idx = np.flatnonzero(mask)
    if idx.size < 3:
        return None
    if idx.size > SILHOUETTE_SAMPLE:
        idx = np.sort(rng.choice(idx, size=SILHOUETTE_SAMPLE, replace=False))
    lab = labels[idx]
    if np.unique(lab).size < 2 or np.unique(lab).size >= idx.size:
        return None
    return float(silhouette_score(X[idx], lab))


def _nearest_centroid(X, centroids, chunk: int = 65_536):
    """(labels, distances) of each row's nearest centroid, computed in chunks."""
    np = _require("numpy")
    c = centroids.astype(np.float32)
    c_sq = (c * c).sum(axis=1)
    labels = np.empty(X.shape[0], dtype=np.int32)
    dists = np.empty(X.shape[0], dtype=np.float32)
    for s in range(0, X.shape[0], chunk):
        xb = X[s:s + chunk]
        d = (xb * xb).sum(axis=1)[:, None] - 2.0 * xb @ c.T + c_sq[None, :]
        j = d.argmin(axis=1)
        labels[s:s + chunk] = j
        dists[s:s + chunk] = np.sqrt(np.maximum(d[np.arange(d.shape[0]), j], 0.0))
    return labels, dists


def _centroids(X, labels, k):
    np = _require("numpy")
    out = np.zeros((k, X.shape[1]), dtype=np.float64)
    counts = np.bincount(labels, minlength=k).astype(np.float64)
    np.add.at(out, labels, X)
    return (out / np.maximum(counts, 1)[:, None]).astype(np.float32)


def _extend_from_sample(X, fit_idx, fit_labels, noise_quantile: Optional[float]):
    """
    Give every point a label from a subsample fit: sampled points keep theirs,
    the rest take the nearest cluster centroid. With `noise_quantile`, a point
    farther from its centroid than that quantile of the cluster's own members is
    noise — so HDBSCAN's notion of "doesn't belong anywhere" survives the extension.
    """
    np = _require("numpy")
    n = X.shape[0]
    labels = np.full(n, -1, dtype=np.int32)
    labels[fit_idx] = fit_labels
    member = fit_labels >= 0
    k = int(fit_labels[member].max()) + 1 if member.any() else 0
    if k == 0 or fit_idx.size == n:
        return labels

    cents = _centroids(X[fit_idx[member]], fit_labels[member], k)
    rest = np.ones(n, dtype=bool)
    rest[fit_idx] = False
    rest_idx = np.flatnonzero(rest)
    near, dist = _nearest_centroid(X[rest_idx], cents)

    if noise_quantile is not None:
        _, member_dist = _nearest_centroid(X[fit_idx[member]], cents)
        member_lab = fit_labels[member]
        radius = np.array([
            np.quantile(member_dist[member_lab == c], noise_quantile)
            if (member_lab == c).any() else 0.0
            for c in range(k)
        ], dtype=np.float32)
        near = np.where(dist <= radius[near], near, -1)

    labels[rest_idx] = near
    return labels


def _renumber_by_size(labels):
    """Largest cluster becomes 0; noise stays -1. Returns (labels, n_clusters)."""
    np = _require("numpy")
    member = labels >= 0
    if not member.any():
        return labels.astype(np.int32), 0
    ids, counts = np.unique(labels[member], return_counts=True)
    order = ids[np.argsort(-counts, kind="stable")]
    remap = np.full(int(ids.max()) + 1, -1, dtype=np.int32)
    remap[order] = np.arange(order.size, dtype=np.int32)
    out = np.full(labels.shape, -1, dtype=np.int32)
    out[member] = remap[labels[member]]
    return out, int(order.size)


# ──────────────────────────────────────────────────────────────────────
# Algorithms. Each returns (labels int32, approximate bool) and records the
# parameters it actually used back into `params`.
# ──────────────────────────────────────────────────────────────────────

def _kmeans(X, params, rng, seed, progress):
    np = _require("numpy")
    _require("sklearn", "scikit-learn")
    from sklearn.cluster import MiniBatchKMeans

    n = X.shape[0]

    def fit(data, k):
        return MiniBatchKMeans(n_clusters=k, batch_size=4096, n_init=3,
                               random_state=seed).fit(data)

    k = params.get("k")
    if k in (None, "", "auto"):
        k_min = max(2, int(params.get("k_min", 2)))
        k_max = min(int(params.get("k_max", 20)), MAX_CLUSTERS, n - 1)
        if k_max < k_min:
            raise ClusteringError(f"Too few points ({n}) to search k in {k_min}..{k_max}.")
        # Scan on a subsample: this is model selection, the final fit uses everything.
        scan_idx = _subsample(n, 100_000, rng)
        Xs = X[scan_idx]
        scores = {}
        for i, kk in enumerate(range(k_min, k_max + 1)):
            progress(10 + int(50 * i / max(1, k_max - k_min + 1)),
                     f"Choosing k: trying k={kk} ({i + 1}/{k_max - k_min + 1})")
            lab = fit(Xs, kk).labels_
            s = _silhouette(Xs, lab, rng)
            scores[kk] = s if s is not None else -1.0
        k = max(scores, key=lambda kk: scores[kk])
        params["k_mode"] = "auto"
        params["k_scores"] = {str(kk): round(v, 4) for kk, v in scores.items()}
        params["k_min"], params["k_max"] = k_min, k_max
    else:
        k = int(k)
        if not 2 <= k <= MAX_CLUSTERS:
            raise ClusteringError(f"k must be between 2 and {MAX_CLUSTERS}.")
        if k >= n:
            raise ClusteringError(f"k={k} needs more than {n} points.")
        params["k_mode"] = "fixed"
    params["k"] = int(k)

    progress(65, f"Fitting k-means (k={k}) on all {n:,} patches")
    model = fit(X, int(k))
    labels, _ = _nearest_centroid(X, model.cluster_centers_)
    return labels, False


def _hdbscan(X, params, rng, seed, progress):
    np = _require("numpy")
    _require("sklearn", "scikit-learn")
    try:
        from sklearn.cluster import HDBSCAN
    except ImportError as e:
        raise ClusteringError("HDBSCAN needs scikit-learn >= 1.3.") from e

    n = X.shape[0]
    cap = int(params.get("fit_sample", HDBSCAN_FIT_SAMPLE))
    fit_idx = _subsample(n, cap, rng)
    n_fit = fit_idx.size
    mcs = params.get("min_cluster_size")
    mcs = int(mcs) if mcs not in (None, "") else max(10, int(round(0.005 * n_fit)))
    ms = params.get("min_samples")
    ms = int(ms) if ms not in (None, "") else None
    params.update(fit_sample=int(n_fit), min_cluster_size=mcs, min_samples=ms)

    progress(15, f"Fitting HDBSCAN on {n_fit:,} of {n:,} patches")
    fit_labels = HDBSCAN(min_cluster_size=mcs, min_samples=ms).fit_predict(X[fit_idx])
    progress(75, "Assigning remaining patches to nearest cluster")
    labels = _extend_from_sample(X, fit_idx, fit_labels.astype(np.int32),
                                 noise_quantile=float(params.get("noise_quantile", 0.95)))
    params["noise_quantile"] = float(params.get("noise_quantile", 0.95))
    return labels, n_fit < n


def _leiden(X, params, rng, seed, progress):
    np = _require("numpy")
    pynndescent = _require("pynndescent")
    ig = _require("igraph")
    leidenalg = _require("leidenalg")

    n = X.shape[0]
    k = max(2, min(int(params.get("n_neighbors", 15)), n - 1))
    resolution = float(params.get("resolution", 1.0))
    params.update(n_neighbors=k, resolution=resolution)

    progress(10, f"Building {k}-nearest-neighbour graph over {n:,} patches")
    index = pynndescent.NNDescent(X, n_neighbors=k + 1, metric="euclidean",
                                  low_memory=True, random_state=seed, n_jobs=None)
    nbrs, _ = index.neighbor_graph
    del index

    progress(55, "Building graph")
    src = np.repeat(np.arange(n, dtype=np.int64), nbrs.shape[1])
    dst = nbrs.reshape(-1).astype(np.int64)
    keep = (dst >= 0) & (dst != src)
    # Undirected, one edge per pair: order endpoints and drop duplicates.
    a = np.minimum(src[keep], dst[keep])
    b = np.maximum(src[keep], dst[keep])
    pairs = np.unique(a * n + b)
    edges = np.stack([pairs // n, pairs % n], axis=1)
    del src, dst, keep, a, b, pairs
    g = ig.Graph(n=n, edges=edges, directed=False)
    del edges

    progress(70, f"Running Leiden (resolution={resolution})")
    part = leidenalg.find_partition(
        g, leidenalg.RBConfigurationVertexPartition,
        resolution_parameter=resolution, n_iterations=-1, seed=seed,
    )
    return np.asarray(part.membership, dtype=np.int32), False


def _agglomerative(X, params, rng, seed, progress):
    np = _require("numpy")
    _require("scipy")
    from scipy.cluster.hierarchy import fcluster, linkage

    n = X.shape[0]
    cap = min(int(params.get("fit_sample", AGGLOMERATIVE_FIT_SAMPLE)), AGGLOMERATIVE_MAX_SAMPLE)
    fit_idx = _subsample(n, cap, rng)
    Xf = X[fit_idx].astype(np.float64)
    params["fit_sample"] = int(fit_idx.size)

    progress(15, f"Building Ward tree on {fit_idx.size:,} of {n:,} patches")
    Z = linkage(Xf, method="ward")

    k = params.get("k")
    if k in (None, "", "auto"):
        k_min = max(2, int(params.get("k_min", 2)))
        k_max = min(int(params.get("k_max", 20)), MAX_CLUSTERS, fit_idx.size - 1)
        scores = {}
        for kk in range(k_min, k_max + 1):
            lab = fcluster(Z, kk, criterion="maxclust") - 1
            s = _silhouette(Xf, lab, rng)
            scores[kk] = s if s is not None else -1.0
        k = max(scores, key=lambda kk: scores[kk])
        params["k_mode"] = "auto"
        params["k_scores"] = {str(kk): round(v, 4) for kk, v in scores.items()}
    else:
        k = int(k)
        if not 2 <= k <= MAX_CLUSTERS:
            raise ClusteringError(f"k must be between 2 and {MAX_CLUSTERS}.")
        params["k_mode"] = "fixed"
    params["k"] = int(k)

    fit_labels = (fcluster(Z, int(k), criterion="maxclust") - 1).astype(np.int32)
    progress(75, "Assigning remaining patches to nearest cluster")
    labels = _extend_from_sample(X, fit_idx, fit_labels, noise_quantile=None)
    return labels, fit_idx.size < n


_DISPATCH = {
    "kmeans": _kmeans,
    "hdbscan": _hdbscan,
    "leiden": _leiden,
    "agglomerative": _agglomerative,
}


# ──────────────────────────────────────────────────────────────────────
# Entry point
# ──────────────────────────────────────────────────────────────────────

def write_labels(path: Path, labels) -> None:
    np = _require("numpy")
    tmp = path.with_suffix(path.suffix + ".tmp")
    labels.astype("<i2").tofile(tmp)
    os.replace(tmp, path)


def run_clustering(
    reduced_path: Path,
    n_points: int,
    dim: int,
    algorithm: str,
    params: Optional[Dict],
    out_path: Path,
    progress: ProgressFn = _noop,
) -> ClusteringResult:
    np = _require("numpy")
    if algorithm not in _DISPATCH:
        raise ClusteringError(f"Unknown algorithm {algorithm!r}. Supported: {', '.join(ALGORITHMS)}.")
    params = dict(params or {})
    started = time.time()
    seed = int(params.get("random_state", 0))
    params["random_state"] = seed
    rng = np.random.default_rng(seed)

    progress(2, "Loading reduced matrix")
    X = load_reduced(reduced_path, n_points, dim)

    raw, approximate = _DISPATCH[algorithm](X, params, rng, seed, progress)
    if raw.shape[0] != n_points:
        raise ClusteringError(
            f"Internal error: {raw.shape[0]} labels for {n_points} points.")

    labels, n_clusters = _renumber_by_size(raw)
    if n_clusters > MAX_CLUSTERS:
        hint = ("lower the resolution" if algorithm == "leiden"
                else "raise min_cluster_size" if algorithm == "hdbscan" else "use a smaller k")
        raise ClusteringError(
            f"Found {n_clusters} clusters; at most {MAX_CLUSTERS} can be displayed — {hint}.")
    if n_clusters == 0:
        raise ClusteringError(
            "Every patch was labelled noise. Try a smaller min_cluster_size.")

    progress(90, "Scoring (silhouette on a subsample)")
    sil = _silhouette(X, labels, rng)

    progress(97, "Writing labels")
    write_labels(out_path, labels)
    progress(100, "Done")
    return ClusteringResult(
        labels_path=out_path,
        n_points=int(n_points),
        n_clusters=int(n_clusters),
        n_noise=int((labels < 0).sum()),
        silhouette=sil,
        approximate=bool(approximate),
        params=params,
        elapsed_seconds=time.time() - started,
    )
