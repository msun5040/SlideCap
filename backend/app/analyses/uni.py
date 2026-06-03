"""
UNI analysis kind.

UNI is a vision foundation model that produces per-patch CLS-token embeddings
for whole-slide images. Output tree for one slide looks like:

    <output_dir>/
      20x_256px_0px_overlap/        # magnification/patch-size config
        features_uni_v2/{stem}.h5   # (N, 1024) embeddings
        patches/{stem}_patches.h5   # (N, 2) patch top-left coords in slide pixels
        visualization/{stem}.jpg
      contours/{stem}.jpg
      contours_geojson/{stem}.geojson
      thumbnails/{stem}.jpg
      run.log

UNI outputs are uncompressed (HDF5 + plain GeoJSON), so we don't ship a
decompress op. The interesting compute is **per-slide projection**: reduce
the 1024-d embeddings to 2D (UMAP / PCA) and join them with each patch's
spatial coords so the frontend can draw a scatter plot whose points map
back to locations on the WSI.

That "join two sibling files" need is exactly why renderers exist alongside
ops — ops are pure bytes→bytes, but a projection needs both features.h5
and patches.h5 to be useful. See `RendererSpec` in __init__.py.

Projections are cached as JSON next to the source .h5 (e.g.
`features_uni_v2/{stem}.umap.json`). Cache is invalidated by source mtime,
so re-running UNI on a slide auto-busts it.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from . import AnalysisKind, register_kind


UNI = AnalysisKind(
    id="uni",
    name="UNI",
    description="Vision foundation model — per-patch 1024-d CLS embeddings; viewable as UMAP/PCA scatter linked to slide coordinates.",
    # UNI outputs are uncompressed; the only file shape we'd ever want to
    # repair is the tissue-contour geojson, and shapely.make_valid is a no-op
    # on already-valid geometry. Ship a default rule for it anyway so people
    # editing the contour file from the UI get the same safety net CellViT
    # has — costs nothing on valid input.
    default_rules=[],
    output_globs=[
        "*/features_uni_v2/*.h5",
        "*/patches/*.h5",
        "contours_geojson/*.geojson",
    ],
)
register_kind(UNI)


# ── Locating the per-slide UNI files ───────────────────────────────────


def _find_uni_files(output_dir: Path, stem: str) -> Tuple[Path, Path]:
    """
    Locate the features and patches HDF5 for a given slide stem.

    We rglob rather than hardcode `20x_256px_0px_overlap/...` because the
    magnification config varies per run. Errors clearly if files are missing
    so the frontend can surface a useful message instead of an opaque 500.
    """
    features_candidates = list(output_dir.rglob(f"features_uni_v2/{stem}.h5"))
    if not features_candidates:
        features_candidates = list(output_dir.rglob(f"features/{stem}.h5"))  # other UNI variants
    if not features_candidates:
        raise FileNotFoundError(
            f"UNI features .h5 not found for stem {stem!r} under {output_dir}. "
            f"Looked for features_uni_v2/{stem}.h5 and features/{stem}.h5."
        )

    # Pick the first; if there are multiple magnification configs we'd want a
    # `?config=` param — defer until that's actually needed.
    features_h5 = features_candidates[0]

    # patches.h5 lives in a sibling `patches/` dir under the same config dir.
    config_dir = features_h5.parent.parent
    patch_candidates = [
        config_dir / "patches" / f"{stem}_patches.h5",
        config_dir / "patches" / f"{stem}.h5",
    ]
    patches_h5 = next((p for p in patch_candidates if p.exists()), None)
    if patches_h5 is None:
        raise FileNotFoundError(
            f"UNI patches .h5 not found for stem {stem!r} next to {features_h5}. "
            f"Looked for: {[str(p) for p in patch_candidates]}"
        )

    return features_h5, patches_h5


def _read_features(h5_path: Path):
    """Return (N, D) float32 array of embeddings. Lazy h5py import — UNI is
    optional and we don't want main.py imports to fail if h5py isn't installed."""
    try:
        import h5py
    except ImportError:
        raise RuntimeError(
            "h5py is required for the UNI renderers. Install with `pip install h5py` "
            "in the backend's Python env."
        )
    import numpy as np

    with h5py.File(str(h5_path), "r") as f:
        # Most UNI/CLAM exporters use one of these dataset names. Probe in order.
        for key in ("features", "embeddings", "feats"):
            if key in f:
                arr = f[key][:]
                return np.asarray(arr, dtype=np.float32)
        raise RuntimeError(
            f"No features dataset found in {h5_path}. Looked for: features, embeddings, feats. "
            f"Top-level keys present: {list(f.keys())}"
        )


def _read_patch_coords(h5_path: Path):
    """Return (N, 2) int32 array of patch top-left (x, y) in level-0 slide pixels,
    plus the level-0 patch_size for drawing boxes on the WSI viewer.

    UNI's patches.h5 stores ``patch_size=256`` (the model input size) and
    ``patch_size_level0=512`` (the slide region each patch was cut from, at
    full resolution). For the viewer overlay we want level-0 — that's what
    the OSD viewer renders against. Falls back to ``patch_size`` if the
    level-0 attribute isn't present (older outputs)."""
    try:
        import h5py
    except ImportError:
        raise RuntimeError(
            "h5py is required for the UNI renderers. Install with `pip install h5py`."
        )
    import numpy as np

    with h5py.File(str(h5_path), "r") as f:
        coords_key = "coords" if "coords" in f else next((k for k in f.keys() if "coord" in k.lower()), None)
        if coords_key is None:
            raise RuntimeError(
                f"No coords dataset in {h5_path}. Top-level keys: {list(f.keys())}"
            )
        ds = f[coords_key]
        coords = np.asarray(ds[:], dtype=np.int64)

        attrs = dict(ds.attrs) if hasattr(ds, "attrs") else {}
        # Prefer level-0 size so overlay boxes match the WSI's actual pixel scale.
        patch_size = int(attrs.get("patch_size_level0", attrs.get("patch_size", 256)))
        patch_level = int(attrs.get("patch_level", 0))

    if coords.ndim != 2 or coords.shape[1] != 2:
        raise RuntimeError(f"Unexpected coords shape {coords.shape} in {h5_path}; expected (N, 2)")
    return coords, patch_size, patch_level


# ── Projection + caching ───────────────────────────────────────────────


def _cache_path(features_h5: Path, projection: str) -> Path:
    return features_h5.with_suffix(f".{projection}.json")


def _cache_fresh(cache: Path, sources: list[Path]) -> bool:
    if not cache.exists():
        return False
    cache_mtime = cache.stat().st_mtime
    return all(s.exists() and s.stat().st_mtime <= cache_mtime for s in sources)


def _project(features, method: str, params: Dict[str, Any]):
    """Return (N, 2) float array. Methods: 'umap', 'pca'."""
    import numpy as np
    if method == "pca":
        from sklearn.decomposition import PCA
        # PCA is stable + fast; no hyperparameters worth exposing.
        return PCA(n_components=2, random_state=0).fit_transform(features)
    elif method == "umap":
        import umap
        n_neighbors = int(params.get("n_neighbors", 15))
        min_dist = float(params.get("min_dist", 0.1))
        # n_neighbors must be < n_samples; clamp so tiny slides don't crash.
        n_neighbors = max(2, min(n_neighbors, max(2, features.shape[0] - 1)))
        reducer = umap.UMAP(
            n_components=2,
            n_neighbors=n_neighbors,
            min_dist=min_dist,
            random_state=42,
            metric="cosine",  # UNI embeddings are typically cosine-meaningful
        )
        return reducer.fit_transform(features)
    raise ValueError(f"Unknown projection method: {method!r}")


def _render_projection(
    output_dir: Path,
    stem: str,
    method: str,
    params: Dict[str, Any],
) -> dict:
    """Shared core for both UMAP and PCA renderers — locate files, project, cache."""
    import numpy as np
    features_h5, patches_h5 = _find_uni_files(output_dir, stem)
    cache = _cache_path(features_h5, method)

    if not params.get("recompute") and _cache_fresh(cache, [features_h5, patches_h5]):
        return json.loads(cache.read_text())

    features = _read_features(features_h5)            # (N, D)
    coords, patch_size, patch_level = _read_patch_coords(patches_h5)  # (M, 2)

    # Sanity check: feature and coord counts should match. If they don't,
    # truncate to the shorter — better to show *something* than 500.
    n = min(features.shape[0], coords.shape[0])
    if features.shape[0] != coords.shape[0]:
        print(
            f"[uni] feature/coord count mismatch for {stem}: "
            f"features={features.shape[0]} coords={coords.shape[0]}; truncating to {n}"
        )
    features = features[:n]
    coords = coords[:n]

    proj = _project(features, method, params)         # (n, 2)

    # Convert to plain Python types; JSON can't serialize numpy floats.
    result = {
        "method": method,
        "n": int(n),
        "patch_size": patch_size,
        "patch_level": patch_level,
        "stem": stem,
        # Per-point: scatter coord (x, y), spatial coord on slide (slide_x, slide_y), index.
        # slide_x/y are top-left of the patch in level-0 slide pixels.
        "points": [
            {
                "idx": int(i),
                "x": float(proj[i, 0]),
                "y": float(proj[i, 1]),
                "slide_x": int(coords[i, 0]),
                "slide_y": int(coords[i, 1]),
            }
            for i in range(n)
        ],
    }

    # Best-effort cache write; failure here shouldn't poison the response.
    try:
        cache.write_text(json.dumps(result))
    except Exception as e:
        print(f"[uni] failed to cache {method} projection to {cache}: {e}")

    return result


# ── Renderer registrations ─────────────────────────────────────────────


@UNI.register_renderer(
    "umap",
    "UMAP projection",
    "Cosine-metric UMAP of UNI embeddings to 2D, joined with each patch's slide coordinates.",
)
def _render_umap(output_dir: Path, stem: str, params: Dict[str, Any]) -> dict:
    return _render_projection(output_dir, stem, "umap", params)


@UNI.register_renderer(
    "pca",
    "PCA projection",
    "PCA of UNI embeddings to 2D — fast (no caching needed) but typically less interpretable than UMAP.",
)
def _render_pca(output_dir: Path, stem: str, params: Dict[str, Any]) -> dict:
    return _render_projection(output_dir, stem, "pca", params)
