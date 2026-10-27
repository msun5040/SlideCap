"""
Load a Data Pull analysis export into numpy, ready for a notebook.

Data Pull (Export → analysis files) hardlinks each slide's analysis output into

    <output_dir>/analysis-files/<full slide name>/<whatever the analysis produced>

This walks that tree, pairs each slide's features with its patch coordinates, and
returns one matrix plus the per-row slide/coordinate index. Copy it next to your
notebook, or `sys.path.append` the scripts dir and import it.

    from load_uni_pull import load_pull
    d = load_pull("/Volumes/.../analysis-files")
    d.X            # (N, 1536) float32 — patch embeddings
    d.slide        # (N,)       str    — folder name, i.e. the slide
    d.coords       # (N, 2)     int32  — LEVEL-0 top-left pixels
    d.patch_size   # (N,)       int32  — level-0 footprint

The pairing is the part worth not rewriting: features and coords can disagree in
length, and SlideCap resolves that by taking the first min(n_features, n_coords)
of each. Diverge from that and every patch maps to the wrong tissue -- silently,
because the plot still looks fine. This mirrors cohort_projection.py exactly, so
anything built from it can be imported back with make_projection_bundle.py.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import h5py
import numpy as np

FEATURE_KEYS = ("features", "embeddings", "feats")


@dataclass
class Pull:
    X: np.ndarray
    slide: np.ndarray
    coords: np.ndarray
    patch_size: np.ndarray
    slides: list          # folder names, in the order rows appear
    counts: list          # patches per slide, same order


def _first_key(f, candidates) -> Optional[str]:
    for k in candidates:
        if k in f:
            return k
    return None


def _find_pair(slide_dir: Path) -> Optional[tuple[Path, Path]]:
    """(features_h5, patches_h5) for one slide folder, or None."""
    feats = patches = None
    for p in sorted(slide_dir.rglob("*.h5")):
        try:
            with h5py.File(str(p), "r") as f:
                if feats is None and _first_key(f, FEATURE_KEYS):
                    feats = p
                if patches is None and "coords" in f:
                    patches = p
        except OSError:
            continue
    # UNI writes `features` and `coords` into one file and `coords` again into a
    # separate patches file, so one path often serves both. A folder with coords
    # but NO features is a partial run (patching finished, the model did not) --
    # there is nothing to embed, so report it as skipped rather than guessing.
    if feats and not patches:
        patches = feats
    return (feats, patches) if feats and patches else None


def load_pull(root: str | Path, limit_slides: Optional[int] = None,
              max_per_slide: Optional[int] = None, seed: int = 0,
              verbose: bool = True) -> Pull:
    """
    max_per_slide caps how many patches each slide contributes. A full cohort of
    1536-d features is tens of GB -- 292 slides of this pull would be ~17.6 GB --
    so a capped read is usually how you want to explore. The cap is applied as an
    evenly spaced stride, not a random draw, so it covers the whole slide rather
    than clumping, and it is reproducible.
    """
    root = Path(root)
    if not root.is_dir():
        raise SystemExit(f"Not a directory: {root}")

    slide_dirs = sorted(d for d in root.iterdir() if d.is_dir())
    if limit_slides:
        slide_dirs = slide_dirs[:limit_slides]
    if not slide_dirs:
        raise SystemExit(f"No per-slide folders under {root}. Point this at the "
                         "'analysis-files' directory a Data Pull wrote.")

    Xs, names, cs, ps, slides, counts, skipped = [], [], [], [], [], [], []
    for d in slide_dirs:
        pair = _find_pair(d)
        if not pair:
            skipped.append((d.name, "no .h5 with features/coords"))
            continue
        fpath, ppath = pair
        with h5py.File(str(fpath), "r") as f:
            fk = _first_key(f, FEATURE_KEYS)
            n_feat = f[fk].shape[0]
        with h5py.File(str(ppath), "r") as f:
            n_coord = f["coords"].shape[0]
            size = int(f["coords"].attrs.get(
                "patch_size_level0", f["coords"].attrs.get("patch_size", 256)))

        n = min(n_feat, n_coord)          # exactly what SlideCap does
        if n_feat != n_coord and verbose:
            print(f"  {d.name}: features {n_feat} vs coords {n_coord} — using {n}")
        if n == 0:
            skipped.append((d.name, "no patches"))
            continue

        take = np.arange(n)
        if max_per_slide and n > max_per_slide:
            # Evenly spaced, so the sample spans the section instead of taking
            # one corner of it. h5py needs an increasing index list.
            take = np.unique(np.linspace(0, n - 1, max_per_slide).astype(int))

        # Read the slide's block sequentially and subsample in memory. Handing
        # h5py a scattered index list makes it issue hundreds of small reads,
        # which is merely slow on local disk and pathological over SMB. One
        # slide's features are ~60 MB, so holding one at a time costs nothing.
        with h5py.File(str(fpath), "r") as f:
            block = np.asarray(f[_first_key(f, FEATURE_KEYS)][:n], dtype=np.float32)
        Xs.append(block[take] if len(take) != n else block)
        with h5py.File(str(ppath), "r") as f:
            cblock = np.asarray(f["coords"][:n], dtype=np.int32)
        cs.append(cblock[take] if len(take) != n else cblock)
        names.append(np.full(len(take), d.name))
        ps.append(np.full(len(take), size, dtype=np.int32))
        slides.append(d.name)
        counts.append(int(len(take)))

    if not Xs:
        raise SystemExit(f"Found {len(slide_dirs)} folder(s) but no usable features.")

    dims = {x.shape[1] for x in Xs}
    if len(dims) > 1:
        raise SystemExit(f"Slides disagree on feature dimension: {sorted(dims)}. "
                         "They must come from the same analysis configuration.")

    out = Pull(X=np.concatenate(Xs), slide=np.concatenate(names),
               coords=np.concatenate(cs), patch_size=np.concatenate(ps),
               slides=slides, counts=counts)
    if verbose:
        print(f"{out.X.shape[0]:,} patches × {out.X.shape[1]} dims "
              f"across {len(slides)} slide(s)")
        for name, why in skipped:
            print(f"  skipped {name}: {why}")
    return out


def incremental_pca(root: str | Path, n_components: int = 50,
                    limit_slides: Optional[int] = None, verbose: bool = True):
    """
    Reduce a whole pull to `n_components` dims without ever holding it all.

    Two passes over the files -- fit, then transform -- exactly what
    cohort_projection.py does server-side, and for the same reason: 292 slides of
    1536-d float32 is ~17.6 GB, while the same patches at 50 dims are under
    600 MB. Run UMAP on what this returns.

    Returns a Pull whose .X is the reduced matrix.
    """
    from sklearn.decomposition import IncrementalPCA

    root = Path(root)
    slide_dirs = sorted(d for d in root.iterdir() if d.is_dir())
    if limit_slides:
        slide_dirs = slide_dirs[:limit_slides]

    pairs = [(d, _find_pair(d)) for d in slide_dirs]
    pairs = [(d, p) for d, p in pairs if p]

    ipca = IncrementalPCA(n_components=n_components)
    for i, (d, (fpath, _)) in enumerate(pairs, 1):
        with h5py.File(str(fpath), "r") as f:
            key = _first_key(f, FEATURE_KEYS)
            total = f[key].shape[0]
            for start in range(0, total, 8192):
                chunk = np.asarray(f[key][start:start + 8192], dtype=np.float32)
                if chunk.shape[0] >= n_components:
                    ipca.partial_fit(chunk)
        if verbose:
            print(f"  fit {i}/{len(pairs)}", end="\r")

    if verbose:
        print(f"\nfitted {n_components} components; transforming…")

    # Transform per slide too. Calling load_pull() here would pull the full
    # 1536-d matrix into memory, which is the thing this function exists to avoid.
    Xs, names, cs, ps, slides, counts = [], [], [], [], [], []
    for i, (d, (fpath, ppath)) in enumerate(pairs, 1):
        with h5py.File(str(fpath), "r") as f:
            key = _first_key(f, FEATURE_KEYS)
            n_feat = f[key].shape[0]
        with h5py.File(str(ppath), "r") as f:
            n_coord = f["coords"].shape[0]
            size = int(f["coords"].attrs.get(
                "patch_size_level0", f["coords"].attrs.get("patch_size", 256)))
        n = min(n_feat, n_coord)
        if n == 0:
            continue

        with h5py.File(str(fpath), "r") as f:
            key = _first_key(f, FEATURE_KEYS)
            chunks = [ipca.transform(np.asarray(f[key][st:min(st + 8192, n)], dtype=np.float32))
                      for st in range(0, n, 8192)]
        Xs.append(np.concatenate(chunks).astype(np.float32))
        with h5py.File(str(ppath), "r") as f:
            cs.append(np.asarray(f["coords"][:n], dtype=np.int32))
        names.append(np.full(n, d.name))
        ps.append(np.full(n, size, dtype=np.int32))
        slides.append(d.name)
        counts.append(int(n))
        if verbose:
            print(f"  transform {i}/{len(pairs)}", end="\r")

    out = Pull(X=np.concatenate(Xs), slide=np.concatenate(names),
               coords=np.concatenate(cs), patch_size=np.concatenate(ps),
               slides=slides, counts=counts)
    if verbose:
        print(f"\n{out.X.shape[0]:,} patches reduced to {out.X.shape[1]} dims "
              f"({out.X.nbytes / 1e6:.0f} MB)")
    return out


if __name__ == "__main__":
    import sys
    load_pull(sys.argv[1] if len(sys.argv) > 1 else ".")
