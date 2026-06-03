"""
Generate a two-panel UMAP + spatial figure for a single slide's UNI features.

Reads ``features_uni_v2/<stem>.h5`` and the matching ``patches/<stem>_patches.h5``,
runs UMAP on the 1536-d UNI v2 embeddings, runs KMeans on the embeddings to
discover clusters, then draws:

  • LEFT  — 2D UMAP scatter, points colored by cluster id
  • RIGHT — each patch drawn at its level-0 slide coordinates, same coloring

The point: see whether embedding-space clusters correspond to spatial
regions on the WSI (they often do — tumor / stroma / necrosis / etc tend
to occupy contiguous areas).

Usage:
    python backend/scripts/uni_umap_figure.py \\
        --uni-dir "/Volumes/.../analyses/<hash>/UNI" \\
        --output uni_clusters.png

Or for the demo's SL00665 anchor:
    python backend/scripts/uni_umap_figure.py --demo-anchor SL00665
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path


def find_features_h5(uni_dir: Path) -> tuple[Path, Path]:
    """Locate (features.h5, patches.h5) under a UNI output dir."""
    feats = list(uni_dir.rglob("features_uni_v2/*.h5"))
    if not feats:
        raise SystemExit(f"no features_uni_v2/*.h5 under {uni_dir}")
    f = feats[0]
    # patches/<stem>_patches.h5 lives one level up under patches/
    stem = f.stem
    patches = f.parent.parent / "patches" / f"{stem}_patches.h5"
    if not patches.exists():
        # fall back: any *_patches.h5
        ps = list(uni_dir.rglob(f"patches/*_patches.h5"))
        if not ps:
            raise SystemExit(f"no patches/*_patches.h5 under {uni_dir}")
        patches = ps[0]
    return f, patches


def load_features(features_h5: Path):
    """(N, D) float32 embeddings."""
    import h5py
    import numpy as np
    with h5py.File(str(features_h5), "r") as h:
        for key in ("features", "embeddings", "feats"):
            if key in h:
                return np.asarray(h[key][:], dtype=np.float32)
    raise SystemExit(f"no features dataset in {features_h5}")


def load_patch_coords(patches_h5: Path):
    """(N, 2) coords in level-0 slide pixels + patch_size at level 0."""
    import h5py
    import numpy as np
    with h5py.File(str(patches_h5), "r") as h:
        coords = np.asarray(h["coords"][:], dtype=np.int64)
        attrs = dict(h["coords"].attrs)
        patch_size = int(attrs.get("patch_size_level0", attrs.get("patch_size", 256)))
        w = int(attrs.get("level0_width", 0)) or None
        ht = int(attrs.get("level0_height", 0)) or None
    return coords, patch_size, w, ht


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    src = ap.add_mutually_exclusive_group(required=True)
    src.add_argument("--uni-dir", type=Path, help="UNI output dir (top of analyses/<hash>/UNI)")
    src.add_argument("--demo-anchor", choices=["SL00665"], help="Use the demo anchor slide")
    ap.add_argument("--k", type=int, default=8, help="Number of KMeans clusters (default 8)")
    ap.add_argument("--output", type=Path, default=Path("uni_clusters.svg"),
                    help="Output figure (extension picks the format: .svg .png .pdf)")
    ap.add_argument("--dpi", type=int, default=150)
    ap.add_argument("--inset-loc", choices=["upper right", "upper left", "lower right", "lower left"],
                    default="upper right", help="Where to place the UMAP inset on the spatial panel")
    args = ap.parse_args()

    if args.demo_anchor == "SL00665":
        # Default to the redacted symlink farm so the figure honors the
        # demo's PHI redaction (filename doesn't appear, the path is local).
        local = Path(os.path.expanduser("~/.slidecap-demo/anchor-redacted/CS00115/UNI"))
        if not local.exists():
            # Fall back to NETWORK_ROOT — works if the symlink farm hasn't been built yet.
            local = Path(
                "/Volumes/DFCI-LIGONLAB/Ligon Lab/test_directory_pt_slides/analyses/"
                "27809f96b05085e769d8196c88a3802aef3354ef161ddd9bc048d08219c6e21b/UNI"
            )
        uni_dir = local
    else:
        uni_dir = args.uni_dir

    print(f"reading UNI outputs from {uni_dir}")
    features_h5, patches_h5 = find_features_h5(uni_dir)
    print(f"  features: {features_h5.name}")
    print(f"  patches:  {patches_h5.name}")

    import numpy as np
    features = load_features(features_h5)
    coords, patch_size, w, ht = load_patch_coords(patches_h5)
    n = min(features.shape[0], coords.shape[0])
    features = features[:n]
    coords = coords[:n]
    print(f"  {n} patches × {features.shape[1]}-d embeddings; patch_size_level0={patch_size}")

    # UMAP projection
    print("running UMAP…")
    import umap
    reducer = umap.UMAP(
        n_components=2, n_neighbors=15, min_dist=0.1,
        random_state=42, metric="cosine",
    )
    umap_xy = reducer.fit_transform(features)

    # KMeans clustering directly on the high-dim embeddings (not on the 2D
    # UMAP — clustering the original 1536-d space gives more semantically
    # meaningful groups; UMAP's distortion can fuse or split true clusters).
    print(f"running KMeans (k={args.k})…")
    from sklearn.cluster import KMeans
    km = KMeans(n_clusters=args.k, random_state=42, n_init=10)
    labels = km.fit_predict(features)

    # Figure: square spatial panel as the main canvas, square UMAP inset.
    # Crop the spatial axes to the tissue bounding box (with a margin) so
    # the tissue fills the panel — full-slide bounds would show a tiny blob
    # in a sea of empty space.
    print(f"rendering figure → {args.output}")
    import matplotlib.pyplot as plt
    from mpl_toolkits.axes_grid1.inset_locator import inset_axes
    palette = plt.colormaps.get_cmap("tab10").resampled(args.k)

    fig, ax_s = plt.subplots(figsize=(8, 8), dpi=args.dpi)

    # Spatial: each patch drawn at its center, marker size = patch level-0 px.
    # Use marker='s' (square) so adjacent patches look like a contiguous grid
    # at fit-to-axes zoom — matches what the patch-extraction step actually
    # produced on the slide.
    centers_x = coords[:, 0] + patch_size / 2
    centers_y = coords[:, 1] + patch_size / 2

    # Tissue bounding box + margin (5% of the larger span)
    min_x, max_x = centers_x.min(), centers_x.max()
    min_y, max_y = centers_y.min(), centers_y.max()
    span = max(max_x - min_x, max_y - min_y)
    margin = max(span * 0.05, patch_size)
    lo_x, hi_x = min_x - margin, max_x + margin
    lo_y, hi_y = min_y - margin, max_y + margin
    # Make the visible region square so equal-aspect scatter fills the
    # square panel without letterboxing.
    cx, cy = (lo_x + hi_x) / 2, (lo_y + hi_y) / 2
    half = max(hi_x - lo_x, hi_y - lo_y) / 2
    lo_x, hi_x = cx - half, cx + half
    lo_y, hi_y = cy - half, cy + half

    # Scale square-marker size so each patch covers roughly its true area
    # on the cropped axes. matplotlib's `s` is point² area; convert from
    # data units → display points using the axes width.
    fig_width_inches = 8.0
    points_per_data_unit = (fig_width_inches * 72) / (hi_x - lo_x)
    marker_pt = patch_size * points_per_data_unit
    marker_area = marker_pt ** 2

    ax_s.scatter(centers_x, centers_y, c=labels, cmap=palette,
                 s=marker_area, marker="s", alpha=0.9, linewidths=0)
    ax_s.set_title(f"Patch locations on slide  ({n} patches, k={args.k} clusters)")
    ax_s.set_xlabel("slide x (level-0 px)")
    ax_s.set_ylabel("slide y (level-0 px)")
    ax_s.set_xlim(lo_x, hi_x)
    ax_s.set_ylim(hi_y, lo_y)  # flipped: WSI origin top-left
    ax_s.set_aspect("equal", adjustable="box")
    for s in ("top", "right"):
        ax_s.spines[s].set_visible(False)

    # UMAP inset — square, ~32% of the spatial panel
    ax_u = inset_axes(ax_s, width="32%", height="32%", loc=args.inset_loc,
                      borderpad=1.2)
    ax_u.scatter(umap_xy[:, 0], umap_xy[:, 1], c=labels, cmap=palette,
                 s=6, alpha=0.85, linewidths=0)
    ax_u.set_title("UMAP", fontsize=10, pad=2)
    ax_u.set_aspect("equal", adjustable="box")
    ax_u.tick_params(left=False, bottom=False, labelleft=False, labelbottom=False)
    for s in ("top", "right", "bottom", "left"):
        ax_u.spines[s].set_linewidth(0.5)
    # Translucent backdrop so the inset reads cleanly against the spatial scatter
    ax_u.set_facecolor((1, 1, 1, 0.92))

    # Legend across the bottom
    handles = [
        plt.Line2D([0], [0], marker="o", linestyle="",
                   markerfacecolor=palette(i), markeredgecolor="none",
                   markersize=8, label=f"cluster {i}  (n={int((labels == i).sum())})")
        for i in range(args.k)
    ]
    fig.legend(handles=handles, loc="lower center", ncol=min(args.k, 4),
               frameon=False, bbox_to_anchor=(0.5, -0.02), fontsize=9)

    plt.tight_layout(rect=(0, 0.05, 1, 1))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(str(args.output), bbox_inches="tight")
    print(f"done → {args.output.resolve()}")
    # Per-cluster counts for the log
    for i in range(args.k):
        print(f"  cluster {i}: {(labels == i).sum()} patches")


if __name__ == "__main__":
    sys.exit(main())
