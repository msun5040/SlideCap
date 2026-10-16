"""
Turn an externally computed 2D embedding into a SlideCap projection bundle.

SlideCap can already *import* a projection it didn't compute
(scripts/import_projection_bundle.py). This is the other half: it writes the
bundle, so an analysis done in a notebook, on the cluster, or by a collaborator
can be viewed in the workspace without re-running SlideCap's own UMAP.

Input is one .npz holding a row per patch, in a fixed order:

    xy          float   (N, 2)   the embedding — UMAP, t-SNE, PHATE, anything
    slide_hash  str     (N,)     which slide each row came from
    patch_x     int     (N,)     patch position in LEVEL 0 pixels
    patch_y     int     (N,)
    patch_size  int     (N,) or scalar, level-0 pixels (default 256)

and, optionally, any number of clusterings as int16 arrays named
`labels_<name>` (-1 = noise, as in SlideCap's own runs).

    python scripts/make_projection_bundle.py embedding.npz out_dir/ --method umap

Then, on the SlideCap host:

    python scripts/import_projection_bundle.py out_dir/ --cohort-name "..." --dry-run

NOTE the coordinates have to be the SAME patch grid SlideCap reads, or the
WSI overlay will point at the wrong tissue: level-0 pixel coordinates of each
patch's top-left corner. If the embedding came from SlideCap-extracted UNI
features, the patch coords in those .h5 files are already in that frame.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.services.cohort_projection import SlideSource, write_artifact  # noqa: E402

BUNDLE_FORMAT = "slidecap-projection-bundle/1"


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 22), b""):
            h.update(chunk)
    return h.hexdigest()


def check_against_patch_files(patch_map: dict, slide_hash, patch_x, patch_y):
    """
    Compare the embedding's coordinates against the patch .h5 files themselves.

    SlideCap takes patch positions straight out of the `coords` dataset of each
    slide's patches .h5 — level-0 top-left pixels, used as-is — and reads
    `patch_size_level0` (else `patch_size`, else 256) from that dataset's attrs.
    So the only coordinates guaranteed to line up are the ones from that file.

    Row ORDER is what usually goes wrong: drop background patches, shuffle, or
    use all the features when coords is shorter, and every point lands on the
    wrong tissue while the plot still looks perfectly reasonable. Comparing
    row-for-row is the only way to catch that.

    Returns coordinates and patch sizes taken from the files.
    """
    import h5py

    out_x = np.empty_like(patch_x)
    out_y = np.empty_like(patch_y)
    out_size = np.empty(len(patch_x), dtype=np.int32)
    problems = []

    for h in dict.fromkeys(slide_hash.tolist()):
        rows = np.flatnonzero(slide_hash == h)
        path = patch_map.get(h)
        if not path:
            problems.append(f"{h[:12]}: no entry in the patch map")
            continue
        with h5py.File(str(path), "r") as f:
            key = "coords" if "coords" in f else next(iter(f.keys()))
            coords = np.asarray(f[key][:], dtype=np.int64)
            attrs = f[key].attrs
            size = int(attrs.get("patch_size_level0", attrs.get("patch_size", 256)))

        if coords.ndim != 2 or coords.shape[1] != 2:
            problems.append(f"{h[:12]}: coords has shape {coords.shape}, expected (N, 2)")
            continue
        if coords.shape[0] < len(rows):
            problems.append(f"{h[:12]}: {len(rows)} embedding rows but only "
                            f"{coords.shape[0]} coords in the patch file")
            continue
        if coords.shape[0] != len(rows):
            print(f"  {h[:12]}: {len(rows)} rows vs {coords.shape[0]} coords — "
                  "assuming the embedding used the first N, as SlideCap does")

        want = coords[: len(rows)]
        got = np.stack([patch_x[rows], patch_y[rows]], axis=1).astype(np.int64)
        bad = int((want != got).any(axis=1).sum())
        if bad:
            problems.append(f"{h[:12]}: {bad}/{len(rows)} coordinates differ from the patch file")
        out_x[rows], out_y[rows] = want[:, 0], want[:, 1]
        out_size[rows] = size

    if problems:
        print("\nCoordinates do NOT match the patch files:")
        for p in problems:
            print(f"  {p}")
        raise SystemExit(
            "\nRefusing to write a bundle whose patches would land on the wrong tissue.\n"
            "Most likely the embedding's rows were filtered or reordered relative to the\n"
            "features. Carry the coords through the same mask/permutation as the features."
        )

    print(f"Coordinates match the patch files for all {len(set(slide_hash.tolist()))} slide(s).")
    return out_x, out_y, out_size


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("npz", type=Path, help="Embedding, one row per patch.")
    ap.add_argument("out", type=Path, help="Bundle directory to write.")
    ap.add_argument("--method", default="umap",
                    help="Label shown in the workspace (umap, tsne, phate, ...).")
    ap.add_argument("--params", default="{}",
                    help="JSON recorded as provenance, e.g. '{\"n_neighbors\": 15}'.")
    ap.add_argument("--feature-dim", type=int, default=0,
                    help="Dimensionality the embedding came from, for the record.")
    ap.add_argument("--patches", type=Path, default=None,
                    help="JSON map {slide_hash: path/to/<stem>_patches.h5}. With it, patch "
                         "coordinates are taken from (or checked against) the same files "
                         "SlideCap reads, which is the only way to be sure they line up.")
    args = ap.parse_args()

    d = np.load(args.npz, allow_pickle=False)
    for key in ("xy", "slide_hash", "patch_x", "patch_y"):
        if key not in d:
            raise SystemExit(f"{args.npz} has no '{key}' array (found: {list(d.keys())})")

    xy = np.asarray(d["xy"], dtype=np.float32)
    if xy.ndim != 2 or xy.shape[1] != 2:
        raise SystemExit(f"xy must be (N, 2), got {xy.shape}")
    n = xy.shape[0]

    slide_hash = np.asarray(d["slide_hash"]).astype(str)
    patch_x = np.asarray(d["patch_x"], dtype=np.int32)
    patch_y = np.asarray(d["patch_y"], dtype=np.int32)
    for name, arr in (("slide_hash", slide_hash), ("patch_x", patch_x), ("patch_y", patch_y)):
        if arr.shape[0] != n:
            raise SystemExit(f"{name} has {arr.shape[0]} rows, xy has {n}")

    psize = d["patch_size"] if "patch_size" in d else 256
    psize = np.asarray(psize, dtype=np.int32)

    if args.patches:
        patch_x, patch_y, psize = check_against_patch_files(
            json.loads(args.patches.read_text()), slide_hash, patch_x, patch_y)

    # The artifact stores slides as contiguous runs, so rows have to be grouped
    # by slide. Sort rather than reject: an external tool has no reason to know.
    order = np.argsort(slide_hash, kind="stable")
    if not np.array_equal(order, np.arange(n)):
        print("Rows were interleaved across slides — sorting into per-slide runs.")
        xy, slide_hash, patch_x, patch_y = xy[order], slide_hash[order], patch_x[order], patch_y[order]
        if psize.ndim:
            psize = psize[order]

    hashes, starts, counts = np.unique(slide_hash, return_index=True, return_counts=True)
    run = np.argsort(starts)
    hashes, starts, counts = hashes[run], starts[run], counts[run]

    sources, slide_idx = [], np.empty(n, dtype=np.int32)
    for i, (h, start, count) in enumerate(zip(hashes, starts, counts)):
        sources.append(SlideSource(
            slide_hash=str(h), features_h5=Path(), patches_h5=Path(),
            n_patches=int(count),
            patch_size=int(psize[start]) if psize.ndim else int(psize),
        ))
        slide_idx[start:start + count] = i

    args.out.mkdir(parents=True, exist_ok=True)
    artifact = args.out / "projection.bin"
    write_artifact(artifact, xy, slide_idx, patch_x, patch_y, sources,
                   method=args.method, params=json.loads(args.params),
                   feature_dim=args.feature_dim)

    checksums = {artifact.name: sha256(artifact)}
    clusterings = []
    for key in d.files:
        if not key.startswith("labels_"):
            continue
        labels = np.asarray(d[key], dtype=np.int16)
        if labels.shape[0] != n:
            raise SystemExit(f"{key} has {labels.shape[0]} rows, xy has {n}")
        labels = labels[order]
        fname = f"{key}.i16"
        (args.out / fname).write_bytes(labels.astype("<i2").tobytes())
        checksums[fname] = sha256(args.out / fname)
        clusterings.append({
            "algorithm": "imported",
            "labels": fname,
            "params": {"label": key[len("labels_"):]},
            "n_clusters": int(labels.max()) + 1 if labels.size else 0,
            "n_noise": int((labels < 0).sum()),
        })
        print(f"  clustering '{key[len('labels_'):]}': "
              f"{clusterings[-1]['n_clusters']} clusters, {clusterings[-1]['n_noise']} noise")

    manifest = {
        "format": BUNDLE_FORMAT,
        "projection": {
            "method": args.method,
            "params": json.loads(args.params),
            "point_count": int(n),
            "feature_dim": args.feature_dim,
            "artifact": artifact.name,
            "slide_hashes": [str(h) for h in hashes],
        },
        "clusterings": clusterings,
        "checksums": checksums,
    }
    (args.out / "bundle.json").write_text(json.dumps(manifest, indent=2))

    print(f"\n{args.method} · {len(hashes)} slides · {n:,} patches → {args.out}")
    print("Next, on the SlideCap host:")
    print(f"  python scripts/import_projection_bundle.py {args.out} --cohort-name \"...\" --dry-run")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
