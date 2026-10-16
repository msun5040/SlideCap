"""
Build a synthetic slide library for the demo instance.

The demo mode that already exists redacts accessions **in the browser**, while
the API, the filenames on disk and anything visible in DevTools still carry the
real ones. That is fine for a screen-share with clinicians; it is not fine for a
room of IT staff who may open the network tab. This script sidesteps the problem
entirely: it generates a library that contains no real data at any layer, so
there is nothing to redact.

Everything here is fabricated -- accession numbers come from a reserved X9xxxx
range that the lab does not use, and the images are generated noise shaped to
look like tissue at thumbnail zoom. No real slide is copied, so no real label
image, header or filename can leak.

    python scripts/seed_demo_library.py                 # ~/.slidecap-demo/library
    python scripts/seed_demo_library.py --slides 60 --force
"""
from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import numpy as np
import tifffile
from PIL import Image

# Fabricated accessions only. A reserved range makes it obvious at a glance that
# nothing here came from the hospital system.
ACCESSION_PREFIX = "BS"
RESERVED_SERIES = "X9"
STAINS = ["HNE", "HNE", "HNE", "IHC-GFAP", "IHC-KI67", "IHC-OLIG2", "SPECIAL-RET"]
BLOCKS = ["A1", "A2", "B1", "B2", "C1"]

TILE = 256
MPP_UM = 0.5          # microns per pixel stamped on every demo slide
BASE = 4096          # level 0 edge; 3 further levels are written as subIFDs
LEVELS = 4


def _smooth_noise(rng: np.random.Generator, size: int, cells: int) -> np.ndarray:
    """A coarse random field resized up smoothly — one octave of value noise."""
    small = (rng.random((cells, cells)) * 255).astype(np.uint8)
    return np.asarray(
        Image.fromarray(small).resize((size, size), Image.BICUBIC),
        dtype=np.float32,
    ) / 255.0


def tissue_image(rng: np.random.Generator, size: int) -> np.ndarray:
    """
    An H&E-ish field: smooth tissue regions, denser nuclei inside them, white
    slide elsewhere. Not a real specimen — just something that reads as tissue
    at the zoom levels a demo actually gets looked at.
    """
    # Several octaves summed, coarse to fine, give organic edges instead of the
    # rectangular blocks a single nearest-neighbour upsample produces.
    field = sum(_smooth_noise(rng, size, c) * w
                for c, w in ((4, 0.55), (9, 0.25), (19, 0.13), (41, 0.07)))
    field = (field - field.min()) / (np.ptp(field) + 1e-6)

    tissue = field > 0.46
    # Fade the edge so sections don't end on a hard line.
    edge = np.clip((field - 0.42) / 0.10, 0, 1)[..., None]

    img = np.full((size, size, 3), 244, dtype=np.float32)          # glass
    eosin = np.array([206, 139, 177], dtype=np.float32)            # cytoplasm, pink
    haem = np.array([108, 74, 158], dtype=np.float32)              # nuclei, purple

    # Cytoplasm varies with a finer octave so it isn't a flat wash of colour.
    shade = 0.82 + 0.3 * _smooth_noise(rng, size, 60)[..., None]
    body = np.clip(eosin * shade, 0, 255)
    img = img * (1 - edge) + body * edge

    # Nuclei: small dark dots, only where there is tissue, denser in denser areas.
    density = _smooth_noise(rng, size, 24)
    nuclei = (rng.random((size, size)) < (0.004 + 0.012 * density)) & tissue
    img[nuclei] = haem

    return img.astype(np.uint8)


def write_slide(path: Path, rng: np.random.Generator) -> None:
    """A tiled, pyramidal TIFF under an .svs name — openslide opens it directly."""
    base = tissue_image(rng, BASE)
    levels = [base]
    for _ in range(LEVELS - 1):
        prev = levels[-1]
        levels.append(prev[::2, ::2])          # simple decimation is enough here

    with tifffile.TiffWriter(path, bigtiff=True) as tif:
        opts = dict(tile=(TILE, TILE), compression="jpeg", photometric="rgb")
        # 0.5 um/px = 20000 px/cm. QC and the analyses both reject implausible
        # MPP, so the demo has to carry a believable one.
        px_per_cm = 1 / (MPP_UM / 10_000)
        tif.write(levels[0], subifds=LEVELS - 1,
                  resolution=(px_per_cm, px_per_cm), resolutionunit="CENTIMETER",
                  **opts)
        for lvl in levels[1:]:
            tif.write(lvl, subfiletype=1, **opts)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--root", type=Path, default=Path.home() / ".slidecap-demo" / "library")
    ap.add_argument("--slides", type=int, default=40)
    ap.add_argument("--cases", type=int, default=12)
    ap.add_argument("--years", type=int, nargs="+", default=[2024, 2025])
    ap.add_argument("--force", action="store_true", help="Replace an existing library.")
    ap.add_argument("--keep-db", action="store_true",
                    help="Leave the demo database alone (default: start it empty).")
    args = ap.parse_args()

    slides_root = args.root / "slides"
    if slides_root.exists():
        if not args.force:
            raise SystemExit(f"{slides_root} already exists — pass --force to rebuild it.")
        shutil.rmtree(slides_root)

    # The demo database carries over between runs, and if the demo was ever
    # pointed at the real share it still holds those rows -- stale counts at
    # best, and not what you want behind a screen being shown to visitors.
    # Start clean unless asked otherwise.
    demo_db = args.root.parent / "database.sqlite"
    if demo_db.exists() and not args.keep_db:
        stamp = demo_db.with_name(f"database.replaced-{__import__('datetime').datetime.now():%Y%m%d-%H%M%S}.sqlite")
        demo_db.rename(stamp)
        print(f"Set the old demo database aside: {stamp.name}\n")

    rng = np.random.default_rng(20260101)
    made = 0
    # Spread the remainder so --slides 40 across 12 cases really makes 40, not 36,
    # and cases end up with slightly different slide counts like real ones do.
    base, extra = divmod(args.slides, args.cases)
    counts = [base + (1 if i < extra else 0) for i in range(args.cases)]

    for case_n in range(args.cases):
        year = args.years[case_n % len(args.years)]
        acc = f"{ACCESSION_PREFIX}{str(year)[2:]}-{RESERVED_SERIES}{case_n + 1:04d}"
        year_dir = slides_root / str(year)
        year_dir.mkdir(parents=True, exist_ok=True)
        for n in range(counts[case_n]):
            if made >= args.slides:
                break
            block = BLOCKS[n % len(BLOCKS)]
            stain = STAINS[(case_n + n) % len(STAINS)]
            name = f"{acc}_{block}-{n + 1}_{stain}_{made + 100000:06d}.svs"
            write_slide(year_dir / name, rng)
            made += 1
            print(f"  [{made}/{args.slides}] {name}")

    # The external/ folder the app expects, with one project in it.
    ext = slides_root / "external" / "DEMO-PROJECT"
    ext.mkdir(parents=True, exist_ok=True)
    for i in range(1, 4):
        write_slide(ext / f"demo_outside_{i}.svs", rng)
    print(f"  + 3 external slide(s) in external/DEMO-PROJECT/")

    print(f"\n{made} synthetic slide(s) under {slides_root}")
    print("Nothing here is real: fabricated accessions, generated images, no copied files.")
    print("\nNext:  ./scripts/run-demo.sh")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
