"""
Figure out why a slide file won't open in the viewer.

Usage:
    python scripts/diagnose_slide_source.py "L:\\Ligon Lab\\...\\slide.tif"

Prints, in order:
  1. what tifffile sees inside the file (tiled? pyramid levels? compression?)
  2. which large_image sources are installed on this machine
  3. the result of opening the file with each source, one by one

The viewer (`_get_ts` in app/main.py) walks the same source list, so whatever
succeeds here is what the viewer will use — and if everything fails, the error
printed here is exactly the one the DZI endpoint returns as a 500.
"""
from __future__ import annotations

import sys
from pathlib import Path


def describe_tiff(path: Path) -> None:
    print("── TIFF structure ──")
    try:
        import tifffile
    except ImportError:
        print("  tifffile not installed — skipping")
        return
    try:
        with tifffile.TiffFile(str(path)) as tf:
            print(f"  is_bigtiff={tf.is_bigtiff}  is_ome={tf.is_ome}  is_svs={tf.is_svs}")
            print(f"  series: {len(tf.series)}  pages: {len(tf.pages)}")
            for i, s in enumerate(tf.series[:3]):
                print(f"  series[{i}]: shape={s.shape} dtype={s.dtype} levels={len(s.levels)}")
            p = tf.pages[0]
            print(f"  page[0]: {p.imagewidth}x{p.imagelength} "
                  f"tiled={bool(p.is_tiled)} "
                  f"tile={getattr(p, 'tilewidth', 0)}x{getattr(p, 'tilelength', 0)} "
                  f"compression={p.compression!r} photometric={p.photometric!r}")
            if not p.is_tiled:
                print("  ⚠ NOT TILED — the 'tiff' and 'openslide' sources will both reject this file.")
            if len(tf.series) and len(tf.series[0].levels) < 2:
                print("  ⚠ NO PYRAMID (single resolution level) — viewing will be slow even if it opens.")
    except Exception as e:
        print(f"  tifffile could not read it: {type(e).__name__}: {e}")


def try_sources(path: Path) -> None:
    print("\n── large_image sources ──")
    try:
        import large_image
        from large_image.tilesource import AvailableTileSources
    except Exception as e:
        print(f"  large_image not installed: {e}")
        return

    try:
        installed = sorted(AvailableTileSources.keys())
        print(f"  installed: {', '.join(installed) or '(none)'}")
    except Exception as e:
        print(f"  could not list sources: {e}")

    # Mirrors TILE_SOURCE_ORDER in app/main.py — kept literal here so the
    # script doesn't have to import (and boot) the whole FastAPI app.
    TILE_SOURCE_ORDER = ("tiff", "openslide", "vips", "gdal", "bioformats", "pil")

    print("\n── open attempts ──")
    opened = False
    for source in TILE_SOURCE_ORDER:
        try:
            ts = large_image.open(str(path), sourceName=source, encoding="JPEG")
            md = ts.getMetadata()
            print(f"  ✓ {source}: {md['sizeX']}x{md['sizeY']}, {md['levels']} levels, "
                  f"tile {md.get('tileWidth')}x{md.get('tileHeight')}")
            opened = True
            break
        except Exception as e:
            print(f"  ✗ {source}: {type(e).__name__}: {e}")

    if not opened:
        try:
            ts = large_image.open(str(path), encoding="JPEG")
            md = ts.getMetadata()
            print(f"  ✓ auto: {type(ts).__name__} — {md['sizeX']}x{md['sizeY']}, {md['levels']} levels")
            opened = True
        except Exception as e:
            print(f"  ✗ auto: {type(e).__name__}: {e}")

    if not opened:
        print("\n  Nothing could open this file. Options:")
        print("   a) install another source, e.g.  pip install large-image-source-vips")
        print("   b) convert it to a pyramidal tiled TIFF:")
        print("        vips tiffsave in.tif out.tif --tile --pyramid --compression=jpeg "
              "--tile-width=256 --tile-height=256 --bigtiff")
        print("      (or:  pip install large-image-converter && large_image_converter in.tif out.tif)")


def main() -> int:
    if len(sys.argv) != 2:
        print(__doc__)
        return 2
    path = Path(sys.argv[1])
    if not path.exists():
        print(f"No such file: {path}")
        return 1
    print(f"File: {path}  ({path.stat().st_size / 1024 / 1024:.0f} MB)\n")
    if path.suffix.lower() in ('.tif', '.tiff', '.svs'):
        describe_tiff(path)
    try_sources(path)
    return 0


if __name__ == "__main__":
    # Make `app` importable when running as a plain script
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    raise SystemExit(main())
