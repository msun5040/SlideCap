"""
Convert plain TIFFs into pyramidal tiled TIFFs so the viewer can serve them.

Usage:
    python scripts/convert_to_pyramidal.py "L:\\...\\slide.tif"                 # -> slide.pyramid.tif
    python scripts/convert_to_pyramidal.py "L:\\...\\slide.tif" -o out.tif
    python scripts/convert_to_pyramidal.py "L:\\...\\external\\GBM-project"      # whole folder
    python scripts/convert_to_pyramidal.py <path> --replace                     # overwrite the original

Files that are already tiled + pyramidal, and scanner formats (.svs/.ndpi/.mrxs),
are skipped — run it on a folder as often as you like.

With --replace the original is swapped out only after the new file is written
and verified to open, and the original is kept alongside as `<name>.orig.tif`.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.services import tiff_pyramid  # noqa: E402


def verify(path: Path) -> str:
    """Open the converted file the same way the viewer will."""
    import large_image
    from large_image.tilesource import loadTileSources
    loadTileSources()
    ts = large_image.open(str(path), encoding='JPEG')
    md = ts.getMetadata()
    return f"{md['sizeX']}x{md['sizeY']}, {md['levels']} levels, tile {md.get('tileWidth')}"


def convert_one(src: Path, out: Path | None, replace: bool, force: bool) -> bool:
    if not force and not tiff_pyramid.needs_pyramid(src):
        print(f"skip  {src.name} (already tiled + pyramidal, or not a plain TIFF)")
        return False

    dst = out or src.with_suffix('.pyramid.tif')
    print(f"\nconvert {src.name}  ({src.stat().st_size / 1024 / 1024:.0f} MB)")
    info = tiff_pyramid.convert(src, dst, progress=lambda m: print(f"  {m}"))
    print(f"  wrote {dst.name}  ({info['output_size_bytes'] / 1024 / 1024:.0f} MB, "
          f"{info['levels']} levels, {info['compression']})")

    try:
        print(f"  verified: {verify(dst)}")
    except Exception as e:
        print(f"  ⚠ VERIFY FAILED: {type(e).__name__}: {e} — leaving the original alone")
        return False

    if replace:
        backup = src.with_suffix('.orig' + src.suffix)
        src.rename(backup)
        dst.rename(src)
        print(f"  replaced original (kept as {backup.name})")
    return True


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('path', help='a .tif file, or a folder to walk recursively')
    ap.add_argument('-o', '--output', help='output file (single-file mode only)')
    ap.add_argument('--replace', action='store_true',
                    help='swap the converted file in for the original (original kept as *.orig.tif)')
    ap.add_argument('--force', action='store_true', help='convert even if it looks fine already')
    args = ap.parse_args()

    target = Path(args.path)
    if not target.exists():
        print(f"No such path: {target}")
        return 1

    if target.is_file():
        return 0 if convert_one(target, Path(args.output) if args.output else None,
                                args.replace, args.force) else 1

    if args.output:
        print("-o only applies to a single file")
        return 2

    files = [f for f in sorted(target.rglob('*'))
             if f.is_file() and f.suffix.lower() in ('.tif', '.tiff')
             and not f.name.endswith('.orig.tif') and not f.name.endswith('.pyramid.tif')]
    if not files:
        print(f"No .tif files under {target}")
        return 0

    print(f"{len(files)} TIFF(s) under {target}")
    converted = sum(convert_one(f, None, args.replace, args.force) for f in files)
    print(f"\nconverted {converted} of {len(files)}")
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
