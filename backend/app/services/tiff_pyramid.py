"""
Convert plain TIFFs into pyramidal, tiled TIFFs the viewer can serve.

Why this exists: the tile pipeline (`_get_ts` in app/main.py) opens slides via
large_image's `tiff` / `openslide` sources. Both need a **tiled** TIFF, and
openslide additionally wants a resolution pyramid. Scanner formats (.svs, .ndpi)
have both. A plain .tif — what QuPath, ImageJ and most one-off exports write —
is striped and single-resolution, so every source rejects it with
"No available tilesource" and the DZI endpoint 500s.

Converting once up front is also the right move performance-wise: without a
pyramid, every zoomed-out view would have to read and downsample the full-res
image on each request.

Output is a classic pyramidal TIFF — each level a separate top-level IFD, 256px
tiles, levels after the first flagged as reduced-resolution. That's the layout
both openslide's generic-tiff driver and large_image's tiff source expect.

Compression is JPEG when the `imagecodecs` package is available (much smaller
files), otherwise deflate (lossless, bigger). Neither adds a hard dependency —
tifffile handles deflate on its own.
"""
from __future__ import annotations

import math
from pathlib import Path
from typing import Callable, Optional

TILE = 256
JPEG_QUALITY = 85

# Formats the viewer opens natively — converting these would only lose quality.
NATIVE_EXTS = {'.svs', '.ndpi', '.mrxs'}


def jpeg_available() -> bool:
    """Whether tifffile can write JPEG-compressed tiles (needs imagecodecs)."""
    try:
        import imagecodecs  # noqa: F401
        return True
    except Exception:
        return False


def pyramid_check(path: Path) -> tuple[bool, str]:
    """
    Decide whether converting this file would make it viewable.

    Returns (should_convert, reason). The reason is always populated — it's
    what the DZI endpoint reports when a file can't be opened AND can't be
    converted, so "nothing happened" is never unexplained.

    Returns False whenever we can't tell, so callers never convert on a guess.
    """
    ext = path.suffix.lower()
    if ext in NATIVE_EXTS:
        return False, f"{ext} is a scanner format — converting it would only lose quality"
    if ext not in ('.tif', '.tiff'):
        return False, f"not a TIFF ({ext or 'no extension'}); only plain TIFFs can be converted"

    try:
        import tifffile
    except ImportError:
        return False, ("the 'tifffile' package isn't installed on this server, so plain TIFFs "
                       "can't be converted — run: pip install tifffile")

    try:
        with tifffile.TiffFile(str(path)) as tf:
            if tf.is_svs or tf.is_ndpi:
                return False, "already a scanner-format TIFF"
            page = tf.pages[0]
            tiled = bool(page.is_tiled)
            try:
                nlevels = len(tf.series[0].levels)
            except Exception:
                nlevels = 1
            if not tiled:
                return True, "plain (untiled) TIFF — converting"
            if nlevels < 2:
                return True, "tiled but single-resolution TIFF — converting"
            return False, (
                f"already tiled with {nlevels} pyramid levels "
                f"(compression={page.compression!r}, {page.imagewidth}x{page.imagelength}) — "
                "converting wouldn't help; the file's compression or layout is what the "
                "tile sources are rejecting"
            )
    except Exception as e:
        return False, f"tifffile couldn't read it either ({type(e).__name__}: {e}) — the file may be corrupt"


def needs_pyramid(path: Path) -> bool:
    """True if converting this file would make it viewable. See pyramid_check."""
    return pyramid_check(path)[0]


def convert(
    src: Path,
    dst: Path,
    tile: int = TILE,
    quality: int = JPEG_QUALITY,
    progress: Optional[Callable[[str, Optional[float]], None]] = None,
) -> dict:
    """
    Write a pyramidal tiled TIFF of `src` to `dst`. Returns a summary dict.

    Writes to `dst.partial` and renames on success, so an interrupted run never
    leaves a half-written file that later looks like a valid cached pyramid.

    Memory: an uncompressed source is memory-mapped (no RAM cost); a compressed
    one is decoded in full, so peak usage is roughly the uncompressed image plus
    half again for the first downsample.
    """
    import numpy as np
    import tifffile
    from PIL import Image

    # progress(message, fraction) — fraction is 0..1, or None when unknown.
    say = progress or (lambda _m, _f=None: None)

    say("reading source image", 0.0)
    try:
        arr = tifffile.memmap(str(src))
        say("memory-mapped (uncompressed source)", 0.05)
    except Exception:
        arr = tifffile.imread(str(src))
        say("decoded source into memory", 0.05)

    # Normalize to something a tiled RGB/grayscale TIFF can hold.
    if arr.ndim == 3 and arr.shape[2] == 4:
        arr = arr[:, :, :3]
    elif arr.ndim == 3 and arr.shape[2] not in (1, 3):
        raise ValueError(f"unsupported channel count: {arr.shape[2]}")
    if arr.ndim == 3 and arr.shape[2] == 1:
        arr = arr[:, :, 0]
    if arr.dtype != np.uint8:
        # The viewer serves 8-bit JPEG regardless; scale rather than clip so a
        # 16-bit scan doesn't come out black.
        peak = float(arr.max()) or 1.0
        say(f"scaling {arr.dtype} -> uint8", 0.08)
        arr = (arr.astype('float32') / peak * 255).astype('uint8')

    photometric = 'rgb' if arr.ndim == 3 else 'minisblack'
    use_jpeg = jpeg_available() and photometric == 'rgb'
    if use_jpeg:
        comp, comp_args = 'jpeg', {'level': quality}
    else:
        comp, comp_args = 'deflate', {'level': 6}
    h0, w0 = arr.shape[:2]
    # Levels are written until the largest side fits in one tile — known up
    # front, so the UI gets a real percentage rather than a spinner.
    total_levels = 1 if max(h0, w0) <= tile else math.ceil(math.log2(max(h0, w0) / tile)) + 1
    say(f"writing {total_levels} levels ({comp})", 0.1)
    partial = dst.with_suffix(dst.suffix + '.partial')
    dst.parent.mkdir(parents=True, exist_ok=True)

    # Each level has a quarter the pixels of the one before, so level 1 alone is
    # ~75% of the work. Weight progress by pixels, or the bar sits at 10% for
    # most of the conversion and then leaps to done. Capped below 100 so it
    # never reads as finished while the last levels are still being written.
    total_work = sum(0.25 ** k for k in range(total_levels))
    done_work = 0.0

    levels = 0
    with tifffile.TiffWriter(str(partial), bigtiff=True) as tw:
        level = arr
        while True:
            h, w = level.shape[:2]
            start = 0.1 + 0.9 * (done_work / total_work)

            say(f"level {levels + 1} of {total_levels} ({w}x{h})", min(0.97, start))

            def tile_rows(a=level):
                """
                Yield the level's tiles in row-major order. Feeding tifffile a
                generator instead of the whole array keeps a memory-mapped
                source lazy — tiles are read as they're consumed. tifffile pads
                partial edge tiles itself.

                Progress is reported per level, not per row: tifffile compresses
                on a thread pool and drains this generator almost instantly, so
                per-row reporting would race to ~76% and then sit there. Coarse
                and true beats smooth and wrong; the UI shows elapsed time so a
                long level still reads as "working".
                """
                H, W = a.shape[:2]
                for y in range(0, H, tile):
                    for x in range(0, W, tile):
                        yield a[y:y + tile, x:x + tile]

            tw.write(
                tile_rows(),
                shape=level.shape,
                dtype=level.dtype,
                tile=(tile, tile),
                photometric=photometric,
                compression=comp,
                compressionargs=comp_args,
                # Levels after the first are reduced-resolution pages — how
                # openslide's generic-tiff driver recognizes a pyramid.
                subfiletype=1 if levels else 0,
            )
            done_work += 0.25 ** levels
            levels += 1
            if max(h, w) <= tile:
                break
            level = np.asarray(
                Image.fromarray(level).resize((max(1, w // 2), max(1, h // 2)), Image.BILINEAR)
            )

    say("done", 1.0)
    partial.replace(dst)
    return {
        "source": str(src),
        "output": str(dst),
        "width": w0,
        "height": h0,
        "levels": levels,
        "tile_size": tile,
        "compression": comp,
        "output_size_bytes": dst.stat().st_size,
    }
