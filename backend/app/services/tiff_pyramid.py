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


def needs_pyramid(path: Path) -> bool:
    """
    True if this file is a TIFF the viewer can't serve as-is (not tiled, or
    tiled but with no pyramid). False for scanner formats and for TIFFs that
    are already fine — and False if we can't tell, so callers never convert
    on a guess.
    """
    if path.suffix.lower() in NATIVE_EXTS or path.suffix.lower() not in ('.tif', '.tiff'):
        return False
    try:
        import tifffile
        with tifffile.TiffFile(str(path)) as tf:
            if tf.is_svs or tf.is_ndpi:
                return False
            page = tf.pages[0]
            if not page.is_tiled:
                return True
            return len(tf.series[0].levels) < 2
    except Exception:
        return False


def convert(
    src: Path,
    dst: Path,
    tile: int = TILE,
    quality: int = JPEG_QUALITY,
    progress: Optional[Callable[[str], None]] = None,
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

    say = progress or (lambda _m: None)

    try:
        arr = tifffile.memmap(str(src))
        say(f"memory-mapped {src.name} (uncompressed source)")
    except Exception:
        arr = tifffile.imread(str(src))
        say(f"decoded {src.name} into memory")

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
        say(f"scaling {arr.dtype} -> uint8 (peak {peak:.0f})")
        arr = (arr.astype('float32') / peak * 255).astype('uint8')

    photometric = 'rgb' if arr.ndim == 3 else 'minisblack'
    use_jpeg = jpeg_available() and photometric == 'rgb'
    if use_jpeg:
        comp, comp_args = 'jpeg', {'level': quality}
    else:
        comp, comp_args = 'deflate', {'level': 6}
    say(f"compression: {comp}" + ("" if use_jpeg else " (install imagecodecs for smaller JPEG output)"))

    h0, w0 = arr.shape[:2]
    partial = dst.with_suffix(dst.suffix + '.partial')
    dst.parent.mkdir(parents=True, exist_ok=True)

    levels = 0
    with tifffile.TiffWriter(str(partial), bigtiff=True) as tw:
        level = arr
        while True:
            h, w = level.shape[:2]
            say(f"level {levels}: {w}x{h}")
            tw.write(
                level,
                tile=(tile, tile),
                photometric=photometric,
                compression=comp,
                compressionargs=comp_args,
                # Levels after the first are reduced-resolution pages — how
                # openslide's generic-tiff driver recognizes a pyramid.
                subfiletype=1 if levels else 0,
            )
            levels += 1
            if max(h, w) <= tile:
                break
            level = np.asarray(
                Image.fromarray(level).resize((max(1, w // 2), max(1, h // 2)), Image.BILINEAR)
            )

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
