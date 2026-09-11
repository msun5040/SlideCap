"""
Local, pre-flight quality control for slides.

Runs cheap CPU-only checks on the SlideCap server *before* a slide is rsynced to
the GPU cluster, so predictably-bad slides (unreadable, no tissue, missing MPP)
never waste a transfer + GPU slot. Deliberately light: it must cost far less than
the analysis it gates, so tissue is estimated from a downscaled thumbnail rather
than by reimplementing the model's patching.

Each check returns pass | warn | fail; the slide's automated status is the worst
of them. A human can later override via the manual_status column (see SlideQC).
"""
from __future__ import annotations

from pathlib import Path
from typing import Optional

# Bump when the checks/thresholds change so cached results can be re-evaluated.
QC_VERSION = "v3"   # v3 adds the level-0 corruption probe

# Corruption probe: how many level-0 windows to read, and how big.
# 8 x 512px is well under a second on a local disk and enough to catch a
# truncated file; it is not a full integrity scan.
PROBE_REGIONS = 8
PROBE_SIZE = 512

# ── Absolute tissue AREA thresholds (mm²) ──
# Percentage-of-slide is a poor gate: a valid needle biopsy is only a few % of a
# big glass slide. Gate on real tissue area instead (computed from MPP), which is
# independent of slide size. A 1 mm needle core is roughly 0.3–1 mm²; a truly
# blank slide is ~0 mm². Tune these to your specimen mix.
TISSUE_AREA_FAIL_MM2 = 0.05   # essentially no tissue → fail
TISSUE_AREA_WARN_MM2 = 0.40   # very small even for a biopsy → warn
# Fallback when MPP is unknown (can't compute area): only fail near-zero coverage,
# never warn, since the percentage denominator (slide size) is unreliable.
TISSUE_PCT_FAIL_FALLBACK = 0.2

# Plausible WSI resolution range, micrometres per pixel at level 0.
MPP_MIN = 0.1   # ~100x
MPP_MAX = 2.0   # coarser than ~5x is suspect for these models

# Minimum sensible level-0 dimension (px).
MIN_DIM = 1000

_STATUS_RANK = {"pass": 0, "warn": 1, "fail": 2}


def _worst(statuses: list[str]) -> str:
    if not statuses:
        return "warn"
    return max(statuses, key=lambda s: _STATUS_RANK.get(s, 1))


def _thumbnail_tissue(filepath: Path):
    """
    Tissue fraction plus a handful of level-0 coordinates that land on tissue.

    Detection is saturation-based (more robust than a plain grayscale cutoff):
    glass background is near-grey/white = low saturation, while stained tissue
    (H&E pink/purple, IHC brown) is colourful = higher saturation. We also count
    any reasonably dark pixel, so faint/pale sections aren't missed. Near-black
    scanner borders are excluded. Good enough to flag blank slides + estimate
    area; not a substitute for a real tissue mask.

    Returns (tissue_pct, sample_points) or (None, []). The points feed the
    corruption probe below — reading where tissue actually is, because that's
    where an analysis will read.
    """
    try:
        import numpy as np
        from openslide import open_slide

        slide = open_slide(str(filepath))
        w0, h0 = slide.dimensions
        thumb = slide.get_thumbnail((1024, 1024)).convert("RGB")
        arr = np.asarray(thumb).astype(np.float32)
        if arr.size == 0:
            return None, []
        mx = arr.max(axis=2)
        mn = arr.min(axis=2)
        sat = np.where(mx > 0, (mx - mn) / np.maximum(mx, 1.0), 0.0)  # 0..1
        gray = arr.mean(axis=2)
        # tissue = colourful (stained) OR moderately dark — but not near-black
        tissue = ((sat > 0.10) | (gray < 210)) & (gray > 15)
        pct = float(tissue.mean() * 100.0)

        # Spread the probe points over tissue rather than clustering them: take
        # every Nth tissue pixel from the flattened mask.
        th, tw = tissue.shape
        ys, xs = np.nonzero(tissue)
        points: list = []
        if len(xs) > 0:
            step = max(1, len(xs) // PROBE_REGIONS)
            for i in range(0, len(xs), step):
                if len(points) >= PROBE_REGIONS:
                    break
                # thumbnail px -> level-0 px
                points.append((int(xs[i] * w0 / tw), int(ys[i] * h0 / th)))
        # Always probe the far corner too. Tiles are laid out roughly in raster
        # order, so a partially-written file loses the bottom-right first — and
        # tissue-spread points may never reach it.
        points.append((max(0, w0 - PROBE_SIZE), max(0, h0 - PROBE_SIZE)))
        return pct, points
    except Exception:
        return None, []


def _truncation_check(filepath: Path) -> Optional[str]:
    """
    Is the file shorter than its own tile table says it should be?

    An interrupted copy to the network drive is the common way a slide goes bad,
    and it's detectable without decoding anything: walk the TIFF tile offsets and
    byte counts, take the furthest byte any tile claims to occupy, and compare
    against the actual file size. Deterministic, ~instant, and it catches the
    whole truncation class rather than whichever tiles a sampling probe happens
    to land on.

    Returns an error string when truncated, else None (including when the file
    isn't a TIFF we can parse — the region probe still covers that case).
    """
    try:
        import tifffile
    except ImportError:
        return None
    try:
        size = filepath.stat().st_size
        needed = 0
        with tifffile.TiffFile(str(filepath)) as tf:
            for page in tf.pages:
                offs = page.tags.get("TileOffsets") or page.tags.get("StripOffsets")
                cnts = page.tags.get("TileByteCounts") or page.tags.get("StripByteCounts")
                if not offs or not cnts:
                    continue
                o = offs.value if isinstance(offs.value, (list, tuple)) else [offs.value]
                c = cnts.value if isinstance(cnts.value, (list, tuple)) else [cnts.value]
                for a, b in zip(o, c):
                    end = int(a) + int(b)
                    if end > needed:
                        needed = end
        if needed and size < needed:
            short = needed - size
            return (f"file is truncated — tile table needs {needed:,} bytes but the "
                    f"file is {size:,} ({short:,} bytes missing). Re-copy the slide.")
    except Exception:
        # Unparseable as TIFF, or an exotic layout — not our call to fail it here.
        return None
    return None


def _probe_regions(filepath: Path, points: list) -> tuple:
    """
    Read small level-0 regions to catch corruption that metadata checks miss.

    This exists because of a real failure: a slide passed QC, was transferred,
    took a GPU slot, and then died mid-segmentation with

        OpenSlideError: Corrupt JPEG data: premature end of data segment

    killing the whole batch. Nothing earlier in QC touches those bytes —
    `large_image.open` reads metadata, and `get_thumbnail` is served from a
    low-res pyramid level that can be perfectly intact while a level-0 tile is
    truncated.

    Deliberately uses openslide's read_region, the same call the analysis
    pipeline makes (trident OpenSlideWSI.read_region), so a slide that will
    break there breaks here instead — on cheap CPU, before the transfer.

    Limits worth knowing: this samples, it does not verify the whole file, and
    openslide only raises on damage its decoder chokes on. Measured behaviour —
    a truncated file raises, but tile bytes that were overwritten in place
    decode to garbage without error. `_truncation_check` covers the first case
    deterministically; nothing cheap covers the second.

    Returns (status, detail).
    """
    if not points:
        return "warn", "no tissue found to probe"
    try:
        from openslide import open_slide
        from openslide.lowlevel import OpenSlideError
    except ImportError:
        return "warn", "openslide unavailable — could not probe for corruption"

    try:
        slide = open_slide(str(filepath))
        w0, h0 = slide.dimensions
    except Exception as e:
        return "fail", f"could not open for probing: {type(e).__name__}: {e}"[:200]

    read = 0
    for (x, y) in points:
        # Keep the window inside the slide, or openslide pads rather than reads.
        px = max(0, min(x, w0 - PROBE_SIZE))
        py = max(0, min(y, h0 - PROBE_SIZE))
        try:
            slide.read_region((px, py), 0, (PROBE_SIZE, PROBE_SIZE))
            read += 1
        except OpenSlideError as e:
            return "fail", f"corrupt image data at level-0 ({px}, {py}): {e}"[:200]
        except Exception as e:
            return "fail", f"unreadable region at ({px}, {py}): {type(e).__name__}: {e}"[:200]
    return "pass", f"{read} level-0 regions read cleanly"


def run_qc(slide_hash: str, filepath: Path) -> dict:
    """Run all universal checks. Returns {status, metrics, checks, qc_version}."""
    checks: list[dict] = []
    metrics: dict = {}

    # ── 0. Truncation, before anything tries to decode.
    #        A short file usually fails to open anyway, but the decoder's message
    #        for that is "decoder error -2", which tells nobody anything. Checking
    #        the tile table first turns it into "N bytes missing, re-copy the
    #        slide" — and catches the nastier case where the header survives and
    #        only trailing tiles are gone, which opens fine and dies later on a GPU.
    truncated = _truncation_check(filepath)
    if truncated:
        checks.append({"name": "image_data", "status": "fail", "detail": truncated})
        return {"status": "fail", "metrics": metrics, "checks": checks, "qc_version": QC_VERSION}

    # ── 1. File openable + dimensions (via large_image, same lib the viewer uses)
    try:
        import large_image
        ts = large_image.open(str(filepath))
        md = ts.getMetadata()
        w, h = int(md.get("sizeX") or 0), int(md.get("sizeY") or 0)
        metrics["width"], metrics["height"] = w, h
        metrics["magnification"] = md.get("magnification")
        checks.append({"name": "file_openable", "status": "pass", "detail": "opened OK"})
        if w < MIN_DIM or h < MIN_DIM:
            checks.append({"name": "dimensions", "status": "warn", "detail": f"small: {w}x{h}"})
        else:
            checks.append({"name": "dimensions", "status": "pass", "detail": f"{w}x{h}"})
        # mm_x is millimetres/pixel → µm/pixel
        mm_x = md.get("mm_x")
        mpp = round(mm_x * 1000.0, 4) if mm_x else None
        metrics["mpp"] = mpp
    except Exception as e:
        # Can't even open it — everything else is moot.
        checks.append({"name": "file_openable", "status": "fail", "detail": str(e)[:200]})
        return {"status": "fail", "metrics": metrics, "checks": checks, "qc_version": QC_VERSION}

    # ── 2. MPP / magnification present and in a plausible range
    mpp = metrics.get("mpp")
    if mpp is None:
        checks.append({"name": "mpp", "status": "warn", "detail": "no MPP metadata — patching may misbehave"})
    elif mpp < MPP_MIN or mpp > MPP_MAX:
        checks.append({"name": "mpp", "status": "warn", "detail": f"MPP {mpp} µm/px outside [{MPP_MIN}, {MPP_MAX}]"})
    else:
        checks.append({"name": "mpp", "status": "pass", "detail": f"{mpp} µm/px"})

    # ── 3. Tissue — gate on absolute AREA (mm²), not % of slide, so small
    #        biopsies aren't penalised for sitting on a big glass slide.
    tissue_pct, probe_points = _thumbnail_tissue(filepath)
    metrics["tissue_pct"] = round(tissue_pct, 2) if tissue_pct is not None else None
    area_mm2 = None
    if tissue_pct is not None and mpp and w and h:
        # tissue pixels at full res × (µm/px)² → µm² → mm²
        tissue_px = (tissue_pct / 100.0) * w * h
        area_mm2 = round(tissue_px * (mpp ** 2) / 1e6, 3)
    metrics["tissue_area_mm2"] = area_mm2

    if tissue_pct is None:
        checks.append({"name": "tissue", "status": "warn", "detail": "could not estimate tissue"})
    elif area_mm2 is not None:
        if area_mm2 < TISSUE_AREA_FAIL_MM2:
            checks.append({"name": "tissue", "status": "fail", "detail": f"{area_mm2} mm² tissue — effectively blank"})
        elif area_mm2 < TISSUE_AREA_WARN_MM2:
            checks.append({"name": "tissue", "status": "warn", "detail": f"{area_mm2} mm² tissue — very small (biopsy?)"})
        else:
            checks.append({"name": "tissue", "status": "pass", "detail": f"{area_mm2} mm² tissue ({tissue_pct:.1f}%)"})
    else:
        # No MPP → can't compute area; only fail near-zero coverage.
        if tissue_pct < TISSUE_PCT_FAIL_FALLBACK:
            checks.append({"name": "tissue", "status": "fail", "detail": f"{tissue_pct:.1f}% tissue, no MPP — effectively blank"})
        else:
            checks.append({"name": "tissue", "status": "pass", "detail": f"{tissue_pct:.1f}% tissue (no MPP for area)"})

    # ── 4. Image-data integrity — read real level-0 regions.
    #        Everything above this point reads metadata or a low-res pyramid
    #        level, which stays readable on a file whose full-resolution tiles
    #        are truncated. This is the check that catches that.
    probe_status, probe_detail = _probe_regions(filepath, probe_points)
    checks.append({"name": "image_data", "status": probe_status, "detail": probe_detail})

    status = _worst([c["status"] for c in checks])
    return {"status": status, "metrics": metrics, "checks": checks, "qc_version": QC_VERSION}
