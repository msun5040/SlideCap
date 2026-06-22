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
QC_VERSION = "v2"

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


def _tissue_fraction(filepath: Path) -> Optional[float]:
    """Fraction (0–100) of a downscaled whole-slide thumbnail that is tissue.

    Detection is saturation-based (more robust than a plain grayscale cutoff):
    glass background is near-grey/white = low saturation, while stained tissue
    (H&E pink/purple, IHC brown) is colourful = higher saturation. We also count
    any reasonably dark pixel, so faint/pale sections aren't missed. Near-black
    scanner borders are excluded. Good enough to flag blank slides + estimate
    area; not a substitute for a real tissue mask.
    """
    try:
        import numpy as np
        from openslide import open_slide

        slide = open_slide(str(filepath))
        thumb = slide.get_thumbnail((1024, 1024)).convert("RGB")
        arr = np.asarray(thumb).astype(np.float32)
        if arr.size == 0:
            return None
        mx = arr.max(axis=2)
        mn = arr.min(axis=2)
        sat = np.where(mx > 0, (mx - mn) / np.maximum(mx, 1.0), 0.0)  # 0..1
        gray = arr.mean(axis=2)
        # tissue = colourful (stained) OR moderately dark — but not near-black
        tissue = ((sat > 0.10) | (gray < 210)) & (gray > 15)
        return float(tissue.mean() * 100.0)
    except Exception:
        return None


def run_qc(slide_hash: str, filepath: Path) -> dict:
    """Run all universal checks. Returns {status, metrics, checks, qc_version}."""
    checks: list[dict] = []
    metrics: dict = {}

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
    tissue_pct = _tissue_fraction(filepath)
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

    status = _worst([c["status"] for c in checks])
    return {"status": status, "metrics": metrics, "checks": checks, "qc_version": QC_VERSION}
