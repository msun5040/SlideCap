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
QC_VERSION = "v1"

# Tissue coverage (% of the thumbnail that is tissue, not background).
TISSUE_FAIL_PCT = 1.0   # below this → fail (effectively blank; empty patch set)
TISSUE_WARN_PCT = 5.0   # 1–5% → warn (sparse, may still be fine)

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

    Uses a simple background test: a pixel counts as tissue when it isn't near-
    white and isn't near-black (slide background is white; scanner borders black).
    Good enough to flag blank/near-blank slides; not a substitute for real masks.
    """
    try:
        import numpy as np
        from openslide import open_slide

        slide = open_slide(str(filepath))
        thumb = slide.get_thumbnail((1024, 1024)).convert("RGB")
        arr = np.asarray(thumb)
        if arr.size == 0:
            return None
        gray = arr.mean(axis=2)
        # tissue = not background-white (<220) and not pure black (>15)
        tissue = (gray < 220) & (gray > 15)
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

    # ── 3. Tissue fraction (the check that catches the empty-patch failure)
    tissue_pct = _tissue_fraction(filepath)
    metrics["tissue_pct"] = round(tissue_pct, 2) if tissue_pct is not None else None
    if tissue_pct is None:
        checks.append({"name": "tissue", "status": "warn", "detail": "could not estimate tissue"})
    elif tissue_pct < TISSUE_FAIL_PCT:
        checks.append({"name": "tissue", "status": "fail", "detail": f"{tissue_pct:.1f}% tissue — effectively blank"})
    elif tissue_pct < TISSUE_WARN_PCT:
        checks.append({"name": "tissue", "status": "warn", "detail": f"{tissue_pct:.1f}% tissue — sparse"})
    else:
        checks.append({"name": "tissue", "status": "pass", "detail": f"{tissue_pct:.1f}% tissue"})

    status = _worst([c["status"] for c in checks])
    return {"status": status, "metrics": metrics, "checks": checks, "qc_version": QC_VERSION}
