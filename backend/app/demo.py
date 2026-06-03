"""
Demo-mode helpers.

Active only when settings.APP_MODE == "demo". Provides:

- seed_demo_analyses: register the CellViT pipeline so it shows up in the
  Analysis tab without manual setup.
- mock cluster connect/status responses (no SSH).
- prepare_demo_outputs: write a directory of sample CellViT output files
  the UI can read after a "completed" demo job.
- simulate_job_run: background thread that walks a job through
  pending -> transferring -> running -> completed on a 5s + 5s timer,
  pointing each slide's local_output_path at prepare_demo_outputs().
- mock_staging_scan / start_demo_sort: fake staging directory full of
  SL-prefixed slides + a fake sort animation that ticks the existing
  _sort_state so the frontend progress UI lights up.
"""
from __future__ import annotations

import json
import math
import random
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

from .config import settings
from .db import AnalysisJob, JobSlide, Analysis, get_session


# ── Cluster mocks ────────────────────────────────────────────────────────

_FAKE_GPUS = [
    {
        "index": 0,
        "name": "NVIDIA A100-SXM4-40GB",
        "memory_used_mb": 1024,
        "memory_total_mb": 40960,
        "utilization_pct": 12,
    },
    {
        "index": 1,
        "name": "NVIDIA A100-SXM4-40GB",
        "memory_used_mb": 28672,
        "memory_total_mb": 40960,
        "utilization_pct": 73,
    },
    {
        "index": 2,
        "name": "NVIDIA A100-SXM4-40GB",
        "memory_used_mb": 512,
        "memory_total_mb": 40960,
        "utilization_pct": 3,
    },
    {
        "index": 3,
        "name": "NVIDIA A100-SXM4-40GB",
        "memory_used_mb": 8192,
        "memory_total_mb": 40960,
        "utilization_pct": 41,
    },
]


def fake_gpus() -> list[dict]:
    return [dict(g) for g in _FAKE_GPUS]


def mock_cluster_connect(host: str, username: str) -> dict:
    return {
        "connected": True,
        "host": host or "demo.cluster.local",
        "username": username or "demo",
        "message": f"Connected to {host or 'demo.cluster.local'} as {username or 'demo'} (demo)",
        "gpus": fake_gpus(),
    }


def mock_cluster_status(connected: bool) -> dict:
    if not connected:
        return {"connected": False}
    return {
        "connected": True,
        "host": "demo.cluster.local",
        "username": "demo",
        "gpus": fake_gpus(),
    }


# ── Demo output files ───────────────────────────────────────────────────

# Class palette mirrors the upstream CellViT class list. Probabilities are
# what makes the demo overlay *look* like a tumor section under the
# microscope: dense tumor, scattered stroma, occasional lymphocyte clusters,
# rare necrosis. Tune these if the demo starts looking unrealistic.
_CELL_CLASSES = [
    ("Tumor",       0.55, (200,   0,   0)),
    ("Stroma",      0.22, (0,   200,   0)),
    ("Lymphocyte",  0.14, (0,     0, 200)),
    ("Necrosis",    0.06, (120, 120, 120)),
    ("Other",       0.03, (180, 180,   0)),
]


def _slide_dimensions(slide_hash: str) -> tuple[int, int]:
    """
    Return (width, height) in level-0 pixels for the slide. Falls back to a
    50k × 50k canvas if openslide can't open the file — the demo still works
    on a machine where the WSI isn't present, the overlay just won't line up
    with real tissue.
    """
    try:
        from . import main as _main  # local import: avoid circular at module load
        filepath = _main.indexer.get_filepath(slide_hash) if _main.indexer else None
        if filepath and filepath.exists():
            from openslide import open_slide
            slide = open_slide(str(filepath))
            try:
                return int(slide.dimensions[0]), int(slide.dimensions[1])
            finally:
                slide.close()
    except Exception as e:
        print(f"[demo] couldn't read dims for {slide_hash[:12]}: {e}")
    return 50_000, 50_000


def _pick_class(rng: random.Random) -> tuple[str, tuple[int, int, int]]:
    """Sample from _CELL_CLASSES weighted by probability."""
    r = rng.random()
    cum = 0.0
    for name, p, color in _CELL_CLASSES:
        cum += p
        if r <= cum:
            return name, color
    name, _, color = _CELL_CLASSES[-1]
    return name, color


def _cell_polygon(cx: float, cy: float, radius: float, rng: random.Random) -> list[list[float]]:
    """Build a ~circular polygon (8 vertices, slightly jittered) at (cx, cy)."""
    pts: list[list[float]] = []
    for i in range(8):
        angle = (2 * math.pi * i) / 8 + rng.uniform(-0.1, 0.1)
        r = radius * rng.uniform(0.85, 1.15)
        pts.append([round(cx + r * math.cos(angle), 1), round(cy + r * math.sin(angle), 1)])
    pts.append(pts[0])  # close the ring
    return pts


def _generate_demo_cellvit_geojson(
    slide_hash: str,
    slide_label: str,
    n_cells: int = 600,
    n_clusters: int = 6,
) -> str:
    """
    Build a realistic-shaped CellViT GeoJSON for one demo slide.

    Cells are placed in `n_clusters` Gaussian blobs scattered across the slide
    so the overlay looks like clumps of tumor with surrounding stroma rather
    than uniform noise. The seed is derived from `slide_hash` so re-runs of
    the same demo slide produce the same overlay (good for screenshots).
    """
    width, height = _slide_dimensions(slide_hash)
    # Stable seed per slide — same overlay on re-run, different overlays across slides.
    rng = random.Random(int(slide_hash[:8], 16))

    # Pick cluster centers somewhere inside the central 80% of the slide so
    # they land on tissue rather than out-of-bounds black border.
    margin_x = width * 0.1
    margin_y = height * 0.1
    centers = [
        (rng.uniform(margin_x, width - margin_x), rng.uniform(margin_y, height - margin_y))
        for _ in range(n_clusters)
    ]
    # Spread of each cluster ~ 5% of the slide; tighter than that and they're dots.
    sigma = min(width, height) * 0.05

    features: list[dict] = []
    for i in range(n_cells):
        cx0, cy0 = rng.choice(centers)
        cx = max(0.0, min(width, rng.gauss(cx0, sigma)))
        cy = max(0.0, min(height, rng.gauss(cy0, sigma)))
        # Cell radius in level-0 px — at 0.25 mpp that's ~5-8 µm, ballpark for a nucleus.
        radius = rng.uniform(12, 22)
        name, color = _pick_class(rng)
        features.append({
            "type": "Feature",
            "id": i,
            "geometry": {
                "type": "Polygon",
                "coordinates": [_cell_polygon(cx, cy, radius, rng)],
            },
            "properties": {
                "classification": {"name": name, "color": list(color)},
                "object_type": "detection",
            },
        })

    return json.dumps({"type": "FeatureCollection", "features": features})


_DEMO_OUTPUT_FILES = {
    "cell_stats.json": lambda slide_label, slide_hash: json.dumps({
        "slide": slide_label,
        "total_cells": 184523,
        "by_class": {
            "Tumor": 92341,
            "Stroma": 41203,
            "Lymphocyte": 27815,
            "Necrosis": 14502,
            "Other": 8662,
        },
        "tumor_density_per_mm2": 4123,
        "tils_score": 0.187,
    }, indent=2),
    "summary.csv": lambda slide_label, slide_hash: (
        "slide,total_cells,tumor,stroma,lymphocyte,necrosis,other\n"
        f"{slide_label},184523,92341,41203,27815,14502,8662\n"
    ),
    "run.log": lambda slide_label, slide_hash: (
        f"[demo] CellViT SAM-H-x40 run for {slide_label}\n"
        "[demo] Loading checkpoint: CellViT-SAM-H-x40-AMP.pth\n"
        "[demo] Tiling slide @ 0.25 mpp ...\n"
        "[demo] 2483 tiles -> inference\n"
        "[demo] Inference complete in 247.3s\n"
        "[demo] Postprocessing: merging cell instances...\n"
        f"[demo] {{'total_cells': 184523, 'Tumor': 92341, 'Stroma': 41203, "
        "'Lymphocyte': 27815, 'Necrosis': 14502, 'Other': 8662}\n"
        "[demo] Done.\n"
    ),
}


# ── Anchor slide overrides ─────────────────────────────────────────────
#
# Some slides in the demo are wired to REAL analysis outputs (from actual
# cluster runs) instead of the procedural fakes. This lets users see what a
# true overlay or UMAP looks like — cells tracing real nuclei, embeddings
# from genuine UNI features — without having to run the cluster.
#
# Demo and prod share NETWORK_ROOT and the salt, so these slides hash to the
# same value in both DBs. The hash is the stable identifier across re-seeds
# (the SL counter is per-DB, so SL IDs may differ). Map for reference:
#
#   CellViT anchors:
#     d41e68c0... → BS22-D76390 A1-1 H&E (prod: SL00219, demo: SL00552)
#     3df0b9f7... → BS20-E02024 A1-1 H&E (demo: SL00393)
#   UNI anchors:
#     27809f96... → BS23-W44115 A1-1 H&E (demo: SL00665)
#
# To add a slide: run the real analysis (output lands under
# NETWORK_ROOT/analyses/<hash>/<kind>/...), then append the hash to the
# appropriate set below.
_REAL_CELLVIT_SLIDE_HASHES: set[str] = {
    'd41e68c0230d783fe66d0bb22b725e5d7b2fd73466312e2f1b833cbf37abda47',
    '3df0b9f7061025b451723bca25fc4f97f86c0f07c05f75dc5a69fc26536e1671',
}
_REAL_UNI_SLIDE_HASHES: set[str] = {
    '27809f96b05085e769d8196c88a3802aef3354ef161ddd9bc048d08219c6e21b',
}

# Back-compat alias kept for any external caller (none today, but cheap).
_REAL_OUTPUT_SLIDE_HASHES = _REAL_CELLVIT_SLIDE_HASHES


def _real_cellvit_output_dir(slide_hash: str) -> Optional[Path]:
    """
    If we have a real CellViT run for this slide, return a DEMO-ONLY redacted
    view of its dir — a symlink farm where each filename has its PHI accession
    (e.g. ``BS22-D76390``) replaced with the slide's SlideCap case ID
    (e.g. ``CS00039``). The underlying bytes are the real output files; only
    the filenames + directory path are redacted. Prod data is never modified.
    """
    if slide_hash not in _REAL_CELLVIT_SLIDE_HASHES:
        return None
    # Real outputs live under NETWORK_ROOT/analyses/<hash>/CellViT — same
    # convention prod writes when transferring back from the cluster.
    real = Path(settings.NETWORK_ROOT) / "analyses" / slide_hash / "CellViT"
    if not real.exists():
        return None
    return _ensure_redacted_anchor_dir(slide_hash, real, sub="CellViT")


def _real_uni_output_dir(slide_hash: str) -> Optional[Path]:
    """
    UNI version of the anchor override. UNI's output has a deeper nested
    layout (``UNI/20x_256px_0px_overlap/{features_uni_v2,patches}/``) which
    the renderer expects intact, so we mirror that whole subtree of symlinks
    rather than flatten it. Filenames inside are redacted same as CellViT.
    """
    if slide_hash not in _REAL_UNI_SLIDE_HASHES:
        return None
    real = Path(settings.NETWORK_ROOT) / "analyses" / slide_hash / "UNI"
    if not real.exists():
        return None
    return _ensure_redacted_anchor_dir(slide_hash, real, sub="UNI", recursive=True)


def _ensure_redacted_anchor_dir(
    slide_hash: str,
    real_dir: Path,
    *,
    sub: str = "",
    recursive: bool = False,
) -> Path:
    """
    Build (idempotent) a per-slide directory of symlinks where each filename
    has its accession-number prefix swapped for the SlideCap case ID.

    Why symlinks: the file-serving endpoint reads bytes via ``open(path)``,
    which transparently follows symlinks — so requests, downloads, and the
    transforms pipeline all just work. We never copy the real bytes, and we
    never touch the real files. If we can't resolve a case_id (slide not in
    DB, or no case attached), we fall back to the raw real dir so the demo
    still functions — just without redaction.

    Args:
        sub: optional sub-namespace under the case-id dir (e.g. "CellViT" /
             "UNI") so two different analyses on the same slide don't collide.
        recursive: if True, mirror nested subdirectories (UNI's layout is
             ``UNI/20x_256px_0px_overlap/{features_uni_v2,patches}/`` — the
             UNI renderer rglobs for those nested files and won't find them
             unless the directory structure is preserved).

    The accession we strip is derived from the slide's actual SVS filename
    via the FilenameParser (not from the analysis output names — those have
    appended suffixes the parser doesn't understand). We then substring-
    replace that accession in every output file's name.
    """
    from .db import Slide
    from .services.filename_parser import FilenameParser
    from . import main as _main

    with _session_scope() as db:
        slide = db.query(Slide).filter_by(slide_hash=slide_hash).first()
        case_id = slide.case.slidecap_id if (slide and slide.case) else None

    if not case_id:
        return real_dir

    svs_path = _main.indexer.get_filepath(slide_hash) if _main.indexer else None
    accession = None
    if svs_path:
        parsed = FilenameParser().parse(svs_path.name)
        if parsed:
            accession = parsed.accession
    if not accession:
        return real_dir

    redacted_dir = settings.local_data_path / "anchor-redacted" / case_id
    if sub:
        redacted_dir = redacted_dir / sub
    redacted_dir.mkdir(parents=True, exist_ok=True)

    sources = real_dir.rglob("*") if recursive else real_dir.iterdir()
    for src in sources:
        rel = src.relative_to(real_dir)
        # Redact any accession occurrence in *every* path component so nested
        # dirs don't leak it either. (In practice UNI's nested dirs don't
        # contain the accession, but be defensive.)
        redacted_rel = Path(*(p.replace(accession, case_id) for p in rel.parts))
        link = redacted_dir / redacted_rel
        if src.is_dir():
            link.mkdir(parents=True, exist_ok=True)
            continue
        link.parent.mkdir(parents=True, exist_ok=True)
        # Re-link every time so a rerun picks up new files / fixes a broken
        # link without manual cleanup. Symlink creation is cheap.
        if link.is_symlink() or link.exists():
            link.unlink()
        link.symlink_to(src)

    return redacted_dir


def prepare_demo_outputs(
    job_id: int,
    slide_label: str,
    slide_hash: str,
    kind: str = "cellvit",
) -> Path:
    """
    Create (idempotent) a per-slide output directory for a simulated demo job.

    Dispatches on the analysis ``kind``:

    * **cellvit**: real anchor if available, otherwise procedural fake (600
      synthetic cells across the slide).
    * **uni**: real anchor if available, otherwise an empty dir — UNI's
      output is the .h5 embedding files which we don't fake (no useful
      UMAP can be computed from random vectors).

    Anchor returns are DEMO-only redacted symlink farms — see
    ``_ensure_redacted_anchor_dir`` for the redaction logic. Bytes are the
    real cluster output; only the filenames + dir path are PHI-redacted.
    """
    if kind == "uni":
        real_dir = _real_uni_output_dir(slide_hash)
        if real_dir is not None:
            return real_dir
        # No fake UNI output — drop an empty dir + a note so the file tree
        # isn't completely empty. The user gets a clear "no real data" signal
        # rather than a broken UMAP attempt.
        base = settings.local_data_path / "demo-outputs" / f"job-{job_id}"
        safe = "".join(c if c.isalnum() or c in "-_" else "_" for c in slide_label)[:80]
        out = base / safe
        out.mkdir(parents=True, exist_ok=True)
        note = out / "README.txt"
        if not note.exists():
            note.write_text(
                "Demo mode: no real UNI embeddings exist for this slide.\n"
                "Submit a job on one of the anchor slides (see demo.py "
                "_REAL_UNI_SLIDE_HASHES) to see a real UMAP scatter.\n"
            )
        return out

    # default kind = cellvit
    real_dir = _real_cellvit_output_dir(slide_hash)
    if real_dir is not None:
        return real_dir

    base = settings.local_data_path / "demo-outputs" / f"job-{job_id}"
    safe = "".join(c if c.isalnum() or c in "-_" else "_" for c in slide_label)[:80]
    out = base / safe
    out.mkdir(parents=True, exist_ok=True)

    # cells.geojson is the visible overlay — generate it once per slide+job
    # combo and reuse on re-open. Bigger than the other artifacts (~50–80 KB)
    # but still cheap, and the seeded RNG keeps it deterministic.
    cells_path = out / "cells.geojson"
    if not cells_path.exists():
        cells_path.write_text(_generate_demo_cellvit_geojson(slide_hash, slide_label))

    for name, builder in _DEMO_OUTPUT_FILES.items():
        target = out / name
        if not target.exists():
            target.write_text(builder(slide_label, slide_hash))
    return out


# ── Simulated job run ───────────────────────────────────────────────────

def simulate_job_run(
    job_id: int,
    slide_specs: list[tuple[int, str, str]],
    transfer_seconds: float = 5.0,
    run_seconds: float = 5.0,
) -> None:
    """
    Spawn a daemon thread that walks the job through transferring -> running
    -> completed on the given timings, writing demo outputs at completion.

    slide_specs: list of (job_slide_id, slide_label, slide_hash) tuples.
    slide_hash is used to size the generated cells.geojson to the actual
    slide so the overlay lands on tissue, not on empty space.
    """
    def _run() -> None:
        # Phase 1 — transferring
        time.sleep(0.2)  # let the API response fly first
        with _session_scope() as db:
            job = db.query(AnalysisJob).filter_by(id=job_id).first()
            if job:
                job.status = "transferring"
                job.started_at = datetime.utcnow()
            for js_id, _, _ in slide_specs:
                js = db.query(JobSlide).filter_by(id=js_id).first()
                if js:
                    js.status = "transferring"

        time.sleep(transfer_seconds)

        # Phase 2 — running
        with _session_scope() as db:
            job = db.query(AnalysisJob).filter_by(id=job_id).first()
            if job:
                job.status = "running"
            for js_id, _, _ in slide_specs:
                js = db.query(JobSlide).filter_by(id=js_id).first()
                if js:
                    js.status = "running"
                    js.started_at = datetime.utcnow()

        time.sleep(run_seconds)

        # Phase 3 — completed (write outputs, update DB)
        with _session_scope() as db:
            job = db.query(AnalysisJob).filter_by(id=job_id).first()
            # Resolve the analysis kind so prepare_demo_outputs can dispatch
            # to the right anchor lookup (CellViT vs UNI). Default to cellvit
            # for legacy jobs whose analysis row lacks a kind.
            kind = (job.analysis.kind if job and job.analysis else "cellvit") or "cellvit"
            for js_id, slide_label, slide_hash in slide_specs:
                out_dir = prepare_demo_outputs(job_id, slide_label, slide_hash, kind=kind)
                js = db.query(JobSlide).filter_by(id=js_id).first()
                if js:
                    js.status = "completed"
                    js.completed_at = datetime.utcnow()
                    js.local_output_path = str(out_dir)
                    # Real anchor dirs don't ship a cell_stats.json (the cluster
                    # doesn't write one); only the procedural CellViT fakes do.
                    # Guard both reads so the anchor doesn't crash the phase.
                    log_path = out_dir / "run.log"
                    if log_path.exists():
                        js.log_tail = log_path.read_text()[-2000:]
                    stats_path = out_dir / "cell_stats.json"
                    if stats_path.exists():
                        js.cell_stats = stats_path.read_text()
            if job:
                job.status = "completed"
                job.completed_at = datetime.utcnow()
                job.output_path = str(settings.local_data_path / "demo-outputs" / f"job-{job_id}")

    t = threading.Thread(target=_run, name=f"demo-job-{job_id}", daemon=True)
    t.start()


class _session_scope:
    """Tiny context manager for a per-phase DB session — keeps locks short."""
    def __enter__(self):
        self.db = get_session()
        return self.db

    def __exit__(self, exc_type, exc, tb):
        try:
            if exc_type is None:
                self.db.commit()
            else:
                self.db.rollback()
        finally:
            self.db.close()


# ── Analysis seeding ────────────────────────────────────────────────────

# ── Mock staging scan ────────────────────────────────────────────────────

_STAINS = ["HNE", "GFAP", "IDH1", "Ki67", "p53", "ATRX", "OLIG2", "SOX2"]


def mock_staging_scan(count: int = 10, year: int = 2024) -> list[dict]:
    """
    Return `count` synthetic StagingFile entries shaped like
    SL00123_B1-1_HNE_123455.svs. None of these touch the filesystem.
    """
    rng = random.Random(42)  # stable order per scan within a run
    entries: list[dict] = []
    seen: set[str] = set()
    for _ in range(count):
        # Build a unique SL-style accession not already used in this scan
        while True:
            acc = f"SL{rng.randint(100, 99999):05d}"
            if acc not in seen:
                seen.add(acc)
                break
        block = f"B{rng.randint(1, 6)}"
        slide_no = str(rng.randint(1, 4))
        stain = rng.choice(_STAINS)
        rand = f"{rng.randint(100000, 999999)}"
        filename = f"{acc}_{block}-{slide_no}_{stain}_{rand}.svs"
        # Mock some realistic file sizes (200 MB - 2 GB)
        size = rng.randint(200, 2000) * 1024 * 1024
        entries.append({
            "filename": filename,
            "size_bytes": size,
            "parsed": True,
            "accession": acc,
            "block_id": block,
            "slide_number": slide_no,
            "stain_type": stain,
            "year": year,
            "destination": f"slides/{year}/{filename}",
            "conflict": False,
            "conflict_reason": None,
        })
    return entries


def start_demo_sort(sort_state: dict, filenames: list[str], step_seconds: float = 0.4) -> None:
    """
    Drive the existing _sort_state through a fake sort sequence so the
    frontend's progress banner animates without anything actually moving.
    """
    def _run() -> None:
        sort_state.update({
            "running": True,
            "done": False,
            "total": len(filenames),
            "current": 0,
            "current_file": "",
            "sorted": 0,
            "skipped": 0,
            "errors": [],
        })
        for i, fn in enumerate(filenames, start=1):
            sort_state["current"] = i
            sort_state["current_file"] = fn
            time.sleep(step_seconds)
            sort_state["sorted"] += 1
        sort_state["current_file"] = "Indexing…"
        time.sleep(0.6)
        sort_state["running"] = False
        sort_state["done"] = True

    threading.Thread(target=_run, name="demo-sort", daemon=True).start()


# ── Mock dashboard summary ───────────────────────────────────────────────


def mock_dashboard_summary() -> dict:
    """Lightweight dashboard payload — no network drive walks."""
    return {
        "library": {
            "total_slides": 1248,
            "total_cases": 312,
            "years": {
                "2020": 102,
                "2021": 184,
                "2022": 261,
                "2023": 318,
                "2024": 287,
                "2025": 96,
            },
        },
        "staging": {
            "count": 10,
            "total_size_bytes": 10 * 800 * 1024 * 1024,
        },
        "recent_jobs": [],
        "storage": {
            "network_root": settings.NETWORK_ROOT,
            "slides_size_mb": 4_812_000,
            "analyses_size_mb": 612_000,
            "staging_size_mb": 8_000,
        },
    }


def seed_demo_analyses() -> None:
    """Register CellViT in the demo DB if not already present. Idempotent."""
    with _session_scope() as db:
        existing = db.query(Analysis).filter_by(name="CellViT").first()
        if existing:
            return
        analysis = Analysis(
            name="CellViT",
            version="SAM-H-x40",
            description="(DEMO) Cell detection and segmentation — CellViT with SAM-H backbone at 40x",
            kind="cellvit",  # default ruleset comes from analyses/cellvit.py
            script_path="/demo/cellvit/run_cellvit.sh",
            working_directory="/demo/cellvit",
            env_setup="source cellvit_env/bin/activate",
            command_template="./run_cellvit.sh {wsi_dir} {outdir} ./checkpoints/CellViT-SAM-H-x40-AMP.pth {gpu} {batch_size}",
            default_parameters='{"batch_size": 4}',
            gpu_required=True,
            estimated_runtime_minutes=2,
        )
        db.add(analysis)


# ── Anchor cache pre-warm ──────────────────────────────────────────────


def _local_anchor_slide_path(slide_hash: str) -> Optional[Path]:
    """
    Return the local-SSD copy of an anchor slide's SVS, if present.

    The local copy preserves the source's original filename because the
    FilenameParser needs the accession-prefixed name to derive metadata
    downstream (`get_job` etc. call ``parser.parse(path.name)``). The file
    lives only under ``LOCAL_DATA_DIR/anchor-slides/`` on the server — its
    name is never sent to the frontend.
    """
    d = settings.local_data_path / "anchor-slides"
    if not d.exists():
        return None
    # Look for any .svs in this dir that hashes to the requested slide. We
    # only have a few anchors so a linear scan is fine; we avoid embedding
    # the hash in the filename so the original parsable name is preserved.
    from .services.hasher import SlideHasher
    h = SlideHasher(settings.salt_path)
    for f in d.glob("*.svs"):
        if h.hash_slide_stem(f.stem) == slide_hash:
            return f
    return None


def _copy_anchor_slide_local(slide_hash: str) -> Optional[Path]:
    """
    Copy an anchor slide's SVS from NETWORK_ROOT to local SSD if not already
    there *and complete*. Returns the local path (or None if the source
    can't be found and no valid local copy exists).

    Size-validates the local copy against the source — a partial copy (e.g.
    from a previous run interrupted by network unmount) is treated as
    missing and re-pulled. Without this gate, openslide would read past the
    truncation point and return garbage tile data, poisoning the tile cache
    until manually wiped.

    Why local SSD at all: tile generation does many random reads against
    the WSI. On SMB these are ~10-50× slower than local SSD. Copying once
    (one-time ~30-90s on cold SMB) makes every subsequent tile read fast
    for the lifetime of the install.
    """
    from . import main as _main
    src = _main.indexer.get_filepath(slide_hash) if _main.indexer else None

    # If we already have a local copy at the canonical (filename-preserving)
    # path AND it matches the source size, trust it.
    existing = _local_anchor_slide_path(slide_hash)
    if existing:
        if not src or not src.exists():
            return existing  # drive unavailable but local exists
        if existing.stat().st_size == src.stat().st_size:
            return existing
        print(
            f"[Demo prewarm] local copy of {slide_hash[:12]} is partial "
            f"({existing.stat().st_size}/{src.stat().st_size} bytes) — re-copying"
        )
        existing.unlink()

    if not src or not src.exists():
        return None

    dst_dir = settings.local_data_path / "anchor-slides"
    dst_dir.mkdir(parents=True, exist_ok=True)
    dst = dst_dir / src.name  # preserve the original parsable name
    import shutil
    print(f"[Demo prewarm] copying anchor SVS to local SSD: {src.name} ({src.stat().st_size / 1024 / 1024:.0f} MB)")
    # Copy to a .partial sidecar first, then atomically rename. If the copy
    # gets interrupted (network unmount), the next run sees the .partial
    # missing and the dst missing → re-pulls from scratch instead of
    # trusting a truncated file.
    tmp = dst.with_suffix(".svs.partial")
    if tmp.exists():
        tmp.unlink()
    shutil.copy2(str(src), str(tmp))
    tmp.rename(dst)
    return dst


def _swap_indexer_to_local_anchors() -> None:
    """
    For each anchor slide with a local copy, point the indexer at the local
    file instead of the NETWORK_ROOT one. tile reads / DeepZoom generators
    will then hit local SSD. Idempotent.

    We also blow away any cached DeepZoomGenerator for the anchor so the
    next request rebuilds it pointing at the local file — otherwise the
    cached one is still bound to the SMB path.
    """
    from . import main as _main
    if not _main.indexer:
        return
    for slide_hash in _REAL_CELLVIT_SLIDE_HASHES | _REAL_UNI_SLIDE_HASHES:
        local = _local_anchor_slide_path(slide_hash)
        if not local:
            continue
        _main.indexer.slide_hash_to_path[slide_hash] = local
        # Drop any cached TileSource bound to the old (network) path so the
        # next request rebuilds against the local SVS.
        _main._ts_cache.pop(slide_hash, None)


def prewarm_anchor_caches() -> None:
    """
    Background warmup so the first click on an anchor's slide, overlay, or
    UMAP is instant instead of taking 30–120s.

    Four artifacts are warmed:

    * **Local SVS copies**: copy each anchor WSI from NETWORK_ROOT to local
      SSD. Eliminates SMB latency from the tile-generation hot path. ~5-30s
      one-time per slide, then permanent.
    * **WSI tiles**: pre-encode the lowest few DeepZoom levels and store as
      JPEGs under ``LOCAL_DATA_DIR/tile-cache/``. The very first viewer open
      typically fans out 50–100 tile requests at the overview zoom; those
      now hit a static file instead of openslide + PIL.
    * **CellViT overlays**: read each anchor's ``*_cells.geojson.snappy``
      into OS page cache so the first transformed-file request decompresses
      from RAM instead of a cold SMB read.
    * **UNI projections**: invoke UMAP + PCA renderers once. The renderer
      writes its JSON cache next to the source .h5; subsequent endpoint
      calls skip the umap-learn fit (~5s saved per slide).

    Idempotent and safe — re-running re-reads already-warm files. Each
    artifact is wrapped in try/except so a failure on one slide doesn't
    block others.
    """
    from . import main as _main
    from .analyses import get_kind

    # Copy SVS files to local SSD first — every downstream step benefits.
    for slide_hash in _REAL_CELLVIT_SLIDE_HASHES | _REAL_UNI_SLIDE_HASHES:
        try:
            local = _copy_anchor_slide_local(slide_hash)
            if local:
                print(f"[Demo prewarm] local SVS ready for {slide_hash[:12]}: {local.name}")
        except Exception as e:
            print(f"[Demo prewarm] SVS copy {slide_hash[:12]} failed: {e}")
    _swap_indexer_to_local_anchors()

    # All slides we care about (CellViT anchors + UNI anchors). Same slide
    # may appear in both sets — set() dedupes for the tile pre-warm.
    all_anchor_hashes = _REAL_CELLVIT_SLIDE_HASHES | _REAL_UNI_SLIDE_HASHES
    for slide_hash in all_anchor_hashes:
        try:
            # 300 tiles ≈ ~30MB cache per slide, covers initial fit-to-screen +
            # one or two zoom levels. Plenty to make "open the slide" feel
            # instant without bloating the cache for higher-zoom levels the
            # user may never explore.
            n = _main.prewarm_slide_tiles(slide_hash, max_tiles=300)
            if n:
                print(f"[Demo prewarm] generated {n} tiles for {slide_hash[:12]}")
            else:
                print(f"[Demo prewarm] tiles already warm for {slide_hash[:12]}")
        except Exception as e:
            print(f"[Demo prewarm] tile prewarm {slide_hash[:12]} failed: {e}")

    # CellViT: touch the snappy bytes so they're in OS page cache.
    cellvit = get_kind("cellvit")
    for slide_hash in _REAL_CELLVIT_SLIDE_HASHES:
        try:
            d = _real_cellvit_output_dir(slide_hash)
            if not d:
                continue
            for f in d.iterdir():
                if f.is_file() and f.name.endswith(".geojson.snappy"):
                    # Read once → in page cache. Cheap (~1 MB read).
                    f.read_bytes()
            print(f"[Demo prewarm] CellViT snappy warm for {slide_hash[:12]}")
        except Exception as e:
            print(f"[Demo prewarm] CellViT {slide_hash[:12]} failed: {e}")

    # UNI: compute UMAP + PCA once. The renderer writes a JSON cache so the
    # heavy umap-learn fit happens here, not on the user's first click.
    uni = get_kind("uni")
    if uni is None:
        return
    for slide_hash in _REAL_UNI_SLIDE_HASHES:
        try:
            d = _real_uni_output_dir(slide_hash)
            if not d:
                continue
            # Stem is derived from the slide's WSI filename via the indexer
            # (parser strips the extension). Same logic the render endpoint
            # uses, so the renderer finds the same files.
            svs = _main.indexer.get_filepath(slide_hash) if _main.indexer else None
            if not svs:
                continue
            # Stem we want is the *redacted* stem — the symlink farm renamed
            # files using the case_id. Reconstruct it from any file in the dir.
            sample = next(d.rglob("*.h5"), None)
            if sample is None:
                print(f"[Demo prewarm] UNI {slide_hash[:12]}: no .h5 found")
                continue
            stem = sample.name.rsplit(".h5", 1)[0].removesuffix("_patches")
            for rid in ("pca", "umap"):
                fn = uni.renderers.get(rid)
                if not fn:
                    continue
                fn.fn(d, stem, {})  # cached on disk by the renderer itself
            print(f"[Demo prewarm] UNI projections cached for {slide_hash[:12]} ({stem})")
        except Exception as e:
            print(f"[Demo prewarm] UNI {slide_hash[:12]} failed: {e}")
