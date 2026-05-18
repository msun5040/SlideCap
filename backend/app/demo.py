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

_DEMO_OUTPUT_FILES = {
    "cells.geojson": lambda slide_label: json.dumps({
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": {"type": "Polygon", "coordinates": [[[100, 100], [120, 100], [120, 120], [100, 120], [100, 100]]]},
                "properties": {"classification": {"name": "Tumor", "color": [200, 0, 0]}},
            },
            {
                "type": "Feature",
                "geometry": {"type": "Polygon", "coordinates": [[[200, 200], [215, 200], [215, 215], [200, 215], [200, 200]]]},
                "properties": {"classification": {"name": "Lymphocyte", "color": [0, 0, 200]}},
            },
        ],
    }, indent=2),
    "cell_stats.json": lambda slide_label: json.dumps({
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
    "summary.csv": lambda slide_label: (
        "slide,total_cells,tumor,stroma,lymphocyte,necrosis,other\n"
        f"{slide_label},184523,92341,41203,27815,14502,8662\n"
    ),
    "run.log": lambda slide_label: (
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


def prepare_demo_outputs(job_id: int, slide_label: str) -> Path:
    """
    Create (idempotent) a per-slide output directory under the demo data dir
    populated with sample CellViT output files. Returns the directory path.
    """
    base = settings.local_data_path / "demo-outputs" / f"job-{job_id}"
    safe = "".join(c if c.isalnum() or c in "-_" else "_" for c in slide_label)[:80]
    out = base / safe
    out.mkdir(parents=True, exist_ok=True)
    for name, builder in _DEMO_OUTPUT_FILES.items():
        target = out / name
        if not target.exists():
            target.write_text(builder(slide_label))
    return out


# ── Simulated job run ───────────────────────────────────────────────────

def simulate_job_run(
    job_id: int,
    slide_specs: list[tuple[int, str]],
    transfer_seconds: float = 5.0,
    run_seconds: float = 5.0,
) -> None:
    """
    Spawn a daemon thread that walks the job through transferring -> running
    -> completed on the given timings, writing demo outputs at completion.

    slide_specs: list of (job_slide_id, slide_label) tuples.
    """
    def _run() -> None:
        # Phase 1 — transferring
        time.sleep(0.2)  # let the API response fly first
        with _session_scope() as db:
            job = db.query(AnalysisJob).filter_by(id=job_id).first()
            if job:
                job.status = "transferring"
                job.started_at = datetime.utcnow()
            for js_id, _ in slide_specs:
                js = db.query(JobSlide).filter_by(id=js_id).first()
                if js:
                    js.status = "transferring"

        time.sleep(transfer_seconds)

        # Phase 2 — running
        with _session_scope() as db:
            job = db.query(AnalysisJob).filter_by(id=job_id).first()
            if job:
                job.status = "running"
            for js_id, _ in slide_specs:
                js = db.query(JobSlide).filter_by(id=js_id).first()
                if js:
                    js.status = "running"
                    js.started_at = datetime.utcnow()

        time.sleep(run_seconds)

        # Phase 3 — completed (write outputs, update DB)
        with _session_scope() as db:
            job = db.query(AnalysisJob).filter_by(id=job_id).first()
            for js_id, slide_label in slide_specs:
                out_dir = prepare_demo_outputs(job_id, slide_label)
                js = db.query(JobSlide).filter_by(id=js_id).first()
                if js:
                    js.status = "completed"
                    js.completed_at = datetime.utcnow()
                    js.local_output_path = str(out_dir)
                    js.log_tail = (out_dir / "run.log").read_text()[-2000:]
                    js.cell_stats = (out_dir / "cell_stats.json").read_text()
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
            script_path="/demo/cellvit/run_cellvit.sh",
            working_directory="/demo/cellvit",
            env_setup="source cellvit_env/bin/activate",
            command_template="./run_cellvit.sh {wsi_dir} {outdir} ./checkpoints/CellViT-SAM-H-x40-AMP.pth {gpu} {batch_size}",
            default_parameters='{"batch_size": 4}',
            gpu_required=True,
            estimated_runtime_minutes=2,
        )
        db.add(analysis)
