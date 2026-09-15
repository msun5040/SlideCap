"""
Import an externally computed cohort projection, with its clusterings, into SlideCap.

The bundle is a directory holding bundle.json plus the files it lists: a projection artifact in the
cohort_projection.write_artifact format, optionally the PCA-reduced matrix, and int16 label files. The
script creates completed CohortProjection / ProjectionClustering rows and copies the files into
LOCAL_DATA_DIR/cohort-projections/ under SlideCap's own names, so the workspace, the WSI cluster mask
and new server-side clusterings all work on the imported run.

Usage (same environment / .env as the backend):
    python scripts/import_projection_bundle.py <bundle dir> --cohort-id 12 --dry-run
    python scripts/import_projection_bundle.py <bundle dir> --cohort-name "My cohort" [--analysis-id 3]

Everything is checked before anything is written: checksums, the artifact header against bundle.json
(point count, slide order), file sizes, label ranges, and that every projected slide is in the cohort
and known to this SlideCap. Slides are reported by slide_hash prefix only.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import struct
import sys
from datetime import datetime
from pathlib import Path

import numpy as np

# Make `app` importable when running as a plain script
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.config import settings  # noqa: E402
from app.db import init_db, init_lock, get_lock, get_session, Slide  # noqa: E402
from app.db.models import Cohort, CohortProjection, ProjectionClustering  # noqa: E402

PROJECTION_DIR_NAME = "cohort-projections"  # must match main.py
ARTIFACT_MAGIC = b"SCPROJ01"
BUNDLE_FORMAT = "slidecap-projection-bundle/1"


def fail(msg: str) -> None:
    raise SystemExit(f"ERROR: {msg}")


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 22), b""):
            h.update(chunk)
    return h.hexdigest()


def read_artifact_header(path: Path) -> dict:
    with open(path, "rb") as fh:
        if fh.read(8) != ARTIFACT_MAGIC:
            fail(f"{path.name} is not a SlideCap projection artifact")
        (hlen,) = struct.unpack("<I", fh.read(4))
        return json.loads(fh.read(hlen).decode("utf-8"))


def validate(bundle_dir: Path) -> dict:
    manifest = bundle_dir / "bundle.json"
    if not manifest.exists():
        fail(f"no bundle.json in {bundle_dir}")
    bundle = json.loads(manifest.read_text())
    if bundle.get("format") != BUNDLE_FORMAT:
        fail(f"unsupported bundle format {bundle.get('format')!r}")
    proj, clusterings = bundle["projection"], bundle.get("clusterings", [])

    print("Verifying checksums…")
    for name, digest in bundle["checksums"].items():
        p = bundle_dir / name
        if not p.exists():
            fail(f"missing file {name}")
        if sha256(p) != digest:
            fail(f"checksum mismatch for {name} (copy incomplete or corrupted)")

    n = int(proj["point_count"])
    header = read_artifact_header(bundle_dir / proj["artifact"])
    if header.get("point_count") != n:
        fail(f"artifact has {header.get('point_count')} points, bundle.json says {n}")
    if header.get("method") != proj["method"]:
        fail("artifact method differs from bundle.json")
    header_hashes = [s["slide_hash"] for s in header["slides"]]
    if header_hashes != proj["slide_hashes"]:
        fail("artifact slide order differs from bundle.json slide_hashes")
    if sum(int(s["n_patches"]) for s in header["slides"]) != n:
        fail("artifact per-slide patch counts do not add up to point_count")

    if proj.get("reduced"):
        size = (bundle_dir / proj["reduced"]).stat().st_size
        if size != n * int(proj["reduced_dim"]) * 4:
            fail(f"reduced matrix is {size} bytes; expected {n:,} x {proj['reduced_dim']} float32")

    for c in clusterings:
        lab = np.fromfile(bundle_dir / c["labels"], dtype="<i2")
        if lab.shape != (n,):
            fail(f"{c['labels']}: {lab.size} labels for {n} points")
        k = int(c["n_clusters"])
        if lab.max() != k - 1 or lab.min() < -1:
            fail(f"{c['labels']}: label range {lab.min()}..{lab.max()} does not match n_clusters={k}")
    return bundle


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("bundle", type=Path)
    group = ap.add_mutually_exclusive_group(required=True)
    group.add_argument("--cohort-id", type=int)
    group.add_argument("--cohort-name")
    ap.add_argument("--analysis-id", type=int, default=None)
    ap.add_argument("--dry-run", action="store_true", help="validate and report; write nothing")
    args = ap.parse_args()

    bundle_dir = args.bundle.expanduser().resolve()
    print(f"DB:       {settings.db_path}")
    print(f"Data dir: {settings.local_data_path}")
    bundle = validate(bundle_dir)
    proj, clusterings = bundle["projection"], bundle.get("clusterings", [])
    hashes = proj["slide_hashes"]
    params = proj.get("params", {})
    print(f"Bundle:   {proj['method']} · {len(hashes)} slides · {proj['point_count']:,} patches"
          + (f" (fit on {params['fit_sample_n']:,})" if params.get("fit_sample_n") else "")
          + f" · {len(clusterings)} clustering(s)")

    init_db(settings.db_path)
    # The write lock is per-process (the running backend has its own); SQLite's busy_timeout covers
    # cross-process contention. Local data path, so no stale-lock cleanup touches the network drive.
    init_lock(settings.local_data_path)
    db = get_session()
    try:
        q = db.query(Cohort)
        cohorts = q.filter_by(id=args.cohort_id).all() if args.cohort_id is not None \
            else q.filter_by(name=args.cohort_name).all()
        if len(cohorts) != 1:
            fail(f"expected exactly one matching cohort, found {len(cohorts)}")
        cohort = cohorts[0]
        in_cohort = {s.slide_hash for s in cohort.slides}
        known = {h for (h,) in db.query(Slide.slide_hash).filter(Slide.slide_hash.in_(hashes)).all()}
        not_known = [h for h in hashes if h not in known]
        not_in_cohort = [h for h in hashes if h not in in_cohort]
        print(f"Cohort:   #{cohort.id} {cohort.name!r} ({len(in_cohort)} slides)")
        print(f"Slides:   {len(hashes) - len(not_known)}/{len(hashes)} known to this SlideCap, "
              f"{len(hashes) - len(not_in_cohort)}/{len(hashes)} in the cohort")
        if not_known or not_in_cohort:
            bad = not_known or not_in_cohort
            fail(f"{len(not_known)} slide(s) unknown here, {len(not_in_cohort)} not in the cohort "
                 f"(e.g. {', '.join(h[:10] for h in bad[:3])}…). Add them to the cohort, or import into "
                 f"the cohort they belong to.")
        if args.dry_run:
            print("Dry run: all checks passed; nothing written.")
            return 0

        root = settings.local_data_path / PROJECTION_DIR_NAME
        root.mkdir(parents=True, exist_ok=True)
        now = datetime.now()
        created_files: list[Path] = []
        created_rows: list = []
        try:
            with get_lock().write_lock():
                pr = CohortProjection(cohort_id=cohort.id, analysis_id=args.analysis_id, method=proj["method"],
                                      status="running", progress_pct=0, progress_stage="Importing",
                                      started_at=now)
                pr.set_params(params)
                pr.set_slide_hashes(hashes)
                db.add(pr)
                db.commit()
                db.refresh(pr)
            created_rows.append(pr)

            art = root / f"projection-{pr.id}.scproj"
            shutil.copyfile(bundle_dir / proj["artifact"], art)
            created_files.append(art)
            reduced_rel = None
            if proj.get("reduced"):
                red = root / f"projection-{pr.id}.reduced.f32"
                shutil.copyfile(bundle_dir / proj["reduced"], red)
                created_files.append(red)
                reduced_rel = f"{PROJECTION_DIR_NAME}/{red.name}"

            with get_lock().write_lock():
                pr.status, pr.progress_pct, pr.progress_stage = "completed", 100, "Imported"
                pr.completed_at = datetime.now()
                pr.artifact_path = f"{PROJECTION_DIR_NAME}/{art.name}"
                pr.reduced_path, pr.reduced_dim = reduced_rel, (int(proj["reduced_dim"]) if reduced_rel else None)
                pr.point_count, pr.feature_dim = int(proj["point_count"]), proj.get("feature_dim")
                db.commit()

            for c in clusterings:
                with get_lock().write_lock():
                    cl = ProjectionClustering(projection_id=pr.id, algorithm=c["algorithm"], status="running",
                                              progress_pct=0, progress_stage="Importing", started_at=now)
                    cl.set_params(c.get("params", {}))
                    db.add(cl)
                    db.commit()
                    db.refresh(cl)
                created_rows.append(cl)
                dst = root / f"clustering-{cl.id}.labels"
                shutil.copyfile(bundle_dir / c["labels"], dst)
                created_files.append(dst)
                with get_lock().write_lock():
                    cl.status, cl.progress_pct, cl.progress_stage = "completed", 100, "Imported"
                    cl.completed_at = datetime.now()
                    cl.labels_path = f"{PROJECTION_DIR_NAME}/{dst.name}"
                    cl.n_clusters, cl.n_noise = int(c["n_clusters"]), int(c.get("n_noise") or 0)
                    cl.silhouette, cl.approximate = c.get("silhouette"), bool(c.get("approximate"))
                    cl.elapsed_seconds = c.get("elapsed_seconds")
                    db.commit()
                print(f"  clustering #{cl.id}: {c['algorithm']} {c.get('params', {}).get('label', '')} "
                      f"({c['n_clusters']} clusters)")
        except BaseException:
            db.rollback()
            with get_lock().write_lock():
                for row in reversed(created_rows):
                    db.delete(row)
                db.commit()
            for p in created_files:
                p.unlink(missing_ok=True)
            print("Import failed; rows and copied files were removed.")
            raise

        print(f"Imported projection #{pr.id} into cohort #{cohort.id}. "
              f"Open the cohort's analysis workspace → Projections to view it.")
        return 0
    finally:
        db.close()


if __name__ == "__main__":
    sys.exit(main())
