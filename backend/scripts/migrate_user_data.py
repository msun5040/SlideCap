"""
Migrate cohorts and request sheets between backend databases.

The registry/data lives in each backend's local SQLite DB (config.py keeps it on
local disk, not the network share), so data created against a dev backend (e.g. a
laptop's localhost:8000) does not appear on the shared server. This script moves
cohorts and request sheets by VALUE, remapping references that use row IDs:

  - cohort slides     -> matched by slides.slide_hash
  - cohort patients   -> cases matched by cases.accession_hash
  - cohort flags      -> already store accession_hashes (portable as-is)
  - request auto-tags -> matched by tags.name

Row IDs differ between databases, but slide_hash / accession_hash / tag name are
stable (both backends index the same network share with the same salt), so the
mapping is exact. Anything that can't be resolved on the target is skipped and
reported rather than silently dropped.

Usage
-----
On the SOURCE machine (e.g. your Mac dev backend):

    LOCAL_DATA_DIR=~/.slidecap python scripts/migrate_user_data.py export \
        --out slidecap_userdata.json

Copy slidecap_userdata.json to the server (commit it, scp it, etc.), then on the
TARGET (the Windows server), as the same user the backend runs as:

    python scripts/migrate_user_data.py import --in slidecap_userdata.json

Import is idempotent: cohorts / sheets that already exist (matched by name) are
skipped. Pass --force to replace them.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.config import settings  # noqa: E402
from app.db import (  # noqa: E402
    init_db, get_session,
    Cohort, CohortPatient, CohortPatientCase, CohortFlag,
    RequestSheet, RequestRow, Slide, Case, Tag,
)

# RequestRow columns copied verbatim (everything except identity / FK / timestamps).
_ROW_SKIP = {"id", "sheet_id", "created_at", "updated_at"}


def _row_value_cols() -> list[str]:
    return [c.name for c in RequestRow.__table__.columns if c.name not in _ROW_SKIP]


def _iso(dt) -> str | None:
    return dt.isoformat() if isinstance(dt, datetime) else None


def _parse_iso(s):
    if not s:
        return None
    try:
        return datetime.fromisoformat(s)
    except Exception:
        return None


# ── Export ──────────────────────────────────────────────────────────────────

def do_export(out_path: Path) -> None:
    init_db(settings.db_path)
    db = get_session()
    try:
        cohorts = []
        for c in db.query(Cohort).all():
            patients = []
            for p in c.patients:
                surgeries = []
                for s in p.surgeries:
                    if not s.case:
                        continue
                    surgeries.append({
                        "accession_hash": s.case.accession_hash,
                        "surgery_label": s.surgery_label,
                        "note": s.note,
                    })
                patients.append({"label": p.label, "note": p.note, "surgeries": surgeries})
            cohorts.append({
                "name": c.name,
                "description": c.description,
                "source_type": c.source_type,
                "source_details": c.source_details,
                "created_by": c.created_by,
                "created_at": _iso(c.created_at),
                "slide_hashes": [s.slide_hash for s in c.slides],
                "patients": patients,
                "flags": [{"name": f.name, "case_hashes": f.get_case_hashes()} for f in c.flags],
            })

        row_cols = _row_value_cols()
        sheets = []
        for sh in db.query(RequestSheet).all():
            sheets.append({
                "name": sh.name,
                "description": sh.description,
                "created_by": sh.created_by,
                "created_at": _iso(sh.created_at),
                "auto_tags": [t.name for t in sh.auto_tags],
                "rows": [{col: getattr(r, col) for col in row_cols} for r in sh.rows],
            })

        payload = {
            "version": 1,
            "exported_at": datetime.utcnow().isoformat(),
            "source_db": str(settings.db_path),
            "cohorts": cohorts,
            "request_sheets": sheets,
        }
        out_path.write_text(json.dumps(payload, indent=2, default=str))
        print(f"Exported {len(cohorts)} cohort(s) and {len(sheets)} request sheet(s) -> {out_path}")
        print(f"  slide links: {sum(len(c['slide_hashes']) for c in cohorts)}, "
              f"request rows: {sum(len(s['rows']) for s in sheets)}")
    finally:
        db.close()


# ── Import ──────────────────────────────────────────────────────────────────

def do_import(in_path: Path, force: bool) -> None:
    payload = json.loads(in_path.read_text())
    init_db(settings.db_path)
    db = get_session()
    try:
        slide_by_hash = {s.slide_hash: s for s in db.query(Slide).all()}
        case_by_hash = {c.accession_hash: c for c in db.query(Case).all()}
        tag_by_name = {t.name: t for t in db.query(Tag).all()}

        missing_slides: set[str] = set()
        missing_cases: set[str] = set()
        missing_tags: set[str] = set()

        # Cohorts
        for cd in payload.get("cohorts", []):
            existing = db.query(Cohort).filter_by(name=cd["name"]).first()
            if existing and not force:
                print(f"[skip] cohort {cd['name']!r} exists (id={existing.id})")
                continue
            if existing:
                db.delete(existing)
                db.flush()

            cohort = Cohort(
                name=cd["name"], description=cd.get("description"),
                source_type=cd.get("source_type"), source_details=cd.get("source_details"),
                created_by=cd.get("created_by"), created_at=_parse_iso(cd.get("created_at")) or datetime.utcnow(),
            )
            resolved = []
            for h in cd.get("slide_hashes", []):
                s = slide_by_hash.get(h)
                if s:
                    resolved.append(s)
                else:
                    missing_slides.add(h)
            cohort.slides = resolved

            for pd in cd.get("patients", []):
                patient = CohortPatient(label=pd["label"], note=pd.get("note"))
                for sd in pd.get("surgeries", []):
                    case = case_by_hash.get(sd["accession_hash"])
                    if not case:
                        missing_cases.add(sd["accession_hash"])
                        continue
                    patient.surgeries.append(CohortPatientCase(
                        case_id=case.id, surgery_label=sd["surgery_label"], note=sd.get("note"),
                    ))
                cohort.patients.append(patient)

            for fd in cd.get("flags", []):
                flag = CohortFlag(name=fd["name"])
                flag.set_case_hashes(fd.get("case_hashes", []))
                cohort.flags.append(flag)

            db.add(cohort)
            print(f"[create] cohort {cd['name']!r}: {len(resolved)}/{len(cd.get('slide_hashes', []))} slides, "
                  f"{len(cd.get('patients', []))} patient(s)")

        # Request sheets
        row_cols = set(_row_value_cols())
        for shd in payload.get("request_sheets", []):
            existing = db.query(RequestSheet).filter_by(name=shd["name"]).first()
            if existing and not force:
                print(f"[skip] request sheet {shd['name']!r} exists (id={existing.id})")
                continue
            if existing:
                db.delete(existing)
                db.flush()

            sheet = RequestSheet(
                name=shd["name"], description=shd.get("description"),
                created_by=shd.get("created_by"), created_at=_parse_iso(shd.get("created_at")) or datetime.utcnow(),
            )
            for tname in shd.get("auto_tags", []):
                tag = tag_by_name.get(tname)
                if tag:
                    sheet.auto_tags.append(tag)
                else:
                    missing_tags.add(tname)
            for rd in shd.get("rows", []):
                clean = {k: v for k, v in rd.items() if k in row_cols}
                sheet.rows.append(RequestRow(**clean))
            db.add(sheet)
            print(f"[create] request sheet {shd['name']!r}: {len(shd.get('rows', []))} row(s)")

        db.commit()
        print("Committed.")
        if missing_slides:
            print(f"  WARNING: {len(missing_slides)} slide_hash not found on target (slides not indexed?).")
        if missing_cases:
            print(f"  WARNING: {len(missing_cases)} accession_hash not found on target.")
        if missing_tags:
            print(f"  WARNING: tags not found on target (auto-tags skipped): {sorted(missing_tags)}")
    finally:
        db.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="cmd", required=True)
    pe = sub.add_parser("export", help="Dump cohorts + request sheets to JSON.")
    pe.add_argument("--out", type=Path, default=Path("slidecap_userdata.json"))
    pi = sub.add_parser("import", help="Load cohorts + request sheets from JSON into this backend's DB.")
    pi.add_argument("--in", dest="in_path", type=Path, required=True)
    pi.add_argument("--force", action="store_true", help="Replace cohorts/sheets that already exist (by name).")
    args = parser.parse_args()

    print(f"DB: {settings.db_path}")
    if args.cmd == "export":
        do_export(args.out)
    else:
        do_import(args.in_path, args.force)


if __name__ == "__main__":
    main()
