"""
Emit the case / block keys for a Data Pull, so a notebook can group patches
correctly without parsing filenames.

Filename parsing does not work across this library: the lab convention parses,
but project-prefixed and externally-registered slides do not, and a regex that
"nearly" works silently merges unrelated slides into one group -- which then
dominates any grouped metric computed from it. The database already holds the
answer, set at registration, so take it from there.

Writes a CSV keyed by the per-slide FOLDER NAME a Data Pull creates, i.e. the
slide's file stem, so it joins straight onto `d.slide` from load_uni_pull.

No accession numbers are written. `case_key` is SlideCap's accession HASH --
stable, groups correctly, and carries no PHI, so the CSV is safe to keep beside
a notebook.

    python scripts/pull_slide_keys.py slide_keys.csv
"""
from __future__ import annotations

import csv
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.config import settings           # noqa: E402
from app.db import Slide, get_session, init_db, init_lock  # noqa: E402
from app.services.hasher import SlideHasher              # noqa: E402
from app.services.indexer import SlideIndexer            # noqa: E402


def main() -> int:
    out = Path(sys.argv[1] if len(sys.argv) > 1 else "slide_keys.csv")

    init_lock(settings.local_data_path)
    init_db(settings.db_path)
    db = get_session()

    ix = SlideIndexer(SlideHasher(settings.salt_path), str(settings.slides_path))
    ix.build_path_cache()          # year dirs
    ix._scan_external_paths()      # external/ subfolders

    rows, no_file = [], 0
    for sl in db.query(Slide).all():
        fp = ix.slide_hash_to_path.get(sl.slide_hash)
        if fp is None:
            no_file += 1
            continue
        # Same sanitisation Data Pull uses for the folder it creates.
        folder = re.sub(r"[^\w.\-]", "_", fp.stem)
        rows.append({
            "folder": folder,
            "case_key": (sl.case.accession_hash if sl.case else "") or "",
            "block_id": sl.block_id or "",
            "stain_type": sl.stain_type or "",
            "scanner": sl.scanner or "",
            "is_external": int(bool(sl.is_external)),
        })

    with out.open("w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=["folder", "case_key", "block_id",
                                           "stain_type", "scanner", "is_external"])
        w.writeheader()
        w.writerows(sorted(rows, key=lambda r: r["folder"]))

    cases = len({r["case_key"] for r in rows if r["case_key"]})
    blocks = len({(r["case_key"], r["block_id"]) for r in rows if r["case_key"]})
    missing_block = sum(1 for r in rows if not r["block_id"])
    print(f"{len(rows)} slides -> {cases} cases, {blocks} case+block groups")
    print(f"  {missing_block} slide(s) have no block_id recorded")
    if no_file:
        print(f"  {no_file} slide(s) skipped: no file in the path cache")
    print(f"wrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
