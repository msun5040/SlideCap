"""
Remove every external slide registered from one external/ subfolder, so the
folder can be re-imported from a corrected CSV.

Why a script and not an endpoint: SlideCap has no delete-slide route at all.
The only built-in removal is ghost cleanup, which acts on slides whose FILE is
gone and sweeps the whole library at once -- neither of which is what's wanted
when the files are fine and only one project's metadata is wrong.

Why the explicit child-table deletes below: this engine runs with SQLite foreign
keys OFF, so `ON DELETE CASCADE` in the schema does nothing. The ORM only
cascades the relationships Slide actually declares (tags, job_slides); rows in
cohort_slides, cohort_group_slides, slide_qc, study_slides and study_group_slides
would be left pointing at deleted ids. Each one is removed by hand.

Dry run by default. --apply takes a timestamped copy of the database first.

    python scripts/purge_external_folder.py --folder MEK
    python scripts/purge_external_folder.py --folder MEK --apply
"""
from __future__ import annotations

import argparse
import shutil
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from sqlalchemy import text

from app.config import settings
from app.db import Case, Slide, get_session, init_db, init_lock
from app.services.hasher import SlideHasher
from app.services.indexer import SlideIndexer

# Every table holding a slide_id. Deleting a slide without clearing these leaves
# rows that point at nothing -- see the module docstring.
SLIDE_CHILD_TABLES = [
    "slide_tags",
    "cohort_slides",
    "cohort_group_slides",
    "job_slides",
    "slide_qc",
    "study_slides",
    "study_group_slides",
]


def files_in_folder(folder: str) -> list[tuple[str, list[str]]]:
    """
    [(relative_path, [candidate slide_hashes])] for slide files under external/<folder>/.

    external/ lives at NETWORK_ROOT/slides/external -- the indexer is rooted at
    settings.slides_path, not at NETWORK_ROOT -- and the hash key is taken
    relative to that external dir, so both come from settings rather than being
    rebuilt from NETWORK_ROOT.

    Two hashes per file, not one. Files in a subfolder are keyed on the
    folder-qualified stem ("MEK/mek_1") today, but before folders were supported
    they were keyed on the bare stem ("mek_1") -- and _scan_external_paths still
    aliases the old hash to the same file. A slide registered back then is stored
    under the bare-stem hash, so matching only the modern one would quietly leave
    those rows behind for a re-import to duplicate.
    """
    hasher = SlideHasher(settings.salt_path)
    external_dir = settings.external_path
    target = external_dir / folder
    if not target.is_dir():
        raise SystemExit(f"No such folder: {target}")

    out = []
    for fp in sorted(target.rglob("*")):
        if not fp.is_file() or fp.suffix.lower() not in SlideIndexer.EXTERNAL_EXTS:
            continue
        rel = fp.relative_to(external_dir)
        key = SlideIndexer.external_key(rel)
        hashes = [hasher.hash_slide_stem(key)]
        if key != rel.stem:
            hashes.append(hasher.hash_slide_stem(rel.stem))
        out.append((rel.as_posix(), hashes))
    return out


def chunked(seq: list, size: int = 400) -> list[list]:
    """SQLite caps bound parameters (999 on older builds), so IN () lists go in batches."""
    return [seq[i:i + size] for i in range(0, len(seq), size)]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--folder", required=True,
                    help="Subfolder of external/, e.g. MEK")
    ap.add_argument("--apply", action="store_true",
                    help="Actually delete. Without it, nothing is written.")
    args = ap.parse_args()

    if not settings.slides_path.is_dir():
        raise SystemExit(f"Slides folder not reachable: {settings.slides_path}\n"
                         "Mount the share (or set NETWORK_ROOT) and try again.")

    db_path = settings.db_path
    print(f"database : {db_path}")
    print(f"external : {settings.external_path / args.folder}\n")

    on_disk = files_in_folder(args.folder)
    print(f"{len(on_disk)} slide file(s) in the folder")
    if not on_disk:
        return 0

    init_lock(settings.local_data_path)
    init_db(db_path)
    db = get_session()

    candidates = [h for _rel, hashes in on_disk for h in hashes]
    slides = []
    for batch in chunked(candidates):
        slides += db.query(Slide).filter(Slide.slide_hash.in_(batch)).all()
    registered = [s for s in slides if s.is_external]
    non_external = [s for s in slides if not s.is_external]

    print(f"{len(registered)} of them are registered in SlideCap")
    if non_external:
        # Would mean a clinical slide shares a hash with one of these files.
        # Refuse rather than guess.
        print(f"\nREFUSING: {len(non_external)} matching slide(s) are not marked external.")
        return 2
    if not registered:
        print("Nothing to remove — the folder is ready to import.")
        return 0

    # What is attached to them. The caller said 'nothing yet'; verify rather
    # than trust it, because this is the irreversible part.
    slide_ids = [s.id for s in registered]
    batches = chunked(slide_ids)

    def each_batch(sql: str):
        """Run `sql` once per id batch; the {marks} placeholder is filled in."""
        for batch in batches:
            marks = ", ".join(f":i{n}" for n in range(len(batch)))
            params = {f"i{n}": sid for n, sid in enumerate(batch)}
            yield db.execute(text(sql.format(marks=marks)), params)

    attachments = {}
    for table in SLIDE_CHILD_TABLES:
        n = sum(r.scalar() or 0 for r in
                each_batch(f"SELECT COUNT(*) FROM {table} WHERE slide_id IN ({{marks}})"))
        if n:
            attachments[table] = n

    if attachments:
        print("\nAttached rows that will also be removed:")
        for t, n in attachments.items():
            print(f"  {t:<22} {n}")
    else:
        print("\nNothing else references these slides.")

    # Synthetic cases that exist only to hold these slides.
    # One grouped count instead of a query per case, so this stays quick on a
    # large folder and doesn't build an ever-growing NOT IN list.
    doomed = set(slide_ids)
    case_ids = {s.case_id for s in registered}
    survivors: dict[int, int] = {cid: 0 for cid in case_ids}
    for batch in chunked(sorted(case_ids)):
        for cid, sid in db.query(Slide.case_id, Slide.id).filter(Slide.case_id.in_(batch)):
            if sid not in doomed:
                survivors[cid] += 1
    orphan_cases = [cid for cid, n in survivors.items() if n == 0]
    print(f"\n{len(case_ids)} case(s) involved; {len(orphan_cases)} would be left empty and removed")

    if not args.apply:
        print("\nDry run — nothing was changed. Re-run with --apply to delete.")
        return 0

    backup = db_path.with_name(f"{db_path.stem}.before-purge-{datetime.now():%Y%m%d-%H%M%S}.sqlite")
    shutil.copy2(db_path, backup)
    print(f"\nbackup   : {backup}")

    for table in SLIDE_CHILD_TABLES:
        list(each_batch(f"DELETE FROM {table} WHERE slide_id IN ({{marks}})"))
    for batch in batches:
        db.query(Slide).filter(Slide.id.in_(batch)).delete(synchronize_session=False)
    for batch in chunked(orphan_cases):
        db.query(Case).filter(Case.id.in_(batch)).delete(synchronize_session=False)
    db.commit()

    print(f"removed  : {len(registered)} slide(s), {len(orphan_cases)} case(s)")
    print("\nThe folder is now unregistered. Re-import it from the corrected CSV via\n"
          "Slide Library → ⋯ → External slides.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
