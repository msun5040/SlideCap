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


def files_in_folder(root: Path, folder: str) -> list[tuple[str, str]]:
    """[(relative_path, slide_hash)] for the slide files under external/<folder>/."""
    hasher = SlideHasher(settings.salt_path)
    external_dir = root / "external"
    target = external_dir / folder
    if not target.is_dir():
        raise SystemExit(f"No such folder: {target}")

    out = []
    for fp in sorted(target.rglob("*")):
        if not fp.is_file() or fp.suffix.lower() not in SlideIndexer.EXTERNAL_EXTS:
            continue
        rel = fp.relative_to(external_dir)
        out.append((rel.as_posix(), hasher.hash_slide_stem(SlideIndexer.external_key(rel))))
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--folder", required=True,
                    help="Subfolder of external/, e.g. MEK")
    ap.add_argument("--apply", action="store_true",
                    help="Actually delete. Without it, nothing is written.")
    args = ap.parse_args()

    root = Path(settings.NETWORK_ROOT)
    if not root.is_dir():
        raise SystemExit(f"Network root not reachable: {root}\nMount the share and try again.")

    db_path = settings.db_path
    print(f"database : {db_path}")
    print(f"external : {root / 'external' / args.folder}\n")

    on_disk = files_in_folder(root, args.folder)
    print(f"{len(on_disk)} slide file(s) in the folder")
    if not on_disk:
        return 0

    init_lock(settings.local_data_path)
    init_db(db_path)
    db = get_session()

    by_hash = {h: rel for rel, h in on_disk}
    slides = db.query(Slide).filter(Slide.slide_hash.in_(list(by_hash))).all()
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
    marks = ", ".join(f":i{n}" for n in range(len(slide_ids)))
    params = {f"i{n}": sid for n, sid in enumerate(slide_ids)}
    attachments = {}
    for table in SLIDE_CHILD_TABLES:
        n = db.execute(text(f"SELECT COUNT(*) FROM {table} WHERE slide_id IN ({marks})"),
                       params).scalar() or 0
        if n:
            attachments[table] = n

    if attachments:
        print("\nAttached rows that will also be removed:")
        for t, n in attachments.items():
            print(f"  {t:<22} {n}")
    else:
        print("\nNothing else references these slides.")

    # Synthetic cases that exist only to hold these slides.
    case_ids = {s.case_id for s in registered}
    orphan_cases = []
    for cid in case_ids:
        remaining = db.query(Slide).filter(Slide.case_id == cid,
                                           ~Slide.id.in_(slide_ids)).count()
        if remaining == 0:
            orphan_cases.append(cid)
    print(f"\n{len(case_ids)} case(s) involved; {len(orphan_cases)} would be left empty and removed")

    if not args.apply:
        print("\nDry run — nothing was changed. Re-run with --apply to delete.")
        return 0

    backup = db_path.with_name(f"{db_path.stem}.before-purge-{datetime.now():%Y%m%d-%H%M%S}.sqlite")
    shutil.copy2(db_path, backup)
    print(f"\nbackup   : {backup}")

    for table in SLIDE_CHILD_TABLES:
        db.execute(text(f"DELETE FROM {table} WHERE slide_id IN ({marks})"), params)
    db.query(Slide).filter(Slide.id.in_(slide_ids)).delete(synchronize_session=False)
    if orphan_cases:
        db.query(Case).filter(Case.id.in_(orphan_cases)).delete(synchronize_session=False)
    db.commit()

    print(f"removed  : {len(registered)} slide(s), {len(orphan_cases)} case(s)")
    print("\nThe folder is now unregistered. Re-import it from the corrected CSV via\n"
          "Slide Library → ⋯ → External slides.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
