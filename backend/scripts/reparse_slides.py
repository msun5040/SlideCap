"""
Re-parse every slide's filename and update block_id / stain_type / random_id
in the DB where the parser now produces a different result.

Usage:
    LOCAL_DATA_DIR=~/.slidecap-demo python scripts/reparse_slides.py [--dry-run]
    LOCAL_DATA_DIR=~/.slidecap python scripts/reparse_slides.py [--dry-run]

The DB and slide path cache are picked up from the same env vars / config.py
that the running backend uses, so this is safe to run while a backend is up
(it will pick up changes on next read).
"""
from __future__ import annotations

import sys
from pathlib import Path

# Make `app` importable when running as a plain script
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.config import settings  # noqa: E402
from app.db import init_db, get_session, Slide  # noqa: E402
from app.services import SlideHasher, SlideIndexer  # noqa: E402


def main(dry_run: bool = False) -> int:
    print(f"DB:          {settings.db_path}")
    print(f"Slides path: {settings.slides_path}")
    print(f"Dry run:     {dry_run}")

    init_db(settings.db_path)
    hasher = SlideHasher(settings.salt_path)
    indexer = SlideIndexer(hasher, str(settings.slides_path))
    cached = indexer.build_path_cache()
    print(f"Path cache:  {cached} slides")

    db = get_session()
    try:
        slides = db.query(Slide).all()
        print(f"Slides:      {len(slides)}")

        changed = 0
        unparseable = 0
        missing = 0
        for slide in slides:
            fp = indexer.get_filepath(slide.slide_hash)
            if not fp:
                missing += 1
                continue
            parsed = indexer.parser.parse(fp.name)
            if not parsed:
                unparseable += 1
                continue

            new_block = parsed.block_id or ''
            new_stain = parsed.stain_type or ''
            new_random = parsed.random_id or ''

            old_block = slide.block_id or ''
            old_stain = slide.stain_type or ''
            old_random = slide.random_id or ''

            if (new_block, new_stain, new_random) != (old_block, old_stain, old_random):
                changed += 1
                print(
                    f"  {slide.slidecap_id or slide.slide_hash[:10]}  {fp.name}\n"
                    f"    block:  {old_block!r:>10}  →  {new_block!r}\n"
                    f"    stain:  {old_stain!r:>10}  →  {new_stain!r}\n"
                    f"    random: {old_random!r:>10}  →  {new_random!r}"
                )
                if not dry_run:
                    slide.block_id = new_block
                    slide.stain_type = new_stain
                    slide.random_id = new_random

        if dry_run:
            db.rollback()
            print(f"\nDry run — no changes written.")
        else:
            db.commit()
            print(f"\nCommitted.")

        print(f"Updated:     {changed}")
        print(f"Unparseable: {unparseable}")
        print(f"No file:     {missing}")
        return 0
    except Exception as e:
        db.rollback()
        print(f"Error: {e}")
        return 1
    finally:
        db.close()


if __name__ == "__main__":
    dry_run = "--dry-run" in sys.argv
    sys.exit(main(dry_run=dry_run))
