"""
Backfill the default transforms rule onto existing Analysis rows.

The `transforms` column was added by the recent migration but only newly-seeded
analyses ship with a default rule. This script populates the rule for analyses
created before the migration.

Run against the prod DB:
    LOCAL_DATA_DIR=~/.slidecap python scripts/backfill_transforms.py

Or in dry-run mode first:
    LOCAL_DATA_DIR=~/.slidecap python scripts/backfill_transforms.py --dry-run

Default rule (only applied to analyses named "CellViT" — extend the matcher
below for other pipelines):

    *.geojson.snappy → [decompress_snappy, fix_geojson_geometry]
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.config import settings  # noqa: E402
from app.db import init_db, get_session, Analysis  # noqa: E402


# Map analysis name → default transforms list. Add entries here as new pipelines
# need built-in rules.
DEFAULTS: dict[str, list[dict]] = {
    "CellViT": [
        {"match": "*.geojson.snappy", "ops": ["decompress_snappy", "fix_geojson_geometry"]},
    ],
}


def main(dry_run: bool = False) -> int:
    print(f"DB:      {settings.db_path}")
    print(f"Dry run: {dry_run}")
    init_db(settings.db_path)
    db = get_session()
    try:
        updated = 0
        skipped = 0
        for name, default_rules in DEFAULTS.items():
            row = db.query(Analysis).filter_by(name=name).first()
            if not row:
                print(f"  - {name}: not registered, skipping")
                continue
            if row.transforms:
                print(f"  - {name}: already has transforms, skipping")
                print(f"      {row.transforms[:120]}{'...' if len(row.transforms) > 120 else ''}")
                skipped += 1
                continue
            new_json = json.dumps(default_rules)
            print(f"  + {name}: setting transforms")
            print(f"      {new_json}")
            if not dry_run:
                row.transforms = new_json
                updated += 1

        if dry_run:
            db.rollback()
            print(f"\nDry run — no changes written. Would update {sum(1 for _ in DEFAULTS) - skipped} row(s).")
        else:
            db.commit()
            print(f"\nCommitted. Updated {updated} row(s).")
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
