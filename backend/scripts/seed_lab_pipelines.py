"""
Seed the lab's standard analysis pipelines into the backend registry.

This is the reproducible source of truth for the shared registry. The
registry lives in each backend's local SQLite DB (see config.py: it is
deliberately local-disk, not on the network share), so pipelines must be
registered against whichever backend users actually hit. For the shared
deployment that means running this ON THE WINDOWS SERVER:

    LOCAL_DATA_DIR=~/.slidecap python scripts/seed_lab_pipelines.py

Idempotent: existing analyses (matched by name) are skipped. Pass --force to
overwrite their fields with the definitions below.

The CellViT postprocess command is resolved to THIS repo's copy of
postprocess_cellvit.py via Path(__file__), so it is correct on whatever host
runs the seeder — no hardcoded per-machine path.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from app.config import settings  # noqa: E402
from app.db import init_db, get_session, Analysis  # noqa: E402

# Resolve the postprocess script to this repo's copy (cross-platform).
# Quoted so paths containing spaces (common on Windows) survive shell=True.
_PP_SCRIPT = Path(__file__).resolve().parent / "postprocess_cellvit.py"
_CELLVIT_POSTPROCESS = (
    f'"{sys.executable}" "{_PP_SCRIPT}" '
    "--input-dir {input_dir} --output-dir {output_dir}"
)

PIPELINES: list[dict] = [
    {
        "name": "CellViT",
        "version": "1.1",
        "description": "cell nucleus segmentation",
        "kind": "cellvit",
        "script_path": "/ligonlab/michael/CellViT_pipeline/run_cellvit_resume.sh",
        "working_directory": "/ligonlab/michael/CellViT_pipeline",
        "env_setup": (
            "source cellvit_env_aries/bin/activate "
            "&& export TMPDIR=/ligonlab/michael/CellViT_pipeline/tmp "
            "&& export RAY_EXPERIMENTAL_NOSET_CUDA_VISIBLE_DEVICES=1"
        ),
        "command_template": (
            "./run_cellvit_resume.sh {wsi_dir} {outdir} "
            "./checkpoints/CellViT-SAM-H-x40-AMP.pth {gpu} {batch_size}"
        ),
        "postprocess_template": _CELLVIT_POSTPROCESS,
        "transforms": (
            '[{"match": "*.geojson.snappy", '
            '"ops": ["decompress_snappy", "fix_geojson_geometry"]}]'
        ),
        "gpu_required": True,
        "estimated_runtime_minutes": 60,
    },
    {
        "name": "UNI",
        "version": "1.1",
        "description": "cell segmentation",
        "kind": "cellvit",
        "script_path": "/ligonlab/Prem/UNI_CCNU/TRIDENT/run_uni_resumable_segmentation.sh",
        "working_directory": "/ligonlab/michael/UNI_pipeline",
        "env_setup": (
            "source trident_env/bin/activate "
            "&& export TMPDIR=/ligonlab/michael/UNI_pipeline/tmp "
            "&& export RAY_EXPERIMENTAL_NOSET_CUDA_VISIBLE_DEVICES=1"
        ),
        "command_template": (
            "./run_uni_resumable_segmentation.sh "
            "--wsi_dir {wsi_dir} --results_dir {outdir} --gpu {gpu}"
        ),
        "postprocess_template": None,
        "transforms": None,
        "gpu_required": True,
        "estimated_runtime_minutes": 60,
    },
]


def main() -> None:
    parser = argparse.ArgumentParser(description="Seed lab analysis pipelines.")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite fields of analyses that already exist (matched by name).",
    )
    args = parser.parse_args()

    print(f"DB: {settings.db_path}")
    init_db(settings.db_path)
    db = get_session()
    try:
        for spec in PIPELINES:
            existing = db.query(Analysis).filter_by(name=spec["name"]).first()
            if existing and not args.force:
                print(f"[skip] {spec['name']} already registered (id={existing.id})")
                continue
            if existing:
                for k, v in spec.items():
                    setattr(existing, k, v)
                existing.active = True
                print(f"[update] {spec['name']} (id={existing.id})")
            else:
                db.add(Analysis(active=True, **spec))
                print(f"[create] {spec['name']}")
        db.commit()
    finally:
        db.close()
    print("Done. Postprocess command:", _CELLVIT_POSTPROCESS)


if __name__ == "__main__":
    main()
