#!/usr/bin/env python3
"""
Post-processing script for CellViT analysis outputs.

Decompresses .geojson.snappy files and fixes self-intersecting geometries.
Copies non-snappy files (images, logs, etc.) as-is.

Usage:
    python postprocess_cellvit.py --input-dir /path/to/raw --output-dir /path/to/processed
"""
import argparse
import shutil
from pathlib import Path

import snappy
from shapely import from_geojson, to_geojson
from shapely.validation import make_valid
import json


def fix_geometry(geojson_bytes: bytes) -> bytes:
    """Parse GeoJSON, fix any invalid geometries, return corrected GeoJSON bytes.

    Only modifies geometries that are actually invalid; valid geometries are
    left untouched to avoid subtle serialization differences. Geometry type
    changes from make_valid (e.g. Polygon -> GeometryCollection) are skipped
    to preserve compatibility with downstream consumers.
    """
    try:
        data = json.loads(geojson_bytes)

        # If data is a list, return as-is
        if isinstance(data, list):
            return json.dumps(data).encode("utf-8")

        if data.get("type") == "FeatureCollection":
            for feature in data.get("features", []):
                geom = feature.get("geometry")
                if geom:
                    try:
                        shape = from_geojson(json.dumps(geom))
                        if not shape.is_valid:
                            fixed = make_valid(shape)
                            # Only substitute if the geometry type is preserved
                            if fixed.geom_type == shape.geom_type:
                                feature["geometry"] = json.loads(to_geojson(fixed))
                    except Exception:
                        pass  # Leave this feature's geometry unchanged
        elif data.get("type") == "Feature":
            geom = data.get("geometry")
            if geom:
                try:
                    shape = from_geojson(json.dumps(geom))
                    if not shape.is_valid:
                        fixed = make_valid(shape)
                        if fixed.geom_type == shape.geom_type:
                            data["geometry"] = json.loads(to_geojson(fixed))
                except Exception:
                    pass

        return json.dumps(data).encode("utf-8")
    except Exception:
        # If anything fails, return the original bytes unchanged
        return geojson_bytes


def postprocess(input_dir: Path, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)

    for src in sorted(input_dir.iterdir()):
        if not src.is_file():
            continue

        if src.name.endswith(".geojson.snappy"):
            # Decompress + fix geometry
            out_name = src.name.removesuffix(".snappy")  # → .geojson
            print(f"  Decompress + fix: {src.name} -> {out_name}")
            compressed = src.read_bytes()
            raw = snappy.decompress(compressed)
            fixed = fix_geometry(raw)
            (output_dir / out_name).write_bytes(fixed)
        else:
            # Copy as-is
            print(f"  Copy: {src.name}")
            shutil.copy2(src, output_dir / src.name)


def main():
    parser = argparse.ArgumentParser(description="Post-process CellViT outputs")
    parser.add_argument("--input-dir", required=True, type=Path, help="Directory with raw CellViT outputs")
    parser.add_argument("--output-dir", required=True, type=Path, help="Directory for processed outputs")
    args = parser.parse_args()

    if not args.input_dir.is_dir():
        print(f"Error: input directory not found: {args.input_dir}")
        raise SystemExit(1)

    print(f"Post-processing CellViT outputs:")
    print(f"  Input:  {args.input_dir}")
    print(f"  Output: {args.output_dir}")
    postprocess(args.input_dir, args.output_dir)
    print("Done.")


if __name__ == "__main__":
    main()
