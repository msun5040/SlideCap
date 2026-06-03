"""
CellViT analysis kind.

CellViT produces per-tile GeoJSON polygons (one polygon per detected cell)
that the cluster post-process script compresses with snappy framing before
upload. The serving-time pipeline mirrors that script: decompress, then run
geometry repair so QuPath-incompatible self-intersections don't break the
viewer's GeoJSON overlay.

Anything CellViT-specific lives here. New analysis kinds (UNI, HoVer-Net, …)
get their own sibling module — they should not extend or share this one.
"""
from __future__ import annotations

import io
import json

from . import AnalysisKind, register_kind


CELLVIT = AnalysisKind(
    id="cellvit",
    name="CellViT",
    description="Cell segmentation pipeline producing per-tile GeoJSON polygons (snappy-compressed).",
    default_rules=[
        {"match": "*.geojson.snappy", "ops": ["decompress_snappy", "fix_geojson_geometry"]},
    ],
    output_globs=["*.geojson", "*.geojson.snappy", "*.geojson.gz"],
)
register_kind(CELLVIT)


# ── Ops ────────────────────────────────────────────────────────────────


_SNAPPY_STREAM_MAGIC = b"\xff\x06\x00\x00sNaPpY"


@CELLVIT.register_op(
    "decompress_snappy",
    "Decompress Snappy-compressed bytes (raw block or stream/framing format — auto-detected)",
)
def _decompress_snappy(data: bytes) -> bytes:
    import snappy

    # Stream/framing format ("snappy framed") starts with this 10-byte magic.
    # That's what most language libraries produce when they want a
    # self-delimiting file (the format Hadoop and SnappyOutputStream use).
    if data.startswith(_SNAPPY_STREAM_MAGIC):
        out = io.BytesIO()
        snappy.stream_decompress(io.BytesIO(data), out)
        return out.getvalue()

    # Otherwise assume raw block format (one compressed chunk, no framing).
    return snappy.decompress(data)


@CELLVIT.register_op(
    "fix_geojson_geometry",
    "Repair invalid GeoJSON polygons via shapely.make_valid (preserves geometry type)",
)
def _fix_geojson_geometry(data: bytes) -> bytes:
    """
    Parse GeoJSON, fix any invalid geometries, return corrected GeoJSON bytes.
    Mirrors the logic in scripts/postprocess_cellvit.py so on-demand transforms
    behave identically to the script-based pipeline.
    """
    from shapely import from_geojson, to_geojson
    from shapely.validation import make_valid

    try:
        doc = json.loads(data)
    except Exception:
        return data  # not JSON — leave alone

    def _fix_feature(feature: dict) -> None:
        geom = feature.get("geometry")
        if not geom:
            return
        try:
            shape = from_geojson(json.dumps(geom))
            if not shape.is_valid:
                fixed = make_valid(shape)
                # Only swap if the geometry type is preserved (e.g. don't replace
                # Polygon with GeometryCollection — downstream consumers may not
                # know what to do with it).
                if fixed.geom_type == shape.geom_type:
                    feature["geometry"] = json.loads(to_geojson(fixed))
        except Exception:
            pass  # leave this feature unchanged

    if isinstance(doc, list):
        return json.dumps(doc).encode("utf-8")

    t = doc.get("type")
    if t == "FeatureCollection":
        for feat in doc.get("features", []) or []:
            _fix_feature(feat)
    elif t == "Feature":
        _fix_feature(doc)

    return json.dumps(doc).encode("utf-8")
