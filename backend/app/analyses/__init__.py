"""
Analysis-kind plugin registry.

Each "kind" of analysis (CellViT, UNI, HoVer-Net, …) lives in its own module
under this package. A kind declares:

  • the **ops** it owns — pure-Python `bytes -> bytes` functions registered
    via the kind's `register_op` decorator
  • a **default ruleset** — what `Analysis.transforms` is today, but living
    in code so the same kind shipped to multiple installs behaves identically
  • optional UI hints (currently just `output_globs` — file patterns the
    viewer should treat as overlay candidates)

Onboarding a new analysis = drop a new module here and call `register_kind()`
at import time. The Analysis Registry UI populates its kind dropdown from
`list_kinds()`, and the transforms editor scopes its op picker to the
selected kind so users can't mix snappy decompression into a UNI pipeline.

We deliberately keep op *definitions* in code (not the DB). The composition
layer — naming and sharing pipelines as presets — is the user-facing
customization surface and will live in DB+UI as a separate change.
"""
from __future__ import annotations

import fnmatch
import importlib
import json
import pkgutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional


# ── Types ──────────────────────────────────────────────────────────────


@dataclass
class TransformOp:
    name: str
    description: str
    fn: Callable[[bytes], bytes]


# A renderer is a kind-specific compute that needs *more* context than an op:
# it gets the slide's output directory and the slide-file stem (so it can
# locate sibling files — e.g. UNI's UMAP renderer joins features.h5 +
# patches.h5). Renderers return JSON the frontend can plot directly.
#
# Renderer signature: (output_dir: Path, stem: str, params: dict) -> dict
# `stem` is the source slide's filename without extension (e.g.
# "BS23-W44115_A1-1_HNE_123437"), used to match the right sibling files
# under output_dir.
RendererFn = Callable[[Path, str, Dict[str, Any]], Any]


@dataclass
class RendererSpec:
    id: str                  # stable, URL-safe identifier ("umap")
    name: str                # human label ("UMAP projection")
    description: str
    fn: RendererFn
    # JSON-shape hint for the frontend: which renderer type this is, so the
    # UI knows what component to mount. Today only "scatter2d" — future:
    # "heatmap", "stats_panel", etc.
    output: str = "scatter2d"


@dataclass
class AnalysisKind:
    """
    A plugin describing one family of analyses (e.g. CellViT, UNI).

    `ops` is the kind-scoped op registry (single-file bytes→bytes transforms,
    applied at file-serve time). `renderers` is the kind-scoped renderer
    registry (multi-file compute returning JSON for the frontend to plot).
    `default_rules` is the ruleset used by `get_result_file` when an Analysis
    row has no transforms override of its own. Kinds are registered at import
    time by their plugin module via `register_kind(...)`.
    """
    id: str                                       # stable identifier ("cellvit"); stored in Analysis.kind
    name: str                                     # human label for the UI
    description: str
    default_rules: list                           # list of {"match": glob, "ops": [...]} dicts
    ops: Dict[str, TransformOp] = field(default_factory=dict)
    renderers: Dict[str, RendererSpec] = field(default_factory=dict)
    output_globs: List[str] = field(default_factory=list)  # file patterns flagged as viewer-renderable

    def register_op(self, name: str, description: str):
        """Decorator used inside a plugin module to add an op to this kind."""
        def deco(fn: Callable[[bytes], bytes]):
            self.ops[name] = TransformOp(name=name, description=description, fn=fn)
            return fn
        return deco

    def register_renderer(self, id: str, name: str, description: str, output: str = "scatter2d"):
        """Decorator used inside a plugin module to add a per-slide renderer."""
        def deco(fn: RendererFn):
            self.renderers[id] = RendererSpec(id=id, name=name, description=description, fn=fn, output=output)
            return fn
        return deco

    def list_ops(self) -> List[dict]:
        return [
            {"name": op.name, "description": op.description}
            for op in sorted(self.ops.values(), key=lambda o: o.name)
        ]

    def list_renderers(self) -> List[dict]:
        return [
            {"id": r.id, "name": r.name, "description": r.description, "output": r.output}
            for r in sorted(self.renderers.values(), key=lambda r: r.name.lower())
        ]


# ── Registry ───────────────────────────────────────────────────────────


_KINDS: Dict[str, AnalysisKind] = {}

# Default kind id used when an Analysis row has no `kind` set yet (legacy
# rows from before this refactor). Matches today's CellViT-only behavior.
DEFAULT_KIND_ID = "cellvit"


def register_kind(kind: AnalysisKind) -> AnalysisKind:
    if kind.id in _KINDS:
        # Re-registration is fine during dev reloads — last-write wins.
        pass
    _KINDS[kind.id] = kind
    return kind


def get_kind(kind_id: Optional[str]) -> Optional[AnalysisKind]:
    if not kind_id:
        return _KINDS.get(DEFAULT_KIND_ID)
    return _KINDS.get(kind_id)


def list_kinds() -> List[dict]:
    return [
        {
            "id": k.id,
            "name": k.name,
            "description": k.description,
            "default_rules": k.default_rules,
            "output_globs": k.output_globs,
            "renderers": k.list_renderers(),
        }
        for k in sorted(_KINDS.values(), key=lambda k: k.name.lower())
    ]


# ── Rule execution ─────────────────────────────────────────────────────


class TransformError(Exception):
    """Raised when an op in a matched rule fails — the file endpoint converts
    this to a 500 with the op name in the detail so callers see what broke."""
    def __init__(self, op_name: str, filename: str, original: Exception):
        self.op_name = op_name
        self.filename = filename
        self.original = original
        super().__init__(f"transform op {op_name!r} failed on {filename}: {original}")


def apply_rules(
    kind: AnalysisKind,
    filename: str,
    data: bytes,
    rules: list,
) -> tuple[bytes, list[str]]:
    """
    Walk `rules`, find the first whose glob matches `filename`, run its ops
    using `kind`'s op registry.

    Ops not declared by this kind are skipped with a warning rather than
    failing — this keeps presets from an older config valid after a kind's
    op set is reshuffled, but still surfaces obvious typos in the log.
    """
    if not rules:
        return data, []
    for rule in rules:
        if not isinstance(rule, dict):
            continue
        match = rule.get("match")
        ops = rule.get("ops") or []
        if not match or not isinstance(ops, list):
            continue
        if not fnmatch.fnmatch(filename, match):
            continue
        applied: list[str] = []
        for op_name in ops:
            op = kind.ops.get(op_name)
            if not op:
                print(f"[analyses] kind={kind.id!r} unknown op {op_name!r} (skipped)")
                continue
            try:
                data = op.fn(data)
                applied.append(op_name)
            except Exception as e:
                raise TransformError(op_name, filename, e)
        return data, applied
    return data, []


def parse_rules(raw: str | None) -> list:
    """Parse the JSON-encoded Analysis.transforms column. Returns [] on any error."""
    if not raw:
        return []
    try:
        rules = json.loads(raw)
        return rules if isinstance(rules, list) else []
    except Exception:
        return []


def resolve_rules(kind: AnalysisKind, analysis_transforms_json: str | None) -> list:
    """
    Resolution order used by the file endpoint:
      1. Analysis.transforms column (admin override on the row)
      2. kind.default_rules (code-shipped default)

    Preset-based resolution (per-user / explicit ?preset_id=) plugs in
    *above* step 1 when the preset table lands.
    """
    rules = parse_rules(analysis_transforms_json)
    if rules:
        return rules
    return list(kind.default_rules)


# ── Plugin discovery ───────────────────────────────────────────────────


def _discover_plugins() -> None:
    """
    Import every sibling module so its `register_kind(...)` call fires.

    Plugins are pure Python — they're trusted code, not user input. We walk
    the package rather than maintaining a manual import list so adding a new
    `analyses/<name>.py` is the only step needed to onboard a new kind.
    """
    package = importlib.import_module(__name__)
    for mod_info in pkgutil.iter_modules(package.__path__):
        if mod_info.name.startswith("_"):
            continue
        importlib.import_module(f"{__name__}.{mod_info.name}")


_discover_plugins()
