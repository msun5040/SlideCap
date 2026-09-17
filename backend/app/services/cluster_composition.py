"""
Patient-paired cluster composition: does the mix of clusters shift between two
groups of slides (e.g. pre vs post treatment) within the same patients?

The unit of analysis is the patient, never the patch — a million patches make any
patch-level difference "significant". Per slide, cluster counts become proportions;
per patient and group, slides are averaged (each slide weighted equally, so one
big resection doesn't dominate) or pooled; then each patient's B − A difference is
the observation.

Proportions sum to one, so one cluster rising forces others down. Tests therefore
use centred log-ratios (CLR) of the proportions; displayed numbers stay in plain
percentage points.

  * Per cluster: paired Wilcoxon signed-rank on CLR differences, BH-corrected
    within the clustering. Below MIN_PAIRS_FOR_TEST patients only effect sizes.
  * Per clustering: a sign-flip permutation test (flipping swaps a patient's A and
    B) on Σ_j (mean CLR difference_j)² — "did the composition change at all?".

Alongside that, `pooled` answers the other question people ask: throw every patch
of group A into one pile and every patch of group B into another — what share of
each pile is each cluster? By default those percentages are of all patches in the
group, so a patient with three big resections counts three times as much as one
small biopsy. Unlike the paired path these are plain counts with no pseudocount:
a group total of zero for a cluster is a true zero.

When that is too much sway, `pool_unit` and `pool_cap_pct` rebalance the pile
without throwing a single patch away. Every slide/case/patient becomes its own
percentage first; the group's figure is then a weighted mean of those percentages.
`pool_unit` picks the weight: "patch" (weight = patches, the raw pile), or "slide"
/ "case" / "patient" (one vote each, whatever the tissue area). `pool_cap_pct`
keeps patch weighting but caps any one case at that share of its group, scaling it
down rather than dropping its patches — the cap is found by bisection so the
capped cases land exactly on the limit. `dominance` reports the largest single
contributor's share after all of that.

Patches are not independent, so the pooled test is still anchored to patients: the
label is randomised in whole patient blocks (swap that patient's A and B slides;
a patient present in only one group moves wholesale to the other), the pooled
shares are recomputed each time, and the observed CLR difference is scored against
that null. The 95% intervals are a patient-level bootstrap of the same statistic.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

import numpy as np

PSEUDOCOUNT = 0.5
MIN_PAIRS_FOR_TEST = 6
EXACT_PERMUTATION_MAX_N = 15
RANDOM_PERMUTATIONS = 10_000
BOOTSTRAP_RESAMPLES = 2_000
MIN_BLOCKS_FOR_POOLED_TEST = 4


@dataclass
class SlideInfo:
    slide_hash: str
    case_hash: Optional[str]
    patient: Optional[str]        # patient label, None if unassigned
    in_a: bool
    in_b: bool
    start: int
    count: int


@dataclass
class CompositionInput:
    slides: List[SlideInfo]
    labels: np.ndarray             # int16 per overlay point
    far: Optional[np.ndarray]      # uint8 per overlay point, or None
    n_clusters: int
    ref_share: np.ndarray          # reference share per cluster (all clusters)
    exclude_clusters: List[int] = field(default_factory=list)
    exclude_far: bool = False
    weighting: str = "slide"      # slide | patch — how a patient's slides combine (paired view)
    pool_unit: str = "patch"      # patch | slide | case | patient — what carries equal weight in the pooled view
    pool_cap_pct: Optional[float] = None   # patch pooling only: no case above this share of its group


def _bh(p: List[Optional[float]]) -> List[Optional[float]]:
    idx = [i for i, v in enumerate(p) if v is not None]
    q: List[Optional[float]] = [None] * len(p)
    if not idx:
        return q
    vals = np.array([p[i] for i in idx], dtype=float)
    m = len(vals)
    order = np.argsort(vals)
    ranked = vals[order] * m / (np.arange(m) + 1)
    ranked = np.minimum.accumulate(ranked[::-1])[::-1]
    out = np.empty(m)
    out[order] = np.minimum(ranked, 1.0)
    for j, i in enumerate(idx):
        q[i] = float(out[j])
    return q


def _clr(P: np.ndarray) -> np.ndarray:
    L = np.log(P)
    return L - L.mean(axis=-1, keepdims=True)


def _permutation_p(D: np.ndarray, seed: int = 0) -> Optional[float]:
    """Sign-flip test on n × k CLR differences."""
    n = D.shape[0]
    if n < 2:
        return None
    obs = float((D.mean(axis=0) ** 2).sum())
    if n <= EXACT_PERMUTATION_MAX_N:
        signs = ((np.arange(2 ** n)[:, None] >> np.arange(n)[None, :]) & 1) * 2 - 1
        stats = (((signs @ D) / n) ** 2).sum(axis=1)
        return float((stats >= obs - 1e-12).mean())
    rng = np.random.default_rng(seed)
    hits = 0
    for s in range(0, RANDOM_PERMUTATIONS, 1000):
        b = min(1000, RANDOM_PERMUTATIONS - s)
        signs = rng.choice([-1.0, 1.0], size=(b, n))
        stats = (((signs @ D) / n) ** 2).sum(axis=1)
        hits += int((stats >= obs - 1e-12).sum())
    return (hits + 1) / (RANDOM_PERMUTATIONS + 1)


def _shares(counts: np.ndarray, kk: int) -> np.ndarray:
    """Counts (…, kk) → shares, with the pseudocount that keeps CLR finite."""
    c = counts + PSEUDOCOUNT
    return c / c.sum(axis=-1, keepdims=True)


def _cap_weights(n: np.ndarray, cap: float) -> np.ndarray:
    """
    Weights min(n_i, C), with C set so no unit exceeds `cap` of the total.

    Nothing is discarded: an over-large case still contributes every one of its
    patches, they just carry less weight each. C is found by bisection because
    capping one unit lowers the total, which can push the next one over the line.
    """
    n = np.asarray(n, dtype=float)
    m = n.size
    if m == 0 or n.sum() <= 0:
        return n
    if cap <= 1.0 / m:                       # below an even share: even is the best we can do
        return np.where(n > 0, 1.0, 0.0)
    if n.max() / n.sum() <= cap + 1e-12:     # already within the cap
        return n
    lo, hi = 0.0, float(n.max())
    for _ in range(60):
        mid = (lo + hi) / 2
        w = np.minimum(n, mid)
        if w.sum() <= 0 or w.max() / w.sum() > cap:
            hi = mid
        else:
            lo = mid
    return np.minimum(n, lo)


def _pooled(inp: CompositionInput, slide_counts: Dict[str, np.ndarray],
            kept: List[int], kk: int, seed: int = 0) -> dict:
    """
    Group totals: everything in group A on one side, everything in group B on the
    other, as a single composition per group.

    With the default `pool_unit="patch"` and no cap that is the plain pile of
    patches. Otherwise each slide / case / patient becomes its own percentage and
    the group's figure is a weighted mean of those, so no patch is dropped and no
    single slide can run away with the group. Significance is judged by randomising
    the group label in whole patient blocks, because patches within a patient are
    anything but independent.
    """
    warnings: List[str] = []
    usable = [s for s in inp.slides if (s.in_a != s.in_b) and slide_counts[s.slide_hash].sum() > 0]
    if not usable:
        return {"n_blocks": 0, "clusters": [], "warnings": ["No slides left in either group."],
                "tested": False, "global_p": None}

    unit = inp.pool_unit if inp.pool_unit in ("patch", "slide", "case", "patient") else "patch"
    cap = inp.pool_cap_pct / 100.0 if (unit == "patch" and inp.pool_cap_pct) else None

    def block_key(s: SlideInfo) -> str:
        # One block per patient; slides without a patient fall back to their case,
        # then to themselves, so they still count towards the totals.
        return s.patient or (f"case:{s.case_hash}" if s.case_hash else f"slide:{s.slide_hash}")

    def unit_key(s: SlideInfo) -> str:
        if unit == "patient":
            return block_key(s)
        if unit == "case" or cap is not None:
            return f"case:{s.case_hash}" if s.case_hash else f"slide:{s.slide_hash}"
        return f"slide:{s.slide_hash}"

    keys = sorted({block_key(s) for s in usable})
    block_of = {k: i for i, k in enumerate(keys)}
    nb = len(keys)

    # Counts per (unit, group). A unit that appears in both groups is two units:
    # the same case before and after is two separate observations.
    units: Dict[tuple, dict] = {}
    for s in usable:
        g = "a" if s.in_a else "b"
        u = units.setdefault((unit_key(s), g), {"block": block_of[block_key(s)], "g": g,
                                                "counts": np.zeros(kk)})
        u["counts"] += slide_counts[s.slide_hash]

    raw_a = float(sum(u["counts"].sum() for u in units.values() if u["g"] == "a"))
    raw_b = float(sum(u["counts"].sum() for u in units.values() if u["g"] == "b"))
    if raw_a == 0 or raw_b == 0:
        return {"n_blocks": nb, "clusters": [], "tested": False, "global_p": None,
                "warnings": ["One of the groups has no patches left."]}

    # Weight per unit, worked out inside its own group.
    cap_notes: List[str] = []
    for g in ("a", "b"):
        sel = [u for u in units.values() if u["g"] == g]
        n = np.array([u["counts"].sum() for u in sel])
        if unit == "patch":
            w = _cap_weights(n, cap) if cap is not None else n
        else:
            w = np.where(n > 0, 1.0, 0.0)
        for u, wi in zip(sel, w):
            u["w"] = float(wi)
        if cap is not None and len(sel):
            even = 1.0 / len(sel)
            if cap < even - 1e-9:
                cap_notes.append(f"a {inp.pool_cap_pct:.0f}% cap is below an even share of the {len(sel)} case(s) "
                                 f"in group {g.upper()} ({even * 100:.0f}% each), so they simply count once each")
            else:
                over = sum(1 for u, raw in zip(sel, n) if u["w"] < raw - 1e-9)
                if over:
                    cap_notes.append(f"{over} case(s) in group {g.upper()} were over the {inp.pool_cap_pct:.0f}% cap "
                                     f"and were scaled down")

    small = [u for u in units.values() if unit != "patch" and 0 < u["counts"].sum() < 50]
    if small:
        warnings.append(f"{len(small)} {unit}(s) have fewer than 50 patches but count as much as the biggest; "
                        f"their percentages are noisy.")
    if cap_notes:
        warnings.append("Cap: " + "; ".join(cap_notes) + ". No patches were dropped.")

    # Per block: weighted composition contributions on each side, so a permutation
    # only has to swap which side a block lands on.
    WP = {"a": np.zeros((nb, kk)), "b": np.zeros((nb, kk))}
    W = {"a": np.zeros(nb), "b": np.zeros(nb)}
    RAW = {"a": np.zeros((nb, kk)), "b": np.zeros((nb, kk))}
    for u in units.values():
        i, g = u["block"], u["g"]
        n = u["counts"].sum()
        p = u["counts"] / n if n > 0 else u["counts"]   # this unit's own percentages
        WP[g][i] += u["w"] * p
        W[g][i] += u["w"]
        RAW[g][i] += u["counts"]

    tot_a = RAW["a"].sum(axis=0)
    tot_b = RAW["b"].sum(axis=0)
    share_a = WP["a"].sum(axis=0) / W["a"].sum()
    share_b = WP["b"].sum(axis=0) / W["b"].sum()
    obs_pp = (share_b - share_a) * 100.0

    both_blocks = [i for i in range(nb) if W["a"][i] > 0 and W["b"][i] > 0]
    one_sided = nb - len(both_blocks)
    if one_sided:
        others = [i for i in range(nb) if i not in both_blocks]
        share = float((RAW["a"][others].sum() + RAW["b"][others].sum()) / (raw_a + raw_b))
        warnings.append(f"{one_sided} patient(s) have slides in only one group; they are in the totals "
                        f"({share * 100:.0f}% of all patches) but say nothing about a within-patient shift.")

    # How much of each group's figure comes from its biggest single contributor,
    # after weighting — this is the number the cap is there to hold down.
    def dominance(g: str) -> float:
        w = np.array([u["w"] for u in units.values() if u["g"] == g])
        return float(w.max() / w.sum()) if w.size and w.sum() > 0 else 0.0

    dom_a, dom_b = dominance("a"), dominance("b")
    # Only worth flagging when one contributor is well above an even share — with
    # three patients a third each is simply what even looks like.
    dom_threshold = max(0.25, 1.5 / max(1, nb))
    for lab, dom in (("A", dom_a), ("B", dom_b)):
        if dom > dom_threshold:
            warnings.append(f"One {unit if unit != 'patch' else 'case'} carries {dom * 100:.0f}% of the group-{lab} "
                            f"figure; the pooled percentages lean on it.")

    grand_wp = WP["a"] + WP["b"]
    grand_w = W["a"] + W["b"]
    sum_wp = grand_wp.sum(axis=0)
    sum_w = grand_w.sum()

    def stats_from_sel(sel: np.ndarray) -> np.ndarray:
        """sel (m × nb) of 1 = keep the block's labels, 0 = swap them → Δ in pp.

        The pooled test scores the same percentage-point difference the table
        shows, not a CLR of it: a permutation test needs no particular scale, and
        a cluster whose own share held steady while its neighbours moved should
        not come out "changed".
        """
        wp_a = sel @ WP["a"] + (1.0 - sel) @ WP["b"]
        w_a = sel @ W["a"] + (1.0 - sel) @ W["b"]
        a = wp_a / np.maximum(w_a, 1e-12)[:, None]
        b = (sum_wp - wp_a) / np.maximum(sum_w - w_a, 1e-12)[:, None]
        return (b - a) * 100.0

    tested = nb >= MIN_BLOCKS_FOR_POOLED_TEST
    pvals: List[Optional[float]] = [None] * kk
    global_p: Optional[float] = None
    if tested:
        obs_abs = np.abs(obs_pp)
        obs_glob = float((obs_pp ** 2).sum())
        hits = np.zeros(kk)
        hits_glob = 0
        total = 0
        if nb <= EXACT_PERMUTATION_MAX_N:
            sel = ((np.arange(2 ** nb)[:, None] >> np.arange(nb)[None, :]) & 1).astype(float)
            d = stats_from_sel(sel)
            hits = (np.abs(d) >= obs_abs - 1e-12).sum(axis=0).astype(float)
            hits_glob = int(((d ** 2).sum(axis=1) >= obs_glob - 1e-12).sum())
            total = sel.shape[0]
            pvals = [float(h / total) for h in hits]
            global_p = float(hits_glob / total)
        else:
            rng = np.random.default_rng(seed)
            for start in range(0, RANDOM_PERMUTATIONS, 500):
                m = min(500, RANDOM_PERMUTATIONS - start)
                sel = rng.integers(0, 2, size=(m, nb)).astype(float)
                d = stats_from_sel(sel)
                hits += (np.abs(d) >= obs_abs - 1e-12).sum(axis=0)
                hits_glob += int(((d ** 2).sum(axis=1) >= obs_glob - 1e-12).sum())
                total += m
            pvals = [float((h + 1) / (total + 1)) for h in hits]
            global_p = float((hits_glob + 1) / (total + 1))

    # Patient-level bootstrap of the same difference.
    ci_lo = np.full(kk, np.nan)
    ci_hi = np.full(kk, np.nan)
    if nb >= 2:
        rng = np.random.default_rng(seed + 1)
        draws = np.empty((BOOTSTRAP_RESAMPLES, kk))
        for start in range(0, BOOTSTRAP_RESAMPLES, 250):
            m = min(250, BOOTSTRAP_RESAMPLES - start)
            idx = rng.integers(0, nb, size=(m, nb))
            wa, wb = W["a"][idx].sum(axis=1), W["b"][idx].sum(axis=1)
            ok = (wa > 0) & (wb > 0)
            a = WP["a"][idx].sum(axis=1) / np.maximum(wa, 1e-12)[:, None]
            b = WP["b"][idx].sum(axis=1) / np.maximum(wb, 1e-12)[:, None]
            d = (b - a) * 100.0
            d[~ok] = np.nan
            draws[start:start + m] = d
        with np.errstate(invalid="ignore"):
            ci_lo = np.nanpercentile(draws, 2.5, axis=0)
            ci_hi = np.nanpercentile(draws, 97.5, axis=0)

    qvals = _bh(pvals)
    clusters = []
    for j, c in enumerate(kept):
        clusters.append({
            "cluster": c,
            "a_pct": float(share_a[j] * 100), "b_pct": float(share_b[j] * 100),
            "delta_pp": float(obs_pp[j]),
            # Undefined when a cluster is empty on one side — reported as nothing
            # rather than an infinity the UI would have to special-case anyway.
            "log2_ratio": (float(np.log2(share_b[j] / share_a[j]))
                           if share_a[j] > 0 and share_b[j] > 0 else None),
            "ci_lo_pp": None if not np.isfinite(ci_lo[j]) else float(ci_lo[j]),
            "ci_hi_pp": None if not np.isfinite(ci_hi[j]) else float(ci_hi[j]),
            "a_patches": int(round(tot_a[j])), "b_patches": int(round(tot_b[j])),
            "p": pvals[j], "q": qvals[j],
        })

    return {
        "n_blocks": nb,
        "n_paired_blocks": len(both_blocks),
        "n_slides_a": sum(1 for s in usable if s.in_a),
        "n_slides_b": sum(1 for s in usable if s.in_b),
        "n_units_a": sum(1 for u in units.values() if u["g"] == "a"),
        "n_units_b": sum(1 for u in units.values() if u["g"] == "b"),
        "n_patches_a": int(round(raw_a)), "n_patches_b": int(round(raw_b)),
        "pool_unit": unit,
        "pool_cap_pct": inp.pool_cap_pct if cap is not None else None,
        "dominance_a": dom_a, "dominance_b": dom_b,
        "tested": tested,
        "global_p": global_p,
        "clusters": clusters,
        "warnings": warnings,
    }


def compute(inp: CompositionInput) -> dict:
    from scipy.stats import wilcoxon

    k = inp.n_clusters
    kept = [c for c in range(k) if c not in set(inp.exclude_clusters)]
    if len(kept) < 2:
        raise ValueError("At least two clusters must remain after exclusions.")
    col = {c: i for i, c in enumerate(kept)}
    kk = len(kept)
    warnings: List[str] = []

    # Per-slide counts over kept clusters.
    slide_counts: Dict[str, np.ndarray] = {}
    for s in inp.slides:
        lab = inp.labels[s.start:s.start + s.count].astype(np.int64)
        m = lab >= 0
        if inp.exclude_far and inp.far is not None:
            m &= inp.far[s.start:s.start + s.count] == 0
        lab = lab[m]
        full = np.bincount(lab, minlength=k)[:k] if lab.size else np.zeros(k, dtype=np.int64)
        slide_counts[s.slide_hash] = full[kept].astype(np.float64)

    both = [s for s in inp.slides if s.in_a and s.in_b]
    if both:
        warnings.append(f"{len(both)} slide(s) are in both groups and were left out.")
    no_patient = [s for s in inp.slides if (s.in_a or s.in_b) and not s.patient]
    if no_patient:
        warnings.append(f"{len(no_patient)} grouped slide(s) aren't assigned to a patient and were left out.")
    empty = [s for s in inp.slides if (s.in_a != s.in_b) and s.patient and slide_counts[s.slide_hash].sum() == 0]
    if empty:
        warnings.append(f"{len(empty)} slide(s) have no patches left after exclusions and were left out.")

    usable = [s for s in inp.slides if s.patient and (s.in_a != s.in_b)
              and slide_counts[s.slide_hash].sum() > 0]

    # Cases split across the two groups.
    case_groups: Dict[str, set] = {}
    for s in usable:
        if s.case_hash:
            case_groups.setdefault(s.case_hash, set()).add("a" if s.in_a else "b")
    split_cases = sum(1 for g in case_groups.values() if len(g) > 1)
    if split_cases:
        warnings.append(f"{split_cases} case(s) have slides in both groups (check the grouping).")

    patients: Dict[str, dict] = {}
    for s in usable:
        g = "a" if s.in_a else "b"
        p = patients.setdefault(s.patient, {"a": [], "b": []})
        p[g].append(s)

    multi_case = sum(1 for p in patients.values()
                     for g in ("a", "b") if len({s.case_hash for s in p[g]}) > 1)
    if multi_case:
        warnings.append(f"{multi_case} patient-group(s) span more than one case (e.g. two post surgeries); "
                        f"their slides were combined.")

    def group_comp(slides: List[SlideInfo]) -> np.ndarray:
        if inp.weighting == "patch":
            c = np.sum([slide_counts[s.slide_hash] for s in slides], axis=0) + PSEUDOCOUNT
            return c / c.sum()
        props = [(slide_counts[s.slide_hash] + PSEUDOCOUNT) / (slide_counts[s.slide_hash].sum() + PSEUDOCOUNT * kk)
                 for s in slides]
        return np.mean(props, axis=0)

    paired, unpaired = [], []
    for label in sorted(patients):
        p = patients[label]
        if p["a"] and p["b"]:
            paired.append({
                "patient": label,
                "a": group_comp(p["a"]), "b": group_comp(p["b"]),
                "a_slides": len(p["a"]), "b_slides": len(p["b"]),
                "a_patches": int(sum(slide_counts[s.slide_hash].sum() for s in p["a"])),
                "b_patches": int(sum(slide_counts[s.slide_hash].sum() for s in p["b"])),
            })
        else:
            unpaired.append({"patient": label, "has": "a" if p["a"] else "b"})

    n = len(paired)
    ref = np.asarray(inp.ref_share, dtype=float)[kept]
    ref = ref / ref.sum() if ref.sum() > 0 else ref
    clusters = []
    pvals: List[Optional[float]] = []
    if n:
        A = np.array([r["a"] for r in paired])
        B = np.array([r["b"] for r in paired])
        Dpp = (B - A) * 100.0
        Dclr = _clr(B) - _clr(A)
    for j, c in enumerate(kept):
        row = {"cluster": c, "ref_pct": float(ref[j] * 100)}
        if n:
            row.update({
                "a_median_pct": float(np.median(A[:, j]) * 100),
                "b_median_pct": float(np.median(B[:, j]) * 100),
                "median_delta_pp": float(np.median(Dpp[:, j])),
                "n_up": int((Dpp[:, j] > 0).sum()),
                "n_down": int((Dpp[:, j] < 0).sum()),
            })
            p = None
            if n >= MIN_PAIRS_FOR_TEST and np.any(Dclr[:, j] != 0):
                try:
                    p = float(wilcoxon(Dclr[:, j], zero_method="wilcox", alternative="two-sided").pvalue)
                except ValueError:
                    p = None
            row["p"] = p
            pvals.append(p)
        else:
            pvals.append(None)
        clusters.append(row)
    for row, q in zip(clusters, _bh(pvals)):
        row["q"] = q

    return {
        "n_pairs": n,
        "pooled": _pooled(inp, slide_counts, kept, kk),
        "kept_clusters": kept,
        "weighting": inp.weighting,
        "tested": n >= MIN_PAIRS_FOR_TEST,
        "global_p": _permutation_p(Dclr) if n >= MIN_PAIRS_FOR_TEST else None,
        "clusters": clusters,
        "patients": [
            {"patient": r["patient"], "a_pct": (r["a"] * 100).round(3).tolist(), "b_pct": (r["b"] * 100).round(3).tolist(),
             "a_slides": r["a_slides"], "b_slides": r["b_slides"],
             "a_patches": r["a_patches"], "b_patches": r["b_patches"]}
            for r in paired
        ],
        "unpaired": unpaired,
        "warnings": warnings,
    }
