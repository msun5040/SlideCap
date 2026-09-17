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
each pile is each cluster? Those percentages are of all patches in the group, so a
patient with three big resections counts three times as much as one small biopsy.
That is the intended reading, but it also means the numbers are not a patient
average, so `dominance` reports the largest single patient's share of each pile.

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
    weighting: str = "slide"      # slide | patch


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


def _pooled(inp: CompositionInput, slide_counts: Dict[str, np.ndarray],
            kept: List[int], kk: int, seed: int = 0) -> dict:
    """
    Group totals: every patch of group A in one pile, every patch of B in another.

    Shares are of all patches in the group — the plain "what is this group made
    of?" number. Significance is judged by randomising the group label in whole
    patient blocks, because patches within a patient are anything but independent.
    """
    warnings: List[str] = []
    usable = [s for s in inp.slides if (s.in_a != s.in_b) and slide_counts[s.slide_hash].sum() > 0]
    if not usable:
        return {"n_blocks": 0, "clusters": [], "warnings": ["No slides left in either group."],
                "tested": False, "global_p": None}

    # One block per patient; slides without a patient fall back to their case, then
    # to themselves, so they still count towards the totals.
    blocks: Dict[str, List[SlideInfo]] = {}
    for s in usable:
        key = s.patient or (f"case:{s.case_hash}" if s.case_hash else f"slide:{s.slide_hash}")
        blocks.setdefault(key, []).append(s)
    keys = sorted(blocks)

    Ab = np.zeros((len(keys), kk))
    Bb = np.zeros((len(keys), kk))
    for i, key in enumerate(keys):
        for s in blocks[key]:
            (Ab if s.in_a else Bb)[i] += slide_counts[s.slide_hash]

    tot_a, tot_b = Ab.sum(axis=0), Bb.sum(axis=0)
    n_a, n_b = float(tot_a.sum()), float(tot_b.sum())
    if n_a == 0 or n_b == 0:
        return {"n_blocks": len(keys), "clusters": [], "tested": False, "global_p": None,
                "warnings": ["One of the groups has no patches left."]}

    share_a, share_b = _shares(tot_a, kk), _shares(tot_b, kk)
    obs_pp = (share_b - share_a) * 100.0

    both_blocks = [i for i in range(len(keys)) if Ab[i].sum() > 0 and Bb[i].sum() > 0]
    one_sided = len(keys) - len(both_blocks)
    if one_sided:
        share = float((Ab[[i for i in range(len(keys)) if i not in both_blocks]].sum() +
                       Bb[[i for i in range(len(keys)) if i not in both_blocks]].sum()) / (n_a + n_b))
        warnings.append(f"{one_sided} patient(s) have slides in only one group; they are in the totals "
                        f"({share * 100:.0f}% of all patches) but say nothing about a within-patient shift.")

    # How much of each pile comes from its biggest contributor.
    dom_a = float(Ab.sum(axis=1).max() / n_a)
    dom_b = float(Bb.sum(axis=1).max() / n_b)
    # Only worth flagging when one patient is well above an even share — with
    # three patients a third each is simply what even looks like.
    dom_threshold = max(0.25, 1.5 / len(keys))
    for lab, dom in (("A", dom_a), ("B", dom_b)):
        if dom > dom_threshold:
            warnings.append(f"One patient contributes {dom * 100:.0f}% of the group-{lab} patches; "
                            f"the pooled percentages lean on them.")

    nb = len(keys)
    Tot = Ab + Bb
    grand = Tot.sum(axis=0)

    def stats_from_sel(sel: np.ndarray) -> np.ndarray:
        """sel (m × nb) of 1 = keep the block's labels, 0 = swap them → Δ in pp.

        The pooled test scores the same percentage-point difference the table
        shows, not a CLR of it: a permutation test needs no particular scale, and
        a cluster whose own share held steady while its neighbours moved should
        not come out "changed".
        """
        a = sel @ Ab + (1.0 - sel) @ Bb
        b = grand - a
        return (_shares(b, kk) - _shares(a, kk)) * 100.0

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

    # Patient-level bootstrap of the same pooled difference.
    ci_lo = np.full(kk, np.nan)
    ci_hi = np.full(kk, np.nan)
    if nb >= 2:
        rng = np.random.default_rng(seed + 1)
        draws = np.empty((BOOTSTRAP_RESAMPLES, kk))
        for start in range(0, BOOTSTRAP_RESAMPLES, 250):
            m = min(250, BOOTSTRAP_RESAMPLES - start)
            idx = rng.integers(0, nb, size=(m, nb))
            a = Ab[idx].sum(axis=1)
            b = Bb[idx].sum(axis=1)
            ok = (a.sum(axis=1) > 0) & (b.sum(axis=1) > 0)
            d = (_shares(b, kk) - _shares(a, kk)) * 100.0
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
            "log2_ratio": float(np.log2(share_b[j] / share_a[j])),
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
        "n_patches_a": int(round(n_a)), "n_patches_b": int(round(n_b)),
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
