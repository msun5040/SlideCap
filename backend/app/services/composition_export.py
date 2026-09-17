"""Raw overlay cluster counts; independent of paired-comparison eligibility.

Percentages use retained patches as the denominator, with no pseudocount or
statistical weighting. Cases/patients are pooled within each group. Missing
patients remain distinct by case rather than becoming one synthetic patient.
"""
import csv
import io

import numpy as np


def composition_csv(slides, clusterings, level="slide", layout="long", exclude_far=False):
    if level not in ("slide", "case", "patient") or layout not in ("long", "wide"):
        raise ValueError("Invalid export level or layout.")
    id_cols = {
        "slide": ["slide_id", "slide_name", "case_id", "patient_id", "patient", "group"],
        "case": ["case_id", "patient_id", "patient", "group"],
        "patient": ["patient_id", "patient", "group"],
    }[level]
    # unit_id remains unambiguous even for deleted metadata or unassigned cases.
    cols = ["unit_id"] + id_cols + ["group_role", "clustering_id", "clustering", "n_slides", "kept_patches",
                                  "total_patches", "far_patches", "excluded_far", "excluded_clusters"]
    rows, all_kept = [], set()
    for cl in clusterings:
        k = cl["k"]
        kept = [c for c in range(k) if c not in set(cl["excluded"])]
        if not kept:
            raise ValueError("At least one cluster must remain for export.")
        all_kept.update(kept)
        units = {}
        for sl in slides:
            if level == "slide":
                uid = sl["slide_id"] or sl["slide_hash"]
            elif level == "case":
                uid = sl["case_id"] or sl["case_hash"] or sl["slide_hash"]
            else:
                uid = ("patient:" + sl["patient_id"] if sl["patient_id"] else
                       "unassigned:" + (sl["case_id"] or sl["case_hash"] or sl["slide_hash"]))
            group_key = sl.get("group_key", sl["group"])
            u = units.setdefault((uid, group_key), {
                "ids": [uid] + [sl.get(c, "") for c in id_cols], "counts": np.zeros(k, dtype=np.int64),
                "total": 0, "far": 0, "n_slides": 0})
            start, end = sl["start"], sl["start"] + sl["count"]
            lab = np.asarray(cl["labels"][start:end], dtype=np.int64)
            far = cl["far"][start:end] if cl["far"] is not None else None
            if len(lab) != sl["count"] or (far is not None and len(far) != sl["count"]):
                raise ValueError("Cluster assignments do not match the overlay's patches.")
            mask = (lab >= 0) & (lab < k)
            if exclude_far and far is not None:
                mask &= far == 0
            u["counts"] += np.bincount(lab[mask], minlength=k)
            u["total"] += sl["count"]
            u["far"] += int(np.count_nonzero(far)) if far is not None else 0
            u["n_slides"] += 1
        for key in sorted(units):
            u = units[key]
            denominator = int(u["counts"][kept].sum())
            base = u["ids"] + [key[1], cl["id"], cl["label"], u["n_slides"], denominator, u["total"],
                               u["far"], exclude_far, ";".join(str(c + 1) for c in sorted(set(cl["excluded"])))]
            values = {c: [int(u["counts"][c]), round(int(u["counts"][c]) / denominator * 100, 6)
                          if denominator else ""] for c in kept}
            rows.append((base, values))
    buf = io.StringIO()
    writer = csv.writer(buf)
    if layout == "long":
        writer.writerow(cols + ["cluster", "n_patches", "pct_of_unit"])
        for base, values in rows:
            for c, pair in values.items():
                writer.writerow(base + [c + 1] + pair)
    else:
        # One union schema for every clustering. A blank means excluded/not in
        # this clustering; zero means a real cluster with no assigned patches.
        clusters = sorted(all_kept)
        writer.writerow(cols + [f"cluster_{c + 1}_{suffix}" for c in clusters for suffix in ("n", "pct")])
        for base, values in rows:
            writer.writerow(base + [v for c in clusters for v in values.get(c, ["", ""])])
    return buf.getvalue()
