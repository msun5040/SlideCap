"""Regression checks that need NumPy, but no live database, slides or GPU server.

Run: python -m unittest discover -s backend/tests -v
"""
import ast
import csv
import importlib.util
import io
from pathlib import Path
import sys
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace, ModuleType
import unittest
from unittest.mock import patch

import numpy as np

ROOT = Path(__file__).resolve().parents[1]


def load_service(name):
    spec = importlib.util.spec_from_file_location(name, ROOT / 'app' / 'services' / f'{name}.py')
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


cc = load_service('cluster_composition')
export = load_service('composition_export')


def slide(h, start, count, case='CS1', patient_id='', group='a'):
    return dict(slide_hash=h, slide_id=h, slide_name=h, case_id=case, case_hash=case,
                patient_id=patient_id, patient='same label' if patient_id else '',
                group=group, group_key=group, start=start, count=count)


def clustering(k, labels, excluded=(), cid=1, far=None):
    return dict(id=cid, label=f'k={k}', k=k, labels=np.array(labels),
                far=np.array(far) if far is not None else None, excluded=excluded)


class ExportTests(unittest.TestCase):
    def rows(self, slides, clusterings, **kw):
        return list(csv.DictReader(io.StringIO(export.composition_csv(slides, clusterings, **kw))))

    def test_all_slides_include_unpaired_and_ungrouped(self):
        rows = self.rows([slide('SL1', 0, 2), slide('SL2', 2, 2, case='CS2', group='')],
                         [clustering(2, [0, 1, 1, 1])])
        self.assertEqual(len(rows), 4)
        self.assertEqual(rows[-1]['n_patches'], '2')
        self.assertEqual(rows[-1]['pct_of_unit'], '100.0')

    def test_case_counts_pool_without_pseudocount(self):
        rows = self.rows([slide('SL1', 0, 2), slide('SL2', 2, 3)],
                         [clustering(2, [0, 1, 1, 1, 1])], level='case')
        self.assertEqual(rows[0]['n_slides'], '2')
        self.assertEqual(rows[0]['n_patches'], '1')
        self.assertEqual(rows[0]['pct_of_unit'], '20.0')

    def test_patients_with_same_label_and_unassigned_cases_remain_separate(self):
        slides = [slide('S1', 0, 1, patient_id='1'), slide('S2', 1, 1, patient_id='2'),
                  slide('S3', 2, 1, case='CS3'), slide('S4', 3, 1, case='CS4')]
        rows = self.rows(slides, [clustering(2, [0, 1, 0, 1])], level='patient')
        self.assertEqual(len({r['unit_id'] for r in rows}), 4)

    def test_groups_do_not_merge_even_with_matching_display_names(self):
        a, b = slide('S1', 0, 1), slide('S2', 1, 1, group='b')
        a['group'] = b['group'] = 'same group name'
        rows = self.rows([a, b], [clustering(2, [0, 1])], level='case')
        self.assertEqual(len(rows), 4)

    def test_wide_union_columns_preserve_cluster_identity(self):
        rows = self.rows([slide('S1', 0, 3)],
                         [clustering(2, [0, 1, 1], excluded=[0]),
                          clustering(3, [0, 1, 2], excluded=[1], cid=2)], layout='wide')
        self.assertNotIn(None, rows[0])
        self.assertNotIn(None, rows[1])
        self.assertEqual(rows[0]['cluster_1_n'], '')
        self.assertEqual(rows[0]['cluster_2_n'], '2')
        self.assertEqual(rows[0]['cluster_3_n'], '')
        self.assertEqual(rows[1]['cluster_1_n'], '1')
        self.assertEqual(rows[1]['cluster_2_n'], '')
        self.assertEqual(rows[1]['cluster_3_n'], '1')

    def test_far_and_cluster_exclusions_change_denominator(self):
        rows = self.rows([slide('S1', 0, 4)],
                         [clustering(3, [0, 1, 1, 2], excluded=[2], far=[0, 1, 0, 0])], exclude_far=True)
        self.assertEqual([r['pct_of_unit'] for r in rows], ['50.0', '50.0'])
        self.assertEqual(rows[0]['total_patches'], '4')
        self.assertEqual(rows[0]['kept_patches'], '2')
        self.assertEqual(rows[0]['far_patches'], '1')

    def test_zero_denominator_is_blank_not_a_false_zero_percent(self):
        rows = self.rows([slide('S1', 0, 1)], [clustering(2, [0], far=[1])], exclude_far=True)
        self.assertEqual(rows[0]['n_patches'], '0')
        self.assertEqual(rows[0]['pct_of_unit'], '')


class CompositionTests(unittest.TestCase):
    def test_distinct_patient_ids_do_not_form_a_false_pair(self):
        slides = [cc.SlideInfo('a', 'ca', '1', True, False, 0, 2, 'P1'),
                  cc.SlideInfo('b', 'cb', '2', False, True, 2, 2, 'P1')]
        result = cc.compute(cc.CompositionInput(slides, np.array([0, 0, 1, 1]), None, 2, np.array([.5, .5])))
        self.assertEqual(result['n_pairs'], 0)
        self.assertEqual(result['pooled']['n_blocks'], 2)
        self.assertEqual({p['patient_id'] for p in result['unpaired']}, {'1', '2'})

    def test_real_pair_keeps_display_label(self):
        slides = [cc.SlideInfo('a', 'ca', '1', True, False, 0, 2, 'P1'),
                  cc.SlideInfo('b', 'cb', '1', False, True, 2, 2, 'P1')]
        result = cc.compute(cc.CompositionInput(slides, np.array([0, 0, 1, 1]), None, 2, np.array([.5, .5])))
        self.assertEqual(result['n_pairs'], 1)
        self.assertEqual(result['patients'][0]['patient'], 'P1')
        self.assertEqual(result['patients'][0]['patient_id'], '1')

    def test_capped_resamples_match_explicit_case_replication(self):
        counts = np.array([[960, 5370], [63, 171], [157, 1], [95, 108]], dtype=float)
        copies = np.array([[1, 1, 1, 1], [2, 0, 1, 1], [0, 0, 1, 1], [0, 0, 0, 0]])
        shares = cc._capped_shares(counts, copies, .4)
        for i in range(3):
            expanded = np.repeat(counts, copies[i], axis=0)
            n = expanded.sum(axis=1)
            w = cc._cap_weights(n, .4)
            expected = (w[:, None] * expanded / n[:, None]).sum(axis=0) / w.sum()
            np.testing.assert_allclose(shares[i], expected)
        self.assertTrue(np.isnan(shares[3]).all())

    def test_capped_permutation_recomputes_weights(self):
        counts = np.array([[[960, 5370], [63, 171]], [[157, 1], [95, 108]],
                           [[94, 22], [144, 52]], [[70, 83], [60, 91]]], dtype=float)
        slides, sc = [], {}
        for i in range(4):
            for g in range(2):
                h = f'{i}-{g}'
                slides.append(cc.SlideInfo(h, h, str(i), g == 0, g == 1, 0, int(counts[i, g].sum())))
                sc[h] = counts[i, g]
        with patch.object(cc, 'BOOTSTRAP_RESAMPLES', 20):
            result = cc._pooled(cc.CompositionInput(slides, np.array([]), None, 2, np.array([.5, .5]),
                                                   pool_cap_pct=40), sc, [0, 1], 2)
        self.assertEqual([c['p'] for c in result['clusters']], [.75, .75])


class AssignmentCacheTests(unittest.TestCase):
    def test_simultaneous_labels_and_far_requests_generate_once(self):
        # Load the real endpoint helper without starting FastAPI's filesystem /
        # DB initialization or requiring the optional WSI readers.
        tree = ast.parse((ROOT / 'app' / 'main.py').read_text())
        node = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == '_overlay_assignment')
        node.body = [n for n in node.body if not isinstance(n, ast.ImportFrom)]
        for arg in node.args.args:
            arg.annotation = None
        lock = threading.RLock()
        calls = []
        def assign(*args):
            calls.append(1)
            time.sleep(.03)
            return np.array([0, 1], dtype='<i2'), np.array([0, 1], dtype=np.uint8), None
        orm = ModuleType('sqlalchemy.orm')
        orm.object_session = lambda obj: None
        with tempfile.TemporaryDirectory() as tmp, patch.dict(sys.modules, {'sqlalchemy.orm': orm}):
            ov = SimpleNamespace(id=1, projection_id=2, projection=SimpleNamespace(reduced_dim=2),
                                 reduced_path='input', point_count=2, get_report=lambda: {})
            env = dict(np=np, po=SimpleNamespace(open_reduced=lambda *a: None, assign=assign),
                       _projection_root=lambda: Path(tmp), _overlay_ref_lock=lambda _: lock,
                       _clustering_centroids=lambda *a: {'report': {'agreement': 1, 'n_clusters': 2}},
                       settings=SimpleNamespace(local_data_path=Path(tmp)),
                       _update_model_row=lambda *a, **kw: None, ProjectionOverlay=object)
            import json
            env['json'] = json
            exec(compile(ast.Module(body=[node], type_ignores=[]), '<assignment helper>', 'exec'), env)
            with ThreadPoolExecutor(2) as pool:
                results = list(pool.map(lambda _: env['_overlay_assignment'](ov, SimpleNamespace(id=3)), range(2)))
            self.assertEqual(len(calls), 1)
            for labels, far in results:
                np.testing.assert_array_equal(labels, [0, 1])
                np.testing.assert_array_equal(far, [0, 1])


if __name__ == '__main__':
    unittest.main()
