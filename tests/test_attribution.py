import unittest,json,copy
from pathlib import Path
from visit_attribution.model import analyze
ROOT=Path(__file__).resolve().parents[1]
class AttributionTests(unittest.TestCase):
    def setUp(self):self.d=json.loads((ROOT/'fixtures/input.json').read_text())
    def test_known_sessions_channels_and_future(self):
        r=analyze(self.d);self.assertEqual((r['session_count'],r['conversion_count'],r['total_revenue_cents']),(3,3,1600))
        self.assertEqual(r['future_event_ids'],['F1']);self.assertEqual(r['channel_totals']['last_non_direct']['email']['revenue_cents'],1500)
    def test_exact_session_gap_stays_in_same_session(self):
        self.d['events'][3]['at']='2026-01-01T00:45:00Z';r=analyze(self.d);self.assertEqual(r['session_count'],2)
    def test_cross_session_attribution_and_money_conservation(self):
        r=analyze(self.d);p=next(c for c in r['conversions'] if c['id']=='P2')
        self.assertEqual(p['session_id'],'P2');self.assertEqual(p['models']['last_non_direct']['touch_id'],'E1')
        for model,totals in r['channel_totals'].items():
            self.assertEqual(sum(t['revenue_cents'] for t in totals.values()),1600)
    def test_lookback_boundary_is_inclusive(self):
        self.d['events'][3]['at']='2026-01-01T01:00:00Z';r=analyze(self.d)
        self.assertEqual(next(c for c in r['conversions'] if c['id']=='P2')['models']['last_non_direct']['channel'],'email')
        self.d['lookback_seconds']=3599;r=analyze(self.d)
        self.assertEqual(next(c for c in r['conversions'] if c['id']=='P2')['models']['last_non_direct']['channel'],'direct')
    def test_offset_duplicate_and_conflict(self):
        duplicate=copy.deepcopy(self.d['events'][0]);duplicate['at']='2026-01-01T08:00:00+08:00';self.d['events'].append(duplicate)
        self.assertEqual(analyze(self.d)['duplicate_events'],1)
        duplicate['channel']='else'
        with self.assertRaises(ValueError):analyze(self.d)
    def test_same_instant_first_and_last_are_deterministic(self):
        self.d['events'].append({'id':'E0','user':'U1','at':'2026-01-01T00:00:00Z','kind':'touch','channel':'search','amount_cents':None})
        p=analyze(self.d)['conversions'][0]
        self.assertEqual(p['models']['first_non_direct']['channel'],'search');self.assertEqual(p['models']['last_non_direct']['channel'],'email')
    def test_no_touch_and_invalid_amount(self):
        r=analyze(self.d);self.assertEqual(r['channel_totals']['last_non_direct']['unattributed']['revenue_cents'],100)
        self.d['events'][0]['amount_cents']=1
        with self.assertRaises(ValueError):analyze(self.d)
    def test_order_and_empty_events(self):
        r=analyze(self.d);self.d['events'].reverse();self.assertEqual(r,analyze(self.d))
        self.d['events']=[];self.assertEqual(analyze(self.d)['session_count'],0)
if __name__=='__main__':unittest.main()
