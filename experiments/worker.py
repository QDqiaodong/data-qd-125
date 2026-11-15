"""Child-process worker for experiments/run_experiments.py.

Modes:
  functional - contract scenarios plus a small-data model equivalence check;
  scale      - large synthetic load with per-stage peak RSS measurement.

The worker never imports anything from its own directory; the project src tree
is inserted into sys.path explicitly so ``python -I`` still finds the package.
"""
import json
import os
import resource
import sqlite3
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))

from visit_attribution import pipeline  # noqa: E402
from visit_attribution.model import analyze  # noqa: E402

GAP = 1800
LOOKBACK = 3600


def touch(eid, user, at, channel):
    return {'id': eid, 'user': user, 'at': at, 'kind': 'touch',
            'channel': channel, 'amount_cents': None}


def purchase(eid, user, at, amount):
    return {'id': eid, 'user': user, 'at': at, 'kind': 'purchase',
            'channel': None, 'amount_cents': amount}


def batch(batch_id, as_of, events):
    return {'batch_id': batch_id, 'as_of': as_of,
            'session_gap_seconds': GAP, 'lookback_seconds': LOOKBACK,
            'events': events}


def write_batch(workdir, name, payload):
    path = workdir / name
    path.write_text(json.dumps(payload))
    return path


def peak_rss_mib():
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024


def result(db):
    conn = pipeline.connect(db)
    try:
        return pipeline.build_result(conn)
    finally:
        conn.close()


def functional_mode(workdir, *_):
    workdir.mkdir(parents=True, exist_ok=True)
    checks = {}

    # 1. late bridge: two sessions collapse into one, historical attribution rewritten
    db = workdir / 'late.db'
    pipeline.apply_batch(db, write_batch(workdir, 'late-b1.json', batch(
        'B1', '2026-01-02T00:00:00Z',
        [touch('E2', 'U1', '2026-01-01T00:30:00Z', 'email'),
         purchase('P1', 'U1', '2026-01-01T00:35:00Z', 100)])))
    before = result(db)
    pipeline.apply_batch(db, write_batch(workdir, 'late-b2.json', batch(
        'B2', '2026-01-02T00:00:00Z',
        [touch('E1', 'U1', '2026-01-01T00:00:00Z', 'search'),
         purchase('P2', 'U1', '2026-01-01T01:00:01Z', 200)])))
    after = result(db)
    checks['late_bridge'] = {
        'sessions_before': before['session_count'],
        'sessions_after': after['session_count'],
        'merged_session_id': after['sessions'][0]['id'],
        'p1_channel_after': next(c for c in after['conversions']
                                 if c['id'] == 'P1')['models']['first_non_direct']['channel'],
        'p1_event_ids': after['sessions'][0]['event_ids'],
        'passed': (before['session_count'] == 1 and after['session_count'] == 1 and
                   after['sessions'][0]['id'] == 'E1' and
                   after['sessions'][0]['event_ids'] == ['E1', 'E2', 'P1', 'P2'] and
                   next(c for c in after['conversions'] if c['id'] == 'P1'
                        )['models']['first_non_direct']['channel'] == 'search')}

    # 2. same-second touches across batches: ID lexicographic order
    db = workdir / 'same-second.db'
    pipeline.apply_batch(db, write_batch(workdir, 'ss-b1.json', batch(
        'B1', '2026-01-01T01:00:00Z',
        [touch('EZ', 'U1', '2026-01-01T00:00:00Z', 'email')])))
    pipeline.apply_batch(db, write_batch(workdir, 'ss-b2.json', batch(
        'B2', '2026-01-01T01:00:00Z',
        [touch('EA', 'U1', '2026-01-01T00:00:00Z', 'search'),
         purchase('P1', 'U1', '2026-01-01T00:00:00Z', 100)])))
    conv = result(db)['conversions'][0]
    checks['same_second'] = {
        'first_channel': conv['models']['first_non_direct']['channel'],
        'last_channel': conv['models']['last_non_direct']['channel'],
        'passed': (conv['models']['first_non_direct']['channel'] == 'search' and
                   conv['models']['last_non_direct']['channel'] == 'email')}

    # 3. empty batch advances as_of and activates a retained future event
    db = workdir / 'empty.db'
    pipeline.apply_batch(db, write_batch(workdir, 'em-b1.json', batch(
        'B1', '2026-01-01T01:00:00Z',
        [touch('F1', 'U1', '2026-01-01T02:00:00Z', 'search')])))
    summary = pipeline.apply_batch(db, write_batch(workdir, 'em-b2.json', batch(
        'B2', '2026-01-02T00:00:00Z', [])))
    state = result(db)
    checks['empty_advance'] = {
        'activated_events': summary['activated_events'],
        'recomputed_users': summary['recomputed_users'],
        'as_of': state['as_of'],
        'future_left': state['future_event_ids'],
        'passed': (summary['activated_events'] == 1 and summary['recomputed_users'] == 1 and
                   state['as_of'] == '2026-01-02T00:00:00Z' and
                   state['future_event_ids'] == [])}

    # 4. replay: same batch id and bytes is a side-effect-free no-op
    db = workdir / 'replay.db'
    source = write_batch(workdir, 'rp-b1.json', batch(
        'B1', '2026-01-01T01:00:00Z',
        [touch('E1', 'U1', '2026-01-01T00:00:00Z', 'email')]))
    first = pipeline.apply_batch(db, source)
    conn = sqlite3.connect(db)
    snapshot = conn.execute('SELECT * FROM events').fetchall()
    conn.close()
    second = pipeline.apply_batch(db, source)
    conn = sqlite3.connect(db)
    unchanged = conn.execute('SELECT * FROM events').fetchall() == snapshot
    batch_count = conn.execute('SELECT COUNT(*) FROM batches').fetchone()[0]
    conn.close()
    checks['replay'] = {'first_replayed': first['replayed'],
                        'second_replayed': second['replayed'],
                        'rows_unchanged': unchanged, 'batch_rows': batch_count,
                        'passed': (not first['replayed'] and second['replayed'] and
                                   unchanged and batch_count == 1)}
    # same batch id with different bytes must be refused
    try:
        pipeline.apply_batch(db, write_batch(workdir, 'rp-bad.json', batch(
            'B1', '2026-01-01T01:00:00Z',
            [touch('E1', 'U1', '2026-01-01T00:00:01Z', 'email')])))
        checks['replay_conflict'] = {'passed': False, 'reason': 'conflict was accepted'}
    except pipeline.PipelineError:
        checks['replay_conflict'] = {'passed': True}

    # 5. crash recovery: injected fault rolls everything back, retry succeeds
    db = workdir / 'crash.db'
    source = write_batch(workdir, 'cr-b1.json', batch(
        'B1', '2026-01-01T01:00:00Z',
        [touch(f'E{i}', 'U1', f'2026-01-01T00:{i:02d}:00Z', 'email')
         for i in range(5)]))
    os.environ['VA_FAULT_AFTER_EVENTS'] = '2'
    try:
        try:
            pipeline.apply_batch(db, source)
            crashed = False
        except RuntimeError:
            crashed = True
    finally:
        os.environ.pop('VA_FAULT_AFTER_EVENTS', None)
    conn = sqlite3.connect(db)
    leftover = conn.execute('SELECT COUNT(*) FROM events').fetchone()[0]
    conn.close()
    pipeline.apply_batch(db, source)
    recovered = result(db)['session_count']
    checks['crash_recovery'] = {'fault_raised': crashed, 'rows_after_rollback': leftover,
                                'sessions_after_retry': recovered,
                                'passed': crashed and leftover == 0 and recovered == 1}

    # 6. small-data equivalence against the in-memory full model
    db = workdir / 'equiv.db'
    batches = [
        batch('B1', '2026-01-01T01:00:00Z', [
            touch('E1', 'U1', '2026-01-01T00:00:00Z', 'email'),
            touch('E2', 'U1', '2026-01-01T00:10:00Z', 'direct'),
            purchase('P1', 'U1', '2026-01-01T00:15:00Z', 1000),
            touch('F1', 'U1', '2026-01-01T02:00:00Z', 'search')]),
        batch('B2', '2026-01-01T01:30:00Z', [
            purchase('P2', 'U1', '2026-01-01T00:45:01Z', 500),
            purchase('P3', 'U2', '2026-01-01T00:20:00Z', 100)]),
        batch('B3', '2026-01-02T00:00:00Z', [])]
    for i, payload in enumerate(batches, 1):
        pipeline.apply_batch(db, write_batch(workdir, f'eq-b{i}.json', payload))
    full = analyze({'as_of': batches[-1]['as_of'], 'session_gap_seconds': GAP,
                    'lookback_seconds': LOOKBACK,
                    'events': [e for b in batches for e in b['events']]})
    incremental = result(db)
    checks['small_data_equivalence'] = {'passed': full == incremental}

    checks['all_passed'] = all(item['passed'] for item in checks.values())
    return checks


def _iso(dt):
    return dt.strftime('%Y-%m-%dT%H:%M:%SZ')


def scale_mode(workdir, total_events, skew_share):
    """Ingest synthetic events in contract-sized batches (<=8 MB per file).

    The 8 MB per-batch JSON cap is part of the input contract, so the load is
    split into batches of EVENTS_PER_BATCH events; this also bounds the Python
    memory of parsing one batch.
    """
    workdir.mkdir(parents=True, exist_ok=True)
    total_events = int(total_events)
    # The input contract caps every batch at 8 MB and 10,000 events.
    events_per_batch = 10_000
    batch_count = (total_events + events_per_batch - 1) // events_per_batch
    base = datetime(2026, 1, 1, tzinfo=timezone.utc)
    db = workdir / 'scale.db'
    apply_rss = []
    apply_seconds = []
    written = 0
    skew_idx = 0
    small_uid = 0
    # Target share of events belonging to the single skewed user; the
    # remaining events are pairs (touch+purchase) on distinct small users.
    skew_share = float(skew_share)

    for b in range(batch_count):
        target = min(events_per_batch, total_events - written)
        skew_count = int(round(target * skew_share))
        skew_count -= skew_count % 2  # keep touch/purchase alternation even
        remaining = target - skew_count
        small_pairs = remaining // 2
        skew_count += remaining - small_pairs * 2  # keep exact total
        events = []
        # skewed user: dense alternating stream, one giant single-user history
        for _ in range(skew_count):
            at = base + timedelta(seconds=30 * skew_idx)
            if skew_idx % 2 == 0:
                events.append(touch(f'S{skew_idx:08d}', 'skew-user', _iso(at),
                                    'email' if skew_idx % 4 == 0 else 'direct'))
            else:
                events.append(purchase(f'S{skew_idx:08d}', 'skew-user', _iso(at), 100))
            skew_idx += 1
        # many small users, one touch + one purchase each, never recurring.
        # 60s spacing keeps every small-user event before the +400 day cutoff
        # even with ~150k distinct users, so no event is accidentally future.
        for _ in range(small_pairs):
            at = base + timedelta(seconds=60 * small_uid)
            events.append(touch(f'T{small_uid:08d}', f'user-{small_uid:08d}',
                                _iso(at), 'search'))
            at2 = at + timedelta(seconds=60)
            events.append(purchase(f'Q{small_uid:08d}', f'user-{small_uid:08d}',
                                   _iso(at2), 50))
            small_uid += 1
        assert len(events) == target <= 10_000
        cutoff = base + timedelta(days=400)
        source = write_batch(workdir, f'scale-b{b+1:02d}.json',
                             batch(f'SCALE-{b+1:02d}', _iso(cutoff), events))
        assert source.stat().st_size <= 8_000_000
        start = time.monotonic()
        summary = pipeline.apply_batch(db, source)
        apply_seconds.append(round(time.monotonic() - start, 3))
        apply_rss.append(round(peak_rss_mib(), 2))
        assert summary['inserted_events'] == len(events)
        written += target

    conn = sqlite3.connect(db)
    counts = {name: conn.execute(f'SELECT COUNT(*) FROM {name}').fetchone()[0]
              for name in ('events', 'm_sessions', 'm_session_events', 'm_conversions')}
    inactive = conn.execute('SELECT COUNT(*) FROM events WHERE active=0').fetchone()[0]
    conn.close()
    return {
        'batch_count': batch_count,
        'events_per_batch': events_per_batch,
        'event_count': written,
        'inactive_events': inactive,
        'materialized': counts,
        'apply_peak_rss_mib': max(apply_rss),
        'apply_rss_per_batch_mib': apply_rss,
        'apply_seconds_per_batch': apply_seconds,
        'apply_total_seconds': round(sum(apply_seconds), 3),
        'passed': inactive == 0 and written == total_events,
    }


def scale_export_mode(workdir, *_):
    db = workdir / 'scale.db'
    export_path = workdir / 'scale-report.json'
    start = time.monotonic()
    pipeline.export_db(db, export_path)
    export_seconds = round(time.monotonic() - start, 3)
    # Peak RSS is captured immediately after export and BEFORE any large
    # read-back: this process only streamed rows through cursors, so this
    # number reflects export itself. Summary counts come from small SQL
    # aggregates; the full JSON is parsed in a separate verify process.
    rss = round(peak_rss_mib(), 2)
    conn = sqlite3.connect(db)
    summary = {
        'session_count': conn.execute('SELECT COUNT(*) FROM m_sessions').fetchone()[0],
        'conversion_count': conn.execute('SELECT COUNT(*) FROM m_conversions').fetchone()[0],
        'total_revenue_cents': conn.execute(
            'SELECT COALESCE(SUM(revenue_cents),0) FROM m_conversions').fetchone()[0],
        'duplicate_events': int(conn.execute(
            "SELECT value FROM meta WHERE key='duplicate_events'").fetchone()[0]),
        'inactive_events': conn.execute(
            'SELECT COUNT(*) FROM events WHERE active=0').fetchone()[0],
    }
    conn.close()
    return {
        'export_peak_rss_mib': rss,
        'export_seconds': export_seconds,
        'export_bytes': export_path.stat().st_size,
        'result_summary': summary,
        'passed': summary['inactive_events'] == 0,
    }


def verify_mode(workdir, *_):
    """Parse the large export in its own process (memory not budgeted) and
    compare every result field against the SQL materialization."""
    db = workdir / 'scale.db'
    export_path = workdir / 'scale-report.json'
    report = json.loads(export_path.read_text())
    conn = pipeline.connect(db)
    try:
        expected = pipeline.build_result(conn)
    finally:
        conn.close()
    return {
        'schema_version': report.get('schema_version'),
        'field_order': list(report['result'].keys()),
        'matches_materialization': report['result'] == expected,
        'top_level_keys': list(report.keys()),
        'passed': (report.get('schema_version') == pipeline.SCHEMA_VERSION and
                   report['result'] == expected),
    }


def main():
    mode, workdir = sys.argv[1], Path(sys.argv[2])
    args = sys.argv[3:]
    if mode == 'functional':
        payload = functional_mode(workdir, *args)
    elif mode == 'scale':
        payload = scale_mode(workdir, *args)
    elif mode == 'scale-export':
        payload = scale_export_mode(workdir, *args)
    elif mode == 'verify':
        payload = verify_mode(workdir, *args)
    else:
        raise SystemExit(f'unknown mode {mode}')
    print(json.dumps(payload, sort_keys=True))


if __name__ == '__main__':
    main()
