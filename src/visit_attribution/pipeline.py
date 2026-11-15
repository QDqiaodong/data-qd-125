"""SQLite incremental ingestion pipeline.

Each ``apply`` loads one batch file (the existing input contract plus a
top-level ``batch_id``) and commits events, batch credentials and the
refreshed per-user materialization in a single transaction. Recomputation is
restricted to users touched by newly active events; every scan is streamed
through SQL window queries so neither the whole database nor a whole user
history is held in Python memory.
"""
import hashlib
import json
import os
import sqlite3
import tempfile
from pathlib import Path

from . import common
from .events import instant, iso, parse as parse_events

SCHEMA_VERSION = 2
_CHUNK = 1000
_FAULT_ENV = 'VA_FAULT_AFTER_EVENTS'

_DDL = """
CREATE TABLE IF NOT EXISTS meta(
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS batches(
    batch_id TEXT PRIMARY KEY,
    source_sha256 TEXT NOT NULL,
    raw_length INTEGER NOT NULL,
    as_of TEXT NOT NULL,
    duplicate_events INTEGER NOT NULL,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS events(
    id TEXT PRIMARY KEY,
    user TEXT NOT NULL,
    at TEXT NOT NULL,
    kind TEXT NOT NULL,
    channel TEXT,
    amount_cents INTEGER,
    active INTEGER NOT NULL,
    first_batch_id TEXT NOT NULL
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS idx_events_user_active_at
    ON events(user, active, at, id);
CREATE TABLE IF NOT EXISTS m_sessions(
    user TEXT NOT NULL,
    seq INTEGER NOT NULL,
    session_id TEXT NOT NULL,
    start TEXT NOT NULL,
    end TEXT NOT NULL,
    purchases INTEGER NOT NULL,
    revenue_cents INTEGER NOT NULL,
    PRIMARY KEY(user, seq)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS m_session_events(
    user TEXT NOT NULL,
    session_seq INTEGER NOT NULL,
    event_seq INTEGER NOT NULL,
    event_id TEXT NOT NULL,
    PRIMARY KEY(user, session_seq, event_seq)
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS m_conversions(
    user TEXT NOT NULL,
    seq INTEGER NOT NULL,
    purchase_id TEXT NOT NULL,
    at TEXT NOT NULL,
    session_id TEXT NOT NULL,
    revenue_cents INTEGER NOT NULL,
    first_channel TEXT NOT NULL,
    first_touch_id TEXT,
    last_channel TEXT NOT NULL,
    last_touch_id TEXT,
    PRIMARY KEY(user, seq)
) WITHOUT ROWID;
"""


class PipelineError(ValueError):
    """All contract violations raised by the incremental pipeline."""


def connect(db_path):
    conn = sqlite3.connect(str(db_path), isolation_level=None)
    conn.execute('PRAGMA busy_timeout=5000')
    # Cap the page cache and disable memory-mapped I/O so large stores cannot
    # push peak RSS beyond the batch-parsing footprint; temporary result sets
    # spill to files under the temp directory.
    conn.execute('PRAGMA cache_size=-8000')
    conn.execute('PRAGMA mmap_size=0')
    conn.executescript(_DDL)
    return conn


def _meta(conn, key):
    row = conn.execute('SELECT value FROM meta WHERE key=?', (key,)).fetchone()
    return None if row is None else row[0]


def _set_meta(conn, key, value):
    conn.execute('INSERT INTO meta(key,value) VALUES(?,?) '
                 'ON CONFLICT(key) DO UPDATE SET value=excluded.value', (key, str(value)))


def _load_batch(source):
    raw = common.read_raw(source)
    data = common.parse_raw(raw)
    if not isinstance(data, dict):
        raise PipelineError('batch must be a JSON object')
    if 'batch_id' not in data:
        raise PipelineError('incremental batch requires top-level batch_id')
    batch_id = common.ident(data['batch_id'])
    body = {key: value for key, value in data.items() if key != 'batch_id'}
    cutoff, gap, lookback, events, duplicates = parse_events(body)
    return {
        'batch_id': batch_id,
        'raw': raw,
        'sha256': hashlib.sha256(raw).hexdigest(),
        'cutoff': cutoff,
        'gap': gap,
        'lookback': lookback,
        'events': events,
        'duplicates': duplicates,
    }


def _recompute_user(conn, user, gap, lookback):
    """Replace one user's materialization using set-based SQL.

    All heavy work (gap sessionization via window functions, inclusive-window
    touch attribution via indexed range probes) runs inside SQLite; temporary
    tables are materialized on disk. Python only drives the statements, so the
    per-user history length never appears as a Python collection.
    """
    conn.execute('DELETE FROM m_sessions WHERE user=?', (user,))
    conn.execute('DELETE FROM m_session_events WHERE user=?', (user,))
    conn.execute('DELETE FROM m_conversions WHERE user=?', (user,))

    conn.execute('CREATE TEMP TABLE IF NOT EXISTS r_events('
                 'seq INTEGER PRIMARY KEY, id TEXT, at TEXT, kind TEXT, '
                 'channel TEXT, amount INTEGER, new_session INTEGER, session_seq INTEGER)')
    conn.execute('DELETE FROM r_events')
    # Convert ISO-8601 UTC timestamps to integer epoch seconds via string
    # slicing (strptime would be per-row Python); this avoids julianday float
    # error at exact gap boundaries such as exactly 1800 seconds.
    conn.execute(
        'INSERT INTO r_events(seq,id,at,kind,channel,amount,new_session,session_seq) '
        'WITH ordered AS ('
        "SELECT row_number() OVER (ORDER BY at,id) AS seq, id, at, kind, channel, amount_cents, "
        "strftime('%s', at) AS epoch, lag(strftime('%s', at)) OVER (ORDER BY at,id) AS prev_epoch "
        'FROM events WHERE user=? AND active=1'
        ') SELECT seq,id,at,kind,channel,amount_cents,'
        'CASE WHEN prev_epoch IS NULL OR cast(epoch AS INTEGER)-cast(prev_epoch AS INTEGER) > ? '
        'THEN 1 ELSE 0 END,'
        'sum(CASE WHEN prev_epoch IS NULL OR cast(epoch AS INTEGER)-cast(prev_epoch AS INTEGER) > ? '
        'THEN 1 ELSE 0 END) OVER (ORDER BY seq) '
        'FROM ordered ORDER BY seq', (user, gap, gap))
    if conn.execute('SELECT count(*) FROM r_events').fetchone()[0] == 0:
        return
    # Temp-table indexes make the lookback range probes touch only the
    # purchase window instead of scanning the whole user history. The channel
    # predicate is applied as a residual so one (kind,at,id) index serves both
    # non-direct and direct probes; (id) resolves the channel lookups.
    conn.execute('CREATE INDEX IF NOT EXISTS idx_r_kind_at ON r_events(kind,at,id)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_r_id ON r_events(id)')
    conn.execute('ANALYZE r_events')

    conn.execute(
        'INSERT INTO m_sessions(user,seq,session_id,start,end,purchases,revenue_cents) '
        'SELECT ?, session_seq, min(id), min(at), max(at), '
        "sum(CASE WHEN kind='purchase' THEN 1 ELSE 0 END), "
        "coalesce(sum(CASE WHEN kind='purchase' THEN amount ELSE 0 END),0) "
        'FROM r_events GROUP BY session_seq', (user,))
    conn.execute(
        'INSERT INTO m_session_events(user,session_seq,event_seq,event_id) '
        'SELECT ?, session_seq, row_number() OVER (PARTITION BY session_seq ORDER BY seq), id '
        'FROM r_events', (user,))

    # Attributed touch windows: [purchase_at - lookback, purchase_at], inclusive.
    # ISO-8601 UTC strings compare chronologically, matching events.instant().
    # lookback is a validated integer in 1..31 days, so inlining it is safe.
    # Each slot is an indexed LIMIT-1 probe scoped to the lookback window, so
    # its cost depends on window density, not on the user's history length;
    # COALESCE evaluates the direct fallback lazily only when no non-direct
    # touch exists in the window.
    start = "strftime('%Y-%m-%dT%H:%M:%SZ',p.at,'-" + str(int(lookback)) + " seconds')"

    def probe(channel_filter, direction):
        order = 't.at ASC,t.id ASC' if direction == 'first' else 't.at DESC,t.id DESC'
        return (f"(SELECT t.id FROM r_events t WHERE t.kind='touch' AND {channel_filter} "
                f"AND t.at BETWEEN {start} AND p.at ORDER BY {order} LIMIT 1)")

    nd_filter = "t.channel!='direct'"
    d_filter = "t.channel='direct'"
    first_nd_id = probe(nd_filter, 'first')
    last_nd_id = probe(nd_filter, 'last')
    first_d_id = probe(d_filter, 'first')
    last_d_id = probe(d_filter, 'last')
    channel_of = ('(SELECT c.channel FROM r_events c WHERE c.id='
                  'coalesce({nd},{d}))')
    conn.execute(
        'INSERT INTO m_conversions(user,seq,purchase_id,at,session_id,revenue_cents,'
        'first_channel,first_touch_id,last_channel,last_touch_id) '
        'WITH p AS (SELECT seq,id,at,amount,session_seq FROM r_events WHERE kind=?) '
        'SELECT ?, row_number() OVER (ORDER BY p.seq), p.id, p.at, s.session_id, p.amount, '
        f"coalesce({channel_of.format(nd=first_nd_id, d=first_d_id)},'unattributed'), "
        f'coalesce({first_nd_id},{first_d_id}), '
        f"coalesce({channel_of.format(nd=last_nd_id, d=last_d_id)},'unattributed'), "
        f'coalesce({last_nd_id},{last_d_id}) '
        'FROM p JOIN m_sessions s ON s.user=? AND s.seq=p.session_seq ORDER BY p.seq',
        ('purchase', user, user))


def apply_batch(db_path, source):
    batch = _load_batch(source)
    conn = connect(db_path)
    try:
        conn.execute('BEGIN IMMEDIATE')
        known = conn.execute('SELECT source_sha256, raw_length FROM batches WHERE batch_id=?',
                             (batch['batch_id'],)).fetchone()
        if known is not None:
            if known[0] != batch['sha256'] or known[1] != len(batch['raw']):
                raise PipelineError('batch_id already exists with different source bytes; replay refused')
            conn.execute('COMMIT')
            return {'batch_id': batch['batch_id'], 'replayed': True, 'inserted_events': 0,
                    'activated_events': 0, 'recomputed_users': 0, 'as_of': _meta(conn, 'as_of')}

        gap = _meta(conn, 'session_gap_seconds')
        if gap is None:
            _set_meta(conn, 'schema_version', SCHEMA_VERSION)
            _set_meta(conn, 'session_gap_seconds', batch['gap'])
            _set_meta(conn, 'lookback_seconds', batch['lookback'])
            _set_meta(conn, 'as_of', iso(batch['cutoff']))
            _set_meta(conn, 'duplicate_events', 0)
        else:
            if int(gap) != batch['gap'] or int(_meta(conn, 'lookback_seconds')) != batch['lookback']:
                raise PipelineError('gap/lookback are fixed by the first batch and cannot change')
            previous_cutoff = instant(_meta(conn, 'as_of'))
            if batch['cutoff'] < previous_cutoff:
                raise PipelineError('as_of cannot move backwards')

        cutoff_iso = iso(batch['cutoff'])
        new_events = []
        inserted = 0
        affected = set()
        for event in batch['events']:
            existing = conn.execute(
                'SELECT user,at,kind,channel,amount_cents FROM events WHERE id=?',
                (event['id'],)).fetchone()
            normalized = (event['user'], iso(event['at']), event['kind'],
                          event['channel'], event['amount_cents'])
            if existing is not None:
                if existing != normalized:
                    raise PipelineError('conflicting event ID: ' + event['id'])
                continue
            new_events.append((event['id'],) + normalized +
                              (1 if event['at'] <= batch['cutoff'] else 0, batch['batch_id']))
            affected.add(event['user'])
            if len(new_events) >= _CHUNK:
                conn.executemany(
                    'INSERT INTO events(id,user,at,kind,channel,amount_cents,active,first_batch_id) '
                    'VALUES(?,?,?,?,?,?,?,?)', new_events)
                inserted += len(new_events)
                new_events.clear()
                fault = os.environ.get(_FAULT_ENV)
                if fault and inserted >= int(fault):
                    raise RuntimeError('injected fault after %d inserted events' % inserted)
        if new_events:
            conn.executemany(
                'INSERT INTO events(id,user,at,kind,channel,amount_cents,active,first_batch_id) '
                'VALUES(?,?,?,?,?,?,?,?)', new_events)
            inserted += len(new_events)
            fault = os.environ.get(_FAULT_ENV)
            if fault and inserted >= int(fault):
                raise RuntimeError('injected fault after %d inserted events' % inserted)

        activated_users = {row[0] for row in conn.execute(
            'SELECT DISTINCT user FROM events WHERE active=0 AND at<=?', (cutoff_iso,))}
        activated = conn.execute('UPDATE events SET active=1 WHERE active=0 AND at<=?',
                                 (cutoff_iso,)).rowcount
        affected |= activated_users

        _set_meta(conn, 'as_of', cutoff_iso)
        _set_meta(conn, 'duplicate_events',
                  int(_meta(conn, 'duplicate_events')) + batch['duplicates'])
        conn.execute(
            'INSERT INTO batches(batch_id,source_sha256,raw_length,as_of,duplicate_events) '
            'VALUES(?,?,?,?,?)',
            (batch['batch_id'], batch['sha256'], len(batch['raw']),
             cutoff_iso, batch['duplicates']))

        for user in sorted(affected):
            _recompute_user(conn, user, batch['gap'], batch['lookback'])
        conn.execute('COMMIT')
        return {'batch_id': batch['batch_id'], 'replayed': False,
                'inserted_events': inserted, 'activated_events': activated,
                'recomputed_users': len(affected),
                'duplicate_events': batch['duplicates'], 'as_of': cutoff_iso}
    except Exception:
        conn.execute('ROLLBACK')
        raise
    finally:
        conn.close()


class _JsonWriter:
    """Streaming indent-2 JSON writer that preserves insertion order."""

    def __init__(self, stream):
        self.f = stream
        self.stack = []

    def _sep(self):
        if self.stack[-1]:
            self.f.write(',')
        self.f.write('\n' + ' ' * (2 * len(self.stack)))
        self.stack[-1] += 1

    def begin_object(self):
        self.f.write('{')
        self.stack.append(0)

    def begin_root_object(self):
        self.begin_object()

    def key(self, name):
        self._sep()
        self.f.write(json.dumps(name, ensure_ascii=False))
        self.f.write(': ')

    def scalar(self, value):
        json.dump(value, self.f, ensure_ascii=False, allow_nan=False)

    def begin_object_value(self):
        self.begin_object()

    def begin_array_value(self):
        self.f.write('[')
        self.stack.append(0)

    def array_scalar(self, value):
        self._sep()
        self.scalar(value)

    def array_begin_object(self):
        self._sep()
        self.begin_object()

    def end_object(self):
        count = self.stack.pop()
        if count:
            self.f.write('\n' + ' ' * (2 * len(self.stack)))
        self.f.write('}')

    def end_array(self):
        count = self.stack.pop()
        if count:
            self.f.write('\n' + ' ' * (2 * len(self.stack)))
        self.f.write(']')

    def finish(self):
        self.f.write('\n')


def _channel_totals(conn):
    totals = {'first_non_direct': {}, 'last_non_direct': {}}
    for model, column in (('first_non_direct', 'first_channel'),
                          ('last_non_direct', 'last_channel')):
        rows = conn.execute(
            f'SELECT {column}, COUNT(*), SUM(revenue_cents) FROM m_conversions '
            f'GROUP BY {column} ORDER BY {column}')
        for channel, conversions, revenue in rows:
            totals[model][channel] = {'conversions': conversions,
                                      'revenue_cents': revenue}
    return totals


def _sessions_payload(conn):
    sessions = []
    users = [row[0] for row in conn.execute(
        'SELECT DISTINCT user FROM m_sessions ORDER BY user')]
    for user in users:
        event_rows = conn.execute(
            'SELECT session_seq,event_id FROM m_session_events '
            'WHERE user=? ORDER BY session_seq,event_seq', (user,))
        events_by_session = {}
        for session_seq, event_id in event_rows:
            events_by_session.setdefault(session_seq, []).append(event_id)
        for seq, session_id, start, end, purchases, revenue in conn.execute(
                'SELECT seq,session_id,start,end,purchases,revenue_cents '
                'FROM m_sessions WHERE user=? ORDER BY seq', (user,)):
            sessions.append({'id': session_id, 'user': user, 'start': start, 'end': end,
                             'event_ids': events_by_session.get(seq, []),
                             'purchases': purchases, 'revenue_cents': revenue})
    return sessions


def _conversions_payload(conn):
    conversions = []
    users = [row[0] for row in conn.execute(
        'SELECT DISTINCT user FROM m_conversions ORDER BY user')]
    for user in users:
        for (purchase_id, at_iso, session_id, revenue,
             first_channel, first_touch, last_channel, last_touch) in conn.execute(
                'SELECT purchase_id,at,session_id,revenue_cents,'
                'first_channel,first_touch_id,last_channel,last_touch_id '
                'FROM m_conversions WHERE user=? ORDER BY seq', (user,)):
            conversions.append({
                'id': purchase_id, 'user': user, 'at': at_iso,
                'session_id': session_id, 'revenue_cents': revenue,
                'models': {
                    'first_non_direct': {'channel': first_channel, 'touch_id': first_touch},
                    'last_non_direct': {'channel': last_channel, 'touch_id': last_touch}}})
    return conversions


def build_result(conn):
    """Materialized result, field-for-field equivalent to model.analyze(...)."""
    as_of = _meta(conn, 'as_of')
    if as_of is None:
        raise PipelineError('database has no applied batches')
    duplicates = int(_meta(conn, 'duplicate_events'))
    sessions = _sessions_payload(conn)
    conversions = _conversions_payload(conn)
    future_ids = [row[0] for row in conn.execute(
        'SELECT id FROM events WHERE active=0 ORDER BY id')]
    return {
        'as_of': as_of,
        'duplicate_events': duplicates,
        'future_event_ids': future_ids,
        'sessions': sessions,
        'conversions': conversions,
        'session_count': len(sessions),
        'conversion_count': len(conversions),
        'total_revenue_cents': sum(c['revenue_cents'] for c in conversions),
        'channel_totals': _channel_totals(conn),
    }


def export_db(db_path, output):
    dest = Path(output)
    if dest.is_symlink():
        raise PipelineError('output cannot be a symlink')
    if Path(db_path).resolve() == dest.resolve():
        raise PipelineError('database and output must differ')
    conn = connect(db_path)
    temp_name = None
    try:
        as_of = _meta(conn, 'as_of')
        if as_of is None:
            raise PipelineError('database has no applied batches')
        duplicates = int(_meta(conn, 'duplicate_events'))
        dest.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(mode='w', encoding='utf-8',
                                         dir=dest.parent, delete=False) as stream:
            temp_name = stream.name
            w = _JsonWriter(stream)
            w.begin_root_object()
            w.key('schema_version')
            w.scalar(SCHEMA_VERSION)
            w.key('result')
            w.begin_object_value()

            w.key('as_of')
            w.scalar(as_of)
            w.key('duplicate_events')
            w.scalar(duplicates)
            w.key('future_event_ids')
            w.begin_array_value()
            future_cursor = conn.execute('SELECT id FROM events WHERE active=0 ORDER BY id')
            for (event_id,) in future_cursor:
                w.array_scalar(event_id)
            w.end_array()

            w.key('sessions')
            w.begin_array_value()
            # Stream users and nested sessions; no user list or per-user event
            # dictionary is ever materialized in Python.
            user_cursor = conn.execute(
                'SELECT DISTINCT user FROM m_sessions ORDER BY user')
            for (user,) in user_cursor:
                sessions = conn.execute(
                    'SELECT seq,session_id,start,end,purchases,revenue_cents '
                    'FROM m_sessions WHERE user=? ORDER BY seq', (user,))
                for seq, session_id, start, end, purchases, revenue in sessions:
                    w.array_begin_object()
                    w.key('id')
                    w.scalar(session_id)
                    w.key('user')
                    w.scalar(user)
                    w.key('start')
                    w.scalar(start)
                    w.key('end')
                    w.scalar(end)
                    w.key('event_ids')
                    w.begin_array_value()
                    event_cursor = conn.execute(
                        'SELECT event_id FROM m_session_events '
                        'WHERE user=? AND session_seq=? ORDER BY event_seq', (user, seq))
                    for (event_id,) in event_cursor:
                        w.array_scalar(event_id)
                    w.end_array()
                    w.key('purchases')
                    w.scalar(purchases)
                    w.key('revenue_cents')
                    w.scalar(revenue)
                    w.end_object()
            w.end_array()

            w.key('conversions')
            w.begin_array_value()
            conv_user_cursor = conn.execute(
                'SELECT DISTINCT user FROM m_conversions ORDER BY user')
            for (user,) in conv_user_cursor:
                conv_cursor = conn.execute(
                    'SELECT purchase_id,at,session_id,revenue_cents,'
                    'first_channel,first_touch_id,last_channel,last_touch_id '
                    'FROM m_conversions WHERE user=? ORDER BY seq', (user,))
                for (purchase_id, at_iso, session_id, revenue,
                     first_channel, first_touch, last_channel, last_touch) in conv_cursor:
                    w.array_begin_object()
                    w.key('id')
                    w.scalar(purchase_id)
                    w.key('user')
                    w.scalar(user)
                    w.key('at')
                    w.scalar(at_iso)
                    w.key('session_id')
                    w.scalar(session_id)
                    w.key('revenue_cents')
                    w.scalar(revenue)
                    w.key('models')
                    w.begin_object_value()
                    for model, channel, touch in (
                            ('first_non_direct', first_channel, first_touch),
                            ('last_non_direct', last_channel, last_touch)):
                        w.key(model)
                        w.begin_object_value()
                        w.key('channel')
                        w.scalar(channel)
                        w.key('touch_id')
                        w.scalar(touch)
                        w.end_object()
                    w.end_object()
                    w.end_object()
            w.end_array()

            session_count = conn.execute('SELECT COUNT(*) FROM m_sessions').fetchone()[0]
            conversion_count = conn.execute('SELECT COUNT(*) FROM m_conversions').fetchone()[0]
            total_revenue = conn.execute(
                'SELECT COALESCE(SUM(revenue_cents),0) FROM m_conversions').fetchone()[0]
            w.key('session_count')
            w.scalar(session_count)
            w.key('conversion_count')
            w.scalar(conversion_count)
            w.key('total_revenue_cents')
            w.scalar(total_revenue)

            w.key('channel_totals')
            w.begin_object_value()
            for model in ('first_non_direct', 'last_non_direct'):
                column = 'first_channel' if model == 'first_non_direct' else 'last_channel'
                w.key(model)
                w.begin_object_value()
                grouped = conn.execute(
                    f'SELECT {column},COUNT(*),SUM(revenue_cents) FROM m_conversions '
                    f'GROUP BY {column} ORDER BY {column}')
                for channel, conversions_count, revenue in grouped:
                    w.key(channel)
                    w.begin_object_value()
                    w.key('conversions')
                    w.scalar(conversions_count)
                    w.key('revenue_cents')
                    w.scalar(revenue)
                    w.end_object()
                w.end_object()
            w.end_object()

            w.end_object()  # result
            w.end_object()  # root
            w.finish()
        os.replace(temp_name, dest)
        temp_name = None
    finally:
        conn.close()
        if temp_name and os.path.exists(temp_name):
            os.unlink(temp_name)
    return {
        'as_of': as_of,
        'duplicate_events': duplicates,
        'session_count': session_count,
        'conversion_count': conversion_count,
        'total_revenue_cents': total_revenue,
        'output_bytes': dest.stat().st_size,
    }
