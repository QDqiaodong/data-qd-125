"""Differentially private day/channel distinct-user releases.

``privacy-init`` creates a standalone SQLite ledger holding a fixed total
epsilon budget; ``privacy-release`` answers one request against the
incremental pipeline store: distinct users per (UTC day, public channel)
over a half-open date interval, with unknown channels folded into the
reserved ``other`` bucket and the full public domain zero-filled.

Privacy accounting is user-level under add/remove-whole-user adjacency:
each user contributes to at most ``L`` buckets (the first ``L`` non-empty
buckets in date/channel order), so the L1 sensitivity of the count vector
is ``L`` and every bucket receives independent Laplace(``L``/``epsilon``)
noise. Production noise draws come from ``secrets.SystemRandom``; tests
inject a deterministic source.

The ledger accumulates epsilon exactly (decimal strings scaled to
integers), refuses overspend, serializes concurrent writers with
``BEGIN IMMEDIATE``, and stores the released payload bytes in the same
transaction that charges the budget. Replaying a request with the same
``release_id`` and identical parameters returns the stored payload without
resampling noise or charging again; the same id with different parameters
is rejected. The output file is replaced atomically after the ledger
commit, so a post-commit write failure keeps the old file and a retry
republishes the stored payload for free.

The source store is opened read-only and read inside a single snapshot
transaction, so concurrent batch application cannot produce a mixed read
and the release path never modifies source tables or attribution rows.

Erasure coordination (generation fence): every committed source mutation
(``apply``/``erase``) advances a ``write_generation`` counter in the source
``meta`` table. A release records the generation of its snapshot and, while
holding a fresh read-only source transaction, re-reads that generation inside
the ledger write transaction. If an erasure (or any writer) committed in
between, the release aborts *before* charging or storing anything and the
caller retries against the new snapshot. The recheck read transaction stays
open until the ledger commits: a concurrent writer cannot reach its exclusive
commit lock while the shared lock is held, so the order is always a clean
"release before erasure" or "erasure before release" linearization, never a
stale charge. Writers that hit the lock wait on ``busy_timeout`` (bounded
blocking, retryable) rather than being refused outright.
"""
import hashlib
import json
import math
import os
import re
import secrets
import sqlite3
import tempfile
import time
import urllib.parse
from datetime import date, timedelta
from pathlib import Path

from . import common

SCHEMA_VERSION = 1
_EPSILON_SCALE = 10 ** 18
_EPSILON_RE = re.compile(r'(\d{1,12})(?:\.(\d{1,18}))?')
_DAY_RE = re.compile(r'\d{4}-\d{2}-\d{2}')
_MAX_DAYS = 366
_MAX_CHANNELS = 64
_MAX_L = 10 ** 6
_FAULT_ENV = 'VA_PRIVACY_FAULT_AFTER_COMMIT'
# Cross-process test barrier: when set to a directory, a release signals
# <dir>/snapshot-read after its source snapshot and waits for release-proceed;
# <dir>/fence-held is signaled while the generation recheck lock is open.
_SNAPSHOT_BARRIER_ENV = 'VA_PRIVACY_SNAPSHOT_BARRIER'
_FENCE_BARRIER_ENV = 'VA_ERASE_BARRIER'

# Test-only hook invoked inside the source snapshot transaction after the
# watermark read; lets tests interleave a concurrent source write.
_snapshot_hook = None
# Test-only hook invoked after the snapshot is built and before the ledger
# write transaction starts; lets tests commit an erasure in the fence window.
_commit_barrier = None


def _barrier_dir(env_name):
    path = os.environ.get(env_name)
    return Path(path) if path else None


def _barrier_signal(directory, name):
    if directory is not None:
        (directory / name).write_text('1')


def _barrier_wait(directory, name):
    if directory is None:
        return
    flag = directory / name
    deadline = time.monotonic() + 30
    while not flag.exists():
        if time.monotonic() > deadline:
            raise PrivacyError('test barrier timed out waiting for ' + name)
        time.sleep(0.02)

_DDL = (
    'CREATE TABLE meta('
    'key TEXT PRIMARY KEY,'
    'value TEXT NOT NULL'
    ') WITHOUT ROWID',
    'CREATE TABLE releases('
    'release_id TEXT PRIMARY KEY,'
    'spec_sha256 TEXT NOT NULL,'
    'epsilon_scaled TEXT NOT NULL,'
    'payload BLOB NOT NULL,'
    'source_generation INTEGER NOT NULL DEFAULT 0,'
    "created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))"
    ') WITHOUT ROWID',
)


class PrivacyError(ValueError):
    """All contract violations raised by the privacy ledger and release path."""


def _parse_decimal(value, name):
    """Positive finite decimal string -> exact scaled integer (1e-18 units)."""
    if not isinstance(value, str):
        raise PrivacyError(name + ' must be a decimal string')
    match = _EPSILON_RE.fullmatch(value)
    if match is None:
        raise PrivacyError(name + ' must be a positive finite decimal string '
                                 '(1-12 integer digits, optional 1-18 fraction digits)')
    scaled = int(match.group(1)) * _EPSILON_SCALE + int(match.group(2).ljust(18, '0')
                                                         if match.group(2) else 0)
    if scaled <= 0:
        raise PrivacyError(name + ' must be positive')
    return scaled


def _canonical(scaled):
    """Scaled integer -> canonical decimal string (no trailing zeros)."""
    whole, frac = divmod(scaled, _EPSILON_SCALE)
    if frac == 0:
        return str(whole)
    return ('%d.%018d' % (whole, frac)).rstrip('0')


def _day(value, name):
    if not isinstance(value, str) or not _DAY_RE.fullmatch(value):
        raise PrivacyError(name + ' must be an ISO calendar date YYYY-MM-DD')
    try:
        return date.fromisoformat(value)
    except ValueError:
        raise PrivacyError(name + ' is not a real calendar date') from None


def _load_request(path):
    data = common.parse_raw(common.read_raw(path))
    if not isinstance(data, dict):
        raise PrivacyError('request must be a JSON object')
    common.require(data, 'release_id start_date end_date channels L epsilon')
    release_id = common.ident(data['release_id'])
    start = _day(data['start_date'], 'start_date')
    end = _day(data['end_date'], 'end_date')
    if not start < end:
        raise PrivacyError('start_date must be before end_date (half-open interval)')
    if (end - start).days > _MAX_DAYS:
        raise PrivacyError('date interval exceeds %d days' % _MAX_DAYS)
    channels = common.rows(data['channels'], _MAX_CHANNELS)
    public = set()
    for channel in channels:
        common.ident(channel)
        if channel == 'other':
            raise PrivacyError("channel 'other' is reserved for the fallback bucket")
        if channel in public:
            raise PrivacyError('duplicate channel: ' + channel)
        public.add(channel)
    limit = common.number(data['L'], 1, _MAX_L)
    epsilon_scaled = _parse_decimal(data['epsilon'], 'epsilon')
    epsilon = _canonical(epsilon_scaled)
    days = []
    cursor = start
    while cursor < end:
        days.append(cursor.isoformat())
        cursor += timedelta(days=1)
    domain = sorted(public) + ['other']
    spec = {'release_id': release_id, 'start_date': start.isoformat(),
            'end_date': end.isoformat(), 'channels': sorted(public),
            'L': limit, 'epsilon': epsilon}
    digest = hashlib.sha256(
        json.dumps(spec, sort_keys=True, separators=(',', ':'),
                   ensure_ascii=False).encode('utf-8')).hexdigest()
    return {'release_id': release_id, 'start': start, 'end': end, 'days': days,
            'domain': domain, 'public': sorted(public), 'L': limit,
            'epsilon': epsilon, 'epsilon_scaled': epsilon_scaled,
            'spec_sha256': digest}


def _connect_readonly(path):
    if not Path(path).exists():
        raise PrivacyError('source database does not exist: ' + str(path))
    uri = 'file:' + urllib.parse.quote(str(Path(path).resolve()), safe='/') + '?mode=ro'
    conn = sqlite3.connect(uri, uri=True, isolation_level=None)
    conn.execute('PRAGMA busy_timeout=5000')
    return conn


def _read_generation(conn):
    row = conn.execute("SELECT value FROM meta WHERE key='write_generation'").fetchone()
    return 0 if row is None else int(row[0])


def _read_snapshot(source, request):
    """Consistent single-transaction read of watermark, generation and counts."""
    conn = _connect_readonly(source)
    try:
        conn.execute('BEGIN')
        try:
            tables = {row[0] for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'")}
            if not {'meta', 'events'} <= tables:
                raise PrivacyError('source database has no applied batches')
            row = conn.execute("SELECT value FROM meta WHERE key='as_of'").fetchone()
            if row is None:
                raise PrivacyError('source database has no applied batches')
            as_of = row[0]
            generation = _read_generation(conn)
            if _snapshot_hook is not None:
                _snapshot_hook()
            public = request['public']
            if public:
                placeholders = ','.join('?' * len(public))
                mapped = ('CASE WHEN channel IN (' + placeholders + ') '
                          'THEN channel ELSE \'other\' END')
                params = list(public)
            else:
                mapped = "'other'"
                params = []
            params += [request['start'].isoformat() + 'T00:00:00Z',
                       request['end'].isoformat() + 'T00:00:00Z', request['L']]
            rows = conn.execute(
                'WITH mapped AS ('
                'SELECT user, substr(at,1,10) AS day, ' + mapped + ' AS bucket '
                "FROM events WHERE active=1 AND kind='touch' AND at>=? AND at<?"
                '), present AS ('
                'SELECT DISTINCT user, day, bucket FROM mapped'
                '), ranked AS ('
                'SELECT day, bucket, row_number() OVER ('
                'PARTITION BY user ORDER BY day, bucket) AS rn FROM present'
                ') SELECT day, bucket, COUNT(*) FROM ranked WHERE rn<=? '
                'GROUP BY day, bucket', params).fetchall()
            counts = {(day, bucket): n for day, bucket, n in rows}
            conn.execute('COMMIT')
            barrier = _barrier_dir(_SNAPSHOT_BARRIER_ENV)
            if barrier is not None:
                # Pause with a finished snapshot but no ledger charge: lets an
                # external process commit an erasure inside the fence window.
                _barrier_signal(barrier, 'snapshot-read')
                _barrier_wait(barrier, 'release-proceed')
            return {'as_of': as_of, 'generation': generation, 'counts': counts}
        except Exception:
            try:
                conn.execute('ROLLBACK')
            except sqlite3.Error:
                pass
            raise
    finally:
        conn.close()


def _fence_generation(source, generation):
    """Re-read the source generation while holding a shared read transaction.

    The transaction is left open and returned to the caller; its shared lock
    blocks a concurrent writer's exclusive commit until the ledger transaction
    has finished, closing the check-then-act window. A stale generation raises
    before any ledger charge.
    """
    conn = _connect_readonly(source)
    conn.execute('BEGIN')
    current = _read_generation(conn)
    if current != generation:
        conn.execute('ROLLBACK')
        conn.close()
        raise PrivacyError(
            'source changed since snapshot (generation %d -> %d); '
            'release aborted without charging, retry the request'
            % (generation, current))
    fence_barrier = _barrier_dir(_FENCE_BARRIER_ENV)
    if fence_barrier is not None:
        # Hold the shared lock and let an external eraser reach (and block on)
        # the writer lock. The eraser's own blocked state is the synchronization
        # point; waiting for a file here would deadlock, so we only signal and
        # give the scheduler a short, bounded window.
        _barrier_signal(fence_barrier, 'fence-held')
        time.sleep(1.0)
    return conn


def _laplace(scale, rng):
    """One Laplace(0, scale) draw from a uniform [0,1) source."""
    u = rng() - 0.5
    if u == 0.0:
        return 0.0
    if u <= -0.5:  # rng()==0.0 has probability 2**-53; keep log1p finite
        u = -0.49999999999999994
    return scale * math.copysign(math.log1p(-2.0 * abs(u)), u)


def _build_payload(request, snapshot, rng):
    scale = request['L'] / float(request['epsilon'])
    counts = []
    true = snapshot['counts']
    for day in request['days']:
        for channel in request['domain']:
            noisy = true.get((day, channel), 0) + _laplace(scale, rng)
            counts.append({'date': day, 'channel': channel, 'noisy_count': noisy})
    report = {'schema_version': SCHEMA_VERSION,
              'release_id': request['release_id'],
              'start_date': request['start'].isoformat(),
              'end_date': request['end'].isoformat(),
              'channels': request['domain'],
              'L': request['L'],
              'epsilon': request['epsilon'],
              'noise_scale': scale,
              'source_as_of': snapshot['as_of'],
              'counts': counts}
    return (json.dumps(report, ensure_ascii=False, allow_nan=False, indent=2)
            + '\n').encode('utf-8')


def _open_ledger(path):
    if not Path(path).exists():
        raise PrivacyError('ledger is not initialized: ' + str(path))
    conn = sqlite3.connect(str(path), isolation_level=None)
    conn.execute('PRAGMA busy_timeout=5000')
    tables = {row[0] for row in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'")}
    if not {'meta', 'releases'} <= tables:
        conn.close()
        raise PrivacyError('ledger is not initialized: ' + str(path))
    # Additive migration: ledgers created before erasure support lack the
    # source-generation fence column. Existing rows are stamped generation 0
    # (the pre-fence era); their stored payloads and budget are untouched.
    columns = {row[1] for row in conn.execute('PRAGMA table_info(releases)')}
    if 'source_generation' not in columns:
        conn.execute('ALTER TABLE releases ADD COLUMN source_generation INTEGER NOT NULL DEFAULT 0')
    return conn


def _meta(conn, key):
    row = conn.execute('SELECT value FROM meta WHERE key=?', (key,)).fetchone()
    if row is None:
        raise PrivacyError('ledger metadata is incomplete')
    return row[0]


def _set_meta(conn, key, value):
    conn.execute('INSERT INTO meta(key,value) VALUES(?,?) '
                 'ON CONFLICT(key) DO UPDATE SET value=excluded.value', (key, value))


def init_ledger(path, budget_text):
    """Create a new ledger with a fixed total epsilon budget.

    Schema and metadata commit in one transaction; a failed init rolls back
    to zero tables, and a file with zero tables is treated as initializable,
    so retrying after an interrupted init always works.
    """
    budget = _parse_decimal(budget_text, 'budget')
    dest = Path(path)
    conn = sqlite3.connect(str(dest), isolation_level=None)
    try:
        conn.execute('PRAGMA busy_timeout=5000')
        conn.execute('BEGIN IMMEDIATE')
        try:
            if conn.execute('SELECT COUNT(*) FROM sqlite_master').fetchone()[0]:
                raise PrivacyError('ledger already initialized: ' + str(dest))
            for statement in _DDL:
                conn.execute(statement)
            _set_meta(conn, 'schema_version', SCHEMA_VERSION)
            _set_meta(conn, 'epsilon_budget_scaled', str(budget))
            _set_meta(conn, 'epsilon_spent_scaled', '0')
            conn.execute('COMMIT')
        except Exception:
            try:
                conn.execute('ROLLBACK')
            except sqlite3.Error:
                pass
            raise
    finally:
        conn.close()
    return {'ledger': str(dest), 'budget': _canonical(budget),
            'schema_version': SCHEMA_VERSION}


def release(source, ledger, request_path, output, rng=None):
    """Charge the ledger and publish one noisy day/channel count release.

    Fresh releases are fenced against source erasures: the snapshot's source
    generation is re-checked inside the ledger write transaction under a
    held read-only source lock, and a stale snapshot aborts without charging.
    Replays of an already stored release_id never touch the source.
    """
    if rng is None:
        if os.environ.get('VA_PRIVACY_RNG_ZERO'):
            # Test-only deterministic source: every Laplace draw is exactly 0.
            rng = lambda: 0.5
        else:
            rng = secrets.SystemRandom().random
    request = _load_request(request_path)
    dest = Path(output)
    if dest.is_symlink():
        raise PrivacyError('output cannot be a symlink')
    here = {Path(p).resolve() for p in (source, ledger, request_path)}
    if dest.resolve() in here:
        raise PrivacyError('output must differ from source, ledger and request')
    payload = None
    replayed = False
    as_of = None
    conn = _open_ledger(ledger)
    try:
        row = conn.execute(
            'SELECT spec_sha256, payload, source_generation FROM releases '
            'WHERE release_id=?', (request['release_id'],)).fetchone()
        if row is not None:
            if row[0] != request['spec_sha256']:
                raise PrivacyError('release_id already used with different parameters')
            payload = row[1]
            replayed = True
        else:
            snapshot = _read_snapshot(source, request)
            candidate = _build_payload(request, snapshot, rng)
            as_of = snapshot['as_of']
            if _commit_barrier is not None:
                _commit_barrier(snapshot['generation'])
            conn.execute('BEGIN IMMEDIATE')
            source_lock = None
            try:
                row = conn.execute(
                    'SELECT spec_sha256, payload, source_generation FROM releases '
                    'WHERE release_id=?',
                    (request['release_id'],)).fetchone()
                if row is None:
                    # Hold the shared source lock until the ledger commits so a
                    # concurrent erasure cannot land between check and commit.
                    source_lock = _fence_generation(source, snapshot['generation'])
                    spent = int(_meta(conn, 'epsilon_spent_scaled'))
                    budget = int(_meta(conn, 'epsilon_budget_scaled'))
                    if spent + request['epsilon_scaled'] > budget:
                        raise PrivacyError('insufficient privacy budget: '
                                           'requested %s, %s of %s remaining'
                                           % (request['epsilon'],
                                              _canonical(budget - spent),
                                              _canonical(budget)))
                    conn.execute(
                        'INSERT INTO releases(release_id,spec_sha256,epsilon_scaled,'
                        'payload,source_generation) VALUES(?,?,?,?,?)',
                        (request['release_id'], request['spec_sha256'],
                         str(request['epsilon_scaled']), candidate,
                         snapshot['generation']))
                    _set_meta(conn, 'epsilon_spent_scaled',
                              str(spent + request['epsilon_scaled']))
                    conn.execute('COMMIT')
                    payload = candidate
                else:
                    # A concurrent identical request won the ledger race: its
                    # stored payload is the idempotent answer for us too.
                    conn.execute('COMMIT')
                    if row[0] != request['spec_sha256']:
                        raise PrivacyError(
                            'release_id already used with different parameters')
                    payload = row[1]
                    replayed = True
            except BaseException:
                try:
                    conn.execute('ROLLBACK')
                except sqlite3.Error:
                    pass
                raise
            finally:
                if source_lock is not None:
                    source_lock.close()
        spent = int(_meta(conn, 'epsilon_spent_scaled'))
        budget = int(_meta(conn, 'epsilon_budget_scaled'))
    finally:
        conn.close()
    if os.environ.get(_FAULT_ENV):
        raise OSError('injected fault after ledger commit')
    dest.parent.mkdir(parents=True, exist_ok=True)
    temp = None
    try:
        with tempfile.NamedTemporaryFile(mode='wb', dir=dest.parent, delete=False) as stream:
            temp = stream.name
            stream.write(payload)
        os.replace(temp, dest)
        temp = None
    finally:
        if temp and os.path.exists(temp):
            os.unlink(temp)
    summary = {'release_id': request['release_id'],
               'replayed': replayed,
               'epsilon': request['epsilon'],
               'epsilon_spent': _canonical(spent),
               'budget': _canonical(budget),
               'buckets': len(request['days']) * len(request['domain']),
               'output_bytes': dest.stat().st_size}
    if not replayed:
        summary['source_as_of'] = as_of
    return summary
