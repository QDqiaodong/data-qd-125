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
"""
import hashlib
import json
import math
import os
import re
import secrets
import sqlite3
import tempfile
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

# Test-only hook invoked inside the source snapshot transaction after the
# watermark read; lets tests interleave a concurrent source write.
_snapshot_hook = None

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


def _read_snapshot(source, request):
    """Consistent single-transaction read of watermark and clipped counts."""
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
            return {'as_of': as_of, 'counts': counts}
        except Exception:
            try:
                conn.execute('ROLLBACK')
            except sqlite3.Error:
                pass
            raise
    finally:
        conn.close()


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
    """Charge the ledger and publish one noisy day/channel count release."""
    if rng is None:
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
    conn = _open_ledger(ledger)
    try:
        row = conn.execute('SELECT spec_sha256, payload FROM releases WHERE release_id=?',
                           (request['release_id'],)).fetchone()
        if row is None:
            snapshot = _read_snapshot(source, request)
            candidate = _build_payload(request, snapshot, rng)
            as_of = snapshot['as_of']
            conn.execute('BEGIN IMMEDIATE')
            try:
                row = conn.execute(
                    'SELECT spec_sha256, payload FROM releases WHERE release_id=?',
                    (request['release_id'],)).fetchone()
                if row is None:
                    spent = int(_meta(conn, 'epsilon_spent_scaled'))
                    budget = int(_meta(conn, 'epsilon_budget_scaled'))
                    if spent + request['epsilon_scaled'] > budget:
                        raise PrivacyError('insufficient privacy budget: '
                                           'requested %s, %s of %s remaining'
                                           % (request['epsilon'],
                                              _canonical(budget - spent),
                                              _canonical(budget)))
                    conn.execute(
                        'INSERT INTO releases(release_id,spec_sha256,epsilon_scaled,payload) '
                        'VALUES(?,?,?,?)',
                        (request['release_id'], request['spec_sha256'],
                         str(request['epsilon_scaled']), candidate))
                    _set_meta(conn, 'epsilon_spent_scaled',
                              str(spent + request['epsilon_scaled']))
                    conn.execute('COMMIT')
                    payload = candidate
                else:
                    conn.execute('COMMIT')
            except Exception:
                try:
                    conn.execute('ROLLBACK')
                except sqlite3.Error:
                    pass
                raise
        if row is not None:
            if row[0] != request['spec_sha256']:
                raise PrivacyError('release_id already used with different parameters')
            payload = row[1]
            replayed = True
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
