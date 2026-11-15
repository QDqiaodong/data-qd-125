"""Offline channel commission ledger and payout reservations.

The commission ledger is a standalone SQLite file.  It never writes to the
incremental access store: ``commission-sync`` opens that store read-only and
reads ``meta.as_of``, ``meta.write_generation`` and ``m_conversions`` from one
committed snapshot.  No external payment is executed; ``payout-confirm`` only
turns an existing reservation into a permanent internal paid record.

Rights are keyed by the globally unique ``purchase_id``.  Only the already
materialized ``last_channel`` and ``revenue_cents`` are used; attribution is
never recomputed.  Commission is ``revenue_cents * rate_bps // 10000`` using
Python integers (no floating point), where each channel rate is fixed at
``commission-init`` time.  Unconfigured channels, ``direct`` and
``unattributed`` have a zero rate.

The ledger stores no user, touch or session identifiers.  Entries are
append-only signed amounts: every rights change is posted as a reversal of the
old right followed by a grant of the new right.  Rights whose commission is
zero are tracked in ``rights`` but produce no zero accounting entry, so every
stored entry is strictly positive or negative.
"""
import hashlib
import json
import os
import sqlite3
import time
import urllib.parse
from pathlib import Path

from . import common
from .events import instant

SCHEMA_VERSION = 1
_BPS_DENOMINATOR = 10000
_MAX_CHANNELS = 10000
_MAX_AMOUNT = 10 ** 9
_ZERO_RATE_CHANNELS = frozenset(('direct', 'unattributed'))
_FAULT_ENV = 'VA_COMMISSION_FAULT_AFTER_COMMIT'
_SNAPSHOT_BARRIER_ENV = 'VA_COMMISSION_SNAPSHOT_BARRIER'
_TX_BARRIER_ENV = 'VA_COMMISSION_TX_BARRIER'
_BARRIER_TIMEOUT = 30.0

# Test-only hook invoked inside the source snapshot read transaction after the
# watermark and conversion rows are read; lets tests interleave a concurrent
# uncommitted source write and prove the snapshot stays consistent.
_snapshot_hook = None

_TABLES = ('meta', 'rates', 'source_state', 'syncs', 'rights', 'entries',
           'plans', 'plan_allocations', 'payouts', 'payout_allocations')

_TABLE_DDL = (
    'CREATE TABLE meta('
    'key TEXT PRIMARY KEY,'
    'value TEXT NOT NULL'
    ') WITHOUT ROWID',
    'CREATE TABLE rates('
    'channel TEXT PRIMARY KEY,'
    'bps INTEGER NOT NULL CHECK(bps BETWEEN 0 AND 10000)'
    ') WITHOUT ROWID',
    'CREATE TRIGGER rates_no_update BEFORE UPDATE ON rates BEGIN '
    "SELECT RAISE(ABORT,'commission rates are immutable'); END",
    'CREATE TRIGGER rates_no_delete BEFORE DELETE ON rates BEGIN '
    "SELECT RAISE(ABORT,'commission rates are immutable'); END",
    'CREATE TRIGGER rates_no_insert BEFORE INSERT ON rates '
    "WHEN EXISTS(SELECT 1 FROM meta WHERE key='rates_finalized') "
    "BEGIN SELECT RAISE(ABORT,'commission rates are immutable'); END",
    'CREATE TABLE source_state('
    'id INTEGER PRIMARY KEY CHECK(id=1),'
    'source_path TEXT NOT NULL,'
    'as_of TEXT NOT NULL,'
    'generation INTEGER NOT NULL,'
    'content_sha256 TEXT NOT NULL,'
    'updated_at TEXT NOT NULL DEFAULT (strftime(\'%Y-%m-%dT%H:%M:%SZ\',\'now\'))'
    ')',
    'CREATE TABLE syncs('
    'sync_id TEXT PRIMARY KEY,'
    'spec_sha256 TEXT NOT NULL,'
    'as_of TEXT NOT NULL,'
    'generation INTEGER NOT NULL,'
    'content_sha256 TEXT NOT NULL,'
    'source_path TEXT NOT NULL,'
    'grants INTEGER NOT NULL,'
    'reversals INTEGER NOT NULL,'
    'new_rights INTEGER NOT NULL,'
    'changed_rights INTEGER NOT NULL,'
    'removed_rights INTEGER NOT NULL,'
    'expired_plans INTEGER NOT NULL,'
    "created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))"
    ') WITHOUT ROWID',
    'CREATE TABLE rights('
    'purchase_id TEXT PRIMARY KEY,'
    'channel TEXT NOT NULL,'
    'revenue_cents INTEGER NOT NULL,'
    'commission_cents INTEGER NOT NULL,'
    'sync_id TEXT NOT NULL'
    ') WITHOUT ROWID',
    'CREATE TABLE entries('
    # Monotonic sequence assigned inside the ledger write transaction: it
    # preserves posting order without AUTOINCREMENT (whose sqlite_sequence
    # side table would escape the plain table set).
    'seq INTEGER PRIMARY KEY,'
    'purchase_id TEXT NOT NULL,'
    'channel TEXT NOT NULL,'
    'amount_cents INTEGER NOT NULL CHECK(amount_cents != 0),'
    "kind TEXT NOT NULL CHECK(kind IN ('grant','reversal')),"
    'sync_id TEXT NOT NULL,'
    "created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))"
    ')',
    'CREATE INDEX entries_channel ON entries(channel)',
    'CREATE TRIGGER entries_no_update BEFORE UPDATE ON entries BEGIN '
    "SELECT RAISE(ABORT,'commission entries are immutable'); END",
    'CREATE TRIGGER entries_no_delete BEFORE DELETE ON entries BEGIN '
    "SELECT RAISE(ABORT,'commission entries are immutable'); END",
    'CREATE TABLE plans('
    'plan_id TEXT PRIMARY KEY,'
    'spec_sha256 TEXT NOT NULL,'
    "status TEXT NOT NULL CHECK(status IN ('active','confirmed','cancelled','expired')),"
    'total_cents INTEGER NOT NULL,'
    'resolution TEXT,'
    "created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),"
    'resolved_at TEXT'
    ') WITHOUT ROWID',
    'CREATE TABLE plan_allocations('
    'plan_id TEXT NOT NULL,'
    'channel TEXT NOT NULL,'
    'amount_cents INTEGER NOT NULL CHECK(amount_cents > 0),'
    'PRIMARY KEY(plan_id, channel)'
    ') WITHOUT ROWID',
    'CREATE TABLE payouts('
    'plan_id TEXT PRIMARY KEY,'
    'total_cents INTEGER NOT NULL,'
    "confirmed_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))"
    ') WITHOUT ROWID',
    'CREATE TRIGGER payouts_no_update BEFORE UPDATE ON payouts BEGIN '
    "SELECT RAISE(ABORT,'paid records are permanent'); END",
    'CREATE TRIGGER payouts_no_delete BEFORE DELETE ON payouts BEGIN '
    "SELECT RAISE(ABORT,'paid records are permanent'); END",
    'CREATE TABLE payout_allocations('
    'plan_id TEXT NOT NULL,'
    'channel TEXT NOT NULL,'
    'amount_cents INTEGER NOT NULL CHECK(amount_cents > 0),'
    'PRIMARY KEY(plan_id, channel)'
    ') WITHOUT ROWID',
    'CREATE TRIGGER payout_allocations_no_update BEFORE UPDATE ON payout_allocations '
    "BEGIN SELECT RAISE(ABORT,'paid records are permanent'); END",
    'CREATE TRIGGER payout_allocations_no_delete BEFORE DELETE ON payout_allocations '
    "BEGIN SELECT RAISE(ABORT,'paid records are permanent'); END",
)


class CommissionError(ValueError):
    """All contract violations raised by commission and payout commands."""


# ---------------------------------------------------------------------------
# Small helpers


def _canonical_json(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'),
                      ensure_ascii=False)


def _digest(value):
    return hashlib.sha256(_canonical_json(value).encode('utf-8')).hexdigest()


def _positive_amount(value, name):
    if type(value) is not int or not 1 <= value <= _MAX_AMOUNT:
        raise CommissionError(name + ' must be a positive integer cents amount')
    return value


def _open(path):
    conn = sqlite3.connect(str(path), isolation_level=None)
    conn.execute('PRAGMA busy_timeout=5000')
    return conn


def _open_existing(path):
    if not Path(path).exists():
        raise CommissionError('ledger is not initialized: ' + str(path))
    conn = _open(path)
    tables = {row[0] for row in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'")}
    if not set(_TABLES) <= tables:
        conn.close()
        raise CommissionError('ledger is not initialized: ' + str(path))
    return conn


def _meta(conn, key):
    row = conn.execute('SELECT value FROM meta WHERE key=?', (key,)).fetchone()
    return None if row is None else row[0]


def _set_meta(conn, key, value):
    conn.execute('INSERT INTO meta(key,value) VALUES(?,?) '
                 'ON CONFLICT(key) DO UPDATE SET value=excluded.value',
                 (key, str(value)))


def _rates(conn):
    return {channel: bps for channel, bps in
            conn.execute('SELECT channel,bps FROM rates ORDER BY channel')}


def _rate_for(rates, channel):
    if channel in _ZERO_RATE_CHANNELS:
        return 0
    return rates.get(channel, 0)


def _commission(revenue_cents, rate_bps):
    # Integer arithmetic only: truncation toward zero for non-negative values.
    return revenue_cents * rate_bps // _BPS_DENOMINATOR


def _barrier_dir(env_name):
    value = os.environ.get(env_name)
    return Path(value) if value else None


def _barrier_signal(directory, name):
    if directory is not None:
        (directory / name).write_text('1')


def _barrier_wait(directory, name):
    if directory is None:
        return
    flag = directory / name
    deadline = time.monotonic() + _BARRIER_TIMEOUT
    while not flag.exists():
        if time.monotonic() > deadline:
            raise CommissionError('test barrier timed out waiting for ' + name)
        time.sleep(0.02)


def _in_transaction(conn, action):
    conn.execute('BEGIN IMMEDIATE')
    try:
        result = action()
        conn.execute('COMMIT')
        return result
    except BaseException:
        try:
            conn.execute('ROLLBACK')
        except sqlite3.Error:
            pass
        raise


def _load_rates_file(path):
    raw = common.read_raw(path)
    data = common.parse_raw(raw)
    if not isinstance(data, dict):
        raise CommissionError('rates request must be a JSON object')
    common.require(data, 'rates')
    rates = data['rates']
    if not isinstance(rates, dict):
        raise CommissionError('rates must be an object mapping channel to basis points')
    if len(rates) > _MAX_CHANNELS:
        raise CommissionError('too many configured channels')
    cleaned = {}
    for channel, bps in rates.items():
        common.ident(channel)
        if type(bps) is not int or not 0 <= bps <= 10000:
            raise CommissionError('rate for %s must be an integer in 0..10000' % channel)
        if channel in _ZERO_RATE_CHANNELS:
            # Reserved channels are pinned at zero; any other rate is refused.
            if bps != 0:
                raise CommissionError(
                    "channel '%s' is reserved and its rate is fixed at zero" % channel)
        cleaned[channel] = bps
    return {'rates': cleaned,
            'spec_sha256': _digest({'rates': {k: cleaned[k] for k in sorted(cleaned)}})}


# ---------------------------------------------------------------------------
# Initialization


def init_ledger(path, rates_path):
    """Create the standalone ledger with an immutable channel rate table."""
    request = _load_rates_file(rates_path)
    dest = Path(path)
    conn = _open(dest)
    try:
        _in_transaction(conn, lambda: _init_schema(conn, dest, request))
    finally:
        conn.close()
    return {'ledger': str(dest), 'schema_version': SCHEMA_VERSION,
            'channels_configured': len(request['rates']),
            'rates': request['rates']}


def _init_schema(conn, dest, request):
    if conn.execute('SELECT COUNT(*) FROM sqlite_master').fetchone()[0]:
        raise CommissionError('ledger already initialized: ' + str(dest))
    for statement in _TABLE_DDL:
        conn.execute(statement)
    _set_meta(conn, 'schema_version', SCHEMA_VERSION)
    _set_meta(conn, 'rates_sha256', request['spec_sha256'])
    conn.executemany('INSERT INTO rates(channel,bps) VALUES(?,?)',
                     sorted(request['rates'].items()))
    # Freeze the mapping: later INSERT/UPDATE/DELETE on rates abort (triggers).
    _set_meta(conn, 'rates_finalized', 1)


# ---------------------------------------------------------------------------
# Source snapshot (read-only, single committed read transaction)


def _snapshot_barrier():
    directory = _barrier_dir(_SNAPSHOT_BARRIER_ENV)
    if directory is not None:
        _barrier_signal(directory, 'snapshot-read')
        _barrier_wait(directory, 'sync-proceed')


def _read_source_snapshot(source):
    path = Path(source)
    if not path.exists():
        raise CommissionError('source database does not exist: ' + str(path))
    resolved = str(path.resolve())
    uri = 'file:' + urllib.parse.quote(resolved, safe='/') + '?mode=ro'
    conn = sqlite3.connect(uri, uri=True, isolation_level=None)
    conn.execute('PRAGMA busy_timeout=5000')
    try:
        conn.execute('BEGIN')
        try:
            tables = {row[0] for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'")}
            if not {'meta', 'm_conversions'} <= tables:
                raise CommissionError('source database has no applied batches')
            row = conn.execute("SELECT value FROM meta WHERE key='as_of'").fetchone()
            if row is None:
                raise CommissionError('source database has no applied batches')
            as_of = row[0]
            instant(as_of)  # watermark must be a strict ISO-8601 UTC instant
            generation_row = conn.execute(
                "SELECT value FROM meta WHERE key='write_generation'").fetchone()
            generation = 0 if generation_row is None else int(generation_row[0])
            if generation < 0:
                raise CommissionError('source snapshot has a negative write_generation')
            purchases = []
            seen = None
            for purchase_id, channel, revenue in conn.execute(
                    'SELECT purchase_id,last_channel,revenue_cents '
                    'FROM m_conversions ORDER BY purchase_id'):
                if seen is not None and purchase_id == seen:
                    raise CommissionError(
                        'source snapshot contains duplicate purchase_id: ' + purchase_id)
                seen = purchase_id
                if not isinstance(purchase_id, str) or not purchase_id:
                    raise CommissionError('source snapshot has an invalid purchase_id')
                if not isinstance(channel, str) or not channel:
                    raise CommissionError(
                        'source snapshot has an invalid last_channel for ' + purchase_id)
                if type(revenue) is not int or not 0 <= revenue <= _MAX_AMOUNT:
                    raise CommissionError(
                        'source snapshot has an invalid revenue_cents for ' + purchase_id)
                purchases.append([purchase_id, channel, revenue])
            content_sha256 = _digest({'purchases': purchases})
            if _snapshot_hook is not None:
                _snapshot_hook()
            conn.execute('COMMIT')
        except BaseException:
            try:
                conn.execute('ROLLBACK')
            except sqlite3.Error:
                pass
            raise
    finally:
        conn.close()
    _snapshot_barrier()
    return {'source_path': resolved, 'as_of': as_of, 'generation': generation,
            'content_sha256': content_sha256, 'purchases': purchases}


# ---------------------------------------------------------------------------
# commission-sync


def _sync_spec(sync_id, snapshot):
    return _digest({'sync_id': sync_id, 'source_path': snapshot['source_path'],
                    'as_of': snapshot['as_of'], 'generation': snapshot['generation'],
                    'content_sha256': snapshot['content_sha256']})


def _check_source_state(conn, snapshot):
    state = conn.execute(
        'SELECT source_path, as_of, generation, content_sha256 FROM source_state '
        'WHERE id=1').fetchone()
    if state is None:
        return
    path, as_of, generation, content_sha256 = state
    if path != snapshot['source_path']:
        raise CommissionError(
            'ledger is bound to source %s; sync from %s refused'
            % (path, snapshot['source_path']))
    if instant(snapshot['as_of']) < instant(as_of):
        raise CommissionError(
            'source watermark moved backwards (as_of %s -> %s); sync refused'
            % (as_of, snapshot['as_of']))
    if snapshot['generation'] < generation:
        raise CommissionError(
            'source generation moved backwards (%d -> %d); sync refused'
            % (generation, snapshot['generation']))
    if (snapshot['generation'] == generation
            and snapshot['content_sha256'] != content_sha256):
        raise CommissionError(
            'source content changed without a new generation (%d); sync refused'
            % generation)


def _rights_snapshot(conn):
    return {purchase_id: (channel, revenue, commission) for
            (purchase_id, channel, revenue, commission) in conn.execute(
                'SELECT purchase_id,channel,revenue_cents,commission_cents FROM rights')}


def _channel_totals(conn, table='entries', column='amount_cents',
                    where='', group='channel'):
    totals = {}
    for channel, total in conn.execute(
            'SELECT %s, COALESCE(SUM(%s),0) FROM %s %s GROUP BY %s'
            % (group, column, table, where, group)):
        totals[channel] = total
    return totals


def sync(source, ledger, sync_id):
    """Apply one committed source snapshot to the commission ledger."""
    sync_id = common.ident(sync_id)
    # Validate the ledger first so an uninitialized ledger is reported as such
    # regardless of the source path; the read-only snapshot then runs without
    # any ledger transaction open.
    conn = _open_existing(ledger)
    try:
        snapshot = _read_source_snapshot(source)
        spec_sha256 = _sync_spec(sync_id, snapshot)
        barrier = _barrier_dir(_TX_BARRIER_ENV)
        summary = _in_transaction(
            conn, lambda: _sync_transaction(conn, snapshot, sync_id, spec_sha256, barrier))
    finally:
        conn.close()
    if os.environ.get(_FAULT_ENV):
        raise OSError('injected fault after commission sync commit')
    return summary


def _sync_transaction(conn, snapshot, sync_id, spec_sha256, barrier):
    known = conn.execute(
        'SELECT spec_sha256, as_of, generation, content_sha256, grants, reversals,'
        ' new_rights, changed_rights, removed_rights, expired_plans '
        'FROM syncs WHERE sync_id=?', (sync_id,)).fetchone()
    if known is not None:
        if known[0] != spec_sha256:
            raise CommissionError(
                'sync_id already used with a different source snapshot; sync refused')
        return {'sync_id': sync_id, 'replayed': True, 'as_of': known[1],
                'generation': known[2], 'content_sha256': known[3],
                'entries': known[4] + known[5], 'grants': known[4],
                'reversals': known[5], 'new_rights': known[6],
                'changed_rights': known[7], 'removed_rights': known[8],
                'expired_plans': known[9]}

    _check_source_state(conn, snapshot)
    rates = _rates(conn)
    old_rights = _rights_snapshot(conn)
    before = _channel_totals(conn, 'rights', 'commission_cents')

    desired = {}
    for purchase_id, channel, revenue in snapshot['purchases']:
        commission = _commission(revenue, _rate_for(rates, channel))
        desired[purchase_id] = (channel, revenue, commission)

    next_seq = conn.execute(
        'SELECT COALESCE(MAX(seq),0)+1 FROM entries').fetchone()[0]
    grants = reversals = new_rights = changed_rights = removed_rights = 0

    def post(purchase_id, channel, amount, kind):
        nonlocal next_seq
        if amount == 0:
            return
        conn.execute('INSERT INTO entries(seq,purchase_id,channel,amount_cents,kind,'
                     'sync_id) VALUES(?,?,?,?,?,?)',
                     (next_seq, purchase_id, channel, amount, kind, sync_id))
        next_seq += 1

    for purchase_id, new in desired.items():
        old = old_rights.get(purchase_id)
        if old is None:
            new_rights += 1
            if new[2] != 0:
                post(purchase_id, new[0], new[2], 'grant')
                grants += 1
            conn.execute(
                'INSERT INTO rights(purchase_id,channel,revenue_cents,'
                'commission_cents,sync_id) VALUES(?,?,?,?,?)',
                (purchase_id, new[0], new[1], new[2], sync_id))
        elif old != new:
            changed_rights += 1
            if old[2] != 0:
                post(purchase_id, old[0], -old[2], 'reversal')
                reversals += 1
            if new[2] != 0:
                post(purchase_id, new[0], new[2], 'grant')
                grants += 1
            conn.execute(
                'UPDATE rights SET channel=?,revenue_cents=?,commission_cents=?,'
                'sync_id=? WHERE purchase_id=?',
                (new[0], new[1], new[2], sync_id, purchase_id))
    for purchase_id, old in old_rights.items():
        if purchase_id in desired:
            continue
        removed_rights += 1
        if old[2] != 0:
            post(purchase_id, old[0], -old[2], 'reversal')
            reversals += 1
        conn.execute('DELETE FROM rights WHERE purchase_id=?', (purchase_id,))

    after = _channel_totals(conn, 'rights', 'commission_cents')
    reduced = {channel for channel, total in before.items()
               if after.get(channel, 0) < total}
    expired_plans = _expire_plans(conn, reduced)

    conn.execute(
        'INSERT INTO syncs(sync_id,spec_sha256,as_of,generation,content_sha256,'
        'source_path,grants,reversals,new_rights,changed_rights,removed_rights,'
        'expired_plans) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)',
        (sync_id, spec_sha256, snapshot['as_of'], snapshot['generation'],
         snapshot['content_sha256'], snapshot['source_path'], grants, reversals,
         new_rights, changed_rights, removed_rights, expired_plans))
    _set_meta(conn, 'last_sync_id', sync_id)
    conn.execute(
        'INSERT INTO source_state(id,source_path,as_of,generation,content_sha256) '
        'VALUES(1,?,?,?,?) ON CONFLICT(id) DO UPDATE SET '
        'source_path=excluded.source_path, as_of=excluded.as_of, '
        'generation=excluded.generation, content_sha256=excluded.content_sha256',
        (snapshot['source_path'], snapshot['as_of'], snapshot['generation'],
         snapshot['content_sha256']))
    if barrier is not None:
        # Test-only pause inside the ledger write transaction so a SIGKILL can
        # prove the whole sync (entries, rights, expirations, credential, state)
        # is one atomic unit.
        _barrier_signal(barrier, 'sync-tx-applied')
        _barrier_wait(barrier, 'sync-tx-go')
    return {'sync_id': sync_id, 'replayed': False, 'as_of': snapshot['as_of'],
            'generation': snapshot['generation'],
            'content_sha256': snapshot['content_sha256'],
            'entries': grants + reversals, 'grants': grants,
            'reversals': reversals, 'new_rights': new_rights,
            'changed_rights': changed_rights, 'removed_rights': removed_rights,
            'expired_plans': expired_plans}


def _expire_plans(conn, reduced_channels):
    if not reduced_channels:
        return 0
    placeholders = ','.join('?' * len(reduced_channels))
    plan_ids = [row[0] for row in conn.execute(
        'SELECT DISTINCT p.plan_id FROM plans p JOIN plan_allocations a '
        "ON a.plan_id=p.plan_id WHERE p.status='active' AND a.channel IN (%s) "
        'ORDER BY p.plan_id' % placeholders, sorted(reduced_channels))]
    if not plan_ids:
        return 0
    conn.executemany("UPDATE plans SET status='expired', resolution='sync_invalidated',"
                     " resolved_at=strftime('%Y-%m-%dT%H:%M:%SZ','now') WHERE plan_id=?",
                     [(plan_id,) for plan_id in plan_ids])
    return len(plan_ids)


# ---------------------------------------------------------------------------
# Payout plans (offline reservations; no external payment)


def _load_plan_file(path):
    raw = common.read_raw(path)
    data = common.parse_raw(raw)
    if not isinstance(data, dict):
        raise CommissionError('payout plan request must be a JSON object')
    common.require(data, 'plan_id amounts')
    plan_id = common.ident(data['plan_id'])
    amounts = data['amounts']
    if not isinstance(amounts, dict) or not amounts:
        raise CommissionError('amounts must be a non-empty channel to cents object')
    if len(amounts) > _MAX_CHANNELS:
        raise CommissionError('too many channels in one payout plan')
    cleaned = {}
    for channel, amount in amounts.items():
        common.ident(channel)
        _positive_amount(amount, 'amount for ' + channel)
        cleaned[channel] = amount
    ordered = {channel: cleaned[channel] for channel in sorted(cleaned)}
    total = sum(ordered.values())
    if total > _MAX_AMOUNT:
        raise CommissionError('payout plan total is too large')
    spec = _digest({'plan_id': plan_id, 'amounts': ordered})
    return {'plan_id': plan_id, 'amounts': ordered, 'total_cents': total,
            'spec_sha256': spec}


def _ledger_balances(conn):
    accrued = _channel_totals(conn, 'entries', 'amount_cents')
    paid = _channel_totals(conn, 'payout_allocations', 'amount_cents')
    reserved = _channel_totals(
        conn, 'plan_allocations a JOIN plans p ON p.plan_id=a.plan_id',
        'a.amount_cents', "WHERE p.status='active'", group='a.channel')
    return accrued, paid, reserved


def _available(net, other_reserved):
    return max(0, net - other_reserved)


def _plan_payload(conn, plan_id):
    row = conn.execute(
        'SELECT plan_id,status,total_cents,resolution,created_at,resolved_at '
        'FROM plans WHERE plan_id=?', (plan_id,)).fetchone()
    if row is None:
        raise CommissionError('unknown payout plan: ' + plan_id)
    allocations = [{'channel': channel, 'amount_cents': amount}
                   for channel, amount in conn.execute(
                       'SELECT channel,amount_cents FROM plan_allocations '
                       'WHERE plan_id=? ORDER BY channel', (plan_id,))]
    return {'plan_id': row[0], 'status': row[1], 'total_cents': row[2],
            'resolution': row[3], 'created_at': row[4], 'resolved_at': row[5],
            'allocations': allocations}


def plan_payout(ledger, request_path):
    """Atomically reserve commission balances for every channel of a plan."""
    request = _load_plan_file(request_path)
    conn = _open_existing(ledger)
    try:
        return _in_transaction(conn, lambda: _plan_transaction(conn, request))
    finally:
        conn.close()


def _plan_transaction(conn, request):
    known = conn.execute('SELECT spec_sha256 FROM plans WHERE plan_id=?',
                         (request['plan_id'],)).fetchone()
    if known is not None:
        if known[0] != request['spec_sha256']:
            raise CommissionError(
                'plan_id already used with different allocations; plan refused')
        payload = _plan_payload(conn, request['plan_id'])
        return {'replayed': True, 'plan': payload}
    if _meta(conn, 'last_sync_id') is None:
        raise CommissionError(
            'ledger has no successful sync; payout plans require settled rights')

    accrued, paid, reserved = _ledger_balances(conn)
    shortfalls = []
    for channel, amount in request['amounts'].items():
        net = accrued.get(channel, 0) - paid.get(channel, 0)
        available = _available(net, reserved.get(channel, 0))
        if available < amount:
            shortfalls.append({'channel': channel, 'requested_cents': amount,
                               'available_cents': available})
    if shortfalls:
        raise CommissionError(
            'insufficient available commission for channels: '
            + ', '.join(s['channel'] for s in shortfalls)
            + ' (no partial reservation was created)')
    conn.execute(
        "INSERT INTO plans(plan_id,spec_sha256,status,total_cents) "
        "VALUES(?,?,'active',?)",
        (request['plan_id'], request['spec_sha256'], request['total_cents']))
    conn.executemany(
        'INSERT INTO plan_allocations(plan_id,channel,amount_cents) VALUES(?,?,?)',
        [(request['plan_id'], channel, amount)
         for channel, amount in request['amounts'].items()])
    return {'replayed': False, 'plan': _plan_payload(conn, request['plan_id'])}


def _confirm_barrier():
    directory = _barrier_dir(_TX_BARRIER_ENV)
    if directory is not None:
        _barrier_signal(directory, 'confirm-tx-applied')
        _barrier_wait(directory, 'confirm-tx-go')


def confirm_payout(ledger, plan_id):
    """Turn one plan's active reservation into a permanent paid record."""
    plan_id = common.ident(plan_id)
    conn = _open_existing(ledger)
    try:
        return _in_transaction(conn, lambda: _confirm_transaction(conn, plan_id))
    finally:
        conn.close()


def _confirm_transaction(conn, plan_id):
    row = conn.execute('SELECT status FROM plans WHERE plan_id=?',
                       (plan_id,)).fetchone()
    if row is None:
        raise CommissionError('unknown payout plan: ' + plan_id)
    status = row[0]
    if status == 'confirmed':
        payload = _plan_payload(conn, plan_id)
        return {'replayed': True, 'plan': payload,
                'payout': {'plan_id': plan_id, 'total_cents': payload['total_cents']}}
    if status != 'active':
        raise CommissionError(
            'plan %s is %s and cannot be confirmed' % (plan_id, status))
    allocations = [(channel, amount) for channel, amount in conn.execute(
        'SELECT channel,amount_cents FROM plan_allocations '
        'WHERE plan_id=? ORDER BY channel', (plan_id,))]
    accrued, paid = _channel_totals(conn, 'entries', 'amount_cents'), \
        _channel_totals(conn, 'payout_allocations', 'amount_cents')
    # Confirm re-checks availability without the plan's own active reservation
    # (it is being converted, not reserved twice). A concurrent sync cannot
    # interleave: both paths hold the ledger's write transaction.
    for channel, amount in allocations:
        net = accrued.get(channel, 0) - paid.get(channel, 0)
        other = conn.execute(
            'SELECT COALESCE(SUM(a.amount_cents),0) FROM plan_allocations a '
            "JOIN plans p ON p.plan_id=a.plan_id WHERE p.status='active' "
            'AND a.channel=? AND a.plan_id!=?', (channel, plan_id)).fetchone()[0]
        if _available(net, other) < amount:
            raise CommissionError(
                'plan %s no longer has sufficient available balance for channel %s'
                % (plan_id, channel))
    conn.execute(
        "UPDATE plans SET status='confirmed', resolution='confirmed', "
        "resolved_at=strftime('%Y-%m-%dT%H:%M:%SZ','now') WHERE plan_id=?",
        (plan_id,))
    conn.execute('INSERT INTO payouts(plan_id,total_cents) VALUES(?,?)',
                 (plan_id, sum(amount for _, amount in allocations)))
    conn.executemany(
        'INSERT INTO payout_allocations(plan_id,channel,amount_cents) VALUES(?,?,?)',
        [(plan_id, channel, amount) for channel, amount in allocations])
    _confirm_barrier()
    payload = _plan_payload(conn, plan_id)
    return {'replayed': False, 'plan': payload,
            'payout': {'plan_id': plan_id,
                       'total_cents': sum(amount for _, amount in allocations)}}


def cancel_payout(ledger, plan_id):
    """Release one active plan's reservation; confirmed payouts cannot cancel."""
    plan_id = common.ident(plan_id)
    conn = _open_existing(ledger)
    try:
        return _in_transaction(conn, lambda: _cancel_transaction(conn, plan_id))
    finally:
        conn.close()


def _cancel_transaction(conn, plan_id):
    row = conn.execute('SELECT status FROM plans WHERE plan_id=?',
                       (plan_id,)).fetchone()
    if row is None:
        raise CommissionError('unknown payout plan: ' + plan_id)
    status = row[0]
    if status == 'cancelled':
        return {'replayed': True, 'plan': _plan_payload(conn, plan_id)}
    if status == 'confirmed':
        raise CommissionError('plan %s is confirmed and cannot be cancelled' % plan_id)
    if status == 'expired':
        # The sync already released this reservation; nothing left to release.
        return {'replayed': True, 'plan': _plan_payload(conn, plan_id)}
    conn.execute(
        "UPDATE plans SET status='cancelled', resolution='cancelled', "
        "resolved_at=strftime('%Y-%m-%dT%H:%M:%SZ','now') WHERE plan_id=?",
        (plan_id,))
    return {'replayed': False, 'plan': _plan_payload(conn, plan_id)}


# ---------------------------------------------------------------------------
# commission-report


def _plan_status_rows(conn):
    plans = []
    for plan_id, status, total, resolution, created_at, resolved_at in conn.execute(
            'SELECT plan_id,status,total_cents,resolution,created_at,resolved_at '
            'FROM plans ORDER BY plan_id'):
        allocations = [{'channel': channel, 'amount_cents': amount}
                       for channel, amount in conn.execute(
                           'SELECT channel,amount_cents FROM plan_allocations '
                           'WHERE plan_id=? ORDER BY channel', (plan_id,))]
        plans.append({'plan_id': plan_id, 'status': status,
                      'total_cents': total, 'resolution': resolution,
                      'created_at': created_at, 'resolved_at': resolved_at,
                      'allocations': allocations})
    return plans


def build_report(conn):
    """Per-channel accrued, paid, reserved, available, debt and plans."""
    accrued = _channel_totals(conn, 'entries', 'amount_cents')
    paid = _channel_totals(conn, 'payout_allocations', 'amount_cents')
    reserved = _channel_totals(
        conn, 'plan_allocations a JOIN plans p ON p.plan_id=a.plan_id',
        'a.amount_cents', "WHERE p.status='active'", group='a.channel')
    rates = _rates(conn)
    # Report every channel with a configured rate, an accounting entry, a paid
    # allocation, an active reservation or a current right (even a zero one).
    channels = set(rates)
    channels.update(accrued)
    channels.update(paid)
    channels.update(reserved)
    channels.update(row[0] for row in conn.execute(
        'SELECT DISTINCT channel FROM rights'))
    channels.update(row[0] for row in conn.execute(
        'SELECT DISTINCT channel FROM plan_allocations'))
    rows = []
    for channel in sorted(channels):
        channel_accrued = accrued.get(channel, 0)
        channel_paid = paid.get(channel, 0)
        channel_reserved = reserved.get(channel, 0)
        net = channel_accrued - channel_paid
        channel_plans = [
            {'plan_id': plan_id, 'status': status, 'amount_cents': amount}
            for plan_id, status, amount in conn.execute(
                'SELECT a.plan_id,p.status,a.amount_cents FROM plan_allocations a '
                'JOIN plans p ON p.plan_id=a.plan_id WHERE a.channel=? '
                'ORDER BY a.plan_id', (channel,))]
        rows.append({
            'channel': channel,
            'rate_bps': _rate_for(rates, channel),
            'accrued_cents': channel_accrued,
            'net_cents': net,
            'paid_cents': channel_paid,
            'reserved_cents': channel_reserved,
            'available_cents': _available(net, channel_reserved),
            'debt_cents': max(0, -net),
            'plans': channel_plans,
        })
    plans = _plan_status_rows(conn)
    rights_total = conn.execute(
        'SELECT COALESCE(SUM(commission_cents),0) FROM rights').fetchone()[0]
    entries_total = conn.execute(
        'SELECT COALESCE(SUM(amount_cents),0) FROM entries').fetchone()[0]
    confirmed_plan_total = conn.execute(
        "SELECT COALESCE(SUM(total_cents),0) FROM plans WHERE status='confirmed'"
    ).fetchone()[0]
    payout_total = conn.execute(
        'SELECT COALESCE(SUM(amount_cents),0) FROM payout_allocations').fetchone()[0]
    active_reserved_total = conn.execute(
        'SELECT COALESCE(SUM(a.amount_cents),0) FROM plan_allocations a '
        "JOIN plans p ON p.plan_id=a.plan_id WHERE p.status='active'"
    ).fetchone()[0]
    plan_allocation_total = conn.execute(
        'SELECT COALESCE(SUM(amount_cents),0) FROM plan_allocations').fetchone()[0]
    paid_by_channel = _channel_totals(conn, 'payout_allocations', 'amount_cents')
    confirmed_by_channel = _channel_totals(
        conn, 'plan_allocations a JOIN plans p ON p.plan_id=a.plan_id',
        'a.amount_cents', "WHERE p.status='confirmed'", group='a.channel')
    state = conn.execute(
        'SELECT source_path, as_of, generation, content_sha256 FROM source_state '
        'WHERE id=1').fetchone()
    last_sync_id = _meta(conn, 'last_sync_id')
    last_sync = None if last_sync_id is None else conn.execute(
        'SELECT sync_id, as_of, generation FROM syncs WHERE sync_id=?',
        (last_sync_id,)).fetchone()
    rights_by_channel = _channel_totals(conn, 'rights', 'commission_cents')
    channel_set = set(rights_by_channel) | set(accrued)
    per_channel_ok = all(rights_by_channel.get(channel, 0) == accrued.get(channel, 0)
                         for channel in channel_set)
    checks = {
        'entries_match_current_rights': per_channel_ok and entries_total == rights_total,
        'payouts_match_confirmed_plans': payout_total == confirmed_plan_total,
        'plan_allocations_conserved': plan_allocation_total == sum(
            plan['total_cents'] for plan in plans),
        'payout_allocations_match_confirmed_plans': all(
            paid_by_channel.get(channel, 0) == confirmed_by_channel.get(channel, 0)
            for channel in set(paid_by_channel) | set(confirmed_by_channel)),
        'no_channel_over_reserved': all(
            row['reserved_cents'] <= max(
                0, row['accrued_cents'] - row['paid_cents'])
            for row in rows),
        'available_matches_net_minus_reserved': all(
            row['available_cents'] == max(
                0, row['accrued_cents'] - row['paid_cents'] - row['reserved_cents'])
            for row in rows),
    }
    report = {
        'schema_version': SCHEMA_VERSION,
        'rates_sha256': _meta(conn, 'rates_sha256'),
        'source': None if state is None else {
            'path': state[0], 'as_of': state[1], 'generation': state[2],
            'content_sha256': state[3]},
        'last_sync': None if last_sync is None else {
            'sync_id': last_sync[0], 'as_of': last_sync[1],
            'generation': last_sync[2]},
        'channels': rows,
        'plans': plans,
        'totals': {
            'accrued_cents': entries_total,
            'rights_cents': rights_total,
            'paid_cents': payout_total,
            'reserved_cents': active_reserved_total,
            'available_cents': sum(row['available_cents'] for row in rows),
            'debt_cents': sum(row['debt_cents'] for row in rows),
            'entries': conn.execute('SELECT COUNT(*) FROM entries').fetchone()[0],
            'rights': conn.execute('SELECT COUNT(*) FROM rights').fetchone()[0],
        },
        'checks': checks,
        'conservation_ok': all(checks.values()),
    }
    return report


def report(ledger, output):
    dest = Path(output)
    if dest.is_symlink():
        raise CommissionError('output cannot be a symlink')
    if Path(ledger).resolve() == dest.resolve():
        raise CommissionError('ledger and report output must differ')
    conn = _open_existing(ledger)
    try:
        # Single read transaction: one consistent view of rights, entries,
        # plans and payouts while concurrent syncs/confirmations commit.
        conn.execute('BEGIN')
        try:
            payload = build_report(conn)
            conn.execute('COMMIT')
        except BaseException:
            try:
                conn.execute('ROLLBACK')
            except sqlite3.Error:
                pass
            raise
    finally:
        conn.close()
    common.save(dest, payload)
    return {'output': str(dest), 'conservation_ok': payload['conservation_ok'],
            'channels': len(payload['channels']), 'plans': len(payload['plans']),
            'output_bytes': dest.stat().st_size}
