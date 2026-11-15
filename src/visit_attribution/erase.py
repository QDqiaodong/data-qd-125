"""Right-to-erasure user erasure for the incremental source store.

``erase`` removes every row belonging to a set of users in one source
transaction: all normalized events (including inactive future events),
session/session-member materialization and conversions. The only retained
trace is a permanent tombstone holding the SHA256 of the user identifier's
UTF-8 bytes -- never the identifier itself -- plus an erasure request
credential. Batch credentials, the watermark and other users' rows are
untouched. A tombstone for a user that never existed is written too, so
unknown users are banned from future ingestion exactly like known ones.

Requests are idempotent: a request id carries a canonical spec hash over the
normalized (request id, sorted user-hash set); replaying the same request is
side-effect free, while the same id with any different parameter is refused.

Concurrency with ``apply`` and ``privacy-release`` is handled without
refusing parallel work outright:

* writers (apply/erase) serialize on short ``BEGIN IMMEDIATE`` transactions.
  Every committed writer advances a ``write_generation`` fence stored in
  ``meta``. Erase deletes are index range scans partitioned by user and
  require no cross-user recomputation, so the exclusive window stays short;
  contention waits on ``busy_timeout`` instead of failing immediately.
* privacy releases keep the source store read-only and fence their snapshot
  against the generation (see :mod:`visit_attribution.privacy`). A release
  whose snapshot predates an erasure commit is forced to abort/retry; it can
  neither charge nor publish from a stale snapshot.

Crash safety: every mutation is one source transaction, so a crash before
commit rolls back completely and a retry neither revives data nor performs a
half deletion. The ``VA_ERASE_FAULT_AFTER_COMMIT`` fault models a crash after
the source commit; on retry the request credential makes the run resolve as
an idempotent replay.
"""
import hashlib
import json
import os
from pathlib import Path

from . import common
from .pipeline import PipelineError, _bump_generation, _generation, connect

FAULT_AFTER_COMMIT = 'VA_ERASE_FAULT_AFTER_COMMIT'
_MAX_USERS = 10000
# SQLite parameter caps are far above this; chunking keeps one statement small.
_CHUNK = 500


def _user_hash(user):
    if not isinstance(user, str):
        raise PipelineError('users must be identifier strings')
    common.ident(user)
    return hashlib.sha256(user.encode('utf-8')).hexdigest()


def _load_request(path):
    """Parse and canonicalize an erase request file."""
    raw = common.read_raw(path)
    data = common.parse_raw(raw)
    if not isinstance(data, dict):
        raise PipelineError('erase request must be a JSON object')
    common.require(data, 'request_id users')
    request_id = common.ident(data['request_id'])
    users = common.rows(data['users'], _MAX_USERS)
    if not users:
        raise PipelineError('users must not be empty')
    hashes = sorted({_user_hash(user) for user in users})
    spec = {'request_id': request_id, 'user_sha256': hashes}
    digest = hashlib.sha256(
        json.dumps(spec, sort_keys=True, separators=(',', ':'),
                   ensure_ascii=False).encode('utf-8')).hexdigest()
    return {'request_id': request_id, 'hashes': hashes, 'spec_sha256': digest}


def _chunks(items):
    for start in range(0, len(items), _CHUNK):
        yield items[start:start + _CHUNK]


def erase(db_path, request_path):
    """Erase one request's user set from the source store, idempotently."""
    request = _load_request(request_path)
    conn = connect(Path(db_path))
    try:
        conn.execute('BEGIN IMMEDIATE')
        try:
            known = conn.execute(
                'SELECT spec_sha256 FROM erasures WHERE request_id=?',
                (request['request_id'],)).fetchone()
            if known is not None:
                if known[0] != request['spec_sha256']:
                    raise PipelineError(
                        'request_id already exists with different parameters; erase refused')
                conn.execute('COMMIT')
                return {'request_id': request['request_id'], 'replayed': True,
                        'erased_users': 0, 'unknown_users': 0,
                        'tombstones_written': 0,
                        'tombstones_total':
                        conn.execute('SELECT COUNT(*) FROM tombstones').fetchone()[0],
                        'generation': _generation(conn)}

            # A user already claimed by a *different* erasure request blocks the
            # whole batch; tombstones are permanent and map to one request.
            placeholders = ','.join('?' * len(request['hashes']))
            claimed = conn.execute(
                'SELECT COUNT(*) FROM tombstones WHERE user_sha256 IN (%s)'
                % placeholders, request['hashes']).fetchone()[0]
            if claimed:
                raise PipelineError(
                    '%d user(s) already erased by another request; batch refused' % claimed)

            # events.user stores the identifier, while the request speaks
            # hashes. Stream distinct identifiers once and keep only target
            # matches, so presence detection and DELETEs never scan event
            # history into Python and identifiers are never retained.
            targets = set(request['hashes'])
            hash_to_user = {}
            for (user,) in conn.execute('SELECT DISTINCT user FROM events'):
                digest = hashlib.sha256(user.encode('utf-8')).hexdigest()
                if digest in targets:
                    hash_to_user[digest] = user
            unknown = len(targets) - len(hash_to_user)

            for chunk in _chunks(request['hashes']):
                raw_users = [hash_to_user[h] for h in chunk if h in hash_to_user]
                if raw_users:
                    user_predicate = ','.join('?' * len(raw_users))
                    for table in ('events', 'm_sessions', 'm_session_events',
                                  'm_conversions'):
                        conn.execute('DELETE FROM %s WHERE user IN (%s)'
                                     % (table, user_predicate), raw_users)
                conn.executemany(
                    'INSERT INTO tombstones(user_sha256, request_id) VALUES(?,?)',
                    [(h, request['request_id']) for h in chunk])

            conn.execute(
                'INSERT INTO erasures(request_id,spec_sha256,user_count,unknown_users) '
                'VALUES(?,?,?,?)',
                (request['request_id'], request['spec_sha256'],
                 len(request['hashes']), unknown))
            _bump_generation(conn)
            generation = _generation(conn)
            total = conn.execute('SELECT COUNT(*) FROM tombstones').fetchone()[0]
            conn.execute('COMMIT')
        except Exception:
            try:
                conn.execute('ROLLBACK')
            except Exception:
                pass
            raise
    finally:
        conn.close()

    if os.environ.get(FAULT_AFTER_COMMIT):
        raise OSError('injected fault after erasure commit')

    return {'request_id': request['request_id'], 'replayed': False,
            'erased_users': len(request['hashes']) - unknown,
            'unknown_users': unknown,
            'tombstones_written': len(request['hashes']),
            'tombstones_total': total,
            'generation': generation}
