"""Additive schema in the existing gateway Store; no snapshot rollback."""

SCHEMA = """
CREATE TABLE IF NOT EXISTS h1_inbox(
 item_id TEXT PRIMARY KEY, version INTEGER NOT NULL, policy_json TEXT NOT NULL,
 state TEXT NOT NULL, token TEXT NOT NULL DEFAULT '', lease_until REAL,
 accepted INTEGER NOT NULL, created REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS h1_inbox_pending ON h1_inbox(state,created);
CREATE TABLE IF NOT EXISTS h1_tasks(
 task_id TEXT PRIMARY KEY, item_id TEXT UNIQUE NOT NULL, revision INTEGER NOT NULL,
 source_version INTEGER NOT NULL DEFAULT 1,
 request_json TEXT NOT NULL, policy_json TEXT NOT NULL, route_json TEXT NOT NULL,
 intent TEXT NOT NULL, route_state TEXT NOT NULL, dispatch_state TEXT NOT NULL,
 execution_state TEXT NOT NULL, delivery_state TEXT NOT NULL, verification_state TEXT NOT NULL,
 business_id TEXT UNIQUE, fingerprint TEXT, scope TEXT NOT NULL, generation INTEGER NOT NULL DEFAULT 0,
 attempt TEXT NOT NULL DEFAULT '', run_id TEXT NOT NULL DEFAULT '', context_json TEXT,
 result_json TEXT, deadline REAL NOT NULL, execution_deadline REAL NOT NULL,
 lease_until REAL, stamps TEXT NOT NULL, last_error TEXT NOT NULL DEFAULT '', created REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS h1_task_scope ON h1_tasks(scope,dispatch_state);
CREATE TABLE IF NOT EXISTS h1_dispatch(
 task_id TEXT PRIMARY KEY, state TEXT NOT NULL, attempt TEXT NOT NULL DEFAULT '',
 FOREIGN KEY(task_id) REFERENCES h1_tasks(task_id)
);
CREATE TABLE IF NOT EXISTS h1_sessions(
 scope TEXT PRIMARY KEY, generation INTEGER NOT NULL, task_id TEXT, state TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS h1_receipts(
 event_id TEXT PRIMARY KEY, task_id TEXT NOT NULL, generation INTEGER NOT NULL,
 kind TEXT NOT NULL, payload TEXT NOT NULL, observed REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS h1_effects(
 effect_id TEXT PRIMARY KEY, task_id TEXT NOT NULL, generation INTEGER NOT NULL,
 body TEXT NOT NULL, target TEXT NOT NULL, state TEXT NOT NULL,
 platform_id TEXT, created REAL NOT NULL, platform_time REAL
);
CREATE TABLE IF NOT EXISTS h1_cutover(
 scope TEXT PRIMARY KEY, state TEXT NOT NULL, evidence TEXT NOT NULL, manifest TEXT
);
CREATE TABLE IF NOT EXISTS h1_revisions(
 task_id TEXT NOT NULL, revision INTEGER NOT NULL, request_json TEXT NOT NULL,
 reason TEXT NOT NULL, created REAL NOT NULL, PRIMARY KEY(task_id,revision)
);
"""


def initialize(store):
    with store.transaction() as db:
        for statement in SCHEMA.split(';'):
            if statement.strip():
                db.execute(statement)
        db.execute("INSERT OR IGNORE INTO gw_meta VALUES('harness_schema','1.0')")
        columns = {r['name'] for r in db.execute('PRAGMA table_info(h1_tasks)')}
        if 'source_version' not in columns:
            db.execute('ALTER TABLE h1_tasks ADD COLUMN source_version INTEGER NOT NULL DEFAULT 1')
        if db.execute("SELECT value FROM gw_meta WHERE key='harness_schema'").fetchone()[0] != '1.0':
            raise ValueError('unsupported harness schema')
