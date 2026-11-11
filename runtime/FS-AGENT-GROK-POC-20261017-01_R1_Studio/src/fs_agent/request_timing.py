"""Small body-free request timings; durations are monotonic, anchors are UTC.

No monitoring service or model call. Stored beside the original inbox and
expired with it. Missing evidence stays missing rather than becoming zero.
"""
import json
import time
from contextlib import contextmanager
from contextvars import ContextVar

CURRENT=ContextVar('am_request',default=None)

def set_context(store,event):return CURRENT.set((store,event))
def reset_context(token):CURRENT.reset(token)

@contextmanager
def remote(name):
    current=CURRENT.get()
    if current:
        with measure(current[0],current[1],name):yield
    else:yield


def initialize(store):
    with store.lock:
        store.conn.executescript('''
        CREATE TABLE IF NOT EXISTS am_request_timings(
          event_id TEXT PRIMARY KEY,received REAL NOT NULL,accepted REAL NOT NULL,
          started REAL,processed REAL,actionable REAL,finished REAL,readback_verified INTEGER,
          model_busy INTEGER NOT NULL,stages TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS am_outbox_events(outbox_id TEXT PRIMARY KEY,event_id TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS am_latency_anchors(event_id TEXT PRIMARY KEY,callback REAL,publish REAL,client REAL,verify REAL,verify_state TEXT);
        ''')
        columns={r[1] for r in store.conn.execute('PRAGMA table_info(am_latency_anchors)')}
        for name in ('task_start','outbox_ready','delivery_start','source_resume'):
            if name not in columns:store.conn.execute('ALTER TABLE am_latency_anchors ADD COLUMN '+name+' REAL')


def anchor(store,event,name,*,state=None):
    if not event or name not in ('callback','publish','client','verify','task_start','outbox_ready','delivery_start','source_resume'):return
    with store.tx() as c:
        c.execute('INSERT OR IGNORE INTO am_latency_anchors(event_id) VALUES(?)',(event,))
        c.execute('UPDATE am_latency_anchors SET '+name+'=COALESCE('+name+',?) WHERE event_id=?',(store.clock(),event))
        if state:c.execute('UPDATE am_latency_anchors SET verify_state=? WHERE event_id=?',(state,event))


def add(store,event,name,seconds):
    if not event:return
    with store.tx() as c:
        old=c.execute('SELECT stages FROM am_request_timings WHERE event_id=?',(event,)).fetchone()
        if not old:return
        stages=json.loads(old['stages']);part=stages.setdefault(name,{'seconds':0,'calls':0})
        part['seconds']+=max(0,seconds);part['calls']+=1
        c.execute('UPDATE am_request_timings SET stages=? WHERE event_id=?',(json.dumps(stages),event))


@contextmanager
def measure(store,event,name):
    start=time.monotonic()
    try:yield
    finally:add(store,event,name,time.monotonic()-start)


def origin(store,outbox_id):
    rows=store.rows('SELECT event_id FROM am_outbox_events WHERE outbox_id=?',(outbox_id,))
    return rows[0]['event_id'] if rows else None
