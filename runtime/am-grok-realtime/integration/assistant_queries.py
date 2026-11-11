"""Read project facts from the host's existing, audience-scoped ledger snapshots.

No model, Feishu consumer, sender or business writer is created here. Cold and
negative reads use the existing asynchronous snapshot refresh, never a 45-second
wait in the assistant intake lane.
"""
from __future__ import annotations

from collections import OrderedDict
import threading

from am_gateway.assistant.retrieval import LedgerIndex, eligible_question
from am_gateway.models import key


class FastReadPending(RuntimeError):
    """The durable intake should retry after other requests can run."""


class StudioProjectQueries:
    def __init__(self, bridge, authority, settings=None):
        self.authority = authority
        self.snapshots = getattr(getattr(bridge, 'am_self', None), 'snapshots', None)
        self.settings = dict(settings or {})
        self.bindings = self.settings.get('ledger_project_refs', {})
        if not isinstance(self.bindings, dict) or any(
                not isinstance(k, str) or not k or not isinstance(v, str) or not v
                for k, v in self.bindings.items()):
            raise ValueError('ledger_project_refs must bind trusted project IDs to source refs')
        self.enabled = self.settings.get('enabled', True) is True
        self.max_age = float(self.settings.get('max_snapshot_age_seconds', 60))
        if not 1 <= self.max_age <= 300:
            raise ValueError('snapshot age must be between 1 and 300 seconds')
        self._refreshes = OrderedDict()
        self._indices = OrderedDict()
        self._lock = threading.RLock()

    @property
    def bound(self):
        return bool(self.enabled and self.snapshots is not None
                    and callable(getattr(self.snapshots, 'get', None))
                    and callable(getattr(self.snapshots, 'refresh', None)))

    @staticmethod
    def _unavailable(reason='unavailable'):
        return {'status': reason if reason in {'unavailable', 'incomplete', 'unbound'} else 'unavailable',
                'reason_code': reason, 'project_ids': [], 'matches': [],
                'body': '这次还无法可靠读取当前台账，暂时不能判断项目是否有记录。请稍后再查；原请求已保留。',
                'source_version': '', 'source_revision': None, 'source_ref': '',
                'requires_refresh': False}

    def _refresh(self, request_key):
        with self._lock:
            future = self._refreshes.get(request_key)
            if future is None:
                future = self.snapshots.refresh()
                self._refreshes[request_key] = future
                # This is only an optimisation cache: eviction never loses an
                # intake, delivery or feedback record, and does not send anything.
                while len(self._refreshes) > 512:
                    self._refreshes.popitem(last=False)
            else:
                self._refreshes.move_to_end(request_key)
        if not future.done():
            raise FastReadPending('ledger_refresh_in_progress')
        try:
            future.result()
        except Exception:
            return False
        return True

    def __call__(self, event, actor, project_ids, *, focus_project_id=None):
        if not self.bound or event.attachments or not eligible_question(event.body):
            return None
        # Project names and message-supplied project IDs never confer access.
        allowed = tuple(dict.fromkeys(project_ids))
        if event.channel == 'group':
            current_group = set(self.authority.projects_for_chat(event.chat_id))
            allowed = tuple(p for p in allowed if p in current_group)
        # The coordinator supplies current directory grants. The existing
        # StudioSender rechecks the exact message audience before transmission;
        # repeating its network guard in this pure read would add serial I/O.
        if not allowed:
            value = self._unavailable('unbound')
            value['body'] = '当前对话还没有可用的项目台账绑定，暂时无法核对进展；这不表示项目不存在。'
            return value
        request_key = key(event.item_id, event.version_time, event.content_hash)
        try:
            snapshot = self.snapshots.get(wait=False)
        except Exception as exc:
            if str(exc) == 'SOURCE_LOADING':
                if not self._refresh(request_key):
                    return self._unavailable()
                snapshot = self.snapshots.get(wait=False)
            else:
                # One asynchronous recovery attempt; transport failures are not
                # converted to an empty set of projects.
                if str(exc) not in {'SOURCE_READ_UNAVAILABLE', 'QUERY_SNAPSHOT_EXPIRED'}:
                    return self._unavailable()
                if not self._refresh(request_key):
                    return self._unavailable()
                try:
                    snapshot = self.snapshots.get(wait=False)
                except Exception:
                    return self._unavailable()
        clock = getattr(self.snapshots, 'clock', None)
        # A request's completed refresh is a lower freshness bound, not a
        # requirement for the global cache to return that exact old version.
        # Another request may already have published a newer snapshot.
        with self._lock:
            refreshed = self._refreshes.get(request_key)
        refresh_satisfied = False
        if refreshed is not None and refreshed.done():
            try:
                refreshed_value = refreshed.result()
            except Exception:
                refreshed_value = None
            if refreshed_value is not None:
                observed = refreshed_value.get('observed_at')
                expired = (callable(clock) and isinstance(observed, (int, float))
                           and clock() - observed > self.max_age)
                if expired:
                    # A pending approval may outlive the first successful
                    # refresh. Its next source check must be able to refresh.
                    with self._lock:
                        if self._refreshes.get(request_key) is refreshed:
                            self._refreshes.pop(request_key)
                else:
                    try:
                        # This get happens after the successful refresh. It
                        # also retains the source port's current access check.
                        snapshot = self.snapshots.get(wait=False)
                    except Exception:
                        return self._unavailable()
                    refresh_satisfied = snapshot.get('source') == refreshed_value.get('source')
        snapshot = dict(snapshot)
        snapshot['include_read_time'] = False
        if callable(clock):
            observed = snapshot.get('observed_at')
            if not isinstance(observed, (int, float)) or clock() - observed > self.max_age:
                snapshot['stale'] = True
        version = snapshot.get('version', '')
        # QuerySnapshots.get() labels any stored version a cache hit. A source
        # read after this request's successful refresh is fresh enough to test
        # a miss, including when another request has advanced the cache.
        if refresh_satisfied:
            snapshot['cache_hit'] = False
        index_key = key(version, snapshot.get('stale'), snapshot.get('refresh_error'),
                        snapshot.get('complete'), snapshot.get('cache_hit'), self.bindings)
        with self._lock:
            index = self._indices.get(index_key)
            if index is None:
                index = LedgerIndex(snapshot, self.bindings)
                self._indices[index_key] = index
                while len(self._indices) > 4:
                    self._indices.popitem(last=False)
        result = index.query(event.body, allowed, focus_project_id=focus_project_id)
        if result.get('status') == 'unsupported':
            return None
        needs_fresh = result.get('requires_refresh') or result.get('status') in {'no_match', 'unbound'}
        if needs_fresh:
            with self._lock:
                already_refreshed = request_key in self._refreshes
            if not already_refreshed:
                self._refresh(request_key)
                raise FastReadPending('ledger_refresh_before_negative_answer')
            if not self._refresh(request_key):
                return self._unavailable()
            with self._lock:
                refreshed_value = self._refreshes[request_key].result()
            if (not refresh_satisfied and (refreshed_value.get('version') != version or
                    (result.get('requires_refresh') and snapshot.get('cache_hit') is True
                     and refreshed_value.get('complete') is True))):
                raise FastReadPending('completed_refresh_requires_new_snapshot')
            # get() on the next intake retry observes the newly committed source.
            # If the successfully refreshed source is still incomplete, do not
            # pretend its miss is a confirmed absence.
            if result.get('requires_refresh'):
                return self._unavailable('incomplete')
        result = dict(result)
        result['source_observed_at'] = snapshot.get('observed_at')
        result['answer_fingerprint'] = key(result.get('status'), result.get('body'),
                                           result.get('project_ids'), result.get('source_ref'))
        return result

    def health(self):
        return {'enabled': self.enabled, 'bound': self.bound,
                'source': 'existing_query_snapshots', 'max_snapshot_age_seconds': self.max_age,
                'explicit_reference_bindings': len(self.bindings), 'model_calls': 0,
                'production_experience_verified': False}
