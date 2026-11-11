"""Owner-private R2 rollout and bounded local validation in the original host.

No listener, consumer, provider, coordinator, credentials or sending endpoint.
Validation uses the existing worker pool and the exact gateway coordinator.
Control/capture files are private, expire in <=24h, and never grant data access.
"""
from __future__ import annotations

from dataclasses import asdict, fields
import hashlib
import json
import math
import os
from pathlib import Path
import re
import time

from am_reply.compiler import build_legacy_contract, _FIELD_PATTERNS, _COMPLEX_QUESTION, _PROVENANCE_QUESTION
from am_reply.contracts import fallback_text, validate_contract
from am_reply.model import CoordinatorRewriteModel
from am_reply.pipeline import ReplyRewriter, requests_verbatim
from am_reply.types import RewriteConfig, RewriteRequest
from .output_rewrite import AmOutputFinalizer
from .types import ConversationError, canonical


def _hash(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def _write(path, value):
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temporary = path.with_suffix('.tmp')
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, 'w') as handle:
        json.dump(value, handle, ensure_ascii=False, indent=2, allow_nan=False)
    os.replace(temporary, path)


def _read(path):
    stat = path.lstat()
    if path.is_symlink() or stat.st_uid != os.getuid() or stat.st_mode & 0o077 or stat.st_size > 50000:
        raise ValueError('R2_PRIVATE_FILE_REQUIRED')
    return json.loads(path.read_text())


class _ObservedCoordinator:
    def __init__(self, original):
        self.original = original
        self.usage = {'attempted': 0, 'started': 0, 'completed': 0, 'errors': [], 'calls': []}

    def complete(self, prompt, **options):
        self.usage['attempted'] += 1
        started = time.monotonic()
        began = [None]
        def on_start(*args, **kwargs):
            self.usage['started'] += 1
            began[0] = time.monotonic()
        try:
            result = self.original.complete(prompt, **options, on_start=on_start)
            self.usage['completed'] += 1
            self.usage['calls'].append({'elapsed_seconds': time.monotonic()-started,
                'queue_and_setup_seconds': None if began[0] is None else began[0]-started,
                'provider_seconds': getattr(result, 'elapsed', None), 'usage': getattr(result, 'usage', None)})
            return result
        except Exception as exc:
            code = str(exc)
            self.usage['errors'].append(code if re.fullmatch('[A-Z_]{1,80}', code) else 'MODEL_ERROR')
            raise


class OwnerPrivateRewrite:
    def __init__(self, bridge, settings):
        if type(settings) is not dict:
            raise ValueError('OUTPUT_REWRITE_SETTINGS_INVALID')
        settings = dict(settings)
        self.validation_enabled = settings.pop('validation_enabled', False)
        if type(self.validation_enabled) is not bool or set(settings)-{f.name for f in fields(RewriteConfig)}:
            raise ValueError('OUTPUT_REWRITE_SETTINGS_INVALID')
        self.bridge, self.service = bridge, bridge.service
        self.settings = settings
        # Construct now to reject invalid budgets/modes at configuration time.
        ReplyRewriter(CoordinatorRewriteModel(self.service.g.provider), RewriteConfig(**settings))
        self.directory = Path(self.service.g.config.root)/'state/am-conversation-r2'
        self.directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.future = None
        self.last_cleanup = 0

    def control(self):
        base = {'mode': self.settings.get('mode', 'off'), 'capture': False, 'expires_at': None}
        if not self.validation_enabled:
            return base
        try:
            value = _read(self.directory/'control.json')
            if (type(value) is not dict or set(value) != {'mode','capture','expires_at'}
                    or value['mode'] not in ('off','shadow','enforce') or type(value['capture']) is not bool
                    or type(value['expires_at']) not in (int,float) or not math.isfinite(value['expires_at'])
                    or not self.service.s.clock() < value['expires_at'] <= self.service.s.clock()+86400):
                return base
            return value
        except (OSError, ValueError, TypeError):
            return base

    def _allowed(self, ctx):
        current = self.bridge._current(ctx)
        row = current['row']
        return (ctx.is_owner and ctx.actor_id == self.service.owner
                and row['user'] == self.service.owner and row['chat_type'] == 'p2p')

    def route_to_model_lane(self, ctx, text):
        if not self._allowed(ctx) or self.control()['mode'] == 'off':
            return False
        # Never inject a model call into the <=5 second immutable read path.
        # A routing miss claims nothing; the original durable worker owns it.
        if requests_verbatim(text) or _COMPLEX_QUESTION.search(text) or _PROVENANCE_QUESTION.search(text):
            return False
        return sum(bool(p.search(text)) for p in _FIELD_PATTERNS.values()) == 1

    def __call__(self, **arguments):
        ctx = arguments['ctx']
        control = self.control()
        if not self._allowed(ctx) or control['mode'] == 'off':
            return arguments['draft']
        current = self.bridge._current(ctx)
        current['r2_control'] = control
        def guard(context):
            self.bridge.agent._guard(context, 'read')
            self.bridge.agent._checkpoint(context, 'output_rewrite_guard')
            if not self._allowed(context) or self.control() != control:
                raise ConversationError('OUTPUT_REWRITE_SCOPE_CHANGED')
        def metrics(metric):
            self.bridge._timing(dict(metric, event_id=current['row']['id']))
        def observe(request, result):
            current['r2_result'] = result.telemetry()
            if control['capture']:
                expires = min(current['row']['expires'], control['expires_at'], self.service.s.clock()+86400)
                record = {'expires_at': expires, 'event_id': current['row']['id'],
                    'engine_event_id': request.event_id, 'scope_hash': ctx.session_id,
                    'source_access_version': ctx.source_access_version, 'question': request.user_text,
                    'before': request.draft, 'fixed_baseline': fallback_text(request.contract) if request.contract else None,
                    'candidate_after_checks': result.candidate_text, 'pipeline_after': result.text,
                    'contract': asdict(request.contract) if request.contract else None,
                    'mode': control['mode'], **result.telemetry(), 'human_review_completed': False}
                path = self.directory/'captures'/(_hash(current['row']['id'])+'.json')
                _write(path, record)
        finalizer = AmOutputFinalizer(self.service.g.provider,
            settings=dict(self.settings, mode=control['mode']), guard=guard, metrics=metrics, observer=observe)
        return finalizer(**arguments)

    def host_decision(self, ctx, event_id, answer, accepted):
        current = self.bridge._current(ctx)
        observation = current.get('r2_result')
        if observation:
            metric = dict(observation, event_id=current['row']['id'], span='output_rewrite_decision', seconds=0,
                mode=current['r2_control']['mode'], host_accepted=accepted,
                candidate_adopted=accepted and observation['disposition']=='accepted')
            self.bridge._timing(metric)
        path = self.directory/'captures'/(_hash(current['row']['id'])+'.json')
        try:
            record = _read(path)
            if record['engine_event_id'] == event_id:
                record.update(host_accepted=accepted, final_text=answer,
                    candidate_adopted=accepted and record['disposition']=='accepted')
                _write(path, record)
        except (OSError, ValueError, KeyError):
            pass

    def tick(self):
        if not self.validation_enabled:
            return
        now = self.service.s.clock()
        if now-self.last_cleanup > 60:
            for subfolder in ('captures','results'):
                for path in (self.directory/subfolder).glob('*.json'):
                    try:
                        value = _read(path)
                        if value.get('expires_at', 0) <= now:
                            path.unlink()
                    except (OSError, ValueError):
                        pass
            self.last_cleanup = now
        if self.future is not None:
            if not self.future.done():
                return
            try:
                self.future.result()
            except Exception:
                pass  # Validation I/O failure cannot terminate the original consumer.
            self.future = None
        requests = sorted((self.directory/'requests').glob('*.json'))
        if not requests:
            return
        path = requests[0]
        if not re.fullmatch('[a-z0-9-]{1,60}', path.stem):
            return
        try:
            job = _read(path)
            if (type(job) is not dict or set(job)-{'operation','case','index','max_seconds','expires_at','model_request'}
                    or job.get('operation') not in ('probe','survey','response_probe','task_model_probe')
                    or type(job.get('expires_at')) not in (int,float)
                    or not now < job['expires_at'] <= now+600):
                raise ValueError('R2_JOB_INVALID')
        except (OSError, ValueError, TypeError):
            path.rename(path.with_suffix('.rejected'))
            return
        # Claim before scheduling; an unknown/crashed job is never retried.
        path.rename(path.with_suffix('.claimed'))
        self.future = self.service.g.pool.submit(self._job, path.stem, job)

    def _job(self, label, job):
        try:
            if self.service.s.clock() >= job['expires_at']:
                raise ValueError('R2_JOB_EXPIRED')
            if job['operation'] == 'task_model_probe':
                request=job.get('model_request',{})
                if (not re.fullmatch(r'SYN-[A-Z]{3}-0[1-4]',str(job.get('case','')))
                    or set(request)!={'prompt','json_schema','max_tokens','timeout'}
                    or type(request['prompt']) is not str or len(request['prompt'])>30000
                    or len(json.dumps(request['json_schema']))>20000
                    or type(request['max_tokens']) is not int or not 1<=request['max_tokens']<=2048
                    or type(request['timeout']) not in (int,float) or not 0<request['timeout']<=120):
                    raise ValueError('R2_MODEL_PROBE_INVALID')
                # Model-only transport: never invokes tools or publishes messages.
                coordinator=_ObservedCoordinator(self.service.g.provider)
                generated=coordinator.complete(request['prompt'],run_id=__import__('uuid').uuid4().hex,
                    json_schema=request['json_schema'],native_chat=True,max_tokens=request['max_tokens'],
                    timeout=request['timeout'],_queue_timeout=min(30,request['timeout']))
                result={'text':generated.text,'model_calls':coordinator.usage,'synthetic_only':True}
            elif job['operation'] == 'response_probe':
                from am_response.evaluation import run_probe
                coordinator = _ObservedCoordinator(self.service.g.provider)
                result = run_probe(coordinator, job.get('case'), max_seconds=job.get('max_seconds', 90))
                result['model_calls'] = coordinator.usage
            else:
                result = self._probe(job) if job['operation']=='probe' else self._survey(job, label)
            result['ok'] = True
        except Exception as exc:
            code = str(exc)
            result = {'ok':False,'error':code if re.fullmatch('[A-Z_]{1,80}',code) else 'R2_VALIDATION_FAILED'}
        result.update(label=label, operation=job['operation'], completed_at=self.service.s.clock(),
            expires_at=self.service.s.clock()+86400, original_host_pid=os.getpid(),
            model=self.service.g.config.api_model, model_digest=self.service.g.config.api_model_digest,
            original_coordinator=True, external_business_calls=0)
        _write(self.directory/'results'/(label+'.json'), result)

    def _probe(self, job):
        from am_reply.examples import scenarios
        case = next((c for c in scenarios() if c['id']==job.get('case')), None)
        if case is None:
            raise ValueError('R2_CASE_INVALID')
        budget = job.get('max_seconds',6.0)
        if type(budget) not in (int,float) or not 1 <= budget <= 30:
            raise ValueError('R2_BUDGET_INVALID')
        coordinator = _ObservedCoordinator(self.service.g.provider)
        rewriter = ReplyRewriter(CoordinatorRewriteModel(coordinator), RewriteConfig(mode='enforce',max_seconds=budget))
        result = rewriter.rewrite(RewriteRequest(case['id'],case['question'],case['draft'],case['contract'],
            time.monotonic()+budget+1,protected=case['protected']),guard=lambda:None)
        return {'id':case['id'], 'question':case['question'], 'before':case['draft'],
            'fixed_baseline':None if case['protected'] else fallback_text(case['contract']),
            'candidate_after_checks':result.candidate_text,'after':result.text, **result.telemetry(),
            'model_calls':coordinator.usage,'human_review_completed':False,'style_acceptance_passed':False}

    def _survey(self, job, label):
        index = job.get('index',0)
        if type(index) is not int or not 0 <= index < 100:
            raise ValueError('R2_INDEX_INVALID')
        rows = self.service.s.rows("SELECT * FROM am_inbox WHERE user=? AND chat_type='p2p' AND kind='conversation' AND expires>? ORDER BY created DESC LIMIT 1",(self.service.owner,self.service.s.clock()))
        if not rows:
            raise ValueError('R2_CURRENT_OWNER_ORIGIN_REQUIRED')
        # Read-only use of a genuine unexpired origin. No inbox/event is invented,
        # renewed, claimed, answered or sent by this corpus projection survey.
        with self.bridge.request_context(dict(rows[0])) as ctx:
            projects = self.bridge.projects.list_projects(ctx)
            if index >= len(projects):
                return {'total_projects':len(projects),'index':index,'cases':[]}
            project = self.bridge.projects.read_project(ctx,projects[index]['id'])
            evidence = canonical(project)
            results, private = [], []
            for field in _FIELD_PATTERNS:
                alias = next((a for a in project.get('aliases',[]) if a),project['title'])
                question = alias+'的'+field+'是什么？'
                contract = build_legacy_contract(ctx, 'r2-survey:'+label, question, '', [evidence])
                if contract:validate_contract(contract)
                results.append({'field':field, 'contract_formed':bool(contract),
                    'origin':contract.origin if contract else None,
                    'disposition':'not_covered' if contract is None else 'source_projection' if contract.origin=='ledger_source_block' else 'llm_eligible_not_executed',
                    'contract_fingerprint':contract.fingerprint() if contract else None,
                    'source_chars':len(project['fields'].get(field,''))})
                private.append({'question':question,'contract':asdict(contract) if contract else None})
            self.bridge.authorize(ctx,'read',{'project_id':project['id']})
            _write(self.directory/'captures'/('survey-'+label+'.json'),{'project':project,'cases':private,
                'expires_at':min(ctx.content_expires_at,self.service.s.clock()+86400),
                'source_access_version':ctx.source_access_version})
            return {'total_projects':len(projects),'index':index,'project_hash':_hash(project['id']),
                'source_snapshot':project.get('snapshot',{}).get('version'),
                'extra_facts':len(project.get('information',{}).get('facts',[])),
                'cases':results,'real_model_calls':0,'source_access_version':ctx.source_access_version}
