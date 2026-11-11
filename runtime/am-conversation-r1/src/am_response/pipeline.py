"""Prepare before history commit. No I/O, model retries or business execution."""
from contextvars import ContextVar
from dataclasses import replace
import json
import re
import time

from am_conversation.types import ConversationError
from am_outcome.types import OutcomeEnvelope, VERSION as OUTCOME_VERSION, fingerprint, encode
from am_outcome.adapters import adapt, evidence, family_for
from .schema import VERSION, PROMPT, plan_schema
from .semantics import next_steps

MODES = ('legacy', 'canonical', 'shadow', 'enforce')
INTERRUPTIONS = frozenset(('SOURCE_ACCESS_CHANGED', 'CONTENT_EXPIRED', 'STOP_UNKNOWN',
    'HARNESS_LOCAL_COMMIT_UNKNOWN', 'HARNESS_RESERVATION_RECONCILE_REQUIRED',
    'CONVERSATION_TIME_LIMIT', 'MODEL_PAUSED'))


def interruption_class(code):
    if code in ('MODEL_BUSY', 'MODEL_NOT_DISPATCHED', 'MODEL_BUDGET_EXHAUSTED'):
        return 'not_dispatched'
    if code == 'STOP_UNKNOWN': return 'provider_stop_unknown'
    if code in ('HARNESS_LOCAL_COMMIT_UNKNOWN', 'HARNESS_RESERVATION_RECONCILE_REQUIRED'):
        return 'effect_unknown'
    if code in ('SOURCE_ACCESS_CHANGED', 'CONTENT_EXPIRED'): return 'source_invalid'
    if 'CANCEL' in code or 'STALE' in code or 'REVISION' in code: return 'cancelled_or_stale'
    return 'propagate'


class ResponsePipeline:
    def __init__(self, settings=None, *, binding=None, targets=None, semantic_assessor=None, dependency_lookup=None, clock=time.time):
        settings = settings or {}
        if (type(settings) is not dict or set(settings) - {'mode', 'families', 'profile_version'}
                or settings.get('mode', 'legacy') not in MODES):
            raise ConversationError('RESPONSE_SETTINGS_INVALID')
        families = settings.get('families', [])
        if type(families) is not list or any(type(v) is not str for v in families):
            raise ConversationError('RESPONSE_SETTINGS_INVALID')
        self.mode = settings.get('mode', 'legacy')
        self.families = frozenset(families)
        self.profile = settings.get('profile_version', 'unvalidated')
        if type(self.profile) is not str: raise ConversationError('RESPONSE_SETTINGS_INVALID')
        from .semantics import SemanticAssessor
        self.binding, self.assessor, self.clock = binding, semantic_assessor or SemanticAssessor(), clock
        self.targets = targets
        self.dependency_lookup = dependency_lookup
        self._state = ContextVar('am_response_request', default=None)

    @property
    def enabled(self): return self.mode != 'legacy'

    def begin(self, ctx, event_id, text):
        if not self.enabled or not ctx.is_owner: return None
        bound = self.binding(ctx, event_id) if self.binding else {
            'kind': 'event', 'scope': list(ctx.scope), 'origin_event_id': event_id,
            'engine_event_id': event_id, 'source_access_version': ctx.source_access_version,
            'dependencies': {}, 'dependency_complete': False}
        if bound.get('engine_event_id') != event_id or tuple(bound.get('scope', [])) != ctx.scope:
            raise ConversationError('RESPONSE_BINDING_MISMATCH')
        from am_outcome.completion import resolve_goals
        goals, complete = resolve_goals(text, self.targets(ctx) if self.targets else [])
        return self._state.set({'binding': bound, 'ctx': ctx, 'event': event_id, 'text': text,
            'mode': self.mode, 'families': self.families, 'profile': self.profile,
            'goals': goals, 'goal_set_complete': complete,
            'units': {}, 'claims': [], 'dependency_observations': [], 'proposal': None, 'proposal_outcome': None,
            'started': time.monotonic(), 'task_model_calls': 0, 'task_repair_calls': 0,
            'presentation_calls': 0, 'repair_used': False, 'terminal': None,
            'expires_at': min([ctx.content_expires_at or self.clock()+600,
                               *bound.get('dependency_expiries', {}).values()])})

    def end(self, token):
        if token is not None: self._state.reset(token)

    def state(self): return self._state.get()

    def interruption(self, code):
        state = self.state()
        if state is None: return None
        return {'schema_version': VERSION, 'classification': interruption_class(code),
                'code': code, 'automatic_retry': False, 'lane_released_by_response': False,
                'task_model_calls': state['task_model_calls'], 'task_repair_calls': state['task_repair_calls'],
                'presentation_calls': 0, 'recorded_effects': sum(u['effect'] != 'read' for u in state['units'].values()),
                'expires_at': state['expires_at'],'dependency_shadow':self.dependency_report(state)}

    def dependency_report(self,state):
        from am_outcome.dependencies import compare
        rows=[]
        for record in state['dependency_observations']:
            previous=record['manifest']
            try:
                current=self.dependency_lookup(state['ctx'],previous) if self.dependency_lookup else {'objects':[]}
                verdict=compare(previous,current,now=self.clock())
            except Exception:
                verdict={'decision':'indeterminate','reason':'current_dependency_lookup_unavailable'}
            rows.append({'manifest':previous,'verdict':verdict,'authority':'observation_only'})
        return rows

    def require_current(self, agent, ctx):
        agent._guard(ctx, 'read')
        if ctx.content_expires_at is not None and ctx.content_expires_at <= self.clock():
            raise ConversationError('CONTENT_EXPIRED')
        # Reuse permitted existing phase for both durable and <=5s read binding.
        agent._checkpoint(ctx, 'before_finish')

    def guard_tool(self, name):
        state = self.state()
        if state is None: return
        if state['mode'] == 'shadow': return
        text = state['text']
        if name in ('save_work_note', 'update_work_note') and '跟进' in text and '笔记' not in text:
            raise ConversationError('REQUESTED_FOLLOWUP_NOT_WORKNOTE')

    def record(self, name, arguments, result, effect, *, call_key=None, bound_unit=None):
        state = self.state()
        if state is None: return
        key = call_key or fingerprint([name, arguments])
        unit = bound_unit if bound_unit is not None else adapt(name, arguments, result, effect, key=key)
        key = unit['id']
        state['units'][key] = unit
        if effect == 'read':
            from am_outcome.dependencies import manifest,compare
            dependency_result=dict(result) if isinstance(result,dict) else {}
            if name.startswith(('list_','search_')) or re.search('全部|所有|没有|不存在',state['text']):
                dependency_result['query']={'operation':name,'arguments':arguments,'requires_complete_collection':True}
            observed=manifest(dependency_result,scope=state['ctx'].scope,expires=state['expires_at'],access_revision=state['ctx'].source_access_version)
            state['dependency_observations'].append({'manifest':observed})
        if effect == 'read':
            by_id = {c['id']: c for c in state['claims']}
            by_id.update({c['id']: c for c in evidence(result, key[:12])})
            state['claims'] = list(by_id.values())

    def seed(self, fresh):
        for result in fresh:
            self.record('read_project', {'project_id': result['id']}, result, 'read')

    def envelope(self, reply=None):
        state = self.state()
        if state is None: return None
        binding = self.binding(state['ctx'], state['event']) if self.binding else dict(state['binding'])
        # A later lookup may add dependencies, never change the event/fence.
        for k, v in state['binding'].items():
            if k not in ('dependencies', 'dependency_complete', 'dependency_expiries') and binding.get(k) != v:
                raise ConversationError('RESPONSE_BINDING_CHANGED')
        units = list(state['units'].values())
        blocks = []
        for unit in units:
            if unit['effect'] != 'read' or unit['completion'] != 'fulfilled' or unit.get('receipt'):
                blocks.append({'id': unit['block_id'], 'role': 'status', 'required': True,
                               'text': unit['status_text']})
        terminal = reply.status if reply else state.get('terminal')
        if reply and reply.status=='fastpath_answered' and not units:
            blocks.append({'id':'fastpath-result','role':'fastpath','required':True,'text':reply.text})
        if terminal and terminal not in ('answered', 'recorded', 'fastpath_answered'):
            blocks.append({'id': 'terminal', 'role': 'terminal', 'required': True, 'text': reply.text if reply else ''})
        # Preview and critical state blocks are always host-owned and ordered.
        pending = []
        if reply and reply.pending_actions:
            pending = list(reply.pending_actions)
        families = sorted({u['family'] for u in units if u['family'] != 'capability'})
        if not families:
            families = ['query' if terminal in (None, 'answered', 'fastpath_answered', 'fastpath_missing') else 'control']
        if pending and 'approval' not in families: families.append('approval')
        statuses = [u['completion'] for u in units if u['family'] != 'capability']
        if pending or 'awaiting_approval' in statuses: completion = 'awaiting_approval'
        elif statuses and all(s == 'fulfilled' for s in statuses): completion = 'fulfilled'
        elif 'fulfilled' in statuses or 'partial' in statuses: completion = 'partial'
        elif 'active' in statuses: completion = 'active'
        else: completion = 'blocked'
        if terminal in ('context_limit', 'round_limit', 'fastpath_missing', 'budget_exhausted'): completion = 'partial' if units else 'blocked'
        if terminal in ('denied', 'execution_disabled', 'execution_unknown', 'execution_failed', 'version_conflict', 'preview_pending', 'preview_too_large'): completion = 'blocked'
        if terminal in ('clarification', 'no_pending_action'): completion = 'awaiting_input'
        if terminal == 'cancelled': completion = 'cancelled'
        if terminal == 'fastpath_answered': completion = 'fulfilled'
        # Multiple obligations cannot be proven from just the number of calls.
        # Keep partial until a trusted target resolver provides explicit goals.
        from am_outcome.completion import check_goals
        goal_results, goal_completion = check_goals(state['goals'], state['goal_set_complete'], units)
        intent = {'user_request': state['text'], 'requested_object_type': 'followup' if '跟进' in state['text'] and '笔记' not in state['text'] else None,
                  'completion_basis': 'typed_tool_results', 'goal_set_complete': state['goal_set_complete'],
                  'goals': goal_results}
        exact_control_complete = (terminal in ('execution_succeeded', 'knowledge_approved')
                                  and any(u['completion'] == 'fulfilled' for u in units))
        if exact_control_complete:
            intent['goal_set_complete'] = True
            intent['completion_basis'] = 'original_exact_version_control_and_receipt'
            completion = 'fulfilled'
        if state['goal_set_complete']:
            completion = goal_completion
        # Known unfinished duties remain visible even when other clauses are unresolved.
        for index, goal in enumerate(goal_results):
            if goal['completion'] != 'fulfilled' and goal['operation']!='unresolved':
                blocks.append({'id': 'goal:' + str(index), 'role': 'gap', 'required': True,
                               'text': '尚未完成：' + goal['request_spans'][0] + '的请求操作。'})
        if state['goal_set_complete']:
            matched={ref for g in goal_results for ref in g.get('observed_refs',[])}
            if any(u['effect']!='read' and u['completion']=='fulfilled' and u['id'] not in matched for u in units):
                blocks.append({'id':'unmatched-effects','role':'gap','required':True,'text':'另有已提交结果未能对应本轮目标，需核对；已提交动作不会自动重放。'})
        allowed_steps=next_steps(goal_results,units)
        if any(s['intent']=='question' for s in allowed_steps) and completion not in ('fulfilled','cancelled'):
            completion='partial' if any(g['completion']=='fulfilled' for g in goal_results) else 'awaiting_input'
        if completion == 'fulfilled' and terminal != 'fastpath_answered' and not intent['goal_set_complete']:
            completion = 'partial'
            blocks.append({'id': 'goal-coverage', 'role': 'gap', 'required': True,
                           'text': '上述结果已保留；本轮完整目标是否全部满足，仍需核对。'})
        deps = {'global_source_access_version': state['ctx'].source_access_version,
                'objects': binding.get('dependencies', {}), 'complete': binding.get('dependency_complete', False),
                'shadow_observations':self.dependency_report(state), 'policy': 'global_guard_retained', 'query_sets': [u['coverage'] for u in units if u['effect'] == 'read']}
        from .coverage import claim_requirements
        required_claims,allowed_omissions=claim_requirements(state['text'],state['claims'])
        return OutcomeEnvelope.freeze({'schema_version': OUTCOME_VERSION, 'binding': binding,
            'intent': intent, 'families': families, 'completion': completion,
            'work_units': units, 'claims': state['claims'], 'protected_blocks': blocks,
            'required_claim_ids':required_claims,'allowed_omission_ids':allowed_omissions,
            'pending_actions': pending, 'dependencies': deps, 'allowed_next_steps': allowed_steps,
            'expires_at': min([state['expires_at'], *binding.get('dependency_expiries', {}).values()]),
            'profile_version': self.profile, 'response_version': VERSION})

    def before_model(self, messages):
        state = self.state()
        if state is None: return messages
        envelope = self.envelope()
        state['proposal_outcome'] = envelope
        if state['mode'] == 'shadow': return messages
        data = envelope.data
        # The model sees the exact hash of the complete frozen outcome, plus a
        # bounded projection; omissions are explicit and never silently cut facts.
        from .schema import project
        model_data = project(envelope)
        return messages + [{'role': 'system', 'content': PROMPT + '\nhost_outcome=' + encode(model_data)}]

    def model_call_started(self):
        state = self.state()
        if state is not None:
            key = 'task_repair_calls' if state.pop('repair_pending', False) else 'task_model_calls'
            state[key] += 1

    def needs_repair(self):
        state = self.state()
        if state is None or state['mode'] == 'shadow' or state['repair_used'] or not state['goal_set_complete']: return False
        from am_outcome.completion import check_goals
        goals, completion = check_goals(state['goals'], True, list(state['units'].values()))
        if completion == 'fulfilled' or any(s['intent']=='question' for s in next_steps(goals,list(state['units'].values()))): return False
        if any(u['status'] == 'unknown' for u in state['units'].values()): return False
        state['repair_used'] = True
        state['repair_pending'] = True
        return True

    def proposed(self, plan):
        state = self.state()
        if state is not None: state['proposal'] = plan

    def validate(self, plan, envelope, maximum):
        from am_conversation.model import validate_arguments
        try:
            if 'snapshot' in plan:
                from .schema import expand
                plan = expand(plan,envelope)
            validate_arguments(plan, plan_schema())
        except ConversationError: return None, 'schema_invalid'
        data = envelope.data
        if plan['envelope_fingerprint'] != data['outcome_hash']: return None, 'outcome_mismatch'
        claims = {c['id']: c for c in data['claims']}
        blocks = {b['id']: b for b in data['protected_blocks']}
        required = [b['id'] for b in data['protected_blocks'] if b['required']]
        chosen, parts, used, has_free = [], [], set(), False
        for segment in plan['segments']:
            if segment['kind'] == 'locked':
                ident = segment['block_id']
                if ident not in blocks or ident in chosen: return None, 'block_invalid'
                chosen.append(ident); parts.append(blocks[ident]['text']); continue
            has_free = True
            if (not segment['claim_ids'] and segment['intent'] not in ('question','recommendation')) or any(c not in claims for c in segment['claim_ids']):
                return None, 'claims_invalid'
            if segment['intent'] in ('recommendation', 'question'):
                allowed={s['id']:s for s in data['allowed_next_steps']}
                if not segment.get('next_step_ids') or any(i not in allowed or allowed[i]['intent']!=segment['intent'] for i in segment['next_step_ids']):
                    return None, 'next_step_unverified'
                if segment['claim_ids']:return None,'non_statement_claim_reference'
            if re.search(r'(?i)<\s*/?at\b|https?://|action_[0-9a-f]|ou_[a-z0-9]', segment['text']):
                return None, 'authority_markup'
            used.update(segment['claim_ids']); parts.append(segment['text'])
        if [x for x in chosen if x in required] != required: return None, 'required_block_missing_or_reordered'
        omitted = plan['omitted_optional_claim_ids']
        if any(c not in data.get('allowed_omission_ids',[]) or c in used for c in omitted): return None, 'omission_invalid'
        if not set(data.get('required_claim_ids',claims)).issubset(used):return None,'required_claim_missing'
        if used | set(omitted) != set(claims): return None, 'unaccounted_claims'
        text = '\n\n'.join(parts)
        if not text.strip() or len(text) > maximum: return None, 'output_budget'
        if has_free:
            if self.assessor is None or self.profile == 'unvalidated': return None, 'semantic_profile_unavailable'
            if not set(data['families']).issubset(self.families): return None, 'family_unvalidated'
            # Not a fixture allow-all. An installed evaluator must identify its
            # frozen profile; absence, uncertainty or mismatch refuses adoption.
            verdict = self.assessor(plan, envelope)
            if self.state() is not None:self.state()['semantic_verdict']=verdict
            if (not isinstance(verdict, dict) or verdict.get('profile_version') != self.profile
                    or verdict.get('decision') != 'pass'):
                return None, 'semantic_indeterminate_or_rejected'
            covered=set(verdict.get('covered_claim_ids',[]))
            if not used.issubset(covered) or not set(data.get('required_claim_ids',claims)).issubset(covered):
                return None,'semantic_fact_coverage_missing'
        if has_free:
            checks=iter(verdict.get('checks',[]));rendered=[]
            for segment,part in zip(plan['segments'],parts):
                check=next(checks,{}) if segment['kind']=='text' else {}
                rendered.append(('资料记录：\n' if check.get('render_as_source_quote') else '')+part)
            text='\n\n'.join(rendered)
            if len(text)>maximum:return None,'output_budget'
        return text, (('source_projection' if verdict.get('expression_class')=='source_projection' else 'candidate') if has_free else 'protected_projection')

    def finish(self, agent, ctx, reply):
        state = self.state()
        if state is None: return reply, None
        if state['mode'] != 'shadow':self.require_current(agent, ctx)
        started = time.monotonic()
        envelope = self.envelope(reply)
        state['terminal'] = reply.status
        data = envelope.data
        plan = state['proposal']
        candidate, reason = None, 'no_proposal'
        if plan is not None:
            # Revalidate against the exact pre-model outcome, then require the
            # current host result to remain identical. Never attach a new hash.
            prior = state['proposal_outcome']
            if prior is not None and prior.digest == envelope.digest:
                candidate, reason = self.validate(plan, envelope, agent.config.max_reply_chars)
            else: reason = 'outcome_changed_after_proposal'
        selected = 'canonical'
        text = reply.text
        if state['mode'] == 'enforce' and candidate is not None:
            text, selected = candidate, reason
        elif plan is not None:
            selected = 'shadow' if state['mode'] == 'shadow' else 'fallback'
        # Invalid free model text is never the canonical fallback. Host-owned
        # source quotes and required status blocks are the only fallback data.
        if state['mode'] == 'shadow':
            selected, text = 'shadow', reply.text
        elif reply.status == 'answered':
            if selected not in ('candidate', 'protected_projection', 'source_projection'):
                parts = []
                from .semantics import source_text
                for claim in data['claims']:
                    parts.append(str(claim.get('predicate', '资料')) + '：\n' + source_text(claim))
                parts.extend(b['text'] for b in data['protected_blocks'])
                parts.extend(s['text'] for s in data['allowed_next_steps'] if s['intent']=='question')
                text = '\n\n'.join(parts) or '目前还没有可核对的业务结果，请补充具体对象或必要资料。'
        else:
            for block in data['protected_blocks']:
                if block['text'] and block['text'] not in text:
                    text += '\n\n' + block['text']
            for step in data['allowed_next_steps']:
                if step['intent']=='question' and step['text'] not in text:text+='\n\n'+step['text']
        if len(text) > agent.config.max_reply_chars:
            # Preserve required blocks whole. Do not slice a preview or receipt.
            required = '\n\n'.join(b['text'] for b in data['protected_blocks'])
            text = required + '\n\n本次资料较多，完整正文尚未呈现，请缩小查询范围。'
            if len(text) > agent.config.max_reply_chars:
                raise ConversationError('RESPONSE_PROTECTED_BUDGET')
            selected, reason = 'fallback', 'output_budget'
        if state['mode'] != 'shadow':self.require_current(agent, ctx)
        presentation = {'schema_version': VERSION, 'presentation_id': fingerprint([data['binding'], 'final']),
            'binding_key': fingerprint(data['binding']), 'outcome': data,
            'semantic_hash': fingerprint(text), 'selected_source': selected, 'reason': reason,
            'dispatch_budget_exhausted':state.get('dispatch_budget_exhausted',False),
            'candidate_hash': fingerprint(plan) if plan is not None else None,
            'semantic_verdict': state.get('semantic_verdict'),
            'replay_input': {'envelope':data,'user_question':state['text'],'legacy_text':reply.text} if state['mode']=='shadow' else None,
            'candidate_plan':plan,
            'observation_kind':'offline_replay_pending' if state['mode']=='shadow' else 'online',
            'eligible': bool(data['claims'] or data['allowed_next_steps']) and set(data['families']).issubset(self.families) and self.assessor is not None and self.profile != 'unvalidated',
            'attempted': plan is not None, 'accepted': candidate is not None,
            'adopted': selected == 'candidate', 'business_completion': data['completion'],
            'delivery': 'prepared', 'naturalness': 'not_evaluated', 'mode': state['mode'],
            'task_model_calls': state['task_model_calls'], 'task_repair_calls': state['task_repair_calls'],
            'model_calls_total': state['task_model_calls'] + state['task_repair_calls'],
            'presentation_calls': 0, 'prepare_ms': (time.monotonic()-started)*1000,
            'total_ms': (time.monotonic()-state['started'])*1000, 'expires_at': data['expires_at']}
        return replace(reply, text=text), presentation
