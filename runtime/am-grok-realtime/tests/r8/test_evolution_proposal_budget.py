"""Proposal calls obey the same foreground and host-budget boundaries as replay."""
from test_evolution_policy import controlled

from am_gateway.assistant.evolution import ReplayBudget
from am_gateway.assistant.improvement import ImprovementJournal


def pending(controlled):
    build, _, store, _, _ = controlled
    engine = build()
    journal = ImprovementJournal(store, 'owner')
    journal.record_feedback('budget-feedback', actor_id='owner', text='回复太慢。')
    proposal = journal.analyze_once()
    calls = []

    def proposer(**request):
        calls.append(request)
        return {'patch': {'reply.layout': 'compact'}, 'rationale': '保留事实并比较简明表达'}

    engine.proposer = proposer
    arguments = dict(scope='controlled', development_ref='dev', holdout_ref='held',
                     budget=ReplayBudget(max_seconds=300, per_call_seconds=60))
    return engine, journal, store, proposal, calls, arguments


def test_foreground_yield_does_not_consume_proposal_retry(controlled):
    engine, journal, store, proposal, calls, arguments = pending(controlled)
    contexts = []

    def busy(context):
        contexts.append(context)
        return True

    engine.should_defer = busy
    report = engine.propose_from_journal(journal, **arguments)
    assert report['state'] == 'deferred' and report['proposal_model_called'] is False
    assert report['proposal_id'] == proposal['proposal_id']
    assert contexts[0]['mode'] == 'proposal' and contexts[0]['effects_allowed'] is False
    assert calls == []
    assert store.rows('SELECT * FROM r7_evo_proposal_attempts') == []
    assert store.rows('SELECT * FROM r7_evo_candidates') == []
    engine.should_defer = None
    assert engine.propose_from_journal(journal, **arguments)['state'] == 'proposed'
    assert len(calls) == 1


def test_zero_host_budget_defers_before_proposal_without_failure(controlled):
    engine, journal, store, _, calls, arguments = pending(controlled)
    engine.host_remaining_seconds = lambda: 0
    report = engine.propose_from_journal(journal, **arguments)
    assert report['state'] == 'deferred'
    assert report['reason'] == 'host_budget_exhausted_before_submission'
    assert report['step_calls_used'] == 0 and calls == []
    assert store.rows('SELECT * FROM r7_evo_proposal_attempts') == []


def test_host_remaining_budget_reduces_sixty_second_proposal_timeout(controlled):
    engine, journal, _, _, calls, arguments = pending(controlled)
    engine.host_remaining_seconds = lambda: 1
    report = engine.propose_from_journal(journal, **arguments)
    assert report['state'] == 'proposed'
    assert calls[0]['timeout_seconds'] == 1


def test_late_proposal_is_measured_against_clamped_not_original_timeout(controlled):
    engine, journal, store, _, calls, arguments = pending(controlled)
    original = engine.proposer

    def late(**request):
        value = original(**request)
        engine.monotonic.now += 2
        return value

    engine.proposer = late
    engine.host_remaining_seconds = lambda: 1
    report = engine.propose_from_journal(journal, **arguments)
    assert calls[0]['timeout_seconds'] == 1
    assert report['state'] == 'proposal_timeout'
    assert store.rows('SELECT * FROM r7_evo_candidates') == []
    assert store.one('SELECT state FROM r7_evo_proposal_attempts')['state'] == 'timeout'
