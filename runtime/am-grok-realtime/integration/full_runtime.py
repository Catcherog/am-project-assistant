"""R4 factory selected by the existing Bridge's multi_ai_gateway host hook.

No Feishu websocket is created here. Building ports does not start inference,
polling, worker servers or delivery. install(activate=True) starts the assembly.
"""
from __future__ import annotations

from dataclasses import replace
import importlib
import os
from pathlib import Path

from am_gateway.models import ContractError
from am_gateway.phase1.agents import ArtifactStore, ProposalRepository, build_agent_runners
from am_gateway.phase1.contracts import registry
from am_gateway.phase1.control_api import ControlAPI
from am_gateway.phase1.execution import ConfiguredWebhookWake, HTTPReceiverAdapter, ReceiverHTTPServer, ReceiverPump
from am_gateway.phase1.receiver import DurableReceiver, LocalReceiverAdapter
from am_gateway.phase1.routing import Classifier
from am_gateway.phase1.service import RuntimeService
from am_gateway.phase1.terra import TerraCLI
from am_gateway.store import Store
from integration.phase1_runtime import install as install_candidate
from integration.phase1_artifacts import ArtifactDelivery
from integration.studio_phase1_factory import build_phase1_bundle
from integration.studio_agent_models import bind_host_models


def _secret(settings, field, default_name):
    name = settings.get(field, default_name)
    value = os.environ.get(name, '')
    if len(value) < 24:
        raise ContractError('missing or short credential in environment variable ' + name)
    return value


def _host_callable(reference):
    if not reference:
        return None
    if not isinstance(reference, str) or ':' not in reference:
        raise ContractError('host binding must be module:function')
    module, name = reference.split(':', 1)
    function = getattr(importlib.import_module(module), name)
    if not callable(function):
        raise ContractError('host binding is not callable')
    return function


class _ManagedWake:
    def __init__(self, wake, receiver):
        self.wake, self.receiver = wake, receiver

    def start(self):
        # Resume a previously accepted job after restart without dispatching it
        # again. Waking an empty queue never sends an unsolicited notification.
        if self.receiver.health().get('states', {}).get('accepted', 0):
            self.wake.notify()

    def stop(self):
        return self.wake.stop()


def _require_drained_foreground(store, *, interactive_enabled):
    """A configuration change cannot reroute an existing live/unknown run.

    Existing lifecycle status/cancel calls use the configured adapter. Keep the
    original configuration until those calls have observed an actual terminal
    state; changing a namespace is not a recovery operation.
    """
    with store.transaction() as db:
        tables = {row[0] for row in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if 'h4_receiver_runs' in tables:
            previous = 'grok_bot' if interactive_enabled else 'interactive'
            live = db.execute("SELECT 1 FROM h4_receiver_runs WHERE namespace=? "
                "AND state IN ('accepted','running','cancel_requested','unknown') LIMIT 1", (previous,)).fetchone()
            if live:
                raise ContractError('foreground_adapter_switch_requires_original_runs_to_finish')
        if 'h1_tasks' in tables:
            operator = 'NOT LIKE' if interactive_enabled else 'LIKE'
            live = db.execute("SELECT 1 FROM h1_tasks WHERE intent IN "
                "('project.query','conversation.reply','work_message.assist') "
                f"AND scope {operator} 'interactive:%' "
                "AND dispatch_state IN ('pending','dispatching','accepted','unknown') "
                "AND execution_state NOT IN ('succeeded','failed','cancelled') LIMIT 1").fetchone()
            if live:
                raise ContractError('foreground_adapter_switch_requires_original_runs_to_finish')


def build_ports(bridge, host_config, client, root):
    """Callable host_factory used by integration.r2_runtime.install."""
    settings = dict(host_config.get('phase1_gateway', host_config.get('multi_ai_gateway', {})))
    classification = settings.get('classifier', {})
    classifier = None
    if classification.get('enabled'):
        classifier = Classifier(TerraCLI(classification['binary'], model=classification.get('model', 'gpt-6.1-terra')))
    bundle = build_phase1_bundle(bridge, host_config, client, root, classifier=classifier)
    ports = bundle.ports

    def assemble(actual_ports):
        store = Store(actual_ports.database_path)
        _require_drained_foreground(store,
            interactive_enabled=settings.get('interactive', {}).get('enabled') is True)
        root_path = Path(root).resolve()
        artifact_root = root_path / settings.get('artifact_directory', 'r4-artifacts')
        if not artifact_root.resolve().is_relative_to(root_path):
            raise ContractError('artifact directory must be inside runtime root')
        agent_runners = build_agent_runners(artifact_root=artifact_root,
            config=settings.get('agents', {}), store=store, owner_id=actual_ports.config.owner_id,
            models=bind_host_models(bridge, settings.get('agents', {})))
        caps, adapters, receivers, pumps, servers = registry(), {}, [], [], []
        bot = settings.get('grok_bot', {})
        mode = bot.get('mode', 'local_pull')
        if mode == 'local_pull':
            receiver = DurableReceiver(store, namespace='grok_bot',
                intents=('project.query', 'conversation.reply', 'work_message.assist'),
                worker_lease_seconds=bot.get('worker_lease_seconds', 60))
            wake = None
            if bot.get('wake_url'):
                wake = ConfiguredWebhookWake(bot['wake_url'],
                    token=os.environ.get(bot.get('wake_token_env', 'AM_GROK_WAKE_TOKEN')),
                    timeout=bot.get('wake_timeout_seconds', 2))
                servers.append(_ManagedWake(wake, receiver))
            adapter = LocalReceiverAdapter(receiver, wake=wake)
            adapters['grok_bot'] = adapter
            receivers.append(receiver)
            for intent in receiver.intents:
                caps[intent] = adapter.capability(intent)
        elif mode == 'remote_receiver':
            adapter = HTTPReceiverAdapter(bot['receiver_url'],
                _secret(bot, 'receiver_token_env', 'AM_GROK_RECEIVER_TOKEN'),
                namespace='grok_bot', evidence_ref=bot.get('boundary_evidence_ref', ''),
                verified_boundary=bot.get('boundary_verified') is True)
            adapters['grok_bot'] = adapter
            for intent in ('project.query', 'conversation.reply', 'work_message.assist'):
                caps[intent] = adapter.capability(intent)
        elif mode != 'disabled':
            raise ContractError('unknown Grok Bot integration mode')
        for name, runner in agent_runners.items():
            if settings.get('agents', {}).get(name, {}).get('enabled', True) is not True:
                continue
            intent = runner.intent
            receiver = DurableReceiver(store, namespace=name, intents=(intent,), worker_lease_seconds=60)
            adapter = LocalReceiverAdapter(receiver)
            adapters[name] = adapter
            caps[intent] = adapter.capability(intent)
            receivers.append(receiver)
            pumps.append(ReceiverPump(receiver, runner))
            adapter.wake = pumps[-1]
        interactive_runner = None
        interactive = settings.get('interactive', {})
        if interactive.get('enabled') is True:
            from integration.assistant_interactive import bind_interactive_runner
            interactive_runner = bind_interactive_runner(bridge, bundle.authority, store, settings)
            receiver = DurableReceiver(store, namespace='interactive',
                intents=('project.query', 'conversation.reply', 'work_message.assist'),
                worker_lease_seconds=60)
            adapter = LocalReceiverAdapter(receiver)
            adapters['interactive'] = adapter
            receivers.append(receiver)
            for intent in receiver.intents:
                caps[intent] = adapter.capability(intent)
            workers = interactive.get('workers', 2)
            if type(workers) is not int or not 1 <= workers <= 4:
                raise ContractError('interactive_workers_must_be_between_1_and_4')
            # The Harness still serializes each execution scope; the receiver
            # atomically claims each operation once. Extra pumps permit other
            # chats to reach the existing model gate without a global worker
            # queue. MODEL_BUSY remains a known failure, never a second model.
            for worker in range(workers):
                pumps.append(ReceiverPump(receiver, interactive_runner,
                    worker_id='interactive:local:' + str(worker + 1)))
            adapter.wake = pumps[-1]
        actual_ports.config = replace(actual_ports.config, capabilities=caps)
        actual_ports.adapters = adapters
        ingress, _ = install_candidate(actual_ports, activate=False)
        collection = bundle.bind(ingress)
        # The host hook must be consumed by old pollers, and the shared ledger
        # rejects an item already captured by the active Harness ingress.
        bridge.multi_ai_legacy_ownership = bundle.legacy_ownership
        for pump in pumps:
            pump.harness = ingress.gateway
        if mode == 'local_pull':
            api = bot.get('worker_api', {})
            bot_receiver = next(r for r in receivers if r.namespace == 'grok_bot')
            servers.append(ReceiverHTTPServer(bot_receiver, harness=ingress.gateway,
                admin_token=_secret(api, 'admin_token_env', 'AM_GATEWAY_RECEIVER_ADMIN_TOKEN'),
                worker_token=_secret(api, 'worker_token_env', 'AM_RECEIVER_WORKER_TOKEN'),
                host=api.get('host', '127.0.0.1'), port=int(api.get('port', 8766))))
        sender = bundle.sender if settings.get('send_live') is True else None
        service = RuntimeService(ingress, receivers=receivers, pumps=pumps,
            collector=collection, sender=sender, readback=bundle.readback,
            artifact_delivery=ArtifactDelivery(ingress.gateway, ArtifactStore(artifact_root),
                client, bundle.authority, bundle.readers, send_live=settings.get('send_live') is True),
            servers=servers, ownership=bundle.legacy_ownership,
            auto_scopes=True, dispatch_enabled=settings.get('dispatch_enabled', True) is True,
            delivery_workers=settings.get('delivery_workers', 4))
        service.interactive_runner = interactive_runner
        business_factory = _host_callable(settings.get('business_executor_factory'))
        executor = business_factory(bridge, host_config, client, root) if business_factory else None
        control = settings.get('control_api', {})
        if control.get('enabled', True) is True:
            api = ControlAPI(service, token=_secret(control, 'token_env', 'AM_GATEWAY_OPERATOR_TOKEN'),
                host=control.get('host', '127.0.0.1'), port=int(control.get('port', 8767)),
                proposals=ProposalRepository(store, actual_ports.config.owner_id), executor=executor,
                artifacts=ArtifactStore(artifact_root))
            service.servers.append(api)
            service.control_api = api
        service.bundle = bundle
        service.artifacts = ArtifactStore(artifact_root)
        service.proposals = ProposalRepository(store, actual_ports.config.owner_id)
        if settings.get('assistant', {}).get('enabled') is True:
            from integration.assistant_host import bind_assistant
            bind_assistant(service, bundle, bridge, settings, client, root, executor=executor)
            service.assistant.interactive_runner = interactive_runner
        ingress.scheduler = service
        return ingress, service

    ports.runtime_factory = assemble
    return ports
