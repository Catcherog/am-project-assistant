"""Explicit isolated-host snapshot binding; never install a live consumer.

The caller supplies its already verified identities, approved chats and existing
SDK client. Authorization, ordinary ingress and legacy ownership stay with the
original Phase1Ports. This helper neither manufactures a production factory nor
loads credentials, starts workers, performs reads, or sends messages.
"""
from dataclasses import replace

from integration.phase1_runtime import Phase1Ports
from integration.studio_host import BindingError, HostScope, ScopedReadTransport


def bind_snapshot_ports(existing_ports, *, bridge, client, approved_group_chats,
                        existing_reader=None):
    """Bind the docs/phase1/07 ports, optionally reusing registered threads."""
    if not isinstance(existing_ports, Phase1Ports):
        raise BindingError('existing_phase1_ports_required')
    cfg = existing_ports.config
    cfg.validate()
    app_id = bridge.config.get('app_id')
    chats = frozenset(approved_group_chats)
    if (not app_id or not callable(getattr(client, 'request', None))
            or not callable(getattr(bridge, 'user_allowed', None))
            or not callable(existing_ports.authorization_for)
            or cfg.owner_chat_id in chats
            or any(not isinstance(chat, str) or not chat.startswith('oc_') for chat in chats)):
        raise BindingError('verified_snapshot_host_bindings_required')
    allowed = chats | {cfg.owner_chat_id}
    reader = existing_reader
    if reader is None:
        reader = ScopedReadTransport(client, allowed, app_id=app_id,
                                     bot_id=cfg.bot_id, tenant_id=cfg.tenant_id,
                                     identity='am_bot')
    elif (not isinstance(reader, ScopedReadTransport)
          or reader.client is not client or reader.chats != allowed
          or reader.app_id != app_id or reader.bot_id != cfg.bot_id
          or reader.tenant_id != cfg.tenant_id or reader.identity != 'am_bot'):
        raise BindingError('existing_snapshot_reader_scope_mismatch')
    scope = HostScope(bridge, cfg, chats, identity='am_bot')
    return replace(existing_ports, snapshot_reader=reader,
                   verified_snapshot_scope=scope.verified_snapshot)
