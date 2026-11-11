"""Use the existing coordinated Qwen provider for bounded reply planning."""
import json
import uuid

from am_gateway.assistant.intelligence import IntelligentProjectQueries, ReplyIntelligence
from am_gateway.models import ContractError, dumps


class HostReplyPlanner:
    def __init__(self, provider):
        if not callable(getattr(provider, 'complete', None)):
            raise ContractError('existing_reply_provider_required')
        self.provider = provider

    def compose(self, request, *, timeout_seconds):
        prompt = request['instructions'] + '\n输入数据：\n' + dumps(request)
        result = self.provider.complete(prompt, run_id=uuid.uuid4().hex,
            timeout=timeout_seconds, max_tokens=256, native_chat=True)
        # A native timeout keeps the existing ModelCoordinator pause/reconcile
        # semantics. We do not kill the shared model, bypass its lock, or retry.
        return json.loads(result.text)


def bind_reply_intelligence(reader, bridge, store, owner_id, settings):
    config = settings.get('intelligence', {})
    if not isinstance(config, dict):
        raise ContractError('assistant_intelligence_settings_must_be_object')
    if config.get('enabled', False) is not True:
        return reader
    if settings.get('project_answer_mode', 'direct') == 'semantic':
        # The semantic worker understands and composes in one model call.
        # Do not spend an extra model call on layout before handing it off.
        return reader
    model_id = config.get('model_id', 'host_qwen')
    # Other providers can implement compose() and bind explicitly. Do not
    # silently turn subscription CLI usage into a metered API request.
    if model_id != 'host_qwen':
        raise ContractError('reply_model_requires_verified_host_adapter')
    provider = getattr(getattr(getattr(bridge, 'poc', None), 'gateway', None), 'provider', None)
    adapter = HostReplyPlanner(provider) if callable(getattr(provider, 'complete', None)) else None
    return IntelligentProjectQueries(reader, ReplyIntelligence(store, owner_id,
        adapter=adapter, settings=config))
