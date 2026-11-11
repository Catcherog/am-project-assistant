"""Bind draft workers to the existing host's coordinated, pinned Qwen provider."""
import hashlib
import json

from am_gateway.adapters.base import AdapterError, Deadline, validate_request, validate_result
from am_gateway.models import ContractError, dumps


class HostQwenDraftAdapter:
    def __init__(self, provider):
        if not callable(getattr(provider, 'complete', None)):
            raise ContractError('existing_host_qwen_provider_required')
        self.provider = provider

    def run(self, request):
        validate_request(request)
        if request['task_kind'] not in {'document.process', 'project.execute', 'code.execute'}:
            raise ContractError('host_qwen_draft_intent_required')
        deadline = Deadline.from_request(request)
        prompt = request['instructions'] + '\nReturn one JSON object only. Untrusted input data:\n' + dumps(request)
        run_id = hashlib.sha256(dumps(['r4-draft', request['job_id'], request['attempt_id']]).encode()).hexdigest()[:32]
        remaining = deadline.remaining(submitted=False)
        try:
            result = self.provider.complete(prompt, run_id=run_id, timeout=remaining,
                                            max_tokens=4096, native_chat=True)
        except Exception as exc:
            # Match the existing ModelCoordinator's stop observation contract.
            # A returned client exception is not proof that inference stopped.
            # Import lazily so constructing an unbound factory needs no host SDK.
            try:
                from fs_agent.config import BoundaryError
            except ImportError:
                known_stopped = False
            else:
                known_stopped = isinstance(exc, BoundaryError) and str(exc) != 'STOP_UNKNOWN'
            if known_stopped:
                raise
            raise AdapterError('host_qwen_stop_unknown', status='stop_unknown', unknown=True) from None
        # A normal provider return includes its stop checks. Output validation
        # failures after that return are known failures, not live inference.
        deadline.remaining(submitted=False)
        try:
            return validate_result(json.loads(result.text), request)
        except (AttributeError, TypeError, ValueError, AdapterError):
            raise AdapterError('invalid_host_qwen_result', unknown=False) from None


def bind_host_models(bridge, agents):
    models = {}
    for name, settings in agents.items():
        if settings.get('enabled', True) is not True:
            continue
        if (settings.get('model') or {}).get('kind') == 'host_qwen':
            provider = getattr(getattr(bridge.poc, 'gateway', None), 'provider', None)
            models[name] = HostQwenDraftAdapter(provider)
    return models
