"""Versioned immutable JSON snapshots, including the entire nested value."""
from dataclasses import dataclass
import hashlib
import json
import math

VERSION = 'am-outcome/2.0.2'


def encode(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)


def fingerprint(value):
    return hashlib.sha256(encode(value).encode('utf-8')).hexdigest()


@dataclass(frozen=True)
class OutcomeEnvelope:
    """Store bytes, not mutable dictionaries disguised by a frozen dataclass."""
    serialized: str

    @classmethod
    def freeze(cls, data):
        data = json.loads(encode(data))
        if data.get('schema_version') != VERSION:
            raise ValueError('OUTCOME_SCHEMA_VERSION')
        binding = data['binding']
        if (binding.get('kind') not in ('event', 'run') or not binding.get('origin_event_id')
                or not binding.get('engine_event_id') or len(binding.get('scope', [])) != 5):
            raise ValueError('OUTCOME_BINDING_INVALID')
        if binding['kind'] == 'run' and not all(k in binding for k in ('run_id', 'revision', 'cancel_epoch', 'generation')):
            raise ValueError('OUTCOME_RUN_BINDING_REQUIRED')
        if not math.isfinite(data['expires_at']):
            raise ValueError('OUTCOME_EXPIRY_INVALID')
        claim_ids={c['id'] for c in data['claims']}
        required=set(data.setdefault('required_claim_ids',list(c['id'] for c in data['claims'])))
        omissions=set(data.setdefault('allowed_omission_ids',[]))
        if required & omissions or required | omissions != claim_ids:
            raise ValueError('OUTCOME_CLAIM_REQUIREMENTS_INVALID')
        blocks = data['protected_blocks']
        ids = [b['id'] for b in blocks]
        if len(ids) != len(set(ids)):
            raise ValueError('OUTCOME_BLOCK_COLLISION')
        for unit in data['work_units']:
            if unit['effect'] != 'read' or unit['completion'] != 'fulfilled' or unit.get('receipt'):
                if unit.get('block_id') not in ids:
                    raise ValueError('OUTCOME_REQUIRED_STATUS_MISSING')
        data['outcome_hash'] = fingerprint(data)
        return cls(encode(data))

    @property
    def data(self):
        return json.loads(self.serialized)

    @property
    def digest(self):
        return self.data['outcome_hash']
