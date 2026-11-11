"""Local complete-file publication only. This is not a Grok Bot transport."""
import hashlib
import json
import os
from pathlib import Path
import tempfile

from ..models import ContractError, dumps
from .contracts import validate_envelope


def publish(root, envelope):
    validate_envelope(envelope)
    root = Path(root).resolve()
    root.mkdir(mode=0o700,parents=True,exist_ok=True)
    data = dumps(envelope).encode()
    if len(data)>131072 or envelope.get('schema_version')!='1.0':
        raise ContractError('unsupported or oversized envelope')
    digest = hashlib.sha256(data).hexdigest()
    target = root/(digest+'.json')
    fd, name = tempfile.mkstemp(prefix='.publish-',dir=root)
    try:
        with os.fdopen(fd,'wb') as stream:
            stream.write(data); stream.flush(); os.fsync(stream.fileno())
        os.replace(name,target)
        directory = os.open(root,os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if os.path.exists(name):
            os.unlink(name)
    return {'path':str(target),'sha256':digest,'size':len(data),'task_id':envelope['identity']['task_id']}


def read(root, ref):
    root = Path(root).resolve()
    path = Path(ref['path'])
    if path.parent != root or path.is_symlink() or path.name != ref['sha256']+'.json':
        raise ContractError('reference outside published root')
    fd = os.open(path,os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(fd,'rb') as stream:
        data = stream.read(131073)
    if len(data)>131072 or len(data)!=ref['size'] or hashlib.sha256(data).hexdigest()!=ref['sha256']:
        raise ContractError('partial or altered published payload')
    envelope = json.loads(data)
    if envelope.get('schema_version')!='1.0' or envelope.get('identity',{}).get('task_id')!=ref['task_id']:
        raise ContractError('payload identity or schema mismatch')
    return validate_envelope(envelope)
