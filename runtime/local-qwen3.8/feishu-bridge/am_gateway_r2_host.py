"""Load the R2 candidate only when explicitly enabled in the existing host."""
from pathlib import Path
import hashlib
import json
import os
import sys
import time


def install(bridge, config, client, root):
    settings = config.get('multi_ai_gateway', {})
    if settings.get('enabled') is not True:
        return None, None
    if config.get('grok_routing', {}).get('enabled') or config.get('grok_realtime', {}).get('enabled'):
        raise RuntimeError('R2_REQUIRES_OLD_GROK_ROUTE_DISABLED')
    package = Path(__file__).resolve().parents[2] / 'am-grok-realtime'
    for path in (package / 'src', package):
        if not path.is_dir():
            raise RuntimeError('R2_PACKAGE_UNAVAILABLE')
        sys.path.insert(0, str(path))
    from integration.r2_runtime import install as install_runtime
    import integration.r2_runtime as runtime
    import am_gateway
    for module in (runtime, am_gateway):
        if not Path(module.__file__).resolve().is_relative_to(package):
            raise RuntimeError('R2_MODULE_PATH_MISMATCH')
    ingress, scheduler = install_runtime(bridge, config, client, root)
    paths = [Path(__file__).resolve()]
    paths += sorted((package / 'src' / 'am_gateway').rglob('*.py'))
    paths += sorted((package / 'integration').glob('*.py'))
    report = {'pid': os.getpid(), 'loaded_at': time.time(),
              'module_files': {'runtime': runtime.__file__, 'gateway': am_gateway.__file__},
              'files': {str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in paths},
              'consumer': 'original_unique_bridge', 'ledger_write': False}
    from bridge import atomic_json
    atomic_json(Path(root) / 'r2-loaded-manifest.json', report)
    return ingress, scheduler
