"""Body-free business health and exact loaded artifact fingerprints."""
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import threading
import time
from fs_agent.provider import atomic_json


class Health:
    def __init__(self,root):
        self.root=Path(root);self.lock=threading.RLock();self.areas={}
    def mark(self,area,state,*,request_id=None,error=None,elapsed=None,counts=None):
        with self.lock:
            old=self.areas.get(area,{})
            value={'state':state,'time':time.time(),'last_success':time.time() if state=='ok' else old.get('last_success')}
            if request_id:value['request_id']=hashlib.sha256(request_id.encode()).hexdigest()[:16]
            if error:value['error']=error
            if elapsed is not None:value['elapsed_seconds']=round(elapsed,4)
            if counts is not None:value['counts']=counts
            self.areas[area]=value
            atomic_json(self.root/'business-health.json',{'time':time.time(),'areas':self.areas})


def loaded_manifest(bridge,config):
    folder=Path(__file__).parent
    import fs_agent
    package=Path(fs_agent.__file__).parent
    files={}
    roots=[(folder,'feishu-bridge'),(package,'fs_agent')]
    if getattr(getattr(bridge,'am_self',None),'followup',None) is not None:
        import am_followup
        roots.append((Path(am_followup.__file__).parent,'am_followup'))
    if getattr(getattr(bridge,'am_self',None),'conversation',None) is not None:
        import am_conversation
        roots.append((Path(am_conversation.__file__).parent,'am_conversation'))
        import am_outcome, am_response
        roots.extend(((Path(am_outcome.__file__).parent, 'am_outcome'),
                      (Path(am_response.__file__).parent, 'am_response')))
        if getattr(bridge.am_self.conversation.agent, 'terminal_expression', None) is not None:
            import am_style
            roots.append((Path(am_style.__file__).parent,'am_style'))
        if getattr(bridge.am_self.conversation.agent, 'output_finalizer', None) is not None:
            import am_reply
            roots.append((Path(am_reply.__file__).parent,'am_reply'))
    if getattr(getattr(bridge,'am_self',None),'harness',None) is not None:
        import am_harness
        roots.append((Path(am_harness.__file__).parent,'am_harness'))
    import sys
    if 'am_training_window' in sys.modules:
        roots.append((Path(sys.modules['am_training_window'].__file__).parent,'am_training_window'))
    if 'am_doc_engine' in sys.modules:
        roots.append((Path(sys.modules['am_doc_engine'].__file__).parent,'am_doc_engine'))
    for root,prefix in roots:
        for p in root.rglob('*.py'):
            if '.venv' not in p.parts:files[prefix+'/'+str(p.relative_to(root))]=hashlib.sha256(p.read_bytes()).hexdigest()
    deps={}
    for name in ('lark-oapi','python-docx','pypdf','lxml'):
        try:deps[name]=importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError:deps[name]='unavailable'
    provider=bridge.poc.config if bridge.poc else None
    value={'pid':os.getpid(),'loaded_at':time.time(),'files':files,'config_sha256':hashlib.sha256(json.dumps(config,sort_keys=True).encode()).hexdigest(),
           'dependencies':deps,'features':config.get('am_self',{}),'disclosure_group_count':len(config.get('group_ledger',{}).get('disclosure',{})),
           'model':provider.api_model if provider else None,'model_digest':provider.api_model_digest if provider else None,
           'prompt_schema_version':'am-self-r1-v1','runtime_provider':'local_qwen_api','consumer':'original_unique_bridge',
           'information_sync_enabled':getattr(bridge,'information',None) is not None,
           'harness':config.get('am_harness',{})}
    atomic_json(bridge.root/'loaded-manifest.json',value)
    return value
