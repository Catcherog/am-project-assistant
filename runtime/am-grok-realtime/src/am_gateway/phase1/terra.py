"""Optional locally verified Codex CLI transport for the requested Terra classifier.

No OpenAI API key is assumed. The host must provide the exact installed binary
and a model identifier verified from its catalog. Never launches a Grok executor.
"""
import json
import os
from pathlib import Path
import signal
import subprocess
import tempfile

from ..models import ContractError, dumps


class TerraCLI:
    def __init__(self, binary, *, model, run_observer=None):
        self.binary = str(Path(binary).resolve())
        if not os.access(self.binary, os.X_OK) or model != 'gpt-6.1-terra':
            raise ContractError('verified Terra CLI binary and exact model required')
        self.model, self.run_observer = model, run_observer

    def __call__(self, request, *, timeout):
        with tempfile.TemporaryDirectory(prefix='harness-terra-') as directory:
            root = Path(directory)
            schema, output = root/'schema.json', root/'result.json'
            schema.write_text(dumps(request['schema']))
            command = [self.binary,'exec','--ignore-user-config','--ignore-rules','--ephemeral',
                       '--skip-git-repo-check','--sandbox','read-only','--model',self.model,
                       '--output-schema',str(schema),'--output-last-message',str(output),'--json',
                       '-c','approval_policy="never"','-c','web_search="disabled"',
                       '-c','model_reasoning_effort="low"','-c','project_doc_max_bytes=0']
            for feature in ('shell_tool','unified_exec','apps','multi_agent','memories',
                            'in_app_browser','browser_use','skill_search','shell_snapshot'):
                command += ['--disable',feature]
            command += ['--enable','skip_host_skill_discovery','-']
            prompt = request['prompt']+'\n仅分类以下 JSON 数据：\n'+dumps({
                'message':request['message'],'source':request['source'],'attachments':request['attachments']})
            proc = subprocess.Popen(command, cwd=root, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE, text=True, start_new_session=True)
            try:
                stdout, stderr = proc.communicate(prompt, timeout=timeout)
            except subprocess.TimeoutExpired:
                os.killpg(proc.pid, signal.SIGKILL)
                proc.communicate()
                if self.run_observer:
                    self.run_observer({'model':self.model,'status':'timeout','process_reaped':proc.poll() is not None})
                raise TimeoutError('terra_deadline') from None
            items = []
            for line in stdout.splitlines():
                try:
                    event = json.loads(line)
                except ValueError:
                    continue
                if isinstance(event.get('item'), dict):
                    items.append(event['item'].get('type'))
            forbidden = set(items)-{'agent_message','reasoning','error'}
            status = 'ok' if proc.returncode == 0 and output.is_file() and not forbidden else 'transport_error'
            if self.run_observer:
                self.run_observer({'model':self.model,'status':status,'item_types':items,
                                   'diagnostic_error_items':items.count('error'),
                                   'process_reaped':proc.poll() is not None,'exit_code':proc.returncode})
            if status != 'ok':
                raise ContractError('terra_transport_or_tool_contract_error')
            return json.loads(output.read_text())
