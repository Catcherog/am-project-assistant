"""Authenticated operator API and compact pending-review page.

The operator token is bound to the configured owner. No request body can choose
an actor. Worker credentials belong to the separate receiver service.
"""
from __future__ import annotations

import hmac
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import threading
from urllib.parse import parse_qs, unquote, urlsplit

from ..models import ContractError, Event, dumps
from .contracts import INTENTS, Policy


PAGE = '''<!doctype html><html lang="zh-CN"><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>AM · 任务与待审</title><style>
:root{color:#26362b;background:#f5f2e9;font:16px/1.6 system-ui}
body{max-width:1080px;margin:40px auto;padding:0 24px}h1{font-size:30px;margin-bottom:4px}
p{color:#657065}.bar{display:flex;gap:10px;flex-wrap:wrap;margin:24px 0}
input,select,button,textarea{font:inherit;border:1px solid #cbd3c7;border-radius:9px;padding:9px 12px}
button{background:#e3ebdf;color:#294c32;cursor:pointer}button:disabled{opacity:.45}
input{min-width:270px;background:white}article{background:#fffdf6;border:1px solid #dbe0d3;border-radius:16px;padding:20px;margin:14px 0}
.state{font-size:14px;color:#66735f}pre{white-space:pre-wrap;overflow-wrap:anywhere;font:inherit}
textarea{box-sizing:border-box;width:100%;background:white}small{color:#66735f}#notice{color:#755527}
</style><h1>AM · 任务与待审</h1><p>查看来源、补充信息、审阅修改，并确认需要执行的操作。</p>
<div class="bar"><input id="token" type="password" autocomplete="off" placeholder="操作员访问令牌">
<button id="refresh">读取任务</button><select id="filter"><option value="pending">待处理</option><option value="all">全部任务</option></select></div>
<div id="notice" role="status"></div><main id="tasks"></main><h2>待批准操作</h2><section id="proposals"></section>
<script>
const $=x=>document.getElementById(x),note=x=>$('notice').textContent=x;
async function api(path,body){const r=await fetch(path,{method:body?'POST':'GET',headers:{'Authorization':'Bearer '+$('token').value,'Content-Type':'application/json'},body:body?JSON.stringify(body):undefined});const v=await r.json();if(!r.ok)throw Error(v.error||r.status);return v}
function el(tag,text,cls){const n=document.createElement(tag);if(text!==undefined)n.textContent=text;if(cls)n.className=cls;return n}
function button(label,fn){const b=el('button',label);b.onclick=async()=>{b.disabled=true;try{await fn();await load()}catch(e){note(e.message)}finally{b.disabled=false}};return b}
async function load(){note('读取中…');try{const data=await api('/v1/tasks?pending='+($('filter').value==='pending'));$('tasks').replaceChildren();for(const t of data.tasks){const c=el('article');c.append(el('small',t.task_id),el('pre',t.request_text),el('div',[t.route_state,t.execution_state,t.delivery_state].join(' · '),'state'));if(t.reply_text)c.append(el('pre',t.reply_text));const q=el('textarea');q.placeholder='补充说明或明确任务类型';const s=el('select');for(const i of data.intents){const o=el('option',i);o.value=i;s.append(o)}const bar=el('div',undefined,'bar');if(!t.business_id){bar.append(s,button('按此类型继续',()=>api('/v1/review',{task_id:t.task_id,action:'resume',intent:s.value,supplement:q.value})),button('忽略',()=>api('/v1/review',{task_id:t.task_id,action:'ignore'})));if(t.route_state==='classification_error')bar.append(button('重试分类',()=>api('/v1/review',{task_id:t.task_id,action:'technical_retry'})));c.append(q)}else{bar.append(button('核对执行状态',()=>api('/v1/reconcile',{task_id:t.task_id})),button('请求停止',()=>api('/v1/cancel',{task_id:t.task_id})))}c.append(bar);$('tasks').append(c)}const p=await api('/v1/proposals');$('proposals').replaceChildren();for(const x of p.proposals){const c=el('article');c.append(el('small',x.proposal_id+' · v'+x.version+' · '+x.status));const q=el('textarea');q.rows=8;q.value=JSON.stringify(x.payload,null,2);c.append(q);if(x.status==='draft'){const bar=el('div',undefined,'bar');bar.append(button('保存修改',()=>api('/v1/proposals/'+x.proposal_id+'/revise',{expected_version:x.version,payload:JSON.parse(q.value)})),button('确认执行',()=>{if(q.value!==JSON.stringify(x.payload,null,2))throw Error('请先保存修改并重新审阅');return api('/v1/proposals/'+x.proposal_id+'/confirm',{version:x.version,digest:x.digest})}),button('不采用',()=>api('/v1/proposals/'+x.proposal_id+'/reject',{version:x.version,digest:x.digest})));c.append(bar)}$('proposals').append(c)}note('已更新')}catch(e){note(e.message)}}
$('refresh').onclick=load;$('filter').onchange=load;
</script></html>'''


def review_card(task):
    route = json.loads(task['route_json'])
    question = route.get('clarification_question') or '请补充执行所需信息，或选择继续处理的类型。'
    actions = [{'tag': 'button', 'text': {'tag': 'plain_text', 'content': '忽略'},
                'value': {'kind': 'harness_review', 'task_id': task['task_id'], 'action': 'ignore'}}]
    if task['route_state'] == 'classification_error':
        actions.insert(0, {'tag': 'button', 'text': {'tag': 'plain_text', 'content': '重试分类'},
                          'value': {'kind': 'harness_review', 'task_id': task['task_id'],
                                    'action': 'technical_retry'}})
    else:
        actions.insert(0, {'tag': 'select_static', 'placeholder': {'tag': 'plain_text', 'content': '选择任务类型'},
                          'options': [{'text': {'tag': 'plain_text', 'content': intent}, 'value': intent}
                                      for intent in INTENTS],
                          'value': {'kind': 'harness_review', 'task_id': task['task_id'], 'action': 'resume'}})
    return {'config': {'wide_screen_mode': True}, 'header': {'template': 'green',
            'title': {'tag': 'plain_text', 'content': 'AM · 待确认任务'}},
            'elements': [{'tag': 'div', 'text': {'tag': 'plain_text', 'content': question}},
                         {'tag': 'action', 'actions': actions}]}


class ControlAPI:
    def __init__(self, service, *, token, host='127.0.0.1', port=0,
                 proposals=None, executor=None, artifacts=None):
        if not isinstance(token, str) or len(token) < 24:
            raise ContractError('operator token must contain at least 24 characters')
        if host not in {'127.0.0.1', '::1', 'localhost'}:
            raise ContractError('operator API binds loopback; use an authenticated tunnel for remote access')
        self.service, self.harness = service, service.harness
        self.token, self.host, self.port = token, host, port
        self.proposals, self.executor, self.artifacts = proposals, executor, artifacts
        self.server = self.thread = None

    @property
    def url(self):
        return 'http://127.0.0.1:' + str(self.server.server_port if self.server else self.port)

    def _tasks(self, pending=False):
        rows = self.harness.store.rows('SELECT * FROM h1_tasks ORDER BY created DESC LIMIT 200')
        result = []
        for task in rows:
            if pending and ((task['route_state'] in {'resolved', 'ignored'}
                            and task['execution_state'] in {'succeeded', 'cancelled'}
                            and task['verification_state'] == 'sufficient')):
                continue
            row = {k: task[k] for k in ('task_id', 'business_id', 'revision', 'intent', 'route_state',
                'dispatch_state', 'execution_state', 'delivery_state', 'verification_state', 'last_error')}
            row['request_text'] = json.loads(task['request_json']).get('body', '')
            row['reply_text'] = json.loads(task['result_json'] or '{}').get('reply_text', '')
            result.append(row)
        return {'tasks': result, 'intents': list(INTENTS)}

    def handle(self, method, raw_path, data):
        parsed = urlsplit(raw_path)
        path, query = parsed.path.rstrip('/'), parse_qs(parsed.query)
        actor = self.harness.config.owner_id
        if path.startswith('/v1/assistant/'):
            if self.service.assistant is None:
                raise ContractError('project assistant is not bound')
            answer = self.service.assistant.handle(method, raw_path, data, actor=actor)
            if method == 'POST':
                self.service.notify()
            return answer
        if method == 'GET':
            if path == '/health':
                return self.service.health()
            if path == '/v1/tasks':
                return self._tasks(query.get('pending', ['false'])[0] == 'true')
            if path == '/v1/metrics':
                return self.service.metrics()
            if path == '/v1/proposals':
                return {'proposals': self.proposals.list() if self.proposals else []}
            if path.startswith('/v1/tasks/'):
                task = self.harness.task(unquote(path[len('/v1/tasks/'):]))
                return {'task': task, 'review_card': review_card(task)}
            if path == '/v1/information':
                task = self.harness.task(query.get('task_id', [''])[0])
                with self.harness.store.transaction() as db:
                    _, policy = self.harness._authorize(db, task['task_id'], task['generation'], require_running=False)
                    return self.harness.information.snapshot(policy, db=db)
        if method == 'POST':
            if path == '/v1/review':
                answer = self.service.ingress.review.apply(authenticated_actor=actor,
                    task_id=data['task_id'], action=data['action'],
                    intent=data.get('intent'), supplement=data.get('supplement', ''))
                self.service.notify()
                return answer or {'recorded': True}
            if path == '/v1/cancel':
                answer = self.harness.request_cancel(data['task_id'])
                self.service.receipts_once()
                return answer
            if path == '/v1/reconcile':
                self.service.receipts_once()
                self.service.delivery_once()
                return {'receiver_status': self.harness.query_execution(data['task_id']),
                        'task': self.harness.task(data['task_id'])}
            if path == '/v1/send-receipt':
                self.harness.record_send_receipt(task_id=data['task_id'], effect_id=data['effect_id'],
                                                 platform_id=data['platform_id'])
                self.service.delivery_once()
                return {'task': self.harness.task(data['task_id'])}
            if path in {'/v1/artifact-receipt', '/v1/artifacts/reconcile'}:
                if self.service.artifact_delivery is None:
                    raise ContractError('artifact sender is not bound')
                answer = self.service.artifact_delivery.reconcile(data['effect_id'],
                    file_key=data.get('file_key', ''), platform_id=data.get('platform_id', ''))
                self.service.delivery_once()
                return answer
            if path.startswith('/v1/legacy/'):
                ownership = self.service.ownership
                if ownership is None:
                    raise ContractError('legacy claim port is not bound')
                if path == '/v1/legacy/claim':
                    return {'claimed': ownership.claim(Event.from_dict(data['event']))}
                if path == '/v1/legacy/status':
                    return ownership.status(data['item_id']) or {'state': 'not_claimed'}
                if path == '/v1/legacy/finish':
                    return ownership.finish(data['item_id'], state=data['state'],
                                            result_ref=data.get('result_ref', ''))
            if path == '/v1/cutover/prepare':
                return self.harness.prepare_rollback(data['scope'])
            if path == '/v1/cutover/import':
                if self.service.ownership is None:
                    raise ContractError('legacy ownership port is not bound')
                return self.service.ownership.import_rollback(data['manifest'])
            prefix = '/v1/proposals/'
            if path.startswith(prefix) and self.proposals:
                proposal_id, action = path[len(prefix):].rsplit('/', 1)
                if action == 'revise':
                    return self.proposals.revise(proposal_id, expected_version=data['expected_version'],
                                                 actor=actor, payload=data['payload'])
                if action == 'reject':
                    return self.proposals.reject(proposal_id, version=data['version'], digest=data['digest'], actor=actor)
                if action == 'confirm':
                    if self.executor is None:
                        raise ContractError('business executor is not bound; proposal is retained')
                    return self.proposals.confirm(proposal_id, version=data['version'], digest=data['digest'],
                                                  actor=actor, executor=self.executor, harness=self.harness)
        raise ContractError('unknown API operation')

    def start(self):
        if self.server:
            return
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_):
                return

            def respond(self, code, content, media='application/json; charset=utf-8'):
                body = content if isinstance(content, bytes) else dumps(content).encode()
                self.send_response(code)
                self.send_header('Content-Type', media)
                self.send_header('Content-Length', str(len(body)))
                self.send_header('Cache-Control', 'no-store')
                self.send_header('X-Content-Type-Options', 'nosniff')
                self.end_headers()
                self.wfile.write(body)

            def request(self):
                if self.command == 'GET' and self.path in {'/', '/index.html'}:
                    return self.respond(200, PAGE.encode(), 'text/html; charset=utf-8')
                supplied = self.headers.get('Authorization', '')
                if not hmac.compare_digest(supplied, 'Bearer ' + outer.token):
                    return self.respond(401, {'error': 'operator authentication required'})
                try:
                    data = {}
                    if self.command == 'POST':
                        length = int(self.headers.get('Content-Length', '0'))
                        if length < 1 or length > 262144:
                            raise ContractError('bounded JSON request body required')
                        data = json.loads(self.rfile.read(length))
                        if not isinstance(data, dict):
                            raise ContractError('object request required')
                    if self.command == 'GET' and self.path.startswith('/v1/artifacts/'):
                        artifact_id = unquote(urlsplit(self.path).path[len('/v1/artifacts/'):])
                        if outer.artifacts is None:
                            raise ContractError('artifact store is not bound')
                        task = next((t for t in outer.harness.store.rows('SELECT * FROM h1_tasks WHERE result_json IS NOT NULL')
                                    if any(a.get('artifact_id') == artifact_id for a in
                                           json.loads(t['result_json']).get('artifacts', []))), None)
                        if task is None:
                            raise ContractError('artifact is not attached to a task result')
                        with outer.harness.store.transaction() as db:
                            outer.harness._authorize(db, task['task_id'], task['generation'], require_running=False)
                        meta = outer.artifacts.get(artifact_id, task_id=task['task_id'])
                        return self.respond(200, outer.artifacts.read(artifact_id, task_id=task['task_id']), meta.get('media_type', 'application/octet-stream'))
                    self.respond(200, outer.handle(self.command, self.path, data))
                except (ContractError, KeyError, ValueError, TypeError) as exc:
                    self.respond(400, {'error': str(exc)[:250]})
                except Exception as exc:
                    outer.service._error('control_api', exc)
                    self.respond(500, {'error': type(exc).__name__})

            do_GET = do_POST = request

        self.server = ThreadingHTTPServer((self.host, self.port), Handler)
        self.server.daemon_threads = True
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True, name='am-r4-control')
        self.thread.start()

    def stop(self):
        if self.server:
            self.server.shutdown()
            self.server.server_close()
            if self.thread:
                self.thread.join(2)
            self.server = self.thread = None
