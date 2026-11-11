import json
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import time
import unittest

from bridge import Bridge
from group_ai import QwenGroupAssistant, GroupAIError, Reply, strict_json, validate_intent, validate_ranking, validate_grounded
from group_ledger import GroupLedger, parse_rows, TTL
from test_group_ledger import VALUES, Port
import test_group_ledger


def intent(route='project', reference='需求75', fields=None, previous=False):
    return {'route':route,'reference':reference,'fields':fields or ['progress'],'use_previous':previous}


def grounded(text='样机预计到货，实际收货尚未核实。寿命试验完成后才可评估。'):
    return {'paragraphs':[{'text':text,'evidence':['s2','s3']}]}


class Provider:
    def __init__(self, answers):self.answers=list(answers);self.calls=[]
    def complete(self,prompt,**options):
        self.calls.append((prompt,options))
        item=self.answers.pop(0)
        if isinstance(item,Exception):raise item
        return SimpleNamespace(text=json.dumps(item,ensure_ascii=False) if isinstance(item,dict) else item,elapsed=.01)


class QwenGroupTests(unittest.TestCase):
    def setUp(self):
        self.tmp=TemporaryDirectory();self.pauses=[]
    def tearDown(self):self.tmp.cleanup()
    def brain(self, answers):
        self.provider=Provider(answers)
        return QwenGroupAssistant(self.provider,self.tmp.name,pause=self.pauses.append)

    def test_physics_is_general_and_never_reads_ledger(self):
        b=self.brain([intent('general','',[]),'薛定谔方程描述量子态随时间的演化。'])
        r=b.reply('介绍一下薛定谔方程',[],None,lambda:self.fail('no ledger read'))
        self.assertEqual(r.mode,'general');self.assertIsNone(r.project)
        self.assertIn('量子态',r.text);self.assertEqual(len(self.provider.calls),2)
        self.assertNotIn('项目清单',r.text)

    def test_project_is_fresh_grounded_and_qwen_written(self):
        b=self.brain([intent(),grounded()]);reads=[]
        def load():reads.append(1);return parse_rows(VALUES)
        r=b.reply('需求75现在进展怎么样',[],None,load)
        self.assertEqual(reads,[1]);self.assertEqual(r.mode,'project')
        self.assertIn('实际收货尚未核实',r.text)
        self.assertEqual(b.last_grounding,'source_blocks')
        self.assertIn('台账原文）：',r.text);self.assertEqual(len(self.provider.calls),0)
        self.assertIn('[查看来源原文](https://',r.text)

    def test_read_time_is_fetch_time_not_generation_finish(self):
        from datetime import datetime
        from zoneinfo import ZoneInfo
        b=self.brain([intent(),grounded()]);clock=[2000000000.0];b.clock=lambda:clock[0]
        complete=self.provider.complete
        def wrapped(*args,**kwargs):
            result=complete(*args,**kwargs)
            if len(self.provider.calls)==2:clock[0]+=120
            return result
        self.provider.complete=wrapped
        result=b.reply('需求75进度',[],None,lambda:parse_rows(VALUES))
        stamp=datetime.fromtimestamp(2000000000,ZoneInfo('Asia/Shanghai'))
        self.assertIn(f'读取：{stamp:%Y-%m-%d %H:%M}',result.text)
        self.assertEqual(b.last_source_read_at,2000000000)

    def test_rerank_uses_titles_only_and_selects_known_id(self):
        vals=[r[:] for r in VALUES];vals[2][0]='RA-2099-012\nDemoDrive Digital Twin开发项目'
        p=parse_rows(vals)[1]
        ans={'paragraphs':[{'text':'目前设计中，尚未批准。','evidence':['s2']}]}
        b=self.brain([intent(reference='示例平台'),{'match':'one','ids':[2]},ans])
        r=b.reply('示例平台最近进展怎么样',[],None,lambda:parse_rows(vals))
        self.assertEqual(r.project,p['key'])
        self.assertEqual(b.counts['rerank'],1)
        self.assertNotIn(VALUES[1][4],self.provider.calls[1][0])

    def test_unknown_id_does_not_fall_back_to_catalog_or_other_project(self):
        b=self.brain([intent(reference='需求999')])
        r=b.reply('需求999进度',[],None,lambda:parse_rows(VALUES))
        self.assertIn('未找到编号：需求999',r.text);self.assertNotIn('DemoDrive20',r.text)
        self.assertEqual(len(self.provider.calls),0)

    def test_ambiguous_ranking_asks_short_question(self):
        b=self.brain([intent(reference='刹车'),{'match':'ambiguous','ids':[1,2]}])
        r=b.reply('刹车项目进度如何',[],None,lambda:parse_rows(VALUES))
        self.assertIn('哪一个',r.text);self.assertEqual(r.mode,'clarify')
        self.assertNotIn('需求75',r.text)

    def test_generated_fabrication_is_not_used_by_extract_path(self):
        b=self.brain([grounded(text='样机已到货，全部完成。'),grounded()])
        r=b.project_answer('柔轮试验进度',intent(),parse_rows(VALUES)[0],time.monotonic()+120)
        self.assertNotIn('全部完成',r.text);self.assertIn('尚未核实',r.text)
        self.assertEqual(len(self.provider.calls),0)

    def test_extract_path_does_not_invent_model_claims(self):
        b=self.brain([grounded(text='已完成999台。'),grounded(text='已完成999台。')])
        r=b.project_answer('柔轮试验进度',intent(),parse_rows(VALUES)[0],time.monotonic()+120)
        self.assertIn('查看来源原文',r.text);self.assertNotIn('999台',r.text)
        self.assertEqual(len(self.provider.calls),0)
        self.assertEqual(b.last_grounding,'source_blocks')

    def test_old_project_answer_is_not_used_as_source_or_general_history(self):
        history=[{'question':'需求75进度','answer':'旧事实：采购已经完成。','mode':'project','expires':time.time()+100}]
        b=self.brain([intent('general','薛定谔方程'),'这是量子力学方程。'])
        b.reply('介绍薛定谔方程',history,'old_project',lambda:self.fail('no ledger'))
        self.assertNotIn('旧事实',self.provider.calls[1][0])

    def test_unknown_model_stop_pauses_shared_model_lane(self):
        b=self.brain([RuntimeError('STOP_UNKNOWN')])
        with self.assertRaises(GroupAIError):b.reply('你好',[],None,lambda:[])
        self.assertEqual(self.pauses,['GROUP_STOP_UNKNOWN'])

    def test_restart_inflight_does_not_assume_model_stopped(self):
        Path(self.tmp.name,'group-ai-status.json').write_text('{"phase":"in_flight","total_model_calls":2}')
        self.brain([])
        self.assertEqual(self.pauses,['GROUP_MODEL_STOP_UNKNOWN'])

    def test_schema_and_evidence_reject_untrusted_output(self):
        with self.assertRaises(GroupAIError):strict_json('{"route":"general","route":"project"}')
        wrong=intent();wrong['tool']='shell'
        with self.assertRaises(GroupAIError):validate_intent(wrong)
        for value in ({'match':'one','ids':[999]},{'match':'one','ids':[True]},{'match':'none','ids':[1]}):
            with self.assertRaises(GroupAIError):validate_ranking(value,parse_rows(VALUES))
        invented=grounded();invented['paragraphs'][0]['evidence']=['s999']
        for value in (grounded(text='已批准。'),grounded(text='负责人AR999王某。'),invented,
                      grounded(text='样机到货，寿命试验已经完成。')):
            with self.assertRaises(GroupAIError):validate_grounded(value,{'progress':VALUES[1][4]},'需求75')


class NativeTransportTests(unittest.TestCase):
    def client(self,result):
        from fs_agent.local_api import LocalQwenClient
        client=LocalQwenClient('http://127.0.0.1:11434/v1','test_model','0'*64)
        self.requests=[]
        def request(path,body,**kw):self.requests.append((path,body));return result
        client.request=request
        return client
    def response(self):
        return {'model':'test_model','message':{'role':'assistant','content':'{"route":"general"}'},'done':True,'done_reason':'stop','eval_count':5}
    def test_native_json_and_no_thinking_or_tools(self):
        client=self.client(self.response());schema={'type':'object'}
        client.complete('测试',native_chat=True,json_schema=schema,max_tokens=256)
        path,body=self.requests[0]
        self.assertEqual(path,'/api/chat');self.assertIs(body['think'],False)
        self.assertEqual(body['tools'],[]);self.assertEqual(body['format'],schema)
        self.assertEqual(body['options']['num_ctx'],16384)
    def test_native_rejects_tool_thinking_wrong_model_and_truncation(self):
        from fs_agent.config import BoundaryError
        for change in ('tool','thinking','model','length'):
            value=self.response()
            if change=='tool':value['message']['tool_calls']=[{'function':'anything'}]
            elif change=='thinking':value['message']['thinking']='unexpected reasoning'
            elif change=='model':value['model']='other'
            else:value['done_reason']='length'
            with self.assertRaises(BoundaryError):self.client(value).complete('测试',native_chat=True)


class ContextTests(unittest.TestCase):
    def setUp(self):
        self.tmp=TemporaryDirectory();self.now=2000000000;self.sent=[];self.port=Port()
        self.ai=SimpleNamespace(total_calls=0,last_grounding=None,paused=lambda:False)
        self.contexts=[]
        def reply(question,history,previous,load_projects,**kw):
            self.contexts.append((question,history,previous));self.ai.total_calls+=2
            return Reply('通用回答：'+question,mode='general')
        self.ai.reply=reply
        self.route=GroupLedger(self.tmp.name,self.port,lambda *a:self.sent.append(a),clock=lambda:self.now,assistant=self.ai)
    def tearDown(self):self.route.close();self.tmp.cleanup()
    def event(self,mid='om_1',text='介绍薛定谔方程',user='ou_member',chat='oc_one'):
        return test_group_ledger.LedgerTests.event(self,mid,text,user,chat)
    def ask(self,mid='om_1',**kw):
        self.assertTrue(self.route.ingest(self.event(mid=mid,**kw)));self.route.step()

    def test_authentication_precedes_all_model_calls(self):
        self.port.fail_guard_at=1;self.ask()
        self.assertEqual(self.contexts,[]);self.assertEqual(self.sent,[])

    def test_history_scope_and_absolute_ttl(self):
        self.ask();self.now+=30;self.ask('om_2',text='它有什么作用')
        self.assertEqual(len(self.contexts[-1][1]),1)
        self.assertEqual(self.port.reads,0)
        self.ask('om_3',user='ou_other');self.assertEqual(self.contexts[-1][1],[])
        self.ask('om_4',chat='oc_other');self.assertEqual(self.contexts[-1][1],[])
        expires=[r[0] for r in self.route.db.execute("SELECT expires FROM group_turns WHERE chat='oc_one' AND user='ou_member'")]
        self.assertEqual(expires,[2000000000+TTL,2000000000+TTL])
        self.now=2000000000+TTL+1;self.route.cleanup()
        self.assertEqual(self.route.history('oc_one','ou_member'),[])

    def test_project_facts_are_reloaded_and_not_a_memory_snapshot(self):
        def reply(question,history,previous,load_projects,**kw):
            self.ai.total_calls+=2;projects=load_projects()
            return Reply(projects[0]['项目进展'],projects[0]['key'],'project')
        self.ai.reply=reply;self.ask()
        self.port.values=[r[:] for r in VALUES];self.port.values[1][4]='新的台账事实'
        self.ask('om_2')
        self.assertEqual(self.port.reads,2);self.assertEqual(self.sent[-1][1],'新的台账事实')

    def test_general_knowledge_clears_project_focus(self):
        with self.route.db:self.route.db.execute("INSERT INTO ledger_focus VALUES('oc_one','ou_member','old',?)",(self.now+TTL,))
        self.ask()
        self.assertEqual(self.route.db.execute('SELECT count(*) FROM ledger_focus').fetchone()[0],0)

    def test_shared_model_lane_waits_for_document_work(self):
        busy=[True];calls=[]
        poc=SimpleNamespace(step=lambda:False,model_busy=lambda:busy[0],store=SimpleNamespace(meta=lambda key:None))
        route=SimpleNamespace(assistant=object(),step=lambda:calls.append('group') or True)
        b=Bridge({'app_id':'cli_test','state_directory':self.tmp.name,'access_mode':'all_private_users'},lambda *a:None,poc=poc,group_ledger=route)
        try:
            b.process_cycle();self.assertEqual(calls,[])
            busy[0]=False;b.process_cycle();self.assertEqual(calls,['group'])
        finally:b.db.close()


if __name__=='__main__':unittest.main()
