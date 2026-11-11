import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from bridge import Bridge
from group_ledger import (GroupLedger, FeishuLedgerPort, LedgerError, TTL, SOURCE,
                          parse_rows, render_answer, select_projects)

VALUES = [
    ['项目／需求', '个人跟进优先级', '关键日期', '待办', '项目进展', '人员与职责', '企业优先级'],
    ['需求75:柔轮试验', 'P0', '预计2026.11.1', '确认收货', '2026.10.28\n样机预计到货，实际收货尚未核实。\n寿命试验完成后才可评估。', 'AR1测试甲', 'P1'],
    ['RA-2099-011\nDemoDrive20小型项目\n其他背景', '', '', '', '2026.10.27\n设计中，尚未批准。', '', ''],
    ['QI-2099-007\nDemoDrive80刹车项目', '', '', '', '2026.10.26\n测试中。', '', ''],
    ['RA-2099-010\nDemoDrive70FV4无刹车项目', '', '', '', '方案待确认。', '', ''],
]


class Port:
    app_id = 'cli_test'
    tenant_key = 'tenant'
    bot_open_id = 'ou_bot'

    def __init__(self):
        self.reads = self.guards = 0
        self.fail_guard_at = None
        self.fail_read = False
        self.values = VALUES

    def guard(self, chat, user):
        self.guards += 1
        if self.guards == self.fail_guard_at:
            raise LedgerError('MEMBER_LEFT')

    def read(self):
        self.reads += 1
        if self.fail_read:
            raise LedgerError('PLATFORM_READ_99991672')
        return parse_rows(self.values)


class LedgerTests(unittest.TestCase):
    def test_live_owner_actions_heading_preserves_current_row_values(self):
        values = [row[:] for row in VALUES]
        values[0][3] = '本人待办与跟进'
        self.assertEqual(parse_rows(values), parse_rows(VALUES))

    def test_duplicate_owner_actions_alias_is_not_silently_selected(self):
        values = [row[:] for row in VALUES]
        values[0].append('本人待办与跟进')
        for row in values[1:]:
            row.append('不同的待办')
        with self.assertRaisesRegex(LedgerError, 'LEDGER_HEADER_CHANGED'):
            parse_rows(values)

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.now = 2000000000
        self.sent = []
        self.port = Port()
        self.route = GroupLedger(self.tmp.name, self.port, lambda mid, text: self.sent.append((mid, text)), clock=lambda: self.now)

    def tearDown(self):
        self.route.close()
        self.tmp.cleanup()

    def event(self, mid='om_1', text='需求75进展如何', user='ou_member', chat='oc_one'):
        return {'header': {'app_id': 'cli_test', 'tenant_key': 'tenant', 'event_type': 'im.message.receive_v1'},
                'event': {'sender': {'sender_type': 'user', 'sender_id': {'open_id': user}, 'tenant_key': 'tenant'},
                          'message': {'message_id': mid, 'chat_id': chat, 'chat_type': 'group', 'message_type': 'text',
                                      'content': json.dumps({'text': '@_user_1 ' + text}),
                                      'mentions': [{'key': '@_user_1', 'id': {'open_id': 'ou_bot'}}],
                                      'create_time': str(self.now * 1000)}}}

    def ask(self, text='需求75进展如何', mid='om_1', **kw):
        self.assertTrue(self.route.ingest(self.event(mid, text, **kw)))
        self.route.step()
        return self.sent[-1][1] if self.sent else ''

    def test_verified_group_member_and_live_read_not_fixture(self):
        text = self.ask(user='ou_new_colleague', chat='oc_new_group')
        self.assertIn('实际收货尚未核实', text)
        self.assertIn('寿命试验完成后才可评估', text)
        self.assertIn(SOURCE, text)
        self.assertNotIn('60%', text)
        self.assertEqual(self.port.reads, 1)
        self.assertEqual(self.port.guards, 2)
        self.assertEqual(self.route.db.execute('SELECT question,state FROM ledger_inbox').fetchone()[:], ('', 'done'))

    def test_only_verified_mentions_and_bound_identity(self):
        mutations = [lambda e:e['header'].update(app_id='cli_other'),
                     lambda e:e['header'].update(tenant_key='other'),
                     lambda e:e['event']['sender'].update(sender_type='app'),
                     lambda e:e['event']['message'].update(chat_type='p2p'),
                     lambda e:e['event']['message'].update(mentions=[]),
                     lambda e:e['event']['message'].update(content=json.dumps({'text':'@AM221D221 需求75'})),
                     lambda e:e['event']['message'].update(create_time=str((self.now-TTL-1)*1000)),
                     lambda e:e['event']['message'].update(create_time=str((self.now+61)*1000)),
                     lambda e:e['event']['message'].update(chat_id='../../wrong')]
        for mutate in mutations:
            event=self.event();mutate(event)
            self.assertFalse(self.route.ingest(event))
        self.assertEqual(self.port.reads,0)
        self.assertFalse(self.route.step())

    def test_post_with_real_at(self):
        e=self.event()
        e['event']['message'].update(message_type='post',content=json.dumps({'title':'','content':[[{'tag':'at','user_id':'@_user_1'},{'tag':'text','text':'需求75进度'}]]}))
        self.assertTrue(self.route.ingest(e));self.route.step()
        self.assertIn('实际收货尚未核实', self.sent[0][1])

    def test_no_at_does_not_reach_poc_or_model(self):
        b=Bridge({'state_directory':self.tmp.name,'app_id':'cli_test','access_mode':'all_private_users'},
                 lambda *a:self.fail('unexpected send'), lambda *a:self.fail('model not allowed'),
                 poc=SimpleNamespace(ingest=lambda *a:self.fail('group must not enter PoC')), group_ledger=self.route)
        try:
            e=self.event();e['event']['message']['mentions']=[]
            self.assertFalse(b.ingest(e))
            self.assertTrue(b.ingest(self.event()))
            self.assertEqual(b.db.execute('SELECT count(*) FROM inbox').fetchone()[0],0)
        finally:b.db.close()

    def test_duplicate_event_and_unknown_delivery_never_resends(self):
        self.ask()
        self.assertFalse(self.route.ingest(self.event()))
        self.assertFalse(self.route.step())
        def fail(*_):raise TimeoutError()
        self.route.reply=fail
        self.ask(mid='om_2')
        self.assertEqual(self.route.db.execute("SELECT state FROM ledger_inbox WHERE mid='om_2'").fetchone()[0],'delivery_unknown')
        self.assertFalse(self.route.step())
        self.assertEqual(len(self.sent),1)

    def test_restart_does_not_resend_sending(self):
        self.route.ingest(self.event())
        with self.route.db:self.route.db.execute("UPDATE ledger_inbox SET state='sending'")
        reopened=GroupLedger(self.tmp.name,self.port,lambda *a:self.fail('must not resend'),clock=lambda:self.now)
        try:
            self.assertFalse(reopened.step())
            self.assertEqual(reopened.db.execute('SELECT state FROM ledger_inbox').fetchone()[0],'delivery_unknown')
        finally:reopened.close()

    def test_membership_checked_before_read_and_before_reply(self):
        self.port.fail_guard_at=1
        self.ask()
        self.assertEqual(self.port.reads,0);self.assertEqual(self.sent,[])
        self.port.fail_guard_at=3
        self.ask(mid='om_2')
        self.assertEqual(self.port.reads,1);self.assertEqual(self.sent,[])

    def test_read_failure_never_uses_previous_facts(self):
        self.ask()
        self.port.fail_read=True
        text=self.ask('下一步呢',mid='om_2')
        self.assertIn('未能读取最新',text)
        self.assertNotIn('样机预计',text)

    def test_follow_up_isolated_by_person_and_group(self):
        self.ask()
        text=self.ask('那谁负责？',mid='om_2')
        self.assertIn('AR1测试甲',text)
        for i,kw in enumerate([{'user':'ou_other'},{'chat':'oc_other'}],3):
            text=self.ask('那谁负责？',mid=f'om_{i}',**kw)
            self.assertNotIn('AR1测试甲',text)
            self.assertIn('请提供',text)

    def test_focus_absolute_expiry_not_extended_by_followup(self):
        self.ask()
        expiry=self.route.db.execute('SELECT expires FROM ledger_focus').fetchone()[0]
        self.now+=60;self.ask('下一步是什么',mid='om_2')
        self.assertEqual(self.route.db.execute('SELECT expires FROM ledger_focus').fetchone()[0],expiry)
        self.now=expiry+1;self.route.cleanup()
        self.assertEqual(self.route.db.execute('SELECT count(*) FROM ledger_focus').fetchone()[0],0)

    def test_fresh_source_every_question(self):
        first=self.ask()
        self.port.values=[r[:] for r in VALUES]
        self.port.values[1][4]='2026.10.29\n到货已确认，寿命试验尚未完成。'
        second=self.ask(mid='om_2')
        self.assertIn('实际收货尚未核实',first)
        self.assertIn('到货已确认',second)
        self.assertNotIn('实际收货尚未核实',second)
        self.assertEqual(self.port.reads,2)

    def test_exact_ids_unknown_ids_and_ambiguous_names(self):
        p=parse_rows(VALUES)
        self.assertEqual(select_projects('RA-2099-011进展',p)[0]['row'],3)
        self.assertEqual(select_projects('DemoDrive20项目现在怎么样',p)[0]['row'],3)
        self.assertEqual(select_projects('DemoDrive70进展',p)[0]['row'],5)
        self.assertEqual(select_projects('需求750进度',p),[])
        self.assertEqual(select_projects('RA-2099-013 DemoDrive20',p),[])
        self.assertEqual(len(select_projects('刹车项目进展',p)),2)
        text,_=render_answer('刹车项目进展',p)
        self.assertIn('匹配到多个',text)

    def test_header_mapping_and_unknown_format_fail_closed(self):
        shuffled=[[row[i] for i in (4,2,0,5,3,1,6)] for row in VALUES]
        self.assertEqual(parse_rows(shuffled)[0]['项目进展'],VALUES[1][4])
        bad=[row[:] for row in VALUES];bad[0][4]='旧版汇总'
        with self.assertRaises(LedgerError):parse_rows(bad)

    def test_current_information_heading_preserves_original_text(self):
        values=[row[:] for row in VALUES]
        values[0][4]='项目信息'
        projects=parse_rows(values)
        text,_=render_answer('需求75进展如何',projects)
        self.assertIn('项目信息（台账原文）',text)
        self.assertIn(VALUES[1][4],text)
        values[0].append('项目进展')
        with self.assertRaises(LedgerError):parse_rows(values)
        bad=[row[:] for row in VALUES];bad[1][4]={'url':'unrecognized'}
        with self.assertRaises(LedgerError):parse_rows(bad)

    def test_native_read_requests_are_fixed_gets(self):
        calls=[]
        class Client:
            def request(self,request):
                calls.append(request)
                if request.uri.endswith('/sheets/query'):
                    data={'sheets':[{'sheet_id':'REPLACE_WITH_SHEET_ID','grid_properties':{'row_count':5}}]}
                else:data={'valueRange':{'range':'REPLACE_WITH_SHEET_ID!A1:G5','values':VALUES}}
                return SimpleNamespace(raw=SimpleNamespace(content=json.dumps({'code':0,'data':data}).encode()))
        port=FeishuLedgerPort(Client(),app_id='cli_test',tenant_key='tenant',bot_open_id='ou_bot',department_root_id='od-demo')
        self.assertEqual(len(port.read()),4)
        self.assertEqual([r.http_method.name for r in calls],['GET','GET'])
        self.assertTrue(all('REPLACE_WITH_SPREADSHEET_TOKEN' in r.uri for r in calls))

    def test_sheet_text_cannot_create_a_native_mention(self):
        self.port.values=[r[:] for r in VALUES]
        self.port.values[1][4]='待核实 <at user_id="all">所有人</at>'
        answer=self.ask()
        self.assertNotIn('<at',answer)
        self.assertIn('待核实',answer)


class DepartmentTests(unittest.TestCase):
    def setUp(self):
        self.port=FeishuLedgerPort(None,app_id='cli_test',tenant_key='tenant',bot_open_id='ou_bot',department_root_id='od-demo',disclosure={'oc_group':['*']})
        self.profile={'open_id':'ou_member','status':{'is_activated':True},'department_ids':['od-child']}
        self.pages={None:{'items':[{'open_department_id':'od-child'}],'has_more':False}}
        self.calls=[]
        def get(uri,query=None):
            self.calls.append(uri)
            if '/users/' in uri:return {'user':self.profile}
            if '/departments/' in uri:
                self.assertEqual(uri,'/open-apis/contact/v3/departments/od-demo/children')
                self.assertEqual(query['fetch_child'],'true')
                value=self.pages.get(query.get('page_token'))
                if value is None:raise LedgerError('PLATFORM_READ_40004')
                return value
            if uri.endswith('/members/is_in_chat'):return {'is_in_chat':True}
            if uri.endswith('/members'):return {'items':[{'member_id':'ou_member','member_id_type':'open_id'}]}
            return {'tenant_key':'tenant','chat_status':'normal','chat_mode':'group'}
        self.port.get=get

    def test_direct_demo_and_descendant(self):
        self.port.guard('oc_group','ou_member')
        self.assertTrue(any('/departments/od-demo/children' in x for x in self.calls))
        self.profile['department_ids']=['od-demo']
        self.port.authorize_user('ou_member')

    def test_other_department_and_lookalike_name_rejected(self):
        self.pages[None]['items'][0]['name']='OTHER_DEMO示例协作部门'
        self.profile['department_ids']=['od-th5']
        with self.assertRaisesRegex(LedgerError,'DEMO_MEMBERSHIP_UNVERIFIED'):
            self.port.guard('oc_group','ou_member')

    def test_membership_refreshed_and_employment_checked(self):
        self.port.authorize_user('ou_member')
        self.profile['department_ids']=['od-th5']
        with self.assertRaises(LedgerError):self.port.authorize_user('ou_member')
        self.profile['department_ids']=['od-demo']
        for flag in ('is_resigned','is_frozen','is_exited'):
            self.profile['status']={'is_activated':True,flag:True}
            with self.assertRaisesRegex(LedgerError,'QUESTIONER_NOT_ACTIVE'):
                self.port.authorize_user('ou_member')

    def test_missing_or_inaccessible_ancestry_never_grants(self):
        for departments in ([],['od-unknown'],None,['0']):
            self.profile['department_ids']=departments
            with self.assertRaises(LedgerError):self.port.authorize_user('ou_member')

    def test_one_verified_branch_of_multiple_departments(self):
        self.profile['department_ids']=['od-unknown','od-child']
        self.port.authorize_user('ou_member')

    def test_pagination_and_incomplete_subtree(self):
        self.pages={None:{'items':[],'has_more':True,'page_token':'next'},
                    'next':{'items':[{'open_department_id':'od-child'}],'has_more':False}}
        self.port.authorize_user('ou_member')
        self.pages['next']={'items':[],'has_more':True,'page_token':'next'}
        with self.assertRaises(LedgerError):self.port.authorize_user('ou_member')
        self.pages={None:{'items':[{'open_department_id':'od-child','status':{'is_deleted':True}}]}}
        with self.assertRaises(LedgerError):self.port.authorize_user('ou_member')

    def test_missing_root_fails_closed(self):
        with self.assertRaisesRegex(LedgerError,'DEMO_DEPARTMENT_NOT_CONFIGURED'):
            FeishuLedgerPort(None,app_id='cli_test',tenant_key='tenant',bot_open_id='ou_bot',department_root_id=None)

    def test_non_demo_and_department_change_block_source_or_delivery(self):
        with tempfile.TemporaryDirectory() as directory:
            sent=[];reads=[]
            event={'header':{'app_id':'cli_test','tenant_key':'tenant','event_type':'im.message.receive_v1'},
                   'event':{'sender':{'sender_type':'user','sender_id':{'open_id':'ou_member'}},
                            'message':{'chat_type':'group','chat_id':'oc_group','message_id':'om_demotest','message_type':'text',
                                       'create_time':'2000000000000','content':json.dumps({'text':'@_user_1 需求75'}),
                                       'mentions':[{'key':'@_user_1','id':{'open_id':'ou_bot'}}]}}}
            def read():
                reads.append(True)
                self.profile['department_ids']=['od-th5']
                return parse_rows(VALUES)
            self.port.read=read
            route=GroupLedger(directory,self.port,lambda *args:sent.append(args),clock=lambda:2000000000)
            try:
                self.profile['department_ids']=['od-th5']
                self.assertTrue(route.ingest(event));route.step()
                self.assertEqual(reads,[]);self.assertEqual(sent,[])
                self.profile['department_ids']=['od-child']
                event['event']['message']['message_id']='om_changed'
                self.assertTrue(route.ingest(event));route.step()
                self.assertEqual(reads,[True]);self.assertEqual(sent,[])
            finally:route.close()


if __name__=='__main__':unittest.main()
