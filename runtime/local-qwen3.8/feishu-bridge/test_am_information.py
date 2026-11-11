"""Integration boundaries: shared source, identity, crash/retry and model priority."""
from copy import deepcopy
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace as NS
import json
import sqlite3
import tempfile
import threading
import unittest
from unittest.mock import Mock

from am_info_sync.store import Store
from am_info_sync.models import Source, Observation, Principal, ScopeViolation
from am_info_sync.runtime import InformationService
from am_info_sync.catalog import EntityCatalog
from am_info_sync.extraction import ExtractionError
from am_info_sync.adapters.messages import MessageAdapter
from am_information import AMInformation, SharedLedgerReader
from info_readers import ExistingSDKReader, ExistingTasksReader, ExistingMeegleReader
from bridge import Bridge


class InformationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.store = Store(Path(self.temp.name)/'sync.sqlite3')
        self.service = InformationService(self.store)

    def tearDown(self):
        self.store.close(); self.temp.cleanup()

    def source(self, **kw):
        return Source('s', 'im_chat', 'chat1', 'tenant', 'owner', enabled=True, **kw)

    def test_capture_repeats_still_route_and_failure_prevents_route(self):
        self.store.register_source(self.source())
        capture = self.service.bridge('app', 'tenant')
        e = {'header': {'app_id':'app','tenant_key':'tenant','event_type':'im.message.receive_v1','event_id':'e'},
             'event': {'message': {'chat_id':'chat1','message_id':'m','create_time':str(self.store.clock()),
                                   'message_type':'text','content':json.dumps({'text':'sample'})}}}
        calls=[]
        for _ in range(2):capture.capture_then_route(e, lambda:calls.append(1))
        self.assertEqual(calls,[1,1])
        self.assertEqual(self.store.conn.execute('SELECT count(*) FROM inbox').fetchone()[0],1)
        capture.store.accept_event=Mock(side_effect=sqlite3.OperationalError('disk full'))
        with self.assertRaises(sqlite3.OperationalError):capture.capture_then_route(e, lambda:calls.append(2))
        self.assertEqual(calls,[1,1])

    def test_bridge_capture_precedes_all_business_routes(self):
        bridge = Bridge({'state_directory':self.temp.name,'app_id':'app','access_mode':'all_private_users'},lambda *a:None)
        calls=[]
        bridge.information=NS(capture_verified=lambda p:calls.append('capture'))
        bridge.am_self=NS(ingest=lambda p:(calls.append('route') or True,True))
        self.assertTrue(bridge.ingest({}));self.assertEqual(calls,['capture','route'])
        bridge.information.capture_verified=Mock(side_effect=OSError('disk'))
        with self.assertRaises(OSError):bridge.ingest({})
        self.assertEqual(calls,['capture','route']);bridge.db.close()

    def host(self):
        source = Source('demo-ledger','ledger_snapshot','fixed/sheet','tenant','owner',
            readers=('group:demo-query',),output_chats=('public',),enabled=True,settings={
                'host_managed':True,'subject_ids':[], 'fact_mappings':{'ledger_record':{
                    'subject_path':['subject_id'],'value_path':['record'],'quote_path':['quote']}}})
        self.store.register_source(source)
        raw = [{'key':'P1','title':'Project 1','source_revision':1,'identity_status':'verified',
                '项目进展':'预计周五完成；尚未验收','关键日期':'原计划','待办':'跟进','人员与职责':'AR1 示例'}]
        read=Mock(side_effect=lambda:deepcopy(raw))
        ledger=NS(port=NS(read=read),mutex=threading.RLock(),
                  catalog=NS(sync=lambda rows,**kw:rows))
        host=NS(store=self.store,service=self.service,catalog=EntityCatalog(self.store),
                am=NS(ledger=ledger),ledger_url='https://example.invalid/ledger',require_ledger_available=lambda:None)
        return host,raw,read

    def test_shared_ledger_revision_dedupe_and_current_query(self):
        host,raw,read=self.host();hub=SharedLedgerReader(host)
        first=hub.read();second=hub.read(force=True)
        q=self.service.query('P1',Principal('owner','tenant'))
        self.assertEqual(len(q['facts']),1)
        self.assertEqual(q['facts'][0]['field'],'ledger_record')
        self.assertIn('尚未验收',q['facts'][0]['value']['fields']['项目进展'])
        self.assertIsNone(q['coverage']['sources'][0]['collected_through_ms'])
        self.assertEqual(self.store.conn.execute('SELECT count(*) FROM observations').fetchone()[0],1)
        raw[0]['source_revision']=2;raw[0]['项目进展']='已更正：仍待验证'
        hub.read(force=True)
        q=self.service.query('P1',Principal('employee','tenant',('demo-query',),'public'))
        self.assertEqual(len(q['facts']),1)
        self.assertIn('仍待验证',q['facts'][0]['value']['fields']['项目进展'])
        self.assertEqual(self.service.query('P1',Principal('outsider','tenant'))['facts'],[])
        self.assertEqual(self.service.query('P1',Principal('owner','tenant',(),'other'))['facts'],[])

    def test_ledger_no_ghost_after_project_removed(self):
        host,raw,read=self.host();hub=SharedLedgerReader(host);hub.read()
        raw[0]=dict(raw[0],key='P2',source_revision=2,title='Project 2')
        hub.read(force=True)
        self.assertEqual(self.service.query('P1',Principal('owner','tenant'))['facts'],[])

    def test_shared_ledger_failed_read_keeps_old_but_stale(self):
        host,raw,read=self.host();hub=SharedLedgerReader(host);hub.read()
        read.side_effect=ConnectionError('offline')
        with self.assertRaises(ConnectionError):hub.read(force=True)
        q=self.service.query('P1',Principal('owner','tenant'))
        self.assertTrue(q['facts'][0]['stale'])
        read.side_effect=lambda:deepcopy(raw);hub.read(force=True)
        self.assertFalse(self.service.query('P1',Principal('owner','tenant'))['facts'][0]['stale'])

    def test_host_managed_source_not_queued_twice(self):
        host,_,_=self.host()
        self.assertEqual(self.service.scheduler.enqueue_due(),0)

    def test_busy_model_does_not_exhaust_failures(self):
        self.store.register_source(self.source(settings={'subject_id':'P1'}))
        self.store.record(Observation('s','m','v',self.store.clock(),text='sample'))
        self.service.extraction_worker.extractor=Mock(side_effect=ExtractionError('host_model_deferred'))
        for _ in range(12):
            self.store.conn.execute('UPDATE jobs SET available_at_ms=0')
            result=self.service.analyze_once()
            self.assertEqual(result['status'],'deferred')
        row=self.store.conn.execute("SELECT state,attempts FROM jobs WHERE kind='extract'").fetchone()
        self.assertEqual(tuple(row),('pending',0))

    def test_sdk_cannot_change_identity_or_call_mutation(self):
        sdk=Mock();reader=ExistingSDKReader(sdk,app_id='app',tenant_id='tenant')
        for src,path in [(replace(self.source(),tenant_id='other'),'/open-apis/im/v1/messages'),
                         (self.source(),'/open-apis/auth/v3/tenant_access_token/internal'),
                         (replace(self.source(),settings={'read_identity':'user'}),'/open-apis/im/v1/messages')]:
            with self.assertRaises(ScopeViolation):reader.get(src,path)
        sdk.request.assert_not_called()

    def test_tasks_requires_explicit_guid_and_preserves_status(self):
        reader=Mock();adapter=ExistingTasksReader(reader)
        src=Source('task','task_snapshot','guid','tenant','owner',enabled=True,
            settings={'allowed_object_ids':['guid'],'subject_id':'T1'})
        reader.get.return_value={'task':{'guid':'guid','summary':'Task','completed_at':'0','updated_at':'1000'}}
        page=adapter.fetch_page(src,0,2000)
        self.assertEqual(page.observations[0].payload['record']['status'],'open')
        self.assertEqual(page.observations[0].payload['comments'],'not_read')
        self.assertFalse(page.time_coverage)
        with self.assertRaises(ScopeViolation):adapter.fetch_page(replace(src,remote_id='other'),0,2000)

    def test_meegle_bound_targets_only_and_no_write(self):
        port=Mock();port.settings={'targets':{'alias':{'deliverable':'D1'}}}
        adapter=ExistingMeegleReader(port,lambda:{'user':'owner'})
        src=Source('proj','project_snapshot','D1','tenant','owner',enabled=True,settings={'target_alias':'alias'})
        port._state.return_value=({'space':'S','project':'P','node':'N'}, {'business_status':{'state':'doing'},'professional_review':{'value':'reviewed'},
            'project_id':'P','node_id':'N','revision':'v','url':'https://example.invalid'}, {}, [{'name':'a'}])
        port._run.return_value={'list':[{'basic':{'node_key':'N','status':'doing'},
            'schedule':{'estimate_finish_time':2000,'actual_finish_time':None}}]}
        obs=adapter.fetch_page(src,0,2000).observations[0]
        self.assertEqual(obs.payload['record']['professional_review'],{'value':'reviewed'})
        self.assertEqual(obs.payload['record']['attachment_content'],'pending')
        self.assertEqual(obs.payload['record']['node_plan']['estimate_finish_time'],2000)
        self.assertIsNone(obs.payload['record']['node_actual']['actual_finish_time'])
        port.commit.assert_not_called()
        with self.assertRaises(ScopeViolation):adapter.fetch_page(replace(src,remote_id='other'),0,2000)

    def test_private_facts_do_not_change_public_counts(self):
        host,_,_=self.host();SharedLedgerReader(host).read()
        before=self.service.query('P1',Principal('employee','tenant',('demo-query',),'public'))
        self.store.register_source(self.source(settings={'subject_id':'P1'}))
        obs=Observation('s','m','v',self.store.clock(),text='private forecast')
        self.store.record(obs)
        self.service.facts.process(obs,[{'source_id':'s','object_id':'m','version':'v','subject_id':'P1',
            'field':'forecast','value':'private','quote':'private forecast','assertion_type':'forecast'}])
        after=self.service.query('P1',Principal('employee','tenant',('demo-query',),'public'))
        self.assertEqual(before['facts'],after['facts'])
        self.assertEqual(before['coverage'],after['coverage'])
        self.assertEqual(before['conflicts'],after['conflicts'])

    def test_history_never_promotes_bot_answers_to_evidence(self):
        source=self.source(settings={'allowed_sender_ids':['owner']})
        def item(mid,who,kind):
            return {'message_id':mid,'chat_id':'chat1','create_time':'1000','update_time':'1000',
                'msg_type':'text','body':{'content':json.dumps({'text':'sample'})},
                'sender':{'id':who,'id_type':'open_id','sender_type':kind}}
        client=Mock();client.get.return_value={'items':[item('m1','owner','user'),item('m2','bot','app'),item('m3','other','user')], 'has_more':False}
        page=MessageAdapter(client).fetch_page(source,0,2000)
        self.assertEqual([o.object_id for o in page.observations],['m1'])

    def test_old_query_cache_obeys_revoked_source(self):
        from am_query import QuerySnapshots
        ledger=NS(port=NS(information_source_check=Mock(side_effect=ScopeViolation('revoked'))),clock=lambda:1000)
        snapshots=QuerySnapshots(ledger)
        try:
            snapshots.latest='v';snapshots.versions['v']={'rows':[{'secret':True}]}
            with self.assertRaises(ScopeViolation):snapshots.get()
        finally:snapshots.close()


if __name__ == '__main__':unittest.main()
