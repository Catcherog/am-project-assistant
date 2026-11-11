import json
import tempfile
from types import SimpleNamespace as NS
import unittest
from unittest.mock import Mock
from am_info_sync.models import Source, Principal, Observation, ScopeViolation, PermissionDenied
from am_info_sync.store import Store
from am_info_sync.runtime import InformationService
from info_sources import TasksCollection, ApprovalInstance, PinnedUserReader, record_observation, MeegleProject, mql_records, Discovery
from info_extraction import BoundedCoordinatorExtractor

class SourceTests(unittest.TestCase):
    def setUp(self):
        self.store=Store(':memory:');self.service=InformationService(self.store)
    def tearDown(self):self.store.close()
    def discovery(self):
        value=Discovery.__new__(Discovery)
        value.h=NS(store=self.store,start_ms=0,
            am=NS(owner='owner',g=NS(config=NS(tenant_key='tenant'))),
            _register=self.store.register_source)
        return value

    def test_repeated_docx_discovery_keeps_policy_and_observation_identity(self):
        from am_info_sync.document_policy import document_policy
        discovery=self.discovery()
        before=discovery.source('docx:r','docx','r',['subject','existing'],
                                file_name='Registered title',url='https://example.invalid/docx/r#one')
        observation=lambda source:Observation(source.source_id,'r','version',100,
            text='Unchanged evidence',payload={'time_basis':'observed_at'},url=source.settings.get('url'))
        self.store.record(observation(before))
        after=discovery.source('docx:r','docx','r',['other'],
                               file_name='Another backlink',url='https://example.invalid/docx/r#two')
        self.assertEqual(document_policy(before,'subject'),document_policy(after,'subject'))
        self.assertEqual(set(after.settings['subject_ids']),{'subject','existing','other'})
        self.store.record(observation(after))  # Must not raise IntegrityConflict.
        self.assertEqual(self.store.conn.execute('SELECT count(*) FROM observations').fetchone()[0],1)

    def test_docx_rediscovery_without_labels_does_not_remove_registered_citation(self):
        discovery=self.discovery()
        before=discovery.source('docx:r','docx','r',['subject'],file_name='Title',url='https://example.invalid/docx/r')
        after=discovery.source('docx:r','docx','r',['subject'])
        self.assertEqual(after.settings,before.settings)

    def test_docx_initial_absent_labels_stay_absent_on_new_backlink(self):
        discovery=self.discovery()
        before=discovery.source('docx:r','docx','r',['subject'])
        after=discovery.source('docx:r','docx','r',['subject'],file_name='Title',url='https://example.invalid/docx/r')
        self.assertEqual(after.settings,before.settings)

    def test_docx_membership_representation_change_keeps_existing_fence(self):
        from am_info_sync.document_policy import document_policy
        discovery=self.discovery()
        before=discovery.source('docx:r','docx','r',['subject'],file_name='Title')
        after=discovery.source('docx:r','docx','r',['other'],file_name='Other label')
        self.assertNotEqual(document_policy(before,'subject'),document_policy(after,'subject'))

    def test_file_parser_filename_and_document_read_identity_are_not_frozen(self):
        discovery=self.discovery()
        discovery.source('file:r','file_snapshot','r',['subject'],file_name='input.docx')
        after=discovery.source('file:r','file_snapshot','r',['subject'],file_name='input.pdf')
        self.assertEqual(after.settings['file_name'],'input.pdf')
        discovery.source('docx:r','docx','r',['subject'],file_name='Title')
        with self.assertRaises(ScopeViolation):
            discovery.source('docx:r','docx','other',['subject'],file_name='Changed')

    def source(self,kind='tasks_collection'):
        s=Source('s',kind,'r','tenant','owner',enabled=True,settings={'subject_id':'subject',
            'fact_mappings':{'system_record':{'value_path':['record'],'quote_path':['quote']}}})
        self.store.register_source(s);return s
    def test_tasks_removed_from_related_scope_are_not_completed(self):
        source=self.source();reader=Mock();reader.call.return_value={'items':[{'guid':'g','summary':'T','status':'pending','completed_at':'0'}],'has_more':False}
        adapter=TasksCollection(reader,self.store);page=adapter.fetch_page(source,0,100)
        self.store.record(page.observations[0]);self.service.facts.process(page.observations[0],[])
        self.assertEqual(len(self.service.query('subject',Principal('owner','tenant'))['facts']),1)
        reader.call.return_value={'items':[],'has_more':False};gone=adapter.fetch_page(source,0,200).observations[0]
        self.assertTrue(gone.deleted);self.assertTrue(gone.payload['scope_membership_removed'])
        self.store.record(gone);self.assertEqual(self.service.query('subject',Principal('owner','tenant'))['facts'],[])
    def test_task_pagination_does_not_retire_items_on_first_page(self):
        source=self.source();reader=Mock();adapter=TasksCollection(reader,self.store)
        reader.call.return_value={'items':[{'guid':'g','summary':'T'}],'has_more':False}
        self.store.record(adapter.fetch_page(source,0,100).observations[0])
        reader.call.return_value={'items':[],'has_more':True,'page_token':'next'}
        self.assertFalse(adapter.fetch_page(source,0,200).observations)
        reader.call.return_value={'items':[{'guid':'g','summary':'T'}],'has_more':False}
        self.assertFalse(any(o.deleted for o in adapter.fetch_page(source,0,200,'next').observations))
    def test_approval_overall_and_task_status_separate(self):
        source=self.source('approval_snapshot');reader=Mock()
        reader.call.return_value={'instance_code':'r','status':'PENDING','task_list':[{'id':'t','status':'APPROVED'}],'form':'[]'}
        obs=ApprovalInstance(reader).fetch_page(source,0,100).observations[0]
        self.assertEqual(obs.payload['record']['status'],'PENDING')
        self.assertEqual(obs.payload['record']['tasks'][0]['status'],'APPROVED')
        self.store.record(obs);self.service.facts.process(obs,[])
        self.assertEqual(self.service.query('subject',Principal('employee','tenant',('demo-query',)))['facts'],[])
    def test_read_cli_rejects_mutations_and_unlisted_paths(self):
        reader=PinnedUserReader(Mock())
        for args in (['approval','tasks','approve'],['api','POST','/open-apis/im/v1/messages'],['api','GET','/open-apis/auth/v3/token'],['api','GET','/open-apis/calendar/v4/calendars']):
            with self.assertRaises(ScopeViolation):reader.call(args)
    def test_structured_record_does_not_silently_clip(self):
        source=self.source();obs=record_observation(source,'x',{'description':'x'*20000},100)
        self.assertEqual(obs.completeness,'partial');self.assertGreater(len(obs.text),20000)
    def test_mql_retains_exact_numbers_for_requirement_mapping(self):
        result=mql_records({'data':{'1':[{'moql_field_list':[{'name':'自增数字','value_type':'long_value','value':{'long_value':75}}]}]}})
        self.assertEqual(result,[{'自增数字':75}])
    def test_child_scope_is_rechecked(self):
        business=Mock();business.settings={'user_key':'u'};business._run.return_value={'user_key':'u'};business.g.authorized.return_value=True
        reader=MeegleProject(business,lambda:{'user':'owner'},Mock(),lambda source:False)
        source=Source('s','workitem_snapshot','1','t','o',enabled=True,settings={'parent_id':'p'})
        with self.assertRaises(PermissionDenied):reader.fetch_page(source,0,100)
        self.assertEqual(business._run.call_count,1)
    def test_bounded_extractor_uses_original_native_channel_and_forecast_field(self):
        coordinator=Mock();coordinator.complete.return_value=NS(text=json.dumps({'complete':True,'candidates':[
            {'subject_id':'P','field':'plan','value':'预计周五完成','quote':'预计周五完成','assertion_type':'forecast'}]}))
        extract=BoundedCoordinatorExtractor(coordinator,lambda o:{'subjects':[]})
        result=extract(Observation('s','o','v',1,text='预计周五完成'))
        self.assertEqual(result[0]['field'],'forecast');self.assertEqual(result[0]['version'],'v')
        kwargs=coordinator.complete.call_args.kwargs
        self.assertTrue(kwargs['native_chat']);self.assertEqual(kwargs['max_tokens'],1024)
        self.assertEqual(len(kwargs['run_id']),32)
    def test_incomplete_model_output_is_not_published(self):
        coordinator=Mock();coordinator.complete.return_value=NS(text='{"complete":false,"candidates":[]}')
        with self.assertRaisesRegex(Exception,'semantic_model_incomplete'):
            BoundedCoordinatorExtractor(coordinator,lambda o:{})(Observation('s','o','v',1,text='资料'))

if __name__=='__main__':unittest.main()
