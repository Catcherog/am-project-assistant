import unittest
from unittest.mock import Mock
from am_info_sync.store import Store
from am_info_sync.models import Source, Principal, SnapshotChanged
from am_info_sync.runtime import InformationService
from info_sources import record_observation,Discovery
from types import SimpleNamespace as NS
from info_cloud import BaseTable,SheetBook

class CloudTests(unittest.TestCase):
    def setUp(self):self.store=Store(':memory:');self.service=InformationService(self.store)
    def tearDown(self):self.store.close()
    def source(self,kind):
        s=Source('s',kind,'token','tenant','owner',enabled=True,settings={
            'table_id':'tbl1','subject_id':'D','fact_mappings':{'system_record':{'value_path':['record'],'quote_path':['quote']}}})
        self.store.register_source(s);return s
    def matrix(self,rev=1,more=False,rows=None,ids=None):
        return {'data':rows or [['x','y']],'record_id_list':ids or ['rec1'],
            'field_id_list':['f1','f2'],'fields':['同名','同名'],'has_more':more,'rev':rev}
    def test_base_retains_duplicate_column_names_by_field_id(self):
        s=self.source('bitable_snapshot');u=Mock();u.call.side_effect=[{'tables':[{'id':'tbl1','rev':1,'records_count':1}]},self.matrix()]
        o=BaseTable(u,self.store).fetch_page(s,0,100).observations[0]
        self.assertEqual(o.payload['record']['values'],{'f1':'x','f2':'y'})
        self.store.record(o);self.service.facts.process(o,[])
        self.assertEqual(len(self.service.query('D',Principal('owner','tenant'))['facts']),1)
    def test_base_mid_page_revision_change_restarts(self):
        s=self.source('bitable_snapshot');u=Mock();u.call.side_effect=[{'tables':[{'id':'tbl1','rev':1,'records_count':2}]},self.matrix(more=True),self.matrix(rev=2)]
        reader=BaseTable(u,self.store);first=reader.fetch_page(s,0,100)
        with self.assertRaises(SnapshotChanged):reader.fetch_page(s,0,100,first.next_cursor)
    def test_collection_records_do_not_manufacture_conflicts(self):
        s=self.source('bitable_snapshot')
        for ident,status in [('one','APPROVED'),('two','PENDING')]:
            o=record_observation(s,ident,{'id':ident,'status':status},100)
            self.store.record(o);self.service.facts.process(o,[])
        q=self.service.query('D',Principal('owner','tenant'))
        self.assertEqual(q['conflicts'],[]);self.assertEqual(len(q['effective']),2)
    def test_sheet_changed_while_read_does_not_commit(self):
        s=self.source('sheet_snapshot');u=Mock()
        meta={'revision':1,'sheets':[{'sheet_id':'S','row_count':1,'column_count':1,'sheet_name':'表'}]}
        u.call.side_effect=[meta,{'has_more':False,'ranges':[{'actual_range':'A1:A1','cells':[[{'value':'A'}]],'row_indices':[1],'col_indices':['A']}]},{'revision':2}]
        with self.assertRaises(SnapshotChanged):SheetBook(u,self.store).fetch_page(s,0,100)
        self.assertEqual(self.store.current_records(),[])
    def test_sheet_zero_and_false_are_valid_rows(self):
        s=self.source('sheet_snapshot');u=Mock()
        meta={'revision':1,'sheets':[{'sheet_id':'S','row_count':3,'column_count':1,'sheet_name':'表'}]}
        u.call.side_effect=[meta,{'has_more':False,'ranges':[{'actual_range':'A1:A3',
            'cells':[[{'value':0}],[{'value':False}],[{'value':None}]],
            'row_indices':[1,2,3],'col_indices':['A']}]},meta]
        result=SheetBook(u,self.store).fetch_page(s,0,100)
        self.assertEqual([o.object_id for o in result.observations],['S:row:1','S:row:2'])
        self.assertIs(result.observations[1].payload['record']['cells'][0]['value'],False)
    def test_new_message_jobs_precede_existing_doc_backlog(self):
        doc=Source('doc','docx','d','t','o',enabled=True);chat=Source('chat','im_chat','c','t','o',enabled=True)
        self.store.register_source(doc);self.store.register_source(chat)
        self.store.enqueue('extract','old',{'source_id':'doc'});self.store.enqueue('extract','new',{'source_id':'chat'})
        job=self.store.claim_job('extract',prefer_source_kinds=('im_chat',))
        self.assertEqual(job['dedupe_key'],'new')
    def test_recent_child_is_not_requeued_ahead_of_unknown_children(self):
        import time,json
        with self.store.transaction() as db:db.execute('CREATE TABLE host_discovery_state(key TEXT PRIMARY KEY,value TEXT NOT NULL)')
        d=Discovery.__new__(Discovery);d.h=NS(store=self.store)
        d.state('child:known',{'at':time.time(),'parent':'parent','node':'node'})
        parent=NS(source_id='parent-source',remote_id='parent')
        for _ in range(20):d.child(parent,'known','node')
        d.child(parent,'new','node')
        rows=list(self.store.conn.execute("SELECT key FROM host_discovery_state WHERE key LIKE 'child_pending:%'"))
        self.assertEqual([r[0] for r in rows],['child_pending:new'])
    def test_cached_file_keeps_second_project_association(self):
        import time
        from am_info_sync.models import digest
        with self.store.transaction() as db:db.execute('CREATE TABLE host_discovery_state(key TEXT PRIMARY KEY,value TEXT NOT NULL)')
        d=Discovery.__new__(Discovery);d.h=NS(store=self.store);d.source=Mock()
        url='https://tenant.feishu.cn/file/token'
        d.state('document_url:'+digest(url),{'at':time.time(),'kind':'file','token':'token','title':'报告.pdf'})
        result=d.register_document(url,['project-2'])
        self.assertEqual(d.source.call_args.args[3],['project-2'])
        self.assertEqual(result['source_ids'],['file:token'])
    def test_document_link_counts_distinguish_types_and_pending(self):
        with self.store.transaction() as db:db.execute('CREATE TABLE host_discovery_state(key TEXT PRIMARY KEY,value TEXT NOT NULL)')
        d=Discovery.__new__(Discovery);d.h=NS(store=self.store,catalog=Mock());d.user=Mock()
        d.user.call.return_value={'has_more':False,'values':['https://tenant.feishu.cn/docx/a','https://tenant.feishu.cn/wiki/b','https://tenant.feishu.cn/wiki/c']}
        d.register_document=Mock(side_effect=[{'status':'registered','document_type':'docx','source_ids':['a']},
            {'status':'registered','document_type':'file','source_ids':['b']},
            {'status':'unsupported_type','document_type':'mindnote','source_ids':[]}])
        d.documents_from_ledger();result=d.state('ledger_document_links')
        self.assertEqual(result['registered_documents'],2)
        self.assertEqual(result['docx_registered'],1)
        self.assertEqual(result['registered_documents_by_type'],{'docx':1,'file':1})
        self.assertEqual(len(result['pending_other_types']),1)

if __name__=='__main__':unittest.main()
