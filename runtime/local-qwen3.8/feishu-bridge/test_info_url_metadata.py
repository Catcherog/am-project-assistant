from dataclasses import replace
import unittest
from am_info_sync.store import Store
from am_info_sync.models import Source,Observation,IntegrityConflict

class URLMetadataTests(unittest.TestCase):
    def setUp(self):
        self.now=1000;self.store=Store(':memory:',clock=lambda:self.now)
        self.store.register_source(Source('s','docx','document','tenant','owner',enabled=True,raw_retention_seconds=1))
        self.obs=Observation('s','document','v1',1000,text='原文条件必须保留',payload={'revision_id':1})
        self.store.record(self.obs)
    def tearDown(self):self.store.close()
    def test_verified_link_enrichment_deduplicates_without_changing_expiry(self):
        before=dict(self.store.conn.execute('SELECT * FROM observations').fetchone())
        updated=replace(self.obs,url='https://tenant.feishu.cn/docx/document')
        self.assertEqual(self.store.record(updated,authoritative=True),'duplicate')
        after=dict(self.store.conn.execute('SELECT * FROM observations').fetchone())
        for key in ('received_at_ms','raw_expires_at_ms','purged','text','payload_json','version'):
            self.assertEqual(before[key],after[key])
        self.assertEqual(self.store.get_observation('s','document').url,updated.url)
        self.assertEqual(self.store.conn.execute('SELECT count(*) FROM observations').fetchone()[0],1)
    def test_changed_body_or_unverified_link_still_conflicts(self):
        updated=replace(self.obs,url='https://tenant.feishu.cn/docx/document')
        with self.assertRaises(IntegrityConflict):self.store.record(updated)
        with self.assertRaises(IntegrityConflict):self.store.record(replace(updated,text='伪造已验收'),authoritative=True)
        with self.assertRaises(IntegrityConflict):self.store.record(replace(updated,payload={'revision_id':1,'extra':'changed'}),authoritative=True)
    def test_link_refresh_does_not_restore_purged_raw(self):
        self.now=3000;self.store.purge_expired_raw()
        updated=replace(self.obs,url='https://tenant.feishu.cn/docx/document')
        self.assertEqual(self.store.record(updated,authoritative=True),'duplicate')
        self.assertIsNone(self.store.get_observation('s','document'))
        row=self.store.conn.execute('SELECT purged,text,payload_json,raw_expires_at_ms,url FROM observations').fetchone()
        self.assertEqual(tuple(row[:4]),(1,'','{}',2000));self.assertEqual(row['url'],updated.url)

    def test_existing_link_cannot_be_replaced_or_cleared(self):
        linked=replace(self.obs,url='https://tenant.feishu.cn/docx/document')
        self.store.record(linked,authoritative=True)
        for url in (None,'','https://tenant.feishu.cn/docx/other'):
            with self.subTest(url=url),self.assertRaises(IntegrityConflict):
                self.store.record(replace(linked,url=url),authoritative=True)
        self.assertEqual(self.store.get_observation('s','document').url,linked.url)

    def test_other_evidence_flags_still_conflict(self):
        linked=replace(self.obs,url='https://tenant.feishu.cn/docx/document')
        for changes in ({'deleted':True},{'completeness':'partial'},{'parent_id':'different'}):
            with self.subTest(changes=changes),self.assertRaises(IntegrityConflict):
                self.store.record(replace(linked,**changes),authoritative=True)

    def test_metadata_fill_does_not_trigger_existing_reacquisition_option(self):
        self.store.register_source(replace(self.store.get_source('s'),settings={'reacquire_expired_for_pending':True}))
        self.now=3000;self.store.purge_expired_raw()
        before=dict(self.store.conn.execute('SELECT * FROM observations').fetchone())
        jobs=[dict(r) for r in self.store.conn.execute('SELECT * FROM jobs')]
        self.store.record(replace(self.obs,url='https://tenant.feishu.cn/docx/document'),authoritative=True)
        after=dict(self.store.conn.execute('SELECT * FROM observations').fetchone())
        for key in before:
            if key not in ('url','body_hash'):self.assertEqual(before[key],after[key],key)
        self.assertEqual(jobs,[dict(r) for r in self.store.conn.execute('SELECT * FROM jobs')])
        self.assertIsNone(self.store.get_observation('s','document'))

    def test_metadata_fill_does_not_reselect_conflicted_version(self):
        self.store.conn.execute("UPDATE objects SET state='conflict' WHERE source_id='s'")
        before=dict(self.store.conn.execute('SELECT * FROM objects').fetchone())
        self.store.record(replace(self.obs,url='https://tenant.feishu.cn/docx/document'),authoritative=True)
        self.assertEqual(before,dict(self.store.conn.execute('SELECT * FROM objects').fetchone()))

    def test_historical_link_enrichment_keeps_current_version(self):
        latest=replace(self.obs,version='v2',occurred_at_ms=1100,text='新版本仍待核',payload={'revision_id':2})
        self.store.record(latest,authoritative=True)
        before=dict(self.store.conn.execute('SELECT * FROM objects').fetchone())
        result=self.store.record(replace(self.obs,url='https://tenant.feishu.cn/docx/document'),authoritative=True)
        self.assertEqual(result,'historical')
        self.assertEqual(before,dict(self.store.conn.execute('SELECT * FROM objects').fetchone()))

if __name__=='__main__':unittest.main()
