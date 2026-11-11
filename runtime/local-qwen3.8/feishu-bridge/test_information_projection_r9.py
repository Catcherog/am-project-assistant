"""Authorized information projection contracts; no model or platform calls."""
from copy import deepcopy
from types import SimpleNamespace as NS
from unittest.mock import Mock
import unittest

from am_conversation.capabilities import information
from am_conversation.types import ConversationError, canonical
from am_info_sync.document_snapshot import projection_version
from am_info_sync.models import Observation, Principal, Source
from am_info_sync.runtime import InformationService
from am_info_sync.store import Store


def fact(index=0, **changes):
    return dict({'fact_id':'f'+str(index), 'subject_id':'p', 'field':'progress',
        'value':'可安排测试', 'assertion_type':'forecast', 'authority':'unconfirmed',
        'protected':False, 'stale':False, 'polarity':'conditional',
        'condition':'样机到货且复核通过', 'conditions':['保留原审批边界'],
        'attribution':'工程师转述', 'time_scope':'下周',
        'evidence':{'source_id':'s', 'object_id':'o'+str(index), 'version':'v1',
                    'quote':'工程师转述：样机到货且复核通过后，下周可安排测试。',
                    'raw_available':True, 'raw_expires_at_ms':2_000_000,
                    'received_at_ms':900_000, 'verification':'verbatim_at_extraction',
                    'url':'https://example.com/source'}}, **changes)


def payload(**changes):
    return dict({'version':'state-1', 'facts':[fact()],
        'coverage':{'scope':'registered', 'sources':[], 'processing_pending':False,
                    'needs_refresh_or_review':False, 'global_completeness_claim':False},
        'conflicts':[], 'incomplete_reasons':[], 'pending_confirmation':['f0'],
        'effective':[]}, **changes)


def run(raw=None, args=None, *, context_expiry=1500.0, now_ms=1_000_000):
    raw=payload() if raw is None else raw
    row={'expires':context_expiry, 'user':'owner'}
    query=Mock(return_value=raw)
    host=NS(query=query, store=NS(clock=lambda:now_ms))
    bridge=NS(authorize=Mock(), service=NS(information=host), _current=lambda ctx:{'row':row})
    ctx=NS(content_expires_at=context_expiry)
    out=information(bridge,ctx,dict({'project_id':'p'},**(args or {})),'event')
    return out,bridge,raw


class InformationProjectionTests(unittest.TestCase):
    def test_small_status_sets_are_preserved_exactly_and_source_is_not_mutated(self):
        raw=payload(conflicts=[{'field':'progress','kind':'unconfirmed_discrepancy','fact_ids':['f0','f1']}],
                    incomplete_reasons=['raw_evidence_expired'], pending_confirmation=['f0','f1'],
                    effective=[{'field':'plan','value':'原计划','fact_ids':['f2'],'stale':True}])
        before=deepcopy(raw)
        out,_,_=run(raw)
        for name in ('conflicts','incomplete_reasons','pending_confirmation','effective'):
            self.assertEqual(out[name],raw[name])
            self.assertTrue(out['status_pages'][name]['complete'])
        self.assertEqual(raw,before)
        self.assertTrue(out['complete_response'])  # Read completeness, not business acceptance.

    def test_qualifiers_and_authority_survive_with_original_quote(self):
        out,_,raw=run()
        selected=out['facts'][0]
        for name in ('condition','conditions','attribution','polarity','time_scope','authority','protected'):
            self.assertEqual(selected[name],raw['facts'][0][name])
        self.assertEqual(selected['evidence']['quote'],raw['facts'][0]['evidence']['quote'])
        self.assertEqual(selected['evidence']['received_at_ms'],900_000)
        self.assertEqual(selected['authority'],'unconfirmed')

    def test_fact_filter_does_not_hide_project_level_conflict(self):
        conflict={'field':'progress','kind':'confirmed_conflict','fact_ids':['f0','f1']}
        raw=payload(facts=[fact(),fact(1)],conflicts=[conflict])
        out,_,_=run(raw,{'fact_id':'f0'})
        self.assertEqual(len(out['facts']),1)
        self.assertEqual(out['conflicts'],[conflict])

    def test_second_fact_page_still_carries_global_status(self):
        raw=payload(facts=[fact(),fact(1)],pending_confirmation=['f0','f1'])
        out,_,_=run(raw,{'offset':1,'limit':1})
        self.assertEqual(out['facts'][0]['fact_id'],'f1')
        self.assertEqual(out['pending_confirmation'],['f0','f1'])
        self.assertFalse(out['complete_response'])

    def test_unavailable_raw_quote_is_not_copied_or_rebuilt_from_value(self):
        value=fact(value='上游允许保留的派生状态')
        value['evidence']=dict(value['evidence'],quote='过期原文绝不复制',raw_available=False)
        out,_,_=run(payload(facts=[value]))
        self.assertNotIn('过期原文绝不复制',canonical(out))
        self.assertNotIn('quote',out['facts'][0]['evidence'])
        self.assertEqual(out['facts'][0]['value'],'上游允许保留的派生状态')
        self.assertFalse(out['facts'][0]['evidence']['raw_available'])

    def test_available_flag_does_not_override_expired_timestamp(self):
        value=fact();value['evidence']=dict(value['evidence'],quote='到期原文',raw_expires_at_ms=1_000_000)
        out,_,_=run(payload(facts=[value]))
        self.assertNotIn('到期原文',canonical(out))
        self.assertEqual(out['facts'][0]['evidence']['quote_omitted_reason'],'raw_evidence_expired')

    def test_source_quote_cannot_acquire_longer_conversation_retention(self):
        value=fact();value['evidence']=dict(value['evidence'],quote='期限较短的原文',raw_expires_at_ms=1_100_000)
        out,_,_=run(payload(facts=[value]),context_expiry=1500)
        self.assertNotIn('期限较短的原文',canonical(out))
        self.assertEqual(out['facts'][0]['evidence']['quote_omitted_reason'],
                         'source_retention_shorter_than_conversation')

    def test_unknown_quote_expiry_does_not_silently_extend_retention(self):
        for expiry in (None,'unknown',True):
            with self.subTest(expiry=expiry):
                value=fact();value['evidence']=dict(value['evidence'],quote='不明期限原文')
                if expiry is None:value['evidence'].pop('raw_expires_at_ms')
                else:value['evidence']['raw_expires_at_ms']=expiry
                out,_,_=run(payload(facts=[value]))
                self.assertNotIn('不明期限原文',canonical(out))

    def test_large_condition_withholds_bare_value_instead_of_dropping_qualifier(self):
        value=fact(value='可以正式发布',condition='仅在完成以下复核后：'+'条件'*700)
        out,_,_=run(payload(facts=[value]))
        selected=out['facts'][0]
        self.assertNotIn('value',selected)
        self.assertIn('condition',selected['qualifiers_omitted'])
        self.assertFalse(out['complete_response'])

    def test_oversized_first_fact_has_visible_omission_and_advancing_cursor(self):
        value=fact(value='X'*2500,condition='C'*950,conditions=['D'*950],attribution='A'*950)
        out,_,_=run(payload(facts=[value,fact(1)]),{'limit':1})
        self.assertTrue(out['facts'][0]['value_omitted'])
        self.assertEqual(out['next_offset'],1)
        self.assertTrue(out['has_more'])

    def test_large_status_collection_is_explicitly_pageable_without_loss(self):
        ids=['f'+str(i).zfill(3)+'x'*40 for i in range(60)]
        raw=payload(pending_confirmation=ids)
        first,_,_=run(raw)
        page=first['status_pages']['pending_confirmation']
        self.assertEqual(page['total'],60)
        self.assertTrue(page['has_more'])
        self.assertFalse(first['complete_response'])
        self.assertEqual(page['read_with']['section'],'@pending_confirmation')
        collected=[];offset=0
        while offset<len(ids):
            out,_,_=run(raw,{'section':'@pending_confirmation','offset':offset,'limit':12})
            collected.extend(out['pending_confirmation'])
            self.assertGreater(out['next_offset'],offset)
            offset=out['next_offset']
        self.assertEqual(collected,ids)

    def test_large_effective_value_can_be_read_on_dedicated_page(self):
        item={'field':'plan','value':'原文'*1000,'fact_ids':['f0'],'stale':False}
        raw=payload(effective=[item])
        summary,_,_=run(raw)
        self.assertTrue(summary['status_pages']['effective']['details_omitted'])
        out,_,_=run(raw,{'section':'@effective','limit':1})
        self.assertEqual(out['effective'],[item])
        self.assertTrue(out['complete_response'])

    def test_item_too_large_even_on_status_page_is_explicit_and_not_clipped(self):
        item={'field':'plan','value':'V'*20_000,'fact_ids':['f0']}
        out,_,_=run(payload(effective=[item]),{'section':'@effective','limit':1})
        self.assertTrue(out['effective'][0]['details_omitted'])
        self.assertNotIn('value',out['effective'][0])
        self.assertEqual(out['next_offset'],1)
        self.assertFalse(out['complete_response'])
        self.assertTrue(out['coverage']['needs_refresh_or_review'])

    def test_result_budget_stays_below_existing_tool_limit(self):
        raw=payload(facts=[fact(i,value='V'*2000) for i in range(12)],
                    conflicts=[{'field':'progress','kind':'unconfirmed_discrepancy','fact_ids':['f'+str(i)]} for i in range(80)],
                    pending_confirmation=['f'+str(i)+'x'*40 for i in range(80)],
                    effective=[{'field':'plan','value':'E'*800,'fact_ids':['f0']} for _ in range(80)],
                    incomplete_reasons=['reason-'+str(i) for i in range(80)])
        raw['coverage']['sources']=[{'source_id':'source-'+str(i),'stale':True,
            'unprocessed_jobs':1,'gap_kinds':['extraction']} for i in range(80)]
        out,_,_=run(raw,{'limit':12})
        self.assertLess(len(canonical(out)),10000)
        self.assertFalse(out['complete_response'])
        self.assertTrue(out['coverage']['source_details_omitted'])

    def test_system_record_summary_and_section_contract_is_preserved(self):
        value=fact(field='system_record',value={'title':'需求','basic':{'name':'样机','secret_unknown':'not-selected'},'details':{'status':'pending'}})
        summary,_,_=run(payload(facts=[value]))
        self.assertIn('selection_note',summary['facts'][0]['value'])
        self.assertNotIn('secret_unknown',summary['facts'][0]['value']['basic'])
        detail,_,_=run(payload(facts=[value]),{'section':'details'})
        self.assertEqual(detail['facts'][0]['value'],{'details':{'status':'pending'}})
        self.assertEqual(detail['facts'][0]['condition'],value['condition'])

    def test_source_review_flag_is_not_erased_when_projection_is_complete(self):
        raw=payload();raw['coverage']['needs_refresh_or_review']=True
        out,_,_=run(raw)
        self.assertTrue(out['coverage']['needs_refresh_or_review'])

    def test_projection_limitation_does_not_overwrite_its_source_reason(self):
        raw=payload(pending_confirmation=['f'+str(i)+'x'*50 for i in range(80)])
        out,_,_=run(raw)
        self.assertFalse(out['coverage']['source_needs_refresh_or_review'])
        self.assertTrue(out['coverage']['needs_refresh_or_review'])
        self.assertIn('pending_confirmation',out['coverage']['projection_omissions'])

    def test_authorization_failure_prevents_source_read(self):
        query=Mock()
        bridge=NS(authorize=Mock(side_effect=ConversationError('PROJECT_ACCESS_DENIED')),
                  service=NS(information=NS(query=query)))
        with self.assertRaisesRegex(ConversationError,'PROJECT_ACCESS_DENIED'):
            information(bridge,None,{'project_id':'other'},'event')
        query.assert_not_called()

    def test_absent_information_service_remains_explicitly_unavailable(self):
        bridge=NS(authorize=Mock(),service=NS())
        self.assertFalse(information(bridge,None,{'project_id':'p'},'event')['available'])

    def test_actual_fact_engine_payload_keeps_conflict_and_pending_status(self):
        store=Store(':memory:',clock=lambda:1_000_000)
        try:
            store.register_source(Source('s','manual','local','tenant','owner',enabled=True,
                raw_retention_seconds=3600,settings={'subject_id':'p'}))
            service=InformationService(store)
            for i,text in enumerate(('材料已交付','材料并未交付')):
                obs=Observation('s','o'+str(i),'v1',1_000_000,text=text)
                store.record(obs)
                service.facts.process(obs,[{'source_id':'s','object_id':obs.object_id,'version':'v1',
                    'subject_id':'p','field':'progress','value':text,'quote':text,'assertion_type':'reported'}])
            raw=service.query('p',Principal('owner','tenant'))
            raw['version']=projection_version(raw)
            out,_,_=run(raw)
            self.assertEqual(out['conflicts'],raw['conflicts'])
            self.assertEqual(out['pending_confirmation'],raw['pending_confirmation'])
            self.assertEqual(len(out['pending_confirmation']),2)
            self.assertTrue(all(f['authority']=='unconfirmed' for f in out['facts']))
        finally:
            store.close()


if __name__=='__main__':
    unittest.main()
