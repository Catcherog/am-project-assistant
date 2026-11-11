import pytest
from am_info_sync.extraction import ExtractionError
from test_info_semantics_r9 import extractor_for, fact, observation, MULTI_CONTEXT


@pytest.mark.parametrize('punct',['；','，'])
def test_cross_comma_quote_covers_original_offsets_without_project_prefix(punct):
    source='演示甲项目若样机到货，下周才可能开始测试'+punct+'实际尚未开测'
    extractor,_=extractor_for([
        fact('forecast','若样机到货，下周才可能开始测试',assertion='forecast'),
        fact('progress','实际尚未开测')])
    assert len(extractor(observation(source)))==2


@pytest.mark.parametrize('quote,subject',[
    ('下周才可能开始测试','a'),
    ('若样机到货，下周才可能开始测试','b')])
def test_condition_or_project_cannot_be_lost_by_offset_fix(quote,subject):
    extractor,_=extractor_for([fact('forecast',quote,subject=subject,assertion='forecast')],context=MULTI_CONTEXT)
    with pytest.raises(ExtractionError):extractor(observation('演示甲项目若样机到货，下周才可能开始测试'))


def test_valid_hint_conflict_is_diagnosed_before_call_even_with_empty_response():
    extractor,provider=extractor_for([],context=dict(MULTI_CONTEXT,conversation_hint={'subject_id':'a'}))
    with pytest.raises(ExtractionError,match='^subject_hint_conflict$'):
        extractor(observation('演示乙项目尚未开测'))
    provider.complete.assert_not_called()


def test_actual_qwen_conditional_plan_misclassification_is_not_accepted():
    extractor,_=extractor_for([
        fact('plan','若样机到货，下周才可能开始测试'),
        fact('progress','实际尚未开测')])
    with pytest.raises(ExtractionError,match='conditional_forecast_dimension'):
        extractor(observation('演示甲项目若样机到货，下周才可能开始测试；实际尚未开测'))
