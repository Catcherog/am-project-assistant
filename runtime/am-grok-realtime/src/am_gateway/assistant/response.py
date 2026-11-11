"""Shared response guidance carried to actual workers, not a second model pass."""
from copy import deepcopy


RESPONSE_TEXT = (
    '先直接回答当前问题，再补必要依据或下一步。用自然、简洁的中文，避免官腔、重复铺垫和内部诊断标记。'
    '单字段查询短答；综合项目问题按需要说明进展、卡点、计划和下一步，不强行填满没有来源的字段。'
    '逐项处理用户的子问题；如果一部分尚未完成，明确指出剩余部分，不能只回答最快的一项就声称完成。'
    '保留预计、计划、已标记完成、对方反馈、待核验等限定；附件已生成不等于已送达，已发送不等于对方已处理。'
    '项目事实必须来自本任务可读的当前来源；来源失败、范围未绑定或结果不完整时，不能声称项目不存在。'
    '同一任务的前稿可继续修改，但模型旧输出不自动变成项目事实或批准。'
    '建议与已经执行的动作分开说；承接提示不算实质答复。无需为了润色再串行调用另一模型。'
)


def response_guidance(*, channel, owner_request=False):
    return {'version': '2026-11-10.R7', 'channel': channel,
            'addressing': '本人需要称呼时用示例负责人。' if owner_request else '按当前发问者交流，不一律称为示例负责人。',
            'instructions': RESPONSE_TEXT,
            'authority': 'expression_only_no_new_permissions',
            'requires_extra_model_call': False}


def model_response_guidance(envelope):
    supplied = envelope.get('output', {}).get('response_guidance')
    return deepcopy(supplied) if isinstance(supplied, dict) else response_guidance(channel='task')
