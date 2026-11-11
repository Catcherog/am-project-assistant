"""Readable text previews; immutable server metadata still owns exact approval."""
import json,re
from .types import canonical,ConversationError

def display(bridge,ctx,reply):
    if not bridge.text_previews:return reply.text
    if reply.status=='needs_confirmation' and reply.pending_actions:
        parts=[]
        for index,ident in enumerate(reply.pending_actions,1):
            action=bridge.store.get_action(ctx,ident)
            marker='动作编号：'+ident+' v'+str(action['version'])
            if marker not in reply.text.splitlines() or action['preview'] not in reply.text:raise ConversationError('PREVIEW_VERSION_CHANGED')
            parts.append('待确认第'+str(index)+'项（第'+str(action['version'])+'版）\n'+action['preview'])
        return '\n\n'.join(parts)+'\n\n核对后回复“确认第1项”；也可以继续修改，或说“取消第1项”。'
    if reply.status=='knowledge_candidate':
        parts=[]
        for ident in reply.source_ids:
            try:record=bridge.knowledge.get(ctx,ident)
            except ConversationError:continue
            if record['status']!='candidate' or record['content'] not in reply.text:continue
            label={'private':'当前私聊','project':'指定项目','tenant':'公司工作范围'}[record['scope']]
            parts.append('待采用第'+str(len(parts)+1)+'条（'+label+'，第'+str(record['version'])+'版）\n'+record['content'])
        if parts:return '\n\n'.join(parts)+'\n\n确认内容和适用范围后，回复“采用第1条”；也可先修改。'
    return reply.text.replace('带上预览中的动作编号','说明预览中的事项序号')

def translate(bridge,ctx,row):
    if not bridge.text_previews:return row['body']
    text=row['body'].strip()
    old=bridge.s.rows('SELECT translated FROM am_conversation_controls WHERE event_id=? AND scope=?',(row['id'],ctx.session_id))
    if old:return old[0]['translated']
    action=re.fullmatch(r'(确认|执行|取消)(?:第)?([1-9一二三四五六七八九])项[。！!\s]*',text)
    knowledge=re.fullmatch(r'(?:采用|批准|同意采用)(?:第)?([1-9一二三四五六七八九])条[。！!\s]*',text)
    shorthand=text in ('采用这条规则','采用这条知识','这条规则可以','以后按这条来','按刚才那份执行','确认刚才这份','可以','发吧','按这个来') or bool(re.fullmatch(r'(?:好的?|行|可以|没问题)[，,\s]*(?:就)?(?:这么|这样)(?:办|发|执行)[吧。！!\s]*',text))
    if not action and not knowledge and not shorthand:return text
    if not ctx.is_owner:return text
    outputs=bridge.s.rows("SELECT reply FROM am_conversation_outputs WHERE scope=? AND state='acknowledged' AND expires>? ORDER BY rowid DESC LIMIT 1",(ctx.session_id,bridge.s.clock()))
    if not outputs:return text
    shown=json.loads(outputs[0]['reply']);translated=text
    def number(s):return int(s) if s.isdigit() else '一二三四五六七八九'.index(s)+1
    if shown.get('status')=='needs_confirmation' and (action or shorthand):
        ids=shown.get('pending_actions',[]);n=number(action[2]) if action else 1
        if not action and len(ids)!=1:return text
        if not action and len(bridge.store.list_actions(ctx,active_only=True))!=1 and text not in ('按刚才那份执行','确认刚才这份'):return '确认执行'
        if 1<=n<=len(ids):
            ident=ids[n-1];item=bridge.store.action_metadata(ctx,ident) if action and action[1]=='取消' else bridge.store.get_action(ctx,ident)
            if '动作编号：'+ident+' v'+str(item['version']) not in shown['text'].splitlines():raise ConversationError('PREVIEW_VERSION_CHANGED')
            translated=('取消 '+ident) if action and action[1]=='取消' else '确认执行 '+ident+' v'+str(item['version'])
    elif shown.get('status')=='knowledge_candidate' and (knowledge or shorthand):
        records=[]
        for ident in shown.get('source_ids',[]):
            try:record=bridge.knowledge.get(ctx,ident)
            except ConversationError:continue
            if record['status']=='candidate' and record['content'] in shown['text']:records.append(record)
        n=number(knowledge[1]) if knowledge else 1
        if not knowledge and len(records)!=1:return text
        if 1<=n<=len(records):translated='批准知识 '+records[n-1]['id']+' v'+str(records[n-1]['version'])
    if translated!=text:
        bridge.s.execute('INSERT INTO am_conversation_controls VALUES(?,?,?,?)',(row['id'],ctx.session_id,translated,row['expires']))
    return translated
