"""One briefing contract for previews, interactive reads and scheduled delivery."""
import json
from datetime import datetime
from fs_agent.project_catalog import canonical
from fs_agent.config import BoundaryError
from fs_agent.scheduler import TZ
from am_views import paging,page_actions,short,stamp

class BriefingMixin:
    def briefing_types(self,plan):
        rule=json.loads(plan['rule'])
        if 'briefing_content' in rule:return [x for x in ('changes','todos') if x in rule['briefing_content']]
        # UX1's exact subscription preview included todos. Only a completed
        # draft bound to this plan proves that approval; legacy CLI stays changes.
        for d in self.s.rows("SELECT result,data FROM am_ux_drafts WHERE state='done' AND result IS NOT NULL"):
            if json.loads(d['result']).get('plan')==plan['id'] and json.loads(d['data']).get('subscription'):return ['changes','todos']
        return ['changes']

    def briefing_data(self,events,projects,types):
        return {'types':types,'events':events,'todos':[{'project':p['key'],'title':p['title'],'text':p['待办']} for p in projects if p.get('待办','').strip() not in ('','—')] if 'todos' in types else [],
                'read_at':self.s.clock(),'read_status':'complete' if all(p.get('read_status','complete')=='complete' for p in projects) else 'partial',
                'source_revisions':list(dict.fromkeys(p.get('source_revision') for p in projects)),
                'event_ids':[e['id'] for e in events]}

    def briefing_summary(self,data):
        groups={}
        for e in data['events']:groups.setdefault(e['title'],[]).append(e['field'])
        lines=[f"项目变化 {len(data['events'])} 条"+('；台账候选待办 '+str(len(data['todos']))+' 项。' if 'todos' in data['types'] else '。')]
        for title,fields in list(groups.items())[:3]:lines.append(short(title,65,'项目记录')+'：'+ '、'.join(dict.fromkeys(fields))+'有变化。')
        if not groups:lines.append('本次没有可新投递的变化。')
        if data['read_status']!='complete':lines.append('读取不完整，不能判定全部变化。')
        lines+=['记录变化不代表批准或验收改变。','读取：'+stamp(data['read_at'])]
        return '\n'.join(lines)

    def briefing_payload(self,plan):
        sub=self.s.rows('SELECT id FROM am_subscriptions WHERE plan_id=?',(plan['id'],))
        deliveries=self.s.rows('SELECT event_id,state FROM am_briefing_deliveries WHERE subscription_id=?',(sub[0]['id'],)) if sub else []
        seen={r['event_id'] for r in deliveries if r['state']!='not_sent'}
        with self.ledger.mutex:
            events=[dict(c) for c in self.ledger.catalog.changes() if c['id'] not in seen]
            # The worker has refreshed the authorized source before scan. Read
            # stored catalog records here; never call remote APIs in its tx.
            projects=self._briefing_projects
        data=self.briefing_data(events,projects,self.briefing_types(plan))
        return {'body':self.briefing_summary(data),'briefing_data':data},data['event_ids']

    def briefing(self,row,*,changes_only=False):
        projects=self.projects(row)
        with self.ledger.mutex:events=[dict(e) for e in self.ledger.catalog.changes()]
        data=self.briefing_data(events,projects,['changes'] if changes_only else ['changes','todos'])
        return self.ux_briefing(row,data)

    def ux_briefing(self,row,data,section=None,page=0):
        actions=[]
        if section:
            rows=data['events'] if section=='changes' else data['todos'];selected,page,label=paging(rows,page)
            lines=[label]
            for e in selected:
                lines.append(e['title']+'\n'+(e['field']+'\n修改前全文：\n'+e['before_value']+'\n修改后全文：\n'+e['after_value']+'\n读取：'+stamp(e['read_at']) if section=='changes' else e['text']))
            actions+=page_actions(page,len(rows),{'page':'briefing_detail','section':section})
            actions.append(('返回简报',{'page':'briefing_detail'}));text='\n\n'.join(lines)
        else:
            text=self.briefing_summary(data)
            actions=[('完整变化',{'page':'briefing_detail','section':'changes'})]
            if 'todos' in data['types']:actions.append(('候选待办',{'page':'briefing_detail','section':'todos'}))
            actions.append(('返回首页',{'page':'home'}))
        return self.ux_page(row,'变化简报',text,actions,data={'briefing_snapshot':data,'detail':bool(section)})

    def handle_briefing_card(self,row,body):
        v=body['value'];o=self.scheduler.occurrence(v['briefing']);payload=json.loads(o['payload'])
        if (row['user']!=self.owner or payload['target']['kind']!='owner' or payload['target']['user']!=row['user']
            or row['chat']!=payload['target']['chat'] or body['source_message']!=o['message_id']
            or v.get('nonce')!=payload.get('briefing_nonce') or o['state']!='sent'):raise BoundaryError('BRIEFING_BINDING_INVALID')
        return self.ux_briefing(row,payload['briefing_data'])
