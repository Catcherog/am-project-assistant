"""Explicit progress collection and exact per-field approval cards."""
import json
import re
from fs_agent.config import BoundaryError
from fs_agent.ledger_changes import LedgerChanges
from fs_agent.project_catalog import canonical,digest
from group_ledger import select_projects
from sheets_write_port import SheetsWritePort


class UpdateCommands:
    def collection_source(self,row):
        source={'message_id':row['id'],'reporter':row['user'],'chat':row['chat'],'created':row['created']}
        source.update(self.source_meta(row))
        # Legacy rows without the validated original fingerprint require a new
        # source confirmation. Never reconstruct it from normalized route text.
        return source

    def init_updates(self,*,metadata_reader=None,execution_policy=None):
        self.updates=LedgerChanges(self.s,self.owner,SheetsWritePort(self.g,self.ledger,metadata_reader=metadata_reader,execution_policy=execution_policy),enabled=lambda:self.flags['ledger_write'])

    def update_card(self,row,p):
        if not self.flags['ledger_write']:return self.ux_proposal(row,p)
        patches=json.loads(p['patches']);basis=json.loads(p['basis']);r=self.updates.collection(p['collection_id'])
        lines=['项目：'+basis['title'],'提案 '+p['id']+' v'+str(p['version']),'以下内容为报告人自报，尚不代表专业验收。']
        for field,patch in patches.items():lines += ['字段：'+field,'修改前全文：\n'+str(patch['before']),'修改后全文：\n'+str(patch['after'])]
        lines += ['批准仅覆盖所选字段；关键日期、人员、优先级与格式保持。','写入身份：原 AM 机器人。写前重读，发生冲突会停止。','资料来源：'+r['message_key'],
                  '若报告人就是本人，本卡确认同时核对上述解释及批准所选字段。','正式执行状态：'+('需受控执行窗口和原应用编辑权限' if self.flags['ledger_write'] else '正式写回未启用；可先审阅提案')]
        actions=[('approve:'+field,'批准仅修改'+field) for field in patches]
        if len(patches)>1:actions.append(('approve:all','批准全部所示字段'))
        self.card(row,object_id=p['id'],version=p['version'],snapshot={'proposal_hash':digest(patches),'collection_version':r['version']},title='精确台账差异',body='\n\n'.join(lines),actions=actions,flow='ledger_proposal',expires=p['expires'])
        return None

    def handle_update_card(self,row,c,action):
        if c['flow']=='progress_check':
            r=self.updates.confirm_reporter(row['user'],c['object_id'],c['version']);p=self.updates.propose(r['id'])
            if row['user']==self.owner and row['chat_type']=='p2p':return self.update_card(row,p)
            owner=self.owner_origin()
            if owner:self.update_card(owner,p)
            return '已记录解释核对；尚未取得台账写入批准。'
        if c['flow']=='ledger_proposal' and action.startswith('approve:'):
            if not self.flags['ledger_write']:raise BoundaryError('LEDGER_WRITE_NOT_ENABLED')
            p=self.updates.proposal(c['object_id']);patches=json.loads(p['patches']);snapshot=json.loads(c['snapshot'])
            if digest(patches)!=snapshot['proposal_hash']:raise BoundaryError('PROPOSAL_CHANGED')
            selected=list(patches) if action=='approve:all' else [action.split(':',1)[1]]
            self.updates.approve(row['user'],p['id'],c['version'],selected,source_card=c['message_id'],combined_reporter_check=True)
            result=self.updates.execute(p['id'])
            return '提案 '+p['id']+'：'+result['state']+'。'+('执行条件：'+result['execution_block'] if result.get('execution_block') else '仅代表所示写入/回读状态；不代表专业验收。')
        raise BoundaryError('CARD_ACTION_NOT_SUPPORTED')

    def update_command(self,row):
        q=row['body']
        if q.startswith('/precheck'):
            if not self.flags['precheck']:raise BoundaryError('PRECHECK_NOT_ENABLED')
            if q in ('/precheck','/precheck 帮助'):
                return True,'当前已登记目标：'+('、'.join(getattr(self.g.logic.business,'settings',{}).get('targets',{})) or '尚无可用目标')+'\n/precheck 目标别名 test|8d|charter：先绑定项目、节点和交付物，再接收资料。\n/precheck 要求 必须出现的明确字段或章节；另一个要求\n随后在当前私聊发送文件，或使用 /doc 继续。要求只来自已授权目标配置或本人的明确输入。'
            if q.startswith('/precheck 要求 '):
                self.g.logic.set_precheck_requirements(row,q[len('/precheck 要求 '):]);return True,'本次目标要求已记录，原预检及采用状态失效。发送资料或 /doc 继续重新检查。'
            match=re.fullmatch(r'/precheck (\S+) (test|8d|charter)',q)
            if not match:raise BoundaryError('PRECHECK_TARGET_AND_TYPE_REQUIRED')
            d=self.g.logic.start_precheck(row,match[1],match[2])
            self.s.select(row,'doc')
            self.s.execute("INSERT INTO bridge_mode VALUES(?,'poc',?) ON CONFLICT(scope) DO UPDATE SET mode='poc',expires=excluded.expires",(self.scope(row),row['expires']))
            return True,'已绑定预检目标：'+match[1]+'。发送 DOCX、文字层 PDF 或文本材料；未取得要求基线时只能标明无法核实，不会宣称通过。'
        if not q.startswith(('/update','更新需求','更新项目')):return False,None
        if not self.flags['progress_collection']:raise BoundaryError('PROGRESS_COLLECTION_NOT_ENABLED')
        correction=re.fullmatch(r'/update 更正 ([a-f0-9]{32}) v(\d+)[：:](.+)',q,re.S)
        if correction:
            source=dict(self.collection_source(row),corrects_collection=correction[1])
            r=self.updates.invalidate_source(row['user'],correction[1],int(correction[2]),correction=correction[3],correction_source=source)
            if row['user']==self.owner and row['chat_type']=='p2p':return True,self.update_card(row,self.updates.propose(r['id']))
            return True,'更正已记录，旧的未执行批准失效；已写回内容不会自动回滚。请核对后回复 /update 核对 '+r['id']+' v'+str(r['version'])
        check=re.fullmatch(r'/update 核对 ([a-f0-9]{32}) v(\d+)',q)
        if check:
            r=self.updates.confirm_reporter(row['user'],check[1],int(check[2]));p=self.updates.propose(r['id'])
            if row['user']==self.owner and row['chat_type']=='p2p':return True,self.update_card(row,p)
            owner=self.owner_origin()
            if owner:self.update_card(owner,p)
            return True,'已记录解释核对；尚未取得台账写入批准。'
        if row['user']!=self.owner and q.startswith(('/update 查看','/update 对账','/update 撤回','/update 执行')):raise BoundaryError('OWNER_ONLY')
        m=re.fullmatch(r'/update (查看|对账|撤回|执行) ([a-f0-9]{32})',q)
        if m:
            if m[1]=='撤回':
                r=self.updates.collection(m[2]);r=self.updates.invalidate_source(row['user'],r['id'],r['version']);return True,'资料状态：'+r['state']+'；已写入内容不会自动回滚。'
            p=self.updates.proposal(m[2])
            if m[1]=='对账':p=self.updates.reconcile(p['id']);return True,'提案状态：'+p['state']+'；未重新写入。'
            if m[1]=='执行':p=self.updates.execute(p['id']);return True,'提案状态：'+p['state']+'；'+str(p.get('execution_block') or '按原批准范围执行')
            return True,self.update_card(row,p)
        match=re.match(r'^(?:/update\s+|更新)([^：:]+)[：:](.+)$',q,re.S)
        if not match:return True,'更新示例：/update 需求75：上电测试今天完成，报告明天下午提交，低温复测还没做。\n先核对解释和差异，再批准写入。'
        if row['chat_type']=='group':
            self.ledger.port.guard(row['chat'],row['user']);projects=self.ledger.port.read()
            with self.ledger.mutex:projects=self.ledger.catalog.sync(projects)
            if hasattr(self.ledger.port,'filter_projects'):projects=self.ledger.port.filter_projects(row['chat'],projects)
        else:projects=self.projects(row)
        matches=select_projects(match[1],projects)
        if len(matches)!=1 or matches[0]['identity_status']!='verified':raise BoundaryError('PROJECT_IDENTITY_UNVERIFIED')
        source=self.collection_source(row)
        r=self.updates.collect(row['user'],row['id'],matches[0]['key'],match[2],source,row['created'])
        if row['user']!=self.owner or row['chat_type']=='group':
            facts=json.loads(r['facts'])
            return True,'请核对以下解释（核对不等于专业验收或批准写台账）：\n'+'\n'.join(f['original']+'｜'+f['status']+'／自报待核验' for f in facts)+'\n引用本条并 @AM 回复：/update 核对 '+r['id']+' v'+str(r['version'])
        parts=match[2].split('｜待办：',1)
        patches=None
        if len(parts)==2:
            from fs_agent.ledger_changes import merge_progress,split_facts
            basis=self.updates.port.snapshot(matches[0]['key'])
            todo=basis['fields']['待办']['value']
            if not isinstance(todo,str):raise BoundaryError('RICH_TEXT_REQUIRES_EXACT_PATCH')
            patches={'项目进展':merge_progress(basis['fields']['项目进展']['value'],split_facts(parts[0],row['created']),self.s.clock()),'待办':'\n'.join(dict.fromkeys([x for x in todo.splitlines() if x.strip() and x.strip()!='—']+parts[1].splitlines()))}
        p=self.updates.propose(r['id'],patches)
        return True,self.update_card(row,p)
