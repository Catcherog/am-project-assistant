"""DEMO Sheets value writes with independent protected-state reads.

The public v2 PUT API has no documented CAS request field. An explicitly
configured target execution window is required in addition to field approval.
"""
import hashlib
import json
import subprocess
import tempfile
import time
from pathlib import Path
from fs_agent.config import BoundaryError
from fs_agent.project_catalog import canonical,digest
from group_ledger import TOKEN,SHEET,compact


class CliProtectedReader:
    """Read-only metadata helper using an existing, identity-pinned CLI profile."""
    def __init__(self,binary,sha256,identity):
        self.binary=Path(binary);self.sha256=sha256;self.identity=identity
    def call(self,args):
        if hashlib.sha256(self.binary.read_bytes()).hexdigest()!=self.sha256:raise BoundaryError('SHEETS_METADATA_TOOL_CHANGED')
        command=[str(self.binary),*args,'--profile',self.identity['profile']]
        result=subprocess.run(command,capture_output=True,timeout=30)
        if result.returncode or len(result.stdout)>4_000_000:raise BoundaryError('SHEETS_METADATA_UNAVAILABLE')
        try:return json.loads(result.stdout)
        except ValueError:raise BoundaryError('SHEETS_METADATA_INVALID') from None
    def read(self,row):
        current=self.call(['whoami'])
        if any(current.get(k)!=self.identity.get(k) for k in ('profile','appId','identity')) or current.get('onBehalfOf',{}).get('openId')!=self.identity.get('onBehalfOf',{}).get('openId'):raise BoundaryError('SHEETS_METADATA_IDENTITY_CHANGED')
        results=[]
        for region in ('A1:G1',f'A{row}:G{row}'):
            result=self.call(['sheets','+cells-get','--spreadsheet-token',TOKEN,'--sheet-id',SHEET,'--range',region,'--include','value,formula,style,comment,data_validation','--as','user'])
            data=result.get('data',result)
            if data.get('has_more') or len(data.get('ranges',[]))!=1:raise BoundaryError('SHEETS_METADATA_PARTIAL')
            block=data['ranges'][0]
            if block.get('truncated') or block.get('actual_range')!=region or len(block.get('row_indices',[]))!=1 or block['row_indices'][0]!=(1 if region=='A1:G1' else row):raise BoundaryError('SHEETS_METADATA_RANGE_CHANGED')
            if block.get('col_indices')!=list('ABCDEFG') or len(block.get('cells',[]))!=1 or len(block['cells'][0])!=7:raise BoundaryError('SHEETS_METADATA_PARTIAL')
            results.append(dict(zip(block['col_indices'],block['cells'][0])))
        return results


class SheetsWritePort:
    def __init__(self,gateway,ledger,*,metadata_reader=None,execution_policy=None):
        self.g,self.ledger=gateway,ledger
        self.metadata_reader=metadata_reader
        self.execution_policy=execution_policy or {}
    def snapshot(self,project_ref):
        self.ledger.port.authorize_user(self.g.config.owner)
        projects=self.ledger.port.read()
        with self.ledger.mutex:projects=self.ledger.catalog.sync(projects,revision=projects[0].get('source_revision') if projects else None)
        matches=[p for p in projects if p['key']==project_ref]
        if len(matches)!=1:raise BoundaryError('PROJECT_IDENTITY_UNVERIFIED')
        p=matches[0]
        identity={'spreadsheet':TOKEN,'sheet':SHEET,'business_id':p.get('business_id')}
        basic={'identity':identity,'identity_status':p['identity_status'],'row':p['row'],'title':p['title'],
               'fields':{f:{'value':p[f],'formula':None,'attributes':{},'rich':False} for f in ('待办','项目进展')},
               'headers':[],'protected':None,'write_ready':False,'write_limitation':'PROTECTED_STATE_UNVERIFIED','read_at':self.g.store.clock(),'revision':p.get('source_revision')}
        if not self.metadata_reader:return basic
        headers,cells=self.metadata_reader.read(p['row'])
        names={col:cell.get('value') for col,cell in headers.items()}
        fields={}
        for field in ('待办','项目进展'):
            aliases={field,'项目信息'} if field=='项目进展' else {field}
            cols=[col for col,name in names.items() if name in aliases]
            if len(cols)!=1:raise BoundaryError('HEADERS_CHANGED')
            col=cols[0];cell=cells[col]
            if cell.get('value','')!=p[field]:raise BoundaryError('SHEETS_READ_IDENTITIES_DISAGREE')
            fields[field]={'value':cell.get('value',''),'formula':cell.get('formula'),'attributes':{k:v for k,v in cell.items() if k not in ('value','formula')},
                           'rich':bool(cell.get('rich_text') or cell.get('multiple_values')),'column':col}
        protected={col:cell for col,cell in cells.items() if col not in {f['column'] for f in fields.values()}}
        policy=self.execution_policy
        controlled=project_ref in policy.get('project_refs',[]) and policy.get('exclusive_window_until',0)>self.g.store.clock()
        return dict(basic,fields=fields,headers=names,protected=protected,write_ready=controlled,
                    write_limitation=None if controlled else 'CONTROLLED_EDIT_WINDOW_REQUIRED',metadata_identity='existing_pinned_read_profile',write_identity='original_bot')
    def source_current(self,source):
        mid=source.get('message_id')
        if not mid or source.get('fingerprint_kind')!='raw-content-v1':return False
        try:
            m=self.g.reminders.port.read(mid)
            content=json.loads(m['content']) if isinstance(m['content'],str) else m['content']
            if m['deleted']:return False
            if m.get('sender_id')!=source.get('reporter') or m.get('sender_type')!='user' or m.get('chat_id')!=source.get('chat') or m.get('msg_type')!=source.get('message_type'):return False
            return digest(content)==source.get('content_fingerprint')
        except Exception:return False
    def write_cell(self,snapshot,field,value,key):
        if field not in ('待办','项目进展') or snapshot['identity']['spreadsheet']!=TOKEN or snapshot['identity']['sheet']!=SHEET:raise BoundaryError('WRITE_SCOPE_INVALID')
        if not snapshot['write_ready'] or self.execution_policy.get('exclusive_window_until',0)<=self.g.store.clock():raise BoundaryError('CONTROLLED_EDIT_WINDOW_REQUIRED')
        if snapshot['fields'][field]['rich'] or not isinstance(value,str):raise BoundaryError('RICH_TEXT_WRITER_NOT_VERIFIED')
        if snapshot['fields'][field].get('formula'):raise BoundaryError('FORMULA_PROTECTED')
        import lark_oapi as lark
        col=snapshot['fields'][field]['column'];row=snapshot['row'];region=f'{SHEET}!{col}{row}:{col}{row}'
        request=(lark.BaseRequest.builder().http_method(lark.HttpMethod.PUT).uri(f'/open-apis/sheets/v2/spreadsheets/{TOKEN}/values')
                 .token_types({lark.AccessTokenType.TENANT}).body({'valueRange':{'range':region,'values':[[value]]}}).build())
        try:
            response=self.g.api.client.request(request);result=json.loads(response.raw.content)
        except Exception:raise BoundaryError('WRITE_OUTCOME_UNKNOWN') from None
        if result.get('code')!=0:raise BoundaryError('WRITE_PLATFORM_RESPONSE_UNVERIFIED')
        data=result.get('data',{})
        if data.get('updatedRange')!=region or data.get('updatedCells')!=1:raise BoundaryError('WRITE_RECEIPT_UNVERIFIED')
        return {'request_key':key,'range':region,'revision':data.get('revision'),'updated_cells':1,'server_cas':False}
