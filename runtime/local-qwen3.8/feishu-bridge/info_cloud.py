"""Bounded cloud table snapshots. Values retain source field/row identity."""
import json
from am_info_sync.models import Observation, Page, SnapshotChanged, ScopeViolation, canonical
from am_info_sync.adapters.http import InvalidResponse
from info_sources import record_observation


def retire_missing(store,source,window,revision,observations,done):
    """Absence from a complete current scope is not a business cancellation."""
    with store.transaction() as db:
        db.execute('''CREATE TABLE IF NOT EXISTS host_snapshot_members(
            source_id TEXT,window INTEGER,revision TEXT,object_id TEXT,
            PRIMARY KEY(source_id,window,revision,object_id))''')
        for obs in observations:
            db.execute('INSERT OR IGNORE INTO host_snapshot_members VALUES(?,?,?,?)',
                       (source.source_id,window,str(revision),obs.object_id))
        if done:
            removed=db.execute("""SELECT object_id FROM objects WHERE source_id=? AND state='active'
                AND object_id NOT IN (SELECT object_id FROM host_snapshot_members
                WHERE source_id=? AND window=? AND revision=?)""",
                (source.source_id,source.source_id,window,str(revision))).fetchall()
            for row in removed:
                observations.append(Observation(source.source_id,row[0],'scope-removed:'+str(window)+':'+str(revision),
                    window,deleted=True,payload={'scope_membership_removed':True}))
            db.execute('DELETE FROM host_snapshot_members WHERE source_id=? AND (window<>? OR revision<>?)',
                       (source.source_id,window,str(revision)))


class BaseTable:
    def __init__(self,user,store):self.user,self.store=user,store
    def fetch_page(self,source,start_ms,end_ms,cursor=None):
        if not source.enabled or source.kind!='bitable_snapshot':raise ScopeViolation('base_source_invalid')
        position=json.loads(cursor) if cursor else {'offset':0}
        offset=position['offset']
        if type(offset) is not int or not 0<=offset<=100000:raise ScopeViolation('base_cursor_invalid')
        if not cursor:
            tables=self.user.call(['base','+table-list','--base-token',source.remote_id]).get('tables')
            if not isinstance(tables,list):raise InvalidResponse('base_table_metadata_missing')
            matches=[t for t in tables if t.get('id')==source.settings['table_id']]
            if len(matches)!=1:raise ScopeViolation('registered_base_table_not_available')
            position.update(revision=matches[0].get('rev'),expected_count=matches[0].get('records_count'))
        data=self.user.call(['base','+record-list','--base-token',source.remote_id,
            '--table-id',source.settings['table_id'],'--format','json','--limit','200','--offset',str(offset)])
        rows,ids=data.get('data'),data.get('record_id_list')
        keys,names=data.get('field_id_list'),data.get('fields')
        revision=data.get('rev')
        if (not all(isinstance(v,list) for v in (rows,ids,keys,names)) or len(rows)!=len(ids)
                or len(keys)!=len(names) or len(set(keys))!=len(keys) or revision is None
                or type(data.get('has_more')) is not bool):raise InvalidResponse('base_matrix_incomplete')
        if position.get('revision')!=revision:raise SnapshotChanged('base_revision_changed')
        observations=[]
        for ident,row in zip(ids,rows):
            if not isinstance(ident,str) or not isinstance(row,list) or len(row)!=len(keys):raise InvalidResponse('base_row_incomplete')
            record={'record_id':ident,'table_id':source.settings['table_id'],
                    'field_labels':dict(zip(keys,names)),'values':dict(zip(keys,row)),
                    'scope':'source_fields; embedded attachment bodies not read'}
            observations.append(record_observation(source,ident,record,end_ms))
        more=data['has_more']
        if more and not rows:raise InvalidResponse('base_empty_nonfinal_page')
        if not more and offset+len(rows)!=position.get('expected_count'):
            raise InvalidResponse('base_record_count_mismatch')
        retire_missing(self.store,source,end_ms,revision,observations,not more)
        token=canonical({'offset':offset+len(rows),'revision':revision,'expected_count':position['expected_count']}) if more else None
        return Page(tuple(observations),token,time_coverage=False)


def column_name(number):
    result=''
    while number:
        number,remainder=divmod(number-1,26);result=chr(65+remainder)+result
    return result


class SheetBook:
    def __init__(self,user,store):self.user,self.store=user,store
    def fetch_page(self,source,start_ms,end_ms,cursor=None):
        if not source.enabled or source.kind!='sheet_snapshot':raise ScopeViolation('sheet_source_invalid')
        position=json.loads(cursor) if cursor else {'sheet':0,'row':1}
        meta=self.user.call(['sheets','+workbook-info','--spreadsheet-token',source.remote_id])
        revision=meta.get('revision');sheets=meta.get('sheets')
        if revision is None or not isinstance(sheets,list):raise InvalidResponse('workbook_metadata_incomplete')
        if cursor and position.get('revision')!=revision:raise SnapshotChanged('workbook_revision_changed')
        index,first=position['sheet'],position['row']
        if type(index) is not int or type(first) is not int or index<0 or first<1:raise ScopeViolation('sheet_cursor_invalid')
        observations=[];token=None
        if index<len(sheets):
            sheet=sheets[index];sid=sheet.get('sheet_id');rows=sheet.get('row_count');cols=sheet.get('column_count')
            if not sid or type(rows) is not int or type(cols) is not int:raise InvalidResponse('sheet_dimensions_missing')
            if not 0<=rows<=5000 or not 0<=cols<=100:raise InvalidResponse('sheet_dimensions_exceed_registered_budget')
            last=min(first+49,rows);region='A'+str(first)+':'+column_name(cols)+str(last)
            if rows and cols:
                data=self.user.call(['sheets','+cells-get','--spreadsheet-token',source.remote_id,
                    '--sheet-id',sid,'--range',region,'--include','value,formula,style','--max-chars','2000000'])
                ranges=data.get('ranges')
                if data.get('has_more') or not isinstance(ranges,list) or len(ranges)!=1:raise InvalidResponse('sheet_range_incomplete')
                block=ranges[0]
                if block.get('truncated') or block.get('actual_range')!=region:raise InvalidResponse('sheet_range_truncated')
                cells,indices=block.get('cells'),block.get('row_indices')
                if not isinstance(cells,list) or not isinstance(indices,list) or len(cells)!=len(indices):raise InvalidResponse('sheet_rows_incomplete')
                if indices!=list(range(first,last+1)) or block.get('col_indices')!=[column_name(n) for n in range(1,cols+1)]:
                    raise InvalidResponse('sheet_coordinate_coverage_incomplete')
                for number,row in zip(indices,cells):
                    if not isinstance(row,list) or len(row)!=cols or any(not isinstance(c,dict) for c in row):
                        raise InvalidResponse('sheet_cell_coverage_incomplete')
                    if all(c.get('value') in (None,'') and c.get('formula') in (None,'') for c in row):continue
                    record={'sheet':sheet.get('sheet_name'), 'row':number,
                            'columns':block.get('col_indices'),'cells':row}
                    observations.append(record_observation(source,sid+':row:'+str(number),record,end_ms))
            after=self.user.call(['sheets','+workbook-info','--spreadsheet-token',source.remote_id])
            if after.get('revision')!=revision:raise SnapshotChanged('workbook_changed_during_read')
            if last<rows:token=canonical({'sheet':index,'row':last+1,'revision':revision})
            elif index+1<len(sheets):token=canonical({'sheet':index+1,'row':1,'revision':revision})
        retire_missing(self.store,source,end_ms,revision,observations,token is None)
        return Page(tuple(observations),token,time_coverage=False)
