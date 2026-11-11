"""Feishu card JSON 2.0 primitives; user text stays plain text."""
def plain(text):return {'tag':'plain_text','content':str(text)}

def paragraph(text):return {'tag':'div','text':plain(text)}

def columns(groups,weights=None):
    weights=weights or [1]*len(groups)
    return {'tag':'column_set','flex_mode':'none','columns':[
        {'tag':'column','width':'weighted','weight':weight,'elements':group}
        for weight,group in zip(weights,groups)]}

def button(label,value,*,name=None,submit=False):
    result={'tag':'button','text':plain(label),'type':'default','width':'fill',
            'behaviors':[{'type':'callback','value':value}]}
    if name:result['name']=name
    if submit:result['form_action_type']='submit'
    return result

def field(name,label,*,kind='input',options=None,default=None,required=True):
    item={'tag':kind,'name':name,'required':required,'width':'fill','placeholder':plain(label)}
    if options is not None:item['options']=[{'text':plain(label),'value':value} for value,label in options]
    if kind=='picker_datetime' and isinstance(default,str):default=default[:16]
    if default not in (None,'',[]):item['selected_values' if kind=='multi_select_static' else 'initial_option' if kind=='select_static' else 'initial_datetime' if kind=='picker_datetime' else 'default_value']=default
    if kind=='input':item.update(input_type='multiline_text',max_length=1000)
    return [paragraph(label),item]

def card(title,elements):
    return {'schema':'2.0','config':{'wide_screen_mode':True,'update_multi':True},
            'header':{'title':plain(title)},'body':{'direction':'vertical','elements':elements}}


def bind_rendered_card(value):
    """Hidden, platform-preserved element ID binds the complete render snapshot.

    Feishu readback replaces callback values with opaque action IDs. A hash of
    the complete submitted card (including those values) lets an unknown PATCH
    be tied to the exact attempted view without displaying technical IDs.
    """
    import copy
    from .project_catalog import digest
    value=copy.deepcopy(value)
    elements=value.get('body',{}).get('elements',[])
    if elements:
        elements[0].pop('element_id',None)
        marker='uxv_'+digest(value)[:32]
        elements[0]['element_id']=marker
    return value


def card_readback_matches(actual,expected):
    import json
    if isinstance(actual,str):
        try:actual=json.loads(actual)
        except ValueError:return False
    if actual==expected:return True
    if not isinstance(actual,dict) or not isinstance(actual.get('json_card'),str):return False
    try:raw=json.loads(actual['json_card'])
    except ValueError:return False
    bound=bind_rendered_card(expected)
    elements=bound.get('body',{}).get('elements',[])
    if not elements or expected!=bound:return False
    marker=elements[0]['element_id']
    raw_elements=raw.get('body',{}).get('property',{}).get('elements',[])
    if not raw_elements or raw_elements[0].get('id')!=marker:return False
    def texts(node):
        if not isinstance(node,dict):return []
        if 'property' in node:node=dict(node['property'],tag=node.get('tag'))
        result=[]
        if node.get('tag')=='markdown' and isinstance(node.get('content'),str):
            import re
            link=re.fullmatch(r'\[([^\]]+)\]\((https?://[^)]+)\)',node['content'])
            if link:return ['link:'+link[1]+':'+link[2]]
            mentions=re.findall(r'<at id="([^"]+)"></at>',node['content'])
            if mentions and not re.sub(r'<at id="[^"]+"></at>','',node['content']).strip():return ['at:'+ident for ident in mentions]
        if node.get('tag')=='at' and isinstance(node.get('userID'),str):return ['at:'+node['userID']]
        if node.get('tag')=='link':
            url=node.get('url')
            if isinstance(url,dict):url=url.get('url')
            if isinstance(url,str):return ['link:'+node.get('content','')+':'+url]
        if node.get('tag') in ('plain_text','lark_md','markdown') and isinstance(node.get('content'),str) and node['content'].strip():result.append(node['content'])
        for key in ('title','text','elements','columns'):
            v=node.get(key)
            if isinstance(v,dict):result+=texts(v)
            elif isinstance(v,list):
                for child in v:result+=texts(child)
        return result
    # Reject missing/extra/reordered visible text as well as a wrong render ID.
    return texts(raw.get('header',{}))+texts(raw.get('body',{}))==texts(expected.get('header',{}))+texts(expected.get('body',{}))
