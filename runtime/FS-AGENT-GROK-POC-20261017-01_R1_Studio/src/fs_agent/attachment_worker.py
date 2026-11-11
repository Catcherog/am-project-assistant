"""Disposable parser process with CPU/data/output bounds and no network use."""
import base64
import io
import json
import resource
import sys
import zipfile
from pathlib import PurePosixPath
from .attachment_parser import MAX_FILE_BYTES,MAX_EXPANDED_BYTES,MAX_PAGES,MAX_OUTPUT_CHARS,PARSER_VERSION


def extract(data,source_id):
    segments=[];table_cells=[];limitations=[];used=0;parser='';version=''
    def add(text,location):
        nonlocal used
        text=text.strip()
        if not text:return
        remaining=MAX_OUTPUT_CHARS-used
        if len(text)>remaining:
            limitations.append('文字超出12000字符检查范围；仅检查已列来源位置')
            text=text[:max(0,remaining)]
        if text:
            segments.append({'ref':f'{source_id}:{len(segments)+1}','location':location,'original':text,'identity':'reported'});used+=len(text)
    if data.startswith(b'%PDF-'):
        import pypdf
        parser='pypdf';version=pypdf.__version__
        reader=pypdf.PdfReader(io.BytesIO(data),strict=True)
        if reader.is_encrypted:return {'read_status':'failed','limitation':'已提供加密PDF，未解密或读取','segments':[],'parser_name':parser,'parser_version':version}
        if len(reader.pages)>MAX_PAGES:limitations.append('PDF超过100页，仅检查前100页')
        for i,page in enumerate(reader.pages[:MAX_PAGES],1):
            text=page.extract_text() or ''
            if not text.strip():limitations.append(f'第{i}页无可读文字层；扫描图像未OCR')
            add(text,f'PDF第{i}页')
            if used>=MAX_OUTPUT_CHARS:
                if i<len(reader.pages):limitations.append('后续页未检查')
                break
        limitations.append('PDF仅提取文字层；复杂表格结构、图像及签章真实性需人工核验')
    elif data.startswith(b'PK'):
        from docx import Document
        from docx.table import Table
        from docx.text.paragraph import Paragraph
        import docx
        parser='python-docx';version=docx.__version__
        with zipfile.ZipFile(io.BytesIO(data)) as z:
            infos=z.infolist()
            if len(infos)>2000 or sum(i.file_size for i in infos)>MAX_EXPANDED_BYTES:raise ValueError('ZIP_EXPANSION_LIMIT')
            if any(i.flag_bits&1 or PurePosixPath(i.filename).is_absolute() or '..' in PurePosixPath(i.filename).parts for i in infos):raise ValueError('ZIP_LAYOUT_INVALID')
            if 'word/document.xml' not in z.namelist():raise ValueError('DOCX_BODY_MISSING')
            xml=z.read('word/document.xml')
            if any(tag in xml for tag in (b'<w:ins',b'<w:del',b'<w:moveFrom',b'<w:moveTo')):limitations.append('包含修订记录，修订内容未完整核验')
            if any(i.filename.startswith('word/media/') for i in infos):limitations.append('包含图片，图像与签审内容未核验')
            if any(i.filename.startswith(('word/header','word/footer','word/footnotes','word/endnotes','word/embeddings')) for i in infos):limitations.append('页眉、页脚、脚注或嵌入对象不属于本次正文检查范围')
        document=Document(io.BytesIO(data))
        def walk(container,prefix='DOCX正文',depth=0):
            if depth>12:raise ValueError('NESTED_TABLE_LIMIT')
            for n,block in enumerate(container.iter_inner_content(),1):
                if used>=MAX_OUTPUT_CHARS:limitations.append('后续正文未检查');return
                if isinstance(block,Paragraph):add(block.text,f'{prefix}/段落{n}')
                elif isinstance(block,Table):
                    seen=set()
                    for r,row in enumerate(block.rows,1):
                        for c,cell in enumerate(row.cells,1):
                            location=f'{prefix}/表格{n}/行{r}/列{c}'
                            # Preserve empty grid positions and merge ambiguity;
                            # text-only segments cannot establish field/value pairs.
                            props=cell._tc.tcPr
                            ambiguous=cell._tc in seen or cell._tc.grid_span!=1 or (props is not None and props.vMerge is not None)
                            ambiguous=ambiguous or bool(row.grid_cols_before or row.grid_cols_after)
                            table_cells.append({'location':location,'column':c,'columns':len(row.cells),'ambiguous':ambiguous})
                            # Merged cells can appear repeatedly in the grid.
                            if cell._tc in seen:continue
                            seen.add(cell._tc);walk(cell,location,depth+1)
        walk(document)
    else:raise ValueError('FORMAT_SIGNATURE_INVALID')
    if not segments:limitations.append('未提取到可检查文字，不能据此判断材料未提供')
    return {'read_status':'partial' if limitations else 'complete','limitation':'；'.join(dict.fromkeys(limitations)) or None,
            'segments':segments,'table_cells':table_cells,'parser_name':parser,'parser_version':version,'parser_policy_version':PARSER_VERSION,'checked_characters':used}


def main():
    resource.setrlimit(resource.RLIMIT_CPU,(12,12))
    try:resource.setrlimit(resource.RLIMIT_DATA,(512*1024*1024,512*1024*1024))
    except (ValueError,OSError):pass  # macOS uses the parent RSS watchdog below.
    resource.setrlimit(resource.RLIMIT_FSIZE,(2*1024*1024,2*1024*1024))
    request=json.loads(sys.stdin.buffer.read(12*1024*1024))
    data=base64.b64decode(request['data'],validate=True)
    if len(data)>MAX_FILE_BYTES:raise ValueError('FILE_LIMIT')
    # Fail any accidental parser attempt to resolve a document link over network.
    def audit(event,args):
        if event.startswith('socket.') or event in ('subprocess.Popen','os.system'):raise RuntimeError('PARSER_NETWORK_OR_EXEC_DENIED')
    # Trusted library initialization may inspect the local Python architecture.
    # The untrusted document is parsed only after the audit boundary is active.
    import pypdf
    import docx
    sys.addaudithook(audit)
    print(json.dumps(extract(data,request['source_id']),ensure_ascii=False))

if __name__=='__main__':
    try:main()
    except Exception:sys.exit(2)
