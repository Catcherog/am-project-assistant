"""Bounded local DOCX/PDF extraction. Never follows links or runs document content."""
import base64
import hashlib
import json
import subprocess
import sys
import time
from pathlib import Path

MAX_FILE_BYTES=8*1024*1024
MAX_EXPANDED_BYTES=32*1024*1024
MAX_PAGES=100
MAX_OUTPUT_CHARS=12000
PARSER_SECONDS=15
PARSER_VERSION='am-attachments-2'


def parse_attachment(name,data,source):
    source=dict(source,parser_name='am-local-attachments',parser_version=PARSER_VERSION,mime_type='application/pdf' if data.startswith(b'%PDF-') else 'application/vnd.openxmlformats-officedocument.wordprocessingml.document')
    if len(data)>MAX_FILE_BYTES:
        return dict(source,read_status='failed',limitation='文件超过8MiB上限，未作全文检查',segments=[])
    request={'name':name,'data':base64.b64encode(data).decode(),'source_id':source['id']}
    try:
        started=time.monotonic()
        process=subprocess.Popen([sys.executable,'-I','-m','fs_agent.attachment_worker'],stdin=subprocess.PIPE,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
        initial=json.dumps(request).encode()
        try:
            while True:
                try:
                    out,err=process.communicate(input=initial,timeout=.2)
                    break
                except subprocess.TimeoutExpired:
                    initial=None
                    if time.monotonic()-started>PARSER_SECONDS:raise subprocess.TimeoutExpired(process.args,PARSER_SECONDS)
                    memory=subprocess.run(['/bin/ps','-o','rss=','-p',str(process.pid)],capture_output=True,timeout=1)
                    try:rss=int(memory.stdout.strip() or b'0')*1024
                    except ValueError:rss=0
                    if rss>512*1024*1024:raise ValueError('PARSER_MEMORY_LIMIT')
        finally:
            if process.poll() is None:process.kill();process.communicate()
        if process.returncode or len(out)>256*1024:raise ValueError('PARSER_FAILED')
        parsed=json.loads(out)
        if parsed['read_status'] not in ('complete','partial','failed'):raise ValueError('PARSER_STATE_INVALID')
        return dict(source,**parsed)
    except subprocess.TimeoutExpired:
        return dict(source,read_status='failed',limitation='解析超过15秒限制，未作完整检查',segments=[])
    except (ValueError,OSError,KeyError):
        return dict(source,read_status='failed',limitation='文件损坏、解析失败或资源超限；文件已提供，无法完整核验',segments=[])
