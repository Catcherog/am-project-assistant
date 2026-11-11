"""Live, bounded document inputs. No credential reads and no model-owned permissions."""
from __future__ import annotations

import hashlib
import json
import subprocess
import tempfile
import time
import re
from pathlib import Path
from urllib.parse import urlsplit

from .config import BoundaryError
from am_doc_engine.models import fingerprint

TEXT_KEYS = {1: 'page', 2: 'text', **{i: 'heading'+str(i-2) for i in range(3,12)},
             12: 'bullet', 13: 'ordered', 14: 'code', 15: 'quote', 17: 'todo'}
CONTAINERS = {19, 22, 24, 25, 31, 32, 34}


def text_of(block):
    key = TEXT_KEYS.get(block.get('block_type'))
    return ''.join(e.get('text_run', {}).get('content', '') for e in block.get(key, {}).get('elements', [])) if key else ''


def token_from_url(url, hosts):
    value = urlsplit(url)
    parts = value.path.strip('/').split('/')
    if value.scheme != 'https' or value.hostname not in hosts or value.username or value.password or len(parts) != 2:
        raise BoundaryError('DOCUMENT_SOURCE_URL_DENIED')
    if parts[0] not in ('wiki', 'docx') or not parts[1].isalnum():
        raise BoundaryError('DOCUMENT_SOURCE_TYPE_UNSUPPORTED')
    return parts


class NativeDocs:
    """Uses the original AM SDK client; does not create a socket consumer or share files."""
    def __init__(self, client, guard, *, deadline=None, write_guard=None):
        self.client, self.guard, self.deadline = client, guard, deadline
        self.write_guard = write_guard or guard

    def check(self):
        if self.guard() is False:
            raise BoundaryError('DOCUMENT_SCOPE_OR_REVISION_CHANGED')
        if self.deadline is not None and time.monotonic() >= self.deadline:
            raise BoundaryError('EXECUTION_TIMEOUT')

    def call(self, method, path, *, query=None, body=None):
        import lark_oapi as lark
        self.check()
        if method!='GET' and self.write_guard() is False:
            raise BoundaryError('DOCUMENT_WRITE_AUTH_CHANGED')
        builder = (lark.BaseRequest.builder().http_method(getattr(lark.HttpMethod, method))
                   .uri(path).token_types({lark.AccessTokenType.TENANT}))
        if query: builder.queries([(k, str(v)) for k, v in query.items()])
        if body is not None: builder.body(body)
        response = self.client.request(builder.build())
        if len(response.raw.content) > 8_000_000: raise BoundaryError('DOCUMENT_RESPONSE_LIMIT')
        try: value = json.loads(response.raw.content)
        except (ValueError, TypeError): raise BoundaryError('DOCUMENT_RESPONSE_INVALID') from None
        if value.get('code') != 0: raise BoundaryError('DOCUMENT_PLATFORM_'+str(value.get('code', 'UNKNOWN')))
        # A write receipt must be journalled even if cancellation arrives in flight.
        if method == 'GET': self.check()
        return value.get('data', {})

    def resolve(self, url, hosts):
        kind, token = token_from_url(url, hosts)
        if kind == 'wiki':
            node = self.call('GET', '/open-apis/wiki/v2/spaces/get_node', query={'token': token})['node']
            if node.get('obj_type') != 'docx': raise BoundaryError('DOCUMENT_SOURCE_TYPE_UNSUPPORTED')
            token = node['obj_token']
        return token

    def restrict_copy(self,token):
        """Only called for this action's newly created, still-empty template copy."""
        path='/open-apis/drive/v1/permissions/'+token+'/public'
        self.call('PATCH',path,query={'type':'docx'},body={'external_access':False,'invite_external':False,
            'link_share_entity':'closed','share_entity':'only_full_access'})
        permission=self.call('GET',path,query={'type':'docx'}).get('permission_public',{})
        if permission.get('link_share_entity')!='closed' or permission.get('external_access') is not False:
            raise BoundaryError('DOCUMENT_PRIVATE_ACCESS_UNVERIFIED')

    def read(self, token):
        path = '/open-apis/docx/v1/documents/'+token
        meta = self.call('GET', path)['document']
        revision = meta['revision_id']; blocks = []; seen = set(); page = None; pages = 0
        for _ in range(40):
            # AM may read the current document but cannot read the history endpoint
            # selected by a positive revision (1770032). Fence latest-page reads
            # with metadata on both sides; never accept a moving version.
            query = {'document_revision_id': -1, 'page_size': 500}
            if page: query['page_token'] = page
            data = self.call('GET', path+'/blocks', query=query); pages += 1
            if not isinstance(data.get('items'), list): raise BoundaryError('DOCUMENT_BLOCKS_INVALID')
            blocks.extend(data['items'])
            if not data.get('has_more'): break
            page = data.get('page_token')
            if not page or page in seen: raise BoundaryError('DOCUMENT_PAGINATION_INVALID')
            seen.add(page)
        else: raise BoundaryError('DOCUMENT_PAGINATION_LIMIT')
        after = self.call('GET', path)['document']
        if after['revision_id'] != revision: raise BoundaryError('DOCUMENT_SOURCE_CHANGED_DURING_READ')
        by_id = {b['block_id']: b for b in blocks}
        if len(by_id) != len(blocks) or token not in by_id: raise BoundaryError('DOCUMENT_BLOCKS_INCOMPLETE')
        missing = {child for b in blocks for child in b.get('children', [])} - set(by_id)
        if missing: raise BoundaryError('DOCUMENT_BLOCKS_INCOMPLETE')
        paths = {}; ordered = []
        def visit(ident, route):
            if ident in paths: raise BoundaryError('DOCUMENT_BLOCK_GRAPH_INVALID')
            paths[ident] = route; block = by_id[ident]; ordered.append(block)
            for i, child in enumerate(block.get('children', [])): visit(child, route+(i,))
        visit(token, ())
        if len(paths) != len(blocks): raise BoundaryError('DOCUMENT_ORPHAN_BLOCKS')
        unsupported = []
        for b in ordered:
            kind = b['block_type']
            if kind not in TEXT_KEYS and kind not in CONTAINERS: unsupported.append(b['block_id'])
            if kind in TEXT_KEYS:
                for e in b.get(TEXT_KEYS[kind], {}).get('elements', []):
                    if set(e) - {'text_run'}: unsupported.append(b['block_id'])
        return {'token': token, 'revision': revision, 'title': meta['title'], 'blocks': ordered,
                'paths': {k: list(v) for k,v in paths.items()}, 'pages': pages,
                'read_state': 'partial' if unsupported else 'complete', 'unread_blocks': sorted(set(unsupported)),
                'text': '\n'.join(text_of(b) for b in ordered if text_of(b)), 'hash': fingerprint(ordered)}


class OwnerCatalog:
    """Existing user CLI is usable only by the gateway owner, with a pinned CLI identity."""
    def __init__(self, settings, principal, owner, guard, *, deadline):
        self.cfg, self.guard, self.deadline = settings, guard, deadline
        if principal['user'] != owner: raise BoundaryError('DOCUMENT_CATALOG_OWNER_ONLY')
        self.binary = Path(settings['cli'])
        if hashlib.sha256(self.binary.read_bytes()).hexdigest() != settings['cli_sha256']:
            raise BoundaryError('DOCUMENT_CLI_CHANGED')

    def run(self, args, cwd, *, after_guard=True):
        if self.guard() is False: raise BoundaryError('DOCUMENT_SCOPE_OR_REVISION_CHANGED')
        remaining = self.deadline-time.monotonic()
        if remaining <= 0: raise BoundaryError('EXECUTION_TIMEOUT')
        prefix=[str(self.binary)]
        if self.cfg.get('cli_profile'):prefix+=['--profile',self.cfg['cli_profile']]
        try: result = subprocess.run([*prefix, *args, '--as', 'user'], cwd=cwd, capture_output=True, timeout=min(remaining,30))
        except subprocess.TimeoutExpired: raise BoundaryError('DOCUMENT_CATALOG_TIMEOUT') from None
        if result.returncode or len(result.stdout)>2_000_000: raise BoundaryError('DOCUMENT_CATALOG_UNAVAILABLE')
        try: value=json.loads(result.stdout)
        except ValueError: raise BoundaryError('DOCUMENT_CATALOG_RESPONSE_INVALID') from None
        if value.get('ok') is False: raise BoundaryError('DOCUMENT_CATALOG_UNAVAILABLE')
        if after_guard and self.guard() is False: raise BoundaryError('DOCUMENT_SCOPE_OR_REVISION_CHANGED')
        return value

    def read(self):
        with tempfile.TemporaryDirectory(prefix='amdoc-catalog-') as directory:
            who = self.run(['whoami'], directory)
            if (who.get('identity') != 'user' or not who.get('available')
                    or who.get('appId') != self.cfg['cli_app_id']
                    or who.get('onBehalfOf', {}).get('openId') != self.cfg['cli_user_id']):
                raise BoundaryError('DOCUMENT_CLI_IDENTITY_CHANGED')
            rows = []; offset = 0; revision = None
            for _ in range(10):
                manifest = self.run(['base', '+record-list', '--base-token', self.cfg['base_token'],
                    '--table-id', self.cfg['table_id'], '--format', 'ndjson', '--limit', '2000',
                    '--offset', str(offset), '--output', './records-'+str(offset)+'.ndjson'], directory)
                if revision is None: revision = manifest.get('rev')
                if revision is None or manifest.get('rev') != revision: raise BoundaryError('DOCUMENT_CATALOG_CHANGED')
                path = Path(manifest['record_file']).resolve()
                if not path.is_relative_to(Path(directory).resolve()) or path.stat().st_size>4_000_000:
                    raise BoundaryError('DOCUMENT_CATALOG_OUTPUT_INVALID')
                page = [json.loads(line) for line in path.read_text().splitlines()]
                if len(page) != manifest['records_count']: raise BoundaryError('DOCUMENT_CATALOG_PARTIAL')
                rows.extend(page)
                if not manifest.get('has_more'): break
                next_offset = manifest.get('next_offset')
                if not isinstance(next_offset, int) or next_offset <= offset: raise BoundaryError('DOCUMENT_CATALOG_PARTIAL')
                offset = next_offset
            else: raise BoundaryError('DOCUMENT_CATALOG_PARTIAL')
        if len({r['record_id'] for r in rows}) != len(rows): raise BoundaryError('DOCUMENT_CATALOG_DUPLICATE')
        return {'revision': str(revision), 'rows': rows, 'hash': fingerprint(rows)}


class OwnerNativeDocs(NativeDocs):
    """Owner-bound publication via the existing CLI identity, never delegated to peers.

The AM bot's document read grants do not imply create/write grants. This port uses
the already signed-in owner only for an explicit owner document action. It neither
grants permissions nor falls back to another account when access is denied.
"""
    def __init__(self,settings,principal,owner,guard,*,deadline,write_guard=None):
        super().__init__(None,guard,deadline=deadline,write_guard=write_guard)
        self.runner=OwnerCatalog(settings,principal,owner,guard,deadline=deadline)
        with tempfile.TemporaryDirectory(prefix='amdoc-identity-') as directory:
            who=self.runner.run(['whoami'],directory)
        if (who.get('identity')!='user' or not who.get('available') or who.get('appId')!=settings['cli_app_id']
                or who.get('onBehalfOf',{}).get('openId')!=settings['cli_user_id']):
            raise BoundaryError('DOCUMENT_CLI_IDENTITY_CHANGED')

    def call(self,method,path,*,query=None,body=None):
        self.check()
        allowed=(method=='GET' and (re.fullmatch(r'/open-apis/docx/v1/documents/[A-Za-z0-9]+(?:/blocks)?',path)
                 or re.fullmatch(r'/open-apis/drive/v1/permissions/[A-Za-z0-9]+/public',path)
                 or path in ('/open-apis/drive/v1/files','/open-apis/wiki/v2/spaces/get_node'))
            or method=='POST' and re.fullmatch(r'/open-apis/drive/v1/files/[A-Za-z0-9]+/copy',path)
            or method=='PATCH' and re.fullmatch(r'/open-apis/docx/v1/documents/[A-Za-z0-9]+/blocks/batch_update',path))
        if not allowed:raise BoundaryError('DOCUMENT_OWNER_ENDPOINT_DENIED')
        if method!='GET' and self.write_guard() is False:raise BoundaryError('DOCUMENT_WRITE_AUTH_CHANGED')
        args=['api',method,path]
        if query:args+=['--params',json.dumps(query)]
        with tempfile.TemporaryDirectory(prefix='amdoc-native-') as directory:
            if body is not None:
                payload=Path(directory)/'body.json';payload.write_text(json.dumps(body,ensure_ascii=False));payload.chmod(0o600)
                args+=['--data','@'+str(payload)]
            result=self.runner.run(args,directory,after_guard=method=='GET')
        if result.get('code',0)!=0:raise BoundaryError('DOCUMENT_PLATFORM_'+str(result['code']))
        return result.get('data',{})

    def restrict_copy(self,token):
        self.check()
        if self.write_guard() is False:raise BoundaryError('DOCUMENT_WRITE_AUTH_CHANGED')
        # Saving an owner draft authorizes keeping that new draft private. These
        # fixed values only reduce the copied template's link-sharing defaults.
        body={'external_access':False,'invite_external':False,'link_share_entity':'closed','share_entity':'only_full_access'}
        with tempfile.TemporaryDirectory(prefix='amdoc-private-') as directory:
            self.runner.run(['drive','permission.public','patch','--token',token,'--type','docx',
                '--data',json.dumps(body),'--yes'],directory,after_guard=False)
        permission=self.call('GET','/open-apis/drive/v1/permissions/'+token+'/public',query={'type':'docx'}).get('permission_public',{})
        if permission.get('link_share_entity')!='closed' or permission.get('external_access') is not False:
            raise BoundaryError('DOCUMENT_PRIVATE_ACCESS_UNVERIFIED')
