#!/usr/bin/env python3
"""JSON stdin bridge to an explicitly configured, already verified Feishu CLI.

No CLI syntax is guessed here. --config defines argv templates for list and get.
The bridge uses shell=False, a separate user identity, approved chat inventory,
bounded subprocess output, and exact REST-shaped JSON responses. No LLM is used.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import selectors
import signal
import sqlite3
import string
import subprocess
import sys
import time
from urllib.parse import unquote, urlsplit


class ReadBridgeError(RuntimeError):
    pass


_PARAMS = {'container_id', 'container_id_type', 'start_time', 'end_time', 'page_size',
           'page_token', 'sort_type', 'user_id_type'}
_FIELDS = _PARAMS | {'identity', 'url', 'message_id', 'params_json'}


def _argv(template, fields):
    if not isinstance(template, list) or not template or any(not isinstance(x, str) for x in template):
        raise ReadBridgeError('configured_argv_required')
    formatter, result = string.Formatter(), []
    for argument in template:
        for _, field, spec, conversion in formatter.parse(argument):
            if field is not None and (field not in _FIELDS or spec or conversion):
                raise ReadBridgeError('unsupported_argument_placeholder')
        result.append(argument.format_map(fields))
    return result


def _run(argv, *, timeout, output_limit):
    if not Path(argv[0]).is_absolute() or not os.access(argv[0], os.X_OK):
        raise ReadBridgeError('installed_absolute_cli_binary_required')
    if os.name != 'posix':
        raise ReadBridgeError('posix_process_groups_required')
    process = subprocess.Popen(argv, shell=False, stdin=subprocess.DEVNULL,
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                               start_new_session=True, bufsize=0)
    selector = selectors.DefaultSelector()
    output, error_size = bytearray(), 0
    deadline = time.monotonic() + timeout
    try:
        for name, stream in [('out',process.stdout),('err',process.stderr)]:
            os.set_blocking(stream.fileno(), False)
            selector.register(stream, selectors.EVENT_READ, name)
        while selector.get_map():
            remaining = deadline-time.monotonic()
            if remaining <= 0:
                raise ReadBridgeError('cli_read_timeout')
            for event, _ in selector.select(min(.1, remaining)):
                chunk = os.read(event.fileobj.fileno(), 65536)
                if not chunk:
                    selector.unregister(event.fileobj)
                    event.fileobj.close()
                elif event.data == 'out':
                    if len(output)+len(chunk) > output_limit:
                        raise ReadBridgeError('cli_read_output_limit')
                    output.extend(chunk)
                else:
                    error_size += len(chunk)
                    if error_size > 262144:
                        raise ReadBridgeError('cli_read_stderr_limit')
        try:
            exit_code = process.wait(timeout=max(.001,deadline-time.monotonic()))
        except subprocess.TimeoutExpired:
            raise ReadBridgeError('cli_read_timeout') from None
        if exit_code != 0:
            raise ReadBridgeError('cli_read_failed')
        return bytes(output)
    finally:
        selector.close()
        for signum in (signal.SIGTERM, signal.SIGKILL):
            try:
                os.killpg(process.pid, signum)
            except ProcessLookupError:
                pass
        process.wait(timeout=2)
        for stream in (process.stdout, process.stderr):
            stream.close()


class FeishuCLIReadBridge:
    def __init__(self, config, *, state_path):
        self.identity = config.get('identity')
        self.chats = frozenset(config.get('allowed_chats', []))
        if (not isinstance(self.identity, str) or not self.identity or self.identity in {'am_bot','bot'}
                or not self.chats or any(not isinstance(chat,str) or not chat for chat in self.chats)):
            raise ReadBridgeError('explicit_user_identity_and_approved_chats_required')
        self.commands = config.get('commands', {})
        self.timeout = float(config.get('timeout_seconds', 25))
        self.output_limit = int(config.get('max_output_bytes', 8000000))
        if not 0 < self.timeout <= 60 or not 1024 <= self.output_limit <= 16000000:
            raise ReadBridgeError('invalid_read_limits')
        self.state_path = str(Path(state_path).resolve())
        Path(self.state_path).parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.state_path) as db:
            db.execute('CREATE TABLE IF NOT EXISTS known_threads(identity TEXT,thread_id TEXT,chat_id TEXT,PRIMARY KEY(identity,thread_id))')
        Path(self.state_path).chmod(0o600)

    def __call__(self, request):
        if not isinstance(request, dict) or request.get('operation') != 'feishu_read' or request.get('identity') != self.identity:
            raise ReadBridgeError('read_operation_or_identity_mismatch')
        parsed = urlsplit(request.get('url',''))
        if (parsed.scheme,parsed.netloc) != ('https','open.feishu.cn') or parsed.query or parsed.fragment:
            raise ReadBridgeError('feishu_read_origin_required')
        params = request.get('params', {})
        if not isinstance(params,dict) or set(params)-_PARAMS or any(not isinstance(v,(str,int)) or isinstance(v,bool) for v in params.values()):
            raise ReadBridgeError('unsupported_read_parameters')
        if parsed.path == '/open-apis/im/v1/messages':
            operation, message_id = 'list', ''
            container, kind = params.get('container_id'), params.get('container_id_type')
            if kind == 'chat':
                if container not in self.chats:
                    raise ReadBridgeError('chat_outside_approved_scope')
            elif kind == 'thread':
                with sqlite3.connect(self.state_path) as db:
                    known = db.execute('SELECT chat_id FROM known_threads WHERE identity=? AND thread_id=?',
                                       (self.identity,container)).fetchone()
                if not known or known[0] not in self.chats:
                    raise ReadBridgeError('thread_not_observed_in_approved_chat')
            else:
                raise ReadBridgeError('supported_container_required')
        elif re.fullmatch('/open-apis/im/v1/messages/om_[A-Za-z0-9_-]+', parsed.path):
            operation, message_id = 'get', unquote(parsed.path.rsplit('/',1)[-1])
            if set(params)-{'user_id_type'}:
                raise ReadBridgeError('exact_message_get_parameters_required')
        else:
            raise ReadBridgeError('only_message_list_and_exact_get_are_bound')
        command = self.commands.get(operation)
        if not isinstance(command,dict):
            raise ReadBridgeError('verified_read_command_is_not_configured')
        fields = {name:'' for name in _FIELDS}
        fields.update({name:str(value) for name,value in params.items()})
        fields.update(identity=self.identity,url=request['url'],message_id=message_id,
                      params_json=json.dumps(params,ensure_ascii=False,separators=(',',':')))
        argv = _argv(command.get('argv'),fields)
        for optional in command.get('optional_args',[]):
            if optional.get('when') not in _PARAMS:
                raise ReadBridgeError('invalid_optional_argument_condition')
            if fields[optional['when']]:
                argv += _argv(optional['argv'],fields)
        raw = _run(argv,timeout=self.timeout,output_limit=self.output_limit)
        try:
            response = json.loads(raw)
            for component in command.get('response_path',[]):
                response = response[component]
        except (ValueError,KeyError,TypeError):
            raise ReadBridgeError('cli_must_return_one_feishu_json_object') from None
        if command.get('response_format') == 'lark_cli_v1':
            # The installed user CLI wraps the unchanged API data in
            # {ok, identity, data}; explicitly bind and check that envelope.
            if (not isinstance(response, dict) or response.get('ok') is not True
                    or response.get('identity') != 'user' or not isinstance(response.get('data'), dict)):
                raise ReadBridgeError('verified_user_cli_response_required')
            response = {'code': 0, 'data': response['data']}
        if not isinstance(response,dict) or type(response.get('code')) is not int:
            raise ReadBridgeError('raw_feishu_response_required')
        if response['code'] != 0:
            return response
        items = response.get('data',{}).get('items')
        if not isinstance(items,list):
            raise ReadBridgeError('message_items_required')
        if operation=='get' and (len(items)!=1 or items[0].get('message_id')!=message_id):
            raise ReadBridgeError('exact_message_response_mismatch')
        threads = []
        for item in items:
            if not isinstance(item,dict) or item.get('chat_id') not in self.chats:
                raise ReadBridgeError('response_chat_outside_approved_scope')
            if operation=='list' and params['container_id_type']=='chat' and item['chat_id']!=params['container_id']:
                raise ReadBridgeError('response_chat_mismatch')
            if operation=='list' and params['container_id_type']=='thread' and item.get('thread_id')!=params['container_id']:
                raise ReadBridgeError('response_thread_mismatch')
            if item.get('thread_id'):
                threads.append((self.identity,item['thread_id'],item['chat_id']))
        with sqlite3.connect(self.state_path) as db:
            db.executemany('INSERT OR REPLACE INTO known_threads VALUES(?,?,?)',threads)
        return response


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--config',required=True)
    args = parser.parse_args(argv)
    try:
        config_path = Path(args.config).resolve()
        config = json.loads(config_path.read_text(encoding='utf-8'))
        state_path = config.get('state_path') or str(config_path.with_suffix('.state.sqlite3'))
        bridge = FeishuCLIReadBridge(config,state_path=state_path)
        raw = sys.stdin.buffer.read(1048577)
        if len(raw)>1048576:
            raise ReadBridgeError('input_request_limit')
        response = bridge(json.loads(raw))
        print(json.dumps(response,ensure_ascii=False,separators=(',',':')))
        return 0
    except (ReadBridgeError,ValueError,OSError,KeyError,TypeError) as exc:
        # Do not return CLI stderr, credentials, or private response fragments.
        code = str(exc) if isinstance(exc,ReadBridgeError) else type(exc).__name__
        print(json.dumps({'status':'error','error':code},ensure_ascii=False))
        return 1


if __name__=='__main__':
    raise SystemExit(main())
