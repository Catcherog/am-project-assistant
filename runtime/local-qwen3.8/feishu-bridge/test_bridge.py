import json
from pathlib import Path
import tempfile
import time
import unittest
from unittest.mock import patch

from bridge import Bridge, TTL


class BridgeTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.calls, self.sent = [], []
        self.config = {'app_id':'cli_test','allowed_open_ids':['ou_owner'],
                       'state_directory':self.tmp.name}
        def complete(messages):
            self.calls.append(messages)
            return '合成回复'
        self.bridge = Bridge(self.config, lambda mid,text:self.sent.append((mid,text)), complete)

    def tearDown(self):
        self.bridge.db.close()
        self.tmp.cleanup()

    def event(self, mid='om_1', chat='oc_1', text='合成问题'):
        return {'header':{'app_id':'cli_test','event_type':'im.message.receive_v1','tenant_key':'tenant'},
                'event':{'sender':{'sender_type':'user','sender_id':{'open_id':'ou_owner'},'tenant_key':'tenant'},
                         'message':{'message_id':mid,'chat_id':chat,'chat_type':'p2p','message_type':'text',
                                    'content':json.dumps({'text':text}), 'create_time':str(int(time.time()*1000))}}}

    def test_authorization_and_input(self):
        for mutate in [lambda e:e['header'].update(app_id='cli_other'),
                       lambda e:e['event']['sender']['sender_id'].update(open_id='ou_other'),
                       lambda e:e['event']['message'].update(chat_type='group'),
                       lambda e:e['event']['sender'].update(tenant_key='other'),
                       lambda e:e['event']['message'].update(content='{"text":42}')]:
            event=self.event(); mutate(event)
            self.assertFalse(self.bridge.ingest(event))
        self.assertFalse(self.bridge.process_one())

    def test_duplicate_and_no_system_prompt(self):
        event=self.event()
        self.assertTrue(self.bridge.ingest(event))
        self.assertFalse(self.bridge.ingest(event))
        self.assertEqual(self.calls, [])  # Callback never waits on inference.
        self.assertTrue(self.bridge.process_one())
        self.assertFalse(self.bridge.ingest(event))
        self.assertFalse(self.bridge.process_one())
        self.assertEqual(len(self.sent),1)
        self.assertEqual(self.calls, [[{'role':'user','content':'合成问题'}]])

    def test_all_private_users_with_isolated_context(self):
        self.config['access_mode'] = 'all_private_users'
        for i,user in enumerate(['ou_owner','ou_colleague','ou_owner','ou_colleague']):
            event = self.event(str(i),chat='same_synthetic_chat')
            event['event']['sender']['sender_id']['open_id'] = user
            self.assertTrue(self.bridge.ingest(event))
            self.bridge.process_one()
        self.assertEqual([len(c) for c in self.calls],[1,1,3,3])
        group=self.event('group');group['event']['message']['chat_type']='group'
        self.assertFalse(self.bridge.ingest(group))
        missing=self.event('missing');missing['event']['sender']['sender_id']={}
        self.assertFalse(self.bridge.ingest(missing))

    def test_history_isolation_and_reset(self):
        for mid,chat in [('a','chat1'),('b','chat2'),('c','chat1')]:
            self.bridge.ingest(self.event(mid,chat)); self.bridge.process_one()
        self.assertEqual([len(c) for c in self.calls],[1,1,3])
        self.bridge.ingest(self.event('d','chat1','/new'));self.bridge.process_one()
        self.bridge.ingest(self.event('e','chat1'));self.bridge.process_one()
        self.bridge.ingest(self.event('f','chat2'));self.bridge.process_one()
        self.assertEqual([len(c) for c in self.calls],[1,1,3,1,3])

    def test_expiration(self):
        self.bridge.ingest(self.event()); self.bridge.process_one()
        with patch('bridge.time.time', return_value=time.time()+TTL+1):
            self.bridge.prune()
        self.assertEqual(self.bridge.db.execute('SELECT count(*) FROM turns').fetchone()[0],0)
        event=self.event('old'); event['event']['message']['create_time']=str(int((time.time()-TTL-1)*1000))
        self.assertFalse(self.bridge.ingest(event))

    def test_ambiguous_send_never_replayed(self):
        def fail(*args):raise TimeoutError()
        self.bridge.reply=fail
        self.bridge.ingest(self.event());self.bridge.process_one()
        self.assertEqual(self.bridge.db.execute('SELECT state FROM inbox').fetchone()[0],'delivery_unknown')
        self.assertFalse(self.bridge.process_one())
        self.assertFalse(self.bridge.ingest(self.event()))
        self.assertEqual(len(self.calls),1)

    def test_bounded_queue(self):
        for i in range(8):self.assertTrue(self.bridge.ingest(self.event(str(i))))
        with self.assertRaisesRegex(RuntimeError,'QUEUE_FULL'):
            self.bridge.ingest(self.event('ninth'))

    def test_restart_does_not_resend(self):
        self.bridge.ingest(self.event())
        with self.bridge.db:self.bridge.db.execute("UPDATE inbox SET state='sending'")
        other=Bridge(self.config,lambda *args:self.fail('must not send'))
        try:
            self.assertFalse(other.process_one())
            self.assertEqual(other.db.execute('SELECT state FROM inbox').fetchone()[0],'delivery_unknown')
        finally:other.db.close()


if __name__ == '__main__':unittest.main()
