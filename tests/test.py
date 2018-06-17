import time
import unittest
import uuid

import redis
from tests import common
# This script should be run from within the credis/ directory

_CLIENT_ID = str(uuid.uuid4())  # Used as the channel name receiving acks.


class Basics(unittest.TestCase):
    def testAck(self):
        common.Start()

        head_client = redis.StrictRedis("127.0.0.1", 6370)
        tail_client = redis.StrictRedis("127.0.0.1", 6371)
        # The ack client needs to be separate, since subscriptions
        # are blocking
        ack_client = redis.StrictRedis("127.0.0.1", 6371)
        p = ack_client.pubsub(ignore_subscribe_messages=True)
        p.subscribe(_CLIENT_ID)
        time.sleep(0.5)
        p.get_message()
        ssn = head_client.execute_command("MEMBER.PUT", "task_spec",
                                          "some_random_value", _CLIENT_ID)
        time.sleep(0.5)
        put_ack = p.get_message()

        assert ssn == 0
        assert int(put_ack["data"]) == ssn  # Check the sequence number


class GcsModeTests(unittest.TestCase):
    def setUp(self):
        self.ack_client = common.AckClient()
        self.master_client = common.MasterClient()
        self.head_client = common.GetHeadFromMaster(self.master_client)

    @classmethod
    def tearDownClass(cls):
        for p in common.INIT_PORTS:
            common.KillNode(port=p, stateless=True)

    def testNormal(self):
        common.Start()
        # By default, the execution mode is kNormal, which disallows flush/ckpt.
        with self.assertRaises(redis.exceptions.ResponseError) as ctx:
            self.ack_client.execute_command('TAIL.CHECKPOINT')
            self.assertTrue('GcsMode indicates no checkpointing support' in
                            str(ctx.exception))

        with self.assertRaises(redis.exceptions.ResponseError) as ctx:
            self.head_client.execute_command('HEAD.FLUSH')
        print(str(ctx.exception))
        self.assertTrue(
            'GcsMode indicates no flushing support' in str(ctx.exception))

    def testCkptOnly(self):
        common.Start(gcs_mode=common.GCS_CKPTONLY)
        self.ack_client.execute_command('TAIL.CHECKPOINT')

        with self.assertRaises(redis.exceptions.ResponseError) as ctx:
            self.head_client.execute_command('HEAD.FLUSH')
        print(str(ctx.exception))
        self.assertTrue(
            'GcsMode indicates no flushing support' in str(ctx.exception))

    def testCkptFlush(self):
        common.Start(gcs_mode=common.GCS_CKPTFLUSH)
        self.ack_client.execute_command('TAIL.CHECKPOINT')
        self.head_client.execute_command('HEAD.FLUSH')


class CheckpointFlush(unittest.TestCase):
    def setUp(self):
        self.ack_client = common.AckClient()
        self.master_client = common.MasterClient()
        self.head_client = common.GetHeadFromMaster(self.master_client)

    @classmethod
    def tearDownClass(cls):
        for p in common.INIT_PORTS:
            common.KillNode(port=p, stateless=True)

    def testCannotFlush(self):
        common.Start(gcs_mode=common.GCS_CKPTFLUSH)
        r = self.head_client.execute_command('HEAD.FLUSH')
        self.assertEqual(0, r)

    def testBasics(self):
        common.Start(gcs_mode=common.GCS_CKPTFLUSH)

        self.head_client.execute_command('MEMBER.PUT', 'k1', 'v1', _CLIENT_ID)

        self.assertEqual(b'v1', self.ack_client.execute_command('READ', 'k1'))

        # 1 entry checkpointed.
        self.assertEqual(1, self.ack_client.execute_command('TAIL.CHECKPOINT'))

        # 0 entry checkpointed.
        self.assertEqual(0, self.ack_client.execute_command('TAIL.CHECKPOINT'))

        self.head_client.execute_command('MEMBER.PUT', 'k1', 'v2', _CLIENT_ID)
        self.assertEqual(1, self.ack_client.execute_command('TAIL.CHECKPOINT'))

        self.head_client.execute_command('MEMBER.PUT', 'k1', 'v3', _CLIENT_ID)

        # Process k1 (first seqnum).  Physically, 0 key has been flushed out of
        # _redis_ memory state, because k1 has 2 dirty writes.
        self.assertEqual(0, self.head_client.execute_command('HEAD.FLUSH'))

        # Process k1 (second seqnum).
        self.assertEqual(0, self.head_client.execute_command('HEAD.FLUSH'))
        # It remains in memory because of a dirty write (k1, v3).
        self.assertEqual(b'v3', self.ack_client.execute_command('GET k1'))

        # Now all seqnums checkpointed.
        self.assertEqual(1, self.ack_client.execute_command('TAIL.CHECKPOINT'))
        # Process k1 (3rd seqnum).  1 means it's physically flushed.
        self.assertEqual(1, self.head_client.execute_command('HEAD.FLUSH'))

        # Check that redis's native GET returns nothing.
        self.assertIsNone(self.ack_client.execute_command('GET k1'))
        # READ is credis' read mechanism, can read checkpoints.
        self.assertEqual(b'v3', self.ack_client.execute_command('READ k1'))

    def testFlushOnly(self):
        common.Start(gcs_mode=common.GCS_FLUSHONLYUNSAFE)

        self.head_client.execute_command('MEMBER.PUT', 'k1', 'v1', _CLIENT_ID)
        self.assertEqual(1, self.head_client.execute_command('HEAD.FLUSH'))
        self.head_client.execute_command('MEMBER.PUT', 'k1', 'v1', _CLIENT_ID)
        self.head_client.execute_command('MEMBER.PUT', 'k1', 'v1', _CLIENT_ID)
        self.head_client.execute_command('MEMBER.PUT', 'k1', 'v1', _CLIENT_ID)
        self.assertEqual(3, self.head_client.execute_command('HEAD.FLUSH'))
        self.assertEqual(0, self.head_client.execute_command('HEAD.FLUSH'))
        self.head_client.execute_command('MEMBER.PUT', 'k2', 'v2', _CLIENT_ID)
        self.assertEqual(1, self.head_client.execute_command('HEAD.FLUSH'))

        # Nothing in the keyspace.
        self.assertEqual([], self.head_client.execute_command('KEYS *'))


if __name__ == "__main__":
    unittest.main(verbosity=2)
