import time
import unittest
import uuid

import redis
import common
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
            common.KillNode(port=p)

    def testNormal(self):
        common.Start()
        # By default, the execution mode is kNormal, which disallows flush/ckpt.
        with self.assertRaises(redis.exceptions.ResponseError) as ctx:
            self.ack_client.execute_command('TAIL.CHECKPOINT')
        self.assertTrue('GcsMode is set to kNormal' in str(ctx.exception))

        with self.assertRaises(redis.exceptions.ResponseError) as ctx:
            self.head_client.execute_command('HEAD.FLUSH', _CLIENT_ID)
        self.assertTrue(
            'GcsMode is NOT set to kCkptFlush' in str(ctx.exception))

    def testCkptOnly(self):
        common.Start(gcs_mode=common.GCS_CKPTONLY)
        self.ack_client.execute_command('TAIL.CHECKPOINT')

        with self.assertRaises(redis.exceptions.ResponseError) as ctx:
            self.head_client.execute_command('HEAD.FLUSH', _CLIENT_ID)
        self.assertTrue(
            'GcsMode is NOT set to kCkptFlush' in str(ctx.exception))

    def testCkptFlush(self):
        common.Start(gcs_mode=common.GCS_CKPTFLUSH)
        self.ack_client.execute_command('TAIL.CHECKPOINT')
        self.head_client.execute_command('HEAD.FLUSH', _CLIENT_ID)


if __name__ == "__main__":
    unittest.main(verbosity=2)
