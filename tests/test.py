import time

import pytest

import redis
from tests.common import startcredis


# This script should be run from within the credis/ directory


def test_ack():
    head_client = redis.StrictRedis("127.0.0.1", 6370)
    tail_client = redis.StrictRedis("127.0.0.1", 6371)
    # The ack client needs to be separate, since subscriptions
    # are blocking
    ack_client = redis.StrictRedis("127.0.0.1", 6371)
    p = ack_client.pubsub(ignore_subscribe_messages=True)
    p.subscribe("answers")
    time.sleep(0.5)
    p.get_message()
    ssn = head_client.execute_command("MEMBER.PUT", "task_spec",
                                      "some_random_value")
    time.sleep(0.5)
    put_ack = p.get_message()

    assert ssn == 0
    assert int(put_ack["data"]) == ssn  # Check the sequence number
