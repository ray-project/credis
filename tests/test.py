import time
import uuid

import pytest

import redis
from tests.common import startcredis

# This script should be run from within the credis/ directory

_CLIENT_ID = str(uuid.uuid4())  # Used as the channel name receiving acks.


def test_ack():
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
