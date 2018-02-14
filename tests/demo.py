"""Usage:

# For testing.
$ python -m pytest tests/demo.py

# Python-side benchmarks are launched as a standalone script.
$ python tests/demo.py
"""
import multiprocessing
import time
import uuid

import numpy as np

import redis
from tests import common
from tests.common import *

ack_client = AckClient()
master_client = MasterClient()
head_client = GetHeadFromMaster(master_client)
act_pubsub = None

# Put() ops can be ignored when failures occur on the servers.  Try a few times.
fails_since_last_success = 0
max_fails = 3
ops_completed = multiprocessing.Value('i', 0)
_CLIENT_ID = str(uuid.uuid4())  # Used as the channel name receiving acks.


# From redis-py/.../test_pubsub.py.
def wait_for_message(pubsub, timeout=0.1, ignore_subscribe_messages=False):
    now = time.time()
    timeout = now + timeout
    while now < timeout:
        message = pubsub.get_message(
            ignore_subscribe_messages=ignore_subscribe_messages)
        if message is not None:
            return message
        time.sleep(1e-5)  # 10us.
        now = time.time()
    return None


def Put(i):
    global head_client
    global ack_pubsub
    global fails_since_last_success

    i_str = str(i)  # Serialize it once.
    put_issued = False

    for k in range(3):  # Try 3 times.
        try:
            sn = head_client.execute_command("MEMBER.PUT", i_str, i_str,
                                             _CLIENT_ID)
            put_issued = True
            break
        except redis.exceptions.ConnectionError:
            head_client = RefreshHeadFromMaster(master_client)  # Blocking.
            continue
    if not put_issued:
        raise Exception("Irrecoverable redis connection issue; put client %s" %
                        head_client)

    # Wait for the ack.
    ack = None
    good_client = False
    for k in range(3):  # Try 3 times.
        try:
            # if k > 0:
            # print('k %d pubsub %s' % (k, ack_pubsub.connection))
            # NOTE(zongheng): 1e-4 seems insufficient for an ACK to be
            # delivered back.  1e-3 has the issue of triggering a retry, but
            # then receives an ACK for the old sn (an issue clients will need
            # to address).  Using 10ms for now.
            ack = wait_for_message(ack_pubsub, timeout=1e-2)
            good_client = True
            break
        except redis.exceptions.ConnectionError as e:
            _, ack_pubsub = RefreshTailFromMaster(master_client,
                                                  _CLIENT_ID)  # Blocking.
            continue

    if not good_client:
        raise Exception("Irrecoverable redis connection issue; ack client %s" %
                        ack_pubsub.connection)
    elif ack is None:
        # Connected but an ACK was not received after timeout (the update was
        # likely ignored by the store).  Retry.
        fails_since_last_success += 1
        if fails_since_last_success >= max_fails:
            raise Exception(
                "A maximum of %d update attempts have failed; "
                "no acks from the store are received. i = %d, client = %s" %
                (max_fails, i, ack_pubsub.connection))
        _, ack_pubsub = RefreshTailFromMaster(master_client, _CLIENT_ID)
        print("%d updates have been ignored since last success, "
              "retrying Put(%d) with fresh ack client %s" %
              (fails_since_last_success, i, ack_pubsub.connection))
        time.sleep(1)
        Put(i)
    else:
        # TODO(zongheng): this is a stringent check.  See NOTE above: sometimes
        # we can receive an old ACK.
        assert int(ack["data"]) == sn
        fails_since_last_success = 0


def SeqPut(n, sleep_secs):
    """For i in range(n), sequentially put i->i into redis."""
    global ack_client
    global ack_pubsub
    global ops_completed
    ack_client, ack_pubsub = AckClientAndPubsub(
        client_id=_CLIENT_ID, client=None)
    ops_completed.value = 0

    latencies = []
    for i in range(n):
        # if i % 50 == 0:
        # print('i = %d' % i)
        start = time.time()
        Put(i)  # i -> i
        latencies.append((time.time() - start) * 1e6)  # Microsecs.
        time.sleep(sleep_secs)
        ops_completed.value += 1  # No lock needed.

    nums = np.asarray(latencies)
    print(
        'throughput %.1f writes/sec; latency (us): mean %.5f std %.5f num %d' %
        (len(nums) * 1.0 / np.sum(nums) * 1e6, np.mean(nums), np.std(nums),
         len(nums)))


# Asserts that the redis state is exactly {i -> i | i in [0, n)}.
def Check(n):
    read_client, _ = RefreshTailFromMaster(master_client, _CLIENT_ID)
    actual = len(read_client.keys(b'*'))
    assert actual == n, "Written %d Expected %d" % (actual, n)
    for i in range(n):
        data = read_client.get(str(i))
        assert int(data) == i, i


def test_demo():
    # Launch driver thread.
    n = 1000
    sleep_secs = 0.01
    driver = multiprocessing.Process(target=SeqPut, args=(n, sleep_secs))
    driver.start()

    # Kill / add.
    new_nodes = []
    time.sleep(0.1)
    common.KillNode(index=1)
    new_nodes.append(common.AddNode(master_client))
    driver.join()

    assert ops_completed.value == n
    chain = master_client.execute_command('MASTER.GET_CHAIN')
    chain = [s.split(b':')[-1] for s in chain]
    assert chain == [b'6370', b'6372'], 'chain %s' % chain
    Check(ops_completed.value)

    for proc, _ in new_nodes:
        proc.kill()
    print('Total ops %d, completed ops %d' % (n, ops_completed.value))


def test_kaa():
    """Kill, add, add."""
    # Launch driver thread.
    n = 1000
    sleep_secs = 0.01
    driver = multiprocessing.Process(target=SeqPut, args=(n, sleep_secs))
    driver.start()

    new_nodes = []
    time.sleep(0.1)
    common.KillNode(index=1)
    new_nodes.append(common.AddNode(master_client))
    new_nodes.append(common.AddNode(master_client))

    driver.join()

    assert ops_completed.value == n
    chain = master_client.execute_command('MASTER.GET_CHAIN')
    assert len(chain) == 2 - 1 + len(new_nodes), 'chain %s' % chain
    Check(ops_completed.value)

    for proc, _ in new_nodes:
        proc.kill()


def test_multi_kill_add():
    """Kill, add a few times."""
    # Launch driver thread.
    n = 1000
    sleep_secs = 0.01
    driver = multiprocessing.Process(target=SeqPut, args=(n, sleep_secs))
    driver.start()

    # Kill / add.
    new_nodes = []
    time.sleep(0.1)
    common.KillNode(index=1)  # 6371 dead
    new_nodes.append(common.AddNode(master_client))  # 6372
    common.KillNode(index=1)  # 6372 dead
    new_nodes.append(common.AddNode(master_client))  # 6373
    common.KillNode(index=0)  # 6370 dead, now [6373]
    new_nodes.append(common.AddNode(master_client))  # 6374
    new_nodes.append(common.AddNode(master_client))  # 6375
    # Now [6373, 6374, 6375].
    common.KillNode(index=2)  # 6375 dead, now [6373, 6374]

    driver.join()

    assert ops_completed.value == n
    chain = master_client.execute_command('MASTER.GET_CHAIN')
    chain = [s.split(b':')[-1] for s in chain]
    assert chain == [b'6373', b'6374'], 'chain %s' % chain
    Check(ops_completed.value)

    for proc, _ in new_nodes:
        proc.kill()


def test_dead_old_tail_when_adding():
    # We set "sleep_secs" to a higher value.  So "kill tail", "add node" will
    # be trigered without a refresh request from the driver.  Master will have
    # the following view of its members:
    # init: [ live, live ]
    # kill: [ live, dead ]
    #    - master not told node 1 is dead
    # Tests that when adding, the master detects & removes the dead node first.

    # Launch driver thread.
    n = 5
    sleep_secs = 1
    driver = multiprocessing.Process(target=SeqPut, args=(n, sleep_secs))
    driver.start()

    time.sleep(0.1)
    common.KillNode(index=1)
    proc, _ = common.AddNode(master_client)
    driver.join()

    assert ops_completed.value == n
    chain = master_client.execute_command('MASTER.GET_CHAIN')
    assert len(chain) == 2 - 1 + 1, 'chain %s' % chain
    Check(ops_completed.value)

    proc.kill()


def BenchCredis(num_nodes, num_ops, num_clients):
    common.Start(chain=common.MakeChain(num_nodes))
    time.sleep(0.1)

    # TODO(zongheng): ops_completed needs to be changed
    assert num_clients == 1

    drivers = []
    for i in range(num_clients):
        drivers.append(
            multiprocessing.Process(target=SeqPut, args=(num_ops, 0)))
    for driver in drivers:
        driver.start()
    for driver in drivers:
        driver.join()

    assert ops_completed.value == num_ops
    Check(ops_completed.value)


def BenchVanillaRedis(num_ops):
    common.Start(chain=common.MakeChain(1))
    time.sleep(0.1)
    r = AckClient()  # Just use the chain node as a regular redis server.

    start = time.time()
    for i in range(num_ops):
        i_str = str(i)  # Serialize once.
        r.execute_command('SET', i_str, i_str)
    total_secs = time.time() - start
    print('throughput %.1f writes/sec; latency (us): mean %.5f std ? num %d' %
          (num_ops * 1.0 / total_secs, total_secs * 1e6 / num_ops, num_ops))


if __name__ == '__main__':
    # BenchVanillaRedis(num_ops=100000)
    BenchCredis(num_nodes=1, num_ops=500000, num_clients=1)
    # BenchCredis(num_nodes=2, num_ops=1000000)
    # BenchCredis(num_nodes=3, num_ops=100000)
