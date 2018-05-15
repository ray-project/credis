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
import common
from common import *

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

    # Try to issue the put.
    for k in range(3):
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
    ack_client_okay = False
    for k in range(3):  # Try 3 times.
        try:
            # if k > 0:
            # common.log('k %d pubsub %s' % (k, ack_pubsub.connection))
            # NOTE(zongheng): 1e-4 seems insufficient for an ACK to be
            # delivered back.  1e-3 has the issue of triggering a retry, but
            # then receives an ACK for the old sn (an issue clients will need
            # to address).  Using 10ms for now.
            ack = wait_for_message(ack_pubsub, timeout=1e-2)
            ack_client_okay = True
            break
        except redis.exceptions.ConnectionError as e:
            _, ack_pubsub = RefreshTailFromMaster(master_client,
                                                  _CLIENT_ID)  # Blocking.
            continue

    if not ack_client_okay:
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
        common.log("%d updates have been ignored since last success, "
                   "retrying Put(%d) with fresh ack client %s" %
                   (fails_since_last_success, i, ack_pubsub.connection))
        time.sleep(3)
        Put(i)
    else:
        # TODO(zongheng): this is a stringent check.  See NOTE above: sometimes
        # we can receive an old ACK.
        assert int(ack["data"]) == sn
        fails_since_last_success = 0


def SeqPut(n, sleep_secs):
    """For i in range(n), sequentially put i->i into redis."""
    global master_client
    global ack_client
    global ack_pubsub
    global ops_completed
    ack_client, ack_pubsub = AckClientAndPubsub(
        client_id=_CLIENT_ID, client=None)
    ops_completed.value = 0

    latencies = []
    for i in range(n):
        # if i % 50 == 0:
        # common.log('i = %d' % i)
        start = time.time()
        Put(i)  # i -> i
        latencies.append((time.time() - start) * 1e6)  # Microsecs.
        time.sleep(sleep_secs)
        ops_completed.value += 1  # No lock needed.

    nums = np.asarray(latencies)
    common.log(
        'throughput %.1f writes/sec; latency (us): mean %.5f std %.5f num %d' %
        (len(nums) * 1.0 / np.sum(nums) * 1e6, np.mean(nums), np.std(nums),
         len(nums)))


# Asserts that the redis state is exactly {i -> i | i in [0, n)}.
def Check(n):
    read_client, _ = RefreshTailFromMaster(master_client, _CLIENT_ID)
    actual = len(read_client.keys(b'*'))
    if actual != n:
        common.log('head # keys: %d' % len(head_client.keys(b'*')))
    assert actual == n, "Written %d Expected %d" % (actual, n)
    for i in range(n):
        data = read_client.get(str(i))
        assert int(data) == i, i


def test_demo(startcredis):
    master_mode = startcredis["master_mode"]
    # Launch driver thread.
    n = 1000
    sleep_secs = 0.01
    driver = multiprocessing.Process(target=SeqPut, args=(n, sleep_secs))
    driver.start()

    # Kill / add.
    new_nodes = []
    time.sleep(0.1)
    common.KillNode(index=1)
    time.sleep(0.1)
    new_nodes.append(common.AddNode(master_client, master_mode=master_mode))
    time.sleep(0.1)
    driver.join()

    assert ops_completed.value == n
    chain = master_client.execute_command('MASTER.GET_CHAIN')
    chain = [s.split(b':')[-1] for s in chain]
    assert chain == [b'6370', b'6372'], 'chain %s' % chain
    Check(ops_completed.value)

    for proc, _ in new_nodes:
        proc.kill()
    common.log('Total ops %d, completed ops %d' % (n, ops_completed.value))


def test_kaa(startcredis):
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


def test_multi_kill_add(startcredis):
    """Kill, add a few times."""
    # Launch driver thread.
    n = 1000
    sleep_secs = 0.01
    driver = multiprocessing.Process(target=SeqPut, args=(n, sleep_secs))
    driver.start()

    # Notify the master that nodes have died, rather than wait for the
    # heartbeat by sleeping between kills.
    if startcredis["master_mode"] == MASTER_ETCD:
        notify = master_client
    else:
        notify = None

    # Kill / add.
    new_nodes = []
    time.sleep(0.1)
    common.KillNode(index=1, notify=notify)  # 6371 dead
    new_nodes.append(common.AddNode(master_client))  # 6372
    common.KillNode(index=1, notify=notify)  # 6372 dead
    new_nodes.append(common.AddNode(master_client))  # 6373
    common.KillNode(index=0, notify=notify)  # 6370 dead, now [6373]
    new_nodes.append(common.AddNode(master_client))  # 6374
    new_nodes.append(common.AddNode(master_client))  # 6375
    # Now [6373, 6374, 6375].
    common.KillNode(index=2, notify=notify)  # 6375 dead, now [6373, 6374]

    driver.join()

    assert ops_completed.value == n
    chain = master_client.execute_command('MASTER.GET_CHAIN')
    chain = [s.split(b':')[-1] for s in chain]
    assert chain == [b'6373', b'6374'], 'chain %s' % chain
    Check(ops_completed.value)

    for proc, _ in new_nodes:
        proc.kill()


def test_dead_old_tail_when_adding(startcredis):
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


def test_etcd_kill_middle(startcredis_etcdonly):
    """ Test that if the middle node is removed, the tail continues to get updates
    once the chain is repaired.
    """
    # Start members with a quick heartbeat timeout.
    common.Start(
        chain=common.MakeChain(3),
        master_mode=MASTER_ETCD,
        heartbeat_interval=1,
        heartbeat_timeout=2)

    # Launch driver thread.
    n = 100
    sleep_secs = 0.1
    driver = multiprocessing.Process(target=SeqPut, args=(n, sleep_secs))
    driver.start()

    time.sleep(0.1)
    middle_port = common.PortForNode(1)
    common.KillNode(index=1, notify=master_client)
    driver.join()

    assert ops_completed.value == n
    chain = master_client.execute_command('MASTER.GET_CHAIN')
    assert len(chain) == 2 - 1 + 1, 'chain %s' % chain
    Check(ops_completed.value)


def test_etcd_heartbeat_timeout(startcredis_etcdonly):
    """ Test that failure is detected and repaired within a heartbeat timeout.
    """
    # Start members with a quick heartbeat timeout.
    common.Start(
        chain=common.MakeChain(3),
        master_mode=MASTER_ETCD,
        heartbeat_interval=1,
        heartbeat_timeout=2)

    # Launch driver thread. Note that it will take a minimum of 10 seconds.
    n = 10
    sleep_secs = 1
    driver = multiprocessing.Process(target=SeqPut, args=(n, sleep_secs))
    driver.start()

    time.sleep(0.1)
    middle_port = common.PortForNode(1)
    common.KillNode(index=1) # Don't notify master
    # Heartbeat should expire within 2 sec.
    driver.join()

    assert ops_completed.value == n

    import pdb; pdb.set_trace()
    chain = master_client.execute_command('MASTER.GET_CHAIN')
    assert len(chain) == 2 - 1 + 1, 'chain %s' % chain
    Check(ops_completed.value)


def test_etcd_master_recovery(startcredis_etcdonly):
    """ Test that the master can recover its state from etcd.
    """
    common.Start(
        chain=common.MakeChain(3),
        master_mode=MASTER_ETCD,
        heartbeat_interval=1,
        heartbeat_timeout=10)

    chain = master_client.execute_command('MASTER.GET_CHAIN')
    head = master_client.execute_command('MASTER.REFRESH_HEAD')
    tail = master_client.execute_command('MASTER.REFRESH_TAIL')
    assert len(chain) == 3, 'chain %s' % chain

    common.KillMaster()
    time.sleep(0.2)
    common.StartMaster(master_mode=MASTER_ETCD)
    time.sleep(0.1)

    assert chain == master_client.execute_command('MASTER.GET_CHAIN')
    assert head == master_client.execute_command('MASTER.REFRESH_HEAD')
    assert tail == master_client.execute_command('MASTER.REFRESH_TAIL')

    new_node, _ = common.AddNode(master_client)

    # Sanity check that normal operation can continue.
    assert len(master_client.execute_command('MASTER.GET_CHAIN')) == 4

    new_node.kill()


def test_etcd_master_online_recovery(startcredis_etcdonly):
    """ Test that SeqPut succeeds when the master is killed and restarted mid-way, then a member is
    killed, then a member is added. The restarted master should be able to recover the chain, with
    the new member being the tail, and no updates should be lost.
    """
    common.Start(
        chain=common.MakeChain(3),
        master_mode=MASTER_ETCD,
        heartbeat_interval=1,
        heartbeat_timeout=10)

    # Launch driver thread. Note that it will take a minimum of 10 seconds.
    n = 10
    sleep_secs = 1
    driver = multiprocessing.Process(target=SeqPut, args=(n, sleep_secs))
    driver.start()

    time.sleep(0.1)
    common.KillMaster()
    common.StartMaster(master_mode=MASTER_ETCD)
    time.sleep(0.1)
    assert len(master_client.execute_command('MASTER.GET_CHAIN')) == 3

    time.sleep(0.1)
    middle_port = common.PortForNode(1)
    common.KillNode(index=1, notify=master_client)
    assert len(master_client.execute_command('MASTER.GET_CHAIN')) == 2

    new_node, _ = common.AddNode(master_client, master_mode=MASTER_ETCD)
    time.sleep(0.1)
    driver.join()
    assert len(master_client.execute_command('MASTER.GET_CHAIN')) == 3

    # Heartbeat should expire within 2 sec.
    driver.join()

    assert ops_completed.value == n
    Check(ops_completed.value)

    # Cleanup
    new_node.kill()


def test_etcd_kill_node_while_master_is_dead(startcredis_etcdonly):
    """ Test that SeqPut succeeds when the master is killed and a node is killed WHILE the master is
    dead. The master is then restarted. No updates should be lost.

    TODO: Fails (3/28) because members are not checked for liveness when the master starts up.
    """
    # Choose a long heartbeat timeout so that the master never receives heartbeat expiry notifs.
    common.Start(
        chain=common.MakeChain(3),
        master_mode=MASTER_ETCD,
        heartbeat_interval=1,
        heartbeat_timeout=999)

    # Launch driver thread. Note that it will take a minimum of 10 seconds.
    n = 10
    sleep_secs = 1
    driver = multiprocessing.Process(target=SeqPut, args=(n, sleep_secs))
    driver.start()

    time.sleep(0.1)
    common.KillMaster()
    common.KillNode(index=1)
    common.StartMaster(master_mode=MASTER_ETCD)
    time.sleep(0.2)
    assert len(master_client.execute_command('MASTER.GET_CHAIN')) == 2

    new_node, _ = common.AddNode(master_client, master_mode=MASTER_ETCD)
    time.sleep(0.1)
    assert len(master_client.execute_command('MASTER.GET_CHAIN')) == 3

    driver.join()

    assert ops_completed.value == n
    Check(ops_completed.value)

    # Cleanup
    new_node.kill()


def BenchCredis(num_nodes, num_ops, num_clients, master_mode):
    common.Start(chain=common.MakeChain(num_nodes), master_mode=master_mode)
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
    common.log('throughput %.1f writes/sec; latency (us): mean %.5f std ? num %d' %
          (num_ops * 1.0 / total_secs, total_secs * 1e6 / num_ops, num_ops))


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--num-nodes', '-n',
                        help='Number of nodes to create', type=int, default=3)
    parser.add_argument('--num-ops', '-o',
                        help='Number of ops to do', type=int, default=500000)
    parser.add_argument('--num-clients', '-c',
                        help='Number of clients to create', type=int, default=1)
    parser.add_argument('--master-mode', '-m', type=int,
                        help='Master mode (0 = redis, 1 = etcd)', default=0)
    args = parser.parse_args()
    # BenchVanillaRedis(num_ops=100000)
    BenchCredis(num_nodes=args.num_nodes, num_ops=args.num_ops,
                num_clients=args.num_clients, master_mode=args.master_mode)
    # BenchCredis(num_nodes=2, num_ops=1000000)
    # BenchCredis(num_nodes=3, num_ops=100000)
