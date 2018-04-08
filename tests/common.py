import subprocess
import time

import pytest

import etcd3
import redis

# Ports, in the order of [master; replicas in chain].
INIT_PORTS = [6369, 6370]
INIT_PORTS = [6369, 6370, 6371, 6372]
INIT_PORTS = [6369, 6370, 6371]

PORTS = list(INIT_PORTS)
MAX_USED_PORT = max(PORTS)  # For picking the next port.

# GCS modes.
GCS_NORMAL = 0
GCS_CKPTONLY = 1
GCS_CKPTFLUSH = 2

ETCD_ADDR = '127.0.0.1:12379'
CHAIN_PREFIX = 'testchain'
ETCD_URL = '%s/%s' % (ETCD_ADDR, CHAIN_PREFIX)

MASTER_REDIS = 0
MASTER_ETCD = 1

def log(*args):
    print('%f:' % time.time(), args)


def MakeChain(num_nodes=2):
    global PORTS
    assert num_nodes >= 1
    # 6369 reserved for the master.
    chain = [6369 + i for i in range(num_nodes + 1)]
    PORTS = list(chain)
    return chain


def KillAll():
    subprocess.Popen(["pkill", "-f", "-9", "redis-server.*"]).wait()


def KillNode(index=None, port=None, stateless=False, notify=None):
    global PORTS
    assert index is not None or port is not None
    if port is None:
        assert index >= 0 and index < len(
            PORTS) - 1, "index %d num_chain_nodes %d" % (index, len(PORTS) - 1)
        # assert index == 0 or index == len(
        #     PORTS
        # ) - 2, "middle node failure is not handled, index %d, len %d" % (
        #     index, len(PORTS))
        port_to_kill = PORTS[index + 1]
    else:
        port_to_kill = port
    log('killing port %d' % port_to_kill)
    subprocess.call(["pkill", "-f", "-9", "redis-server.*:%s" % port_to_kill])
    if not stateless:
        if port is None:
            del PORTS[index + 1]
        else:
            del PORTS[PORTS.index(port)]

    # Only use this with etcd mode!
    if notify is not None:
        # Call MASTER.REMOVE with the node that was removed.
        notify.execute_command("MASTER.REMOVE", "127.0.0.1", port_to_kill)

def PortForNode(index):
    global PORTS
    return PORTS[index + 1]


def AddNode(master_client, port=None, gcs_mode=GCS_NORMAL, master_mode=MASTER_REDIS, heartbeat_interval=2, heartbeat_timeout=5):
    global PORTS
    global MAX_USED_PORT
    if port is not None:
        MAX_USED_PORT = port if MAX_USED_PORT is None else max(
            MAX_USED_PORT, port)
        new_port = port
    else:
        new_port = MAX_USED_PORT + 1
        MAX_USED_PORT += 1
    log('launching redis-server --port %d' % new_port)

    with open("%d.log" % new_port, 'w') as output:
        member = subprocess.Popen(
            [
                "redis/src/redis-server",
                "--loadmodule",
                "build/src/libmember.so",
                str(gcs_mode),
                str(master_mode),
                "--port",
                str(new_port),
            ],
            stdout=output,
            stderr=output)
    time.sleep(0.1)

    # Start heartbeat if etcd, or acquire master conn info if redis.
    member_client = redis.StrictRedis("127.0.0.1", new_port)
    if master_mode == MASTER_REDIS:
        master_port = master_client.connection_pool.connection_kwargs['port']
        member_client.execute_command("MEMBER.CONNECT_TO_MASTER", "127.0.0.1:%s" % master_port)
    elif master_mode == MASTER_ETCD:
        member_client.execute_command("MEMBER.CONNECT_TO_MASTER", ETCD_URL,
                                      str(heartbeat_interval), str(heartbeat_timeout))

    master_client.execute_command("MASTER.ADD", "127.0.0.1", str(new_port))
    if port is None:
        PORTS.append(new_port)
    return member, new_port


def StartMaster(request=None, master_mode=MASTER_REDIS):
    global master, master_client

    if master_mode == MASTER_ETCD:
        master_module = "build/src/etcd/libetcd_master.so"
        master_run_arg = ETCD_URL
    else:
        master_module = "build/src/libmaster.so"
        master_run_arg = ""
    with open("%d.log" % PORTS[0], 'w') as output:
        master = subprocess.Popen(
            [
                "redis/src/redis-server",
                "--loadmodule",
                master_module,
                master_run_arg,
                "--port",
                str(PORTS[0]),
            ],
            stdout=output,
            stderr=output)
    if request is not None:
        request.addfinalizer(master.kill)
    master_client = redis.StrictRedis("127.0.0.1", PORTS[0])
    return master, master_client


def KillMaster(request=None):
    master.kill()

def Start(request=None, chain=INIT_PORTS, gcs_mode=GCS_NORMAL, master_mode=MASTER_REDIS, heartbeat_interval=2, heartbeat_timeout=5):
    global PORTS
    global MAX_USED_PORT
    PORTS = list(chain)
    MAX_USED_PORT = max(PORTS)  # For picking the next port.
    assert len(PORTS) > 1, "At least 1 master and 1 chain node"
    log('Setting up initial chain (gcs mode %d, master mode %d): %s' %
          (gcs_mode, master_mode, INIT_PORTS))

    KillAll()

    master, master_client = StartMaster(request, master_mode)

    for port in PORTS[1:]:
        member, _ = AddNode(master_client, port, gcs_mode, master_mode, heartbeat_interval, heartbeat_timeout)
        if request is not None:
            request.addfinalizer(member.kill)

@pytest.fixture(params=[MASTER_REDIS, MASTER_ETCD])
def startcredis(request):
    Start(request, master_mode=request.param)
    return {'master_mode': request.param}

@pytest.fixture()
def startcredis_etcdonly(request):
    Start(request, master_mode=MASTER_ETCD)
    return {'master_mode': MASTER_ETCD}

def AckClient():
    return redis.StrictRedis("127.0.0.1", PORTS[-1])


def AckClientAndPubsub(client_id, client=None):
    if client is None:
        client = AckClient()
    ack_pubsub = client.pubsub(ignore_subscribe_messages=True)
    ack_pubsub.subscribe(client_id)
    return client, ack_pubsub


def MasterClient():
    return redis.StrictRedis("127.0.0.1", PORTS[0])


head_client = redis.StrictRedis("127.0.0.1", PORTS[1])


def GetHeadFromMaster(master_client):
    return head_client


def RefreshHeadFromMaster(master_client):
    log('calling MASTER.REFRESH_HEAD')
    head_addr_port = master_client.execute_command("MASTER.REFRESH_HEAD")
    log('head_addr_port: %s' % head_addr_port)
    splits = head_addr_port.split(b':')
    return redis.StrictRedis(splits[0], int(splits[1]))


def RefreshTailFromMaster(master_client, client_id):
    log('calling MASTER.REFRESH_TAIL at %s' % time.time())
    tail_addr_port = master_client.execute_command("MASTER.REFRESH_TAIL")
    log('tail_addr_port: %s' % tail_addr_port)
    splits = tail_addr_port.split(b':')
    c = redis.StrictRedis(splits[0], int(splits[1]))
    return AckClientAndPubsub(client_id, c)
