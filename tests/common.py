import subprocess
import time

import pytest

import redis

# Ports, in the order of [master; replicas in chain].
INIT_PORTS = [6369, 6370]
INIT_PORTS = [6369, 6370, 6371, 6372]
INIT_PORTS = [6369, 6370, 6371]

PORTS = list(INIT_PORTS)
MAX_USED_PORT = max(PORTS)  # For picking the next port.


def MakeChain(num_nodes=2):
    global PORTS
    assert num_nodes >= 1
    # 6369 reserved for the master.
    chain = [6369 + i for i in range(num_nodes + 1)]
    PORTS = list(chain)
    return chain


def KillNode(index=None, port=None):
    global PORTS
    assert index is not None or port is not None
    if port is None:
        assert index >= 0 and index < len(
            PORTS) - 1, "index %d num_chain_nodes %d" % (index, len(PORTS) - 1)
        assert index == 0 or index == len(
            PORTS
        ) - 2, "middle node failure is not handled, index %d, len %d" % (
            index, len(PORTS))
        port_to_kill = PORTS[index + 1]
    else:
        port_to_kill = port
    print('killing port %d' % port_to_kill)
    subprocess.check_output(
        ["pkill", "-9", "redis-server.*:%s" % port_to_kill])
    if port is None:
        del PORTS[index + 1]
    else:
        del PORTS[PORTS.index(port)]


def AddNode(master_client, port=None):
    global PORTS
    global MAX_USED_PORT
    if port is not None:
        MAX_USED_PORT = port if MAX_USED_PORT is None else max(
            MAX_USED_PORT, port)
        new_port = port
    else:
        new_port = MAX_USED_PORT + 1
        MAX_USED_PORT += 1
    print('launching redis-server --port %d' % new_port)
    member = subprocess.Popen([
        "redis/src/redis-server", "--loadmodule", "build/src/libmember.so",
        "--port",
        str(new_port)
    ], )
    time.sleep(0.1)
    print('calling master add, new_port %s' % new_port)
    master_client.execute_command("MASTER.ADD", "127.0.0.1", str(new_port))
    if port is None:
        PORTS.append(new_port)
    return member, new_port


def Start(request=None, chain=INIT_PORTS):
    global PORTS
    global MAX_USED_PORT
    PORTS = list(chain)
    MAX_USED_PORT = max(PORTS)  # For picking the next port.
    assert len(PORTS) > 1, "At least 1 master and 1 chain node"
    print('Setting up initial chain: %s' % INIT_PORTS)

    subprocess.Popen(["pkill", "-9", "redis-server.*"]).wait()
    master = subprocess.Popen([
        "redis/src/redis-server", "--loadmodule", "build/src/libmaster.so",
        "--port",
        str(PORTS[0])
    ])
    if request is not None:
        request.addfinalizer(master.kill)
    master_client = redis.StrictRedis("127.0.0.1", PORTS[0])

    for port in PORTS[1:]:
        member, _ = AddNode(master_client, port)
        if request is not None:
            request.addfinalizer(member.kill)


@pytest.fixture(autouse=True)
def startcredis(request):
    Start(request)


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
    print('calling MASTER.REFRESH_HEAD')
    head_addr_port = master_client.execute_command("MASTER.REFRESH_HEAD")
    print('head_addr_port: %s' % head_addr_port)
    splits = head_addr_port.split(b':')
    return redis.StrictRedis(splits[0], int(splits[1]))


def RefreshTailFromMaster(master_client, client_id):
    print('calling MASTER.REFRESH_TAIL')
    tail_addr_port = master_client.execute_command("MASTER.REFRESH_TAIL")
    print('tail_addr_port: %s' % tail_addr_port)
    splits = tail_addr_port.split(b':')
    c = redis.StrictRedis(splits[0], int(splits[1]))
    return AckClientAndPubsub(client_id, c)
