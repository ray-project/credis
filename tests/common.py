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

# GCS modes.
GCS_NORMAL = 0
GCS_CKPTONLY = 1
GCS_CKPTFLUSH = 2


def MakeChain(num_nodes=2):
    global PORTS
    assert num_nodes >= 1
    # 6369 reserved for the master.
    chain = [6369 + i for i in range(num_nodes + 1)]
    PORTS = list(chain)
    return chain


def KillAll():
    subprocess.Popen(["pkill", "-9", "redis-server.*"]).wait()


def KillNode(index=None, port=None, stateless=False):
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
    if not stateless:
        if port is None:
            del PORTS[index + 1]
        else:
            del PORTS[PORTS.index(port)]


def AddNode(master_client, port=None, gcs_mode=GCS_NORMAL):
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

    with open("%d.log" % new_port, 'w') as output:
        member = subprocess.Popen(
            [
                "redis/src/redis-server",
                "--loadmodule",
                "build/src/libmember.so",
                str(gcs_mode),
                "--port",
                str(new_port),
            ],
            stdout=output,
            stderr=output)
    time.sleep(0.1)
    master_client.execute_command("MASTER.ADD", "127.0.0.1", str(new_port))
    member_client = redis.StrictRedis("127.0.0.1", new_port)
    master_port = master_client.connection_pool.connection_kwargs['port']
    member_client.execute_command("MEMBER.CONNECT_TO_MASTER", "127.0.0.1",
                                  master_port)
    if port is None:
        PORTS.append(new_port)
    return member, new_port


def Start(request=None, chain=INIT_PORTS, gcs_mode=GCS_NORMAL):
    global PORTS
    global MAX_USED_PORT
    PORTS = list(chain)
    MAX_USED_PORT = max(PORTS)  # For picking the next port.
    assert len(PORTS) > 1, "At least 1 master and 1 chain node"
    print('Setting up initial chain (mode %d): %s' % (gcs_mode, INIT_PORTS))

    KillAll()
    with open("%d.log" % PORTS[0], 'w') as output:
        master = subprocess.Popen(
            [
                "redis/src/redis-server",
                "--loadmodule",
                "build/src/libmaster.so",
                str(gcs_mode),
                "--port",
                str(PORTS[0]),
            ],
            stdout=output,
            stderr=output)
    if request is not None:
        request.addfinalizer(master.kill)
    master_client = redis.StrictRedis("127.0.0.1", PORTS[0])

    for port in PORTS[1:]:
        member, _ = AddNode(master_client, port, gcs_mode)
        if request is not None:
            request.addfinalizer(member.kill)


@pytest.fixture(scope='function', autouse=True, params=[GCS_NORMAL])
def startcredis(request):
    Start(request, gcs_mode=request.param)


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
