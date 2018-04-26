
#!/bin/bash
set -x

MASTER_SERVER=${1:-127.0.0.1}

gcs_normal=0
gcs_ckptonly=1
gcs_ckptflush=2
gcs_mode=${gcs_ckptflush}
gcs_mode=${gcs_normal}

function add() {
    # Assume by default the master & 1 node are already running, at 6369, 6370, respectively.
    port=6370
    while true; do
        result=$(pgrep -a redis-server | grep $port)
        if [ -z "${result}" ]; then
            echo 'breaking...'
            break
        fi
        port=$(expr $port + 1)
    done
    echo 'break!'

    ./redis/src/redis-server --loadmodule ./build/src/libmember.so ${gcs_mode} --port $port --protected-mode no &> $port.log &

    sleep 0.5
    myip=$(curl ipinfo.io/ip)
    ./redis/src/redis-cli -h ${MASTER_SERVER} -p 6369 MASTER.ADD ${myip} $port
}

add
