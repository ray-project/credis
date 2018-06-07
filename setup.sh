#!/bin/bash
set -ex

# Defaults to 1, or 1st arg if provided.
# Master is assigned port 6369.  Chain node i gets port 6369+i.
NUM_NODES=${1:-1}
# If present, use 6369 on this server as the master.
HEAD_SERVER=${2:-""}
port=${3:-6370}  # Initial port.

gcs_normal=0
gcs_ckptonly=1
gcs_ckptflush=2
gcs_mode=${gcs_ckptflush}
gcs_mode=${gcs_normal}

if [ "$(uname)" == "Darwin" ]; then
    myip=$(curl ipinfo.io/ip 2>/dev/null)
    myip="127.0.0.1"  # Assume local development
    maybe_taskset=""
else
    myip=$(hostname -i)
    maybe_taskset="taskset 0x1"
fi

function setup() {
    pushd build
    make -j
    popd

    if [ -z "${HEAD_SERVER}" ]; then
      # Master.
      ./redis/src/redis-server --loadmodule ./build/src/libmaster.so --port 6369 --protected-mode no &> master.log &
    fi

    sleep 2

    for i in $(seq 1 $NUM_NODES); do
      ${maybe_taskset} \
              ./redis/src/redis-server --loadmodule ./build/src/libmember.so ${gcs_mode} --port $port --protected-mode no &> $port.log &

      sleep 2
      if [ -z "${HEAD_SERVER}" ]; then
        ./redis/src/redis-cli -p 6369 MASTER.ADD ${myip} $port
      else
        ./redis/src/redis-cli -h ${HEAD_SERVER} -p 6369 MASTER.ADD ${myip} $port
      fi


# Have chain nodes connect to master.
#    sleep 0.5
#    ./redis/src/redis-cli -p $port MEMBER.CONNECT_TO_MASTER 127.0.0.1 6369
      port=$(expr $port + 1)
    done
    # ./redis/src/redis-cli -p 6370 MONITOR &>monitor-6370.log &
}

setup
