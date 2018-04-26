#!/bin/bash
set -ex

# Defaults to 1, or 1st arg if provided.
# Master is assigned port 6369.  Chain node i gets port 6369+i.
NUM_NODES=${1:-1}
# If present, use 6369 on this server as the master.
HEAD_SERVER=${2:-""}

gcs_normal=0
gcs_ckptonly=1
gcs_ckptflush=2
gcs_mode=${gcs_ckptflush}
gcs_mode=${gcs_normal}

function setup() {
    pushd build
    make -j
    popd

    if [ -z "${HEAD_SERVER}" ]; then
      # Master.
      ./redis/src/redis-server --loadmodule ./build/src/libmaster.so --port 6369 --protected-mode no &> master.log &
      HEAD_SERVER="127.0.0.1"
    fi

    myip=$(curl ipinfo.io/ip)

    port=6369
    for i in $(seq 1 $NUM_NODES); do
      port=$(expr $port + 1)
      taskset 0x1 \
              ./redis/src/redis-server --loadmodule ./build/src/libmember.so ${gcs_mode} --port $port --protected-mode no &> $port.log &

      sleep 0.5
      ./redis/src/redis-cli -h ${HEAD_SERVER} -p 6369 MASTER.ADD ${myip} $port

    done
}

setup
