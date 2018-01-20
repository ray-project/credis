#!/bin/bash
set -x
NUM_CLIENTS=${1:-1}
WRITE_RATIO=${2:-1}

rm -rf client*.log

pkill -f redis-server; ./setup.sh 1;

sleep 2
for i in $(seq 1 $NUM_CLIENTS); do
    ./build/src/redis_seqput_bench $WRITE_RATIO 2>&1 | tee client$i.log &
done
wait

thput=$(grep throughput client*.log | tr -s ' ' | cut -d' ' -f 11 | sort -n | tail -n1 | awk -v N=$NUM_CLIENTS '{time=$1/1000} END {print 500000*N/time}')
latency=$(grep latency client*.log | tr -s ' ' | cut -d' ' -f 8 | awk '{s += $1} END {print s/NR}')
echo "$NUM_CLIENTS $thput $latency" >> redis-wr${WRITE_RATIO}.txt
