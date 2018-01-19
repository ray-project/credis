#!/bin/zsh
set -x
NUM_CLIENTS=${1:-1}
WRITE_RATIO=${2:1}

pkill -f redis-server; ./setup.sh 1;
for i in $(seq 1 $NUM_CLIENTS); do
    ./build/src/redis_seqput_bench $WRITE_RATIO 2>&1 | tee client$i.log &
done
wait
grep throughput client<1-8>.log | cut -d' ' -f 11 | sort -n | tail -n1 | awk '{time=$1/1000} END {print 500000*8/time}'
