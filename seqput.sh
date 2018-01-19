#!/bin/bash
set -ex
NUM_CLIENTS=${1:-1}
NUM_NODES=${2:-1}

# Example usage:
#
#   # To launch.
#   pkill -f redis-server; ./setup.sh 2; make -j;
#   ./seqput.sh 12 2
#
#   # To calculate throughput.
#   grep throughput client<1-12>.log | cut -d' ' -f 11 | sort -n | tail -n1 | awk '{time=$1/1000} END {print 500000*12/time}'

for i in $(seq 1 $NUM_CLIENTS); do
    ./build/src/credis_seqput_bench $NUM_NODES 2>&1 | tee client$i.log &
done
wait
