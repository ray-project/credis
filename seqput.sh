#!/bin/bash
set -x
NUM_CLIENTS=${1:-1}
NUM_NODES=${2:-1}
WRITE_RATIO=${3:-1}

# Example usage:
#
#   # To launch.
#   pkill -f redis-server; ./setup.sh 2; make -j;
#   ./seqput.sh 12 2
#
#   # To calculate throughput.
#   grep throughput client<1-12>.log | cut -d' ' -f 11 | sort -n | tail -n1 | awk '{time=$1/1000} END {print 500000*12/time}'

rm -rf client*.log
pkill -f redis-server; ./setup.sh $NUM_NODES;
for i in $(seq 1 $NUM_CLIENTS); do
    ./build/src/credis_seqput_bench $NUM_NODES $WRITE_RATIO 2>&1 | tee client$i.log &
done
wait

thput=$(grep throughput client*.log | tr -s ' ' | cut -d' ' -f 11 | sort -n | tail -n1 | awk -v N=$NUM_CLIENTS '{time=$1/1000} END {print 500000*N/time}')
latency=$(grep latency client*.log | tr -s ' ' | cut -d' ' -f 8 | awk '{s += $1} END {print s/NR}')
echo "$NUM_CLIENTS $thput $latency" >> chain-${NUM_NODES}node-wr${WRITE_RATIO}.txt
