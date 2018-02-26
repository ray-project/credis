#!/bin/bash
set -x
NUM_CLIENTS=${1:-1}
NUM_NODES=${2:-1}
WRITE_RATIO=${3:-1}
SERVER=${4:-127.0.0.1}

# Example usage:
#
#   # To launch.
#   pkill -f redis-server; ./setup.sh 2; make -j;
#   ./seqput.sh 12 2
#
#   # To calculate throughput.
#   grep throughput client<1-12>.log | cut -d' ' -f 11 | sort -n | tail -n1 | awk '{time=$1/1000} END {print 500000*12/time}'

pkill -f redis_seqput_bench
pkill -f credis_seqput_bench

#ssh -o StrictHostKeyChecking=no ubuntu@${SERVER} << EOF
#cd ~/credis-1
#pkill -f redis-server
#sleep 2
#./setup.sh $NUM_NODES
#sleep 2
#EOF

sleep 4

for i in $(seq 1 $NUM_CLIENTS); do
  logfile=${NUM_CLIENTS}clients-${i}-chain-${NUM_NODES}node-wr${WRITE_RATIO}.log
  ./build/src/credis_seqput_bench $NUM_NODES $WRITE_RATIO $SERVER >${logfile} 2>&1 &
done
wait

logs="${NUM_CLIENTS}clients-*-chain-${NUM_NODES}node-wr${WRITE_RATIO}.log"
outfile="chain-${NUM_NODES}node-wr${WRITE_RATIO}"

# Composite
thput=$(grep throughput ${logs} | tr -s ' ' | cut -d' ' -f 11 | sort -n | tail -n1 | awk -v N=$NUM_CLIENTS '{time=$1/1000} END {print 50000*N/time}')
latency=$(grep latency ${logs} | tr -s ' ' | cut -d' ' -f 8 | awk  -v N=$NUM_CLIENTS '{s += $1} END {print s/N}')
echo "$NUM_CLIENTS $thput $latency" >> ${outfile}.txt

# Reads.
read_thput=$(grep reads_thput ${logs} | tr -s ' ' | cut -d' ' -f 6 | awk '{s+=$1} END {print s}')  # Thput ~= sum(client_i's read_thput)
read_latency=$(grep reads_lat ${logs} | tr -s ' ' | cut -d' ' -f 8 | awk -v N=$NUM_CLIENTS '{s += $1} END {print s/N}')  # Latency ~= sum(client_i's read_lat) / num_clients
echo "$NUM_CLIENTS $read_thput $read_latency" >> ${outfile}-readsportion.txt

# Writes
write_thput=$(grep writes_thput ${logs} | tr -s ' ' | cut -d' ' -f 6 | awk '{s+=$1} END {print s}')  # Thput ~= sum(client_i's write_thput)
write_latency=$(grep writes_lat ${logs} | tr -s ' ' | cut -d' ' -f 8 | awk -v N=$NUM_CLIENTS '{s += $1} END {print s/N}')  # Latency ~= sum(client_i's write_lat) / num_clients
echo "$NUM_CLIENTS $write_thput $write_latency" >> ${outfile}-writesportion.txt
