#!/bin/bash
set -x
NUM_CLIENTS=${1:-1}
WRITE_RATIO=${2:-1}
SERVER=${3:-127.0.0.1}

rm -rf client*.log

pkill -f redis_seqput_bench
pkill -f credis_seqput_bench

ssh -o StrictHostKeyChecking=no ubuntu@${SERVER} << EOF
cd ~/credis
pkill -f redis-server
sleep 2
./setup.sh 1
sleep 2
EOF

sleep 4
for i in $(seq 1 $NUM_CLIENTS); do
    ./build/src/redis_seqput_bench $WRITE_RATIO $SERVER 2>&1 | tee client$i.log &
done
wait

logs="client*.log"
outfile="redis-wr${WRITE_RATIO}"

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
