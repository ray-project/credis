#!/bin/bash
# distributed_seqput.sh
# Usage: run on client server.

set -x
NUM_CLIENTS=${1:-1}
NUM_NODES=${2:-1}
WRITE_RATIO=${3:-1}
HEAD_SERVER=${4:-127.0.0.1}
TAIL_SERVER=${5:-127.0.0.1}
NODE_ADD=${6:-""}
NODE_KILL=${7:-""}
NUM_OPS=${8:-60000}

pkill -f redis_seqput_bench
pkill -f credis_seqput_bench

sleep 4

agg_csv=${NUM_CLIENTS}clients-chain_dist-${NUM_NODES}node-wr${WRITE_RATIO}-$(date +%s).csv
echo 'begin_timestamp_us,latency_us' > ${agg_csv}
echo 'begin_timestamp_us,latency_us' > reads-${agg_csv}
echo 'begin_timestamp_us,latency_us' > writes-${agg_csv}
for i in $(seq 1 $NUM_CLIENTS); do
  logfile=${NUM_CLIENTS}clients-${i}-chain_dist-${NUM_NODES}node-wr${WRITE_RATIO}.log
  ./build/src/credis_seqput_bench $NUM_NODES $WRITE_RATIO $HEAD_SERVER $NUM_OPS $TAIL_SERVER ${agg_csv} >${logfile} 2>&1 &
done

maybe_kill() {
    wait=$1
    eval wait=$wait
    # Optionally, do node removal.
    if [ ! -z "${wait}" ]; then
        sleep ${wait}
        echo 'Performing node removal...'
        ssh -o StrictHostKeyChecking=no ubuntu@${TAIL_SERVER} << EOF
pkill -f redis-server.*:6371
EOF
    fi
}

maybe_add() {
    wait=$1
    eval wait=$wait
# Optionally, do node addition.
if [ ! -z "${wait}" ]; then
    sleep ${wait}
    echo 'Performing node addition...'
    ssh -o StrictHostKeyChecking=no ubuntu@${TAIL_SERVER} << EOF
cd ~/credis
bash add.sh $HEAD_SERVER 6371
EOF
fi
}

maybe_kill ${NODE_KILL} &
maybe_add ${NODE_ADD} &

# NODE_KILL2=$((NODE_KILL * 3))
# NODE_ADD2=$((NODE_ADD * 2))
# maybe_kill ${NODE_KILL2} &
# maybe_add ${NODE_ADD2} &

wait

logs="${NUM_CLIENTS}clients-*-chain_dist-${NUM_NODES}node-wr${WRITE_RATIO}.log"
outfile="chain-${NUM_NODES}node-wr${WRITE_RATIO}"

# Composite
num_ops=$(grep throughput ${logs} | tr -s ' ' | cut -d' ' -f 13 | tr -d ',' | sort -n | tail -n1)
thput=$(grep throughput ${logs} | tr -s ' ' | cut -d' ' -f 11 | sort -n | tail -n1 | awk -v N=$NUM_CLIENTS -v O=$num_ops '{time=$1/1000} END {print O*N/time}')
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
