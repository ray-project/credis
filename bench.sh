#!/bin/bash
set -ex

SERVER=${1:-127.0.0.1}
NODE_ADD=${2:-""}
NODE_KILL=${3:-""}
N=${4:-""}

pushd build
make -j
popd

# for write_ratio in 1 0.66 0.33 ; do
#     # Redis
#     echo 'num_clients throughput latency' > redis-wr${write_ratio}.txt
#     for num_clients in $(seq 32 -1 1); do
#         sleep 5
#         ./bench-redis.sh $num_clients $write_ratio $SERVER
#     done
# 
#     # Chain
#     for num_nodes in 1 2; do
#         echo 'num_clients throughput latency' > chain-${num_nodes}node-wr${write_ratio}.txt
# 
#         for num_clients in $(seq 32 -1 1); do
#             sleep 5
#             ./seqput.sh $num_clients $num_nodes $write_ratio $SERVER &
#             wait
#         done
#     done
# done

for write_ratio in 1 ; do
#for write_ratio in 1 0.66 0.33 ; do

    # # Redis
    # echo 'num_clients throughput latency' > redis-wr${write_ratio}.txt
    # for num_clients in 32 28; do
    #     sleep 5
    #     ./bench-redis.sh $num_clients $write_ratio $SERVER
    # done

    # Chain
    for num_nodes in  2; do
    #for num_nodes in  2 1; do
        echo 'num_clients throughput latency' > chain-${num_nodes}node-wr${write_ratio}.txt

      #  base=2
      #  limit=6
      #  for i in $(seq $limit -1 0); do
      #    num_clients=$(echo "$base^$i" | bc)
        for num_clients in 1; do
        # for num_clients in $(seq 32 -4 1); do
            sleep 5
            ./seqput.sh $num_clients $num_nodes $write_ratio $SERVER $NODE_ADD $NODE_KILL $N &
            wait
        done
      #done
    done

done
