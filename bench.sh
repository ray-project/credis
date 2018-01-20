#!/bin/bash
set -ex

for write_ratio in 1 0.66 0.33 ; do



    # Redis
    echo 'num_clients throughput latency' > redis-wr${write_ratio}.txt
    for num_clients in $(seq 1 16); do
        sleep 5
        ./bench-redis.sh $num_clients $write_ratio
    done

    # Chain
#    for num_nodes in 2 1; do
#        echo 'num_clients throughput latency' >> chain-${num_nodes}node-wr${write_ratio}.txt
#
#        for num_clients in $(seq 16 -1 1); do
##          if [ $num_nodes -eq 2] && [$num_clients -lt 11 ]; then
##            continue
##          fi
#            sleep 5
#            ./seqput.sh $num_clients $num_nodes $write_ratio &
#            wait
#        done
#
#
#    done
#

done
