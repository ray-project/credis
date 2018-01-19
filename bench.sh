#!/bin/bash

for write_ratio in 0.33 0.66 1; do


    # Chain
    for num_nodes in 1 2; do
        echo 'num_clients throughput latency' > chain-${num_nodes}node-wr${write_ratio}.txt

        for num_clients in $(seq 1 16); do
            ./seqput.sh $num_clients $num_nodes $write_ratio
            sleep 2
        done


    done

    # Redis
    echo 'num_clients throughput latency' > redis-wr${write_ratio}.txt
    for num_clients in $(seq 1 16); do
        ./bench-redis.sh $num_clients $write_ratio
        sleep 2
    done


done
