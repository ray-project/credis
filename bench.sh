#!/bin/bash

for write_ratio in 1 0.66; do

    echo 'num_clients throughput latency' > redis-wr${write_ratio}.txt

    for num_clients in $(seq 1 16); do

        ./bench-redis.sh $num_clients $write_ratio
        sleep 2

    done


done
