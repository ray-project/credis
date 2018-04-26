#!/bin/bash
# Usage: run on client server.

set -ex

HEAD_SERVER=${1:-127.0.0.1}
TAIL_SERVER=${2:-127.0.0.1}
NODE_ADD=${3:-""}
NODE_KILL=${4:-""}
N=${5:-""}

pushd build; make -j; popd

for write_ratio in 1 ; do
    # Chain
    for num_nodes in  2; do
        echo 'num_clients throughput latency' > chain-${num_nodes}node-wr${write_ratio}.txt

        for num_clients in 1; do

            # Launch master & head.
            ssh -o StrictHostKeyChecking=no ubuntu@${HEAD_SERVER} << EOF
cd ~/credis
pkill -f -9 redis-server
sleep 2
./setup.sh $num_nodes
sleep 2
EOF
            # Launch tail.  We pass $HEAD_SERVER to setup.sh, which will skip master creation.
            ssh -o StrictHostKeyChecking=no ubuntu@${TAIL_SERVER} << EOF
cd ~/credis
pkill -f -9 redis-server
sleep 2
./setup.sh $num_nodes $HEAD_SERVER
sleep 2
EOF

            sleep 5
            ./distributed_seqput.sh $num_clients $num_nodes $write_ratio \
                                    $HEAD_SERVER $TAIL_SERVER $NODE_ADD $NODE_KILL $N &
            wait
        done
    done

done
