#!/bin/bash
set -x

num_clients=4

# T = 10: huge bump & drop

# for interval in 2.0 1.0 0.5 0.1 0.01; do
# for interval in 10.0 1.0 0.1; do
# for interval in 10.0 5.0 2.0 1.0 0.5 0.1; do
for interval in 10.0 ; do
    sleep 2
    logfile=records-interval${interval}.log
    flusherlog=flusher-interval${interval}.log
    ./setup.sh & (sleep 1; time ./seqput.sh ${num_clients}; sleep 1; sudo pkill -f redis-server) & (sleep 1; psrecord $(pgrep -f 'redis-server.*6370') --interval 1 --duration 440 --log ${logfile}) & (sudo rm -rf /tmp/gcs_ckpt; sleep 1; python flusher.py -t ${interval} &>${flusherlog})
    wait
done
