#!/bin/bash
set -ex

./setup.sh 3

dummy_client_id=0
redis-cli -p 6370 member.put k1 v1 $dummy_client_id
# (nil)
redis-cli -p 6372 get k1
# "v1"
redis-cli -p 6372 tail.checkpoint
# (integer) 1
redis-cli -p 6372 tail.checkpoint
# (integer) 0
redis-cli -p 6372 list.checkpoint
# k1: v1
redis-cli -p 6370 member.put k2 v2 $dummy_client_id
# (nil)
redis-cli -p 6372 tail.checkpoint
# (integer) 1
redis-cli -p 6372 list.checkpoint
# k1: v1; k2: v2

# Flush 1 next.
redis-cli -p 6370 head.flush $dummy_client_id
# k1 is gone.
redis-cli -p 6372 keys '*'
# Flush another.
redis-cli -p 6370 head.flush $dummy_client_id
# k2 is gone.
redis-cli -p 6372 keys '*'
# READ will read from checkpoint file.
redis-cli -p 6372 read k1
redis-cli -p 6372 read k2
# Subsequent flushes will show nothing to flush.
redis-cli -p 6370 head.flush $dummy_client_id
