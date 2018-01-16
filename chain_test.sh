#!/bin/bash
set -ex

./setup.sh 3

# Tests below
echo

# All -1.
redis-cli -p 6370 MEMBER.SN
redis-cli -p 6371 MEMBER.SN
redis-cli -p 6372 MEMBER.SN
echo

# Nothing in checkpoint
redis-cli -p 6372 list.checkpoint

# To test TAIL.CHECKPOINT
dummy_client_id=0
sleep 3
redis-cli -p 6370 member.put k1 v1 $dummy_client_id
# (nil)
redis-cli -p 6372 get k1
# "v1"
redis-cli -p 6372 tail.checkpoint
# (integer) 1
redis-cli -p 6372 list.checkpoint
# k1: v1
redis-cli -p 6372 tail.checkpoint
# (integer) 0
redis-cli -p 6370 member.put k1 v2 $dummy_client_id
# (nil)
redis-cli -p 6370 member.put k2 v2 $dummy_client_id
# (nil)
redis-cli -p 6372 tail.checkpoint
# (integer) 2
redis-cli -p 6372 list.checkpoint
# k1: v2; k2: v2
echo

# To test MEMBER.SN
redis-cli -p 6370 MEMBER.SN
redis-cli -p 6371 MEMBER.SN
redis-cli -p 6372 MEMBER.SN
