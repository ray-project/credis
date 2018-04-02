# Chain Replicated Redis

## Building

```
git submodule init
git submodule update

# Install tcmalloc according to
# https://github.com/gperftools/gperftools/blob/master/INSTALL

cd redis
env USE_TCMALLOC=yes make -j
cd ..

cd glog 
cmake . 
make -j install 
cd ..

cd leveldb 
make -j 
cd ..

# Install grpc. A suitable version is included as a submodule of etcd3 but you
# can install the latest version separately on the system too.
cd etcd3
git submodule update --init
cd grpc 
git submodule update --init 
make 
make install
cd ../..
```

## etcd
To make etcd-related tests pass and experiment with the etcd master, you
need to have etcd running locally, listening on 127.0.0.1:12379.

A really simple way to do this is to install Docker, and then:

```
docker run -d -p 12379:2379 appcelerator/etcd
```

To run etcd without Docker, you will need to install and run it manually.

There are two options for doing this, depending on if you have Go installed.

#### With Go Installed
```$xslt
$ go get github.com/coreos/etcd
# Both listen-client-urls and advertise-client-urls are needed
$ $GOPATH/bin/etcd --listen-client-urls http://127.0.0.1:12379 \
                   --advertise-client-urls http://127.0.0.1:12379
```

#### Without Go installed
```$xslt
$ git clone github.com/coreos/etcd
$ cd etcd
$ ./build
$ ./bin/etcd --listen-client-urls http://127.0.0.1:12379 \
             --advertise-client-urls http://127.0.0.1:12379
```

## Trying it out

First we start the master and two chain members:

```
cd build/src
# Start the master
../../redis/src/redis-server --loadmodule libmaster.so --port 6369
# Start the first chain members
../../redis/src/redis-server --loadmodule libmember.so --port 6370
../../redis/src/redis-server --loadmodule libmember.so --port 6371
```

Now we register the chain members with the master:

```
redis-cli -p 6369 MASTER.ADD 127.0.0.1 6370
redis-cli -p 6369 MASTER.ADD 127.0.0.1 6371
```

Do some write requests to the first server:

```
redis-cli -p 6370
> MEMBER.PUT a 1
> MEMBER.PUT b 2
```

Add a new tail:

```
../../redis/src/redis-server --loadmodule libmember.so --port 6372
redis-cli -p 6369 MASTER.ADD 127.0.0.1 6372
```

Check that replication worked:

```
redis-cli -p 6372
> get a
```

## Trying it out (etcd version)

Start etcd as in the section above, then start the master and two chain members:

```
cd build
redis-server --loadmodule src/etcd/libetcd_master.so 127.0.0.1:12379/my_cool_chain --port 6369
# The first argument is the master type: 0 = MASTER_REDIS (default), 1 = MASTER_ETCD
redis-server --loadmodule src/libmember.so 1 [heartbeat interval] [heartbeat timeout] --port 6370
redis-server --loadmodule src/libmember.so 1 [heartbeat interval] [heartbeat timeout] --port 6371
redis-server --loadmodule src/libmember.so 1 [heartbeat interval] [heartbeat timeout] --port 6372
```

Register the chain members:

```
redis-cli -p 6369 MASTER.ADD 127.0.0.1 6370
redis-cli -p 6369 MASTER.ADD 127.0.0.1 6371
redis-cli -p 6369 MASTER.ADD 127.0.0.1 6372
```

And start the chain members' heartbeats:

```
redis-cli -p 6370 MEMBER.CONNECT_TO_MASTER 127.0.0.1:12379/my_cool_chain
redis-cli -p 6371 MEMBER.CONNECT_TO_MASTER 127.0.0.1:12379/my_cool_chain
redis-cli -p 6372 MEMBER.CONNECT_TO_MASTER 127.0.0.1:12379/my_cool_chain
```

If you kill a node, it will be removed from the chain within the heartbeat timeout you gave.
The default is an interval of 3 and a timeout of 15 sec.

However, you can also inform the master that a node died with MASTER.REMOVE:

```
redis-cli -p 6369 MASTER.REMOVE 127.0.0.1 6370
```


