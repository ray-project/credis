# What is this branch?
This branch is the result of my (@graysonchao) CS262A project in which I made libmaster fault-tolerant and able to automatically detect and repair chain failures.

Full discussion can be found at https://github.com/ray-project/credis/pull/5.

A summary of the main changes:

- `src/etcd/etcd_master.cc`: Implemented a new master that saves and loads it state from an etcd instance. This allows its state to be restored after crashes. It also uses a somewhat baroque way of detecting cluster liveness with heartbeats implemented as etcd lease-associated keys, which lets it repair the chain after members fail. In all other ways, it is the same as the old master.

- `src/etcd/heartbeat_monitor.cc`: Implementation of the heartbeat monitoring logic used by `etcd_master.cc`.

- `src/etcd/etcd_master_client.cc`: A client that can be used to communicate with the etcd master. Supports get/set watermark, but this branch does not have the logic to ask the master for the head or tail (simple to write, but the exact code is lost to history because it got added at the last minute of the semester.)

- `src/member.cc`: Modified the member to accept a "master mode" argument from the command line. "Master mode" can be 0 (meaning default Redis-only master behavior) or 1 (meaning etcd). If etcd master mode is enabled, then `MEMBER.CONNECT_TO_MASTER` takes an etcd url such as `127.0.0.1:2379/[OPTIONAL_PATH_PREFIX]` and uses this etcd instance to report its liveness via heartbeat.

I wrote a long list of criticisms of the above changes [here](https://github.com/ray-project/credis/pull/5#issuecomment-389048723). Those criticisms can serve as a guide for a future implementation. One suggestion about point 4: implement the healthcheck using the MONITOR or INFO commands rather than having the members themselves send heartbeats. This would remove basically all the heartbeat logic from the member. Instead, the master should poll the members. This would make it easier to be compatible with something like Nagios, because you can just put a RESP-to-TCP proxy in between the NOC and the chain.








