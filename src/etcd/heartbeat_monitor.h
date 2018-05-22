#ifndef CREDIS_HEARTBEAT_MONITOR_H
#define CREDIS_HEARTBEAT_MONITOR_H

#include "grpcpp/grpcpp.h"

class HeartbeatMonitor {
 public:
  HeartbeatMonitor(std::shared_ptr<grpc::Channel> channel, int redis_port);

  // Watch all heartbeat keys under CHAIN_ID, calling MASTER.REMOVE as
  // needed (via the redis port) when they expire.
  grpc::Status Monitor(std::string chain_id);

 private:
  std::shared_ptr<grpc::Channel> channel_;
  int redis_port_;
};

#endif  // CREDIS_HEARTBEAT_MONITOR_H
