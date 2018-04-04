#include <leveldb/include/leveldb/status.h>
#include <memory>

#include "etcd3-cpp/include/etcd3.h"
#include "glog/logging.h"
#include "grpcpp/grpcpp.h"

#include "redis/deps/hiredis/hiredis.h"

#include "heartbeat_monitor.h"
#include "src/etcd/etcd_utils.h"
#include "src/utils.h"

using etcd3::pb::RangeRequest;
using etcd3::pb::RangeResponse;
using etcd3::pb::WatchCreateRequest;
using etcd3::pb::WatchRequest;
using etcd3::pb::WatchResponse;
using etcd3::util::RangePrefix;

HeartbeatMonitor::HeartbeatMonitor(std::shared_ptr<grpc::Channel> channel,
                                   int redis_port)
    : channel_(channel), redis_port_(redis_port) {}

grpc::Status HeartbeatMonitor::Monitor(std::string chain_id) {
  etcd3::Client etcd(channel_);
  redisContext* redis_ctx = SyncConnect("127.0.0.1", redis_port_);
  CHECK(!redis_ctx->err) << "Heartbeat monitor couldn't connect to the master.";

  auto wcr = new etcd3::pb::WatchCreateRequest();
  wcr->set_key(chain_id + "/heartbeat");
  wcr->set_range_end(RangePrefix(wcr->key()));
  auto changes = etcd.MakeWatchStream(wcr);

  etcd3::pb::WatchResponse response;
  while (changes->Read(&response)) {
    for (auto ev : response.events()) {
      if (ev.type() == etcd3::pb::Event_EventType_DELETE) {
        auto addr = ev.kv().key();
        addr = addr.substr(addr.find_last_of('/') + 1);
        auto host = addr.substr(0, addr.find_first_of(':'));
        auto port = addr.substr(addr.find_first_of(':') + 1);
        CHECK(redisReconnect(redis_ctx) == REDIS_OK);
        redisReply* reply = reinterpret_cast<redisReply*>(redisCommand(
            redis_ctx, "MASTER.REMOVE %s %s", host.c_str(), port.c_str()));
        CHECK(reply != NULL)
            << "reply is NULL, IO error: " << std::string(redis_ctx->errstr);
        freeReplyObject(reply);
      }
    }
  }
  return grpc::Status::CANCELLED;
}
